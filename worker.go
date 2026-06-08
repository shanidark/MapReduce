package main

import (
	"bufio"
	"context"
	"io"
	pb "mapreduce/proto"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type workerImpl struct {
	pb.UnimplementedWorkerServer
	dir string
}

func hasSpillPartition(name string, partition int) bool {
	if !strings.HasPrefix(name, spillFilePrefix) {
		return false
	}
	suffix := "-" + strconv.Itoa(partition)
	return strings.HasSuffix(name, suffix)
}

func (w *workerImpl) FetchSpill(req *pb.FetchSpillRequest,
	stream pb.Worker_FetchSpillServer) error {
	entries, err := os.ReadDir(w.dir)
	check(err)
	buf := make([]byte, 64*1024)
	for _, entry := range entries {
		if !hasSpillPartition(entry.Name(), int(req.Partition)) {
			continue
		}
		file, err := os.Open(filepath.Join(w.dir, entry.Name()))
		check(err)

		for {
			n, err := file.Read(buf)
			if n > 0 {
				if sendErr := stream.Send(&pb.SpillChunk{Data: buf[:n]}); sendErr != nil {
					file.Close()
					return sendErr
				}
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				file.Close()
				return err
			}
		}
		file.Close()
	}
	return nil

}

// / DO MAP
func OpenMapTaskContext(taskID int, numPartitions uint64, dir string) WorkerContext {
	files := make([]*os.File, numPartitions)
	writers := make([]*bufio.Writer, numPartitions)
	partitionFiles := make(map[int]string, numPartitions)

	for i := range numPartitions {
		name := spillFilePrefix + strconv.Itoa(taskID) + "-" + strconv.FormatUint(i, 10)
		path := filepath.Join(dir, name)
		f, e := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		check(e)
		files[i] = f
		writers[i] = bufio.NewWriter(f)
		partitionFiles[int(i)] = path
	}
	return WorkerContext{
		writers:        writers,
		files:          files,
		PartitionFiles: partitionFiles,
	}
}

func doMap(task *pb.Task, dir string, my_addr string,
	mc pb.MasterClient, ctx context.Context) {
	n := uint64(task.NumPartitions)
	taskID := int(task.TaskId)

	wc := OpenMapTaskContext(taskID, n, dir)
	defer wc.Close()

	file, err := os.Open(task.FilePath)
	check(err)
	defer file.Close()

	buf := make([]byte, ChunkSize)
	for {
		n, err := file.Read(buf)
		if n > 0 {
			chunk := Chunk{
				FileID:  taskID,
				ChunkID: 0,
				Data:    buf[:n],
			}
			Map(chunk, &wc)
		}
		if err == io.EOF {
			break
		}
		check(err)
	}

	_, err = mc.ReportDone(ctx, &pb.TaskDone{
		WorkerAddr: my_addr,
		TaskId:     task.TaskId,
	})
	check(err)
}

// DO REDUCE
func doReduce(task *pb.Task, dir string, my_addr string,
	mc pb.MasterClient, ctx context.Context) {
	partition := int(task.Partition)
	taskID := task.TaskId

	// downloading spill-files
	mergedPath := filepath.Join(dir, "reduce-merged-"+strconv.Itoa(partition))
	merged, err := os.Create(mergedPath)
	check(err)

	for _, addr := range task.SpillAddrs {
		fetchSpillFromWorker(ctx, addr, partition, merged)
	}
	merged.Close()

	// reduce
	kvs := reducePartitionFromFile(mergedPath)

	// making index part
	resultPath := filepath.Join(dir, "index-"+strconv.Itoa(partition))
	f, err := os.Create(resultPath)
	check(err)
	w := bufio.NewWriter(f)
	for _, kv := range kvs {
		w.WriteString(kv.key + " -> " + kv.value + "\n")
	}
	ensure(w.Flush())
	f.Close()

	os.Remove(mergedPath)

	_, err = mc.ReportDone(ctx, &pb.TaskDone{
		WorkerAddr: my_addr,
		TaskId:     taskID,
	})
	check(err)
}

func fetchSpillFromWorker(ctx context.Context, addr string, partition int, out *os.File) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	check(err)
	defer conn.Close()

	wc := pb.NewWorkerClient(conn)
	stream, err := wc.FetchSpill(ctx, &pb.FetchSpillRequest{Partition: int32(partition)})
	check(err)

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		check(err)
		_, err = out.Write(chunk.Data)
		check(err)
	}
}

func reducePartitionFromFile(path string) []KeyValue {
	file, err := os.Open(path)
	check(err)
	defer file.Close()

	index := make(map[string][]string)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		word, doc, ok := strings.Cut(line, " -> ")
		if !ok || word == "" || doc == "" {
			continue
		}
		index[word] = append(index[word], doc)
	}
	check(scanner.Err())

	keys := make([]string, 0, len(index))
	for key := range index {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	kvs := make([]KeyValue, 0, len(keys))
	for _, key := range keys {
		kvs = append(kvs, Reduce(key, index[key]))
	}
	return kvs
}

// Fetching via gRPC
// This method is called from master through gRPC
func (w *workerImpl) FetchResult(req *pb.FetchResultRequest,
	stream pb.Worker_FetchResultServer) error {
	name := "index-" + strconv.Itoa(int(req.Partition))
	path := filepath.Join(w.dir, name)

	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()

	buf := make([]byte, 64*1024)
	for {
		n, err := file.Read(buf)
		if n > 0 {
			if sendErr := stream.Send(&pb.SpillChunk{Data: buf[:n]}); sendErr != nil {
				return sendErr
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
