package main

import (
	"bufio"
	"flag"
	"io"
	"log/slog"
	pb "mapreduce/proto"
	"net"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	// "fmt"
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

var NUM_WORKERS int64 = int64(runtime.NumCPU())
var JOBS_Q_SIZE int64 = NUM_WORKERS * 2
var RESULTS_Q_SIZE int64 = NUM_WORKERS

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func ensure(e error) {
	if e != nil {
		panic(e)
	}
}

func cleanup(dir string) {
	e := os.Remove(filepath.Join(dir, "index"))
	if e != nil && !os.IsNotExist(e) {
		panic(e)
	}
	entries, e := os.ReadDir(dir)
	check(e)
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), spillFilePrefix) {
			os.Remove(filepath.Join(dir, entry.Name()))
		}
	}
}

func setupLogger() {
	var handler slog.Handler
	opts := &slog.HandlerOptions{Level: slog.LevelInfo}

	if os.Getenv("LOG_FORMAT") == "json" {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}
	slog.SetDefault(slog.New(handler))
}

func reducePartition(partition int, mapResults []MapResult) []KeyValue {
	index := make(map[string][]string)
	for _, result := range mapResults {
		path, ok := result.PartitionFiles[partition]
		if !ok {
			continue
		}
		file, e := os.Open(path)
		check(e)
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
		file.Close()
		check(scanner.Err())
	}

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

func GroupShuffle(mapResults []MapResult, numWorkers int64, dir string) {
	ch := make(chan partitionResult, numWorkers)
	var wg sync.WaitGroup
	for p := range int(numWorkers) {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			ch <- partitionResult{partition: p, kvs: reducePartition(p, mapResults)}
		}(p)
	}
	go func() {
		wg.Wait()
		close(ch)
	}()

	ordered := make([][]KeyValue, numWorkers)
	for r := range ch {
		ordered[r.partition] = r.kvs
	}

	f, e := os.Create(filepath.Join(dir, "index"))
	check(e)
	defer f.Close()
	w := bufio.NewWriter(f)
	for _, kvs := range ordered {
		for _, kv := range kvs {
			w.WriteString(kv.key + "->" + kv.value + "\n")
		}
	}
	check(w.Flush())
}

func fetchResultFromWorker(ctx context.Context, addr string, partition int, w *bufio.Writer) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	check(err)
	defer conn.Close()

	wc := pb.NewWorkerClient(conn)
	stream, err := wc.FetchResult(ctx, &pb.FetchResultRequest{Partition: int32(partition)})
	check(err)

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			return
		}
		check(err)
		_, err = w.Write(chunk.Data)
		check(err)
	}

}

func collectResults(impl *masterImpl, dir string) {
	impl.mtx.Lock()
	addrs := make([]string, len(impl.reduceTasks))
	for i, t := range impl.reduceTasks {
		addrs[i] = t.workerAddr
	}
	impl.mtx.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	f, e := os.Create(filepath.Join(dir, "index"))
	check(e)
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()

	for partition, addr := range addrs {
		if addr == "" {
			continue
		}
		fetchResultFromWorker(ctx, addr, partition, w)
	}
}

// connection setups
func runMaster(inputFiles []string, min_workers int) {
	dir, e := os.Getwd()
	check(e)
	cleanup(dir)

	numPartitions := int(NUM_WORKERS)

	mapTasks := make([]mapTask, len(inputFiles))
	for i, path := range inputFiles {
		mapTasks[i] = mapTask{id: i, filePath: path, state: taskIdle}
	}

	reduceTasks := make([]reduceTask, numPartitions)
	for i := range numPartitions {
		reduceTasks[i] = reduceTask{id: len(mapTasks) + i, partition: i, state: taskIdle}
	}

	impl := &masterImpl{
		mapTasks:      mapTasks,
		reduceTasks:   reduceTasks,
		numPartitions: numPartitions,
		minWorkers:    min_workers,
		done:          make(chan struct{}),
		lastSeen:      make(map[string]time.Time),
	}

	lis, e := net.Listen("tcp", ":50051")
	check(e)
	s := grpc.NewServer()
	pb.RegisterMasterServer(s, impl)
	slog.Info("master listening on :50051")

	stopChecker := make(chan struct{})

	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stopChecker:
				return
			case <-ticker.C:
				impl.checkTimeouts(5 * time.Second)
			}
		}
	}()

	go func() {
		// waiting till anything appears in impl.done channel,
		// as smth can appear there only when all reduces are done
		<-impl.done
		close(stopChecker)
		slog.Info("all reduces are done")
		collectResults(impl, dir)
		slog.Info("done")

		impl.mtx.Lock()
		impl.allDone = true
		impl.mtx.Unlock()

		time.Sleep(1 * time.Second)
		s.GracefulStop()
	}()

	if err := s.Serve(lis); err != nil {
		slog.Error("fatal error", "err", err)
		os.Exit(1)
	}
}

func runWorker(m_addr, my_addr string) {
	log := slog.With("worker_addr", my_addr, "master_addr", m_addr)
	dir, e := os.Getwd()
	check(e)
	log.Info("worker starting")

	conn, err := grpc.NewClient(m_addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	check(err)
	master_client := pb.NewMasterClient(conn)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err = master_client.RegisterWorker(ctx, &pb.WorkerInfo{Addr: my_addr})
	check(err)
	log.Info("registered worker")

	go heartbeatLoop(ctx, master_client, my_addr)

	lis, err := net.Listen("tcp", my_addr)
	check(err)
	s := grpc.NewServer()
	pb.RegisterWorkerServer(s, &workerImpl{dir: dir})
	go s.Serve(lis)
	log.Info("worker gRPC server listening")

	for {
		task, err := master_client.RequestTask(ctx, &pb.TaskRequest{WorkerAddr: my_addr})
		if err != nil {
			log.Error("RequestTask error", "err", err)
			return
		}
		if task.Type != pb.Task_IDLE {
			log.Info("task assigned", "type", task.Type, "id", task.TaskId)
		}
		switch task.Type {
		case pb.Task_MAP:
			doMap(task, dir, my_addr, master_client, ctx)
		case pb.Task_REDUCE:
			doReduce(task, dir, my_addr, master_client, ctx)
		case pb.Task_SHUTDOWN:
			return
		case pb.Task_IDLE:
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func main() {
	mode := flag.String("mode", "worker", "master|worker")
	my_addr := flag.String("addr", ":50052", "this worker's listen address")
	master_addr := flag.String("master_addr", ":50051", "master address")
	min_workers := flag.Int("min_workers", 1, "master waits for this many workers b4 starting")
	flag.Parse()

	setupLogger()

	switch *mode {
	case "master":
		runMaster(flag.Args(), *min_workers)
	case "worker":
		runWorker(*master_addr, *my_addr)
	default:
		slog.Error("unknown mode", "mode", *mode)
		os.Exit(1)
	}
}
