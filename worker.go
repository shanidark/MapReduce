package main

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	pb "mapreduce/proto"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

func OpenMapTaskContext(jobID int32, taskID int, numPartitions uint64, dir string, store ObjectStore) WorkerContext {
	files := make([]*os.File, numPartitions)
	writers := make([]*bufio.Writer, numPartitions)
	partitionFiles := make(map[int]string, numPartitions)
	s3Keys := make([]string, numPartitions)

	for i := range numPartitions {
		name := spillFilePrefix + strconv.FormatInt(int64(jobID), 10) + "-" + strconv.Itoa(taskID) + "-" + strconv.FormatUint(i, 10)
		path := filepath.Join(dir, name)
		f, e := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		check(e)
		files[i] = f
		writers[i] = bufio.NewWriter(f)
		partitionFiles[int(i)] = path
		s3Keys[i] = "spill/" + strconv.FormatInt(int64(jobID), 10) + "/" + strconv.Itoa(taskID) + "/" + strconv.FormatUint(i, 10)
	}
	return WorkerContext{
		writers:        writers,
		files:          files,
		PartitionFiles: partitionFiles,
		store:          store,
		s3Keys:         s3Keys,
		jobID:          jobID,
		taskID:         taskID,
	}
}

func doMap(task *pb.Task, dir string, myAddr string, store ObjectStore,
	mc pb.MasterClient, ctx context.Context) {
	start := time.Now()
	defer func() { taskDuration.WithLabelValues("map").Observe(time.Since(start).Seconds()) }()
	log := slog.With("worker_addr", myAddr, "task_id", task.TaskId, "job_id", task.JobId, "phase", "map")
	log.Info("doMap starting", "s3_key", task.FilePath)

	if s := os.Getenv("SIMULATE_SLOW_MAP"); s != "" {
		d, _ := time.ParseDuration(s)
		time.Sleep(d)
	}

	n := uint64(task.NumPartitions)
	taskID := int(task.TaskId)

	wc := OpenMapTaskContext(task.JobId, taskID, n, dir, store)

	reader, err := store.Get(ctx, task.FilePath)
	if err != nil {
		log.Error("cannot open input from s3", "key", task.FilePath, "err", err)
		wc.Close(ctx)
		_, reportErr := mc.ReportDone(ctx, &pb.TaskDone{
			WorkerAddr:   myAddr,
			TaskId:       task.TaskId,
			JobId:        task.JobId,
			Failed:       true,
			ErrorMessage: err.Error(),
		})
		if reportErr != nil {
			log.Error("failed to report task failure", "err", reportErr)
		}
		return
	}

	buf := make([]byte, ChunkSize)
	for {
		nRead, err := reader.Read(buf)
		if nRead > 0 {
			Map(Chunk{FileID: taskID, Data: buf[:nRead]}, &wc)
		}
		if err == io.EOF {
			break
		}
		check(err)
	}
	reader.Close()

	if err := wc.Close(ctx); err != nil {
		log.Error("failed to upload spills to s3", "err", err)
		_, reportErr := mc.ReportDone(ctx, &pb.TaskDone{
			WorkerAddr:   myAddr,
			TaskId:       task.TaskId,
			JobId:        task.JobId,
			Failed:       true,
			ErrorMessage: err.Error(),
		})
		if reportErr != nil {
			log.Error("failed to report task failure", "err", reportErr)
		}
		return
	}

	_, err = mc.ReportDone(ctx, &pb.TaskDone{
		WorkerAddr: myAddr,
		TaskId:     task.TaskId,
		JobId:      task.JobId,
	})
	check(err)
	log.Info("doMap finished")
}

func doReduce(task *pb.Task, dir string, myAddr string, store ObjectStore,
	mc pb.MasterClient, ctx context.Context) {
	start := time.Now()
	defer func() { taskDuration.WithLabelValues("reduce").Observe(time.Since(start).Seconds()) }()
	log := slog.With("worker_addr", myAddr, "task_id", task.TaskId, "job_id", task.JobId, "partition", task.Partition, "phase", "reduce")
	log.Info("doReduce starting")

	if s := os.Getenv("SIMULATE_SLOW_REDUCE"); s != "" {
		d, _ := time.ParseDuration(s)
		time.Sleep(d)
	}

	partition := int(task.Partition)
	taskID := task.TaskId
	jobID := task.JobId

	spillPrefix := "spill/" + strconv.FormatInt(int64(jobID), 10) + "/"
	spillSuffix := "/" + strconv.Itoa(partition)

	allKeys, err := store.List(ctx, spillPrefix)
	if err != nil {
		reportReduceFailure(ctx, mc, myAddr, task, log, "list spills: "+err.Error())
		return
	}

	mergedPath := filepath.Join(dir, "reduce-merged-"+strconv.FormatInt(int64(jobID), 10)+"-"+strconv.Itoa(partition))
	merged, err := os.Create(mergedPath)
	check(err)

	for _, key := range allKeys {
		if !strings.HasSuffix(key, spillSuffix) {
			continue
		}
		reader, err := store.Get(ctx, key)
		if err != nil {
			merged.Close()
			os.Remove(mergedPath)
			reportReduceFailure(ctx, mc, myAddr, task, log, "get spill "+key+": "+err.Error())
			return
		}
		if _, err := io.Copy(merged, reader); err != nil {
			reader.Close()
			merged.Close()
			os.Remove(mergedPath)
			reportReduceFailure(ctx, mc, myAddr, task, log, "copy spill "+key+": "+err.Error())
			return
		}
		reader.Close()
	}
	merged.Close()

	kvs := reducePartitionFromFile(mergedPath)

	indexKey := "index/" + strconv.FormatInt(int64(jobID), 10) + "/" + strconv.Itoa(partition)
	resultPath := filepath.Join(dir, "index-"+strconv.FormatInt(int64(jobID), 10)+"-"+strconv.Itoa(partition))
	f, err := os.Create(resultPath)
	check(err)
	w := bufio.NewWriter(f)
	for _, kv := range kvs {
		w.WriteString(kv.key + " -> " + kv.value + "\n")
	}
	check(w.Flush())
	f.Close()

	uploadFile, err := os.Open(resultPath)
	check(err)
	stat, err := uploadFile.Stat()
	check(err)
	if err := store.Put(ctx, indexKey, uploadFile, stat.Size()); err != nil {
		uploadFile.Close()
		os.Remove(resultPath)
		os.Remove(mergedPath)
		reportReduceFailure(ctx, mc, myAddr, task, log, "upload index: "+err.Error())
		return
	}
	uploadFile.Close()

	os.Remove(mergedPath)
	os.Remove(resultPath)

	_, err = mc.ReportDone(ctx, &pb.TaskDone{
		WorkerAddr: myAddr,
		TaskId:     taskID,
		JobId:      jobID,
	})
	check(err)
	log.Info("doReduce finished")
}

func reportReduceFailure(ctx context.Context, mc pb.MasterClient, myAddr string, task *pb.Task, log *slog.Logger, msg string) {
	log.Error("doReduce failed", "err", msg)
	_, reportErr := mc.ReportDone(ctx, &pb.TaskDone{
		WorkerAddr:   myAddr,
		TaskId:       task.TaskId,
		JobId:        task.JobId,
		Failed:       true,
		ErrorMessage: msg,
	})
	if reportErr != nil {
		log.Error("failed to report reduce failure", "err", reportErr)
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

func heartbeatLoop(ctx context.Context, mc pb.MasterClient, myAddr string) {
	log := slog.With("worker_addr", myAddr)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, err := mc.Heartbeat(ctx, &pb.HeartbeatRequest{WorkerAddr: myAddr})
			if err != nil {
				log.Warn("heartbeat failed", "err", err)
				heartbeatsFailed.Inc()
			}
		}
	}
}
