package main

import (
	"context"
	"flag"
	"log/slog"
	pb "mapreduce/proto"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/google/uuid"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

var NUM_WORKERS int64 = int64(runtime.NumCPU())

func check(e error) {
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

func runClient(masterAddr string, files []string) {
	if len(files) == 0 {
		slog.Error("no files provided to client")
		os.Exit(1)
	}

	ctx := context.Background()

	store, err := NewS3Store(ctx)
	check(err)

	uploadID := uuid.NewString()

	s3Keys := make([]string, 0, len(files))
	for _, path := range files {
		f, err := os.Open(path)
		if err != nil {
			slog.Error("cannot open input file", "path", path, "err", err)
			os.Exit(1)
		}
		stat, err := f.Stat()
		check(err)

		key := "input/" + uploadID + "/" + filepath.Base(path)
		slog.Info("uploading input", "path", path, "key", key, "size", stat.Size())

		if err := store.Put(ctx, key, f, stat.Size()); err != nil {
			f.Close()
			slog.Error("upload failed", "path", path, "err", err)
			os.Exit(1)
		}
		f.Close()
		s3Keys = append(s3Keys, key)
	}

	conn, err := grpc.NewClient(masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	check(err)
	defer conn.Close()
	mc := pb.NewMasterClient(conn)

	resp, err := mc.SubmitJob(ctx, &pb.SubmitJobRequest{FilePaths: s3Keys})
	if err != nil {
		if st, ok := status.FromError(err); ok && st.Code() == codes.ResourceExhausted {
			slog.Error("master is rate-limiting submissions, try again later", "err", err)
			os.Exit(1)
		}
		check(err)
	}
	jobID := resp.JobId
	slog.Info("job submitted", "job_id", jobID, "num_files", len(s3Keys))

	const (
		basePollInterval     = 1 * time.Second
		maxPollInterval      = 30 * time.Second
		maxConsecutiveErrors = 10
	)

	pollInterval := basePollInterval
	consecutiveErrors := 0

	for {
		time.Sleep(pollInterval)
		status, err := mc.GetJobStatus(ctx, &pb.GetJobStatusRequest{JobId: jobID})
		if err != nil {
			consecutiveErrors++
			slog.Warn("GetJobStatus failed",
				"err", err,
				"consecutive_errors", consecutiveErrors,
				"next_retry_in", pollInterval)
			if consecutiveErrors >= maxConsecutiveErrors {
				slog.Error("giving up on job status polling",
					"job_id", jobID,
					"consecutive_errors", consecutiveErrors)
				os.Exit(1)
			}
			pollInterval = min(pollInterval*2, maxPollInterval)
			continue
		}

		consecutiveErrors = 0
		pollInterval = basePollInterval

		if status.Status == pb.GetJobStatusResponse_DONE {
			slog.Info("job done", "job_id", jobID, "index_path", status.IndexPath)
			return
		}
		if status.Status == pb.GetJobStatusResponse_FAILED {
			slog.Error("job failed", "job_id", jobID, "error", status.ErrorMessage)
			os.Exit(1)
		}
		slog.Info("job still running", "job_id", jobID)
	}
}

func runMaster(minWorkers int, metricsAddr string) {
	go serveMetrics(metricsAddr)

	dir, e := os.Getwd()
	check(e)
	cleanup(dir)

	ctx := context.Background()
	store, err := NewS3Store(ctx)
	check(err)

	impl := &masterImpl{
		minWorkers:    minWorkers,
		lastSeen:      make(map[string]time.Time),
		jobs:          make(map[int32]*job),
		dir:           dir,
		store:         store,
		submitLimiter: rate.NewLimiter(5, 10),
	}

	lis, e := net.Listen("tcp", ":50051")
	check(e)
	s := grpc.NewServer()
	pb.RegisterMasterServer(s, impl)
	slog.Info("master listening on :50051")

	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			impl.checkTimeouts(5 * time.Second)
		}
	}()

	if err := s.Serve(lis); err != nil {
		slog.Error("fatal error", "err", err)
		os.Exit(1)
	}
}

func runWorker(mAddr, myAddr, metricsAddr string) {
	go serveMetrics(metricsAddr)
	log := slog.With("worker_addr", myAddr, "masterAddr", mAddr)
	dir, e := os.Getwd()
	check(e)
	log.Info("worker starting")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, err := NewS3Store(ctx)
	check(err)

	conn, err := grpc.NewClient(mAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	check(err)
	masterClient := pb.NewMasterClient(conn)

	_, err = masterClient.RegisterWorker(ctx, &pb.WorkerInfo{Addr: myAddr})
	check(err)
	log.Info("registered worker")

	go heartbeatLoop(ctx, masterClient, myAddr)

	for {
		task, err := masterClient.RequestTask(ctx, &pb.TaskRequest{WorkerAddr: myAddr})
		if err != nil {
			log.Error("RequestTask error", "err", err)
			return
		}
		if task.Type != pb.Task_IDLE {
			log.Info("task assigned", "type", task.Type, "id", task.TaskId)
		}
		switch task.Type {
		case pb.Task_MAP:
			doMap(task, dir, myAddr, store, masterClient, ctx)
		case pb.Task_REDUCE:
			doReduce(task, dir, myAddr, store, masterClient, ctx)
		case pb.Task_SHUTDOWN:
			return
		case pb.Task_IDLE:
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func init() {
	tasksAssigned.WithLabelValues("map").Add(0)
	tasksAssigned.WithLabelValues("reduce").Add(0)
	tasksCompleted.WithLabelValues("map").Add(0)
	tasksCompleted.WithLabelValues("reduce").Add(0)
	tasksReclaimed.WithLabelValues("map").Add(0)
	tasksReclaimed.WithLabelValues("reduce").Add(0)
	taskDuration.WithLabelValues("map").Observe(0)
	taskDuration.WithLabelValues("reduce").Observe(0)
}

func main() {
	mode := flag.String("mode", "worker", "master|worker|client")
	myAddr := flag.String("addr", ":50052", "this worker's listen address")
	masterAddr := flag.String("master_addr", ":50051", "master address")
	minWorkers := flag.Int("min_workers", 1, "master waits for this many workers b4 starting")
	metricsAddr := flag.String("metrics_addr", ":9090", "prometheus metrics endpoint access")
	flag.Parse()

	setupLogger()

	switch *mode {
	case "master":
		runMaster(*minWorkers, *metricsAddr)
	case "client":
		runClient(*masterAddr, flag.Args())
	case "worker":
		runWorker(*masterAddr, *myAddr, *metricsAddr)
	default:
		slog.Error("unknown mode", "mode", *mode)
		os.Exit(1)
	}
}
