package main

import (
	"bufio"
	pb "mapreduce/proto"
	"os"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type Chunk struct {
	FileID int
	Data   []byte
}

type KeyValue struct {
	key   string
	value string
}

type WorkerContext struct {
	writers        []*bufio.Writer
	files          []*os.File
	PartitionFiles map[int]string
	store          ObjectStore
	s3Keys         []string
	jobID          int32
	taskID         int
}

type taskState int

const (
	taskIdle taskState = iota
	taskRunning
	taskDone
	taskFailed
)

type jobStatus int

const (
	jobRunning jobStatus = iota
	jobCollecting
	jobDone
	jobFailed
)

type job struct {
	id            int32
	files         []string
	numPartitions int

	mapTasks    []mapTask
	reduceTasks []reduceTask
	mapDone     int
	reduceDone  int

	status    jobStatus
	indexPath string
}

type masterImpl struct {
	pb.UnimplementedMasterServer

	mtx        sync.Mutex
	workers    []string
	minWorkers int
	started    bool

	lastSeen map[string]time.Time

	nextJobID int32
	jobs      map[int32]*job

	dir   string
	store ObjectStore

	submitLimiter *rate.Limiter
}

type mapTask struct {
	id         int
	filePath   string
	state      taskState
	workerAddr string
	attempts   int
}

type reduceTask struct {
	id         int
	partition  int
	state      taskState
	workerAddr string
	attempts   int
}
