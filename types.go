package main

import (
	"bufio"
	pb "mapreduce/proto"
	"os"
	"sync"
	"time"
)

type Chunk struct {
	FileID      int
	ChunkID     int
	StartOffset int64
	Data        []byte
}

type SpillFile struct {
	Partition int
	Path      string
}

type MapResult struct {
	FileID         int
	ChunkID        int
	PartitionFiles map[int]string
}

type KeyValue struct {
	key   string
	value string
}

type WorkerContext struct {
	writers        []*bufio.Writer
	files          []*os.File
	PartitionFiles map[int]string
}

type partitionResult struct {
	partition int
	kvs       []KeyValue
}

type taskState int

const (
	taskIdle taskState = iota
	taskRunning
	taskDone
)

type masterImpl struct {
	pb.UnimplementedMasterServer

	mtx        sync.Mutex
	workers    []string
	minWorkers int
	started    bool

	mapTasks      []mapTask
	reduceTasks   []reduceTask
	mapDone       int
	reduceDone    int
	numPartitions int

	lastSeen map[string]time.Time

	allDone bool
	done    chan struct{}
}

type mapTask struct {
	id         int
	filePath   string
	state      taskState
	workerAddr string
}

type reduceTask struct {
	id         int
	partition  int
	state      taskState
	workerAddr string
}
