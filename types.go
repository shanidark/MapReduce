package main

import (
	"bufio"
	"os"
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
