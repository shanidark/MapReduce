package main

import (
	"bufio"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

const spillFilePrefix = "map-"

var reg_nonWord = regexp.MustCompile("[^a-zA-Z0-9]+")

func Process(word string) string {
	return reg_nonWord.ReplaceAllString(word, "")
}

func OpenWorkerContext(workerID int, numPartitions uint64, dir string) WorkerContext {
	files := make([]*os.File, numPartitions)
	writers := make([]*bufio.Writer, numPartitions)
	partitionFiles := make(map[int]string, numPartitions)
	for i := range numPartitions {
		name := spillFilePrefix + strconv.Itoa(workerID) + "-" + strconv.FormatUint(i, 10)
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

func (wc *WorkerContext) Close() {
	for i, w := range wc.writers {
		ensure(w.Flush())
		ensure(wc.files[i].Close())
	}
}

func Map(chunk Chunk, wc *WorkerContext) MapResult {
	docname := strconv.Itoa(chunk.FileID)
	n := uint64(len(wc.writers))

	for _, entry := range strings.Fields(string(chunk.Data)) {
		word := Process(entry)
		if word == "" {
			continue
		}
		hash := Hash(word) % n
		_, e := wc.writers[hash].WriteString(word + " -> " + docname + "\n")
		check(e)
	}

	return MapResult{
		FileID:         chunk.FileID,
		ChunkID:        chunk.ChunkID,
		PartitionFiles: wc.PartitionFiles,
	}
}
