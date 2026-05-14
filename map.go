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

func Map(chunk Chunk, workerID int, numPartitions uint64, dir string) MapResult {
	docname := strconv.Itoa(chunk.FileID)
	content := string(chunk.Data)

	files := make([]*os.File, numPartitions)
	writers := make([]*bufio.Writer, numPartitions)
	for i := range numPartitions {
		name := spillFilePrefix + strconv.Itoa(workerID) + "-" + strconv.FormatUint(i, 10)
		path := filepath.Join(dir, name)
		f, e := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		check(e)
		files[i] = f
		writers[i] = bufio.NewWriter(f)
	}

	for _, entry := range strings.Fields(content) {
		word := Process(entry)
		if word == "" {
			continue
		}
		hash := Hash(word) % numPartitions
		_, e := writers[hash].WriteString(word + " -> " + docname + "\n")
		check(e)
	}

	partitionFiles := make(map[int]string, numPartitions)
	for i, w := range writers {
		ensure(w.Flush())
		ensure(files[i].Close())
		partitionFiles[i] = files[i].Name()
	}
	return MapResult{
		FileID:         chunk.FileID,
		ChunkID:        chunk.ChunkID,
		PartitionFiles: partitionFiles,
	}
}
