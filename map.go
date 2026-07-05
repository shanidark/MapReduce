package main

import (
	"regexp"
	"strconv"
	"strings"
)

const (
	spillFilePrefix = "map-"
	// ChunkSize — размер буфера при чтении входного файла в doMap.
	// 64MB — компромисс между памятью и числом syscall-ов.
	ChunkSize = 64 * 1024 * 1024
)

var reg_nonWord = regexp.MustCompile("[^a-zA-Z0-9]+")

func Process(word string) string {
	return reg_nonWord.ReplaceAllString(word, "")
}

func (wc *WorkerContext) Close() {
	for i, w := range wc.writers {
		check(w.Flush())
		check(wc.files[i].Close())
	}
}

func Map(chunk Chunk, wc *WorkerContext) {
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
}
