package main

import (
	"context"
	"os"
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

func (wc *WorkerContext) Close(ctx context.Context) error {
	for i, w := range wc.writers {
		check(w.Flush())
		check(wc.files[i].Close())
	}

	for i, path := range wc.PartitionFiles {
		f, err := os.Open(path)
		check(err)
		stat, err := f.Stat()
		if err != nil {
			f.Close()
			return err
		}
		if stat.Size() == 0 {
			f.Close()
			os.Remove(path)
			continue
		}
		if err := wc.store.Put(ctx, wc.s3Keys[i], f, stat.Size()); err != nil {
			f.Close()
			return err
		}
		f.Close()
		os.Remove(path)
	}
	return nil
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
