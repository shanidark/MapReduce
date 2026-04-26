package main

import (
	"strings"
	"os"
	"path/filepath"
	"bufio"
	"strconv"
	"regexp"
)

var reg_nonWord = regexp.MustCompile("[^a-zA-Z0-9]+")

func Process(word string) string {
	return reg_nonWord.ReplaceAllString(word, "")
}


func Map(chunk Chunk, numPartitions uint64) {
	docname := strconv.Itoa(chunk.FileID)
	content := string(chunk.Data)
	var result []KeyValue
	
	text := strings.Fields(content)
	for _, entry := range(text) {
		word := Process(entry)
		if word == "" {
			continue
		}

		var kv KeyValue
		kv.key = word
		kv.value = docname
		result = append(result, kv)
	}

	Write(result, numPartitions)
}

func Write(inp []KeyValue, n uint64) {
	if n == 0 || len(inp) == 0 {
		return
	}
	dir, e := os.Getwd()
	check(e)

	var writers []*bufio.Writer
	var files []*os.File
	for i := range(n) {
		path := filepath.Join(dir, "reduce-worker-" + strconv.FormatUint(i, 10))
		f, e := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)	
		check(e)
		files = append(files, f)
		writer := bufio.NewWriter(f)
		writers = append(writers, writer)
	}
	

	for _, entry := range(inp) {
		hash := Hash(entry.key) % n
		writer := writers[hash]
		_, e := writer.WriteString(entry.key + " -> " + entry.value + "\n")
		check(e)
	}

	for _, writer := range(writers) {
		check(writer.Flush())
	}

	for _, file := range(files) {
		check(file.Close())
	}
}
