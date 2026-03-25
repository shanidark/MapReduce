package main

import (
	"strings"
	"os"
	"path/filepath"
	"bufio"
	"strconv"
	"regexp"
)

func Process(word string) string {
	re, e := regexp.Compile("[^a-zA-Z0-9]+")
	check(e)
	return re.ReplaceAllString(word, "")
}


func Map(input KeyValue) {
	docname := input.key
	content := input.value
	var result []KeyValue

	text := strings.Split(content, " ")
	for _, entry := range(text) {
		word := Process(entry)
		var kv KeyValue
		kv.key = word
		kv.value = docname
		result = append(result, kv)
	}

	Write(result, 3)
}

func Write(inp []KeyValue, n uint64) {
	dir, e := os.Getwd()
	check(e)

	var writers []*bufio.Writer
	for i := range(n) {
		path := filepath.Join(dir, "reduce-worker-" + strconv.FormatUint(i, 10))
		f, e := os.Create(path)
		writer := bufio.NewWriter(f)
		writers = append(writers, writer)
		check(e)
	}


	for _, entry := range(inp) {
		hash := Hash(entry.key) % n
		writer := writers[hash]
		_, e := writer.WriteString(entry.key + " -> " + entry.value + "\n")
		check(e)
	}

	for _, writer := range(writers) {
		writer.Flush()
	}
}
