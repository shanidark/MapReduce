package main

import (
	"bufio"
	"sort"
	"sync"

	// "fmt"
	"os"
	"path/filepath"
	"runtime"

	"strings"
)

var NUM_WORKERS int64 = int64(runtime.NumCPU())
var JOBS_Q_SIZE int64 = NUM_WORKERS * 2
var RESULTS_Q_SIZE int64 = NUM_WORKERS

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func ensure(e error) {
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

func GroupShuffle(mapResults []MapResult, numWorkers int64, dir string) {
	index := make(map[string][]string)

	for _, result := range mapResults {
		for _, path := range result.PartitionFiles {
			file, e := os.Open(path)
			check(e)
			scanner := bufio.NewScanner(file)

			for scanner.Scan() {
				line := string(scanner.Bytes())
				if line == "" {
					continue
				}
				word, doc, ok := strings.Cut(line, " -> ")
				if !ok || word == "" || doc == "" {
					continue
				}
				index[word] = append(index[word], doc)
			}
			file.Close()
			check(scanner.Err())
		}
	}

	f, e := os.Create(filepath.Join(dir, "index"))
	check(e)
	defer f.Close()

	keys := make([]string, 0, len(index))
	for key := range index {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	w := bufio.NewWriter(f)
	for _, key := range keys {
		kv := Reduce(key, index[key])
		w.WriteString(kv.key + "->" + kv.value + "\n")
	}
	check(w.Flush())
}

func main() {
	args := os.Args[1:]
	dir, e := os.Getwd()
	check(e)

	cleanup(dir)

	jobs := make(chan Chunk, int(JOBS_Q_SIZE))
	go func() {
		for fileID, path := range args {
			err := ProduceChunks(path, fileID, jobs)
			check(err)
		}
		close(jobs)
	}()

	results := make(chan MapResult, int(RESULTS_Q_SIZE))
	var wg sync.WaitGroup
	for workerID := range int(NUM_WORKERS) {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for chunk := range jobs {
				results <- Map(chunk, workerID, uint64(NUM_WORKERS), dir)
			}
		}(workerID)
	}
	go func() {
		wg.Wait()
		close(results)
	}()

	var mapResults []MapResult
	for r := range results {
		mapResults = append(mapResults, r)
	}

	GroupShuffle(mapResults, NUM_WORKERS, dir)
}
