package main

import (
	"bufio"
	"sort"
	// "fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"runtime"
)

var NUM_WORKERS int64 = int64(runtime.NumCPU())
var JOBS_Q_SIZE int64 = NUM_WORKERS * 2
var RESULTS_Q_SIZE int64 = NUM_WORKERS

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func cleanup(numWorkers uint64) {
	dir, e := os.Getwd()
	check(e)

	indexPath := filepath.Join(dir, "index")
	e = os.Remove(indexPath)
	if e != nil && !os.IsNotExist(e) {
		panic(e)
	}
	for i := range(numWorkers) {
  		path := filepath.Join(dir, "reduce-worker-"+strconv.FormatUint(i, 10))
  		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
  			check(err)
  		}
  	}
}


func GroupShuffle(numWorkers int64) {
	dir, e := os.Getwd()
	check(e)
	index := make(map[string][]string)

	for i := range(numWorkers) {
		path := filepath.Join(dir, "reduce-worker-" + strconv.FormatInt(i, 10))
		file, e := os.Open(path)
		check(e)
		scanner := bufio.NewScanner(file)
		scanner.Buffer(make([]byte, 64*1024), maxLineSize)	

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
		e = scanner.Err()
		check(e)
	}

	path, e := os.Getwd()
	check(e)
	path = filepath.Join(path, "index")	
	f, e := os.Create(path)
	check(e)
	defer f.Close()
	keys := make([]string, len(index))
	i := 0
	for key := range(index) {
		keys[i] = key
		i++
	}
	sort.Strings(keys)

	// записываем в этот файл
	w := bufio.NewWriter(f)
	for _, key := range(keys) {
		// fmt.Println(key, values)
		kv := Reduce(key, index[key])
		str := kv.key + "->" + kv.value + "\n"
		w.WriteString(str)
	}
	check(w.Flush())
}


func main() {
	args := os.Args[1:]
	cleanup(uint64(NUM_WORKERS))
	jobs := make(chan Chunk, int(JOBS_Q_SIZE))
	go func() {
		for fileID, path := range args {
			err := ProduceChunks(path, fileID, jobs)
			check(err)
		}
		close(jobs)
	}()

	for chunk := range jobs {
		Map(chunk, uint64(NUM_WORKERS))
	}
	GroupShuffle(NUM_WORKERS)
	// intermediate := Map(input)
	// result := Reduce()
	// Write(result)
}
