package main

import (
	"bufio"
	// "fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}


func Read(files []string) []KeyValue{
	var result []KeyValue
	for _, file := range(files) {
		dir, e := os.Getwd()
		check(e)
		
		filepath := filepath.Join(dir, file)
		content, e := os.ReadFile(filepath)
		check(e)

		var kv KeyValue
		kv.key = file
		kv.value = string(content)
		result = append(result, kv)
	}
	return result
}


func GroupShuffle() {
	dir, e := os.Getwd()
	check(e)
	index := make(map[string][]string)
	for i := range(int64(3)) {
		path := filepath.Join(dir, "reduce-worker-" + strconv.FormatInt(i, 10))
		file, e := os.ReadFile(path)
		check(e)
		sfile := string(file)
		lines := strings.Split(sfile, "\n")
		for _, line := range(lines[:len(lines)-1]) {
			kvsplit := strings.Split(line, " -> ")
			// fmt.Println(kvsplit)
			index[kvsplit[0]] = append(index[kvsplit[0]], kvsplit[1])
		}
	}

	path, e := os.Getwd()
	check(e)
	path = filepath.Join(path, "index")
	
	f, e := os.Create(path)
	check(e)
	w := bufio.NewWriter(f)

	for key, values := range(index) {
		// fmt.Println(key, values)
		kv := Reduce(key, values)
		str := kv.key + "->" + kv.value + "\n"
		w.WriteString(str)
	}
	w.Flush()
}


func main() {
	args := os.Args[1:]
	input := Read(args)
	for _, file := range(input) {
		Map(file)
	}
	GroupShuffle()
	// intermediate := Map(input)
	// result := Reduce()
	// Write(result)
}
