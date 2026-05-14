package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Process

func TestProcess_stripsNonAlphanumeric(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"hello,", "hello"},
		{"world!", "world"},
		{"foo.bar", "foobar"},
		{"123", "123"},
		{"", ""},
		{"...", ""},
		{"don't", "dont"},
	}
	for _, c := range cases {
		if got := Process(c.in); got != c.want {
			t.Errorf("Process(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// Hash

func TestHash_deterministic(t *testing.T) {
	for _, word := range []string{"hello", "world", "foo", ""} {
		if Hash(word) != Hash(word) {
			t.Errorf("Hash(%q) not deterministic", word)
		}
	}
}

func TestHash_differentWords(t *testing.T) {
	if Hash("hello") == Hash("world") {
		t.Error("hash collision between 'hello' and 'world'")
	}
}

// Reduce

func TestReduce_deduplicates(t *testing.T) {
	kv := Reduce("word", []string{"2", "1", "1", "3", "2"})
	if kv.key != "word" {
		t.Errorf("key = %q, want %q", kv.key, "word")
	}
	if kv.value != "1,2,3" {
		t.Errorf("value = %q, want %q", kv.value, "1,2,3")
	}
}

func TestReduce_singleDoc(t *testing.T) {
	kv := Reduce("foo", []string{"0"})
	if kv.value != "0" {
		t.Errorf("value = %q, want %q", kv.value, "0")
	}
}

func TestReduce_alreadySorted(t *testing.T) {
	kv := Reduce("bar", []string{"0", "1", "2"})
	if kv.value != "0,1,2" {
		t.Errorf("value = %q, want %q", kv.value, "0,1,2")
	}
}

// Map + Write

func TestMap_writesToCorrectPartition(t *testing.T) {
	const n = 4
	dir := t.TempDir()

	chunk := Chunk{FileID: 0, ChunkID: 0, Data: []byte("hello world hello")}
	result := Map(chunk, 0, n, dir)

	for _, word := range []string{"hello", "world"} {
		expectedPartition := int(Hash(word) % n)
		path := result.PartitionFiles[expectedPartition]
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(data), word+" -> 0") {
			t.Errorf("word %q not found in partition %d", word, expectedPartition)
		}
	}
}

func TestMap_skipsEmptyTokens(t *testing.T) {
	const n = 2
	dir := t.TempDir()

	chunk := Chunk{FileID: 1, Data: []byte("... ,,, !!!")}
	result := Map(chunk, 0, n, dir)

	for _, path := range result.PartitionFiles {
		data, _ := os.ReadFile(path)
		if len(data) > 0 {
			t.Errorf("expected empty partition, got %q in %s", data, path)
		}
	}
}

// ProduceChunks

func TestProduceChunks_allLinesDelivered(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")

	lines := []string{"foo bar", "baz qux", "hello world"}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0644); err != nil {
		t.Fatal(err)
	}

	jobs := make(chan Chunk, 10)
	if err := ProduceChunks(path, 0, jobs); err != nil {
		t.Fatal(err)
	}
	close(jobs)

	var got strings.Builder
	for chunk := range jobs {
		got.Write(chunk.Data)
	}

	for _, line := range lines {
		if !strings.Contains(got.String(), line) {
			t.Errorf("line %q not found in chunks", line)
		}
	}
}

func TestProduceChunks_respectsChunkBoundary(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")

	// строим файл > ChunkSize чтобы получить несколько чанков
	line := strings.Repeat("a", 1024) + "\n"
	count := (ChunkSize / len(line)) + 2
	content := strings.Repeat(line, count)
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	jobs := make(chan Chunk, 100)
	if err := ProduceChunks(path, 0, jobs); err != nil {
		t.Fatal(err)
	}
	close(jobs)

	chunks := 0
	totalBytes := 0
	for chunk := range jobs {
		chunks++
		totalBytes += len(chunk.Data)
		// каждый чанк кроме последнего должен заканчиваться на \n
		if len(chunk.Data) > 0 && chunk.Data[len(chunk.Data)-1] != '\n' {
			t.Errorf("chunk %d does not end with newline", chunk.ChunkID)
		}
	}

	if chunks < 2 {
		t.Errorf("expected at least 2 chunks, got %d", chunks)
	}
	if totalBytes != len(content) {
		t.Errorf("total bytes %d != content size %d", totalBytes, len(content))
	}
}
