package main

import (
	"os"
	"bufio"
)

// 64MB чтобы не нагружать память сильно и она была предсказуемой
const ChunkSize int = 64*1024*1024
const maxLineSize = 1024*1024

func makeChunk(fileID int, chunkID int, offset int64, chunkData []byte) Chunk {
	dataCopy := make([]byte, len(chunkData))
	copy(dataCopy, chunkData)

	return Chunk{
		FileID: 	 fileID,
		ChunkID: 	 chunkID,
		StartOffset: offset,
		Data: 		 dataCopy,
	}
}

func sendChunk(fileID int, chunkID int, offset int64, chunkData []byte,
			   jobs chan<- Chunk) int64 {
	if len(chunkData) == 0 {
		return 0
	}
	chunk := makeChunk(fileID, chunkID, offset, chunkData)
	jobs <- chunk
	copied := chunk.Data
	result := int64(len(copied))
	return result
}

func ProduceChunks(path string, FileID int, jobs chan<- Chunk) error {
	file, e := os.Open(path)
	if e != nil {
		return e
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), maxLineSize)
	
	var (
        chunkID  int
        startOffset   int64
		currentSize int64
        chunkData = make([]byte, 0, ChunkSize)
    )
	
	for scanner.Scan() {
		line := scanner.Bytes()
		lineSize := len(line) + 1

		if len(chunkData) > 0 && 
	       currentSize+int64(lineSize) > int64(ChunkSize) {
			sent := sendChunk(FileID, chunkID, startOffset, chunkData, jobs)
			startOffset += sent
			chunkID++
			chunkData = chunkData[:0]
			currentSize = 0
		}

		chunkData = append(chunkData, line...)
		chunkData = append(chunkData, '\n')
		currentSize += int64(lineSize)
	}
	sendChunk(FileID, chunkID, startOffset, chunkData, jobs)
	e = scanner.Err();
	if e != nil {
		return e
	}
	return nil
}
