package main

import (
	"bytes"
	"io"
	"os"
)

// 64MB чтобы не нагружать память сильно и она была предсказуемой
const ChunkSize int = 64 * 1024 * 1024

func makeChunk(fileID int, chunkID int, offset int64, chunkData []byte) Chunk {
	dataCopy := make([]byte, len(chunkData))
	copy(dataCopy, chunkData)

	return Chunk{
		FileID:      fileID,
		ChunkID:     chunkID,
		StartOffset: offset,
		Data:        dataCopy,
	}
}

func sendChunk(fileID int, chunkID int, offset int64, chunkData []byte,
	jobs chan<- Chunk) int64 {
	if len(chunkData) == 0 {
		return 0
	}
	chunk := makeChunk(fileID, chunkID, offset, chunkData)
	jobs <- chunk
	return int64(len(chunk.Data))
}

func ProduceChunks(path string, FileID int, jobs chan<- Chunk) error {
	file, e := os.Open(path)
	if e != nil {
		return e
	}
	defer file.Close()

	var (
		chunkID     int
		startOffset int64
		chunkData   = make([]byte, 0, ChunkSize)
		buf         = make([]byte, ChunkSize)
	)

	for {
		n, err := file.Read(buf)
		if n > 0 {
			block := buf[:n]
			for len(block) > 0 {
				space := ChunkSize - len(chunkData)
				// ищем перенос строки в том, что влезает
				fit := block
				if len(fit) > space {
					fit = fit[:space]
				}
				if nl := bytes.LastIndexByte(fit, '\n'); nl >= 0 {
					chunkData = append(chunkData, fit[:nl+1]...)
					block = block[nl+1:]
					sent := sendChunk(FileID, chunkID, startOffset, chunkData, jobs)
					startOffset += sent
					chunkID++
					chunkData = chunkData[:0]
				} else if len(block) <= space {
					// строка не закончилась в этом блоке
					chunkData = append(chunkData, block...)
					block = block[:0]
				} else {
					// строка длиннее ChunkSize
					chunkData = append(chunkData, fit...)
					block = block[len(fit):]
					sent := sendChunk(FileID, chunkID, startOffset, chunkData, jobs)
					startOffset += sent
					chunkID++
					chunkData = chunkData[:0]
				}
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	sendChunk(FileID, chunkID, startOffset, chunkData, jobs)
	return nil
}
