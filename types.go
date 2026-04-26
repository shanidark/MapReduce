package main

// reader читает файл чанками по 64 мегабайта, и в виде Chunk их и хранит
type Chunk struct {
	FileID 		int
	ChunkID 	int
	StartOffset int64
	Data 		[]byte
}

type SpillFile struct {
    Partition int
    Path      string
}

type MapResult struct {
	FileID         int // из какого файла мапили
	ChunkID        int // какой чанк
	PartitionFiles map[int]string // в какие файлы мы записали наши результаты
}

type KeyValue struct {
	key   string
	value string
}
