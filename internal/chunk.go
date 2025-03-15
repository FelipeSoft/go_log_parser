package internal

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sync"
)

type Chunk struct {
	StartBits int64
	FinalBits int64
}

type ChunkProcessor struct {
	workerID    int
	offsetStart int64
	offsetEnd   int64
	wg          *sync.WaitGroup
	filepath    string
	ch          chan string
}

func NewChunkProcessor(workerId int, filepath string, offsetStart, offsetEnd int64, wg *sync.WaitGroup, ch chan string) *ChunkProcessor {
	return &ChunkProcessor{
		workerID:    workerId,
		offsetStart: offsetStart,
		offsetEnd:   offsetEnd,
		wg:          wg,
		filepath:    filepath,
		ch:          ch,
	}
}

func (c *ChunkProcessor) ProcessChunk() (*int, error) {
	defer c.wg.Done()

	file, err := os.Open(c.filepath)
	if err != nil {
		return nil, fmt.Errorf("could not read the entire file: %v", err)
	}
	defer file.Close()

	_, err = file.Seek(c.offsetStart, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("error on worker chunk reading: %v", err)
	}

	reader := bufio.NewReader(file)

	if c.offsetStart != 0 {
		_, err := reader.ReadBytes('\n')
		if err != nil && err != io.EOF {
			return nil, err
		}
	}

	scanner := bufio.NewScanner(reader)
	var bytesRead int64 = c.offsetStart

	for scanner.Scan() {
		line := scanner.Text()
		bytesRead += int64(len(line)) + 1

		if bytesRead > c.offsetEnd {
			break
		}

		c.ch <- line
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading chunk: %v", err)
	}

	n := int(bytesRead - c.offsetStart)

	return &n, nil
}
