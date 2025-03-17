package application

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/etl_app_transform_service/internal/domain/entity"
)

type Chunk struct {
	StartBits int64
	FinalBits int64
}

type ChunkProcessor struct {
	offsetStart int64
	offsetEnd   int64
	wg          *sync.WaitGroup
	filepath    string
	producer    entity.MessageProducer // using interface
}

func NewChunkProcessor(filepath string, offsetStart, offsetEnd int64, wg *sync.WaitGroup, producer entity.MessageProducer) *ChunkProcessor {
	return &ChunkProcessor{
		offsetStart: offsetStart,
		offsetEnd:   offsetEnd,
		wg:          wg,
		filepath:    filepath,
		producer:    producer,
	}
}

func DefineChunkWorkers(workers int64, filesize int64) []Chunk {
	var currentStart int64 = 0
	var chunks []Chunk
	bytesForWorkers := filesize / int64(workers)

	for idx := range workers {
		finalBits := currentStart + bytesForWorkers
		if idx == workers-1 {
			finalBits = filesize
		}

		chunks = append(chunks, Chunk{
			StartBits: currentStart,
			FinalBits: finalBits,
		})

		currentStart = finalBits + 1
	}

	return chunks
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
		if err := c.producer.Send(line); err != nil {
			return nil, err
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading chunk: %v", err)
	}

	n := int(bytesRead - c.offsetStart)

	return &n, nil
}
