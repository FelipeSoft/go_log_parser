package application

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
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
	producer    entity.MessageProducer
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
    var chunks []Chunk
    bytesPerWorker := filesize / workers

    file, _ := os.Open(os.Getenv("LOG_SERVER_LOCAL_PATH"))
    defer file.Close()

    var currentStart int64 = 0
    for i := int64(0); i < workers; i++ {
        currentEnd := currentStart + bytesPerWorker
        if currentEnd > filesize {
            currentEnd = filesize
        }

        file.Seek(currentEnd, 0)
        reader := bufio.NewReader(file)
        reader.ReadBytes('\n')
        adjustedEnd, _ := file.Seek(0, io.SeekCurrent)

        chunks = append(chunks, Chunk{
            StartBits: currentStart,
            FinalBits: adjustedEnd,
        })
        currentStart = adjustedEnd + 1
    }
    return chunks
}

func (c *ChunkProcessor) ProcessChunk() (*int, error) {
	defer c.wg.Done()

	file, err := os.Open(c.filepath)
	if err != nil {
		return nil, fmt.Errorf("could not open file: %v", err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("error getting file info: %v", err)
	}

	_, err = file.Seek(c.offsetStart, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("seek error: %v", err)
	}

	bufferedReader := bufio.NewReader(file)

	if c.offsetStart != 0 {
		_, err := bufferedReader.ReadBytes('\n')
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("error skipping partial line: %v", err)
		}
	}

	var currentPos int64 = c.offsetStart
	if c.offsetStart != 0 {
		currentPos, _ = file.Seek(0, io.SeekCurrent)
	}

	for {
		if currentPos > c.offsetEnd && c.offsetEnd != fileInfo.Size()-1 {
			break
		}

		lineBytes, err := bufferedReader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("error reading line: %v", err)
		}

		line := strings.TrimSpace(string(lineBytes))

		if err := c.producer.Send(line); err != nil {
			return nil, fmt.Errorf("error sending line: %v", err)
		}

		currentPos += int64(len(lineBytes))
	}

	return nil, nil
}
