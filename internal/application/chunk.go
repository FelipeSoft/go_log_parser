package application

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/etl_app_transform_service/internal/domain/entity"
)

type Chunk struct {
	StartBits int64
	FinalBits int64
}

type ChunkProcessor struct {
	offsetStart int64
	offsetEnd   int64
	filepath    string
	producer    entity.MessageProducer
}

func NewChunkProcessor(filepath string, offsetStart, offsetEnd int64, producer entity.MessageProducer) *ChunkProcessor {
	return &ChunkProcessor{
		offsetStart: offsetStart,
		offsetEnd:   offsetEnd,
		filepath:    filepath,
		producer:    producer,
	}
}

func DefineChunkWorkers(workers int64, filesize int64, filepath string) []Chunk {
	file, err := os.Open(filepath)
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
	}
	defer file.Close()

	chunks := make([]Chunk, 0, workers)
	var start int64 = 0

	for i := int64(0); i < workers && start < filesize; i++ {
		remainingWorkers := workers - i
		chunkSize := (filesize - start + remainingWorkers - 1) / remainingWorkers

		end := min(start+chunkSize, filesize)

		_, err = file.Seek(end, 0)
		if err != nil {
			log.Fatalf("Seek error: %v", err)
		}

		reader := bufio.NewReader(file)
		line, err := reader.ReadBytes('\n')

		var adjustedEnd int64
		switch {
		case err == io.EOF:
			adjustedEnd = filesize
		case err != nil:
			log.Fatalf("Read error: %v", err)
		default:
			adjustedEnd = end + int64(len(line))
		}

		adjustedEnd = min(adjustedEnd, filesize)

		if start < adjustedEnd {
			chunks = append(chunks, Chunk{
				StartBits: start,
				FinalBits: adjustedEnd,
			})
			start = adjustedEnd
		} else {
			break
		}
	}

	return chunks
}

func (c *ChunkProcessor) ProcessChunk(ctx context.Context) (*int, error) {
	go func() {
		select {
		case <-ctx.Done():
			return
		}
	}()

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
		rawEntry := entity.RawEntry{
			StartAt: time.Now(),
			Raw:     line,
		}

		rawEntryBytes, err := json.Marshal(rawEntry)
		if err != nil {
			return nil, fmt.Errorf("error parse line: %v", err)
		}

		if err := c.producer.Send(string(rawEntryBytes)); err != nil {
			return nil, fmt.Errorf("error sending line: %v", err)
		}

		currentPos += int64(len(lineBytes))
	}

	return nil, nil
}
