package main

import (
	"log"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/etl_app_transform_service/internal"
	"github.com/joho/godotenv"
)

func main() {
	start := time.Now()

	err := godotenv.Load("./../../.env")
	if err != nil {
		log.Fatal("could not load the environment variables file")
	}

	var numWorkers int = runtime.NumCPU()
	var workersOffset []internal.Chunk
	var currentStart int64 = 0
	var wg sync.WaitGroup
	ch := make(chan string)

	filepath := os.Getenv("LOG_SERVER_LOCAL_PATH")
	file, err := os.Open(filepath)
	if err != nil {
		log.Fatalf("error during reading log file: %s", err.Error())
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		log.Fatalf("error during get log file stat: %s", err.Error())
	}

	fileSize := fileInfo.Size()
	bytesForWorkers := fileSize / int64(numWorkers)

	for idx := range numWorkers {
		finalBits := currentStart + bytesForWorkers
		if idx == numWorkers-1 {
			finalBits = fileSize
		}

		workersOffset = append(workersOffset, internal.Chunk{
			StartBits: currentStart,
			FinalBits: finalBits,
		})

		currentStart = finalBits + 1
	}

	for idx, offset := range workersOffset {
		wg.Add(1)
		go func(worker int) {
			chunkProcessor := internal.NewChunkProcessor(idx, filepath, offset.StartBits, offset.FinalBits, &wg, ch)
			n, err := chunkProcessor.ProcessChunk()
			if err != nil {
				log.Print(err)
			}
			log.Print(n)
		}(idx)
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	for chunk := range ch {
		log.Print(chunk)
	}

	end := time.Now()
	finishedAt := end.Sub(start)
	log.Printf("Program finished in %f seconds", finishedAt.Seconds())
}
