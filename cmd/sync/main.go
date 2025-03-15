package main

import (
	"log"
	"os"
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

	var wg sync.WaitGroup
	ch := make(chan string)

	wg.Add(1)
	go func() {
		chunkProcessor := internal.NewChunkProcessor(1, filepath, 0, fileSize, &wg, ch)
		chunkProcessor.ProcessChunk()
	}()

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
