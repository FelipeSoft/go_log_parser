package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/etl_app_transform_service/internal/application"
	"github.com/etl_app_transform_service/internal/domain/entity"
	"github.com/etl_app_transform_service/internal/infrastructure/kafka"
	"github.com/etl_app_transform_service/internal/infrastructure/memory"
	_ "github.com/etl_app_transform_service/internal/infrastructure/metrics"
	"github.com/joho/godotenv"
)

func main() {
	var chunkWg sync.WaitGroup
	var rawLogsProducer entity.MessageProducer

	err := godotenv.Load("./../../../.env")
	if err != nil {
		log.Fatal("Could not load the environment variables file")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		cancel()
		log.Println("Shutting down gracefully...")
	}()

	useKafka := os.Getenv("USE_KAFKA") == "true"
	batchLineSize, err := strconv.Atoi(os.Getenv("BATCH_SIZE"))
	if err != nil {
		log.Fatalf("Could not load the BATCH_SIZE environment variable: %s", err.Error())
	}

	if useKafka {
		rawLogsProducer = kafka.NewKafkaProducer("raw_logs")
	} else {
		rawCh := make(chan string, batchLineSize*2)
		rawLogsProducer = memory.NewInMemoryProducer(rawCh)
	}

	numCPU := runtime.NumCPU()
	chunkWorkers := numCPU

	filepath := os.Getenv("LOG_SERVER_LOCAL_PATH")
	file, err := os.Open(filepath)
	if err != nil {
		log.Fatalf("Error during reading log file: %v", err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		log.Fatalf("Error during get log file stat: %v", err)
	}

	filesize := fileInfo.Size()
	workersOffset := application.DefineChunkWorkers(int64(chunkWorkers), filesize, os.Getenv("LOG_SERVER_LOCAL_PATH"))

	for idx, offset := range workersOffset {
		chunkWg.Add(1)
		go func(worker int, startBits int64, finalBits int64) {
			defer chunkWg.Done()
			chunkProcessor := application.NewChunkProcessor(
				filepath,
				startBits,
				finalBits,
				rawLogsProducer,
			)
			_, err := chunkProcessor.ProcessChunk(ctx)
			if err != nil {
				log.Printf("error during processing chunk: %v", err)
			}
		}(idx, offset.StartBits, offset.FinalBits)
	}

	chunkWg.Wait()

	go func() {
		rawLogsProducer.Close()

		if useKafka {
			rawLogsProducer.Close()
		}
	}()
}
