package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/etl_app_transform_service/internal/application"
	"github.com/etl_app_transform_service/internal/application/parser"
	"github.com/etl_app_transform_service/internal/domain/entity"
	"github.com/etl_app_transform_service/internal/infrastructure/kafka"
	"github.com/etl_app_transform_service/internal/infrastructure/memory"
	"github.com/etl_app_transform_service/internal/infrastructure/repository/mongodb"
	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	var httpWg sync.WaitGroup
	server := http.NewServeMux()
	server.Handle("/metrics", promhttp.Handler())

	httpWg.Add(1)
	go func() {
		defer httpWg.Done()
		err := http.ListenAndServe("192.168.200.154:8080", server)
		if err != nil {
			log.Fatalf("Error on HTTP server starting: %v", err)
		}
	}()
	log.Print("HTTP Server for metrics listening on 192.168.200.154:8080")
	
	var chunkWg, logWg sync.WaitGroup
	var batchLimitTimeout time.Duration = 500 * time.Millisecond
	numCPU := runtime.NumCPU()
	chunkWorkers := numCPU
	logProcessorWorkers := numCPU * 2

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		cancel()
		log.Println("Shutting down gracefully...")
	}()

	err := godotenv.Load("./../../.env")
	if err != nil {
		log.Fatal("Could not load the environment variables file")
	}

	batchLineSize, err := strconv.Atoi(os.Getenv("BATCH_SIZE"))
	if err != nil {
		log.Fatalf("Could not load the BATCH_SIZE environment variable: %s", err.Error())
	}

	var rawLogsProducer entity.MessageProducer
	var rawLogsConsumer entity.MessageConsumer

	useKafka := os.Getenv("USE_KAFKA") == "true"

	if useKafka {
		rawLogsProducer = kafka.NewKafkaProducer("raw_logs")
		rawLogsConsumer = kafka.NewKafkaConsumer("raw_logs", "log_processor_group")
	} else {
		rawCh := make(chan string, batchLineSize*2)
		rawLogsProducer = memory.NewInMemoryProducer(rawCh)
		rawLogsConsumer = memory.NewInMemoryConsumer(rawCh)
	}

	var processedLogsProducer entity.MessageProducer
	var processedLogsConsumer entity.MessageConsumer

	if useKafka {
		processedLogsProducer = kafka.NewKafkaProducer("processed_logs")
		processedLogsConsumer = kafka.NewKafkaConsumer("processed_logs", "mongo_consumer_group")
	} else {
		processedCh := make(chan string, batchLineSize*2)
		processedLogsProducer = memory.NewInMemoryProducer(processedCh)
		processedLogsConsumer = memory.NewInMemoryConsumer(processedCh)
	}

	mgoConn := mongodb.StartConnection()
	transformRepository := mongodb.NewTransformMongoDBRepository(mgoConn, os.Getenv("MONGODB_DATABASE"), "logs")

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
			_, err := chunkProcessor.ProcessChunk()
			if err != nil {
				log.Printf("error during processing chunk: %v", err)
			}
		}(idx, offset.StartBits, offset.FinalBits)
	}

	for i := 0; i < logProcessorWorkers; i++ {
		logWg.Add(1)
		go func(workerID int) {
			defer logWg.Done()

			reDefaultStructured := regexp.MustCompile(`^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \[(\w+)\] \[([\w-]+)\] (.+)$`)
			reJson := regexp.MustCompile(`^\s*\{[\s\S]*\}\s*$`)
			reSimpleAlert := regexp.MustCompile(`^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) (\w+) ([\w-]+) (.+)$`)
			reHttp := regexp.MustCompile(`^(\S+) - - \[(.*?)\] "(GET|POST|PUT|DELETE|PATCH|OPTIONS|HEAD) (.+?) HTTP/\d\.\d" (\d{3}) (\d+) "(.*?)" "(.*?)"$`)
			reBracketsStructured := regexp.MustCompile(`^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \[(\w+)\] \[([\w-]+)\] (.+)$`)
			reLevelFirst := regexp.MustCompile(`^(\w+) (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) ([\w-]+) (.+)$`)
			reDeadlock := regexp.MustCompile(`^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \[ERROR\] \[([\w-]+)\] Deadlock detected on table "(.+)"$`)

			patterns := []application.ParserPattern{
				{Regex: reHttp, Parser: parser.NewHttpLogParser(reHttp)},
				{Regex: reDeadlock, Parser: parser.NewDeadlockParser(reDeadlock)},
				{Regex: reJson, Parser: parser.NewJsonLogParser(reJson)},
				{Regex: reBracketsStructured, Parser: parser.NewBracketsStructuredParser(reBracketsStructured)},
				{Regex: reLevelFirst, Parser: parser.NewLevelFirstParser(reLevelFirst)},
				{Regex: reSimpleAlert, Parser: parser.NewSimpleAlertParser(reSimpleAlert)},
				{Regex: reDefaultStructured, Parser: parser.NewDefaultStructuredParser(reDefaultStructured)},
			}

			logFormatFactory := application.NewLogParserFactory(patterns)

			processor := application.NewLogProcessor(
				batchLineSize,
				batchLimitTimeout,
				rawLogsConsumer,
				processedLogsProducer,
				logFormatFactory,
				transformRepository,
			)

			if err := processor.ProcessLogs(ctx); err != nil {
				log.Printf("Log processor error: %v", err)
			}
		}(i)
	}

	go func() {
		ticker := time.NewTicker(1000 * time.Millisecond)
		for {
			<-ticker.C
			received, processed := application.GetMetrics()
			log.Printf("Throughput: received=%d, processed=%d, pending=%d",
				received,
				processed,
				received-processed)
		}
	}()

	chunkWg.Wait()
    rawLogsProducer.Close()

	httpWg.Wait()
    logWg.Wait()

    received, processed := application.GetMetrics()
    if received != processed {
        log.Printf("Pending data after shutdown: received=%d, processed=%d", received, processed)
    }

	if useKafka {
		rawLogsConsumer.Close()
		processedLogsProducer.Close()
		processedLogsConsumer.Close()
	}

	mgoConn.Disconnect(ctx)
}
