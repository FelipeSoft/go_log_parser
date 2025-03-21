package main

import (
	"context"
	"log"
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
)

func main() {	
	var logWg sync.WaitGroup
	var batchLimitTimeout time.Duration = 500 * time.Millisecond
	numCPU := runtime.NumCPU()
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

	err := godotenv.Load("./../../../.env")
	if err != nil {
		log.Fatal("Could not load the environment variables file")
	}

	batchLineSize, err := strconv.Atoi(os.Getenv("BATCH_SIZE"))
	if err != nil {
		log.Fatalf("Could not load the BATCH_SIZE environment variable: %s", err.Error())
	}

	useKafka := os.Getenv("USE_KAFKA") == "true"

	var processedLogsProducer entity.MessageProducer
	var processedLogsConsumer entity.MessageConsumer
	var rawLogsConsumer entity.MessageConsumer

	if useKafka {
		processedLogsProducer = kafka.NewKafkaProducer("processed_logs")
		processedLogsConsumer = kafka.NewKafkaConsumer("processed_logs", "mongo_consumer_group")
		rawLogsConsumer = kafka.NewKafkaConsumer("raw_logs", "log_processor_group")
	} else {
		rawCh := make(chan string, batchLineSize*2)
		processedCh := make(chan string, batchLineSize*2)
		processedLogsProducer = memory.NewInMemoryProducer(processedCh)
		processedLogsConsumer = memory.NewInMemoryConsumer(processedCh)
		rawLogsConsumer = memory.NewInMemoryConsumer(rawCh)
	}

	mgoConn := mongodb.StartConnection()
	transformRepository := mongodb.NewTransformMongoDBRepository(mgoConn, os.Getenv("MONGODB_DATABASE"), "logs")

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

    logWg.Wait()

	if useKafka {
		processedLogsProducer.Close()
		processedLogsConsumer.Close()
	}

	mgoConn.Disconnect(ctx)
}
