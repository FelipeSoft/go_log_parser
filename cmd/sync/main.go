package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/etl_app_transform_service/internal/application"
	"github.com/etl_app_transform_service/internal/application/parser"
	"github.com/etl_app_transform_service/internal/domain/entity"
	"github.com/etl_app_transform_service/internal/infrastructure/kafka"
	"github.com/etl_app_transform_service/internal/infrastructure/memory"
	"github.com/etl_app_transform_service/internal/infrastructure/repository/mongodb"
	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	_ "github.com/etl_app_transform_service/internal/infrastructure/metrics"
)

func main() {
	var wg sync.WaitGroup

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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 3)
	defer cancel()

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

	metricsHost := os.Getenv("METRICS_HOST_LOG_PROCESSOR")
	initMetricsServer(metricsHost)

	wg.Add(1)
	go func() {
		defer wg.Done()
		chunkProcessor := application.NewChunkProcessor(
			filepath,
			0,
			filesize,
			rawLogsProducer,
		)

		_, err = chunkProcessor.ProcessChunk(ctx)
		if err != nil {
			log.Fatalf("Error processing chunk: %v", err)
		}
	}()

	wg.Add(2)
	go func() {
		defer wg.Done()

		reDefaultStructured := regexp.MustCompile(`^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \[(\w+)\] \[([\w-]+)\] (.+)$`)
		reJson := regexp.MustCompile(`^\s*\{[\s\S]*\}\s*$`)
		reSimpleAlert := regexp.MustCompile(`^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) (\w+) ([\w-]+) (.+)$`)
		reHttp := regexp.MustCompile(`^(\S+) - - \[(.*?)\] "(GET|POST|PUT|DELETE|PATCH|OPTIONS|HEAD) (.+?) HTTP/\d\.\d" (\d{3}) (\d+) "(.*?)" "(.*?)"$`)
		reBracketsStructured := regexp.MustCompile(`^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \[(\w+)\] \[([\w-]+)\] (.+)$`)
		reLevelFirst := regexp.MustCompile(`^(\w+) (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) ([\w-]+) (.+)$`)
		reDeadlock := regexp.MustCompile(`^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \[ERROR\] \[([\w-]+)\] Deadlock detected on table "(.+)"$`)

		patterns := []application.ParserPattern{
			{Regex: reDeadlock, Parser: parser.NewDeadlockParser(reDeadlock)},
			{Regex: reHttp, Parser: parser.NewHttpLogParser(reHttp)},
			{Regex: reJson, Parser: parser.NewJsonLogParser(reJson)},
			{Regex: reBracketsStructured, Parser: parser.NewBracketsStructuredParser(reBracketsStructured)},
			{Regex: reLevelFirst, Parser: parser.NewLevelFirstParser(reLevelFirst)},
			{Regex: reSimpleAlert, Parser: parser.NewSimpleAlertParser(reSimpleAlert)},
			{Regex: reDefaultStructured, Parser: parser.NewDefaultStructuredParser(reDefaultStructured)},
		}

		logFormatFactory := application.NewLogParserFactory(patterns)

		logProcessor := application.NewLogProcessor(
			batchLineSize,
			1*time.Second,
			rawLogsConsumer,
			processedLogsProducer,
			logFormatFactory,
			transformRepository,
		)

		if err = logProcessor.ProcessLogs(context.Background()); err != nil {
			log.Fatalf("Error processing logs: %v", err)
		}
	}()

	wg.Wait()
	rawLogsProducer.Close()
	rawLogsConsumer.Close()
	processedLogsProducer.Close()
	processedLogsConsumer.Close()
}

func initMetricsServer(host string) {
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		if err := http.ListenAndServe(host, nil); err != nil {
			log.Fatalf("Metrics server failed: %v", err)
		}
	}()
	log.Printf("Metrics server listening on %s", host)
}

