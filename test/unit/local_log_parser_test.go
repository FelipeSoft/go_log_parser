package unit_test

import (
	"context"
	"log"
	"os"
	"regexp"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/etl_app_transform_service/internal/application"
	"github.com/etl_app_transform_service/internal/application/parser"
	"github.com/etl_app_transform_service/internal/domain/entity"
	"github.com/etl_app_transform_service/internal/infrastructure/memory"
	memory_repository "github.com/etl_app_transform_service/internal/infrastructure/repository/memory"
	"github.com/joho/godotenv"
)

func Test_LocalDynamicLogParser(t *testing.T) {
	var chunkWorkers int = 5
	var logProcessorWorkers int = 5
	var wg sync.WaitGroup
	var batchLimitTimeout time.Duration = 1 * time.Second

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

	rawCh := make(chan string, batchLineSize*2)
	rawLogsProducer = memory.NewInMemoryProducer(rawCh)
	rawLogsConsumer = memory.NewInMemoryConsumer(rawCh)

	var processedLogsProducer entity.MessageProducer
	var processedLogsConsumer entity.MessageConsumer

	processedCh := make(chan string, batchLineSize*2)
	processedLogsProducer = memory.NewInMemoryProducer(processedCh)
	processedLogsConsumer = memory.NewInMemoryConsumer(processedCh)

	transformRepository := memory_repository.NewTransformMockRepository()

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
	workersOffset := application.DefineChunkWorkers(int64(chunkWorkers), filesize, os.Getenv("MOCK_LOG_SERVER_LOCAL_PATH"))

	for idx, offset := range workersOffset {
		wg.Add(1)
		go func(worker int, startBits int64, finalBits int64) {
			defer wg.Done()
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

	for i := range logProcessorWorkers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

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

			if err := processor.ProcessLogs(context.Background()); err != nil {
				log.Printf("Log processor error: %v", err)
			}
		}(i)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for msg := range processedCh {
			t.Log(msg)
		}
	}()

	go func() {
		wg.Wait()
		close(processedCh)
		rawLogsProducer.Close()
		rawLogsConsumer.Close()
		processedLogsProducer.Close()
		processedLogsConsumer.Close()
	}()
}
