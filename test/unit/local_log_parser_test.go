package unit_test

import (
	"log"
	"os"
	"regexp"
	"strconv"
	"sync"
	"testing"

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
	workersOffset := application.DefineChunkWorkers(int64(chunkWorkers), filesize)

	for idx, offset := range workersOffset {
		wg.Add(1)
		go func(worker int, startBits int64, finalBits int64) {
			defer wg.Done()
			chunkProcessor := application.NewChunkProcessor(
				filepath,
				startBits,
				finalBits,
				&wg,
				rawLogsProducer, // Inject producer
			)
			_, err := chunkProcessor.ProcessChunk()
			if err != nil {
				log.Printf("error during processing chunk: %v", err)
			}
		}(idx, offset.StartBits, offset.FinalBits)
	}

	for i := range logProcessorWorkers {
		wg.Add(1)
		go func(lpw int) {
			defer wg.Done()

			reDefaultStructured := regexp.MustCompile(`^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \[(\w+)\] \[([\w-]+)\] (.+)$`)
			reJson := regexp.MustCompile(`^\{.*\}$`)
			reSimpleAlert := regexp.MustCompile(`^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) (\w+) ([\w-]+) (.+)$`)
			reHttp := regexp.MustCompile(`^(\S+) - - \[(.+?)\] "(\w+) (.+?) HTTP\/\d\.\d" (\d+) (\d+|-) ".*?" "(.*?)"$`)
			reBracketsStructured := regexp.MustCompile(`^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \[(\w+)\] \[([\w-]+)\] (.+)$`)
			reLevelFirst := regexp.MustCompile(`^(\w+) (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) ([\w-]+) (.+)$`)

			logFormatFactory := application.NewLogParserFactory(map[*regexp.Regexp]entity.LogParser{
				reDefaultStructured:  parser.NewDefaultStructuredParser(reDefaultStructured),
				reJson:               parser.NewJsonLogParser(reJson),
				reSimpleAlert:        parser.NewSimpleAlertParser(reSimpleAlert),
				reHttp:               parser.NewHttpLogParser(reHttp),
				reBracketsStructured: parser.NewBracketsStructuredParser(reBracketsStructured),
				reLevelFirst:         parser.NewLevelFirstParser(reLevelFirst),
			})

			logProcessorWorker := application.NewLogProcessor(
				batchLineSize,
				rawLogsConsumer,
				processedLogsProducer,
				logFormatFactory,
				transformRepository,
			)
			logProcessorWorker.ProcessLogs()
		}(i)
	}

	go func() {
		for msg := range processedLogsConsumer.Messages() {
			log.Printf("Inserting into MongoDB: %s", msg)
		}
	}()

	wg.Wait()

	rawLogsProducer.Close()
	rawLogsConsumer.Close()
	processedLogsProducer.Close()
	processedLogsConsumer.Close()
}
