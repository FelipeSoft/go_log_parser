package main

import (
	"log"
	"os"
	"regexp"
	"strconv"
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
	start := time.Now()

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
	transformRepository := mongodb.NewTransformMongoDBRepository(mgoConn)

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
	
	chunkProcessor := application.NewChunkProcessor(
		filepath,
		0,
		filesize,
		nil, 
		rawLogsProducer,
	)

	_, err = chunkProcessor.ProcessChunk()
	if err != nil {
		log.Fatalf("Error processing chunk: %v", err)
	}

	rawLogsProducer.Close()

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

	logProcessor := application.NewLogProcessor(
		batchLineSize,
		rawLogsConsumer,
		processedLogsProducer,
		logFormatFactory,
		transformRepository,
	)

	err = logProcessor.ProcessLogs()
	if err != nil {
		log.Fatalf("Error processing logs: %v", err)
	}

	rawLogsConsumer.Close()

	for msg := range processedLogsConsumer.Messages() {
		log.Printf("Inserting into MongoDB: %s", msg)
	}

	processedLogsProducer.Close()
	processedLogsConsumer.Close()

	end := time.Now()
	finishedAt := end.Sub(start)
	log.Printf("Program finished in %f seconds", finishedAt.Seconds())
}