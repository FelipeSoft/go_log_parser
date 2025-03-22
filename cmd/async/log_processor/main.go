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
	_ "github.com/etl_app_transform_service/internal/infrastructure/metrics"
	"github.com/etl_app_transform_service/internal/infrastructure/repository/mongodb"
	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"
)

func main() {
	var logWg sync.WaitGroup

	err := godotenv.Load("./../../../.env")
	if err != nil {
		log.Fatal("Could not load the environment variables file")
	}

	metricsHost := os.Getenv("METRICS_HOST_LOG_PROCESSOR")
	batchLineSize := mustAtoi(os.Getenv("BATCH_SIZE"))
	numCPU := runtime.NumCPU()
	useKafka := os.Getenv("USE_KAFKA") == "true"

	initMetricsServer(metricsHost)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	setupShutdownHandler(cancel)

	processedLogsProducer, processedLogsConsumer, rawLogsConsumer := initMessaging(useKafka, batchLineSize)
	transformRepository := initMongoRepository()
	logFormatFactory := createParserFactory()

	workerPoolSize := numCPU * 2
	g, ctx := errgroup.WithContext(ctx)
	messageChan := make(chan string, workerPoolSize*batchLineSize)

	g.Go(func() error {
		defer close(messageChan)
		for {
			select {
			case msg, ok := <-rawLogsConsumer.Messages():
				if !ok {
					return nil
				}
				select {
				case messageChan <- msg:
				case <-ctx.Done():
					return ctx.Err()
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	for i := range workerPoolSize {
		logWg.Add(1)
		processor := application.NewLogProcessor(
			batchLineSize,
			500*time.Millisecond,
			memory.NewInMemoryConsumer(messageChan),
			processedLogsProducer,
			logFormatFactory,
			transformRepository,
		)
	
		go func(wk int) {
			defer logWg.Done()
			if err := processor.ProcessLogs(ctx); err != nil {
				log.Printf("Worker %d error: %v", wk, err)
			}
			processor.Close()
		}(i)
	}

	if err := g.Wait(); err != nil {
		log.Printf("Shutdown with error: %v", err)
	}

	cleanupResources(useKafka, processedLogsProducer, processedLogsConsumer)
}

func mustAtoi(s string) int {
	i, err := strconv.Atoi(s)
	if err != nil {
		log.Fatalf("Invalid number format: %s", s)
	}
	return i
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

func setupShutdownHandler(cancel context.CancelFunc) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		cancel()
		log.Println("Initiating graceful shutdown...")
	}()
}

func initMessaging(useKafka bool, bufferSize int) (
	entity.MessageProducer,
	entity.MessageConsumer,
	entity.MessageConsumer,
) {
	if useKafka {
		return kafka.NewKafkaProducer("processed_logs"),
			kafka.NewKafkaConsumer("processed_logs", "mongo_group"),
			kafka.NewKafkaConsumer("raw_logs", "log_group")
	}

	ch := make(chan string, bufferSize*10)
	return memory.NewInMemoryProducer(ch),
		memory.NewInMemoryConsumer(ch),
		memory.NewInMemoryConsumer(ch)
}

func initMongoRepository() entity.TransformRepository {
	conn := mongodb.StartConnection()
	return mongodb.NewTransformMongoDBRepository(
		conn,
		os.Getenv("MONGODB_DATABASE"),
		"logs",
	)
}

func createParserFactory() *application.LogParserFactory {
	return application.NewLogParserFactory([]application.ParserPattern{
		createParser(`^(\S+) - - \[(.*?)\] "(GET|POST|PUT|DELETE|PATCH|OPTIONS|HEAD) (.+?) HTTP/\d\.\d" (\d{3}) (\d+) "(.*?)" "(.*?)"$`, parser.NewHttpLogParser),
		createParser(`^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \[ERROR\] \[([\w-]+)\] Deadlock detected on table "(.+)"$`, parser.NewDeadlockParser),
		createParser(`^\s*\{[\s\S]*\}\s*$`, parser.NewJsonLogParser),
		createParser(`^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \[(\w+)\] \[([\w-]+)\] (.+)$`, parser.NewBracketsStructuredParser),
		createParser(`^(\w+) (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) ([\w-]+) (.+)$`, parser.NewLevelFirstParser),
		createParser(`^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) (\w+) ([\w-]+) (.+)$`, parser.NewSimpleAlertParser),
		createParser(`^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \[(\w+)\] \[([\w-]+)\] (.+)$`, parser.NewDefaultStructuredParser),
	})
}

func createParser(pattern string, factory func(*regexp.Regexp) entity.LogParser) application.ParserPattern {
    return application.ParserPattern{
        Regex:  regexp.MustCompile(pattern),
        Parser: factory(regexp.MustCompile(pattern)),
    }
}

func cleanupResources(useKafka bool, producer entity.MessageProducer, consumer entity.MessageConsumer) {
	if useKafka {
		producer.Close()
		consumer.Close()
	}
}
