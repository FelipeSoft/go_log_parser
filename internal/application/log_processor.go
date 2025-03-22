package application

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/etl_app_transform_service/internal/domain/entity"
	"github.com/etl_app_transform_service/internal/infrastructure/metrics"
	"slices"
)

type LogProcessor struct {
	batchLinesSize      int
	batchTimeout        time.Duration
	consumer            entity.MessageConsumer
	producer            entity.MessageProducer
	logParserFactory    *LogParserFactory
	transformRepository entity.TransformRepository
	batchChan           chan entity.LogEntry
	wg                  sync.WaitGroup
}

func NewLogProcessor(
	batchLinesSize int,
	batchTimeout time.Duration,
	consumer entity.MessageConsumer,
	producer entity.MessageProducer,
	logParserFactory *LogParserFactory,
	transformRepository entity.TransformRepository,
) *LogProcessor {
	lp := &LogProcessor{
		batchLinesSize:      batchLinesSize,
		batchTimeout:        batchTimeout,
		consumer:            consumer,
		producer:            producer,
		logParserFactory:    logParserFactory,
		transformRepository: transformRepository,
		batchChan:           make(chan entity.LogEntry, batchLinesSize*2),
	}

	lp.startBatchWorkers(runtime.NumCPU())
	return lp
}

func (lp *LogProcessor) startBatchWorkers(workerCount int) {
	lp.wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func() {
			defer lp.wg.Done()
			lp.batchWorker()
		}()
	}
}

func (lp *LogProcessor) batchWorker() {
	var batch []entity.LogEntry
	ticker := time.NewTicker(lp.batchTimeout)
	defer ticker.Stop()

	for {
		select {
		case entry, ok := <-lp.batchChan:
			if !ok {
				lp.safeFlush(batch)
				return
			}
			batch = append(batch, entry)
			if len(batch) >= lp.batchLinesSize {
				lp.safeFlush(batch)
				batch = nil
			}

		case <-ticker.C:
			lp.safeFlush(batch)
			batch = nil
		}
	}
}

func (lp *LogProcessor) ProcessLogs(ctx context.Context) error {
	defer close(lp.batchChan)

	for {
		select {
		case msg, ok := <-lp.consumer.Messages():
			if !ok {
				return nil
			}

			entry, err := lp.processSingleMessage(msg)
			if err != nil {
				log.Printf("Processing error: %v", err)
				continue
			}

			// Envio bloqueante com timeout
			select {
			case lp.batchChan <- entry:
				metrics.ReceivedMessages.Inc()
			case <-ctx.Done():
				return ctx.Err()
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (lp *LogProcessor) processSingleMessage(msg string) (entity.LogEntry, error) {
	var rawEntry entity.RawEntry
	if err := json.Unmarshal([]byte(msg), &rawEntry); err != nil {
		return entity.LogEntry{}, fmt.Errorf("unmarshal error: %w", err)
	}

	parser, err := lp.logParserFactory.GetParser(rawEntry.Raw)
	if err != nil {
		return entity.LogEntry{}, fmt.Errorf("parser error: %w", err)
	}

	logEntry, err := parser.Parse(rawEntry.Raw)
	if err != nil {
		return entity.LogEntry{}, fmt.Errorf("parse error: %w", err)
	}

	if err := lp.sendProcessedMessage(rawEntry.Raw, logEntry, parser.PreservesRaw()); err != nil {
		return entity.LogEntry{}, fmt.Errorf("send error: %w", err)
	}

	return logEntry, nil
}

func (lp *LogProcessor) sendProcessedMessage(rawMsg string, entry entity.LogEntry, preserveRaw bool) error {
    var data []byte
    if preserveRaw {
        data = []byte(rawMsg)
    } else {
        var err error
        if data, err = json.Marshal(entry); err != nil {
            return err
        }
    }
    return lp.producer.Send(string(data))
}

func (lp *LogProcessor) safeFlush(batch []entity.LogEntry) {
	if len(batch) == 0 {
		return
	}

	go func(b []entity.LogEntry) {
		const maxRetries = 5
		for i := 0; i < maxRetries; i++ {
			if err := lp.transformRepository.Transform(b); err == nil {
				metrics.ProcessedMessages.Add(float64(len(b)))
				return
			}
			time.Sleep(time.Duration(i*i) * 100 * time.Millisecond)
		}
		log.Printf("Failed to persist batch of %d messages after %d retries", len(b), maxRetries)
	}(slices.Clone(batch))
}

func (lp *LogProcessor) Close() {
	lp.wg.Wait()
}