package application

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/etl_app_transform_service/internal/domain/entity"
	"github.com/etl_app_transform_service/internal/infrastructure/metrics"
)

var (
	counter struct {
		received  int64
		processed int64
	}
	muCounter sync.RWMutex
)

type LogProcessor struct {
	batchLinesSize      int
	batchTimeout        time.Duration
	consumer            entity.MessageConsumer
	producer            entity.MessageProducer
	logParserFactory    *LogParserFactory
	transformRepository entity.TransformRepository
}

func NewLogProcessor(
	batchLinesSize int,
	batchTimeout time.Duration,
	consumer entity.MessageConsumer,
	producer entity.MessageProducer,
	logParserFactory *LogParserFactory,
	transformRepository entity.TransformRepository,
) *LogProcessor {
	return &LogProcessor{
		batchLinesSize:      batchLinesSize,
		batchTimeout:        batchTimeout,
		consumer:            consumer,
		producer:            producer,
		logParserFactory:    logParserFactory,
		transformRepository: transformRepository,
	}
}

func GetMetrics() (int64, int64) {
	muCounter.RLock()
	defer muCounter.RUnlock()
	return counter.received, counter.processed
}

func (lp *LogProcessor) ProcessLogs(ctx context.Context) error {
	var (
		mu          sync.Mutex
		batch       []entity.LogEntry
		flushSignal = make(chan struct{}, 1)
	)
	defer func() {
		mu.Lock()
		lp.flushBatch(batch)
		mu.Unlock()
	}()

	ticker := time.NewTicker(lp.batchTimeout)
	defer ticker.Stop()

	go func() {
		defer close(flushSignal)
		for {
			select {
			case <-ticker.C:
				flushSignal <- struct{}{}
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		select {
		case msg, ok := <-lp.consumer.Messages():
			if !ok {
				mu.Lock()
				lp.flushBatch(batch)
				mu.Unlock()
				return nil
			}

			muCounter.Lock()
			counter.received++
			muCounter.Unlock()

			var rawEntry entity.RawEntry
			err := json.Unmarshal([]byte(msg), &rawEntry)
			if err != nil {
				return fmt.Errorf("processMessage failed during unmarshal: %v", err)
			}

			logEntry, err := lp.processMessage(rawEntry)
			logEntry.StartAt = time.Now()

			if err != nil {
				muCounter.Lock()
				counter.received--
				muCounter.Unlock()

				return fmt.Errorf("processMessage failed: %v", err)
			}

			mu.Lock()
			batch = append(batch, logEntry)
			if len(batch) >= lp.batchLinesSize {
				lp.flushBatch(batch)
				batch = nil
			}
			mu.Unlock()

		case <-flushSignal:
			mu.Lock()
			if len(batch) > 0 {
				lp.flushBatch(batch)
				batch = nil
			}
			mu.Unlock()

		case <-ctx.Done():
			mu.Lock()
			defer mu.Unlock()

			drainCtx, cancelDrain := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancelDrain()

			for {
				select {
				case msg := <-lp.consumer.Messages():
					var rawEntry entity.RawEntry
					err := json.Unmarshal([]byte(msg), &rawEntry)
					if err != nil {
						return fmt.Errorf("processMessage failed during unmarshal: %v", err)
					}

					logEntry, err := lp.processMessage(rawEntry)
					if err == nil {
						batch = append(batch, logEntry)
					}
				case <-drainCtx.Done():
					lp.flushBatch(batch)
					return nil
				default:
					lp.flushBatch(batch)
					return nil
				}
			}
		}
	}
}

func (lp *LogProcessor) processMessage(rawEntry entity.RawEntry) (entity.LogEntry, error) {
	msg := rawEntry.Raw

	parser, err := lp.logParserFactory.GetParser(msg)
	if err != nil {
		return entity.LogEntry{}, fmt.Errorf("get parser failed: %v", err)
	}

	logEntry, err := parser.Parse(msg)
	logEntry.StartAt = rawEntry.StartAt

	if err != nil {
		return entity.LogEntry{}, fmt.Errorf("parse failed: %v", err)
	}

	var data []byte
	if parser.PreservesRaw() {
		data = []byte(msg)
	} else {
		data, err = json.Marshal(logEntry)
		if err != nil {
			return entity.LogEntry{}, fmt.Errorf("marshal failed: %v", err)
		}
	}

	err = lp.producer.Send(string(data))
	if err != nil {
		return entity.LogEntry{}, fmt.Errorf("send failed: %v", err)
	} else {
		metrics.LogProcessingBySeconds.Inc()
	}

	return logEntry, nil
}

func (lp *LogProcessor) flushBatch(batch []entity.LogEntry) {
	if len(batch) == 0 {
		return
	}

	const maxRetries = 3
	for i := range maxRetries {
		if err := lp.transformRepository.Transform(batch); err == nil {
			muCounter.Lock()
			counter.processed += int64(len(batch))
			muCounter.Unlock()
			return
		}
		time.Sleep(time.Duration(i*100) * time.Millisecond)
	}

	log.Printf("Persistent fail on insert batch of %d lines", len(batch))
}
