package application

import (
	"encoding/json"
	"fmt"

	"github.com/etl_app_transform_service/internal/domain/entity"
)

type LogProcessor struct {
	batchLinesSize      int
	consumer            entity.MessageConsumer // using interfaces
	producer            entity.MessageProducer
	logParserFactory    *LogParserFactory
	transformRepository entity.TransformRepository
}

func NewLogProcessor(batchLinesSize int, consumer entity.MessageConsumer, producer entity.MessageProducer, logParserFactory *LogParserFactory, transformRepository entity.TransformRepository) *LogProcessor {	return &LogProcessor{
		batchLinesSize:      batchLinesSize,
		consumer:           consumer,
		producer:           producer,
		logParserFactory:    logParserFactory,
		transformRepository: transformRepository,
	}
}

func (lp *LogProcessor) ProcessLogs() error {
	var batch []entity.LogEntry

	for msg := range lp.consumer.Messages() {
		parser, err := lp.logParserFactory.GetParser(msg)
		if err != nil {
			return fmt.Errorf("error parsing line %s: %v", msg, err)
		}
		logEntry, err := parser.Parse(msg)
		if err != nil {
			return fmt.Errorf("error parsing line %s: %v", msg, err)
		}
		batch = append(batch, logEntry)
		if len(batch) >= lp.batchLinesSize {
			data, err := json.Marshal(batch)
			if err != nil {
				return err
			}
			if err := lp.producer.Send(string(data)); err != nil {
				return err
			}
			batch = nil
		}
	}

	if len(batch) > 0 {
		data, err := json.Marshal(batch)
		if err != nil {
			return err
		}
		if err := lp.producer.Send(string(data)); err != nil {
			return err
		}
	}

	return nil
}