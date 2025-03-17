package memory

import (
	"log"

	"github.com/etl_app_transform_service/internal/domain/entity"
)

type TransformMockRepository struct{}

func NewTransformMockRepository() *TransformMockRepository {
	return &TransformMockRepository{}
}

func (s *TransformMockRepository) Transform(logEntries []entity.LogEntry) error {
	for logEntry := range logEntries {
		log.Printf("Log Entry: %v", &logEntry)
	}

	return nil
}
