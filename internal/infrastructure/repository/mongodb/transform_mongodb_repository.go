package mongodb

import (
	"log"

	"github.com/etl_app_transform_service/internal/domain/entity"
	"go.mongodb.org/mongo-driver/mongo"
)

type TransformRepositoryMongoDB struct {
	database *mongo.Client
}

func NewTransformMongoDBRepository(database *mongo.Client) *TransformRepositoryMongoDB {
	return &TransformRepositoryMongoDB{
		database: database,
	}
}

func (s *TransformRepositoryMongoDB) Transform(logEntries []entity.LogEntry) error {
	for _, logEntry := range logEntries {
		log.Printf("Received on MongoDB: %v", logEntry)
	}

	return nil
}
