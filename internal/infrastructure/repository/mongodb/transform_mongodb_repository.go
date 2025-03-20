package mongodb

import (
	"context"
	"log"

	"github.com/etl_app_transform_service/internal/domain/entity"
	metrics "github.com/etl_app_transform_service/internal/infrastructure/prometheus"
	"go.mongodb.org/mongo-driver/mongo"
)

type TransformRepositoryMongoDB struct {
	collection *mongo.Collection
}

func NewTransformMongoDBRepository(client *mongo.Client, dbName, collectionName string) *TransformRepositoryMongoDB {
	db := client.Database(dbName)
	return &TransformRepositoryMongoDB{
		collection: db.Collection(collectionName),
	}
}

func (r *TransformRepositoryMongoDB) Transform(logEntries []entity.LogEntry) error {
	if len(logEntries) == 0 {
		log.Println("No entries to insert")
		return nil
	}

	documents := make([]any, len(logEntries))
	for i, entry := range logEntries {
		latency := entry.CalculateTotalProcessedTimeByMilliseconds()	
		metrics.LogProcessingLatency.WithLabelValues("processed_logs").Observe(float64(latency))
		documents[i] = entry
	}

	_, err := r.collection.InsertMany(context.Background(), documents)
	if err != nil {
		log.Printf("Error inserting batch: %v", err)
		return err
	}

	return nil
}
