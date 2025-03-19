package mongodb

import (
	"context"
	"log"

	"github.com/etl_app_transform_service/internal/domain/entity"
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
	log.Printf("Received batch of %d entries for MongoDB insertion", len(logEntries))

	if len(logEntries) == 0 {
		log.Println("No entries to insert")
		return nil
	}

	documents := make([]any, len(logEntries))
	for i, entry := range logEntries {
		documents[i] = entry
	}

	_, err := r.collection.InsertMany(context.Background(), documents)
	if err != nil {
		log.Printf("Error inserting batch: %v", err)
		return err
	}

	log.Printf("Successfully inserted %d documents", len(logEntries))
	return nil
}

