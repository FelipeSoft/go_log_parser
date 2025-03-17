package mongodb

import (
	"context"
	"log"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func StartConnection() *mongo.Client {
	mgoUrl := os.Getenv("MONGODB_URL")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	mgoConn, err := mongo.Connect(ctx, options.Client().ApplyURI(mgoUrl))
	if err != nil {
		log.Fatalf("Error during connection with mongodb: %v", err)
	}
	log.Printf("Connected successfully with mongodb")
	return mgoConn
}