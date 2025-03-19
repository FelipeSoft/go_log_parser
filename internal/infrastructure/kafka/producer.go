package kafka

import (
	"log"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type KafkaProducer struct {
	producer *kafka.Producer
	topic    string
}

func NewKafkaProducer(topic string) *KafkaProducer {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": os.Getenv("KAFKA_BOOTSTRAP_SERVER"),
	})
	if err != nil {
		log.Fatalf("error creating producer: %v", err)
	}

	return &KafkaProducer{
		producer: producer,
		topic:    topic,
	}
}

func (p *KafkaProducer) Send(message string) error {
	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)

	err := p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &p.topic,
			Partition: kafka.PartitionAny,
		},
		Value: []byte(message),
	}, deliveryChan)

	if err != nil {
		return err
	}

	select {
	case e := <-deliveryChan:
		m := e.(*kafka.Message)
		if m.TopicPartition.Error != nil {
			return m.TopicPartition.Error
		}
	case <-time.After(3 * time.Second):
		return kafka.NewError(kafka.ErrTimedOut, "delivery timeout", false)
	}

	return nil
}

func (p *KafkaProducer) Close() {
    p.producer.Flush(15 * 1000) 
    p.producer.Close()
}