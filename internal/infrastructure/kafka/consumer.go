package kafka

import (
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type KafkaConsumer struct {
	consumer *kafka.Consumer
	topic    string
	messages chan string
	done     chan struct{}
}

func NewKafkaConsumer(topic, groupID string) *KafkaConsumer {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  os.Getenv("KAFKA_BOOTSTRAP_SERVER"),
		"group.id":           groupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "false",
	})
	if err != nil {
		log.Fatalf("error creating consumer: %v", err)
	}

	k := &KafkaConsumer{
		consumer: consumer,
		topic:    topic,
		messages: make(chan string, 1000),
		done:     make(chan struct{}),
	}

	go k.consume()
	return k
}

func (c *KafkaConsumer) consume() {
	defer close(c.messages)
	
	if err := c.consumer.Subscribe(c.topic, nil); err != nil {
		log.Fatalf("error subscribing to topic: %v", err)
	}

	for {
		select {
		case <-c.done:
			return
		default:
			ev := c.consumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				c.messages <- string(e.Value)
				if _, err := c.consumer.CommitMessage(e); err != nil {
					log.Printf("error committing offset: %v", err)
				}
			case kafka.Error:
				log.Printf("consumer error: %v", e)
				if e.Code() == kafka.ErrAllBrokersDown {
					return
				}
			}
		}
	}
}

func (c *KafkaConsumer) Messages() <-chan string {
	return c.messages
}

func (c *KafkaConsumer) Close() {
	close(c.done)
	c.consumer.Close()
}