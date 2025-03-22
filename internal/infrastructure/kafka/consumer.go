package kafka

import (
	"log"
	"os"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type KafkaConsumer struct {
	consumer            *kafka.Consumer
	messages            chan string
	done                chan struct{}
	pausedPartitions    map[string]bool
	pauseMutex          sync.Mutex
	bufferHighWaterMark int
	bufferLowWaterMark  int
	commitChan          chan kafka.TopicPartition
}

func NewKafkaConsumer(topic, groupID string) *KafkaConsumer {
	config := &kafka.ConfigMap{
		"bootstrap.servers":      os.Getenv("KAFKA_BOOTSTRAP_SERVER"),
		"group.id":               groupID,
		"auto.offset.reset":      "earliest",
		"enable.auto.commit":     "false",
		"max.poll.interval.ms":   300000,
		"session.timeout.ms":     10000,
		"heartbeat.interval.ms":  3000,
		"go.events.channel.size": 10000,
	}

	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		log.Fatalf("error creating consumer: %v", err)
	}

	if err := consumer.Subscribe(topic, nil); err != nil {
		log.Fatalf("error subscribing to topic: %v", err)
	}

	k := &KafkaConsumer{
		consumer:            consumer,
		messages:            make(chan string, 10000),
		done:                make(chan struct{}),
		pausedPartitions:    make(map[string]bool),
		bufferHighWaterMark: 8000,
		bufferLowWaterMark:  2000,
		commitChan:          make(chan kafka.TopicPartition, 10000),
	}

	go k.manageBackpressure()
	go k.consume()
	go k.commitWorker()
	return k
}

func (c *KafkaConsumer) consume() {
    defer close(c.messages)
    
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
                c.commitChan <- e.TopicPartition
            case kafka.Error:
                log.Printf("consumer error: %v", e)
            }
        }
    }
}


func (c *KafkaConsumer) manageBackpressure() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			bufferLevel := len(c.messages)
			if bufferLevel >= c.bufferHighWaterMark {
				c.pauseConsumption()
			} else if bufferLevel <= c.bufferLowWaterMark {
				c.resumeConsumption()
			}

		case <-c.done:
			return
		}
	}
}

func (c *KafkaConsumer) pauseConsumption() {
	c.pauseMutex.Lock()
	defer c.pauseMutex.Unlock()

	partitions, err := c.consumer.Assignment()
	if err != nil {
		log.Printf("error getting partitions: %v", err)
		return
	}

	var toPause []kafka.TopicPartition
	for _, tp := range partitions {
		topic := *tp.Topic
		if !c.pausedPartitions[topic] {
			toPause = append(toPause, tp)
			c.pausedPartitions[topic] = true
		}
	}

	if len(toPause) > 0 {
		c.consumer.Pause(toPause)
	}
}

func (c *KafkaConsumer) resumeConsumption() {
	c.pauseMutex.Lock()
	defer c.pauseMutex.Unlock()

	partitions, err := c.consumer.Assignment()
	if err != nil {
		log.Printf("error getting partitions: %v", err)
		return
	}

	var toResume []kafka.TopicPartition
	for _, tp := range partitions {
		topic := *tp.Topic
		if c.pausedPartitions[topic] {
			toResume = append(toResume, tp)
			c.pausedPartitions[topic] = false
		}
	}

	if len(toResume) > 0 {
		c.consumer.Resume(toResume)
	}
}

func (c *KafkaConsumer) commitWorker() {
	var batch []kafka.TopicPartition
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case tp := <-c.commitChan:
			batch = append(batch, tp)
			if len(batch) >= 1000 {
				c.commitBatch(batch)
				batch = nil
			}

		case <-ticker.C:
			if len(batch) > 0 {
				c.commitBatch(batch)
				batch = nil
			}

		case <-c.done:
			c.commitBatch(batch)
			return
		}
	}
}

func (c *KafkaConsumer) commitBatch(batch []kafka.TopicPartition) {
	offsets := make([]kafka.TopicPartition, 0, len(batch))
	for _, tp := range batch {
		offsets = append(offsets, kafka.TopicPartition{
			Topic:     tp.Topic,
			Partition: tp.Partition,
			Offset:    tp.Offset + 1,
		})
	}

	if _, err := c.consumer.CommitOffsets(offsets); err != nil {
		log.Printf("commit error: %v", err)
	}
}

func (c *KafkaConsumer) Messages() <-chan string {
	return c.messages
}

func (c *KafkaConsumer) Close() {
	close(c.done)
	c.consumer.Close()
}
