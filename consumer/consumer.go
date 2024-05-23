package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	colorReset   = "\033[0m"
	colorGreen   = "\033[32m"
	colorYellow  = "\033[33m"
	colorBlue    = "\033[34m"
	colorMagenta = "\033[35m"
	colorCyan    = "\033[36m"
)

func main() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092,localhost:9093,localhost:9094",
		"group.id":          "baz-topic-consumer-group",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		log.Fatalf("error: failed to create consumer: %v", err)
	}

	topic := "baz-topic"

	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		log.Fatalf("error: failed to subscribe to topic %s: %v", topic, err)
	}

	// handle Ctrl-C
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGINT, syscall.SIGTERM)

	metadata, err := c.GetMetadata(&topic, false, 2000)
	if err != nil {
		log.Printf("error: failed to get metadata: %v\n", err)
	}
	PrintMetadata(metadata)

	run := true

	// consume event
	for run {
		select {
		case signal := <-signalChannel:
			log.Printf("caught signal '%v': terminating\n", signal)
			run = false
		default:
			event, err := c.ReadMessage(100 * time.Millisecond)
			if err != nil {
				// errors are informational and automatically handled by the consumer
				continue
			}
			PrintEvent(event)
		}
	}

	c.Close()
}

func PrintMetadata(metadata *kafka.Metadata) {
	log.Printf("Metadata:\n")
	log.Printf("\tBrokers:\n")
	for _, broker := range metadata.Brokers {
		log.Printf("\t\tBroker %d:\n", broker.ID)
		log.Printf("\t\t\tHost: %s\n", broker.Host)
		log.Printf("\t\t\tPort: %d\n", broker.Port)
	}
	log.Printf("\tTopics:\n")
	for _, topicMetadata := range metadata.Topics {
		log.Printf("\t\tTopic: %s\n", topicMetadata.Topic)
		log.Printf("\t\t\tPartitions:\n")
		for _, partition := range topicMetadata.Partitions {
			log.Printf("\t\t\t\tPartition %d:\n", partition.ID)
			log.Printf("\t\t\t\t\tLeader: %d\n", partition.Leader)
			log.Printf("\t\t\t\t\tReplicas: %v\n", partition.Replicas)
		}
	}
}

func PrintEvent(event *kafka.Message) {
	log.Printf("%sConsumed event from topic %s:%s\n", colorGreen, *event.TopicPartition.Topic, colorReset)
	log.Printf("\t%sPartition: %d%s\n", colorMagenta, event.TopicPartition.Partition, colorReset)
	log.Printf("\t%sOffset: %d%s\n", colorCyan, event.TopicPartition.Offset, colorReset)
	log.Printf("\t%sKey: %s%s\n", colorYellow, string(event.Key), colorReset)
	log.Printf("\t%sValue: %s%s\n", colorBlue, string(event.Value), colorReset)
	log.Printf("\tTimestamp: %v\n", event.Timestamp)
}
