package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
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
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092,localhost:9093,localhost:9094",
	})

	if err != nil {
		log.Fatalf("error: failed to create producer: %v", err)
	}

	// goroutine to handle message delivery reports and
	// possibly other event types (errors, stats, etc)
	go func() {
		for e := range p.Events() {
			switch event := e.(type) {
			case *kafka.Message:
				if event.TopicPartition.Error != nil {
					log.Printf("error: failed to deliver message: %v\n", event.TopicPartition)
				} else {
					PrintEvent(event)
				}
			}
		}
	}()

	topic := "baz-topic"

	// set up a channel for handling Ctrl-C
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGINT, syscall.SIGTERM)

	run := true

	// produce event
	for run {
		select {
		case signal := <-signalChannel:
			log.Printf("caught signal '%v': terminating\n", signal)
			run = false
		default:
			uuid := uuid.New()
			key := fmt.Sprintf("baz-key-%s", uuid)
			value := fmt.Sprintf("baz-value-%s", uuid)
			err := p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Key:            []byte(key),
				Value:          []byte(value),
			}, nil)
			if err != nil {
				log.Printf("error: failed to produce event to topic %s: %v", topic, err)
				continue
			}
			// wait for 1 second before producing the next message
			time.Sleep(500 * time.Millisecond)
		}
	}

	// wait for all messages to be delivered
	p.Flush(15 * 1000)
	p.Close()
}

func PrintEvent(event *kafka.Message) {
	log.Printf("%sProduced event to topic %s:%s\n", colorGreen, *event.TopicPartition.Topic, colorReset)
	log.Printf("\t%sPartition: %d%s\n", colorMagenta, event.TopicPartition.Partition, colorReset)
	log.Printf("\t%sOffset: %d%s\n", colorCyan, event.TopicPartition.Offset, colorReset)
	log.Printf("\t%sKey: %s%s\n", colorYellow, string(event.Key), colorReset)
	log.Printf("\t%sValue: %s%s\n", colorBlue, string(event.Value), colorReset)
	// the delivery report event (*kafka.Message) returned by the Kafka producer
	// doesn't include the original message timestamp.
	// It only contains information about the delivery itself,
	// such as the topic, partition, offset, and any errors that occurred during delivery
	// fmt.Printf("\tTimestamp: %s\n", event.Timestamp)
}
