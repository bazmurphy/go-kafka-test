package main

import (
	"context"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092,localhost:9093,localhost:9094",
	})
	if err != nil {
		log.Fatalf("error: failed to create admin client: %v", err)
	}
	defer adminClient.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	topic := "baz-topic"
	numPartitions := 3
	replicationFactor := 1

	topicSpec := []kafka.TopicSpecification{{
		Topic:             topic,
		NumPartitions:     numPartitions,
		ReplicationFactor: replicationFactor,
	}}

	result, err := adminClient.CreateTopics(ctx, topicSpec)
	if err != nil {
		log.Fatalf("error: failed to create topic: %v", err)
	}

	log.Println("admin.go | CreateTopics | result:", result)
}
