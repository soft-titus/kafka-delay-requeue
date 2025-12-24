package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	const originTopic = "test.topic"

	var (
		retryCount      int
		processAfterStr string
	)

	flag.IntVar(&retryCount, "retry-count", 0, "Retry count header (default: 0)")
	flag.StringVar(&processAfterStr, "process-after", "", "Process-after RFC3339 time (default: now + 10s)")
	flag.Parse()

	// process-after default: now + 10 seconds
	var processAfter time.Time
	if processAfterStr == "" {
		processAfter = time.Now().Add(10 * time.Second)
	} else {
		t, err := time.Parse(time.RFC3339, processAfterStr)
		if err != nil {
			log.Fatalf(
				"Invalid process-after value, must be RFC3339 (e.g.: 2025-01-01T10:00:00Z): %v",
				err,
			)
		}
		processAfter = t
	}

	kafkaBroker := fmt.Sprintf(
		"%s:%s",
		os.Getenv("KAFKA_BROKER_HOST"),
		os.Getenv("KAFKA_BROKER_PORT"),
	)

	retriableTopic := os.Getenv("KAFKA_RETRIABLE_TOPIC")
	if retriableTopic == "" {
		log.Fatal("KAFKA_RETRIABLE_TOPIC is not set")
	}

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{kafkaBroker},
		Topic:   retriableTopic,
	})
	defer func() {
		if err := writer.Close(); err != nil {
			log.Printf("failed to close kafka writer: %v", err)
		}
	}()

	// Simple payload
	payload := map[string]string{
		"message": "hello world",
	}

	data, err := json.Marshal(payload)
	if err != nil {
		log.Fatalf("Failed to marshal payload: %v", err)
	}

	headers := []kafka.Header{
		{Key: "retry-count", Value: []byte(strconv.Itoa(retryCount))},
		{Key: "process-after", Value: []byte(processAfter.Format(time.RFC3339))},
		{Key: "origin-topic", Value: []byte(originTopic)},
	}

	err = writer.WriteMessages(
		context.Background(),
		kafka.Message{
			Value:   data,
			Headers: headers,
		},
	)
	if err != nil {
		log.Fatalf("Failed to send message: %v", err)
	}

	log.Printf(
		"Sent message retry-count=%d process-after=%s origin-topic=%s",
		retryCount,
		processAfter.Format(time.RFC3339),
		originTopic,
	)
}
