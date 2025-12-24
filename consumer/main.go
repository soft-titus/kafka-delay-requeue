package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/segmentio/kafka-go"
)

var (
	kafkaBroker       string
	retriableTopic    string
	retriableDLQTopic string
	consumerGroup     string

	delayShort time.Duration
	delayLong  time.Duration

	kafkaClient *kafka.Client
)

func init() {
	// Logging
	level, err := log.ParseLevel(os.Getenv("LOG_LEVEL"))
	if err != nil {
		level = log.InfoLevel
	}
	log.SetLevel(level)
	log.SetFormatter(&log.TextFormatter{FullTimestamp: true})
	log.SetOutput(os.Stdout)

	// Kafka
	kafkaBroker = fmt.Sprintf(
		"%s:%s",
		os.Getenv("KAFKA_BROKER_HOST"),
		os.Getenv("KAFKA_BROKER_PORT"),
	)

	retriableTopic = os.Getenv("KAFKA_RETRIABLE_TOPIC")
	retriableDLQTopic = os.Getenv("KAFKA_RETRIABLE_DLQ_TOPIC")
	consumerGroup = os.Getenv("KAFKA_CONSUMER_GROUP_NAME")

	// Delays
	shortSec, _ := strconv.Atoi(os.Getenv("DELAY_SHORT_INTERVAL_SECONDS"))
	longSec, _ := strconv.Atoi(os.Getenv("DELAY_LONG_INTERVAL_SECONDS"))

	if shortSec <= 0 {
		shortSec = 1
	}
	if longSec <= 0 {
		longSec = 15
	}

	delayShort = time.Duration(shortSec) * time.Second
	delayLong = time.Duration(longSec) * time.Second

	kafkaClient = &kafka.Client{
		Addr: kafka.TCP(kafkaBroker),
	}
}

func main() {
	ctx := context.Background()

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{kafkaBroker},
		Topic:          retriableTopic,
		GroupID:        consumerGroup,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 0, // manual commit
	})
	defer func() {
		if err := r.Close(); err != nil {
			log.Errorf("failed to close kafka reader: %v", err)
		}
	}()

	wRequeue := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{kafkaBroker},
		Topic:   retriableTopic,
	})
	defer func() {
		if err := wRequeue.Close(); err != nil {
			log.Errorf("failed to close kafka requeue writer: %v", err)
		}
	}()

	wDLQ := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{kafkaBroker},
		Topic:   retriableDLQTopic,
	})
	defer func() {
		if err := wDLQ.Close(); err != nil {
			log.Errorf("failed to close kafka DLQ writer: %v", err)
		}
	}()

	log.Info("Starting kafka delay requeue worker...")

	for {
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			log.Errorf("Failed to fetch message: %v", err)
			continue
		}

		log.Infof("Received message: %s", string(msg.Value))

		// Quick heartbeat check
		var raw map[string]interface{}
		if err := json.Unmarshal(msg.Value, &raw); err == nil {
			if msgType, ok := raw["type"].(string); ok && msgType == "heartbeat" {
				log.Infof("Heartbeat message received, skipping processing for partition %d offset %d", msg.Partition, msg.Offset)
				commitMessage(ctx, r, msg)
				continue
			}
		}

		originTopic, retryCount, processAfter, err := extractHeaders(msg.Headers)
		if err != nil {
			pushToDLQ(ctx, msg, wDLQ, r, err.Error())
			continue
		}

		now := time.Now()
		remaining := processAfter.Sub(now)

		lag, err := getPartitionLag(ctx, kafkaClient, retriableTopic, consumerGroup, msg.Partition)
		if err != nil {
			log.Warnf("Failed to fetch partition lag: %v", err)
			lag = 0
		}

		if remaining > 0 {
			// requeue the message back to the same retriable topic
			err = wRequeue.WriteMessages(ctx, kafka.Message{
				Key:     msg.Key,
				Value:   msg.Value,
				Headers: msg.Headers,
			})
			if err != nil {
				log.Errorf("Failed to requeue message to retriable topic: %v", err)
			} else {
				commitMessage(ctx, r, msg)
				log.Infof("Message requeued to retriable topic (retry-count=%d)", retryCount)
			}

			sleepFor := delayLong
			if lag > 1 {
				sleepFor = delayShort
			}

			if sleepFor > remaining {
				sleepFor = remaining
			}

			log.Infof(
				"Message not ready, sleeping %s (remaining=%s, lag=%d)",
				sleepFor,
				remaining,
				lag,
			)

			select {
			case <-time.After(sleepFor):
			case <-ctx.Done():
				log.Info("Shutting down during sleep")
				return
			}

			continue
		}

		wOrigin := kafka.NewWriter(kafka.WriterConfig{
			Brokers: []string{kafkaBroker},
			Topic:   originTopic,
		})

		err = wOrigin.WriteMessages(ctx, kafka.Message{
			Key:     msg.Key,
			Value:   msg.Value,
			Headers: msg.Headers,
		})
		if err != nil {
			log.Errorf("Failed to requeue to origin topic %s: %v", originTopic, err)
			continue
		}

		err = wOrigin.Close()
		if err != nil {
			log.Warnf("Failed to close kafka origin writer: %v", err)
		}

		log.Infof(
			"Message requeued to %s (retry-count=%d)",
			originTopic,
			retryCount,
		)

		commitMessage(ctx, r, msg)
	}
}

func getPartitionLag(ctx context.Context, client *kafka.Client, topic, groupID string, partitionID int) (int64, error) {
	committedResp, err := client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{
		GroupID: groupID,
		Topics: map[string][]int{
			topic: {partitionID},
		},
	})
	if err != nil {
		return 0, fmt.Errorf("offset fetch failed: %w", err)
	}

	var committedOffset int64 = -1
	if partitions, ok := committedResp.Topics[topic]; ok {
		for _, p := range partitions {
			if p.Partition == partitionID && p.CommittedOffset != -1 {
				committedOffset = p.CommittedOffset
			}
		}
	}

	lastOffsetsResp, err := client.ListOffsets(ctx, &kafka.ListOffsetsRequest{
		Topics: map[string][]kafka.OffsetRequest{
			topic: {
				kafka.LastOffsetOf(partitionID),
			},
		},
	})
	if err != nil {
		return 0, fmt.Errorf("list offsets failed: %w", err)
	}

	lastOffset := lastOffsetsResp.Topics[topic][0].LastOffset

	if committedOffset < 0 {
		return lastOffset, nil
	}
	return lastOffset - committedOffset, nil
}

func extractHeaders(headers []kafka.Header) (string, int, time.Time, error) {
	var (
		originTopic  string
		retryCount   *int
		processAfter *time.Time
	)

	for _, h := range headers {
		switch h.Key {
		case "origin-topic":
			originTopic = string(h.Value)

		case "retry-count":
			v, err := strconv.Atoi(string(h.Value))
			if err == nil {
				retryCount = &v
			}

		case "process-after":
			t, err := time.Parse(time.RFC3339, string(h.Value))
			if err == nil {
				processAfter = &t
			}
		}
	}

	if originTopic == "" {
		return "", 0, time.Time{}, fmt.Errorf("missing required header: origin-topic")
	}
	if retryCount == nil {
		return "", 0, time.Time{}, fmt.Errorf("missing required header: retry-count")
	}
	if processAfter == nil {
		return "", 0, time.Time{}, fmt.Errorf("missing or invalid header: process-after")
	}

	return originTopic, *retryCount, *processAfter, nil
}

func pushToDLQ(
	ctx context.Context,
	msg kafka.Message,
	wDLQ *kafka.Writer,
	r *kafka.Reader,
	reason string,
) {
	var payload map[string]interface{}
	if err := json.Unmarshal(msg.Value, &payload); err != nil {
		payload = map[string]interface{}{
			"originalPayload": string(msg.Value),
		}
	}

	payload["failedReason"] = reason
	payload["failedAt"] = time.Now().Format(time.RFC3339)

	data, _ := json.Marshal(payload)

	_ = wDLQ.WriteMessages(ctx, kafka.Message{
		Key:   msg.Key,
		Value: data,
	})

	log.Infof("Message sent to DLQ: %s", reason)
	commitMessage(ctx, r, msg)
}

func commitMessage(ctx context.Context, r *kafka.Reader, msg kafka.Message) {
	if err := r.CommitMessages(ctx, msg); err != nil {
		log.Errorf("Failed to commit message: %v", err)
	}
}
