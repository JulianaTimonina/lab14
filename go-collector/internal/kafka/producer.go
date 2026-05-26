package kafka

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"energy-monitoring-system/go-collector/pkg/models"
)

// KafkaProducer реализует отправку агрегированных данных в Kafka
type KafkaProducer struct {
	producer   *kafka.Producer
	topic      string
	bootstrapServers string
	deliveryChan chan kafka.Event
}

// NewKafkaProducer создаёт новый Kafka producer
func NewKafkaProducer(bootstrapServers, topic string) (*KafkaProducer, error) {
	config := &kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"client.id":         "go-collector",
		"acks":              "all", // гарантированная доставка
		"retries":           3,
		"retry.backoff.ms":  1000,
		"compression.type":  "snappy",
		"batch.size":        16384,
		"linger.ms":         10,
	}

	producer, err := kafka.NewProducer(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	kp := &KafkaProducer{
		producer:         producer,
		topic:            topic,
		bootstrapServers: bootstrapServers,
		deliveryChan:     make(chan kafka.Event, 100),
	}

	// Запускаем мониторинг доставки сообщений
	go kp.monitorDeliveries()

	log.Printf("Kafka producer initialized for topic %s on %s", topic, bootstrapServers)
	return kp, nil
}

// SendAggregatedData отправляет агрегированные данные в Kafka
func (kp *KafkaProducer) SendAggregatedData(data models.AggregatedData) error {
	// Сериализуем данные в JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal aggregated data: %w", err)
	}

	// Создаём сообщение
	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &kp.topic,
			Partition: kafka.PartitionAny,
		},
		Value:     jsonData,
		Key:       []byte(data.WindowStart.Format(time.RFC3339)), // ключ - начало окна
		Timestamp: time.Now(),
	}

	// Отправляем сообщение
	err = kp.producer.Produce(message, kp.deliveryChan)
	if err != nil {
		return fmt.Errorf("failed to produce message: %w", err)
	}

	return nil
}

// SendRawReading отправляет сырое показание счётчика
func (kp *KafkaProducer) SendRawReading(reading models.MeterReading) error {
	jsonData, err := json.Marshal(reading)
	if err != nil {
		return fmt.Errorf("failed to marshal meter reading: %w", err)
	}

	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &kp.topic,
			Partition: kafka.PartitionAny,
		},
		Value:     jsonData,
		Key:       []byte(reading.MeterID),
		Timestamp: reading.Timestamp,
	}

	err = kp.producer.Produce(message, kp.deliveryChan)
	if err != nil {
		return fmt.Errorf("failed to produce raw reading: %w", err)
	}

	return nil
}

// monitorDeliveries отслеживает доставку сообщений
func (kp *KafkaProducer) monitorDeliveries() {
	for e := range kp.deliveryChan {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				log.Printf("Failed to deliver message to %v: %v", 
					ev.TopicPartition, ev.TopicPartition.Error)
			} else {
				log.Printf("Successfully delivered message to %v (offset %v)",
					ev.TopicPartition, ev.TopicPartition.Offset)
			}
		case kafka.Error:
			log.Printf("Kafka error: %v", ev)
		default:
			log.Printf("Kafka event: %v", ev)
		}
	}
}

// Close закрывает producer
func (kp *KafkaProducer) Close() {
	log.Println("Closing Kafka producer...")
	kp.producer.Flush(5000) // ждём до 5 секунд для отправки оставшихся сообщений
	kp.producer.Close()
	close(kp.deliveryChan)
	log.Println("Kafka producer closed")
}

// HealthCheck проверяет соединение с Kafka
func (kp *KafkaProducer) HealthCheck() error {
	// Простая проверка через получение метаданных
	metadata, err := kp.producer.GetMetadata(nil, true, 5000)
	if err != nil {
		return fmt.Errorf("failed to get Kafka metadata: %w", err)
	}

	// Проверяем, что наш топик существует
	topicMetadata, ok := metadata.Topics[kp.topic]
	if !ok {
		return fmt.Errorf("topic %s not found in Kafka", kp.topic)
	}

	if len(topicMetadata.Partitions) == 0 {
		return fmt.Errorf("topic %s has no partitions", kp.topic)
	}

	return nil
}