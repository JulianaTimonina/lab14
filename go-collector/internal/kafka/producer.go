package kafka

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"energy-monitoring-system/go-collector/pkg/models"
)

var (
	messagesProduced = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_messages_produced_total",
		Help: "Total number of Kafka messages produced",
	}, []string{"topic", "status"})

	messagesFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_messages_failed_total",
		Help: "Total number of Kafka messages that failed to produce",
	}, []string{"topic", "error_type"})

	messageLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kafka_message_produce_latency_seconds",
		Help:    "Latency of Kafka message production",
		Buckets: prometheus.DefBuckets,
	}, []string{"topic"})

	producerHealth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "kafka_producer_health",
		Help: "Health status of Kafka producer (1 = healthy, 0 = unhealthy)",
	})
)

// KafkaProducer реализует отправку агрегированных данных в Kafka
type KafkaProducer struct {
	producer          *kafka.Producer
	topic             string
	deadLetterTopic   string
	bootstrapServers  string
	deliveryChan      chan kafka.Event
	errorsChan        chan error
	shutdownChan      chan struct{}
	metricsEnabled    bool
}

// NewKafkaProducer создаёт новый Kafka producer
func NewKafkaProducer(bootstrapServers, topic string) (*KafkaProducer, error) {
	return NewKafkaProducerWithOptions(bootstrapServers, topic, "")
}

// NewKafkaProducerWithOptions создаёт новый Kafka producer с дополнительными опциями
func NewKafkaProducerWithOptions(bootstrapServers, topic, deadLetterTopic string) (*KafkaProducer, error) {
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

	if deadLetterTopic == "" {
		deadLetterTopic = topic + "-dead-letter"
	}

	kp := &KafkaProducer{
		producer:         producer,
		topic:            topic,
		deadLetterTopic:  deadLetterTopic,
		bootstrapServers: bootstrapServers,
		deliveryChan:     make(chan kafka.Event, 100),
		errorsChan:       make(chan error, 100),
		shutdownChan:     make(chan struct{}),
		metricsEnabled:   true,
	}

	// Запускаем мониторинг доставки сообщений
	go kp.monitorDeliveries()
	// Запускаем обработку ошибок
	go kp.handleErrors()

	log.Printf("Kafka producer initialized for topic %s on %s (dead letter topic: %s)", 
		topic, bootstrapServers, deadLetterTopic)
	return kp, nil
}

// sendWithRetry отправляет сообщение с повторными попытками и экспоненциальным backoff
func (kp *KafkaProducer) sendWithRetry(message *kafka.Message, maxRetries int) error {
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		startTime := time.Now()
		err := kp.producer.Produce(message, kp.deliveryChan)
		if err != nil {
			lastErr = err
			log.Printf("Attempt %d failed to produce message: %v", attempt+1, err)
			if kp.metricsEnabled {
				messagesFailed.WithLabelValues(kp.topic, "produce_error").Inc()
			}
			// Экспоненциальный backoff
			backoff := time.Duration(1<<uint(attempt)) * time.Second
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
			time.Sleep(backoff)
			continue
		}
		// Успешная отправка
		if kp.metricsEnabled {
			messagesProduced.WithLabelValues(kp.topic, "success").Inc()
			messageLatency.WithLabelValues(kp.topic).Observe(time.Since(startTime).Seconds())
		}
		return nil
	}
	return fmt.Errorf("failed after %d retries: %w", maxRetries, lastErr)
}

// sendToDeadLetter отправляет сообщение в dead letter topic
func (kp *KafkaProducer) sendToDeadLetter(message *kafka.Message, originalErr error) {
	log.Printf("Sending message to dead letter topic %s due to error: %v", kp.deadLetterTopic, originalErr)
	// Создаём копию сообщения с изменённым топиком
	deadLetterMessage := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &kp.deadLetterTopic,
			Partition: kafka.PartitionAny,
		},
		Value:     message.Value,
		Key:       message.Key,
		Timestamp: time.Now(),
		Headers:   append(message.Headers, kafka.Header{
			Key:   "original_error",
			Value: []byte(originalErr.Error()),
		}),
	}
	// Отправляем без повторных попыток
	err := kp.producer.Produce(deadLetterMessage, nil)
	if err != nil {
		log.Printf("Failed to send message to dead letter topic: %v", err)
	} else {
		log.Printf("Message sent to dead letter topic %s", kp.deadLetterTopic)
	}
}

// SendAggregatedData отправляет агрегированные данные в Kafka
func (kp *KafkaProducer) SendAggregatedData(data models.AggregatedData) error {
	// Сериализуем данные в JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		if kp.metricsEnabled {
			messagesFailed.WithLabelValues(kp.topic, "serialization_error").Inc()
		}
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

	// Отправляем с повторными попытками
	err = kp.sendWithRetry(message, 3)
	if err != nil {
		// Отправляем в dead letter queue
		kp.sendToDeadLetter(message, err)
		return fmt.Errorf("failed to send aggregated data: %w", err)
	}

	return nil
}

// SendRawReading отправляет сырое показание счётчика
func (kp *KafkaProducer) SendRawReading(reading models.MeterReading) error {
	jsonData, err := json.Marshal(reading)
	if err != nil {
		if kp.metricsEnabled {
			messagesFailed.WithLabelValues(kp.topic, "serialization_error").Inc()
		}
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

	err = kp.sendWithRetry(message, 3)
	if err != nil {
		kp.sendToDeadLetter(message, err)
		return fmt.Errorf("failed to send raw reading: %w", err)
	}

	return nil
}

// monitorDeliveries отслеживает доставку сообщений и обновляет метрики
func (kp *KafkaProducer) monitorDeliveries() {
	for e := range kp.deliveryChan {
		switch ev := e.(type) {
		case *kafka.Message:
			startTime := ev.Timestamp
			if startTime.IsZero() {
				startTime = time.Now()
			}
			latency := time.Since(startTime).Seconds()

			if ev.TopicPartition.Error != nil {
				log.Printf("Failed to deliver message to %v: %v", 
					ev.TopicPartition, ev.TopicPartition.Error)
				if kp.metricsEnabled {
					messagesFailed.WithLabelValues(kp.topic, "delivery_error").Inc()
				}
				// Отправляем ошибку в канал обработки
				kp.errorsChan <- fmt.Errorf("delivery failed: %v", ev.TopicPartition.Error)
			} else {
				log.Printf("Successfully delivered message to %v (offset %v)",
					ev.TopicPartition, ev.TopicPartition.Offset)
				if kp.metricsEnabled {
					messagesProduced.WithLabelValues(kp.topic, "success").Inc()
					messageLatency.WithLabelValues(kp.topic).Observe(latency)
				}
			}
		case kafka.Error:
			log.Printf("Kafka error: %v", ev)
			if kp.metricsEnabled {
				messagesFailed.WithLabelValues(kp.topic, "kafka_error").Inc()
			}
			kp.errorsChan <- ev
		default:
			log.Printf("Kafka event: %v", ev)
		}
	}
	close(kp.errorsChan)
}

// handleErrors обрабатывает ошибки и отправляет сообщения в dead letter queue
func (kp *KafkaProducer) handleErrors() {
	for err := range kp.errorsChan {
		log.Printf("Handling error: %v", err)
		// Здесь можно реализовать дополнительные действия, например, алертинг
	}
}

// Close закрывает producer
func (kp *KafkaProducer) Close() {
	log.Println("Closing Kafka producer...")
	close(kp.shutdownChan)
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
		if kp.metricsEnabled {
			producerHealth.Set(0)
		}
		return fmt.Errorf("failed to get Kafka metadata: %w", err)
	}

	// Проверяем, что наш топик существует
	topicMetadata, ok := metadata.Topics[kp.topic]
	if !ok {
		if kp.metricsEnabled {
			producerHealth.Set(0)
		}
		return fmt.Errorf("topic %s not found in Kafka", kp.topic)
	}

	if len(topicMetadata.Partitions) == 0 {
		if kp.metricsEnabled {
			producerHealth.Set(0)
		}
		return fmt.Errorf("topic %s has no partitions", kp.topic)
	}

	if kp.metricsEnabled {
		producerHealth.Set(1)
	}
	return nil
}

// GetMetrics возвращает метрики производителя (для отладки)
func (kp *KafkaProducer) GetMetrics() map[string]interface{} {
	return map[string]interface{}{
		"topic":              kp.topic,
		"bootstrap_servers":  kp.bootstrapServers,
		"dead_letter_topic":  kp.deadLetterTopic,
		"metrics_enabled":    kp.metricsEnabled,
	}
}