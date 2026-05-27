package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"energy-monitoring-system/go-collector/internal/aggregation"
	"energy-monitoring-system/go-collector/internal/arrowflight"
	"energy-monitoring-system/go-collector/internal/etcd-coordination"
	"energy-monitoring-system/go-collector/internal/kafka"
	"energy-monitoring-system/go-collector/pkg/models"
)

const (
	etcdEndpoints        = "http://localhost:2379"
	meterEmulatorURL     = "http://localhost:8080"
	totalShards          = 100
	kafkaBootstrap       = "localhost:9092"
	kafkaTopicRaw        = "meter-readings-raw"
	kafkaTopicAggregated = "meter-readings-aggregated"
	arrowFlightServer    = "localhost:8815"
)

func main() {
	// Парсинг флагов командной строки
	var transport string
	flag.StringVar(&transport, "transport", "kafka", "Transport to use for sending aggregated data (kafka, arrow, both)")
	flag.Parse()

	// Генерируем ID сборщика (можно использовать hostname + pid)
	hostname, _ := os.Hostname()
	collectorID := fmt.Sprintf("%s-%d", hostname, os.Getpid())

	log.Printf("Starting collector %s with transport=%s", collectorID, transport)

	// Инициализируем координатор etcd
	coordinator, err := etcdcoordination.NewCoordinator([]string{etcdEndpoints}, collectorID)
	if err != nil {
		log.Fatalf("Failed to create coordinator: %v", err)
	}
	defer coordinator.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Регистрируем сборщик в etcd
	if err := coordinator.Register(ctx); err != nil {
		log.Fatalf("Failed to register collector: %v", err)
	}

	// Получаем список всех шардов (счётчиков)
	allShards := generateAllShards(totalShards)

	// Перераспределяем шарды между всеми сборщиками
	if err := coordinator.RebalanceShards(ctx, allShards); err != nil {
		log.Printf("Warning: failed to rebalance shards: %v", err)
	}

	// Получаем назначенные шарды
	assignedShards := coordinator.GetAssignedShards()
	log.Printf("Assigned shards: %v", assignedShards)

	// Создаём агрегатор окон
	windowSize := 30 * time.Second
	aggregator := aggregation.NewWindowAggregator(windowSize)

	// Инициализируем транспорты в зависимости от флага
	var kafkaProducer *kafka.KafkaProducer
	var arrowClient *arrowflight.ArrowFlightClient

	if transport == "kafka" || transport == "both" {
		// Инициализируем Kafka producer для агрегированных данных
		kafkaProducer, err = kafka.NewKafkaProducer(kafkaBootstrap, kafkaTopicAggregated)
		if err != nil {
			log.Printf("Warning: failed to create Kafka producer: %v. Continuing without Kafka.", err)
		} else {
			defer kafkaProducer.Close()
			log.Println("Kafka producer initialized successfully")
		}
	}

	if transport == "arrow" || transport == "both" {
		// Инициализируем Arrow Flight клиент
		arrowClient, err = arrowflight.NewArrowFlightClient(arrowFlightServer, 10*time.Second)
		if err != nil {
			log.Printf("Warning: failed to create Arrow Flight client: %v. Continuing without Arrow.", err)
		} else {
			defer arrowClient.Close()
			log.Println("Arrow Flight client initialized successfully")
		}
	}

	// Запускаем сбор данных
	dataChan := make(chan models.MeterReading, 1000)
	go collectData(ctx, assignedShards, dataChan)

	// Запускаем обработку данных
	go processData(ctx, dataChan, aggregator)

	// Запускаем отправку агрегированных данных
	go sendAggregatedData(ctx, aggregator, kafkaProducer, arrowClient)

	// Отслеживаем изменения в назначении шардов
	coordinator.WatchShardsChanges(ctx, func(shards []string) {
		log.Printf("Shards assignment changed: %v", shards)
		// Здесь можно перезапустить сбор данных с новыми шардами
	})

	// Ожидаем сигналов завершения
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down collector...")
}

// generateAllShards генерирует список всех шардов (счётчиков)
func generateAllShards(count int) []string {
	shards := make([]string, count)
	for i := 0; i < count; i++ {
		shards[i] = fmt.Sprintf("meter-%03d", i+1)
	}
	return shards
}

// collectData собирает данные с назначенных счётчиков
func collectData(ctx context.Context, shards []string, dataChan chan<- models.MeterReading) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, meterID := range shards {
				reading, err := fetchMeterReading(meterID)
				if err != nil {
					log.Printf("Failed to fetch reading for %s: %v", meterID, err)
					continue
				}
				dataChan <- reading
			}
		}
	}
}

// fetchMeterReading получает показание счётчика от эмулятора
func fetchMeterReading(meterID string) (models.MeterReading, error) {
	url := fmt.Sprintf("%s/reading/%s", meterEmulatorURL, meterID)
	resp, err := http.Get(url)
	if err != nil {
		return models.MeterReading{}, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return models.MeterReading{}, fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	var reading struct {
		MeterID   string  `json:"meter_id"`
		Timestamp int64   `json:"timestamp"`
		Power     float64 `json:"power"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&reading); err != nil {
		return models.MeterReading{}, fmt.Errorf("failed to decode JSON: %w", err)
	}

	return models.MeterReading{
		MeterID:   reading.MeterID,
		Timestamp: time.Unix(reading.Timestamp, 0),
		Power:     reading.Power,
		Validated: false,
	}, nil
}

// processData обрабатывает сырые данные и добавляет их в агрегатор
func processData(ctx context.Context, dataChan <-chan models.MeterReading, aggregator *aggregation.WindowAggregator) {
	for {
		select {
		case <-ctx.Done():
			return
		case reading := <-dataChan:
			// Здесь можно добавить валидацию через Rust библиотеку
			reading.Validated = true // временно
			aggregator.AddReading(reading)
		}
	}
}

// sendAggregatedData отправляет агрегированные данные
func sendAggregatedData(ctx context.Context, aggregator *aggregation.WindowAggregator, kafkaProducer *kafka.KafkaProducer, arrowClient *arrowflight.ArrowFlightClient) {
	for {
		select {
		case <-ctx.Done():
			return
		case aggregated := <-aggregator.OutputChannel():
			// Отправляем агрегированные данные через Arrow Flight или Kafka
			log.Printf("Aggregated data window %s - %s: %d counters",
				aggregated.WindowStart.Format(time.RFC3339),
				aggregated.WindowEnd.Format(time.RFC3339),
				len(aggregated.Aggregates))

			// Отправляем через Kafka, если producer доступен
			if kafkaProducer != nil {
				if err := kafkaProducer.SendAggregatedData(aggregated); err != nil {
					log.Printf("Failed to send aggregated data to Kafka: %v", err)
				} else {
					log.Printf("Successfully sent aggregated data to Kafka")
				}
			}

			// Отправляем через Arrow Flight, если клиент доступен
			if arrowClient != nil {
				if err := arrowClient.SendAggregatedData(aggregated); err != nil {
					log.Printf("Failed to send aggregated data via Arrow Flight: %v", err)
				} else {
					log.Printf("Successfully sent aggregated data via Arrow Flight")
				}
			}
		}
	}
}
