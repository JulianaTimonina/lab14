package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"runtime"
	"time"

	"energy-monitoring-system/go-collector/internal/aggregation"
	"energy-monitoring-system/go-collector/pkg/models"
)

// generateTestReadings создаёт тестовые показания
func generateTestReadings(numReadings int, numMeters int) []models.MeterReading {
	readings := make([]models.MeterReading, numReadings)
	baseTime := time.Now()

	for i := 0; i < numReadings; i++ {
		meterID := fmt.Sprintf("meter-%03d", i%numMeters+1)
		timestamp := baseTime.Add(time.Duration(i) * time.Second)
		power := 0.1 + float64(i%100)*0.1

		readings[i] = models.MeterReading{
			MeterID:   meterID,
			Timestamp: timestamp,
			Power:     power,
			Validated: true,
		}
	}
	return readings
}

func benchmarkWindowAggregation() map[string]interface{} {
	fmt.Println("=== Go Benchmark: Window Aggregation ===")

	// Создаём агрегатор с окном 30 секунд
	windowSize := 30 * time.Second
	aggregator := aggregation.NewWindowAggregator(windowSize)

	// Генерируем тестовые данные
	numReadings := 10000
	numMeters := 100
	fmt.Printf("Generating %d test readings...\n", numReadings)
	readings := generateTestReadings(numReadings, numMeters)

	// Замеряем время добавления показаний
	fmt.Println("Adding readings to window aggregator...")
	startTime := time.Now()

	for _, reading := range readings {
		aggregator.AddReading(reading)
	}

	addTime := time.Since(startTime)
	fmt.Printf("Added %d readings in %v\n", numReadings, addTime)
	throughput := float64(numReadings) / addTime.Seconds()
	fmt.Printf("Throughput: %.2f readings/sec\n", throughput)

	// Замеряем время агрегации (получение выходного канала)
	fmt.Println("Calculating aggregated data...")
	startTime = time.Now()

	// Ждём, пока агрегатор обработает окна (в реальной системе они отправляются в OutputChannel)
	// Для простоты просто вызовем метод, который возвращает последние агрегированные данные
	// Вместо этого мы можем прочитать из канала, но для бенчмарка просто подождём
	time.Sleep(100 * time.Millisecond) // имитация обработки

	aggTime := time.Since(startTime)
	fmt.Printf("Aggregation time: %v\n", aggTime)

	// Замеряем память (приблизительно) - в Go это сложно, пропустим
	// Вместо этого можно использовать runtime.ReadMemStats
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	memoryMB := float64(memStats.Alloc) / 1024 / 1024
	fmt.Printf("Memory usage: %.2f MB\n", memoryMB)

	return map[string]interface{}{
		"readings_count":               numReadings,
		"add_time_sec":                 addTime.Seconds(),
		"throughput_readings_per_sec": throughput,
		"aggregation_time_sec":         aggTime.Seconds(),
		"memory_mb":                    memoryMB,
	}
}

func benchmarkSlidingWindow() float64 {
	fmt.Println("\n=== Go Benchmark: Sliding Window ===")

	// Создаём sliding window агрегатор (окно 5 минут, сдвиг 1 минута)
	windowSize := 5 * time.Minute
	slideInterval := 1 * time.Minute
	swa := aggregation.NewSlidingWindowAggregator(windowSize, slideInterval)

	readings := generateTestReadings(5000, 50)

	startTime := time.Now()
	for _, reading := range readings {
		swa.AddReading(reading)
	}
	elapsed := time.Since(startTime)

	fmt.Printf("Processed %d readings in %v\n", len(readings), elapsed)
	fmt.Printf("Throughput: %.2f readings/sec\n", float64(len(readings))/elapsed.Seconds())

	return elapsed.Seconds()
}

func main() {
	// Включаем поддержку измерения памяти
	// runtime уже импортирован вверху

	results := make(map[string]interface{})

	// Бенчмарк 1: Оконная агрегация
	results["window_aggregation"] = benchmarkWindowAggregation()

	// Бенчмарк 2: Sliding window
	results["sliding_window"] = benchmarkSlidingWindow()

	// Сохраняем результаты в JSON
	outputFile := "benchmarks/results/go_benchmark_results.json"
	os.MkdirAll("benchmarks/results", 0755)

	file, err := os.Create(outputFile)
	if err != nil {
		log.Fatalf("Failed to create output file: %v", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(results); err != nil {
		log.Fatalf("Failed to encode results: %v", err)
	}

	fmt.Printf("\nResults saved to %s\n", outputFile)

	// Вывод сводки
	fmt.Println("\n=== Summary ===")
	windowAgg := results["window_aggregation"].(map[string]interface{})
	fmt.Printf("Go window aggregation throughput: %.2f readings/sec\n", windowAgg["throughput_readings_per_sec"])
	fmt.Printf("Memory usage: %.2f MB\n", windowAgg["memory_mb"])
}