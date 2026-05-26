package aggregation

import (
	"testing"
	"time"

	"energy-monitoring-system/go-collector/pkg/models"
)

func TestWindowAggregator_AddReading(t *testing.T) {
	windowSize := 30 * time.Second
	aggregator := NewWindowAggregator(windowSize)
	defer aggregator.Stop()

	now := time.Now()
	reading := models.MeterReading{
		MeterID:   "meter-001",
		Timestamp: now,
		Power:     150.5,
		Validated: true,
	}

	aggregator.AddReading(reading)

	// Проверяем, что данные добавлены
	count, start, end := aggregator.GetCurrentWindowStats()
	if count != 1 {
		t.Errorf("Expected 1 reading in window, got %d", count)
	}

	// Проверяем, что окно правильно определено
	expectedStart := now.Truncate(windowSize)
	if !start.Equal(expectedStart) {
		t.Errorf("Expected window start %v, got %v", expectedStart, start)
	}

	expectedEnd := expectedStart.Add(windowSize)
	if !end.Equal(expectedEnd) {
		t.Errorf("Expected window end %v, got %v", expectedEnd, end)
	}
}

func TestWindowAggregator_MultipleReadings(t *testing.T) {
	windowSize := 30 * time.Second
	aggregator := NewWindowAggregator(windowSize)
	defer aggregator.Stop()

	now := time.Now()

	// Добавляем несколько показаний для одного счётчика
	readings := []models.MeterReading{
		{MeterID: "meter-001", Timestamp: now, Power: 100.0, Validated: true},
		{MeterID: "meter-001", Timestamp: now.Add(5 * time.Second), Power: 200.0, Validated: true},
		{MeterID: "meter-001", Timestamp: now.Add(10 * time.Second), Power: 300.0, Validated: true},
	}

	for _, reading := range readings {
		aggregator.AddReading(reading)
	}

	// Проверяем агрегированные данные
	select {
	case aggregated := <-aggregator.OutputChannel():
		// Ждём завершения окна
		time.Sleep(windowSize + 100*time.Millisecond)
		
		stats, exists := aggregated.Aggregates["meter-001"]
		if !exists {
			t.Fatal("Expected aggregates for meter-001")
		}

		if stats.Count != 3 {
			t.Errorf("Expected 3 readings, got %d", stats.Count)
		}

		if stats.Sum != 600.0 {
			t.Errorf("Expected sum 600.0, got %f", stats.Sum)
		}

		if stats.Avg != 200.0 {
			t.Errorf("Expected avg 200.0, got %f", stats.Avg)
		}

		if stats.Min != 100.0 {
			t.Errorf("Expected min 100.0, got %f", stats.Min)
		}

		if stats.Max != 300.0 {
			t.Errorf("Expected max 300.0, got %f", stats.Max)
		}
	case <-time.After(windowSize + 2*time.Second):
		t.Fatal("Timeout waiting for aggregated data")
	}
}

func TestWindowAggregator_MultipleMeters(t *testing.T) {
	windowSize := 30 * time.Second
	aggregator := NewWindowAggregator(windowSize)
	defer aggregator.Stop()

	now := time.Now()

	// Добавляем показания для разных счётчиков
	aggregator.AddReading(models.MeterReading{MeterID: "meter-001", Timestamp: now, Power: 100.0, Validated: true})
	aggregator.AddReading(models.MeterReading{MeterID: "meter-002", Timestamp: now, Power: 200.0, Validated: true})
	aggregator.AddReading(models.MeterReading{MeterID: "meter-003", Timestamp: now, Power: 300.0, Validated: true})

	count, _, _ := aggregator.GetCurrentWindowStats()
	if count != 3 {
		t.Errorf("Expected 3 meters in window, got %d", count)
	}
}

func TestWindowAggregator_WindowRotation(t *testing.T) {
	windowSize := 1 * time.Second // Короткое окно для теста
	aggregator := NewWindowAggregator(windowSize)
	defer aggregator.Stop()

	now := time.Now()

	// Добавляем показание в первое окно
	aggregator.AddReading(models.MeterReading{
		MeterID:   "meter-001",
		Timestamp: now,
		Power:     100.0,
		Validated: true,
	})

	// Ждём завершения окна
	time.Sleep(windowSize + 100*time.Millisecond)

	// Добавляем показание во второе окно
	aggregator.AddReading(models.MeterReading{
		MeterID:   "meter-001",
		Timestamp: now.Add(windowSize),
		Power:     200.0,
		Validated: true,
	})

	// Проверяем, что первое окно было отправлено
	select {
	case aggregated := <-aggregator.OutputChannel():
		stats, exists := aggregated.Aggregates["meter-001"]
		if !exists {
			t.Fatal("Expected aggregates for meter-001")
		}

		if stats.Count != 1 {
			t.Errorf("Expected 1 reading in first window, got %d", stats.Count)
		}

		if stats.Sum != 100.0 {
			t.Errorf("Expected sum 100.0 in first window, got %f", stats.Sum)
		}
	case <-time.After(2 * windowSize):
		t.Fatal("Timeout waiting for first window data")
	}

	// Проверяем текущее окно
	count, _, _ := aggregator.GetCurrentWindowStats()
	if count != 1 {
		t.Errorf("Expected 1 reading in current window, got %d", count)
	}
}

func TestWindowAggregator_OutOfOrderReadings(t *testing.T) {
	windowSize := 30 * time.Second
	aggregator := NewWindowAggregator(windowSize)
	defer aggregator.Stop()

	now := time.Now()
	windowStart := now.Truncate(windowSize)

	// Показание из прошлого (должно быть проигнорировано)
	pastReading := models.MeterReading{
		MeterID:   "meter-001",
		Timestamp: windowStart.Add(-1 * time.Second), // За секунду до начала окна
		Power:     50.0,
		Validated: true,
	}

	aggregator.AddReading(pastReading)

	count, _, _ := aggregator.GetCurrentWindowStats()
	if count != 0 {
		t.Errorf("Expected 0 readings for past timestamp, got %d", count)
	}

	// Показание из будущего окна
	futureReading := models.MeterReading{
		MeterID:   "meter-001",
		Timestamp: windowStart.Add(windowSize), // Начало следующего окна
		Power:     250.0,
		Validated: true,
	}

	aggregator.AddReading(futureReading)

	// Проверяем, что показание добавлено в следующее окно
	// (в текущей реализации оно будет добавлено в nextWindow)
	count, _, _ = aggregator.GetCurrentWindowStats()
	if count != 0 {
		t.Errorf("Expected 0 readings in current window for future timestamp, got %d", count)
	}
}

func BenchmarkWindowAggregator_AddReading(b *testing.B) {
	windowSize := 30 * time.Second
	aggregator := NewWindowAggregator(windowSize)
	defer aggregator.Stop()

	now := time.Now()
	reading := models.MeterReading{
		MeterID:   "meter-001",
		Timestamp: now,
		Power:     150.5,
		Validated: true,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reading.Timestamp = now.Add(time.Duration(i) * time.Millisecond)
		aggregator.AddReading(reading)
	}
}

func BenchmarkWindowAggregator_MultipleMeters(b *testing.B) {
	windowSize := 30 * time.Second
	aggregator := NewWindowAggregator(windowSize)
	defer aggregator.Stop()

	now := time.Now()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		meterID := string(rune('A' + (i % 26))) // A-Z
		reading := models.MeterReading{
			MeterID:   "meter-" + meterID,
			Timestamp: now.Add(time.Duration(i) * time.Millisecond),
			Power:     float64(i % 1000),
			Validated: true,
		}
		aggregator.AddReading(reading)
	}
}