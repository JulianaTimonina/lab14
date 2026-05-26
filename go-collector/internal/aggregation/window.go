package aggregation

import (
	"sync"
	"time"

	"energy-monitoring-system/go-collector/pkg/models"
)

// WindowAggregator реализует tumbling window агрегацию
type WindowAggregator struct {
	windowSize   time.Duration
	currentWindow *window
	nextWindow   *window
	outputChan   chan models.AggregatedData
	mu           sync.RWMutex
	stopChan     chan struct{}
}

// window представляет одно окно агрегации
type window struct {
	startTime time.Time
	endTime   time.Time
	data      map[string]*aggregate // meter_id -> aggregate
}

// aggregate содержит промежуточные агрегации для одного счётчика
type aggregate struct {
	sum   float64
	min   float64
	max   float64
	count int
}

// NewWindowAggregator создаёт новый агрегатор с заданным размером окна
func NewWindowAggregator(windowSize time.Duration) *WindowAggregator {
	now := time.Now()
	start := now.Truncate(windowSize)

	wa := &WindowAggregator{
		windowSize: windowSize,
		currentWindow: &window{
			startTime: start,
			endTime:   start.Add(windowSize),
			data:      make(map[string]*aggregate),
		},
		outputChan: make(chan models.AggregatedData, 100),
		stopChan:   make(chan struct{}),
	}

	// Запускаем таймер для смены окон
	go wa.windowTimer()

	return wa
}

// AddReading добавляет показание в текущее окно
func (wa *WindowAggregator) AddReading(reading models.MeterReading) {
	wa.mu.Lock()
	defer wa.mu.Unlock()

	// Если время показания выходит за пределы текущего окна,
	// создаём новое окно (на практике это редко, т.к. данные приходят в реальном времени)
	if reading.Timestamp.Before(wa.currentWindow.startTime) {
		// Показание из прошлого - игнорируем
		return
	}
	if reading.Timestamp.After(wa.currentWindow.endTime) || reading.Timestamp.Equal(wa.currentWindow.endTime) {
		// Показание из будущего окна - добавляем в следующее окно
		wa.ensureNextWindow(reading.Timestamp)
		wa.addToWindow(wa.nextWindow, reading)
		return
	}

	// Добавляем в текущее окно
	wa.addToWindow(wa.currentWindow, reading)
}

// addToWindow добавляет показание в конкретное окно
func (wa *WindowAggregator) addToWindow(w *window, reading models.MeterReading) {
	agg, exists := w.data[reading.MeterID]
	if !exists {
		agg = &aggregate{
			min: reading.Power,
			max: reading.Power,
		}
		w.data[reading.MeterID] = agg
	}

	agg.sum += reading.Power
	agg.count++
	if reading.Power < agg.min {
		agg.min = reading.Power
	}
	if reading.Power > agg.max {
		agg.max = reading.Power
	}
}

// ensureNextWindow создаёт следующее окно, если его нет
func (wa *WindowAggregator) ensureNextWindow(timestamp time.Time) {
	if wa.nextWindow != nil {
		return
	}

	start := timestamp.Truncate(wa.windowSize)
	wa.nextWindow = &window{
		startTime: start,
		endTime:   start.Add(wa.windowSize),
		data:      make(map[string]*aggregate),
	}
}

// windowTimer управляет сменой окон по времени
func (wa *WindowAggregator) windowTimer() {
	ticker := time.NewTicker(wa.windowSize)
	defer ticker.Stop()

	for {
		select {
		case <-wa.stopChan:
			return
		case <-ticker.C:
			wa.rotateWindow()
		}
	}
}

// rotateWindow завершает текущее окно и начинает новое
func (wa *WindowAggregator) rotateWindow() {
	wa.mu.Lock()
	defer wa.mu.Unlock()

	// Если в текущем окне есть данные, отправляем их
	if len(wa.currentWindow.data) > 0 {
		aggregated := wa.convertToAggregatedData(wa.currentWindow)
		select {
		case wa.outputChan <- aggregated:
			// Успешно отправлено
		default:
			// Канал заполнен, пропускаем (можно добавить логирование)
		}
	}

	// Переключаем окна
	if wa.nextWindow != nil {
		wa.currentWindow = wa.nextWindow
		wa.nextWindow = nil
	} else {
		// Создаём следующее окно
		nextStart := wa.currentWindow.endTime
		wa.currentWindow = &window{
			startTime: nextStart,
			endTime:   nextStart.Add(wa.windowSize),
			data:      make(map[string]*aggregate),
		}
	}
}

// convertToAggregatedData преобразует внутреннее представление окна в AggregatedData
func (wa *WindowAggregator) convertToAggregatedData(w *window) models.AggregatedData {
	aggregates := make(map[string]models.Aggregate)

	for meterID, agg := range w.data {
		avg := 0.0
		if agg.count > 0 {
			avg = agg.sum / float64(agg.count)
		}

		aggregates[meterID] = models.Aggregate{
			Sum:   agg.sum,
			Avg:   avg,
			Min:   agg.min,
			Max:   agg.max,
			Count: agg.count,
		}
	}

	return models.AggregatedData{
		WindowStart: w.startTime,
		WindowEnd:   w.endTime,
		Aggregates:  aggregates,
	}
}

// OutputChannel возвращает канал для чтения агрегированных данных
func (wa *WindowAggregator) OutputChannel() <-chan models.AggregatedData {
	return wa.outputChan
}

// Stop останавливает агрегатор
func (wa *WindowAggregator) Stop() {
	close(wa.stopChan)
	close(wa.outputChan)
}

// GetCurrentWindowStats возвращает статистику текущего окна (для отладки)
func (wa *WindowAggregator) GetCurrentWindowStats() (int, time.Time, time.Time) {
	wa.mu.RLock()
	defer wa.mu.RUnlock()

	return len(wa.currentWindow.data), wa.currentWindow.startTime, wa.currentWindow.endTime
}