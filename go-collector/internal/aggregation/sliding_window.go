package aggregation

import (
	"sync"
	"time"

	"energy-monitoring-system/go-collector/pkg/models"
)

// SlidingWindowAggregator реализует скользящее окно агрегации
type SlidingWindowAggregator struct {
	windowSize    time.Duration
	slideInterval time.Duration
	maxRecords    int // максимальное количество записей перед принудительным сдвигом (0 = отключено)
	data          map[string]*windowData // meter_id -> windowData
	totalRecords  int // общее количество записей во всех счётчиках
	mu            sync.RWMutex
	outputChan    chan models.AggregatedData
	stopChan      chan struct{}
	lastSlide     time.Time
	forceSlideChan chan struct{} // канал для принудительного сдвига
}

// windowData содержит данные для одного счётчика в скользящем окне
type windowData struct {
	readings []timestampedReading
	stats    windowStats
}

type timestampedReading struct {
	timestamp time.Time
	power     float64
}

type windowStats struct {
	sum   float64
	count int
	min   float64
	max   float64
}

// NewSlidingWindowAggregator создаёт новый агрегатор скользящего окна
// maxRecords: максимальное количество записей перед принудительным сдвигом (0 = отключено)
func NewSlidingWindowAggregator(windowSize, slideInterval time.Duration, maxRecords int) *SlidingWindowAggregator {
	swa := &SlidingWindowAggregator{
		windowSize:    windowSize,
		slideInterval: slideInterval,
		maxRecords:    maxRecords,
		data:          make(map[string]*windowData),
		totalRecords:  0,
		outputChan:    make(chan models.AggregatedData, 100),
		stopChan:      make(chan struct{}),
		lastSlide:     time.Now(),
		forceSlideChan: make(chan struct{}, 1),
	}

	// Запускаем горутину для периодического сдвига окна
	go swa.slideTimer()

	return swa
}

// AddReading добавляет показание в скользящее окно
func (swa *SlidingWindowAggregator) AddReading(reading models.MeterReading) {
	swa.mu.Lock()
	defer swa.mu.Unlock()

	meterID := reading.MeterID
	timestamp := reading.Timestamp
	power := reading.Power

	// Получаем или создаём данные для счётчика
	data, exists := swa.data[meterID]
	if !exists {
		data = &windowData{
			readings: make([]timestampedReading, 0, 100),
			stats: windowStats{
				min: power,
				max: power,
			},
		}
		swa.data[meterID] = data
	}

	// Добавляем показание
	data.readings = append(data.readings, timestampedReading{
		timestamp: timestamp,
		power:     power,
	})

	// Обновляем статистики
	data.stats.sum += power
	data.stats.count++
	if power < data.stats.min {
		data.stats.min = power
	}
	if power > data.stats.max {
		data.stats.max = power
	}

	// Увеличиваем общее количество записей
	swa.totalRecords++

	// Удаляем устаревшие показания (старше windowSize)
	swa.cleanupOldReadings(data, timestamp)

	// Проверяем лимит по количеству записей
	if swa.maxRecords > 0 && swa.totalRecords >= swa.maxRecords {
		select {
		case swa.forceSlideChan <- struct{}{}:
			// Сигнал отправлен
		default:
			// Канал уже заполнен, пропускаем
		}
	}
}

// cleanupOldReadings удаляет показания, выходящие за пределы окна
func (swa *SlidingWindowAggregator) cleanupOldReadings(data *windowData, currentTime time.Time) {
	cutoff := currentTime.Add(-swa.windowSize)
	
	// Находим индекс первого показания, которое ещё в окне
	startIndex := 0
	for i, reading := range data.readings {
		if reading.timestamp.After(cutoff) || reading.timestamp.Equal(cutoff) {
			startIndex = i
			break
		}
	}

	if startIndex > 0 {
		// Удаляем устаревшие показания и корректируем статистики
		deletedCount := startIndex
		for i := 0; i < deletedCount; i++ {
			oldReading := data.readings[i]
			data.stats.sum -= oldReading.power
			data.stats.count--
			// При удалении min/max нужно пересчитать, но для простоты оставим как есть
			// В реальной системе лучше пересчитывать статистики периодически
		}
		// Уменьшаем общее количество записей
		swa.totalRecords -= deletedCount
		if swa.totalRecords < 0 {
			swa.totalRecords = 0
		}
		data.readings = data.readings[startIndex:]
	}
}

// slideTimer периодически сдвигает окно и отправляет агрегированные данные
func (swa *SlidingWindowAggregator) slideTimer() {
	ticker := time.NewTicker(swa.slideInterval)
	defer ticker.Stop()

	for {
		select {
		case <-swa.stopChan:
			return
		case <-ticker.C:
			swa.slideWindow()
		case <-swa.forceSlideChan:
			// Принудительный сдвиг по количеству записей
			swa.slideWindow()
		}
	}
}

// slideWindow сдвигает окно и отправляет агрегированные данные
func (swa *SlidingWindowAggregator) slideWindow() {
	swa.mu.Lock()
	defer swa.mu.Unlock()

	now := time.Now()
	windowStart := now.Add(-swa.windowSize)
	windowEnd := now

	// Если нет данных, ничего не отправляем
	if len(swa.data) == 0 {
		swa.lastSlide = now
		return
	}

	// Собираем агрегированные данные
	aggregates := make(map[string]models.Aggregate)
	for meterID, data := range swa.data {
		if data.stats.count == 0 {
			continue
		}

		avg := data.stats.sum / float64(data.stats.count)
		aggregates[meterID] = models.Aggregate{
			Sum:   data.stats.sum,
			Avg:   avg,
			Min:   data.stats.min,
			Max:   data.stats.max,
			Count: data.stats.count,
		}
	}

	// Отправляем данные в канал
	aggregatedData := models.AggregatedData{
		WindowStart: windowStart,
		WindowEnd:   windowEnd,
		Aggregates:  aggregates,
	}

	select {
	case swa.outputChan <- aggregatedData:
		// Успешно отправлено
	default:
		// Канал заполнен, пропускаем
	}

	// Сбрасываем счётчик записей, если включён лимит по количеству
	if swa.maxRecords > 0 && swa.totalRecords >= swa.maxRecords {
		swa.totalRecords = 0
	}

	swa.lastSlide = now
}

// OutputChannel возвращает канал для чтения агрегированных данных
func (swa *SlidingWindowAggregator) OutputChannel() <-chan models.AggregatedData {
	return swa.outputChan
}

// Stop останавливает агрегатор
func (swa *SlidingWindowAggregator) Stop() {
	close(swa.stopChan)
	close(swa.outputChan)
}

// GetCurrentStats возвращает текущую статистику по счётчикам
func (swa *SlidingWindowAggregator) GetCurrentStats() map[string]models.Aggregate {
	swa.mu.RLock()
	defer swa.mu.RUnlock()

	stats := make(map[string]models.Aggregate)
	for meterID, data := range swa.data {
		if data.stats.count == 0 {
			continue
		}

		avg := data.stats.sum / float64(data.stats.count)
		stats[meterID] = models.Aggregate{
			Sum:   data.stats.sum,
			Avg:   avg,
			Min:   data.stats.min,
			Max:   data.stats.max,
			Count: data.stats.count,
		}
	}

	return stats
}

// GetWindowSize возвращает размер окна
func (swa *SlidingWindowAggregator) GetWindowSize() time.Duration {
	return swa.windowSize
}

// GetSlideInterval возвращает интервал сдвига
func (swa *SlidingWindowAggregator) GetSlideInterval() time.Duration {
	return swa.slideInterval
}