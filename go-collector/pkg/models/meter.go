package models

import "time"

// MeterReading представляет показание счётчика
type MeterReading struct {
	MeterID    string    `json:"meter_id"`
	Timestamp  time.Time `json:"timestamp"`
	Power      float64   `json:"power"` // мощность в кВт·ч
	Validated  bool      `json:"validated"`
}

// AggregatedData представляет агрегированные данные за окно
type AggregatedData struct {
	WindowStart time.Time         `json:"window_start"`
	WindowEnd   time.Time         `json:"window_end"`
	Aggregates  map[string]Aggregate `json:"aggregates"`
}

// Aggregate содержит статистики для одного счётчика
type Aggregate struct {
	Sum   float64 `json:"sum"`
	Avg   float64 `json:"avg"`
	Min   float64 `json:"min"`
	Max   float64 `json:"max"`
	Count int     `json:"count"`
}

// ShardAssignment представляет назначение шардов сборщику
type ShardAssignment struct {
	CollectorID string   `json:"collector_id"`
	Shards      []string `json:"shards"` // список meter_id
}