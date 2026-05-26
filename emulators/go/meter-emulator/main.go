package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"
)

// MeterReading представляет показание счётчика
type MeterReading struct {
	MeterID    string  `json:"meter_id"`
	Timestamp  int64   `json:"timestamp"`
	Power      float64 `json:"power"` // мощность в кВт·ч
}

// MeterEmulator эмулирует работу счётчика
type MeterEmulator struct {
	MeterID string
	MinPower float64
	MaxPower float64
}

// NewMeterEmulator создаёт новый эмулятор
func NewMeterEmulator(meterID string, minPower, maxPower float64) *MeterEmulator {
	return &MeterEmulator{
		MeterID:  meterID,
		MinPower: minPower,
		MaxPower: maxPower,
	}
}

// GenerateReading генерирует случайное показание
func (m *MeterEmulator) GenerateReading() MeterReading {
	power := m.MinPower + rand.Float64()*(m.MaxPower-m.MinPower)
	return MeterReading{
		MeterID:   m.MeterID,
		Timestamp: time.Now().Unix(),
		Power:     power,
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())

	// Создаём 100 эмуляторов счётчиков
	emulators := make([]*MeterEmulator, 100)
	for i := 0; i < 100; i++ {
		meterID := fmt.Sprintf("meter-%03d", i+1)
		// Случайный диапазон мощности: 0.1 - 10 кВт·ч
		minPower := 0.1 + rand.Float64()*2.0
		maxPower := minPower + 5.0 + rand.Float64()*5.0
		emulators[i] = NewMeterEmulator(meterID, minPower, maxPower)
	}

	// HTTP обработчик для получения показаний всех счётчиков
	http.HandleFunc("/readings", func(w http.ResponseWriter, r *http.Request) {
		readings := make([]MeterReading, 100)
		for i, emulator := range emulators {
			readings[i] = emulator.GenerateReading()
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(readings)
	})

	// HTTP обработчик для получения показаний конкретного счётчика
	http.HandleFunc("/reading/", func(w http.ResponseWriter, r *http.Request) {
		meterID := r.URL.Path[len("/reading/"):]
		for _, emulator := range emulators {
			if emulator.MeterID == meterID {
				reading := emulator.GenerateReading()
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(reading)
				return
			}
		}
		http.Error(w, "Meter not found", http.StatusNotFound)
	})

	// Статус
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	port := ":8080"
	log.Printf("Starting meter emulator on port %s", port)
	log.Printf("Available endpoints:")
	log.Printf("  GET /readings - all meter readings")
	log.Printf("  GET /reading/{meter_id} - specific meter reading")
	log.Printf("  GET /health - health check")

	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatal(err)
	}
}