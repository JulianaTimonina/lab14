package validation

/*
#cgo LDFLAGS: -L${SRCDIR}/../../../rust-validation/target/release -lenergy_validation -lm -ldl
#include "rust-validation/cgo-binding/validation.h"
*/
import "C"
import (
	"fmt"
	"time"
	"unsafe"

	"energy-monitoring-system/go-collector/pkg/models"
)

// RustValidator использует Rust библиотеку для валидации данных
type RustValidator struct{}

// NewRustValidator создаёт новый валидатор
func NewRustValidator() *RustValidator {
	return &RustValidator{}
}

// Validate проверяет показание счётчика с помощью Rust библиотеки
func (v *RustValidator) Validate(reading models.MeterReading) (bool, string) {
	// Конвертируем Go структуру в C структуру
	cReading := C.CMeterReading{
		meter_id:  C.CString(reading.MeterID),
		timestamp: C.int64_t(reading.Timestamp.Unix()),
		power:     C.double(reading.Power),
	}
	defer C.free(unsafe.Pointer(cReading.meter_id))

	var cResult C.CValidationResult
	defer C.free_validation_result(&cResult)

	// Вызываем Rust функцию
	ret := C.validate_meter_reading_c(&cReading, &cResult)
	if ret != 0 {
		return false, "Failed to call validation function"
	}

	isValid := bool(cResult.is_valid)
	errorMsg := ""
	if !isValid && cResult.error_message != nil {
		errorMsg = C.GoString(cResult.error_message)
	}

	return isValid, errorMsg
}

// ValidateBatch проверяет массив показаний
func (v *RustValidator) ValidateBatch(readings []models.MeterReading) ([]models.MeterReading, []error) {
	validReadings := make([]models.MeterReading, 0, len(readings))
	errors := make([]error, 0, len(readings))

	for _, reading := range readings {
		if isValid, errMsg := v.Validate(reading); isValid {
			reading.Validated = true
			validReadings = append(validReadings, reading)
		} else {
			errors = append(errors, fmt.Errorf("validation failed for meter %s: %s", reading.MeterID, errMsg))
		}
	}

	return validReadings, errors
}

// ValidateWithRetry проверяет с повторными попытками
func (v *RustValidator) ValidateWithRetry(reading models.MeterReading, maxRetries int) (bool, string) {
	for i := 0; i < maxRetries; i++ {
		if isValid, errMsg := v.Validate(reading); isValid {
			return true, ""
		} else if i == maxRetries-1 {
			return false, errMsg
		}
		time.Sleep(time.Duration(i*100) * time.Millisecond)
	}
	return false, "max retries exceeded"
}

// SimpleGoValidator - альтернативная реализация на чистом Go для сравнения
type SimpleGoValidator struct {
	MaxPower       float64
	MaxAgeSeconds  int64
	AllowFuture    bool
}

// NewSimpleGoValidator создаёт простой валидатор на Go
func NewSimpleGoValidator() *SimpleGoValidator {
	return &SimpleGoValidator{
		MaxPower:      1000.0,
		MaxAgeSeconds: 7 * 24 * 3600,
		AllowFuture:   false,
	}
}

// Validate проверяет показание с помощью Go
func (v *SimpleGoValidator) Validate(reading models.MeterReading) (bool, string) {
	// Проверка meter_id
	if len(reading.MeterID) == 0 {
		return false, "empty meter ID"
	}
	if len(reading.MeterID) < 7 || reading.MeterID[:6] != "meter-" {
		return false, fmt.Sprintf("invalid meter ID format: %s", reading.MeterID)
	}

	// Проверка timestamp
	now := time.Now().Unix()
	timestamp := reading.Timestamp.Unix()
	
	if timestamp > now && !v.AllowFuture {
		return false, fmt.Sprintf("timestamp is in the future: %d", timestamp)
	}
	
	if timestamp < now-v.MaxAgeSeconds {
		return false, fmt.Sprintf("timestamp is too old: %d", timestamp)
	}

	// Проверка power
	if reading.Power < 0 {
		return false, fmt.Sprintf("negative power: %f", reading.Power)
	}
	
	if reading.Power > v.MaxPower {
		return false, fmt.Sprintf("power out of range: %f", reading.Power)
	}

	return true, ""
}