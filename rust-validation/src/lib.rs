use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Представление показания счётчика
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeterReading {
    pub meter_id: String,
    pub timestamp: i64, // Unix timestamp в секундах
    pub power: f64,     // мощность в кВт·ч
}

/// Ошибки валидации
#[derive(Debug, Error, PartialEq)]
pub enum ValidationError {
    #[error("Invalid meter ID format: {0}")]
    InvalidMeterId(String),
    
    #[error("Timestamp is in the future: {0}")]
    FutureTimestamp(i64),
    
    #[error("Timestamp is too old: {0}")]
    TooOldTimestamp(i64),
    
    #[error("Power value out of range: {0}. Expected 0-1000")]
    PowerOutOfRange(f64),
    
    #[error("Power value is negative: {0}")]
    NegativePower(f64),
    
    #[error("Power value is NaN")]
    PowerNaN,
}

/// Валидатор показаний счётчиков
pub struct MeterValidator {
    max_power: f64,
    max_age_seconds: i64,
    allow_future: bool,
}

impl Default for MeterValidator {
    fn default() -> Self {
        Self {
            max_power: 1000.0, // максимальная мощность 1000 кВт·ч
            max_age_seconds: 7 * 24 * 3600, // неделя
            allow_future: false,
        }
    }
}

impl MeterValidator {
    /// Создаёт новый валидатор с настройками по умолчанию
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Создаёт валидатор с кастомными настройками
    pub fn with_settings(max_power: f64, max_age_seconds: i64, allow_future: bool) -> Self {
        Self {
            max_power,
            max_age_seconds,
            allow_future,
        }
    }
    
    /// Валидирует показание счётчика
    pub fn validate(&self, reading: &MeterReading) -> Result<(), ValidationError> {
        self.validate_meter_id(&reading.meter_id)?;
        self.validate_timestamp(reading.timestamp)?;
        self.validate_power(reading.power)?;
        Ok(())
    }
    
    /// Валидирует идентификатор счётчика
    fn validate_meter_id(&self, meter_id: &str) -> Result<(), ValidationError> {
        if meter_id.is_empty() {
            return Err(ValidationError::InvalidMeterId("empty".to_string()));
        }
        
        // Простая проверка формата: должен начинаться с "meter-" и иметь цифры
        if !meter_id.starts_with("meter-") {
            return Err(ValidationError::InvalidMeterId(meter_id.to_string()));
        }
        
        // Проверяем, что после "meter-" есть цифры
        let suffix = &meter_id[6..];
        if suffix.is_empty() || !suffix.chars().all(|c| c.is_ascii_digit()) {
            return Err(ValidationError::InvalidMeterId(meter_id.to_string()));
        }
        
        Ok(())
    }
    
    /// Валидирует timestamp
    fn validate_timestamp(&self, timestamp: i64) -> Result<(), ValidationError> {
        let now = chrono::Utc::now().timestamp();
        
        if timestamp > now && !self.allow_future {
            return Err(ValidationError::FutureTimestamp(timestamp));
        }
        
        if timestamp < now - self.max_age_seconds {
            return Err(ValidationError::TooOldTimestamp(timestamp));
        }
        
        Ok(())
    }
    
    /// Валидирует значение мощности
    fn validate_power(&self, power: f64) -> Result<(), ValidationError> {
        if power.is_nan() {
            return Err(ValidationError::PowerNaN);
        }
        
        if power < 0.0 {
            return Err(ValidationError::NegativePower(power));
        }
        
        if power > self.max_power {
            return Err(ValidationError::PowerOutOfRange(power));
        }
        
        Ok(())
    }
    
    /// Валидирует массив показаний, возвращает только валидные
    pub fn validate_batch(&self, readings: &[MeterReading]) -> Vec<MeterReading> {
        readings
            .iter()
            .filter(|reading| self.validate(reading).is_ok())
            .cloned()
            .collect()
    }
    
    /// Валидирует массив показаний, возвращает результаты валидации
    pub fn validate_batch_with_results(&self, readings: &[MeterReading]) -> Vec<Result<MeterReading, ValidationError>> {
        readings
            .iter()
            .map(|reading| self.validate(reading).map(|_| reading.clone()))
            .collect()
    }
}

/// C-совместимый интерфейс для использования из Go через cgo

#[repr(C)]
pub struct CMeterReading {
    meter_id: *const libc::c_char,
    timestamp: libc::int64_t,
    power: libc::c_double,
}

#[repr(C)]
pub struct CValidationResult {
    is_valid: bool,
    error_message: *const libc::c_char,
}

/// Валидирует одно показание через C интерфейс
/// Возвращает 0 если успешно, иначе 1
#[no_mangle]
pub extern "C" fn validate_meter_reading_c(
    reading: *const CMeterReading,
    result: *mut CValidationResult,
) -> libc::c_int {
    if reading.is_null() || result.is_null() {
        return 1;
    }
    
    unsafe {
        let c_reading = &*reading;
        
        // Конвертируем C строку в Rust String
        let meter_id = if c_reading.meter_id.is_null() {
            String::new()
        } else {
            match std::ffi::CStr::from_ptr(c_reading.meter_id).to_str() {
                Ok(s) => s.to_string(),
                Err(_) => return 1,
            }
        };
        
        let rust_reading = MeterReading {
            meter_id,
            timestamp: c_reading.timestamp,
            power: c_reading.power,
        };
        
        let validator = MeterValidator::new();
        match validator.validate(&rust_reading) {
            Ok(_) => {
                (*result).is_valid = true;
                (*result).error_message = std::ptr::null();
                0
            }
            Err(e) => {
                (*result).is_valid = false;
                let error_msg = std::ffi::CString::new(e.to_string()).unwrap();
                (*result).error_message = error_msg.into_raw();
                0
            }
        }
    }
}

/// Освобождает память, выделенную для сообщения об ошибке
#[no_mangle]
pub extern "C" fn free_validation_result(result: *mut CValidationResult) {
    if result.is_null() {
        return;
    }
    
    unsafe {
        if !(*result).error_message.is_null() {
            let _ = std::ffi::CString::from_raw((*result).error_message as *mut _);
            (*result).error_message = std::ptr::null();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_reading() {
        let validator = MeterValidator::new();
        let reading = MeterReading {
            meter_id: "meter-001".to_string(),
            timestamp: chrono::Utc::now().timestamp() - 100,
            power: 150.5,
        };
        
        assert!(validator.validate(&reading).is_ok());
    }
    
    #[test]
    fn test_invalid_meter_id() {
        let validator = MeterValidator::new();
        let reading = MeterReading {
            meter_id: "invalid".to_string(),
            timestamp: chrono::Utc::now().timestamp(),
            power: 150.5,
        };
        
        assert!(matches!(
            validator.validate(&reading),
            Err(ValidationError::InvalidMeterId(_))
        ));
    }
    
    #[test]
    fn test_negative_power() {
        let validator = MeterValidator::new();
        let reading = MeterReading {
            meter_id: "meter-001".to_string(),
            timestamp: chrono::Utc::now().timestamp(),
            power: -10.0,
        };
        
        assert!(matches!(
            validator.validate(&reading),
            Err(ValidationError::NegativePower(_))
        ));
    }
    
    #[test]
    fn test_power_out_of_range() {
        let validator = MeterValidator::with_settings(100.0, 3600, false);
        let reading = MeterReading {
            meter_id: "meter-001".to_string(),
            timestamp: chrono::Utc::now().timestamp(),
            power: 150.0,
        };
        
        assert!(matches!(
            validator.validate(&reading),
            Err(ValidationError::PowerOutOfRange(_))
        ));
    }
    
    #[test]
    fn test_batch_validation() {
        let validator = MeterValidator::new();
        let readings = vec![
            MeterReading {
                meter_id: "meter-001".to_string(),
                timestamp: chrono::Utc::now().timestamp() - 100,
                power: 150.5,
            },
            MeterReading {
                meter_id: "invalid".to_string(),
                timestamp: chrono::Utc::now().timestamp(),
                power: 150.5,
            },
            MeterReading {
                meter_id: "meter-002".to_string(),
                timestamp: chrono::Utc::now().timestamp() - 200,
                power: -10.0,
            },
        ];
        
        let valid = validator.validate_batch(&readings);
        assert_eq!(valid.len(), 1);
        assert_eq!(valid[0].meter_id, "meter-001");
    }
}