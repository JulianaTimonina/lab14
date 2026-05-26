-- Схема базы данных для системы анализа энергопотребления
-- Используется PostgreSQL с расширением TimescaleDB (опционально)

-- Таблица сырых показаний счётчиков
CREATE TABLE IF NOT EXISTS meter_readings (
    id BIGSERIAL PRIMARY KEY,
    meter_id VARCHAR(50) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    power DOUBLE PRECISION NOT NULL,
    validated BOOLEAN DEFAULT FALSE,
    received_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Индексы для ускорения запросов
    CONSTRAINT unique_meter_timestamp UNIQUE (meter_id, timestamp)
);

-- Таблица агрегированных данных (оконная агрегация)
CREATE TABLE IF NOT EXISTS aggregated_readings (
    id BIGSERIAL PRIMARY KEY,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    meter_id VARCHAR(50) NOT NULL,
    sum_power DOUBLE PRECISION,
    avg_power DOUBLE PRECISION,
    min_power DOUBLE PRECISION,
    max_power DOUBLE PRECISION,
    count_readings INTEGER,
    computed_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Индексы
    CONSTRAINT unique_meter_window UNIQUE (meter_id, window_start, window_end)
);

-- Таблица для метрик системы (мониторинг)
CREATE TABLE IF NOT EXISTS system_metrics (
    id BIGSERIAL PRIMARY KEY,
    metric_name VARCHAR(100) NOT NULL,
    metric_value DOUBLE PRECISION NOT NULL,
    labels JSONB,
    timestamp TIMESTAMPTZ DEFAULT NOW()
);

-- Таблица для ошибок валидации
CREATE TABLE IF NOT EXISTS validation_errors (
    id BIGSERIAL PRIMARY KEY,
    meter_id VARCHAR(50),
    timestamp TIMESTAMPTZ,
    power DOUBLE PRECISION,
    error_message TEXT,
    received_at TIMESTAMPTZ DEFAULT NOW()
);

-- Создание hypertable для TimescaleDB (если расширение установлено)
-- Раскомментировать если используется TimescaleDB
/*
SELECT create_hypertable('meter_readings', 'timestamp', if_not_exists => TRUE);
SELECT create_hypertable('aggregated_readings', 'window_start', if_not_exists => TRUE);
SELECT create_hypertable('system_metrics', 'timestamp', if_not_exists => TRUE);
*/

-- Индексы для улучшения производительности
CREATE INDEX IF NOT EXISTS idx_meter_readings_timestamp ON meter_readings (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_meter_readings_meter_id ON meter_readings (meter_id);
CREATE INDEX IF NOT EXISTS idx_aggregated_readings_window ON aggregated_readings (window_start DESC, window_end DESC);
CREATE INDEX IF NOT EXISTS idx_aggregated_readings_meter_id ON aggregated_readings (meter_id);
CREATE INDEX IF NOT EXISTS idx_system_metrics_name_time ON system_metrics (metric_name, timestamp DESC);

-- Представление для удобного доступа к последним данным
CREATE OR REPLACE VIEW latest_aggregated_data AS
SELECT 
    meter_id,
    window_start,
    window_end,
    avg_power,
    sum_power,
    count_readings,
    computed_at
FROM aggregated_readings
WHERE window_start >= NOW() - INTERVAL '1 hour'
ORDER BY window_start DESC;

-- Функция для очистки старых данных (удерживать 30 дней)
CREATE OR REPLACE FUNCTION cleanup_old_data(retention_days INTEGER DEFAULT 30)
RETURNS VOID AS $$
BEGIN
    DELETE FROM meter_readings WHERE timestamp < NOW() - (retention_days || ' days')::INTERVAL;
    DELETE FROM aggregated_readings WHERE window_start < NOW() - (retention_days || ' days')::INTERVAL;
    DELETE FROM system_metrics WHERE timestamp < NOW() - (retention_days || ' days')::INTERVAL;
    DELETE FROM validation_errors WHERE received_at < NOW() - (retention_days || ' days')::INTERVAL;
END;
$$ LANGUAGE plpgsql;