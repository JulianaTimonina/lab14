# Многоступенчатый Dockerfile для Go-сборщика
# Этап 1: Сборка Go приложения
FROM golang:1.21-alpine AS builder

# Установка зависимостей для сборки
RUN apk add --no-cache \
    git \
    make \
    gcc \
    musl-dev \
    pkgconfig \
    libc6-compat

# Установка Rust и Cargo для сборки Rust библиотеки валидации
RUN apk add --no-cache \
    rust \
    cargo \
    cmake

WORKDIR /app

# Копирование go.mod и go.sum
COPY go.mod go.sum ./
RUN go mod download

# Копирование исходного кода Rust библиотеки
COPY rust-validation ./rust-validation

# Сборка Rust библиотеки
WORKDIR /app/rust-validation
RUN cargo build --release

# Возвращаемся в корневую директорию приложения
WORKDIR /app

# Копирование остального исходного кода
COPY . .

# Сборка Go приложения с линковкой Rust библиотеки
RUN CGO_ENABLED=1 \
    GOOS=linux \
    GOARCH=amd64 \
    go build -ldflags="-s -w" -o /app/bin/collector ./cmd/collector

# Этап 2: Создание финального образа
FROM alpine:3.18

# Установка зависимостей времени выполнения
RUN apk add --no-cache \
    ca-certificates \
    tzdata \
    libc6-compat \
    && update-ca-certificates

# Создание пользователя для безопасности
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /app

# Копирование бинарника из builder
COPY --from=builder /app/bin/collector ./collector

# Копирование Rust библиотеки
COPY --from=builder /app/rust-validation/target/release/libenergy_validation.so /usr/local/lib/

# Копирование конфигурационных файлов
COPY go-collector/config ./config

# Установка прав
RUN chown -R appuser:appgroup /app

# Переключение на непривилегированного пользователя
USER appuser

# Экспорт портов
EXPOSE 8080  # HTTP порт для health checks
EXPOSE 9090  # Prometheus метрики
EXPOSE 8815  # Arrow Flight RPC

# Переменные окружения по умолчанию
ENV LOG_LEVEL=info \
    ETCD_ENDPOINTS=localhost:2379 \
    METER_EMULATOR_URL=http://localhost:8080 \
    KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
    ARROW_FLIGHT_SERVER_ENABLED=true \
    ARROW_FLIGHT_SERVER_PORT=8815 \
    WINDOW_SIZE_SECONDS=30 \
    RUST_LIBRARY_PATH=/usr/local/lib/libenergy_validation.so

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health || exit 1

# Команда запуска
CMD ["./collector"]