# Многоступенчатый Dockerfile для Python-обработчика
# Этап 1: Сборка зависимостей
FROM python:3.11-slim AS builder

# Установка системных зависимостей
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Копирование requirements.txt
COPY requirements.txt .

# Установка Python зависимостей в виртуальное окружение
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Этап 2: Финальный образ
FROM python:3.11-slim

# Установка системных зависимостей времени выполнения
RUN apt-get update && apt-get install -y \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Создание пользователя для безопасности
RUN groupadd -r appgroup && useradd -r -g appgroup appuser

WORKDIR /app

# Копирование виртуального окружения из builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Копирование исходного кода Python
COPY . .

# Создание директорий для логов и данных
RUN mkdir -p /var/log/python-processor /data && \
    chown -R appuser:appgroup /app /var/log/python-processor /data

# Переключение на непривилегированного пользователя
USER appuser

# Экспорт портов
# HTTP API
EXPOSE 8000
# Prometheus метрики
EXPOSE 8001

# Переменные окружения по умолчанию
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    LOG_LEVEL=info \
    DATABASE_URL=postgresql://postgres:password@localhost:5432/energy \
    KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
    ARROW_FLIGHT_SERVER=localhost:8815 \
    WINDOW_SIZE_SECONDS=300 \
    SLIDE_INTERVAL_SECONDS=60 \
    API_HOST=0.0.0.0 \
    API_PORT=8000 \
    METRICS_PORT=8001

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Команда запуска
CMD ["python", "main.py"]