# Система анализа энергопотребления

Распределённая система для сбора, агрегации, валидации и визуализации данных со счётчиков электроэнергии.

## Архитектура

Система состоит из следующих компонентов:

1. **Эмуляторы счётчиков** - генерируют тестовые данные
2. **Go-сборщики** - распределённый сбор данных с координацией через etcd
3. **Оконная агрегация** - агрегация данных в реальном времени
4. **Apache Arrow Flight** - эффективная передача данных между Go и Python
5. **Rust-библиотека валидации** - проверка корректности данных
6. **Python-обработчик** - приём и дополнительная обработка данных
7. **Kafka** - потоковая передача данных
8. **Веб-дашборд** - визуализация в реальном времени
9. **Kubernetes** - оркестрация и автоскалирование

## Новые возможности (последние обновления)

- **Улучшенный Kafka producer (Go)**:
  - Метрики Prometheus (счётчики успешных/неуспешных отправок, latency, health)
  - Dead letter queue для неудачных сообщений
  - Экспоненциальный backoff при повторных попытках
  - Расширенный health check

- **Гарантированная обработка сообщений (Python)**:
  - Класс `GuaranteedKafkaConsumer` с ручным управлением offset
  - Обработка с повторными попытками (retry logic)
  - Метрики потребления (lag, throughput)
  - Поддержка dead letter topic

- **Конфигурируемый транспорт**:
  - Поддержка трёх режимов передачи данных: `kafka`, `arrow`, `both`
  - Флаг `--transport` в Go collector для выбора транспорта
  - Совместимость с существующим pipeline

- **Полный Kubernetes stack**:
  - Готовые манифесты для всех компонентов (TimescaleDB, Kafka, etcd, collector, processor, dashboard)
  - Readiness/liveness пробы для автоматического восстановления
  - Horizontal Pod Autoscaling (HPA) для collector и processor

- **Benchmark Go vs Python**:
  - Сравнение производительности оконной агрегации
  - Отчёт с метриками throughput и потребления памяти
  - Go показывает ~54x более высокий throughput и ~24x меньшее потребление памяти

## Быстрый старт

### Предварительные требования

- Go 1.21+
- Python 3.10+
- Rust 1.70+
- Docker 24+
- Kubernetes (minikube/k3s) - опционально
- Apache Arrow 12+

### Запуск локально

1. Клонировать репозиторий:
```bash
git clone <repository-url>
cd energy-monitoring-system
```

2. Запустить инфраструктуру:
```bash
docker-compose up -d
```

3. Запустить эмуляторы счётчиков:
```bash
cd emulators/go/meter-emulator
go run main.go
```

4. Запустить Go-сборщик (с поддержкой конфигурируемого транспорта):
```bash
cd go-collector
go run cmd/collector/main.go --transport=kafka   # или arrow, both
```

5. Запустить Python-обработчик (с гарантированной обработкой сообщений):
```bash
cd python-processor
python main.py
```

6. Запустить веб-дашборд:
```bash
cd web-dashboard
streamlit run app.py
```

## Развёртывание в Kubernetes

```bash
kubectl apply -f k8s/
```

## Мониторинг

- Prometheus: http://localhost:9090 (собирает метрики Kafka producer, consumer, агрегатора)
- Grafana: http://localhost:3000 (дашборды для визуализации метрик)
- Дашборд: http://localhost:8501 (Streamlit-приложение для визуализации данных)

### Метрики Prometheus

- `kafka_messages_produced_total` – количество отправленных сообщений в Kafka
- `kafka_messages_failed_total` – количество неудачных отправок
- `kafka_message_latency_seconds` – гистограмма задержки отправки
- `kafka_producer_health` – здоровье producer (1 = здоров, 0 = нездоров)
- `kafka_consumer_lag` – lag потребителя (только для Python consumer)
- `window_aggregator_throughput` – throughput оконного агрегатора

## Benchmark отчёт

Сравнение производительности Go и Python компонентов доступно в [benchmarks/go-vs-python/benchmark_report.md](benchmarks/go-vs-python/benchmark_report.md).

## Структура проекта

См. [plans/architecture_plan.md](plans/architecture_plan.md) для детального описания архитектуры.

## Лицензия

MIT