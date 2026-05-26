#!/bin/bash

# Скрипт для создания топиков Kafka для системы анализа энергопотребления

KAFKA_BROKER="localhost:9092"

# Функция для создания топика
create_topic() {
    local topic_name=$1
    local partitions=$2
    local replication_factor=$3
    
    echo "Creating topic: $topic_name"
    
    docker exec kafka kafka-topics \
        --bootstrap-server $KAFKA_BROKER \
        --create \
        --topic $topic_name \
        --partitions $partitions \
        --replication-factor $replication_factor \
        --config retention.ms=604800000 \  # 7 дней
        --config cleanup.policy=delete
    
    if [ $? -eq 0 ]; then
        echo "Topic $topic_name created successfully"
    else
        echo "Failed to create topic $topic_name"
    fi
}

# Ожидание доступности Kafka
echo "Waiting for Kafka to be ready..."
until docker exec kafka kafka-topics --bootstrap-server $KAFKA_BROKER --list > /dev/null 2>&1; do
    echo "Kafka is not ready yet, waiting..."
    sleep 5
done

echo "Kafka is ready, creating topics..."

# Создание топиков

# 1. Сырые данные от счётчиков
create_topic "meter-readings-raw" 3 1

# 2. Валидированные данные
create_topic "meter-readings-validated" 3 1

# 3. Агрегированные данные (tumbling window)
create_topic "meter-readings-aggregated-30s" 3 1

# 4. Агрегированные данные (sliding window)
create_topic "meter-readings-aggregated-5min" 3 1

# 5. Ошибки валидации
create_topic "meter-readings-validation-errors" 1 1

# 6. Команды управления
create_topic "meter-commands" 1 1

# 7. Мониторинг и метрики
create_topic "energy-metrics" 3 1

# 8. Оповещения
create_topic "energy-alerts" 1 1

# Вывод списка топиков
echo ""
echo "Listing all topics:"
docker exec kafka kafka-topics \
    --bootstrap-server $KAFKA_BROKER \
    --list

echo ""
echo "Topic details:"
for topic in meter-readings-raw meter-readings-validated meter-readings-aggregated-30s meter-readings-aggregated-5min; do
    echo ""
    echo "Details for topic: $topic"
    docker exec kafka kafka-topics \
        --bootstrap-server $KAFKA_BROKER \
        --describe \
        --topic $topic
done

echo ""
echo "All topics created successfully!"