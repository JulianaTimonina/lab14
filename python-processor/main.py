#!/usr/bin/env python3
"""
Python-обработчик для системы анализа энергопотребления.

Получает данные от Go-сборщиков через Apache Arrow Flight или Kafka,
обрабатывает их и сохраняет в базу данных.
"""

import asyncio
import logging
import signal
import sys
import os
import traceback
from typing import Dict, List, Optional
from datetime import datetime, timedelta

import pyarrow as pa
import pyarrow.flight as flight
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from arrow_client import ArrowFlightClient
from kafka_consumer import KafkaConsumer
from window_processor import WindowProcessor

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class EnergyProcessor:
    """Основной класс обработчика данных."""
    
    def __init__(self, config: Dict):
        self.config = config
        self.running = False
        
        # Инициализация компонентов
        self.arrow_client = None
        self.arrow_server = None
        self.kafka_consumer = None
        self.window_processor = None
        self.db_connection = None
        
        # Очереди для данных
        self.raw_data_queue = asyncio.Queue(maxsize=10000)
        self.aggregated_data_queue = asyncio.Queue(maxsize=1000)
        
    async def initialize(self):
        """Инициализация всех компонентов."""
        logger.info("Initializing Energy Processor...")
        
        # Инициализация подключения к базе данных
        await self._init_database()
        
        # Инициализация Arrow Flight клиента
        if self.config.get('arrow_enabled', True):
            self.arrow_client = ArrowFlightClient(
                host=self.config.get('arrow_host', 'localhost'),
                port=self.config.get('arrow_port', 8815)
            )
            await self.arrow_client.connect()
            logger.info("Arrow Flight client initialized")
        
        # Инициализация Arrow Flight сервера (для приёма данных от Go-сборщика)
        if self.config.get('arrow_server_enabled', False):
            # Импортируем здесь, чтобы избежать циклических зависимостей
            from arrow_server import AggregatedDataFlightServer
            arrow_server_host = self.config.get('arrow_server_host', 'localhost')
            arrow_server_port = self.config.get('arrow_server_port', 8815)
            self.arrow_server = AggregatedDataFlightServer(
                host=arrow_server_host,
                port=arrow_server_port,
                processor=self
            )
            logger.info(f"Arrow Flight server initialized on {arrow_server_host}:{arrow_server_port}")
        
        # Инициализация Kafka потребителя
        if self.config.get('kafka_enabled', True):
            self.kafka_consumer = KafkaConsumer(
                bootstrap_servers=self.config.get('kafka_bootstrap_servers', 'localhost:9092'),
                topics=self.config.get('kafka_topics', ['meter-readings-raw', 'meter-readings-aggregated']),
                group_id=self.config.get('kafka_group_id', 'energy-processor')
            )
            logger.info("Kafka consumer initialized")
        
        # Инициализация оконного процессора
        self.window_processor = WindowProcessor(
            window_size_seconds=self.config.get('window_size_seconds', 300),  # 5 минут
            slide_interval_seconds=self.config.get('slide_interval_seconds', 60)  # 1 минута
        )
        logger.info("Window processor initialized")
        
    async def _init_database(self):
        """Инициализация подключения к базе данных."""
        try:
            self.db_connection = psycopg2.connect(
                host=self.config.get('db_host', 'localhost'),
                port=self.config.get('db_port', 5432),
                database=self.config.get('db_name', 'energy'),
                user=self.config.get('db_user', 'postgres'),
                password=self.config.get('db_password', 'password')
            )
            
            # Создание таблиц если их нет
            with self.db_connection.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS meter_readings (
                        id BIGSERIAL,
                        meter_id VARCHAR(50) NOT NULL,
                        timestamp TIMESTAMPTZ NOT NULL,
                        power DOUBLE PRECISION NOT NULL,
                        validated BOOLEAN DEFAULT FALSE,
                        received_at TIMESTAMPTZ DEFAULT NOW()
                    );
                    
                    CREATE TABLE IF NOT EXISTS aggregated_readings (
                        id BIGSERIAL,
                        window_start TIMESTAMPTZ NOT NULL,
                        window_end TIMESTAMPTZ NOT NULL,
                        meter_id VARCHAR(50) NOT NULL,
                        sum_power DOUBLE PRECISION,
                        avg_power DOUBLE PRECISION,
                        min_power DOUBLE PRECISION,
                        max_power DOUBLE PRECISION,
                        count_readings INTEGER,
                        computed_at TIMESTAMPTZ DEFAULT NOW()
                    );
                    
                    -- Создание hypertable для TimescaleDB
                    SELECT create_hypertable('meter_readings', 'timestamp', if_not_exists => TRUE);
                    SELECT create_hypertable('aggregated_readings', 'window_start', if_not_exists => TRUE);
                """)
                self.db_connection.commit()
                
            logger.info("Database connection established and tables created")
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            logger.error(traceback.format_exc())
            raise
    
    async def start(self):
        """Запуск обработчика."""
        self.running = True
        logger.info("Starting Energy Processor...")
        
        # Запуск задач
        tasks = []
        
        if self.arrow_client:
            tasks.append(asyncio.create_task(self._receive_arrow_data()))
        
        if self.arrow_server:
            # Запуск сервера Arrow Flight в фоне
            tasks.append(asyncio.create_task(self.arrow_server.serve()))
        
        if self.kafka_consumer:
            tasks.append(asyncio.create_task(self._receive_kafka_data()))
        
        tasks.append(asyncio.create_task(self._process_raw_data()))
        tasks.append(asyncio.create_task(self._process_aggregated_data()))
        tasks.append(asyncio.create_task(self._monitor_processing()))
        
        # Ожидание завершения задач
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("Processing tasks cancelled")
        except Exception as e:
            logger.error(f"Error in processing tasks: {e}")
    
    async def _receive_arrow_data(self):
        """Получение данных через Arrow Flight."""
        logger.info("Starting Arrow Flight data receiver")
        
        while self.running:
            try:
                # Получение данных от Arrow сервера
                reader = await self.arrow_client.get_data()
                
                # Чтение данных из RecordBatch
                table = reader.read_all()
                df = table.to_pandas()
                
                # Добавление данных в очередь
                for _, row in df.iterrows():
                    await self.raw_data_queue.put({
                        'meter_id': row['meter_id'],
                        'timestamp': row['timestamp'],
                        'power': row['power'],
                        'source': 'arrow'
                    })
                
                logger.debug(f"Received {len(df)} records via Arrow Flight")
                
            except Exception as e:
                logger.error(f"Error receiving Arrow data: {e}")
                await asyncio.sleep(1)
    
    async def _receive_kafka_data(self):
        """Получение данных через Kafka."""
        logger.info("Starting Kafka data receiver")
        
        while self.running:
            try:
                # Получение сообщений из Kafka
                messages = await self.kafka_consumer.consume()
                
                for message in messages:
                    # Парсинг сообщения (предполагаем JSON формат)
                    data = message.value
                    if isinstance(data, dict):
                        await self.raw_data_queue.put({
                            'meter_id': data.get('meter_id'),
                            'timestamp': datetime.fromisoformat(data.get('timestamp')),
                            'power': data.get('power'),
                            'source': 'kafka'
                        })
                
            except Exception as e:
                logger.error(f"Error receiving Kafka data: {e}")
                await asyncio.sleep(1)
    
    async def _process_raw_data(self):
        """Обработка сырых данных."""
        logger.info("Starting raw data processor")
        
        while self.running:
            try:
                # Получение данных из очереди
                data = await self.raw_data_queue.get()
                
                # Валидация данных
                if not self._validate_reading(data):
                    logger.warning(f"Invalid reading: {data}")
                    continue
                
                # Сохранение в базу данных
                await self._save_raw_reading(data)
                
                # Добавление в оконный процессор
                self.window_processor.add_reading(data)
                
                # Отметка задачи как выполненной
                self.raw_data_queue.task_done()
                
            except Exception as e:
                logger.error(f"Error processing raw data: {e}")
    
    async def _process_aggregated_data(self):
        """Обработка агрегированных данных."""
        logger.info("Starting aggregated data processor")
        
        while self.running:
            try:
                # Получение агрегированных данных из оконного процессора
                aggregated = self.window_processor.get_aggregated_data()
                
                if aggregated:
                    # Сохранение в базу данных
                    await self._save_aggregated_reading(aggregated)
                    
                    # Отправка в очередь для дальнейшей обработки
                    await self.aggregated_data_queue.put(aggregated)
                    
                    logger.info(f"Processed aggregated window: {aggregated['window_start']} - {aggregated['window_end']}")
                
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"Error processing aggregated data: {e}")
    
    async def _save_raw_reading(self, reading: Dict):
        """Сохранение сырого показания в базу данных."""
        try:
            with self.db_connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO meter_readings (meter_id, timestamp, power, validated)
                    VALUES (%s, %s, %s, %s)
                """, (
                    reading['meter_id'],
                    reading['timestamp'],
                    reading['power'],
                    True  # предполагаем, что данные уже валидированы
                ))
                self.db_connection.commit()
        except Exception as e:
            logger.error(f"Failed to save raw reading: {e}")
            self.db_connection.rollback()
    
    async def _save_aggregated_reading(self, aggregated: Dict):
        """Сохранение агрегированных данных в базу данных."""
        try:
            with self.db_connection.cursor() as cursor:
                for meter_id, stats in aggregated['aggregates'].items():
                    cursor.execute("""
                        INSERT INTO aggregated_readings 
                        (window_start, window_end, meter_id, sum_power, avg_power, min_power, max_power, count_readings)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        aggregated['window_start'],
                        aggregated['window_end'],
                        meter_id,
                        stats['sum'],
                        stats['avg'],
                        stats['min'],
                        stats['max'],
                        stats['count']
                    ))
                self.db_connection.commit()
        except Exception as e:
            logger.error(f"Failed to save aggregated reading: {e}")
            self.db_connection.rollback()
    
    def _validate_reading(self, reading: Dict) -> bool:
        """Простая валидация показания."""
        try:
            # Проверка обязательных полей
            if not reading.get('meter_id'):
                return False
            
            if not reading.get('timestamp'):
                return False
            
            power = reading.get('power')
            if power is None or not isinstance(power, (int, float)):
                return False
            
            # Проверка диапазона мощности
            if power < 0 or power > 1000:
                return False
            
            # Проверка формата meter_id
            if not reading['meter_id'].startswith('meter-'):
                return False
            
            return True
            
        except Exception:
            return False
    
    async def _monitor_processing(self):
        """Мониторинг обработки данных."""
        logger.info("Starting processing monitor")
        
        while self.running:
            try:
                queue_size = self.raw_data_queue.qsize()
                if queue_size > 5000:
                    logger.warning(f"Raw data queue is large: {queue_size} items")
                
                await asyncio.sleep(10)
                
            except Exception as e:
                logger.error(f"Error in monitoring: {e}")
                await asyncio.sleep(10)
    
    async def stop(self):
        """Остановка обработчика."""
        self.running = False
        logger.info("Stopping Energy Processor...")
        
        # Закрытие соединений
        if self.db_connection:
            self.db_connection.close()
        
        if self.arrow_client:
            await self.arrow_client.close()
        
        logger.info("Energy Processor stopped")

def load_config() -> Dict:
    """Загрузка конфигурации из переменных окружения с fallback на значения по умолчанию."""
    # Параметры базы данных
    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = int(os.getenv('DB_PORT', '5432'))
    db_name = os.getenv('DB_NAME', 'energy')
    db_user = os.getenv('DB_USER', 'postgres')
    db_password = os.getenv('DB_PASSWORD', 'password')
    
    # Параметры Kafka
    kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    
    return {
        'arrow_enabled': os.getenv('ARROW_ENABLED', 'True').lower() in ('true', '1', 'yes'),
        'arrow_host': os.getenv('ARROW_HOST', 'localhost'),
        'arrow_port': int(os.getenv('ARROW_PORT', '8815')),
        'arrow_server_enabled': os.getenv('ARROW_SERVER_ENABLED', 'False').lower() in ('true', '1', 'yes'),
        'arrow_server_host': os.getenv('ARROW_SERVER_HOST', 'localhost'),
        'arrow_server_port': int(os.getenv('ARROW_SERVER_PORT', '8815')),
        'kafka_enabled': os.getenv('KAFKA_ENABLED', 'True').lower() in ('true', '1', 'yes'),
        'kafka_bootstrap_servers': kafka_bootstrap_servers,
        'kafka_topics': os.getenv('KAFKA_TOPICS', 'meter-readings-raw,meter-readings-aggregated').split(','),
        'kafka_group_id': os.getenv('KAFKA_GROUP_ID', 'energy-processor'),
        'window_size_seconds': int(os.getenv('WINDOW_SIZE_SECONDS', '300')),
        'slide_interval_seconds': int(os.getenv('SLIDE_INTERVAL_SECONDS', '60')),
        'db_host': db_host,
        'db_port': db_port,
        'db_name': db_name,
        'db_user': db_user,
        'db_password': db_password
    }

async def main():
    """Основная функция."""
    config = load_config()
    processor = EnergyProcessor(config)
    
    # Обработка сигналов завершения
    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}, shutting down...")
        asyncio.create_task(processor.stop())
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        await processor.initialize()
        await processor.start()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())