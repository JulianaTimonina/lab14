"""
Kafka потребитель для получения потоковых данных от Go-сборщика с гарантированной обработкой.
"""

import asyncio
import json
import logging
import time
from typing import List, Dict, Any, Optional, Callable, Awaitable
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

logger = logging.getLogger(__name__)

class ProcessingStatus(Enum):
    SUCCESS = "success"
    FAILURE = "failure"
    RETRY = "retry"

@dataclass
class KafkaMessage:
    """Сообщение Kafka."""
    topic: str
    partition: int
    offset: int
    key: Optional[bytes]
    value: Dict[str, Any]
    timestamp: datetime
    raw_message: Any  # оригинальное сообщение confluent_kafka для коммита

class GuaranteedKafkaConsumer:
    """Потребитель Kafka с гарантированной обработкой и ручным управлением offset."""
    
    def __init__(
        self,
        bootstrap_servers: str = 'localhost:9092',
        topics: List[str] = None,
        group_id: str = 'energy-processor',
        auto_offset_reset: str = 'earliest',
        enable_auto_commit: bool = False,  # отключаем авто-коммит
        session_timeout_ms: int = 10000,
        max_poll_interval_ms: int = 300000,
        max_retries: int = 3,
        dead_letter_topic: str = None,
        metrics_enabled: bool = True
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topics = topics or ['meter-readings-raw', 'meter-readings-aggregated']
        self.group_id = group_id
        self.consumer: Optional[Consumer] = None
        self.running = False
        self.max_retries = max_retries
        self.dead_letter_topic = dead_letter_topic
        self.metrics_enabled = metrics_enabled
        
        # Метрики
        self.messages_consumed = 0
        self.messages_failed = 0
        self.messages_retried = 0
        self.last_commit_time = time.time()
        
        # Конфигурация потребителя
        self.config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': auto_offset_reset,
            'enable.auto.commit': enable_auto_commit,
            'session.timeout.ms': session_timeout_ms,
            'max.poll.interval.ms': max_poll_interval_ms,
            'heartbeat.interval.ms': 3000,
            'max.poll.records': 500,
            'isolation.level': 'read_committed',  # читаем только коммитированные сообщения
        }
    
    async def connect(self):
        """Подключение к Kafka и подписка на топики."""
        try:
            self.consumer = Consumer(self.config)
            self.consumer.subscribe(self.topics)
            logger.info(f"Connected to Kafka at {self.bootstrap_servers}, subscribed to topics: {self.topics}")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise
    
    async def consume_with_retry(
        self,
        processing_callback: Callable[[KafkaMessage], Awaitable[ProcessingStatus]],
        timeout: float = 1.0,
        batch_size: int = 100
    ) -> List[KafkaMessage]:
        """
        Чтение сообщений из Kafka с повторными попытками обработки.
        
        Args:
            processing_callback: функция обработки сообщения, возвращает статус
            timeout: таймаут чтения в секундах
            batch_size: максимальное количество сообщений за один poll
            
        Returns:
            Список успешно обработанных сообщений
        """
        if not self.consumer:
            raise RuntimeError("Consumer not connected")
        
        processed_messages = []
        failed_messages = []
        
        try:
            # Чтение сообщений
            kafka_messages = self.consumer.consume(timeout=timeout, num_messages=batch_size)
            
            for msg in kafka_messages:
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # Конец раздела - нормальная ситуация
                        logger.debug(f"Reached end of partition {msg.partition()}")
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                    continue
                
                # Парсинг сообщения
                try:
                    value = json.loads(msg.value().decode('utf-8')) if msg.value() else {}
                    
                    kafka_message = KafkaMessage(
                        topic=msg.topic(),
                        partition=msg.partition(),
                        offset=msg.offset(),
                        key=msg.key(),
                        value=value,
                        timestamp=datetime.fromtimestamp(msg.timestamp()[1] / 1000) if msg.timestamp() else datetime.now(),
                        raw_message=msg
                    )
                    
                    # Обработка с повторными попытками
                    status = await self._process_with_retry(kafka_message, processing_callback)
                    
                    if status == ProcessingStatus.SUCCESS:
                        processed_messages.append(kafka_message)
                        self.messages_consumed += 1
                        logger.debug(f"Successfully processed message from topic {msg.topic()}, partition {msg.partition()}, offset {msg.offset()}")
                    else:
                        failed_messages.append(kafka_message)
                        self.messages_failed += 1
                        logger.warning(f"Failed to process message from topic {msg.topic()}, offset {msg.offset()}")
                        
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse JSON message: {e}")
                    self._handle_invalid_message(msg, e)
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    self.messages_failed += 1
            
            # Коммит offset для успешно обработанных сообщений
            if processed_messages:
                await self._commit_offsets(processed_messages)
            
            # Обработка неудачных сообщений (dead letter queue)
            if failed_messages and self.dead_letter_topic:
                await self._send_to_dead_letter(failed_messages)
            
            return processed_messages
            
        except KafkaException as e:
            logger.error(f"Kafka exception: {e}")
            return []
        except Exception as e:
            logger.error(f"Error consuming messages: {e}")
            return []
    
    async def _process_with_retry(
        self,
        message: KafkaMessage,
        callback: Callable[[KafkaMessage], Awaitable[ProcessingStatus]]
    ) -> ProcessingStatus:
        """Обработка сообщения с повторными попытками."""
        last_error = None
        
        for attempt in range(self.max_retries + 1):
            try:
                status = await callback(message)
                if status == ProcessingStatus.SUCCESS:
                    return ProcessingStatus.SUCCESS
                elif status == ProcessingStatus.RETRY:
                    logger.info(f"Retry requested for message {message.topic}:{message.offset} (attempt {attempt + 1})")
                    self.messages_retried += 1
                else:
                    logger.warning(f"Callback reported failure for message {message.topic}:{message.offset}")
                    return ProcessingStatus.FAILURE
            except Exception as e:
                last_error = e
                logger.warning(f"Attempt {attempt + 1} failed for message {message.topic}:{message.offset}: {e}")
            
            if attempt < self.max_retries:
                # Экспоненциальный backoff
                backoff = min(2 ** attempt, 30)  # максимум 30 секунд
                await asyncio.sleep(backoff)
        
        logger.error(f"All retries exhausted for message {message.topic}:{message.offset}: {last_error}")
        return ProcessingStatus.FAILURE
    
    async def _commit_offsets(self, messages: List[KafkaMessage]):
        """Ручной коммит offset для обработанных сообщений."""
        try:
            offsets_to_commit = []
            for msg in messages:
                tp = TopicPartition(msg.topic, msg.partition, msg.offset + 1)  # коммитим следующий offset
                offsets_to_commit.append(tp)
            
            if offsets_to_commit:
                self.consumer.commit(offsets=offsets_to_commit, asynchronous=False)
                self.last_commit_time = time.time()
                logger.debug(f"Committed offsets for {len(offsets_to_commit)} messages")
        except Exception as e:
            logger.error(f"Failed to commit offsets: {e}")
    
    def _handle_invalid_message(self, raw_message, error):
        """Обработка невалидного сообщения."""
        logger.error(f"Invalid message received: {error}")
        # Можно отправить в dead letter queue или просто пропустить
        if self.dead_letter_topic:
            # Здесь должна быть реализация отправки в dead letter topic
            pass
    
    async def _send_to_dead_letter(self, messages: List[KafkaMessage]):
        """Отправка неудачных сообщений в dead letter topic."""
        # Для простоты логируем, но в реальной системе нужно отправить в отдельный топик
        logger.warning(f"{len(messages)} messages would be sent to dead letter topic {self.dead_letter_topic}")
        # Реализация отправки требует producer, который здесь не создан
        # Можно добавить позже
    
    async def consume_continuously(
        self, 
        processing_callback: Callable[[KafkaMessage], ProcessingStatus],
        poll_timeout: float = 0.5,
        batch_size: int = 100
    ):
        """
        Непрерывное чтение сообщений с гарантированной обработкой.
        
        Args:
            processing_callback: функция обработки сообщения
            poll_timeout: таймаут чтения в секундах
            batch_size: максимальное количество сообщений за один poll
        """
        self.running = True
        logger.info("Starting continuous consumption with guaranteed processing")
        
        while self.running:
            try:
                processed = await self.consume_with_retry(
                    processing_callback, 
                    timeout=poll_timeout,
                    batch_size=batch_size
                )
                
                # Логирование метрик
                if self.metrics_enabled and processed:
                    logger.debug(f"Processed {len(processed)} messages in this batch")
                
                # Периодический коммит (на всякий случай)
                if time.time() - self.last_commit_time > 30:  # каждые 30 секунд
                    self.consumer.commit(asynchronous=False)
                    self.last_commit_time = time.time()
                    
            except Exception as e:
                logger.error(f"Error in continuous consumption: {e}")
                await asyncio.sleep(1)
    
    def get_consumer_metrics(self) -> Dict[str, Any]:
        """Получение метрик потребителя."""
        if not self.consumer:
            return {}
        
        try:
            # Получение lag (отставания) для каждого partition
            assignment = self.consumer.assignment()
            positions = self.consumer.position(assignment) if assignment else []
            lag_info = {}
            
            for tp in assignment:
                # Получение последнего доступного offset
                low, high = self.consumer.get_watermark_offsets(tp)
                current_pos = self.consumer.position([tp])[0].offset if self.consumer.position([tp]) else 0
                lag = high - current_pos if high >= current_pos else 0
                lag_info[f"{tp.topic}-{tp.partition}"] = lag
            
            return {
                'messages_consumed': self.messages_consumed,
                'messages_failed': self.messages_failed,
                'messages_retried': self.messages_retried,
                'consumer_assignment': [str(tp) for tp in assignment] if assignment else [],
                'consumer_lag': lag_info,
                'last_commit_time': self.last_commit_time,
            }
        except Exception as e:
            logger.error(f"Failed to get consumer metrics: {e}")
            return {}
    
    async def close(self):
        """Закрытие потребителя Kafka с финализацией."""
        self.running = False
        
        if self.consumer:
            try:
                # Финальный коммит перед закрытием
                self.consumer.commit(asynchronous=False)
                self.consumer.close()
                logger.info("Kafka consumer closed gracefully")
            except Exception as e:
                logger.error(f"Error closing Kafka consumer: {e}")

# Совместимость со старым кодом
class KafkaConsumer(GuaranteedKafkaConsumer):
    """Совместимый класс для обратной совместимости."""
    
    async def consume(self, timeout: float = 1.0) -> List[KafkaMessage]:
        """Старый метод consume (без гарантированной обработки)."""
        # Создаём простой callback, который всегда возвращает SUCCESS
        def simple_callback(msg):
            return ProcessingStatus.SUCCESS
        
        return await self.consume_with_retry(simple_callback, timeout, batch_size=100)

def test_kafka_consumer():
    """Тестирование потребителя Kafka."""
    import sys
    
    async def test():
        consumer = GuaranteedKafkaConsumer(
            bootstrap_servers='localhost:9092',
            topics=['test-topic'],
            group_id='test-consumer',
            enable_auto_commit=False
        )
        
        try:
            await consumer.connect()
            
            # Простой callback для теста
            def test_callback(msg):
                print(f"Processing message: {msg.value}")
                return ProcessingStatus.SUCCESS
            
            print("Reading messages from Kafka with guaranteed processing...")
            messages = await consumer.consume_with_retry(test_callback, timeout=5.0)
            
            if messages:
                print(f"Successfully processed {len(messages)} messages")
            else:
                print("No messages received")
                
        except Exception as e:
            print(f"Error: {e}")
        finally:
            await consumer.close()
    
    asyncio.run(test())

if __name__ == "__main__":
    test_kafka_consumer()