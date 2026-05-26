"""
Kafka потребитель для получения потоковых данных от Go-сборщика.
"""

import asyncio
import json
import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from datetime import datetime

from confluent_kafka import Consumer, KafkaError, KafkaException

logger = logging.getLogger(__name__)

@dataclass
class KafkaMessage:
    """Сообщение Kafka."""
    topic: str
    partition: int
    offset: int
    key: Optional[bytes]
    value: Dict[str, Any]
    timestamp: datetime

class KafkaConsumer:
    """Потребитель Kafka для чтения данных от Go-сборщика."""
    
    def __init__(
        self,
        bootstrap_servers: str = 'localhost:9092',
        topics: List[str] = None,
        group_id: str = 'energy-processor',
        auto_offset_reset: str = 'earliest',
        enable_auto_commit: bool = True,
        session_timeout_ms: int = 10000,
        max_poll_interval_ms: int = 300000
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topics = topics or ['meter-readings-raw', 'meter-readings-aggregated']
        self.group_id = group_id
        self.consumer: Optional[Consumer] = None
        self.running = False
        
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
    
    async def consume(self, timeout: float = 1.0) -> List[KafkaMessage]:
        """
        Чтение сообщений из Kafka.
        
        Args:
            timeout: таймаут чтения в секундах
            
        Returns:
            Список сообщений Kafka
        """
        if not self.consumer:
            raise RuntimeError("Consumer not connected")
        
        messages = []
        
        try:
            # Чтение сообщений
            kafka_messages = self.consumer.consume(timeout=timeout)
            
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
                    
                    # Создание объекта сообщения
                    kafka_message = KafkaMessage(
                        topic=msg.topic(),
                        partition=msg.partition(),
                        offset=msg.offset(),
                        key=msg.key(),
                        value=value,
                        timestamp=datetime.fromtimestamp(msg.timestamp()[1] / 1000) if msg.timestamp() else datetime.now()
                    )
                    
                    messages.append(kafka_message)
                    
                    logger.debug(f"Received message from topic {msg.topic()}, partition {msg.partition()}, offset {msg.offset()}")
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse JSON message: {e}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
            
            # Автокоммит offset (если включен)
            if messages and self.config.get('enable.auto.commit', True):
                self.consumer.commit(asynchronous=False)
            
            return messages
            
        except KafkaException as e:
            logger.error(f"Kafka exception: {e}")
            return []
        except Exception as e:
            logger.error(f"Error consuming messages: {e}")
            return []
    
    async def consume_continuously(self, callback):
        """
        Непрерывное чтение сообщений с вызовом callback для каждого сообщения.
        
        Args:
            callback: функция, вызываемая для каждого сообщения
        """
        self.running = True
        logger.info("Starting continuous consumption")
        
        while self.running:
            try:
                messages = await self.consume(timeout=0.5)
                
                for message in messages:
                    await callback(message)
                    
            except Exception as e:
                logger.error(f"Error in continuous consumption: {e}")
                await asyncio.sleep(1)
    
    def get_consumer_metrics(self) -> Dict[str, Any]:
        """Получение метрик потребителя."""
        if not self.consumer:
            return {}
        
        try:
            metrics = self.consumer.list_topics().topics
            return {
                'topics': list(metrics.keys()),
                'consumer_assignment': self.consumer.assignment(),
                'consumer_position': self.consumer.position(self.consumer.assignment()) if self.consumer.assignment() else {}
            }
        except Exception as e:
            logger.error(f"Failed to get consumer metrics: {e}")
            return {}
    
    async def close(self):
        """Закрытие потребителя Kafka."""
        self.running = False
        
        if self.consumer:
            try:
                self.consumer.close()
                logger.info("Kafka consumer closed")
            except Exception as e:
                logger.error(f"Error closing Kafka consumer: {e}")

class AsyncKafkaConsumer(KafkaConsumer):
    """Асинхронная обёртка над Kafka потребителем."""
    
    async def consume_async(self, timeout: float = 1.0) -> List[KafkaMessage]:
        """Асинхронное чтение сообщений."""
        return await asyncio.get_event_loop().run_in_executor(
            None, self.consume, timeout
        )

def test_kafka_consumer():
    """Тестирование потребителя Kafka."""
    import sys
    
    async def test():
        consumer = KafkaConsumer(
            bootstrap_servers='localhost:9092',
            topics=['test-topic'],
            group_id='test-consumer'
        )
        
        try:
            await consumer.connect()
            
            # Чтение нескольких сообщений
            print("Reading messages from Kafka...")
            messages = await consumer.consume(timeout=5.0)
            
            if messages:
                print(f"Received {len(messages)} messages:")
                for msg in messages:
                    print(f"  Topic: {msg.topic}, Value: {msg.value}")
            else:
                print("No messages received")
                
        except Exception as e:
            print(f"Error: {e}")
        finally:
            await consumer.close()
    
    asyncio.run(test())

if __name__ == "__main__":
    test_kafka_consumer()