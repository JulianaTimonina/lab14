#!/usr/bin/env python3
"""
Arrow Flight сервер для приёма агрегированных данных от Go-сборщика.
"""

import asyncio
import logging
import sys
import os
from typing import Dict, Any

import pyarrow as pa
import pyarrow.flight as flight
import pyarrow.ipc as ipc

# Добавляем путь для импорта локальных модулей
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from python_processor.main import EnergyProcessor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AggregatedDataFlightServer(flight.FlightServerBase):
    """Сервер Arrow Flight для приёма агрегированных данных."""
    
    def __init__(self, host: str = "localhost", port: int = 8815,
                 processor: EnergyProcessor = None):
        location = flight.Location.for_grpc_tcp(host, port)
        super().__init__(location)
        self.host = host
        self.port = port
        self.processor = processor
        self.schema = self._create_schema()
        logger.info(f"AggregatedDataFlightServer initialized on {host}:{port}")
    
    def _create_schema(self) -> pa.Schema:
        """Создаёт схему Arrow для агрегированных данных."""
        return pa.schema([
            pa.field("window_start", pa.timestamp('ns')),
            pa.field("window_end", pa.timestamp('ns')),
            pa.field("meter_id", pa.string()),
            pa.field("sum", pa.float64()),
            pa.field("avg", pa.float64()),
            pa.field("min", pa.float64()),
            pa.field("max", pa.float64()),
            pa.field("count", pa.int64()),
        ])
    
    async def do_put(self, context, descriptor, reader, writer):
        """Обрабатывает входящие данные (DoPut)."""
        try:
            # Читаем данные из потока
            table = await reader.read_all()
            logger.info(f"Received aggregated data: {table.num_rows} rows")
            
            # Преобразуем в список словарей для обработки
            for batch in table.to_batches():
                df = batch.to_pandas()
                for _, row in df.iterrows():
                    aggregated_data = {
                        'window_start': row['window_start'].to_pydatetime(),
                        'window_end': row['window_end'].to_pydatetime(),
                        'meter_id': row['meter_id'],
                        'sum': float(row['sum']),
                        'avg': float(row['avg']),
                        'min': float(row['min']),
                        'max': float(row['max']),
                        'count': int(row['count']),
                    }
                    # Сохраняем в базу данных через processor
                    if self.processor:
                        asyncio.create_task(
                            self.processor._save_aggregated_reading(aggregated_data)
                        )
                    else:
                        logger.warning("Processor not available, data not saved")
            
            # Отправляем подтверждение
            writer.write(pa.table({"status": ["success"]}))
            logger.info("Successfully processed aggregated data")
            
        except Exception as e:
            logger.error(f"Error processing DoPut: {e}")
            raise
    
    async def get_flight_info(self, context, descriptor):
        """Возвращает информацию о доступных данных."""
        endpoint = flight.FlightEndpoint(
            ticket=flight.Ticket(b"aggregated-data"),
            location=[self.location]
        )
        
        return flight.FlightInfo(
            schema=self.schema,
            descriptor=descriptor,
            endpoints=[endpoint],
            total_records=-1,
            total_bytes=-1
        )
    
    async def do_get(self, context, ticket):
        """Возвращает данные по запросу (DoGet)."""
        # В реальной системе здесь можно возвращать исторические данные
        # Для простоты возвращаем пустую таблицу
        empty_table = pa.table({
            "window_start": pa.array([], type=pa.timestamp('ns')),
            "window_end": pa.array([], type=pa.timestamp('ns')),
            "meter_id": pa.array([], type=pa.string()),
            "sum": pa.array([], type=pa.float64()),
            "avg": pa.array([], type=pa.float64()),
            "min": pa.array([], type=pa.float64()),
            "max": pa.array([], type=pa.float64()),
            "count": pa.array([], type=pa.int64()),
        })
        
        return flight.RecordBatchStream(empty_table)
    
    async def list_flights(self, context, criteria):
        """Возвращает список доступных потоков данных."""
        descriptor = flight.FlightDescriptor.for_path("aggregated-data")
        endpoint = flight.FlightEndpoint(
            ticket=flight.Ticket(b"aggregated-data"),
            location=[self.location]
        )
        
        yield flight.FlightInfo(
            schema=self.schema,
            descriptor=descriptor,
            endpoints=[endpoint],
            total_records=-1,
            total_bytes=-1
        )
    
    async def list_actions(self, context):
        """Возвращает список доступных действий."""
        return [
            ("health", "Проверка здоровья сервера"),
            ("shutdown", "Завершение работы сервера"),
        ]
    
    async def do_action(self, context, action):
        """Выполняет действие."""
        if action.type == "health":
            yield pa.table({"status": ["healthy"]})
        elif action.type == "shutdown":
            logger.info("Shutdown action received")
            yield pa.table({"status": ["shutting down"]})
            # В реальной системе здесь нужно корректно завершить сервер
        else:
            raise flight.FlightUnimplementedError(
                f"Unknown action: {action.type}"
            )


async def run_server(host: str = "localhost", port: int = 8815,
                     processor: EnergyProcessor = None):
    """Запускает сервер Arrow Flight."""
    server = AggregatedDataFlightServer(host, port, processor)
    
    logger.info(f"Starting Arrow Flight server on {host}:{port}")
    await server.serve()
    return server


async def main():
    """Основная функция для запуска сервера."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Arrow Flight Server')
    parser.add_argument('--host', default='localhost', help='Host to bind')
    parser.add_argument('--port', type=int, default=8815, help='Port to bind')
    args = parser.parse_args()
    
    # Запускаем сервер без processor (можно передать позже)
    server = await run_server(args.host, args.port)
    
    # Бесконечное ожидание (сервер работает в фоне)
    await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())