"""
Клиент Apache Arrow Flight для получения данных от Go-сборщика.
"""

import asyncio
import logging
from typing import Optional

import pyarrow as pa
import pyarrow.flight as flight

logger = logging.getLogger(__name__)

class ArrowFlightClient:
    """Клиент для подключения к Arrow Flight серверу."""
    
    def __init__(self, host: str = 'localhost', port: int = 8815):
        self.host = host
        self.port = port
        self.client: Optional[flight.FlightClient] = None
        self.location: Optional[flight.Location] = None
    
    async def connect(self):
        """Подключение к Arrow Flight серверу."""
        try:
            self.location = flight.Location.for_grpc_tcp(self.host, self.port)
            self.client = flight.FlightClient(self.location)
            logger.info(f"Connected to Arrow Flight server at {self.host}:{self.port}")
        except Exception as e:
            logger.error(f"Failed to connect to Arrow Flight server: {e}")
            raise
    
    async def get_data(self, ticket_data: bytes = b'aggregated-data') -> flight.FlightStreamReader:
        """
        Получение данных от сервера.
        
        Args:
            ticket_data: данные билета для запроса
            
        Returns:
            FlightStreamReader для чтения данных
        """
        if not self.client:
            raise RuntimeError("Client not connected")
        
        try:
            # Создание билета
            ticket = flight.Ticket(ticket_data)
            
            # Получение потока данных
            reader = self.client.do_get(ticket)
            logger.debug(f"Received data stream from Arrow Flight server")
            
            return reader
            
        except Exception as e:
            logger.error(f"Failed to get data from Arrow Flight server: {e}")
            raise
    
    async def get_schema(self) -> pa.Schema:
        """Получение схемы данных от сервера."""
        if not self.client:
            raise RuntimeError("Client not connected")
        
        try:
            # Получение информации о доступных данных
            descriptor = flight.FlightDescriptor.for_path("aggregated-data")
            flight_info = self.client.get_flight_info(descriptor)
            
            # Десериализация схемы
            schema = flight_info.schema
            logger.debug(f"Received schema from Arrow Flight server")
            
            return schema
            
        except Exception as e:
            logger.error(f"Failed to get schema from Arrow Flight server: {e}")
            raise
    
    async def list_flights(self):
        """Получение списка доступных потоков данных."""
        if not self.client:
            raise RuntimeError("Client not connected")
        
        try:
            flights = list(self.client.list_flights())
            logger.info(f"Available flights: {len(flights)}")
            
            for flight_info in flights:
                logger.debug(f"Flight: {flight_info.descriptor}")
                
            return flights
            
        except Exception as e:
            logger.error(f"Failed to list flights: {e}")
            raise
    
    async def close(self):
        """Закрытие соединения с сервером."""
        if self.client:
            self.client.close()
            logger.info("Arrow Flight client closed")

class AsyncArrowFlightClient(ArrowFlightClient):
    """Асинхронная версия клиента Arrow Flight."""
    
    async def get_data_async(self, ticket_data: bytes = b'aggregated-data') -> flight.FlightStreamReader:
        """Асинхронное получение данных."""
        return await asyncio.get_event_loop().run_in_executor(
            None, self.get_data, ticket_data
        )
    
    async def get_schema_async(self) -> pa.Schema:
        """Асинхронное получение схемы."""
        return await asyncio.get_event_loop().run_in_executor(
            None, self.get_schema
        )

def test_arrow_client():
    """Тестирование клиента Arrow Flight."""
    import sys
    
    async def test():
        client = ArrowFlightClient('localhost', 8815)
        try:
            await client.connect()
            
            # Получение схемы
            schema = await client.get_schema()
            print(f"Schema: {schema}")
            
            # Получение данных
            reader = await client.get_data()
            table = reader.read_all()
            print(f"Received table with {table.num_rows} rows")
            
            # Конвертация в pandas DataFrame
            df = table.to_pandas()
            print(f"DataFrame shape: {df.shape}")
            print(df.head())
            
        except Exception as e:
            print(f"Error: {e}")
        finally:
            await client.close()
    
    asyncio.run(test())

if __name__ == "__main__":
    test_arrow_client()