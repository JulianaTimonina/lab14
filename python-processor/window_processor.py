"""
Оконный процессор для агрегации данных в реальном времени.
Реализует скользящее окно для вычисления статистик.
"""

import asyncio
import logging
import threading
import time
from typing import Dict, List, Optional, Any
from collections import defaultdict
from datetime import datetime, timedelta
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

@dataclass
class Reading:
    """Показание счётчика."""
    meter_id: str
    timestamp: datetime
    power: float
    validated: bool = True

@dataclass
class WindowStats:
    """Статистики для одного счётчика в окне."""
    sum_power: float = 0.0
    count: int = 0
    min_power: float = float('inf')
    max_power: float = float('-inf')
    
    def update(self, power: float):
        """Обновление статистик новым значением мощности."""
        self.sum_power += power
        self.count += 1
        self.min_power = min(self.min_power, power)
        self.max_power = max(self.max_power, power)
    
    def to_dict(self) -> Dict[str, Any]:
        """Конвертация в словарь."""
        return {
            'sum': self.sum_power,
            'avg': self.sum_power / self.count if self.count > 0 else 0.0,
            'min': self.min_power if self.min_power != float('inf') else 0.0,
            'max': self.max_power if self.max_power != float('-inf') else 0.0,
            'count': self.count
        }

class WindowProcessor:
    """
    Процессор скользящего окна для агрегации данных.
    
    Поддерживает два типа окон:
    1. Tumbling window (фиксированные непересекающиеся окна)
    2. Sliding window (скользящие окна с перекрытием)
    """
    
    def __init__(
        self,
        window_size_seconds: int = 300,  # 5 минут
        slide_interval_seconds: int = 60,  # 1 минута
        max_window_age_seconds: int = 3600,  # 1 час
        window_type: str = 'sliding'  # 'tumbling' или 'sliding'
    ):
        self.window_size = timedelta(seconds=window_size_seconds)
        self.slide_interval = timedelta(seconds=slide_interval_seconds)
        self.max_window_age = timedelta(seconds=max_window_age_seconds)
        self.window_type = window_type
        
        # Хранилище окон
        self.windows: Dict[datetime, Dict[str, WindowStats]] = defaultdict(dict)
        self.window_start_times: List[datetime] = []
        
        # Блокировка для потокобезопасности
        self.lock = threading.RLock()
        
        # Флаг работы
        self.running = False
        self.cleanup_thread: Optional[threading.Thread] = None
        
        # Очередь для агрегированных данных
        self.aggregated_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
        
        logger.info(f"Window processor initialized: {window_type} window, "
                   f"size={window_size_seconds}s, slide={slide_interval_seconds}s")
    
    def start(self):
        """Запуск процессора окон."""
        self.running = True
        
        # Запуск потока очистки старых окон
        self.cleanup_thread = threading.Thread(target=self._cleanup_old_windows, daemon=True)
        self.cleanup_thread.start()
        
        logger.info("Window processor started")
    
    def stop(self):
        """Остановка процессора окон."""
        self.running = False
        
        if self.cleanup_thread:
            self.cleanup_thread.join(timeout=5.0)
        
        logger.info("Window processor stopped")
    
    def add_reading(self, reading_data: Dict[str, Any]):
        """
        Добавление показания в процессор.
        
        Args:
            reading_data: словарь с данными показания
        """
        try:
            # Создание объекта Reading
            if isinstance(reading_data.get('timestamp'), str):
                timestamp = datetime.fromisoformat(reading_data['timestamp'])
            else:
                timestamp = reading_data.get('timestamp', datetime.now())
            
            reading = Reading(
                meter_id=reading_data['meter_id'],
                timestamp=timestamp,
                power=float(reading_data['power']),
                validated=reading_data.get('validated', True)
            )
            
            # Добавление во все подходящие окна
            with self.lock:
                if self.window_type == 'tumbling':
                    self._add_to_tumbling_window(reading)
                else:  # sliding
                    self._add_to_sliding_window(reading)
                
        except Exception as e:
            logger.error(f"Failed to add reading to window processor: {e}")
    
    def _add_to_tumbling_window(self, reading: Reading):
        """Добавление показания в tumbling окно."""
        # Определение начала окна
        window_start = self._get_window_start(reading.timestamp)
        
        # Получение или создание статистик для этого окна
        window_stats = self.windows[window_start]
        if reading.meter_id not in window_stats:
            window_stats[reading.meter_id] = WindowStats()
        
        # Обновление статистик
        window_stats[reading.meter_id].update(reading.power)
        
        # Сохранение времени начала окна
        if window_start not in self.window_start_times:
            self.window_start_times.append(window_start)
            self.window_start_times.sort()
    
    def _add_to_sliding_window(self, reading: Reading):
        """Добавление показания в sliding окно."""
        # Определение возможных начал окон
        possible_starts = self._get_sliding_window_starts(reading.timestamp)
        
        for window_start in possible_starts:
            # Получение или создание статистик для этого окна
            window_stats = self.windows[window_start]
            if reading.meter_id not in window_stats:
                window_stats[reading.meter_id] = WindowStats()
            
            # Обновление статистик
            window_stats[reading.meter_id].update(reading.power)
            
            # Сохранение времени начала окна
            if window_start not in self.window_start_times:
                self.window_start_times.append(window_start)
                self.window_start_times.sort()
    
    def _get_window_start(self, timestamp: datetime) -> datetime:
        """Получение времени начала окна для tumbling окон."""
        # Округление до ближайшего кратного slide_interval
        seconds_since_epoch = timestamp.timestamp()
        window_start_seconds = (seconds_since_epoch // self.slide_interval.total_seconds()) * self.slide_interval.total_seconds()
        return datetime.fromtimestamp(window_start_seconds)
    
    def _get_sliding_window_starts(self, timestamp: datetime) -> List[datetime]:
        """Получение времён начала всех sliding окон, содержащих timestamp."""
        starts = []
        
        # Время окончания самого раннего окна, содержащего timestamp
        window_end = timestamp
        
        # Генерация начал окон
        current_end = window_end
        while current_end - self.window_size <= timestamp:
            window_start = current_end - self.window_size
            if window_start <= timestamp:
                starts.append(window_start)
            current_end -= self.slide_interval
        
        return starts
    
    def get_aggregated_data(self, window_start: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
        """
        Получение агрегированных данных для окна.
        
        Args:
            window_start: время начала окна (если None, возвращает самое старое завершённое окно)
            
        Returns:
            Словарь с агрегированными данными или None
        """
        with self.lock:
            if window_start is None:
                # Поиск самого старого завершённого окна
                window_start = self._get_oldest_completed_window()
                if window_start is None:
                    return None
            
            if window_start not in self.windows:
                return None
            
            window_end = window_start + self.window_size
            window_stats = self.windows[window_start]
            
            # Конвертация статистик в словарь
            aggregates = {
                meter_id: stats.to_dict()
                for meter_id, stats in window_stats.items()
            }
            
            # Удаление окна после обработки (для tumbling окон)
            if self.window_type == 'tumbling':
                del self.windows[window_start]
                if window_start in self.window_start_times:
                    self.window_start_times.remove(window_start)
            
            return {
                'window_start': window_start,
                'window_end': window_end,
                'aggregates': aggregates
            }
    
    def _get_oldest_completed_window(self) -> Optional[datetime]:
        """Получение времени начала самого старого завершённого окна."""
        if not self.window_start_times:
            return None
        
        now = datetime.now()
        
        for window_start in self.window_start_times:
            window_end = window_start + self.window_size
            if window_end < now:  # Окно завершено
                return window_start
        
        return None
    
    def get_current_windows(self) -> List[Dict[str, Any]]:
        """Получение информации о текущих окнах."""
        with self.lock:
            windows_info = []
            now = datetime.now()
            
            for window_start in self.window_start_times:
                window_end = window_start + self.window_size
                window_stats = self.windows[window_start]
                
                windows_info.append({
                    'window_start': window_start,
                    'window_end': window_end,
                    'is_active': window_start <= now <= window_end,
                    'is_completed': window_end < now,
                    'meter_count': len(window_stats),
                    'total_readings': sum(stats.count for stats in window_stats.values())
                })
            
            return windows_info
    
    def _cleanup_old_windows(self):
        """Очистка старых окон."""
        while self.running:
            try:
                with self.lock:
                    now = datetime.now()
                    windows_to_remove = []
                    
                    for window_start in list(self.windows.keys()):
                        window_age = now - window_start
                        if window_age > self.max_window_age:
                            windows_to_remove.append(window_start)
                    
                    for window_start in windows_to_remove:
                        del self.windows[window_start]
                        if window_start in self.window_start_times:
                            self.window_start_times.remove(window_start)
                    
                    if windows_to_remove:
                        logger.debug(f"Cleaned up {len(windows_to_remove)} old windows")
                
                time.sleep(60)  # Проверка каждую минуту
                
            except Exception as e:
                logger.error(f"Error in window cleanup: {e}")
                time.sleep(10)
    
    async def get_aggregated_data_async(self) -> Optional[Dict[str, Any]]:
        """Асинхронное получение агрегированных данных."""
        return await asyncio.get_event_loop().run_in_executor(
            None, self.get_aggregated_data
        )

class TumblingWindowProcessor(WindowProcessor):
    """Процессор tumbling окон."""
    
    def __init__(self, window_size_seconds: int = 300):
        super().__init__(
            window_size_seconds=window_size_seconds,
            slide_interval_seconds=window_size_seconds,  # slide = size для tumbling окон
            window_type='tumbling'
        )

class SlidingWindowProcessor(WindowProcessor):
    """Процессор sliding окон."""
    
    def __init__(self, window_size_seconds: int = 300, slide_interval_seconds: int = 60):
        super().__init__(
            window_size_seconds=window_size_seconds,
            slide_interval_seconds=slide_interval_seconds,
            window_type='sliding'
        )

def test_window_processor():
    """Тестирование процессора окон."""
    import random
    
    processor = SlidingWindowProcessor(window_size_seconds=30, slide_interval_seconds=10)
    processor.start()
    
    try:
        # Генерация тестовых данных
        test_meters = [f"meter-{i:03d}" for i in range(1, 6)]
        
        print("Adding test readings...")
        for i in range(50):
            meter_id = random.choice(test_meters)
            power = random.uniform(0.1, 10.0)
            timestamp = datetime.now() - timedelta(seconds=random.uniform(0, 40))
            
            processor.add_reading({
                'meter_id': meter_id,
                'timestamp': timestamp,
                'power': power
            })
            
            time.sleep(0.1)
        
        # Получение агрегированных данных
        print("\nCurrent windows:")
        windows = processor.get_current_windows()
        for window in windows:
            print(f"  Window {window['window_start'].strftime('%H:%M:%S')} - "
                  f"{window['window_end'].strftime('%H:%M:%S')}: "
                  f"{window['meter_count']} meters, {window['total_readings']} readings")
        
        print("\nAggregated data:")
        aggregated = processor.get_aggregated_data()
        if aggregated:
            print(f"Window: {aggregated['window_start']} - {aggregated['window_end']}")
            for meter_id, stats in list(aggregated['aggregates'].items())[:3]:  # первые 3
                print(f"  {meter_id}: sum={stats['sum']:.2f}, avg={stats['avg']:.2f}, "
                      f"min={stats['min']:.2f}, max={stats['max']:.2f}, count={stats['count']}")
        
    finally:
        processor.stop()

if __name__ == "__main__":
    test_window_processor()