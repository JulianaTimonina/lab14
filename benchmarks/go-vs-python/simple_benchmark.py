#!/usr/bin/env python3
"""
Простой бенчмарк для сравнения производительности Go и Python на идентичной логике.
Имитирует оконную агрегацию как в Go.
"""

import time
import random
from datetime import datetime, timedelta
import sys
import os

def generate_test_readings(num_readings: int, num_meters: int):
    """Генерация тестовых показаний аналогично Go."""
    readings = []
    base_time = datetime.now()
    for i in range(num_readings):
        meter_id = f"meter-{i % num_meters + 1:03d}"
        timestamp = base_time + timedelta(seconds=i)
        power = 0.1 + (i % 100) * 0.1
        readings.append({
            'meter_id': meter_id,
            'timestamp': timestamp,
            'power': power,
            'validated': True
        })
    return readings

class SimpleWindowAggregator:
    """Упрощённый агрегатор окон, аналогичный Go."""
    def __init__(self, window_size_seconds: int):
        self.window_size = timedelta(seconds=window_size_seconds)
        self.windows = {}  # ключ: начало окна (datetime), значение: словарь meter_id -> статистика
        self.stats = {}    # вложенный словарь: window_start -> meter_id -> {sum, count, min, max}
    
    def add_reading(self, reading):
        meter_id = reading['meter_id']
        timestamp = reading['timestamp']
        power = reading['power']
        
        # Определяем начало окна
        window_start = self._get_window_start(timestamp)
        
        if window_start not in self.stats:
            self.stats[window_start] = {}
        if meter_id not in self.stats[window_start]:
            self.stats[window_start][meter_id] = {
                'sum': 0.0,
                'count': 0,
                'min': float('inf'),
                'max': float('-inf')
            }
        
        stat = self.stats[window_start][meter_id]
        stat['sum'] += power
        stat['count'] += 1
        stat['min'] = min(stat['min'], power)
        stat['max'] = max(stat['max'], power)
    
    def _get_window_start(self, timestamp):
        # Окно фиксированное, выравнивание по секундам
        total_seconds = int(timestamp.timestamp())
        window_start_seconds = total_seconds - (total_seconds % self.window_size.seconds)
        return datetime.fromtimestamp(window_start_seconds)
    
    def get_aggregated(self):
        """Возвращает агрегированные данные (имитация)."""
        aggregated = []
        for window_start, meters in self.stats.items():
            for meter_id, stat in meters.items():
                aggregated.append({
                    'window_start': window_start,
                    'meter_id': meter_id,
                    'avg': stat['sum'] / stat['count'] if stat['count'] > 0 else 0.0,
                    'min': stat['min'] if stat['min'] != float('inf') else 0.0,
                    'max': stat['max'] if stat['max'] != float('-inf') else 0.0,
                    'count': stat['count']
                })
        return aggregated

def benchmark_simple_aggregation():
    print("=== Python Simple Benchmark: Window Aggregation ===")
    
    window_size = 30  # секунд, как в Go
    aggregator = SimpleWindowAggregator(window_size)
    
    num_readings = 10000
    num_meters = 100
    print(f"Generating {num_readings} test readings...")
    readings = generate_test_readings(num_readings, num_meters)
    
    print("Adding readings to aggregator...")
    start_time = time.time()
    
    for reading in readings:
        aggregator.add_reading(reading)
    
    add_time = time.time() - start_time
    print(f"Added {num_readings} readings in {add_time:.6f} seconds")
    throughput = num_readings / add_time if add_time > 0 else float('inf')
    print(f"Throughput: {throughput:.2f} readings/sec")
    
    # Агрегация
    start_time = time.time()
    aggregated = aggregator.get_aggregated()
    agg_time = time.time() - start_time
    print(f"Aggregated {len(aggregated)} records in {agg_time:.6f} seconds")
    
    # Память (приблизительно)
    import psutil
    process = psutil.Process(os.getpid())
    memory_mb = process.memory_info().rss / 1024 / 1024
    print(f"Memory usage: {memory_mb:.2f} MB")
    
    return {
        'readings_count': num_readings,
        'add_time_sec': add_time,
        'throughput_readings_per_sec': throughput,
        'aggregation_time_sec': agg_time,
        'memory_mb': memory_mb
    }

if __name__ == "__main__":
    results = benchmark_simple_aggregation()
    print("\n=== Summary ===")
    print(f"Throughput: {results['throughput_readings_per_sec']:.2f} readings/sec")
    print(f"Memory: {results['memory_mb']:.2f} MB")
    
    # Сохраняем результаты
    import json
    output_file = os.path.join(os.path.dirname(__file__), 'results', 'python_simple_benchmark_results.json')
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    print(f"Results saved to {output_file}")