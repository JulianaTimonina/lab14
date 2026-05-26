#!/usr/bin/env python3
"""
Бенчмарк производительности Python-обработчика оконной агрегации.
"""

import sys
import os
import time
import random
from datetime import datetime, timedelta

# Добавляем путь к модулям проекта
sys.path.append(os.path.join(os.path.dirname(__file__), '../../python-processor'))

from window_processor import WindowProcessor

def generate_test_readings(num_readings: int = 10000, num_meters: int = 100):
    """Генерация тестовых показаний."""
    readings = []
    base_time = datetime.now()
    for i in range(num_readings):
        meter_id = f"meter-{random.randint(1, num_meters):03d}"
        timestamp = base_time + timedelta(seconds=random.uniform(0, 3600))
        power = random.uniform(0.1, 10.0)
        readings.append({
            'meter_id': meter_id,
            'timestamp': timestamp,
            'power': power
        })
    return readings

def benchmark_window_aggregation():
    """Бенчмарк оконной агрегации."""
    print("=== Python Benchmark: Window Aggregation ===")
    
    # Создаём процессор с окном 5 минут и сдвигом 1 минута
    processor = WindowProcessor(window_size_seconds=300, slide_interval_seconds=60)
    
    # Генерируем тестовые данные
    num_readings = 10000
    print(f"Generating {num_readings} test readings...")
    readings = generate_test_readings(num_readings)
    
    # Замеряем время добавления показаний
    print("Adding readings to window processor...")
    start_time = time.time()
    
    for reading in readings:
        processor.add_reading(reading)
    
    add_time = time.time() - start_time
    print(f"Added {num_readings} readings in {add_time:.3f} seconds")
    print(f"Throughput: {num_readings / add_time:.2f} readings/sec")
    
    # Замеряем время агрегации
    print("Calculating aggregated data...")
    start_time = time.time()
    aggregated = processor.get_aggregated_data()
    agg_time = time.time() - start_time
    
    if aggregated:
        num_windows = len(aggregated.get('windows', []))
        print(f"Aggregated {num_windows} windows in {agg_time:.3f} seconds")
    else:
        print("No aggregated data produced")
    
    # Замеряем память (приблизительно)
    import psutil
    process = psutil.Process(os.getpid())
    memory_mb = process.memory_info().rss / 1024 / 1024
    print(f"Memory usage: {memory_mb:.2f} MB")
    
    return {
        'readings_count': num_readings,
        'add_time_sec': add_time,
        'throughput_readings_per_sec': num_readings / add_time,
        'aggregation_time_sec': agg_time,
        'memory_mb': memory_mb
    }

def benchmark_sliding_window():
    """Бенчмарк sliding window."""
    print("\n=== Python Benchmark: Sliding Window ===")
    from window_processor import SlidingWindowProcessor
    
    processor = SlidingWindowProcessor(window_size_seconds=300, slide_interval_seconds=60)
    
    readings = generate_test_readings(5000, 50)
    
    start_time = time.time()
    for reading in readings:
        processor.add_reading(reading)
    elapsed = time.time() - start_time
    
    print(f"Processed {len(readings)} readings in {elapsed:.3f} seconds")
    print(f"Throughput: {len(readings) / elapsed:.2f} readings/sec")
    
    return elapsed

def main():
    """Основная функция."""
    results = {}
    
    # Бенчмарк 1: Оконная агрегация
    results['window_aggregation'] = benchmark_window_aggregation()
    
    # Бенчмарк 2: Sliding window
    results['sliding_window'] = benchmark_sliding_window()
    
    # Сохраняем результаты в JSON
    import json
    output_file = os.path.join(os.path.dirname(__file__), 'results', 'python_benchmark_results.json')
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\nResults saved to {output_file}")
    
    # Вывод сводки
    print("\n=== Summary ===")
    print(f"Python window aggregation throughput: {results['window_aggregation']['throughput_readings_per_sec']:.2f} readings/sec")
    print(f"Memory usage: {results['window_aggregation']['memory_mb']:.2f} MB")

if __name__ == "__main__":
    main()