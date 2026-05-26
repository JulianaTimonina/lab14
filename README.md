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

4. Запустить Go-сборщик:
```bash
cd go-collector
go run cmd/collector/main.go
```

5. Запустить Python-обработчик:
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

- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000
- Дашборд: http://localhost:8501

## Структура проекта

См. [plans/architecture_plan.md](plans/architecture_plan.md) для детального описания архитектуры.

## Лицензия

MIT