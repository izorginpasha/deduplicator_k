# Deduplicator: Kafka → ClickHouse

Сервис обработки событий с дедупликацией и гарантированной записью в ClickHouse.

Проект принимает события через HTTP API, публикует их в Kafka, обрабатывает consumer’ом с дедупликацией и батчевой записью в ClickHouse.  
Offset’ы Kafka коммитятся **только после успешной записи в БД**.

---

## Архитектура

```
Client / Tests (Locust)
        │
        ▼
     FastAPI
        │
        ▼
      Kafka
   (events-stream)
        │
        ▼
   Kafka Consumer
   ├─ Deduplicator (RocksDB)
   ├─ Batch buffering
   ├─ ClickHouse insert
   └─ Commit offsets
```

### Принципы
- At-least-once delivery
- Коммит offset’ов **после** записи в ClickHouse
- Дедупликация событий
- Батчевые вставки
- Защита от потери данных при сбоях

---

##  Технологии
- Python 3.12
- FastAPI
- Kafka (KRaft, Bitnami)
- aiokafka
- ClickHouse
- clickhouse-driver
- RocksDB
- Docker / Docker Compose
- asyncio
- Locust (нагрузочное тестирование)

---

##  Структура проекта

```
.
├── api/                            # HTTP API (FastAPI)
├── consumer/
│   ├── consumer.py                # Kafka consumer (batch + dedup + ClickHouse)
│   ├── deduplicator/
│   │   └── deduplicator.py        # Hash + RocksDB дедупликация
│   └── db/
│       └── clickhouse/
│           └── clickhouse_manager.py
├── clickhouse-init-scripts/
│   └── init_create_database.sql
├── locustfile.py                  # Нагрузочное тестирование
├── docker-compose.yml
├── .env
└── README.md
```

---

##  Поток обработки события

1. Клиент отправляет событие в API  
2. API публикует событие в Kafka (`events-stream`)  
3. Consumer читает сообщение  
4. Вычисляется `event_hash`  
5. Проверка дедупликации (RocksDB)  
6. Уникальные события накапливаются в batch  
7. Batch записывается в ClickHouse  
8. Offset’ы Kafka коммитятся  
9. Дубликаты отбрасываются и коммитятся сразу  

---

##  Формат события

```json
{
  "event_id": "uuid",
  "type": "test_event",
  "payload": { "msg": "hello" }
}
```

- `event_id` — уникальный идентификатор события  
- `event_hash` вычисляется в consumer  
- `payload` — произвольные данные  

---

## 🗄 ClickHouse

- Вставка выполняется батчами  
- `event_hash` хранится как `FixedString(32)` (bytes)  
- Вставка выполняется через `asyncio.to_thread`, так как клиент синхронный  

---

##  Дедупликация

- Реализована через RocksDB  
- Ключ — `event_hash`  
- Sliding window (настраивается)  
- Повторные события не попадают в ClickHouse  

---

##  Запуск проекта

### 1. Переменные окружения (`.env`)

```env
TOPIC=events-stream
GROUP_ID=events-consumer



# ================= Kafka (KRaft) =================
KAFKA_CFG_NODE_ID=1
KAFKA_CFG_PROCESS_ROLES=broker,controller
KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@kafka:9093

KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093
KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092
KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER
KAFKA_CFG_INTER_BROKER_LISTENER_NAME=PLAINTEXT
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ALLOW_PLAINTEXT_LISTENER=yes

# ================= CLICKHOUSE =================
CLICKHOUSE_HOST=clickhouse
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=my_password
CLICKHOUSE_PORT=9000
CLICKHOUSE_DB=events
# ================= Rocks =================
PATH_ROCKS=/var/lib/rocksdb
WINDOW_SECONDS=604800
```

---

### 2. Запуск через Docker Compose

```bash
docker compose up -d --build
```

---

### 3. Просмотр логов в реальном времени

```bash
docker logs -f consumer
docker logs -f api
```

---

## 🧪 Ручная отправка события

```python
import requests
import uuid
import time

event = {
    "event_id": str(uuid.uuid4()),
    "type": "manual_test",
    "payload": {"msg": "hello"},
    "sent_at": int(time.time())
}

r = requests.post("http://localhost:8000/event", json=event)
print(r.status_code, r.text)
```

---

##  Нагрузочное тестирование (Locust)

В проекте используется `locustfile.py`.

### Запуск с UI

```bash
pip install locust
locust -f locustfile.py
```

Открой в браузере:

```
http://localhost:8089
```

### Headless режим

```bash
locust -f locustfile.py   --headless   -u 200   -r 50   -t 60s   --host http://localhost:8000
```

---

##  Отладка

### Проверить, что данные пишутся

```bash
docker exec -it clickhouse clickhouse-client -q "SELECT count() FROM default.events"
```

### Типичный лог consumer

```
есть сообщение
уникально
Пишем в ClickHouse batch=1
Batch записан + offsets committed
```

---

##  Важные моменты

- Offset’ы коммитятся только после успешной вставки  
- ClickHouse клиент синхронный — нельзя использовать `await`  
- Для отладки удобно использовать `auto_offset_reset="earliest"`  
- Один consumer = один writer (batch внутри одного цикла)  

---

##  Roadmap

- Prometheus / Grafana метрики  
- Dead-letter queue  
- Retry / backoff для ClickHouse  
- Масштабирование consumer’ов  
- Bloom filter перед RocksDB  
- Структурированные JSON-логи  

---

##  Статус проекта

Проект рабочий.  
Подходит как:
- pet-project  
- MVP  
- основа для продакшн-сервиса  
