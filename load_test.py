import json
import random
import redis
import httpx
from locust import HttpUser, task, between, events

# Конфигурация
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
QUEUE_KEY = 'events:queue'
EVENT_FILE = 'results-1743680955719.json'

# Загружаем события из файла
with open(EVENT_FILE, 'r', encoding='utf-8') as f:
    events = json.load(f)

# Redis sync клиент (используем sync, потому что Locust — sync)
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)


def reset_redis_and_push_events():
    print("🔄 Очищаем Redis и загружаем события в очередь...")
    redis_client.flushdb()
    for event in events:
        redis_client.rpush(QUEUE_KEY, json.dumps(event))
    print(f"✅ Загружено {len(events)} событий в очередь")


@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    reset_redis_and_push_events()


class EventLoadTest(HttpUser):
    wait_time = between(0, 0)

    @task
    def post_event(self):
        event = random.choice(events)
        with self.client.post("/event", json=event, catch_response=True) as response:
            if response.status_code != 200:
                response.failure(f"❌ Ошибка: {response.status_code} — {response.text}")
            else:
                response.success()
