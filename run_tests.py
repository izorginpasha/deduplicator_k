import asyncio
import json
import redis.asyncio as redis
from worker import worker_loop

QUEUE_KEY = "events:queue"
TEST_EVENTS = [
    {
        "event_name": "purchase",
        "userId": 123,
        "client_id": "abc",
        "event_datetime": "2025-04-07T12:00:00Z",
        "product_id": 42,
        "client_id_query": "abc"
    },
    {
        "event_name": "purchase",
        "userId": 123,
        "client_id": "abc",
        "event_datetime": "2025-04-07T12:00:00Z",
        "product_id": 42,
        "client_id_query": "abc"
    },
    {
        "event_name": "view",
        "userId": 999,
        "client_id": "xyz",
        "event_datetime": "2025-04-07T12:00:00Z",
        "product_id": 99,
        "client_id_query": "xyz"
    }
]

redis_client = redis.Redis(host='localhost', port=6379, db=0)

async def reset_redis():
    print("🧹 Очищаем Redis...")
    await redis_client.flushdb()

async def push_test_events():
    print("📤 Отправляем тестовые события...")
    for event in TEST_EVENTS:
        await redis_client.rpush(QUEUE_KEY, json.dumps(event))

async def run_all():
    await reset_redis()
    await push_test_events()
    await worker_loop()

if __name__ == "__main__":
    asyncio.run(run_all())
