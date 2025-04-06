import asyncio
import json
import redis.asyncio as redis
from deduplicator.deduplicator import Deduplicator

QUEUE_KEY = "events:queue"

redis_client = redis.Redis(host='localhost', port=6379, db=0)
dedup = Deduplicator(redis=redis_client)

async def process(event: dict):
    print(f"✅ Обрабатываем уникальное событие: {event}")
    # Здесь может быть логика сохранения в БД, логирования и т.п.

async def worker_loop():
    print("🚀 Воркер запущен и ожидает событий...")
    while True:
        result = await redis_client.blpop(QUEUE_KEY, timeout=1)
        if result:
            _, raw = result
            event = json.loads(raw)

            if await dedup.is_unique(event):
                await process(event)
            else:
                print(f"⛔ Дубликат события: {event}")

        await asyncio.sleep(0.01)

if __name__ == "__main__":
    asyncio.run(worker_loop())
