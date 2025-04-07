import asyncio
import json
import redis.asyncio as redis
from deduplicator.deduplicator import Deduplicator

QUEUE_KEY = "events:queue"

redis_client = redis.Redis(host='localhost', port=6379, db=1)
dedup = Deduplicator(redis=redis_client)


async def process(event: dict):
    print(f"✅ Обрабатываем уникальное событие: ")
    # Здесь может быть логика сохранения в БД, логирования и т.п.


async def worker_loop(worker_id: int):
    print(f"🚀 Воркер {worker_id} запущен и ожидает событий...")
    while True:
        result = await redis_client.blpop(QUEUE_KEY, timeout=1)
        if result:
            _, raw = result
            event = json.loads(raw)

            if await dedup.is_unique(event):
                await process(event)
            else:
                print(f"⛔ Дубликат события от воркера {worker_id}")

        await asyncio.sleep(0.01)


async def start_workers(worker_count: int):
    tasks = []
    for i in range(worker_count):
        # Запускаем несколько воркеров с уникальными ID
        tasks.append(asyncio.create_task(worker_loop(i + 1)))

    # Запускаем все воркеры параллельно
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    # Запускаем 5 воркеров
    asyncio.run(start_workers(5))
