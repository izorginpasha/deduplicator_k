import asyncio
import json
from aiokafka import AIOKafkaConsumer
import redis.asyncio as redis
from deduplicator.deduplicator import Deduplicator
from dotenv import load_dotenv
import os

load_dotenv()

# Очередь для отправки событий
TOPIC = os.getenv("TOPIC")

redis_client = redis.Redis(host='localhost', port=6379, db=0)
deduplicator = Deduplicator(redis=redis_client)

async def handle_event(event: dict):
    print(f"✅ Уникальное событие: {event['event_id']}")
    # Здесь сохраняем в ClickHouse / Postgres

async def consume():
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers='localhost:9092',
        group_id="events-group",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    await consumer.start()
    print("🔄 Kafka Consumer запущен...")
    try:
        async for msg in consumer:
            event = json.loads(msg.value.decode())
            is_duplicate = await deduplicator.is_duplicate(event)
            if not is_duplicate:
                await handle_event(event)
            else:
                print(f"⛔ Дубликат: {event['event_id']}")
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(consume())
