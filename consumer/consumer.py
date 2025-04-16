import asyncio
import json
import os
import signal
import logging

from aiokafka import AIOKafkaConsumer
import redis.asyncio as redis
from clickhouse_driver import Client
from dotenv import load_dotenv

from deduplicator.deduplicator import Deduplicator
from db.clickhouse_manager import ClickHouseManager

# ─── Загрузка переменных окружения ─────────────────────────────────────────────
load_dotenv()

TOPIC = os.getenv("TOPIC", "events")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
NUM_WORKERS = int(os.getenv("NUM_WORKERS", 20))
GROUP_ID = os.getenv("GROUP_ID", "events-group")
CONSUMER_NAME = os.getenv("CONSUMER_NAME", "consumer")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 9000))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "my_password")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "default")

# ─── Настройка логирования ─────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format=f"[{CONSUMER_NAME}] %(message)s")
logger = logging.getLogger(__name__)

# ─── Redis и ClickHouse ────────────────────────────────────────────────────────
redis_client = redis.Redis(host=REDIS_HOST, port=6379, db=1)
clickhouse_client = Client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    user=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    database=CLICKHOUSE_DB
)
ch_manager = ClickHouseManager(clickhouse_client)
deduplicator = Deduplicator(redis=redis_client, clickhouse=ch_manager)

event_queue = asyncio.Queue()

# ─── Обработка уникального события ─────────────────────────────────────────────
async def handle_event(event: dict):
    logger.info(f"✅ Уникальное событие: {event.get('event_id')}")
    # Здесь может быть логика дальнейшей обработки
    # Например, отправка в другую систему или запись в Postgres

# ─── Воркер ─────────────────────────────────────────────────────────────────────
async def worker(worker_id: int):
    while True:
        event = await event_queue.get()
        try:
            await handle_event(event)
        except Exception as e:
            logger.error(f"⚠️ Ошибка в воркере #{worker_id}: {e}")
        finally:
            event_queue.task_done()

# ─── Kafka Consumer ────────────────────────────────────────────────────────────
async def consume():
    ch_manager.create_events_table()  # Убедимся, что таблица создана

    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    await consumer.start()
    logger.info("🔄 Kafka consumer запущен...")

    try:
        async for msg in consumer:
            try:
                event = json.loads(msg.value.decode())


                if not await deduplicator.is_duplicate(event):
                    await event_queue.put(event)
                else:
                    logger.info(f"⛔ Дубликат: {event.get('event_id')}")
            except Exception as e:
                logger.warning(f"⚠️ Ошибка при обработке сообщения: {e}")
    except asyncio.CancelledError:
        logger.info("🛑 Получен сигнал остановки")
    finally:
        await consumer.stop()
        await redis_client.close()
        logger.info("🧹 Kafka consumer и Redis закрыты")

# ─── Точка входа ───────────────────────────────────────────────────────────────
def main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    consumer_task = loop.create_task(consume())
    worker_tasks = [loop.create_task(worker(i)) for i in range(NUM_WORKERS)]

    def shutdown(*_):
        logger.info("🚪 Завершение работы...")
        consumer_task.cancel()
        for task in worker_tasks:
            task.cancel()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        loop.run_until_complete(consumer_task)
    except asyncio.CancelledError:
        pass
    finally:
        loop.run_until_complete(event_queue.join())
        for task in worker_tasks:
            task.cancel()
        loop.run_until_complete(asyncio.gather(*worker_tasks, return_exceptions=True))
        loop.close()
        logger.info("🏁 Все задачи завершены")

if __name__ == "__main__":
    main()
