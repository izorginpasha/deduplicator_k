import asyncio
import json
import os
import signal
import logging

from aiokafka import AIOKafkaConsumer
import redis.asyncio as redis
from clickhouse_driver import Client
from dotenv import load_dotenv

from consumer.deduplicator.deduplicator import Deduplicator
from consumer.db.clickhouse_manager import ClickHouseManager

# ─── Загрузка переменных окружения ─────────────────────────────────────────────
load_dotenv()

TOPIC = os.getenv("TOPIC", "events")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
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



# ─── Kafka Consumer ────────────────────────────────────────────────────────────
async def consume():
    # создание таблици если еще нет
    await ch_manager.create_events_table()

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
        while True:
            msg = await consumer.getone() # получаем сообщение из очереди
            event = json.loads(msg.value.decode())# обрабатываем в словарь

            if not await deduplicator.is_duplicate(event):# отправляем в дедубликатор
                await event_queue.put(event)
            else:
                logger.info(f"⛔ Дубликат: {event.get('event_id')}")
    except Exception as e:
        logger.warning(f"⚠️ Ошибка при обработке сообщения: {e}")
    except asyncio.CancelledError:
        logger.info("🛑 Получен сигнал остановки")
    finally:
        await consumer.stop()
        await redis_client.aclose()
        logger.info("🧹 Kafka consumer и Redis закрыты")



# ─── Точка входа ───────────────────────────────────────────────────────────────
async def async_main():
    consumer_task = asyncio.create_task(consume())
    # worker_tasks = [asyncio.create_task(worker(i)) for i in range(NUM_WORKERS)]

    loop = asyncio.get_running_loop()

    stop_event = asyncio.Event()

    def shutdown():
        logger.info("🚪 Завершение работы...")
        stop_event.set()

    loop.add_signal_handler(signal.SIGINT, shutdown)
    loop.add_signal_handler(signal.SIGTERM, shutdown)

    try:
        await stop_event.wait()  # Ждём сигнала завершения
    finally:
        logger.info("⛔ Остановка consumer и завершение воркеров...")
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass

        # Завершаем очередь: отправим None каждому воркеру
        for _ in range(NUM_WORKERS):
            await event_queue.put(None)

        # Ждём завершения очереди
        await event_queue.join()

        # # Завершаем воркеры
        # await asyncio.gather(*worker_tasks, return_exceptions=True)

        logger.info("🏁 Все задачи завершены")

def main():
    asyncio.run(async_main())

if __name__ == "__main__":
    main()
