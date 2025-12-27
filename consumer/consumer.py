import asyncio
import json
import os
import signal
import logging

from aiokafka import AIOKafkaConsumer
from clickhouse_driver import Client
from dotenv import load_dotenv

from consumer.deduplicator.deduplicator import Deduplicator
from consumer.db.clickhouse.clickhouse_manager import ClickHouseManager
from consumer.db.rocks.rocks_manager import RocksDedupStore
# ─── Загрузка переменных окружения ─────────────────────────────────────────────
load_dotenv()

TOPIC = os.getenv("TOPIC")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")
REDIS_HOST = os.getenv("REDIS_HOST")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
GROUP_ID = os.getenv("GROUP_ID")
CONSUMER_NAME = os.getenv("CONSUMER_NAME")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB")
PATH_ROCKS = os.getenv("PATH_ROCKS")
# ─── Настройка логирования ─────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format=f"[{CONSUMER_NAME}] %(message)s")
logger = logging.getLogger(__name__)

# ───  ClickHouse ────────────────────────────────────────────────────────

clickhouse_client = Client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    user=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    database=CLICKHOUSE_DB
)
rocks_store = RocksDedupStore(
    path=PATH_ROCKS,
    window_seconds=WINDOW_SECONDS,
)

ch_manager = ClickHouseManager(clickhouse_client)

deduplicator = Deduplicator( rocks_store)

event_queue = asyncio.Queue(maxsize=2000)



# ─── Kafka Consumer ────────────────────────────────────────────────────────────
async def consume():

    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=False,
    )
    await consumer.start()
    logger.info(" Kafka consumer запущен...")

    try:
        while True:
            # получаем сообщение из очереди
            msg = await consumer.getone()
            # обрабатываем в словарь
            event = json.loads(msg.value.decode())
            # отправляем в дедубликатор
            if not await deduplicator.is_duplicate(event):
                # уникально в очередь на запись в clickhouse
                await event_queue.put((msg, event))
            else:
                # Дубликаты коментим, логируем и отбрасываем
                await consumer.commit()
                logger.info(f" Дубликат: {event.get('event_id')}")
    except Exception as e:
        # Любая ошибка при обработке сообщения
        logger.warning(f" Ошибка при обработке сообщения: {e}")
    except asyncio.CancelledError:
        # Корректная остановка по сигналу (SIGTERM / shutdown)
        logger.info(" Получен сигнал остановки")
    finally:
        # Корректно закрываем Kafka consumer
        await consumer.stop()
        logger.info("Kafka consumer закрыт")

# ─── writer ───────────────────────────────────────────────────────────────


async def writer(batch_size=500, flush_interval=0.5):
    # Буфер для накопления событий перед записью в ClickHouse
    batch = []
    msgs = []  # сообщения для расчёта offsets

    # Время последней записи (flush)
    # Используем monotonic(), чтобы не зависеть от изменения системного времени
    last_flush = time.monotonic()

    while True:
        # Сколько ещё можно ждать новое событие,
        # прежде чем нужно сделать принудительный flush по времени
        timeout = flush_interval - (time.monotonic() - last_flush)

        try:
            # Пытаемся получить ОДНО событие из очереди,
            # но не ждём дольше, чем timeout секунд
            msg, event = await asyncio.wait_for(
                event_queue.get(),
                timeout=max(0, timeout)  # защита от отрицательного таймаута
            )

            # Добавляем событие в текущий batch
            batch.append(event)
            msgs.append(msg)

            # Сообщаем очереди, что элемент успешно обработан
            event_queue.task_done()

        except asyncio.TimeoutError:
            # Таймаут — это НЕ ошибка.
            # Он означает, что за отведённое время
            # новые события не пришли.
            pass

        # Условие записи batch в ClickHouse:
        # 1) накопили batch_size событий
        # ИЛИ
        # 2) прошло flush_interval секунд с последней записи
        if batch and (
            len(batch) >= batch_size
            or (time.monotonic() - last_flush) >= flush_interval
        ):
            try:
                # 1) Пишем батч в ClickHouse
                await ch_manager.insert_batch(batch)

                # 2) Готовим offsets только для записанных сообщений
                offsets = {}
                for m in msgs:
                    tp = TopicPartition(m.topic, m.partition)
                    next_offset = m.offset + 1

                    prev = offsets.get(tp)
                    if prev is None or next_offset > prev.offset:
                        offsets[tp] = OffsetAndMetadata(next_offset, "")

                # 3) Коммитим offsets ПОСЛЕ успешной вставки
                await consumer.commit(offsets=offsets)

                # 4) Очищаем buffers только после успеха
                batch.clear()
                msgs.clear()
                last_flush = time.monotonic()

            except Exception as e:
                # Не коммитим! Пусть Kafka переотдаст.
                # batch и msgs оставляем, чтобы попробовать вставить снова.
                logger.exception(f"ClickHouse insert/commit failed (no commit): {e}")
                # можно добавить backoff:
                await asyncio.sleep(0.2)

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
        logger.info(" Остановка consumer и завершение воркеров...")
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
