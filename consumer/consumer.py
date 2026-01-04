import asyncio
import signal
import logging
import os
import json
import time
from aiokafka import AIOKafkaConsumer
from aiokafka.structs import TopicPartition, OffsetAndMetadata
from clickhouse_driver import Client
from dotenv import load_dotenv
from aiokafka.errors import KafkaError
import random

from consumer.deduplicator.deduplicator import Deduplicator
from consumer.db.clickhouse.clickhouse_manager import ClickHouseManager
from consumer.db.rocks.rocks_manager import RocksDedupStore
# ─── Загрузка переменных окружения ─────────────────────────────────────────────
load_dotenv()

TOPIC = os.getenv("TOPIC")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
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
WINDOW_SECONDS = int(os.getenv("WINDOW_SECONDS"))
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





# ─── Kafka Consumer ────────────────────────────────────────────────────────────
# создание Kafka Consumer
def make_consumer() -> AIOKafkaConsumer:
    return AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,# коминтим после успешнои записи или отброса дубля
        # настройки стабильности:
        session_timeout_ms=30000,
        heartbeat_interval_ms=10000,
        max_poll_interval_ms=10 * 60 * 1000,  # если ClickHouse иногда тормозит
        request_timeout_ms=60000,
    )
# запуск и перезапуск при падении
async def run_consumer_forever(stop_event: asyncio.Event):
    backoff = 1.0

    while not stop_event.is_set():
        consumer = make_consumer()
        try:
            await consume(consumer)  # твоя функция
            # если consume вышел сам — это уже подозрительно
            logger.error("consume() завершился без stop_event — перезапуск")
        except asyncio.CancelledError:
            raise
        except KafkaError:
            logger.exception("KafkaError: consumer будет перезапущен")
        except Exception:
            logger.exception("Unexpected error: consumer будет перезапущен")
        finally:
            try:
                await consumer.stop()
            except Exception:
                pass

        # backoff, чтобы не долбиться в бесконечном цикле
        await asyncio.sleep(backoff + random.random())
        backoff = min(backoff * 2, 30.0)
# сам обработчик очереди
async def consume(
    consumer: AIOKafkaConsumer,
    batch_size: int = 500,
    flush_interval: float = 5.0,
):
    await consumer.start()
    logger.info("Kafka consumer запущен...")

    batch = []
    msgs = []
    last_flush = time.monotonic()

    async def flush_batch():
        nonlocal batch, msgs, last_flush
        if not batch:
            # чтобы таймер не уходил в 0 и не начинался busy-loop
            last_flush = time.monotonic()
            return

        logger.info(f"Пишем в ClickHouse batch={len(batch)}")
        # clickhouse_driver.Client синхронный -> в отдельный поток
        await asyncio.to_thread(ch_manager.insert_batch, batch)

        # Коммитим offsets только для успешно записанных сообщений
        offsets = {}
        for m in msgs:
            tp = TopicPartition(m.topic, m.partition)
            next_offset = m.offset + 1
            prev = offsets.get(tp)
            if prev is None or next_offset > prev.offset:
                offsets[tp] = OffsetAndMetadata(next_offset, "")

        await consumer.commit(offsets=offsets)

        batch.clear()
        msgs.clear()
        last_flush = time.monotonic()
        logger.info("Batch записан + offsets committed")

    try:
        while True:
            # Ждём сообщение, но не дольше, чем нужно до flush по времени
            timeout = flush_interval - (time.monotonic() - last_flush)
            if timeout < 0:
                timeout = 0

            try:
                msg = await asyncio.wait_for(consumer.getone(), timeout=timeout)
            except asyncio.TimeoutError:
                #  записать накопленное
                if not batch:
                    logger.info("⏳ Очередь пуста, жду сообщения...")
                    await asyncio.sleep(0.2)
                await flush_batch()
                continue

            event = json.loads(msg.value.decode())
            event_hash = deduplicator.hash_event(event)
            logger.info("есть сообщение")

            if deduplicator.is_duplicate(event_hash):
                logger.info("дубль")
                tp = TopicPartition(msg.topic, msg.partition)
                await consumer.commit({tp: OffsetAndMetadata(msg.offset + 1, "")})
                logger.info(f"Дубликат: {event.get('event_id')}")
                continue

            # Уникальное
            event["event_hash"] = event_hash
            logger.info("уникально")

            batch.append(event)
            msgs.append(msg)

            # flush по размеру
            if len(batch) >= batch_size:
                await flush_batch()

    except asyncio.CancelledError:
        logger.info("Получен сигнал остановки")
        # при остановке — дописываем то, что накопили
        try:
            await flush_batch()
        except Exception:
            logger.exception("Не удалось дописать batch при shutdown (offsets не закоммичены)")

    except Exception as e:
        logger.exception(f"Ошибка в consumer loop: {e}")

    finally:
        logger.error("CONSUME EXITED")
        await consumer.stop()
        logger.info("Kafka consumer закрыт")
#

# ─── Точка входа ───────────────────────────────────────────────────────────────
async def async_main():


    # Event для “мягкого” завершения по сигналу
    stop_event = asyncio.Event()

    # Берём текущий event loop, чтобы подписаться на сигналы ОС
    loop = asyncio.get_running_loop()

    def shutdown():
        # Этот обработчик вызывается при Ctrl+C (SIGINT) или SIGTERM (docker stop)
        logger.info("🚪 Получен сигнал завершения, начинаем shutdown...")
        stop_event.set()

    # Регистрируем обработчики сигналов
    loop.add_signal_handler(signal.SIGINT, shutdown)
    loop.add_signal_handler(signal.SIGTERM, shutdown)

    def log_task_result(t: asyncio.Task):
        try:
            t.result()
        except asyncio.CancelledError:
            logger.warning(f"Task {t.get_name()} cancelled")
        except Exception:
            logger.exception(f"Task {t.get_name()} crashed")


    # Запуск корутины
    consumer_task = asyncio.create_task(run_consumer_forever(stop_event), name="kafka-consumer")

    logger.info(f"tasks: consumer={consumer_task!r}")

    consumer_task.add_done_callback(log_task_result)


    logger.info(" задачи созданы: consume ")
    try:
        # Ждём, пока не придёт сигнал остановки
        await stop_event.wait()


    finally:

        logger.info("🛑 Останавливаем consumer (Kafka чтение прекращается)")

        consumer_task.cancel()

        try:

            await consumer_task

        except asyncio.CancelledError:

            pass

        # Даем writer дописать то, что уже лежит в очереди

        logger.info(" Дожидаемся записи очереди в ClickHouse...")







        logger.info("🏁 Все задачи завершены")

def main():
    asyncio.run(async_main())

if __name__ == "__main__":
    main()