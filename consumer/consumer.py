import asyncio
import json
from aiokafka import AIOKafkaConsumer
import redis.asyncio as redis
from deduplicator.deduplicator import Deduplicator
from dotenv import load_dotenv
import os
import signal
import sys
import ydb
from datetime import datetime

load_dotenv()

TOPIC = os.getenv("TOPIC")  # ← значение по умолчанию
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")
с

# Redis клиент
redis_client = redis.Redis(host='localhost', port=6379, db=0)

# Дедупликатор
deduplicator = Deduplicator(redis=redis_client)

# YDB клиент
ydb_client = ydb.Driver(
    endpoint=YDB_ENDPOINT,
    database=YDB_DATABASE,
    credentials=ydb.auth.MetadataTokenCredentials('YOUR_YDB_METADATA_TOKEN')
)
ydb_client.wait()


# YDB таблица для сохранения событий
def get_event_table():
    return ydb_client.table(YDB_DATABASE + "/events")


# Обработчик событий
async def handle_event(event: dict):
    print(f"✅ Уникальное событие: {event['event_id']}")

    # Запись в YDB
    table = get_event_table()
    insert_query = f"""
    INSERT INTO events (event_id, event_name, user_id, event_datetime, product_id, client_id_query)
    VALUES ('{event['event_id']}', '{event['event_name']}', '{event['userId']}', '{event['event_datetime']}', '{event['product_id']}', '{event['client_id_query']}')
    """

    # Выполнение запроса на добавление данных в YDB
    with table.session() as session:
        session.execute(insert_query)


# Основной консюмер
async def consume():
    ### Внимание только для разработки ##########
    await deduplicator.reset()  # ← очистка Redis/Bloom-фильтра

    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="events-group",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )

    await consumer.start()
    print("🔄 Kafka Consumer запущен...")

    try:
        async for msg in consumer:
            try:
                event = json.loads(msg.value.decode())
                is_duplicate = await deduplicator.is_duplicate(event)
                if not is_duplicate:
                    await handle_event(event)
                else:
                    print(f"⛔ Дубликат: {event['event_id']}")
            except Exception as e:
                print(f"⚠️ Ошибка при обработке сообщения: {e}")
    except asyncio.CancelledError:
        print("🛑 Консьюмер остановлен вручную")
    finally:
        await consumer.stop()
        await redis_client.close()
        ydb_client.stop()
        print("🧹 Очистка завершена, соединения закрыты.")


# Обёртка для запуска
def main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    task = loop.create_task(consume())

    # Грейсфул-шатдаун по Ctrl+C
    def shutdown(*args):
        print("🚪 Завершение работы...")
        task.cancel()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        loop.run_until_complete(task)
    except asyncio.CancelledError:
        pass
    finally:
        loop.close()


if __name__ == "__main__":
    main()
