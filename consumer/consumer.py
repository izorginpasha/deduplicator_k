import asyncio
import json
import os
import signal
from aiokafka import AIOKafkaConsumer
import redis.asyncio as redis
from dotenv import load_dotenv
from deduplicator.deduplicator import Deduplicator

load_dotenv()

TOPIC = os.getenv("TOPIC")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")
REDIS_HOST = os.getenv("REDIS_HOST")
NUM_WORKERS = int(os.getenv("NUM_WORKERS", 20))

# Redis клиент
redis_client = redis.Redis(host=REDIS_HOST, port=6379, db=1)

# Дедупликатор
deduplicator = Deduplicator(redis=redis_client)

# Очередь событий
event_queue = asyncio.Queue()

# Обработка уникального события
async def handle_event(event: dict):
    print(f"✅ Уникальное событие: {event['event_id']}")
    # await save_event(event)  # ← тут может быть БД, логика и т.п.

# Воркер: достаёт события из очереди и обрабатывает
async def worker(worker_id: int):
    while True:
        event = await event_queue.get()
        try:
            await handle_event(event)
        except Exception as e:
            print(f"⚠️ Ошибка в воркере #{worker_id}: {e}")
        finally:
            event_queue.task_done()

# Kafka consumer
async def consume():
    await deduplicator.reset()  # ← очистка Redis/Bloom-фильтра (только для dev)

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
                    await event_queue.put(event)
                else:
                    print(f"⛔ Дубликат: {event['event_id']}")
            except Exception as e:
                print(f"⚠️ Ошибка при обработке Kafka-сообщения: {e}")
    except asyncio.CancelledError:
        print("🛑 Консьюмер остановлен вручную")
    finally:
        await consumer.stop()
        await redis_client.close()
        print("🧹 Очистка завершена, соединения закрыты.")

# Главная точка запуска
def main():
    loop = asyncio.new_event_loop()#Создаётся новый экземпляр цикла событий (event loop
    asyncio.set_event_loop(loop)#Устанавливает созданный цикл событий как текущий

    # Создаём задачу для консюмера
    consumer_task = loop.create_task(consume())

    # Запускаем несколько воркеров
    worker_tasks = [loop.create_task(worker(i)) for i in range(NUM_WORKERS)]

    def shutdown(*_):
        print("🚪 Завершение работы...")
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
        loop.run_until_complete(event_queue.join())  # дождаться завершения всех задач
        for task in worker_tasks:
            task.cancel()
        loop.run_until_complete(asyncio.gather(*worker_tasks, return_exceptions=True))
        loop.close()
        print("🏁 Всё завершено")

if __name__ == "__main__":
    main()
