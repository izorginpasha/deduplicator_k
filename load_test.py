import json
import random
import redis.asyncio as redis
import httpx
import asyncio
import signal
import logging
import time
from prometheus_client import Counter, start_http_server

# Конфигурация
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
QUEUE_KEY = 'events:queue'
EVENT_FILE = 'results-1743680955719.json'
API_URL = "http://localhost:8000/event"
DURATION = 30  # общее время выполнения теста (в секундах)
CONCURRENCY = 10
PROMETHEUS_PORT = 8001

# Логгер
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("load_test")

# Метрики Prometheus
rps_counter = Counter("event_rps", "События в секунду")
success_counter = Counter("event_success_total", "Успешные события")
error_counter = Counter("event_error_total", "Ошибки событий")

# Загрузка событий
with open(EVENT_FILE, 'r', encoding='utf-8') as f:
    events_data = json.load(f)
logger.info(f"📦 Загружено {len(events_data)} событий")

# Глобальный сигнал остановки
stop_event = asyncio.Event()

# Обработчик сигналов
def shutdown_signal_handler(*args):
    logger.warning("🛑 Получен сигнал завершения, останавливаем тест...")
    stop_event.set()

# Основная отправка событий
async def send_event(client: httpx.AsyncClient):
    try:
        while not stop_event.is_set():
            event = random.choice(events_data)
            try:
                response = await client.post(API_URL, json=event, timeout=5)
                rps_counter.inc()
                if response.status_code == 200:
                    success_counter.inc()
                else:
                    error_counter.inc()
                    logger.warning(f"❌ {response.status_code} — {response.text}")
            except Exception as e:
                error_counter.inc()
                logger.error(f"Ошибка отправки: {type(e).__name__} — {str(e)}")
    except asyncio.CancelledError:
        logger.info("🟡 Задача отменена.")

# Печать статистики
async def print_stats():
    try:
        while not stop_event.is_set():
            await asyncio.sleep(5)
            logger.info(f"📊 RPS: {rps_counter._value.get():.0f} | ✅: {success_counter._value.get():.0f} | ❌: {error_counter._value.get():.0f}")
    except asyncio.CancelledError:
        pass

# Основной запуск
async def load_test():
    start_http_server(PROMETHEUS_PORT)
    logger.info(f"🚀 Метрики Prometheus доступны на http://localhost:{PROMETHEUS_PORT}")

    start_time = time.time()  # Запоминаем время начала

    async with httpx.AsyncClient() as client:
        tasks = [asyncio.create_task(send_event(client)) for _ in range(CONCURRENCY)]
        stats_task = asyncio.create_task(print_stats())

        logger.info(f"🔥 Запущено {CONCURRENCY} воркеров на {DURATION} секунд...")

        try:
            # Ожидаем завершения теста через DURATION секунд
            await asyncio.wait_for(stop_event.wait(), timeout=DURATION)
        except asyncio.TimeoutError:
            logger.info(f"⌛ Тест завершён по тайм-ауту ({DURATION} секунд).")

        # Завершаем задачи
        for task in tasks:
            task.cancel()
        stats_task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.gather(stats_task, return_exceptions=True)

        logger.info(f"⌛ Тест завершён по тайм-ауту ({DURATION} секунд).")
        logger.info("✅ Все задачи завершены.")
        logger.info(
            f"📊 Итоги: RPS: {rps_counter._value.get():.0f} | ✅: {success_counter._value.get():.0f} | ❌: {error_counter._value.get():.0f}")

# Вход
def main():
    signal.signal(signal.SIGINT, shutdown_signal_handler)
    signal.signal(signal.SIGTERM, shutdown_signal_handler)

    try:
        asyncio.run(load_test())
    except KeyboardInterrupt:
        logger.warning("🧹 Прерывание с клавиатуры, завершаем...")

if __name__ == "__main__":
    main()
