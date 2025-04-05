import redis
import json
import time
from deduplicator.redis_deduplicator import RedisDeduplicator

r = redis.Redis(host='localhost', port=6379, db=0)
QUEUE_KEY = "events:queue"

dedup = RedisDeduplicator(r, key_fields=["user_id", "action", "timestamp"])

def process(event):
    print(f"✅ Обрабатываем уникальное событие: {event}")
    # Логика обработки события (например, логирование или другие действия)

print("Воркер запущен и ожидает событий...")  # Строка для отладки

while True:
    _, raw = r.blpop(QUEUE_KEY)  # Блокирующее ожидание
    event = json.loads(raw)

    if dedup.add(event):
        process(event)
    else:
        print(f"⛔ Дубликат: {event}")

    time.sleep(0.01)  # чтобы не крутилось слишком быстро