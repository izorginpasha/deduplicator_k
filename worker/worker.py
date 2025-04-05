# worker/worker.py

import redis
import json
import time
from deduplicator.core import Deduplicator

redis_client = redis.Redis(host="localhost", port=6379, db=0)
deduplicator = Deduplicator()

def process_event(event: dict):
    if deduplicator.is_duplicate(event):
        print(f"[DUPLICATE] {event}")
    else:
        print(f"[NEW] {event}")
        # Здесь можно сохранить или обработать

def run_worker():
    print("Worker started...")
    while True:
        _, raw_event = redis_client.blpop("event_queue")
        event = json.loads(raw_event)
        process_event(event)

if __name__ == "__main__":
    run_worker()
