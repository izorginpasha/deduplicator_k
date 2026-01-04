# locustfile.py
import json
import random
import uuid
import time
from locust import HttpUser, task, between

with open("../results-1743680955719.json", "r", encoding="utf-8") as f:
    events_data = json.load(f)

class EventUser(HttpUser):
    host = "http://localhost:8000"   # поменяй при необходимости
    wait_time = between(0.001, 0.01)

    @task
    def send_random_event(self):
        event = dict(random.choice(events_data))  # важно: копируем

        # если нужно, чтобы не было дедупа на одинаковых событиях:
        event_id = str(uuid.uuid4())
        event["event_id"] = event_id
        event["sent_at"] = int(time.time())

        with self.client.post("/event", json=event, name="/event", catch_response=True) as response:
            if response.status_code in (200, 201, 202):
                response.success()
            else:
                response.failure(f"{response.status_code}: {response.text[:300]}")
