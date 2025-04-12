# locustfile.py
import json
import random
from locust import HttpUser, task, between

# Загрузка событий
with open("results-1743680955719.json", "r", encoding="utf-8") as f:
    events_data = json.load(f)

class EventUser(HttpUser):
    wait_time = between(0.01, 0.1)  # имитация пользователей (задержка между запросами)

    @task
    def send_random_event(self):
        event = random.choice(events_data)
        with self.client.post("/event", json=event, catch_response=True) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"❌ Ошибка: {response.status_code} — {response.text}")
