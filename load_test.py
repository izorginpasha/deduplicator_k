import json
import httpx
from locust import HttpUser, task, between

# Загрузка JSON данных
with open('results-1743680955719.json', 'r') as f:
    events = json.load(f)

class EventLoadTest(HttpUser):
    wait_time = between(1, 2)  # Задержка между запросами (1-2 секунды)

    @task
    def post_event(self):
        event = events[self.random_event_index()]
        response = self.client.post("/event", json=event)
        if response.status_code != 200:
            print(f"Ошибка при отправке события: {response.status_code}")

    def random_event_index(self):
        # Функция для случайного выбора события из списка
        import random
        return random.randint(0, len(events) - 1)

