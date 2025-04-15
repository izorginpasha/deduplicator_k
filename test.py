import json
import random
import argparse
import time
from locust import FastHttpUser, task, constant
from locust.env import Environment
from locust.stats import stats_printer
from prometheus_client import start_http_server, Counter
import gevent

# Метрики Prometheus
rps_counter = Counter("event_rps", "События в секунду")
success_counter = Counter("event_success_total", "Успешные события")
error_counter = Counter("event_error_total", "Ошибки событий")

# Загрузка событий
with open("results-1743680955719.json", "r", encoding="utf-8") as f:
    events_data = json.load(f)

# Пользователь Locust
class EventUser(FastHttpUser):  # Используем FastHttpUser вместо HttpUser
    wait_time = constant(0)  # Агрессивный режим, без задержек

    @task
    def send_event(self):
        event = random.choice(events_data)
        with self.client.post("/event", json=event, catch_response=True) as response:
            rps_counter.inc()
            if response.status_code == 200:
                success_counter.inc()
                response.success()
            else:
                error_counter.inc()
                response.failure(f"❌ Ошибка: {response.status_code} — {response.text}")

# CLI-парсинг
parser = argparse.ArgumentParser()
parser.add_argument("--host", required=True, help="URL сервера, например http://localhost:8000")
parser.add_argument("--users", type=int, default=10, help="Количество пользователей")
parser.add_argument("--spawn-rate", type=float, default=2, help="Скорость появления пользователей в секунду")
parser.add_argument("--duration", type=int, default=30, help="Продолжительность теста в секундах")
parser.add_argument("--prometheus-port", type=int, default=8001, help="Порт метрик Prometheus")

args = parser.parse_args()

def run_load_test():
    print(f"🚀 Запуск нагрузочного теста на {args.host}")
    start_http_server(args.prometheus_port)
    print(f"📈 Метрики Prometheus доступны на http://localhost:{args.prometheus_port}")

    # Создаём окружение Locust
    env = Environment(user_classes=[EventUser])
    env.create_local_runner()
    env.host = args.host

    gevent.spawn(stats_printer(env.stats))

    env.runner.start(args.users, spawn_rate=args.spawn_rate)
    gevent.sleep(args.duration)
    env.runner.quit()
    print("✅ Тест завершён")

    stats = env.stats.total
    print(f"\n📊 Итоги:")
    print(f"  ➤ Requests: {stats.num_requests}")
    print(f"  ➤ Failures: {stats.num_failures}")
    print(f"  ➤ RPS:      {stats.total_rps:.2f}")
    print(f"  ➤ Avg time: {stats.avg_response_time:.2f} ms")
    print(f"  ➤ 95%ile:   {stats.get_response_time_percentile(0.95):.2f} ms")

if __name__ == "__main__":
    run_load_test()
