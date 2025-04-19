from fastapi import FastAPI, HTTPException
import json
import os
import uuid
from dotenv import load_dotenv
from aiokafka import AIOKafkaProducer

load_dotenv()

EVENT_TOPIC = os.getenv("TOPIC")  # значение по умолчанию
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")
app = FastAPI()

# Kafka продьюсер
producer: AIOKafkaProducer | None = None

# Старт приложения
@app.on_event("startup")
async def startup_event():
    global producer
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)  # Используйте правильный адрес
    await producer.start()
    print("🚀 Kafka producer запущен")


# Завершение приложения
@app.on_event("shutdown")
async def shutdown_event():
    if producer:
        await producer.stop()
        print("🛑 Kafka producer остановлен")


# Роут для приёма событий
@app.post("/event")
async def post_event(event: dict):  # Принимаем событие как обычный словарь
    try:
        event_id = str(uuid.uuid4())
        await send_to_kafka(event, event_id)
        return {"message": "Event received successfully", "event_id": event_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Функция отправки события в Kafka
async def send_to_kafka(event: dict, event_id: str):
    event["event_id"] = event_id  # добавляем event_id прямо в словарь

    try:
        payload = json.dumps(event).encode("utf-8")
    except TypeError as e:
        # Если какой-то тип данных не сериализуется в JSON
        raise ValueError(f"Ошибка сериализации события в JSON: {e}")

    if not producer:
        raise RuntimeError("Kafka producer is not initialized")

    await producer.send_and_wait(EVENT_TOPIC, payload)
    print(f"📤 Отправлено в Kafka: {event_id}")
