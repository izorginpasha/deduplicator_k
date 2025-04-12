from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import json
import os
import uuid
from dotenv import load_dotenv
from aiokafka import AIOKafkaProducer
from api.models.event import Event
from contextlib import asynccontextmanager
load_dotenv()

EVENT_TOPIC = os.getenv("TOPIC", "events")  # значение по умолчанию

app = FastAPI()
producer: AIOKafkaProducer | None = None  # глобальный продьюсер


# Создание асинхронного контекстного менеджера для обработки старта и завершения работы приложения
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Этот код выполняется при старте приложения
    producer = AIOKafkaProducer(bootstrap_servers="localhost:9092")
    await producer.start()
    print("🚀 Kafka producer запущен")
    yield producer  # передаем producer в FastAPI
    # Этот код выполняется при завершении работы приложения
    await producer.stop()
    print("🛑 Kafka producer остановлен")


app = FastAPI(lifespan=lifespan)


# Роут для приёма событий
@app.post("/event")
async def post_event(event: Event):
    try:
        event_id = str(uuid.uuid4())
        await send_to_kafka(event, event_id)
        return {"message": "Event received successfully", "event_id": event_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Функция отправки события в Kafka
async def send_to_kafka(event: Event, event_id: str):
    event_dict = event.dict()
    event_dict["event_id"] = event_id
    payload = json.dumps(event_dict).encode("utf-8")

    if not producer:
        raise RuntimeError("Kafka producer is not initialized")

    await producer.send_and_wait(EVENT_TOPIC, payload)
    print(f"📤 Отправлено в Kafka: {event_id}")
