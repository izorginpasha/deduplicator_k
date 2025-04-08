from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import json
import os
import httpx
from kafka import KafkaProducer
import uuid
from api.models.event import Event
from dotenv import load_dotenv
load_dotenv()

# Очередь для отправки событий
EVENT_TOPIC = os.getenv("TOPIC")
# Инициализация FastAPI приложения
app = FastAPI()

# Kafka продьюсер
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])




@app.post("/event")
async def post_event(event: Event):
    # Быстрая проверка и валидация
    try:
        # Генерация уникального ID для события
        event_id = str(uuid.uuid4())

        # Асинхронно отправляем событие в Kafka
        await send_to_kafka(event, event_id)

        # Быстрый ответ
        return {"message": "Event received successfully", "event_id": event_id}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def send_to_kafka(event: Event, event_id: str):
    # Преобразуем событие в JSON
    event_dict = event.dict()
    event_dict["event_id"] = event_id

    # Отправка события в Kafka
    producer.send(EVENT_TOPIC, value=json.dumps(event_dict).encode('utf-8'))
    producer.flush()
