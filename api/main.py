# api/main.py

from fastapi import FastAPI
from pydantic import BaseModel
import redis
import json

app = FastAPI()

# Подключение к Redis
redis_client = redis.Redis(host="localhost", port=6379, db=0)

class Event(BaseModel):
    user_id: int
    action: str
    timestamp: int

@app.post("/event")
def receive_event(event: Event):
    event_data = event.dict()
    redis_client.rpush("event_queue", json.dumps(event_data))
    return {"status": "accepted"}
