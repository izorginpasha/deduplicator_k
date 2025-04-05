from fastapi import FastAPI, Request
import redis
import json

app = FastAPI()
r = redis.Redis(host='localhost', port=6379, db=0)

QUEUE_KEY = "events:queue"

@app.post("/event")
async def receive_event(request: Request):
    event = await request.json()
    r.rpush(QUEUE_KEY, json.dumps(event))
    return {"status": "accepted"}

