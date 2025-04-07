from fastapi import FastAPI, Request
import json
import redis.asyncio as redis

app = FastAPI()
r = redis.Redis(host='localhost', port=6379, db=1)

QUEUE_KEY = "events:queue"

@app.post("/event")
async def receive_event(request: Request):
    event = await request.json()
    await r.rpush(QUEUE_KEY, json.dumps(event))
    return {"status": "accepted"}
