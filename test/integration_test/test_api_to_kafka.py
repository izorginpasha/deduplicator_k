import asyncio
import os
import pytest
import uuid
import httpx
from httpx import AsyncClient
from aiokafka import AIOKafkaConsumer




@pytest.mark.asyncio
async def test_event_goes_to_kafka():
    topic = os.getenv("TOPIC")
    API_URL = "http://api:8000"
    bootstrap = "kafka:9092"

    # 1) запускаем consumer ДО отправки
    consumer_task = asyncio.create_task(_consume_one(topic, bootstrap))

    # 2) даём consumer подписаться
    await asyncio.sleep(0.5)

    # 3) отправляем событие в API
    payload = {"event_hash": "01" * 32, "source": "itest"}
    async with httpx.AsyncClient(base_url=API_URL, timeout=5.0) as ac:
        r = await ac.post("/event", json=payload)

    assert r.status_code in (200, 201), r.text

    # 4) ждём, что consumer поймает сообщение (таймаут обязателен)
    msg_value = await asyncio.wait_for(consumer_task, timeout=5.0)

    # 5) проверка (тут зависит от формата сообщения)
    # если ты отправляешь JSON строкой:
    assert "itest" in msg_value


async def _consume_one(topic: str, bootstrap: str) -> str:
    """
    Читает ОДНО сообщение из Kafka и возвращает value (как строку).
    """
    c = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        auto_offset_reset="latest",  # только новое
        enable_auto_commit=False,
        group_id=f"itest-{os.urandom(6).hex()}",
    )
    await c.start()
    try:
        await asyncio.sleep(0.2)
        # чтобы  не читать старое:
        await c.seek_to_end()

        msg = await c.getone()
        raw = msg.value
        if isinstance(raw, (bytes, bytearray)):
            return raw.decode("utf-8", errors="replace")
        return str(raw)
    finally:
        await c.stop()
