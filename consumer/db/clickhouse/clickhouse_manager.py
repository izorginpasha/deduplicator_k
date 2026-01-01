import hashlib
import json
from datetime import datetime
from typing import Any
from clickhouse_driver import Client
import asyncio


class ClickHouseManager:
    def __init__(self, client: Client):
        self.client = client

    async def insert_batch(self, events: list[dict]):
        rows = []
        now = datetime.utcnow()

        for e in events:
            eh = e["event_hash"]  # bytes(32)
            et = e.get("event_time", now)
            payload = json.dumps(e, ensure_ascii=False)
            rows.append((et, eh, payload))

        await self.client.insert(
            "events",
            rows,
            column_names=["event_time", "event_hash", "payload"],
        )


