import hashlib
import json
from datetime import datetime
from typing import Any
from clickhouse_driver import Client
import asyncio


class ClickHouseManager:
    def __init__(self, client: Client):
        self.client = client

    async def create_events_table(self) -> None:
        create_table_query = """
            CREATE TABLE IF NOT EXISTS events (
                event_hash String,
                event_data String,
                created_at DateTime
            ) ENGINE = MergeTree()
            ORDER BY event_hash
            TTL created_at + INTERVAL 7 DAY
            SETTINGS ttl_only_drop_parts = 1;
        """
        await self._execute_query(create_table_query)

    async def is_duplicate(self, event_hash: str) -> bool:
        query = """
            SELECT count() 
            FROM events 
            WHERE event_hash = %(hash)s
        """
        result = await self._execute_query(query, {"hash": event_hash})
        return result[0][0] > 0

    async def insert_event(self, event_hash: str, event: dict) -> None:
        if not event_hash:
            raise ValueError("event_hash must be provided")

        # Сериализация данных события в JSON
        payload = json.dumps(event, ensure_ascii=False)

        # Получаем текущую дату и время для поля created_at
        created_at = datetime.utcnow()

        query = """
            INSERT INTO events (event_hash, event_data, created_at)
            VALUES
        """
        try:
            # Вставка хэша, сериализованного события и времени создания в таблицу
            await self._execute_query(query, [(event_hash, payload, created_at)])
        except Exception as e:
            print(f"Ошибка при вставке события: {e}")

    async def _execute_query(self, query: str, params: Any = None):
        loop = asyncio.get_event_loop()
        # Выполнение синхронной операции в отдельном потоке
        return await loop.run_in_executor(None, self.client.execute, query, params)
