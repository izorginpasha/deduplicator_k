import hashlib
import json
from datetime import datetime
from typing import Any
from clickhouse_driver import Client
import asyncio


class ClickHouseManager:
    def __init__(self, client: Client):
        self.client = client




    async def insert_batch(self, insert_batch: list) -> None:


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
