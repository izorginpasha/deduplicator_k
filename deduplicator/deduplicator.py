import json
import hashlib
from datetime import datetime
from typing import Dict, Any
from redis.asyncio import Redis
from pybloom_live import ScalableBloomFilter
from db.clickhouse_manager import ClickHouseManager

class Deduplicator:
    def __init__(self, redis: Redis, clickhouse: ClickHouseManager, ttl_seconds: int = 2 * 60 * 60):
        self.redis = redis
        self.ch = clickhouse
        self.redis_ttl = ttl_seconds
        self.bloom = ScalableBloomFilter(mode=ScalableBloomFilter.SMALL_SET_GROWTH)

    def _serialize_event(self, event: dict) -> str:
        # Преобразуем все значения datetime в строки ISO 8601
        def convert_datetime(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            return obj

        # Применяем преобразование ко всем значениям в event
        event_processed = {k: convert_datetime(v) for k, v in event.items()}
        return json.dumps(event_processed, sort_keys=True)

    def _hash_event(self, event: dict) -> str:
        serialized = self._serialize_event(event)
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    async def is_duplicate(self, event: dict) -> bool:
        event_hash = self._hash_event(event)

        # Проверяем наличие хеша в Redis
        if await self.redis.exists(event_hash):
            return True

        # Проверяем наличие в Bloom-фильтре (синхронный вызов)
        if event_hash in self.bloom:
            return True

        # Если в базе данных уже зарегистрирован — дубликат
        if self.ch.is_duplicate(event_hash):
            await self.redis.set(event_hash, 1, ex=self.redis_ttl)
            return True

        # Регистрируем хеш в Redis и Bloom-фильтре
        await self.redis.set(event_hash, 1, ex=self.redis_ttl)
        self.bloom.add(event_hash)
        self.ch.insert_event(event_hash)
        return False
