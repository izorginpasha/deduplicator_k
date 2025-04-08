import json
import hashlib
from redis.asyncio import Redis
from pybloom_live import ScalableBloomFilter

class Deduplicator:
    def __init__(self, redis: Redis, ttl_seconds: int = 7 * 24 * 60 * 60):
        self.redis = redis
        self.ttl = ttl_seconds
        self.bloom = ScalableBloomFilter(mode=ScalableBloomFilter.SMALL_SET_GROWTH)

    def _serialize_event(self, event: dict) -> str:
        keys = ["event_name", "userId", "client_id", "event_datetime", "product_id", "client_id_query"]
        filtered = {k: event.get(k) for k in keys}
        return json.dumps(filtered, sort_keys=True)

    def _hash_event(self, event: dict) -> str:
        serialized = self._serialize_event(event)
        return hashlib.sha256(serialized.encode()).hexdigest()

    async def is_duplicate(self, event: dict) -> bool:
        event_hash = self._hash_event(event)

        # Быстрая проверка в Redis
        if await self.redis.exists(event_hash):
            return True

        # Точная проверка в Bloom
        if event_hash in self.bloom:
            return True

        # Уникальное — сохраняем
        await self.redis.set(event_hash, 1, ex=self.ttl)
        self.bloom.add(event_hash)
        return False

