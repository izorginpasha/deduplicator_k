import hashlib
import json
from redis.asyncio import Redis

class Deduplicator:
    def __init__(self, redis: Redis, ttl_seconds: int = 7 * 24 * 60 * 60):
        self.redis = redis
        self.ttl = ttl_seconds

    def _serialize_event(self, event: dict) -> str:
        # Поля, которые считаем "ключевыми" для уникальности
        keys_to_include = [
            "event_name", "userId", "client_id", "event_datetime","product_id","client_id_query"
        ]
        filtered = {k: event.get(k) for k in keys_to_include}
        return json.dumps(filtered, sort_keys=True)

    def _hash_event(self, event: dict) -> str:
        serialized = self._serialize_event(event)
        return hashlib.sha256(serialized.encode()).hexdigest()

    async def is_unique(self, event: dict) -> bool:
        event_hash = self._hash_event(event)
        return await self.redis.set(name=event_hash, value=1, ex=self.ttl, nx=True)
