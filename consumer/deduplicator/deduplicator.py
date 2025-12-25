import json
import hashlib
import time
from datetime import datetime
from redis.asyncio import Redis
from pybloom_live import ScalableBloomFilter
from consumer.db.clickhouse_manager import ClickHouseManager


class Deduplicator:
    def __init__(self, clickhouse: ClickHouseManager):
        self.ch = clickhouse

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
        # Если в базе данных уже зарегистрирован — дубликат
        # Асинхронная проверка
        is_duplicate_in_ch = await self.ch.is_duplicate(event_hash)
        if is_duplicate_in_ch:
            # Сразу сохраняем в Redis, если событие найдено в ClickHouse
            await self.redis.set(event_hash, 1, ex=self.redis_ttl)
            return True

        await self._register_event(event_hash, event)

        return False

    def _cleanup_bloom_filter(self):

        self.bloom = ScalableBloomFilter(mode=ScalableBloomFilter.SMALL_SET_GROWTH)
        self.last_cleanup_time = time.time()

    async def _register_event(self, event_hash: str, event: dict):

        # Сохраняем в Redis
        await self.redis.set(event_hash, 1, ex=self.redis_ttl)

        # Добавляем в Bloom-фильтре
        self.bloom.add(event_hash)
        print(f"Событие не найдено, регистрируем: {event_hash}")

        # Вставка в ClickHouse
        await self.ch.insert_event(event_hash, event)
