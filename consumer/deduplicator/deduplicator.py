import json
import hashlib
import time
from datetime import datetime
from consumer.db.rocks.Rocks_manager import RocksDedupStore

class Deduplicator:
    def __init__(self, rocks: RocksDedupStore, ):
        self.rocks = rocks

    def _serialize_event(self, event: dict) -> str:
        # Преобразуем все значения datetime в строки ISO 8601
        def convert_datetime(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            return obj

        # Применяем преобразование ко всем значениям в event
        event_processed = {k: convert_datetime(v) for k, v in event.items()}
        return json.dumps(event_processed, sort_keys=True)

    def _hash_event(self, event: dict) -> bytes:
        serialized = self._serialize_event(event)
        return hashlib.sha256(serialized.encode("utf-8")).digest()

    async def is_duplicate(self, event: dict) -> bool:

        # True  -> дубль
        # False -> уникально (и мы его зарегистрировали в rocks)

        event_hash = self._hash_event(event)

        # Важное: rocks.is_dup_and_touch делает "check + register"
        # (если уникально — запоминает)
        return self.rocks.is_dup_and_touch(event_hash)



