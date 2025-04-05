import redis
import hashlib

class RedisDeduplicator:
    def __init__(self, redis_client, key_fields, window_days=7):
        self.r = redis_client
        self.key_fields = key_fields
        self.ttl = window_days * 86400  # в секундах

    def _generate_key(self, obj):
        parts = [str(obj.get(field, "")) for field in self.key_fields]
        raw = ":".join(parts)
        return f"dedup:{hashlib.sha256(raw.encode()).hexdigest()}"

    def add(self, obj):
        key = self._generate_key(obj)
        return self.r.set(key, 1, ex=self.ttl, nx=True) is not None
