

import hashlib
from cachetools import LRUCache


class Deduplicator:
    def __init__(self, max_size=100_000):
        self.cache = LRUCache(maxsize=max_size)

    def is_duplicate(self, event: dict) -> bool:
        event_str = str(sorted(event.items()))
        event_hash = hashlib.sha256(event_str.encode()).hexdigest()

        if event_hash in self.cache:
            return True
        else:
            self.cache[event_hash] = True
            return False
