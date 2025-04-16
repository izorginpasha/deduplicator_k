import hashlib
from typing import Any
from clickhouse_driver import Client

class ClickHouseManager:
    def __init__(self, client: Client):
        self.client = client

    def create_events_table(self) -> None:
        create_table_query = """
            CREATE TABLE IF NOT EXISTS events (
                event_hash String,
                created_at DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY event_hash
            TTL created_at + INTERVAL 7 DAY
            SETTINGS ttl_only_drop_parts = 1;
        """
        self.client.execute(create_table_query)

    def is_duplicate(self, event_hash: str) -> bool:
        query = """
            SELECT count() 
            FROM events 
            WHERE event_hash = %(hash)s
        """
        result = self.client.execute(query, {"hash": event_hash})
        return result[0][0] > 0

    def insert_event(self, event_hash: str) -> None:
        if not event_hash:
            raise ValueError("event_hash must be provided")
        query = """
            INSERT INTO events (event_hash)
            VALUES
        """
        self.client.execute(query, [[event_hash]])
