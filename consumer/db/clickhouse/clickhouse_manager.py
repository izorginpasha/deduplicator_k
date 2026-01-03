import json
from datetime import datetime
from clickhouse_driver import Client

INSERT_SQL = "INSERT INTO events.events (event_time, event_hash, payload) VALUES"

class ClickHouseManager:
    def __init__(self, client: Client):
        self.client = client

    def insert_batch(self, events: list[dict]):
        now = datetime.utcnow()
        rows = []

        for e in events:
            eh = e["event_hash"]
            if not isinstance(eh, (bytes, bytearray)) or len(eh) != 32:
                raise ValueError("event_hash must be bytes(32)")

            et = e.get("event_time", now)

            payload_dict = dict(e)
            # чтобы json.dumps не упал на bytes
            payload_dict["event_hash"] = bytes(eh).hex()
            payload = json.dumps(payload_dict, ensure_ascii=False)

            rows.append((et, bytes(eh), payload))

        self.client.execute(INSERT_SQL, rows)