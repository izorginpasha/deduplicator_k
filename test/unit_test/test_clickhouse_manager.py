from unittest.mock import Mock
from consumer.db.clickhouse.clickhouse_manager import ClickHouseManager
# тест логика
def test_insert_calls_execute():
    """Проверяем, что insert_batch вызывает client.execute ровно один раз"""
    client = Mock()                     # фейковый клиент
    manager = ClickHouseManager(client)


    events = [
        {"event_hash": b"\x01" * 32}
    ]

    manager.insert_batch(events)

    client.execute.assert_called_once()