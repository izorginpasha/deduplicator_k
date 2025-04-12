from clickhouse_driver import Client
from datetime import datetime
import json

# Создание клиента для подключения к ClickHouse
# Подключение к ClickHouse
client = Client('localhost', port=9000, user='default', password='', database='default')

# SQL-запрос для создания таблицы
CREATE_TABLE_QUERY = '''
CREATE TABLE IF NOT EXISTS events (
    event_id String,
    event_name String,
    event_data String,  -- Или `JSON` если хотите использовать тип данных JSON
    created_at DateTime
) ENGINE = MergeTree()
ORDER BY created_at;
'''


async def save_event(event: dict):
    # Проверка на наличие таблицы и создание её, если она отсутствует
    try:
        # Выполнение запроса для проверки наличия таблицы
        client.execute('SHOW TABLES LIKE \'events\'')
        table_exists = True
    except Exception as e:
        table_exists = False

    if not table_exists:
        # Если таблица не существует, создаём её
        client.execute(CREATE_TABLE_QUERY)
        print("✅ Таблица 'events' была создана.")

    # Преобразуем событие в данные для вставки
    event_data = {
        "event_id": event['event_id'],
        "event_name": event['event_name'],
        "event_data": json.dumps(event),  # Сохраняем event как строку JSON
        "created_at": datetime.utcnow()
    }

    # Вставка данных в таблицу ClickHouse
    client.execute(
        'INSERT INTO events (event_id, event_name, event_data, created_at) VALUES',
        [(event_data['event_id'], event_data['event_name'], event_data['event_data'], event_data['created_at'])]
    )

    print(f"✅ Событие сохранено в ClickHouse с ID: {event['event_id']}")
