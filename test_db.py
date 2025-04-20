import asyncio
from clickhouse_driver import Client
from db.clickhouse_manager import ClickHouseManager

# Подключение к ClickHouse
client = Client(host='clickhouse', port=9000, user='default', password='my_password', database='your_database_name')

# Инициализация менеджера
ch_manager = ClickHouseManager(client)


# Асинхронная функция для выполнения операций
async def main():
    # Создание таблицы
    await ch_manager.create_events_table()

    # Получение списка таблиц
    tables = client.execute("SHOW TABLES")
    print(tables)


# Запуск асинхронной функции
if __name__ == "__main__":
    asyncio.run(main())