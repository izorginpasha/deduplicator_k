import os
import ydb
from dotenv import load_dotenv
import traceback

load_dotenv()

# Загрузка переменных окружения
YDB_ENDPOINT = "grpc://localhost:2136"
YDB_DATABASE = "/local"


# Функция для создания драйвера с конфигурацией
def create_driver(endpoint, database):
    return ydb.Driver(
        endpoint=endpoint,
        database=database,
        credentials=ydb.AnonymousCredentials(),  # Явно отключаем авторизацию
    )


# Основная функция для работы с пулом сессий
def create_database(endpoint, database):
    driver = create_driver(endpoint, database)
    with driver:
        # Ожидание подключения с тайм-аутом 30 секунд
        driver.wait(timeout=5)

        try:
            # Создание пула сессий
            with ydb.QuerySessionPool(driver) as pool:
                # Выполнение операций с пулом
                with pool.session() as session:
                    # Пример запроса на создание базы данных
                    create_db_query = f"CREATE DATABASE IF NOT EXISTS {database}"
                    session.execute(create_db_query)
                    print(f"База данных {database} успешно создана.")
        except Exception as e:
            print(f"Ошибка при создании базы данных: {e}")
            traceback.print_exc()
        except TimeoutError:
            print("Не удалось подключиться к YDB в течение 30 секунд.")

def create_table(endpoint, database, table_name):
    driver = create_driver(endpoint, database)
    with driver:
        try:
            # Ожидание подключения с тайм-аутом 30 секунд
            driver.wait(timeout=5)
            # Создание пула сессий
            with ydb.QuerySessionPool(driver) as pool:
                # Выполнение операций с пулом
                with pool.session() as session:
                    create_table_query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            event_id STRING,
                            event_name STRING,
                            PRIMARY KEY (event_id)
                        )
                    """
                    session.execute(create_table_query)
                    print(f"Таблица {table_name} успешно создана.")
        except Exception as e:
            print(f"Ошибка при создании таблицы: {e}")
            traceback.print_exc()
        except TimeoutError:
            print("Не удалось подключиться к YDB в течение 30 секунд.")


# Основная синхронная функция
def main():
    create_database(YDB_ENDPOINT, YDB_DATABASE)
    create_table(YDB_ENDPOINT, YDB_DATABASE, 'events')


# Запуск программы
if __name__ == "__main__":
    main()
