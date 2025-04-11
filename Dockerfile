# Используем официальный Python образ
FROM python:3.9-slim

# Устанавливаем рабочую директорию
WORKDIR /app

# Устанавливаем системные зависимости и YDB CLI
RUN apt-get update && \
    apt-get install -y curl && \
    curl -sSL https://storage.yandexcloud.net/ydb-releases/ydb-linux-amd64 -o /usr/local/bin/ydb && \
    chmod +x /usr/local/bin/ydb

# Копируем файлы проекта в контейнер
COPY . /app

# Устанавливаем зависимости из requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Открываем порт для FastAPI
EXPOSE 8000

# Команда для запуска приложения
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
