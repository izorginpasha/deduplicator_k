FROM python:3.9-slim

# Установим рабочую директорию
WORKDIR /app

# Копируем файлы проекта в контейнер
COPY . /app
# Устанавливаем PYTHONPATH, чтобы Python знал о папке /app
ENV PYTHONPATH=/app:$PYTHONPATH

# Установим зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Открываем порт для FastAPI
EXPOSE 8000

# Запускаем FastAPI и Kafka-консюмер в одном контейнере с использованием команд оболочки
CMD ["sh", "-c", "uvicorn api.main:app --host 0.0.0.0 --port 8000 & python consumer/consumer.py"]
