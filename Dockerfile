FROM python:3.12-slim

# Обновляем репозитории и устанавливаем необходимые пакеты
RUN apt-get update --allow-releaseinfo-change \
    && apt-get install -y ca-certificates curl gnupg build-essential \
    && apt-get clean

# Копируем requirements.txt
COPY requirements.txt .

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь код
COPY . /app
WORKDIR /app
ENV PYTHONPATH=/app
# Запуск приложения
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
