FROM python:3.12-slim


# Копируем requirements.txt
COPY requirements.txt .

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь код
COPY . /app
WORKDIR /app
ENV PYTHONPATH=/app
