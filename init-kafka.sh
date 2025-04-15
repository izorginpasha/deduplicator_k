#!/bin/bash

# Ожидаем, пока кластер поднимется
sleep 30
TOPIC="${TOPIC:events-stream}"
# Создание топика (можно поменять параметры)
kafka-topics.sh --create \
  --bootstrap-server kafka1:9092 \
  --replication-factor 3 \
  --partitions 3 \
  --topic "$TOPIC"

# Вывод списка топиков
kafka-topics.sh --create --bootstrap-server kafka1:9092 --replication-factor 3 ...

