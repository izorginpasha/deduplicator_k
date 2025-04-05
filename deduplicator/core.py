import json
from datetime import datetime, timedelta

class Deduplicator:
    def __init__(self, key_fields, time_field, window_days=7):
        self.key_fields = key_fields            # поля для сравнения
        self.time_field = time_field            # поле времени (например, dt_add)
        self.window = timedelta(days=window_days)
        self.records = {}  # ключ -> datetime

    def _generate_key(self, obj):
        return tuple(obj.get(field) for field in self.key_fields)

    def _parse_time(self, timestr):
        return datetime.fromisoformat(timestr)

    def add(self, obj):
        """Добавляет объект, если он не дубликат. Возвращает True если новый, False если дубликат."""
        key = self._generate_key(obj)
        obj_time = self._parse_time(obj[self.time_field])
        now = datetime.now()

        # Удаляем устаревшие записи
        self._cleanup(now)

        if key in self.records:
            # Уже есть такой ключ, проверим по времени
            if now - self.records[key] <= self.window:
                return False  # дубликат
        # Иначе добавляем
        self.records[key] = obj_time
        return True

    def _cleanup(self, current_time):
        expired_keys = [
            key for key, ts in self.records.items()
            if current_time - ts > self.window
        ]
        for key in expired_keys:
            del self.records[key]
