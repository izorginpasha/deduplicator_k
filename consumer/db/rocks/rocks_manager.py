

import time, struct
from rocksdict import Rdict


class RocksDedupStore:
    def __init__(self, path: str, window_seconds: int):
        # Открываем rocks по указанному пути.
        # Это embedded-хранилище: живёт в этом же процессе,
        self.db = Rdict(path)

        # Окно дедупликации в секундах (например, 7 дней).
        # В течение этого времени одинаковые event_hash считаются дублями.
        self.window = window_seconds

    def _pack_ts(self, ts: int) -> bytes:
        # Сериализуем timestamp (int) в фиксированные 8 байт (uint64, big-endian).
        # rocks работает только с bytes → bytes.
        return struct.pack(">Q", ts)

    def _unpack_ts(self, b: bytes) -> int:
        # Обратная операция: из 8 байт получаем timestamp (int).
        return struct.unpack(">Q", b)[0]

    def is_dup_and_touch(self, event_hash: bytes) -> bool:

        # Проверяет, является ли событие дубликатом в пределах окна.
        # Возвращает:
        # - True  → событие уже было в окне (дубликат)
        # - False → событие уникально (и мы его регистрируем)

        # Текущее время в секундах (epoch time)
        now = int(time.time())

        # Пытаемся получить значение по ключу event_hash.
        v = self.db.get(event_hash)

        if v is not None:
            # Если ключ найден — значит событие уже встречалось.
            seen = self._unpack_ts(v)

            # Проверяем, попадает ли событие в окно дедупликации.
            if now - seen <= self.window:
                # Событие уже было в пределах окна → дубль.
                return True

        # Если ключа не было или он старше окна:
        # считаем событие уникальным и записываем текущий timestamp.
        self.db.put(event_hash, self._pack_ts(now))

        # Событие уникально.
        return False
