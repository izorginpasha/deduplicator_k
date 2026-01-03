
import consumer.db.rocks.rocks_manager as rocks_manager


class FakeRdict:

    # Мини-фейк вместо rocksdict.Rdict.

    def __init__(self, path: str):
        # path нам не важен, но оставляем для совместимости с настоящим Rdict(path)
        self.path = path
        # in-memory хранилище
        self.store = {}

    def get(self, key: bytes):
        # поведение как у Rocks: вернуть bytes или None
        return self.store.get(key)

    def put(self, key: bytes, value: bytes):
        # записываем значение по ключу
        self.store[key] = value


def test_new_event_returns_false_and_writes_ts(monkeypatch):
    # --- Arrange (подготовка) ---

    # 1) Подменяем Rdict внутри rocks_manager на FakeRdict.
    #    Это ключевая идея: RocksDedupStore создаёт self.db = Rdict(path),
    #   чтобы там создался FakeRdict, а не настоящая RocksDB.
    monkeypatch.setattr(rocks_manager, "Rdict", FakeRdict)

    # 2) Фиксируем время.
    #    Внутри is_dup_and_touch используется time.time(), а значит тест иначе будет "плавать".
    #    Мы делаем время постоянным: 1000 секунд.
    monkeypatch.setattr(rocks_manager.time, "time", lambda: 1_000)

    # 3) Создаём менеджер с окном 10 секунд (window_seconds=10).
    #    path может быть любым, потому что мы подменили RocksDB на FakeRdict.
    manager = rocks_manager.RocksDedupStore(path=":memory:", window_seconds=10)

    # event_hash должен быть bytes (у тебя так и в коде: event_hash: bytes)
    h = b"\x01" * 32

    # --- Act (действие) ---
    res = manager.is_dup_and_touch(h)

    # --- Assert (проверка) ---

    # Для нового события ожидаем False (не дубль)
    assert res is False

    # И ожидаем, что timestamp записался в "базу"
    assert manager.db.get(h) is not None


def test_duplicate_within_window_returns_true_and_does_not_update(monkeypatch):
    # Arrange: подменили базу на FakeRdict
    monkeypatch.setattr(rocks_manager, "Rdict", FakeRdict)

    # 1-й вызов в момент времени t=1000
    monkeypatch.setattr(rocks_manager.time, "time", lambda: 1_000)

    manager = rocks_manager.RocksDedupStore(path=":memory:", window_seconds=10)
    h = b"\x02" * 32

    # Act + Assert: первый раз события нет => False и запись в базу
    assert manager.is_dup_and_touch(h) is False

    # Запоминаем, что именно записали (8 байт timestamp в big-endian)
    first_value = manager.db.get(h)

    # 2-й вызов через 5 секунд: t=1005 (в пределах окна 10 сек)
    monkeypatch.setattr(rocks_manager.time, "time", lambda: 1_005)

    # Теперь должно быть True (дубликат)
    assert manager.is_dup_and_touch(h) is True

    # И важно: раз это дубль, перезаписи быть не должно
    assert manager.db.get(h) == first_value


def test_old_event_outside_window_returns_false_and_updates_ts(monkeypatch):
    monkeypatch.setattr(rocks_manager, "Rdict", FakeRdict)

    # 1-й вызов t=1000: записали timestamp
    monkeypatch.setattr(rocks_manager.time, "time", lambda: 1_000)

    manager = rocks_manager.RocksDedupStore(path=":memory:", window_seconds=10)
    h = b"\x03" * 32

    assert manager.is_dup_and_touch(h) is False
    first_value = manager.db.get(h)

    # 2-й вызов t=1011: прошло 11 секунд, окно = 10 -> событие считается "новым"
    monkeypatch.setattr(rocks_manager.time, "time", lambda: 1_011)

    # Должно вернуть False (не дубль), и timestamp должен обновиться
    assert manager.is_dup_and_touch(h) is False
    assert manager.db.get(h) != first_value


def test_pack_unpack_roundtrip(monkeypatch):
    # Здесь нам RocksDB вообще не нужна, но RocksDedupStore всё равно создаёт Rdict(path),
    # поэтому мы опять подменяем Rdict на FakeRdict, чтобы тест был изолированным.
    monkeypatch.setattr(rocks_manager, "Rdict", FakeRdict)

    manager = rocks_manager.RocksDedupStore(path=":memory:", window_seconds=10)

    # Берём произвольный timestamp
    ts = 123456789

    # pack -> bytes (8 байт)
    packed = manager._pack_ts(ts)

    assert isinstance(packed, bytes)  # это bytes
    assert len(packed) == 8           # ровно 8 байт (uint64)

    # unpack -> обратно в то же число
    assert manager._unpack_ts(packed) == ts