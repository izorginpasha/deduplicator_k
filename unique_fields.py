import json
from collections import defaultdict, Counter

# Загружаем JSON — он должен быть списком объектов
with open("results-1743680955719.json", "r", encoding="utf-8") as f:
    data = json.load(f)

# Собираем значения по каждому полю
field_values = defaultdict(list)

for obj in data:
    for key, value in obj.items():
        field_values[key].append(value)

# Находим поля с наименьшей средней частотой повторения
field_repeat_stats = []

for field, values in field_values.items():
    value_counts = Counter(values)
    total_values = len(values)
    total_unique = len(value_counts)

    # Среднее число повторений одного значения
    average_repeats = total_values / total_unique if total_unique else float("inf")

    # Количество уникальных (одиночных) значений
    unique_once = sum(1 for v in value_counts.values() if v == 1)

    field_repeat_stats.append({
        "field": field,
        "avg_repeats": average_repeats,
        "unique_once": unique_once,
        "total_values": total_values
    })

# Сортируем поля: сначала по наименьшей средней частоте повторений, потом по числу уникальных значений
sorted_fields = sorted(field_repeat_stats, key=lambda x: (x["avg_repeats"], -x["unique_once"]))

# Выводим топ-5 полей
print("Поля с наименее повторяющимися значениями:")
for field_stat in sorted_fields[:5]:
    print(
        f"{field_stat['field']}: avg_repeats={field_stat['avg_repeats']:.2f}, unique_once={field_stat['unique_once']}")

for obj in data:
    for key, value in obj.items():
        field_values[key].append(value)

# Считаем количество уникальных значений по каждому полю
unique_fields = []
for field, values in field_values.items():
    value_counts = Counter(values)
    unique_count = sum(1 for v in value_counts.values() if v == 1)

    if unique_count == len(values):  # Все значения уникальны
        unique_fields.append(field)

print("Поля с уникальными значениями у каждого объекта:")
print(unique_fields)
