# -*- coding: utf-8 -*-
"""Завдання 6: індекси та порівняння виконання запитів до/після (explain executionStats).
Умови порівняння: той самий запит, один warm-up запуск, далі 5 вимірювань; у таблицю йде медіана.
Для 'до' усі вторинні індекси колекції видаляються, для 'після' створюється рівно один індекс."""
import sys, time, json, statistics, datetime as dt
from pz4_common import experiment
from bson import json_util
sys.path.insert(0, __import__("os").path.dirname(__file__))
from pz4_common import *
from pz4_common import drop_secondary, measure, experiment

db = get_db()
U = lambda *a: dt.datetime(*a, tzinfo=dt.timezone.utc)
REPS = 5


rows = []
# E1: діапазон за часом, одна доба (1440 документів)
rows += experiment("E1_range_day", "candles_1m", {"ts": {"$gte": U(2021, 5, 19), "$lt": U(2021, 5, 20)}}, [("ts", 1)], 0,
                   [("ts", 1)], "діапазон ts: 19.05.2021 (1440 хв)")
# E2: діапазон за часом, місяць (~44 640 документів)
rows += experiment("E2_range_month", "candles_1m", {"ts": {"$gte": U(2021, 5, 1), "$lt": U(2021, 6, 1)}}, [("ts", 1)], 0,
                   [("ts", 1)], "діапазон ts: травень 2021 (44 640 хв)")
# E3: топ-10 хвилин за обсягом (сортування + limit)
rows += experiment("E3_top_volume", "candles_1m", {}, [("volume", -1)], 10, [("volume", -1)], "топ-10 хвилин за volume")
# E4: складений індекс на добових даних: 2021 рік, обсяг вище 90-го перцентиля 2021 року
vols = sorted(d["volume"] for d in db.candles_1d.find({"year": 2021}, {"volume": 1}))
X = round(vols[int(0.9 * len(vols))], 2)
json.dump({"X_volume_p90_2021": X}, open(f"{RES}/pz4_index_params.json", "w"))
rows += experiment("E4_year_volume", "candles_1d", {"year": 2021, "volume": {"$gt": X}}, [("volume", -1)], 0,
                   [("year", 1), ("volume", -1)], f"year=2021 та volume>{X}, sort volume desc")
# E5: bucket pattern: усі 24 години доби 19.05.2021 – плоска колекція 1h проти документа-доби
drop_secondary("candles_1h"); drop_secondary("candles_1d_buckets")
rows += experiment("E5a_flat_hours", "candles_1h", {"ts": {"$gte": U(2021, 5, 19), "$lt": U(2021, 5, 20)}}, [("ts", 1)], 0,
                   [("ts", 1)], "24 години доби: плоска candles_1h")
rows += experiment("E5b_bucket_day", "candles_1d_buckets", {"date": U(2021, 5, 19)}, None, 0, [("date", 1)],
                   "24 години доби: один документ-доба (bucket)")
# лишаємо корисні індекси для подальших запитів/агрегацій
db.candles_1m.create_index([("ts", 1)]); db.candles_1m.create_index([("volume", -1)])
db.spark_daily_features.create_index([("date", 1)]); db.candles_1d.create_index([("date", 1)])
db.candles_1d.create_index([("year", 1), ("volume", -1)])
for r in rows:
    r["explain_ms_runs"] = json.dumps(r["explain_ms_runs"]); r["wall_ms_runs"] = json.dumps(r["wall_ms_runs"])
save_csv("pz4_index_comparison", rows)
idx = []
for c in db.list_collection_names():
    if c.endswith("_ts") or c.startswith("system"): continue
    st = db.command("collStats", c)
    for n, sz in st["indexSizes"].items(): idx.append(dict(collection=c, index=n, size_MiB=round(sz / 2**20, 3)))
save_csv("pz4_index_sizes", idx)
print("OK")
