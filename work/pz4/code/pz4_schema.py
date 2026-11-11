# -*- coding: utf-8 -*-
"""Завдання 3: кількість документів, структура, BSON-типи полів, пропущені значення, розміри колекцій."""
import json, sys
from bson import json_util
sys.path.insert(0, __import__("os").path.dirname(__file__))
from pz4_common import *

db = get_db()
colls = ["candles_1m", "candles_1m_ts", "candles_1h", "candles_1d", "candles_1d_buckets", "spark_daily_features"]
# 1. кількість документів і розміри (collStats)
rows = []
for c in colls:
    s = db.command("collStats", c)
    rows.append(dict(collection=c, count=s["count"], avgObjSize_B=s.get("avgObjSize", 0), size_MB=round(s["size"] / 2**20, 2),
                     storageSize_MB=round(s["storageSize"] / 2**20, 2), totalIndexSize_MB=round(s["totalIndexSize"] / 2**20, 2),
                     nindexes=s["nindexes"]))
save_csv("pz4_storage", rows)
for r in rows: print(r)

# 2. BSON-типи кожного поля ($type) та пропущені значення (відсутнє / null / NaN)
types, miss = [], []
for c in colls:
    if c == "candles_1m_ts":
        continue
    fields = [k for k in db[c].find_one().keys() if k != "_id"]
    n = db[c].estimated_document_count()
    for f in fields:
        for r in db[c].aggregate([{"$group": {"_id": {"$type": "$" + f}, "n": {"$sum": 1}}}], allowDiskUse=True):
            types.append(dict(collection=c, field=f, bson_type=r["_id"], docs=r["n"]))
    grp = {"_id": None}
    for f in fields:
        grp[f] = {"$sum": {"$cond": [{"$or": [{"$eq": [{"$type": "$" + f}, "missing"]}, {"$eq": ["$" + f, None]},
                                                {"$eq": ["$" + f, float("nan")]}]}, 1, 0]}}
    r = next(db[c].aggregate([{"$group": grp}], allowDiskUse=True))
    for f in fields:
        miss.append(dict(collection=c, field=f, missing_or_null_or_nan=r[f], total_docs=n))
save_csv("pz4_field_types", types)
save_csv("pz4_missing_values", miss)

# 3. приклади документів (Extended JSON, дати як $date)
ex = {}
for c in colls:
    d = db[c].find_one({}, sort=[("_id", 1)]) if c != "candles_1d_buckets" else db[c].find_one()
    if c == "candles_1d_buckets": d["hours"] = d["hours"][:2] + [{"...": "ще %d годин" % (len(d["hours"]) - 2)}]
    ex[c] = json.loads(json_util.dumps(d, json_options=json_util.RELAXED_JSON_OPTIONS))
json.dump(ex, open(f"{RES}/pz4_sample_documents.json", "w"), ensure_ascii=False, indent=2)
print(json.dumps(ex["candles_1m"], indent=1))

# 4. порівняння розмірів для time-series: звичайна vs timeseries
ts = db.command("collStats", "candles_1m_ts")
json.dump(json.loads(json_util.dumps(ts)), open(f"{RES}/explain/collStats_candles_1m_ts.json", "w"), indent=1)
print({k: ts.get(k) for k in ("count", "size", "storageSize", "totalIndexSize")}, ts.get("timeseries"))
