# -*- coding: utf-8 -*-
"""Завдання 7 (частина ПЗ3): колекція anomalies, індекси, запити та $lookup зі spark_daily_features.
Скрипт перезапускний: читає актуальний work/pz3/results/pz3_anomalies.csv, перезавантажує колекцію
та пише власні результати results/pz4_anom_*.csv, explain/anom_*.json. Обов'язкові колонки: timestamp, date, score; решта (v2: z_vol, sigma_ret, med_vol, rule_version тощо) завантажується як є."""
import sys, time, json, math, datetime as dt
import pandas as pd
from bson import json_util
sys.path.insert(0, __import__("os").path.dirname(__file__))
from pz4_common import *
from pz4_common import experiment, drop_secondary

db = get_db()
SRC = sys.argv[1] if len(sys.argv) > 1 else f"{W}/pz3/results/pz3_anomalies.csv"   # шлях до CSV можна передати аргументом
U = lambda *a: dt.datetime(*a, tzinfo=dt.timezone.utc)

# ---------- 1. завантаження ----------
df = pd.read_csv(SRC)
cols = list(df.columns)
t0 = time.perf_counter()
db.drop_collection("anomalies")
docs = []
for r in df.to_dict("records"):
    d = {}
    for k, v in r.items():
        if isinstance(v, float) and math.isnan(v): v = None            # NaN -> null
        elif hasattr(v, "item"): v = v.item()
        d[k] = v
    if "ts" in r: d["ts_epoch"] = int(r["ts"])                               # Int (секунди Unix)
    d["ts"] = dt.datetime.fromisoformat(str(r["timestamp"])).replace(tzinfo=dt.timezone.utc)   # Date (UTC)
    d["date"] = dt.datetime.fromisoformat(str(r["date"])[:10]).replace(tzinfo=dt.timezone.utc)
    d["year"] = d["date"].year
    del d["timestamp"]
    docs.append(d)
for i in range(0, len(docs), 20000): db.anomalies.insert_many(docs[i:i + 20000], ordered=False)
load_s = time.perf_counter() - t0
n = db.anomalies.count_documents({})
append_timing("load_anomalies", load_s, f"{n} docs з {SRC}")
save_csv("pz4_anom_load", [dict(source=SRC, csv_rows=len(df), mongo_docs=n, columns="|".join(cols), load_s=round(load_s, 2))])
print("loaded", n, cols)

# ---------- 2. типи полів і пропуски ----------
types = []
for f in [k for k in db.anomalies.find_one().keys() if k != "_id"]:
    for r in db.anomalies.aggregate([{"$group": {"_id": {"$type": "$" + f}, "n": {"$sum": 1}}}]):
        types.append(dict(field=f, bson_type=r["_id"], docs=r["n"]))
save_csv("pz4_anom_field_types", types)

# ---------- 3. індекс {date:1, score:-1}: до/після ----------
rows = experiment("ANOM1_year_top_score", "anomalies", {"date": {"$gte": U(2021, 1, 1), "$lt": U(2022, 1, 1)}}, [("score", -1)], 20,
                  [("date", 1), ("score", -1)], "аномалії 2021, топ-20 за score")
rows += experiment("ANOM2_day", "anomalies", {"date": U(2020, 3, 12)}, [("score", -1)], 0, [("date", 1), ("score", -1)],
                   "аномалії доби 12.03.2020, за score")
for r in rows:
    r["explain_ms_runs"] = json.dumps(r["explain_ms_runs"]); r["wall_ms_runs"] = json.dumps(r["wall_ms_runs"])
save_csv("pz4_anom_index_comparison", rows)
db.anomalies.create_index([("date", 1), ("score", -1)]); db.anomalies.create_index([("year", 1)])
save_csv("pz4_anom_indexes", [dict(index=k, key=str(v["key"])) for k, v in db.anomalies.index_information().items()])

# ---------- 4. агрегації ----------
def agg(name, coll, pipe):
    t = time.perf_counter(); res = list(db[coll].aggregate(pipe, allowDiskUse=True)); s = time.perf_counter() - t
    append_timing(f"agg_{name}", s, f"{coll}, {len(res)} рядків"); print(name, len(res), f"{s:.2f} с"); return res

# anomalies per year (+ частка від кількості хвилин не рахується, лише кількість)
res = agg("ANOM_per_year", "anomalies", [{"$group": {"_id": "$year", "n": {"$sum": 1}, "avg_score": {"$avg": "$score"},
                                                     "max_score": {"$max": "$score"}, "days": {"$addToSet": "$date"}}},
                                         {"$project": {"_id": 0, "year": "$_id", "n": 1, "avg_score": 1, "max_score": 1, "days": {"$size": "$days"}}},
                                         {"$sort": {"year": 1}}])
pd.DataFrame(res).to_csv(f"{RES}/pz4_anom_per_year.csv", index=False)

# $lookup: доба з аномаліями -> ознаки Spark (ПЗ2)
look = [{"$group": {"_id": "$date", "n_anom": {"$sum": 1}, "max_score": {"$max": "$score"}}},
        {"$lookup": {"from": "spark_daily_features", "localField": "_id", "foreignField": "date", "as": "f"}},
        {"$unwind": "$f"},
        {"$project": {"_id": 0, "date": "$_id", "n_anom": 1, "max_score": 1, "rv": "$f.rv", "hl_rel": "$f.hl_rel",
                      "ret_1d": "$f.ret_1d", "volume": "$f.volume", "flat_share": "$f.flat_share"}},
        {"$sort": {"n_anom": -1}}]
res = agg("ANOM_lookup_features", "anomalies", look)
out = pd.DataFrame(res); out["date"] = pd.to_datetime(out["date"]).dt.strftime("%Y-%m-%d")
out.to_csv(f"{RES}/pz4_anom_lookup_full.csv", index=False)
out.head(15).to_csv(f"{RES}/pz4_anom_lookup_top15.csv", index=False)
# підсумок: доби з аномаліями проти решти за середнім rv та обсягом
allf = pd.DataFrame(list(db.spark_daily_features.find({"is_complete": 1}, {"_id": 0, "date": 1, "rv": 1, "volume": 1, "hl_rel": 1})))
allf["date"] = pd.to_datetime(allf["date"]).dt.tz_localize(None).dt.strftime("%Y-%m-%d")
flag = allf.date.isin(set(out.date))
save_csv("pz4_anom_days_vs_rest", [dict(group=g, days=int(m.sum()), mean_rv=allf[m].rv.mean(), median_rv=allf[m].rv.median(),
                                          mean_hl_rel=allf[m].hl_rel.mean(), median_volume=allf[m].volume.median())
                                    for g, m in (("доби з аномаліями", flag), ("решта діб", ~flag))])
# розподіл score ($bucket) і за правилами
if "rule" in cols:
    res = agg("ANOM_by_rule", "anomalies", [{"$group": {"_id": "$rule", "n": {"$sum": 1}, "avg_score": {"$avg": "$score"}}}, {"$sort": {"n": -1}}])
    pd.DataFrame(res).rename(columns={"_id": "rule"}).to_csv(f"{RES}/pz4_anom_by_rule.csv", index=False)
res = agg("ANOM_top_dates", "anomalies", [{"$group": {"_id": "$date", "n": {"$sum": 1}}}, {"$sort": {"n": -1}}, {"$limit": 10}])
pd.DataFrame([dict(date=r["_id"].strftime("%Y-%m-%d"), n=r["n"]) for r in res]).to_csv(f"{RES}/pz4_anom_top_dates.csv", index=False)
print("OK")
