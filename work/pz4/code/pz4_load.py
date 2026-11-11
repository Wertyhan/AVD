# -*- coding: utf-8 -*-
"""Завдання 2-3, 7: створення бази bigdata_db, колекцій та імпорт даних.
Колекції: candles_1m (повні 7,8 млн), candles_1m_ts (time-series), candles_1h, candles_1d,
candles_1d_buckets (bucket pattern), spark_daily_features (результати ПЗ2).
Заміряється пропускна здатність insert_many."""
import sys, time, datetime, math
import numpy as np, pandas as pd
from bson.int64 import Int64
from pymongo import MongoClient
sys.path.insert(0, __import__("os").path.dirname(__file__))
from pz4_common import *

DATA = f"{W}/data"
BATCH = 20000
db = get_db()
log = []


def utc_list(series):
    """Конвертація datetime64 (UTC) у список datetime з tz=UTC."""
    return list(pd.DatetimeIndex(series).tz_convert("UTC").to_pydatetime()) if getattr(series.dt, "tz", None) \
        else list(pd.DatetimeIndex(series).tz_localize("UTC").to_pydatetime())


def load_1m(name, ts_mode=False, limit=None):
    """Імпорт хвилинних свічок батчами; повертає (рядків, сек. побудови, сек. вставки)."""
    df = pd.read_parquet(f"{DATA}/btc_1min.parquet")
    if limit: df = df.iloc[:limit]
    db.drop_collection(name)
    if ts_mode:
        db.create_collection(name, timeseries={"timeField": "ts", "metaField": "meta", "granularity": "minutes"})
    coll = db[name]
    n = len(df); t_build = t_ins = 0.0
    for s in range(0, n, BATCH):
        c = df.iloc[s:s + BATCH]
        t0 = time.perf_counter()
        ts = utc_list(c["datetime_utc"]); un = c["Timestamp"].tolist()
        o, h, l, cl, v = (c[k].tolist() for k in ("Open", "High", "Low", "Close", "Volume"))
        if ts_mode:
            docs = [{"ts": ts[i], "meta": {"symbol": "BTCUSD"}, "unix": Int64(un[i]), "open": o[i], "high": h[i],
                     "low": l[i], "close": cl[i], "volume": v[i]} for i in range(len(c))]
        else:
            docs = [{"ts": ts[i], "unix": Int64(un[i]), "open": o[i], "high": h[i], "low": l[i],
                     "close": cl[i], "volume": v[i]} for i in range(len(c))]
        t1 = time.perf_counter()
        coll.insert_many(docs, ordered=False)
        t2 = time.perf_counter()
        t_build += t1 - t0; t_ins += t2 - t1
        if (s // BATCH) % 40 == 0: print(name, s, n, f"{t_ins:.0f}s", flush=True)
    return n, t_build, t_ins


def clean(v):
    """NaN -> None (пропущене значення в MongoDB зберігається як null)."""
    return None if isinstance(v, float) and math.isnan(v) else v


# ---------- candles_1m ----------
for name, ts_mode in (("candles_1m", False), ("candles_1m_ts", True)):
    n, tb, ti = load_1m(name, ts_mode)
    log.append(dict(collection=name, docs=n, batch_size=BATCH, build_docs_s=round(tb, 2), insert_s=round(ti, 2),
                    insert_docs_per_s=round(n / ti), total_s=round(tb + ti, 2)))
    append_timing(f"load_{name}", tb + ti, f"{n} docs, insert only {ti:.1f}s, {n/ti:.0f} docs/s")
    print(log[-1], flush=True)

# ---------- candles_1h, candles_1d, candles_1d_buckets ----------
h = pd.read_parquet(f"{DATA}/btc_1h.parquet"); d = pd.read_parquet(f"{DATA}/btc_1d.parquet")
t0 = time.perf_counter()
db.drop_collection("candles_1h"); db.drop_collection("candles_1d"); db.drop_collection("candles_1d_buckets")
tsh = utc_list(h["datetime_utc"])
db.candles_1h.insert_many([{"ts": tsh[i], "open": r.Open, "high": r.High, "low": r.Low, "close": r.Close,
                            "volume": r.Volume} for i, r in enumerate(h.itertuples())], ordered=False)
tsd = utc_list(d["datetime_utc"])
db.candles_1d.insert_many([{"date": tsd[i], "year": tsd[i].year, "month": tsd[i].month, "open": r.Open, "high": r.High,
                            "low": r.Low, "close": r.Close, "volume": r.Volume, "n_minutes": r.n_minutes}
                           for i, r in enumerate(d.itertuples())], ordered=False)
# bucket pattern: один документ = доба з вкладеним масивом 24 годинних свічок
h["day"] = h["datetime_utc"].dt.floor("D")
buckets = []
for day, g in h.groupby("day"):
    dd = pd.Timestamp(day).tz_convert("UTC").to_pydatetime() if pd.Timestamp(day).tzinfo else pd.Timestamp(day).tz_localize("UTC").to_pydatetime()
    buckets.append({"date": dd, "year": dd.year, "month": dd.month, "n_hours": len(g),
                    "hours": [{"hour": int(x.datetime_utc.hour), "open": x.Open, "high": x.High, "low": x.Low,
                               "close": x.Close, "volume": x.Volume} for x in g.itertuples()]})
db.candles_1d_buckets.insert_many(buckets, ordered=False)

# ---------- spark_daily_features (ПЗ2) ----------
f = pd.read_parquet(f"{W}/pz2/results/pz2_daily_features.parquet")
nan_counts = {c: int(f[c].isna().sum()) for c in f.columns if f[c].isna().any()}
db.drop_collection("spark_daily_features")
docs = []
for r in f.to_dict("records"):
    doc = {k: clean(v) for k, v in r.items()}
    doc["date"] = r["date"].to_pydatetime().replace(tzinfo=datetime.timezone.utc)
    for k in ("year", "month", "dow", "is_complete"): doc[k] = int(r[k])
    doc["n_minutes"] = int(r["n_minutes"]); doc["n_flat"] = int(r["n_flat"])
    docs.append(doc)
db.spark_daily_features.insert_many(docs, ordered=False)
append_timing("load_small_collections", time.perf_counter() - t0, "candles_1h, candles_1d, buckets, spark_daily_features")
save_csv("pz4_load_throughput", log)
save_csv("pz4_spark_nan_to_null", [dict(field=k, nan_replaced_by_null=v) for k, v in nan_counts.items()])
print("DONE", {c: db[c].estimated_document_count() for c in db.list_collection_names()})
