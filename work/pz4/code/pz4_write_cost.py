# -*- coding: utf-8 -*-
"""M3: вартість індексів при записі. 200 000 документів (перші за часом), BATCH=20000, у scratch-колекцію:
(a) без вторинних індексів, (b) з ts_1 + volume_-1 створеними ДО вставки; (c) час побудови обох індексів ПІСЛЯ вставки.
Медіана з 3 запусків; scratch-колекція видаляється наприкінці. -> results/pz4_write_cost.csv"""
import os, sys, time, statistics
import pandas as pd
from bson.int64 import Int64
sys.path.insert(0, os.path.dirname(__file__))
from pz4_common import *
N, BATCH, RUNS = 200000, 20000, 3
db = get_db(); S = "scratch_write_cost"
df = pd.read_parquet(f"{W}/data/btc_1min.parquet").iloc[2_000_000:2_000_000 + N]   # фрагмент ~2014 р. (ліквідний ринок)
ts = list(pd.DatetimeIndex(df["datetime_utc"]).tz_convert("UTC").to_pydatetime()) if getattr(df["datetime_utc"].dt, "tz", None) \
    else list(pd.DatetimeIndex(df["datetime_utc"]).tz_localize("UTC").to_pydatetime())
un, o, h, l, c, v = (df[k].tolist() for k in ("Timestamp", "Open", "High", "Low", "Close", "Volume"))
mk = lambda a, b: [{"ts": ts[i], "unix": Int64(un[i]), "open": o[i], "high": h[i], "low": l[i], "close": c[i], "volume": v[i]} for i in range(a, b)]
def insert_all():
    t = 0.0
    for s in range(0, N, BATCH):
        docs = mk(s, min(s + BATCH, N)); t0 = time.perf_counter(); db[S].insert_many(docs, ordered=False); t += time.perf_counter() - t0
    return t
res = {"no_index": [], "ts_1+volume_-1": [], "build_after": []}; load = []
for _ in range(RUNS):
    db.drop_collection(S); db[S].create_collection if False else None
    res["no_index"].append(insert_all()); load.append(os.getloadavg()[0])
    db.drop_collection(S); db[S].create_index([("ts", 1)]); db[S].create_index([("volume", -1)])
    res["ts_1+volume_-1"].append(insert_all()); load.append(os.getloadavg()[0])
    db.drop_collection(S); insert_all()
    t0 = time.perf_counter(); db[S].create_index([("ts", 1)]); db[S].create_index([("volume", -1)]); res["build_after"].append(time.perf_counter() - t0)
db.drop_collection(S)
assert S not in db.list_collection_names()
med = {k: statistics.median(x) for k, x in res.items()}
rows = []
for k, lab in (("no_index", "без вторинних індексів"), ("ts_1+volume_-1", "з індексами ts_1 і volume_-1 (створені до вставки)")):
    rows.append(dict(variant=lab, docs=N, batch_size=BATCH, insert_s_median=round(med[k], 3), insert_docs_per_s_median=round(N / med[k]),
                     runs_s=str([round(x, 3) for x in res[k]]), slowdown_pct=round((med[k] / med["no_index"] - 1) * 100, 1),
                     throughput_loss_pct=round((1 - med["no_index"] / med[k]) * 100, 1)))
rows.append(dict(variant="побудова ts_1 + volume_-1 після вставки (200 000 док.)", docs=N, batch_size=BATCH, insert_s_median=round(med["build_after"], 3),
                 insert_docs_per_s_median="", runs_s=str([round(x, 3) for x in res["build_after"]]), slowdown_pct="", throughput_loss_pct=""))
for r in rows: r["loadavg_1m_during"] = f"{min(load):.2f}-{max(load):.2f}"
save_csv("pz4_write_cost", rows)
for r in rows: print(r)
