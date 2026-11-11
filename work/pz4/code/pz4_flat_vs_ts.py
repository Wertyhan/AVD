# -*- coding: utf-8 -*-
"""M4: 3 запити на candles_1m (плоска, індекс ts_1) та candles_1m_ts (time-series): доба, місяць, місячний OHLC за 2021 рік.
Прогрів + медіана з 5 (wall-clock клієнта, повне читання результату). -> results/pz4_flat_vs_ts.csv"""
import os, sys, time, statistics, datetime as dt
sys.path.insert(0, os.path.dirname(__file__))
from pz4_common import *
db = get_db(); U = lambda *a: dt.datetime(*a, tzinfo=dt.timezone.utc)
def rng(a, b): return {"ts": {"$gte": a, "$lt": b}}
ohlc = lambda a, b: [{"$match": rng(a, b)}, {"$sort": {"ts": 1}},
    {"$group": {"_id": {"$month": "$ts"}, "open": {"$first": "$open"}, "high": {"$max": "$high"}, "low": {"$min": "$low"},
                "close": {"$last": "$close"}, "volume": {"$sum": "$volume"}}}, {"$sort": {"_id": 1}}]
Q = [("доба 19.05.2021 (1440 хв)", lambda c: list(db[c].find(rng(U(2021, 5, 19), U(2021, 5, 20))))),
     ("місяць травень 2021 (44 640 хв)", lambda c: list(db[c].find(rng(U(2021, 5, 1), U(2021, 6, 1))))),
     ("місячний OHLC за 2021 рік ($match+$sort+$group)", lambda c: list(db[c].aggregate(ohlc(U(2021, 1, 1), U(2022, 1, 1)), allowDiskUse=True)))]
rows = []
for name, fn in Q:
    out = {}
    for c in ("candles_1m", "candles_1m_ts"):
        fn(c)                                   # прогрів
        ts_, la = [], []
        for _ in range(5):
            la.append(os.getloadavg()[0]); t = time.perf_counter(); r = fn(c); ts_.append((time.perf_counter() - t) * 1000)
        out[c] = (statistics.median(ts_), ts_, len(r), la)
    rows.append(dict(query=name, flat_ms_median=round(out["candles_1m"][0], 1), ts_ms_median=round(out["candles_1m_ts"][0], 1),
                     ts_over_flat=round(out["candles_1m_ts"][0] / out["candles_1m"][0], 2), n_returned_flat=out["candles_1m"][2],
                     n_returned_ts=out["candles_1m_ts"][2], flat_runs_ms=str([round(x, 1) for x in out["candles_1m"][1]]),
                     ts_runs_ms=str([round(x, 1) for x in out["candles_1m_ts"][1]]),
                     loadavg_1m=f"{min(out['candles_1m'][3] + out['candles_1m_ts'][3]):.2f}-{max(out['candles_1m'][3] + out['candles_1m_ts'][3]):.2f}"))
    print(rows[-1], flush=True)
rows[0]["indexes_flat"] = str(list(db.candles_1m.index_information())); rows[0]["indexes_ts"] = str(list(db.candles_1m_ts.index_information()))
save_csv("pz4_flat_vs_ts", rows)
