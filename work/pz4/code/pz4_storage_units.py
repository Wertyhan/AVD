# -*- coding: utf-8 -*-
"""M1: співвідношення розмірів у БАЙТАХ; МіБ (2^20) і МБ (10^6) подано явно. -> results/pz4_storage_units.csv"""
import os, sys
sys.path.insert(0, os.path.dirname(__file__))
from pz4_common import *
db = get_db()
CSV_BYTES = os.path.getsize(f"{W}/data/raw/btcusd_1-min_data.csv")    # 396 849 494 Б (вихідний CSV, data/README.md)
rows = [dict(object="CSV btcusd_1-min_data.csv", metric="файл", bytes=CSV_BYTES)]
for c in ["candles_1m", "candles_1m_ts", "candles_1h", "candles_1d", "candles_1d_buckets", "spark_daily_features", "anomalies"]:
    s = db.command("collStats", c)
    for m, k in (("size (BSON без стиснення; для TS - бакети)", "size"), ("storageSize (на диску)", "storageSize"), ("totalIndexSize", "totalIndexSize")):
        rows.append(dict(object=c, metric=m, bytes=s[k]))
for r in rows:
    r["MiB"] = round(r["bytes"] / 2**20, 2); r["MB"] = round(r["bytes"] / 1e6, 2)
    r["ratio_to_csv_bytes"] = round(r["bytes"] / CSV_BYTES, 3)
    r["loadavg_1m"] = f"{os.getloadavg()[0]:.2f}"
save_csv("pz4_storage_units", rows)
for r in rows: print(r)
print("candles_1m size / CSV =", db.command("collStats", "candles_1m")["size"] / CSV_BYTES)
