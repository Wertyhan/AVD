# -*- coding: utf-8 -*-
"""Перевірка віконних агрегацій Spark проти pandas.resample на тих самих хвилинах (ПЗ3, завдання 5)."""
import sys, os, json
import numpy as np, pandas as pd
R = sys.argv[1]; RAW = sys.argv[2]
df = pd.read_csv(RAW)
df.columns = [c.strip() for c in df.columns]
PER = [("2020-03-09", "2020-03-16"), ("2021-05-17", "2021-05-22"), ("2022-11-06", "2022-11-12")]
m = np.zeros(len(df), bool)
for s, e in PER:
    lo = int(pd.Timestamp(s, tz="UTC").timestamp()); hi = int((pd.Timestamp(e, tz="UTC") + pd.Timedelta(days=1)).timestamp())
    m |= ((df.Timestamp >= lo) & (df.Timestamp < hi)).to_numpy()
d = df[m].copy(); d.index = pd.to_datetime(d.Timestamp, unit="s")
print("рядків у періодах:", len(d))
rows = []
for name, rule in (("win15m", "15min"), ("win1h", "1h")):
    ref = d.resample(rule, label="left", closed="left").agg(
        {"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum", "Timestamp": "count"}).dropna(subset=["Open"])
    ref.columns = ["open", "high", "low", "close", "volume", "n_candles"]
    sp = pd.read_csv(os.path.join(R, f"{name}_all.csv"), parse_dates=["win_start", "win_end"])
    # memory-приймач в режимі update зберігає усі проміжні оновлення вікна: лишаємо останнє (з найбільшим n_candles)
    n_raw = len(sp)
    sp = sp.sort_values(["win_start", "n_candles"]).drop_duplicates("win_start", keep="last")
    sp.to_csv(os.path.join(R, f"{name}_final.csv"), index=False)
    sp["win_start"] = pd.to_datetime(sp["win_start"]).dt.tz_localize(None) if sp["win_start"].dt.tz is None else sp["win_start"].dt.tz_convert("UTC").dt.tz_localize(None)
    sp = sp.set_index("win_start").sort_index()
    j = ref.join(sp, how="outer", lsuffix="_pd", rsuffix="_sp")
    only_pd = int(j["open_sp"].isna().sum()); only_sp = int(j["open_pd"].isna().sum())
    both = j.dropna(subset=["open_pd", "open_sp"])
    diffs = {c: float(np.max(np.abs(both[c + "_pd"] - both[c + "_sp"]))) for c in ["open", "high", "low", "close", "volume", "n_candles"]}
    row = {"window": name, "n_pandas": len(ref), "n_spark_raw_updates": n_raw, "n_spark": len(sp), "only_in_pandas": only_pd, "only_in_spark": only_sp,
           "n_compared": len(both), **{"max_abs_diff_" + k: v for k, v in diffs.items()},
           "all_match": bool(only_pd == 0 and only_sp == 0 and max(diffs.values()) < 1e-6)}
    # вікна, записані parquet-приймачем у режимі append (лише закриті водяною позначкою)
    pq = os.path.join(R, "parquet", name)
    if os.path.isdir(pq):
        pp = pd.read_parquet(pq)
        pp["win_start"] = pd.to_datetime(pp["win_start"]).dt.tz_localize(None) if pd.to_datetime(pp["win_start"]).dt.tz is None else pd.to_datetime(pp["win_start"]).dt.tz_convert("UTC").dt.tz_localize(None)
        pp = pp.set_index("win_start").sort_index()
        jj = ref.join(pp, how="inner", lsuffix="_pd", rsuffix="_sp")
        row["parquet_sink_windows"] = len(pp)
        row["parquet_sink_windows_dup"] = int(pp.index.duplicated().sum())
        row["parquet_sink_max_abs_diff"] = float(max(np.max(np.abs(jj[c + "_pd"] - jj[c + "_sp"])) for c in ["open", "high", "low", "close", "volume", "n_candles"]))
        row["parquet_sink_not_flushed_windows"] = len(ref) - len(pp)
    rows.append(row)
    print(row)
pd.DataFrame(rows).to_csv(os.path.join(os.path.dirname(R.rstrip("/")), "pz3_window_verification.csv"), index=False)
