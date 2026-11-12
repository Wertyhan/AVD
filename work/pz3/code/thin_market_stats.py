# -*- coding: utf-8 -*-
"""Статистика тонкого ринку за роками та розподіл z_vol (докази для ANOMALY_DESIGN.md): results/pz3_thin_market_stats.csv, pz3_zvol_quantiles.csv."""
import numpy as np, pandas as pd
from pathlib import Path
PZ = Path(__file__).resolve().parents[1]; DATA = PZ.parent / "data"   # корінь ПЗ3 та каталог data
R = str(PZ / "results")
df = pd.read_parquet(str(DATA / "btc_1min.parquet"), columns=["Timestamp", "Close", "Volume"])
r = np.r_[np.nan, np.diff(np.log(df.Close.to_numpy()))]; act = df.Volume.to_numpy() > 0
y = pd.to_datetime(df.Timestamp, unit="s", utc=True).dt.year.to_numpy()
t = pd.DataFrame({"y": y, "act": act, "zero": (r == 0) & act, "absr": np.where(act, np.abs(r), np.nan)})
g = t.groupby("y").agg(active_share_pct=("act", "mean"), n_active=("act", "sum"), n_zero_ret_active=("zero", "sum"), median_abs_r_active=("absr", "median"))
g["active_share_pct"] = (100 * g.active_share_pct).round(1); g["zero_ret_share_of_active_pct"] = (100 * g.n_zero_ret_active / g.n_active).round(1)
g.to_csv(R + "/pz3_thin_market_stats.csv"); print(g.to_string())
s = pd.read_parquet(str(PZ / "run" / "offline_scored_v2.parquet"), columns=["ts", "scored", "z_vol", "sigma_ret"])
yy = pd.to_datetime(s.ts, unit="s", utc=True).dt.year
m = s.scored & (yy >= 2018)
q = np.nanpercentile(s.z_vol[m], [50, 90, 99, 99.9, 99.99])
lv = pd.read_parquet(str(PZ / "run" / "offline_scored_v2.parquet"), columns=["volume", "scored"])
out = pd.DataFrame({"stat": ["zvol_p50", "zvol_p90", "zvol_p99", "zvol_p99.9", "zvol_p99.99", "share_zvol_gt4_pct", "max_zvol"],
                    "value_2018plus": list(np.round(q, 3)) + [round(100 * float((s.z_vol[m] > 4).mean()), 5), round(float(s.z_vol[m].max()), 3)]})
out.to_csv(R + "/pz3_zvol_quantiles.csv", index=False); print(out.to_string(index=False))
