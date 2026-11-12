# -*- coding: utf-8 -*-
"""Порівняння вхідного parquet ПЗ2: попередній запуск (_old_run) проти чистого перезапуску; опорні числа ПЗ2 для звіту.
Запуск: python code/input_diff.py   ->  results/pz5_input_diff.csv, results/pz2_reference_numbers.csv"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pz5_common import RES, save
P2 = "/home/user/AVD/work/pz2"
o = pd.read_parquet(f"{P2}/_old_run/results/pz2_hourly_features.parquet").set_index("datetime_utc")
n = pd.read_parquet(f"{P2}/results/pz2_hourly_features.parquet").set_index("datetime_utc")
rows = [("rows_old", len(o), len(o[o.index.year >= 2018]), None, None), ("rows_new", len(n), len(n[n.index.year >= 2018]), None, None),
        ("same_datetime_index (1=yes)", int(o.index.equals(n.index)), int(o.index.equals(n.index)), None, None),
        ("same_columns (1=yes)", int(list(o.columns) == list(n.columns)), int(list(o.columns) == list(n.columns)), None, None)]
common = o.index.intersection(n.index)
mod = common[common.year >= 2018]
for c in o.columns:
    a, b = o.loc[common, c], n.loc[common, c]
    if a.dtype.kind == "f":
        dif = ~np.isclose(a, b, rtol=1e-9, atol=0, equal_nan=True)
        rows.append((c, int(dif.sum()), int(dif[a.index.year >= 2018].sum()), float((a - b).abs().max()), float(((a - b).abs() / a.abs().clip(lower=1e-12)).max())))
    else:
        dif = (a != b).values
        rows.append((c, int(dif.sum()), int(dif[a.index.year >= 2018].sum()), 0.0, 0.0))
out = pd.DataFrame(rows, columns=["item", "n_rows_differing_all_years", "n_rows_differing_2018plus", "max_abs_diff", "max_rel_diff"])
# уточнення: у яких годинах відрізняється rv
dif = ~np.isclose(o.loc[common, "rv"], n.loc[common, "rv"], rtol=1e-9, atol=0, equal_nan=True)
di = common[dif]
fm = (di.day == 1) & (di.hour == 0)
extra = pd.DataFrame([("rv_diff_rows_at_month_start_00h", int(fm.sum()), int((fm & (di.year >= 2018)).sum()), None, None),
                      ("rv_diff_rows_other_hours", int((~fm).sum()), int(((~fm) & (di.year >= 2018)).sum()), None, None)],
                     columns=out.columns)
save(pd.concat([out, extra]), "pz5_input_diff")
print(pd.concat([out, extra]).to_string(index=False))
# опорні числа ПЗ2, що використовує звіт ПЗ5 (RULES, Integrity п. 1)
sk = pd.read_csv(f"{P2}/results/pz2_skew_kurtosis.csv"); sk = sk[sk.iloc[:, 0] == "1 год, усі"].iloc[0]
top = pd.read_csv(f"{P2}/results/pz2_top10_rv.csv")
vh = pd.read_csv(f"{P2}/results/pz2_volume_by_hour.csv").sort_values("volume_total", ascending=False).iloc[0]
cl = pd.read_csv(f"{P2}/results/pz2_cleaning.csv").set_index("metric").value
ref = pd.DataFrame([("pz2_rows_raw", int(cl["rows_raw"])), ("pz2_hourly_rows", len(n)),
                    ("pz2_hourly_return_excess_kurtosis_all", sk.iloc[5]),
                    ("pz2_flat_share_2012_01_27", float(top[top.date == "2012-01-27"].flat_share.iloc[0])),
                    ("pz2_top_volume_hour_utc", int(vh.hour)), ("pz2_top_volume_hour_share_pct", float(vh.share_pct))], columns=["parameter", "value"])
save(ref, "pz2_reference_numbers")
print(ref.to_string(index=False))
