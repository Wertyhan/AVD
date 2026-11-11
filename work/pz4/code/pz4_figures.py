# -*- coding: utf-8 -*-
"""Рисунки ПЗ4 (підписи українською, dpi 150) за CSV з results/."""
import pandas as pd, numpy as np, matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import sys; sys.path.insert(0, __import__("os").path.dirname(__file__))
from pz4_common import RES, FIG
plt.rcParams.update({"font.size": 10, "axes.spines.top": False, "axes.spines.right": False, "axes.grid": True, "grid.alpha": .3})
C1, C2 = "#c0504d", "#2e75b6"
ic = pd.concat([pd.read_csv(f"{RES}/pz4_index_comparison.csv"), pd.read_csv(f"{RES}/pz4_anom_index_comparison.csv")])
names = {"E1_range_day": "ts, 1 доба\n(candles_1m)", "E2_range_month": "ts, 1 місяць\n(candles_1m)", "E3_top_volume": "топ-10 volume\n(candles_1m)",
         "E4_year_volume": "year+volume\n(candles_1d)", "E5a_flat_hours": "24 год: плоска\n(candles_1h)", "E5b_bucket_day": "24 год: bucket\n(1d_buckets)",
         "ANOM1_year_top_score": "аномалії 2021\ntop-20", "ANOM2_day": "аномалії доби\n12.03.2020"}


def pair(col, ylabel, fn, title, log=True):
    ex = list(names); x = np.arange(len(ex)); w = .38
    b = [ic[(ic.experiment == e) & (ic.phase == "до")][col].iloc[0] for e in ex]
    a = [ic[(ic.experiment == e) & (ic.phase == "після")][col].iloc[0] for e in ex]
    fig, ax = plt.subplots(figsize=(10, 4.6))
    r1 = ax.bar(x - w / 2, np.maximum(b, .1 if log else 0), w, label="без індексу (COLLSCAN)", color=C1)
    r2 = ax.bar(x + w / 2, np.maximum(a, .1 if log else 0), w, label="з індексом (IXSCAN)", color=C2)
    for r, v in list(zip(r1, b)) + list(zip(r2, a)): ax.text(r.get_x() + r.get_width() / 2, max(v, .1 if log else 0) * 1.1, f"{v:g}", ha="center", fontsize=7)
    if log: ax.set_yscale("log")
    ax.set_xticks(x); ax.set_xticklabels([names[e] for e in ex], fontsize=7.5); ax.set_ylabel(ylabel); ax.set_title(title); ax.legend()
    fig.tight_layout(); fig.savefig(f"{FIG}/{fn}", dpi=150); plt.close(fig)


pair("wall_ms_median", "час виконання, мс (медіана з 5, лог. шкала)", "fig_idx_time.png", "Час запиту до та після створення індексу")
pair("totalDocsExamined", "переглянуто документів (лог. шкала)", "fig_idx_docs.png", "totalDocsExamined до та після індексації")

m = pd.read_csv(f"{RES}/pz4_agg_monthly_ohlc.csv"); m["d"] = pd.to_datetime(dict(year=m.year, month=m.month, day=1))
fig, ax = plt.subplots(figsize=(10, 4)); ax.bar(m.d, m.volume / 1e3, width=25, color=C2)
ax.set_ylabel("обсяг, тис. BTC"); ax.set_title("Місячний обсяг торгів (агрегація $group по candles_1m)"); fig.tight_layout(); fig.savefig(f"{FIG}/fig_monthly_volume.png", dpi=150); plt.close(fig)

h = pd.read_csv(f"{RES}/pz4_agg_volume_by_hour.csv")
fig, ax = plt.subplots(figsize=(10, 4)); ax.bar(h.hour, h.avg_volume_per_min, color=C2)
ax.set_xticks(range(24)); ax.set_xlabel("година доби (UTC)"); ax.set_ylabel("середній обсяг за хвилину, BTC")
ax.set_title("Обсяг за годиною доби, 2018 р. і пізніше ($hour у конвеєрі)"); fig.tight_layout(); fig.savefig(f"{FIG}/fig_volume_by_hour.png", dpi=150); plt.close(fig)

a = pd.read_csv(f"{RES}/pz4_anom_per_year.csv")
fig, ax = plt.subplots(figsize=(10, 4)); ax.bar(a.year, a.n, color=C1)
for x, v in zip(a.year, a.n): ax.text(x, v, str(v), ha="center", va="bottom", fontsize=8)
ax.set_xticks(a.year); ax.set_ylabel("кількість аномальних хвилин"); ax.set_title("Аномалії ПЗ3 у колекції anomalies за роками (агрегація MongoDB)")
fig.tight_layout(); fig.savefig(f"{FIG}/fig_anomalies_per_year.png", dpi=150); plt.close(fig)

s = pd.read_csv(f"{RES}/pz4_storage.csv").set_index("collection")
fig, ax = plt.subplots(1, 2, figsize=(11, 4))
cs = ["candles_1m", "candles_1m_ts"]; x = np.arange(2); w = .27
for i, (c, l, col) in enumerate((("size_MB", "size (дані без стиснення)", "#999"), ("storageSize_MB", "storageSize (на диску)", C2), ("totalIndexSize_MB", "індекси", C1))):
    ax[0].bar(x + (i - 1) * w, s.loc[cs, c], w, label=l, color=col)
    for xx, v in zip(x + (i - 1) * w, s.loc[cs, c]): ax[0].text(xx, v, f"{v:.0f}", ha="center", va="bottom", fontsize=8)
ax[0].set_xticks(x); ax[0].set_xticklabels(["звичайна\n(7,8 млн док.)", "time-series\n(7,8 млн док.)"]); ax[0].set_ylabel("МБ"); ax[0].legend(fontsize=8); ax[0].set_title("Хвилинні свічки: звичайна vs time-series")
cs = ["candles_1h", "candles_1d_buckets"]
for i, (c, l, col) in enumerate((("size_MB", "size", "#999"), ("storageSize_MB", "storageSize", C2))):
    ax[1].bar(np.arange(2) + (i - .5) * .35, s.loc[cs, c], .35, label=l, color=col)
    for xx, v in zip(np.arange(2) + (i - .5) * .35, s.loc[cs, c]): ax[1].text(xx, v, f"{v:.1f}", ha="center", va="bottom", fontsize=8)
ax[1].set_xticks(range(2)); ax[1].set_xticklabels(["годинні, плоска\n(130 276 док.)", "bucket «доба»\n(5 429 док.)"]); ax[1].set_ylabel("МБ"); ax[1].legend(fontsize=8); ax[1].set_title("Годинні свічки: плоска vs bucket")
fig.tight_layout(); fig.savefig(f"{FIG}/fig_storage.png", dpi=150); plt.close(fig)

v = pd.read_csv(f"{RES}/pz4_agg_yearly_volatility.csv")
fig, ax = plt.subplots(figsize=(10, 4)); ax.bar(v.year, v.avg_rv, color=C2)
ax.set_xticks(v.year); ax.set_ylabel("середня реалізована волатильність rv"); ax.set_title("Річна волатильність (агрегація по spark_daily_features, ПЗ2)")
fig.tight_layout(); fig.savefig(f"{FIG}/fig_yearly_volatility.png", dpi=150); plt.close(fig)
print("ok")
