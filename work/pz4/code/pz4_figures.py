# -*- coding: utf-8 -*-
"""Рисунки ПЗ4 (підписи українською, dpi 200) за CSV з results/."""
import pandas as pd, numpy as np, matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import sys; sys.path.insert(0, __import__("os").path.dirname(__file__))
from pz4_common import RES, FIG
from matplotlib.ticker import FuncFormatter
# Розбірливість (правило 2a): рисунки вставляються шириною 16 см (6,3 дюйма), тому figsize ~6,7 дюйма, шрифти >= 8 pt;
# десятковий роздільник - кома (Ukrainian decimal comma), тисячі - вузький пробіл.
sys.path.insert(0, "/home/user/AVD/work/tools_report")
import plotstyle  # правило 2b
def dec(v, d=None):
    """Число -> рядок з десятковою комою (d знаків або %g)."""
    t = f"{v:.{d}f}" if d is not None else f"{v:g}"
    return t.replace(".", ",")
comma_fmt = FuncFormatter(lambda v, _: dec(v))
def commas(ax, which="y"):
    (ax.yaxis if which == "y" else ax.xaxis).set_major_formatter(comma_fmt)
C1, C2 = "#c0504d", "#2e75b6"
ic = pd.concat([pd.read_csv(f"{RES}/pz4_index_comparison.csv"), pd.read_csv(f"{RES}/pz4_anom_index_comparison.csv")])
names = {"E1_range_day": "ts, 1 доба\n(candles_1m)", "E2_range_month": "ts, 1 місяць\n(candles_1m)", "E3_top_volume": "топ-10 volume\n(candles_1m)",
         "E4_year_volume": "year+volume\n(candles_1d)", "E5a_flat_hours": "24 год: плоска\n(candles_1h)", "E5b_bucket_day": "24 год: bucket\n(1d_buckets)",
         "ANOM1_year_top_score": "аномалії 2021\ntop-20", "ANOM2_day": "аномалії доби\n12.03.2020"}


def pair(col, xlabel, fn, title, log=True):
    ex = list(names); y = np.arange(len(ex))[::-1]; h = .38
    b = [ic[(ic.experiment == e) & (ic.phase == "до")][col].iloc[0] for e in ex]
    a = [ic[(ic.experiment == e) & (ic.phase == "після")][col].iloc[0] for e in ex]
    floor = .5 if log else 0
    fig, ax = plt.subplots(figsize=(6.3, 4.7))
    r1 = ax.barh(y + h / 2, np.maximum(b, floor), h, label="без індексу (COLLSCAN)", color=C1)
    r2 = ax.barh(y - h / 2, np.maximum(a, floor), h, label="з індексом (IXSCAN)", color=C2)
    top = max(max(b), max(a))
    for r, v in list(zip(r1, b)) + list(zip(r2, a)):
        ax.text(max(v, floor) * (1.15 if log else 1) + (0 if log else top * .01), r.get_y() + r.get_height() / 2, dec(v), va="center", fontsize=9)
    if log: ax.set_xscale("log"); ax.set_xlim(right=top * 8)
    ax.xaxis.set_major_formatter(comma_fmt)
    ax.set_yticks(y); ax.set_yticklabels([names[e] for e in ex], fontsize=9); ax.set_xlabel(xlabel); ax.set_title(title)
    ax.legend(loc="lower right"); ax.grid(axis="y", visible=False)
    fig.tight_layout(); fig.savefig(f"{FIG}/{fn}", dpi=200); plt.close(fig)


# та сама метрика, що в таблиці 6: executionTimeMillis з explain (медіана з 5); повний час у Python - окремий стовпець CSV
pair("executionTimeMillis_explain_median", "executionTimeMillis, мс (медіана з 5, лог. шкала; 0 = менше 1 мс)", "fig_idx_time.png", "Час виконання запиту до та після індексу")
pair("totalDocsExamined", "переглянуто документів (лог. шкала)", "fig_idx_docs.png", "totalDocsExamined до та після індексації")

m = pd.read_csv(f"{RES}/pz4_agg_monthly_ohlc.csv"); m["d"] = pd.to_datetime(dict(year=m.year, month=m.month, day=1))
fig, ax = plt.subplots(figsize=(6.3, 3.4)); ax.bar(m.d, m.volume / 1e3, width=25, color=C2)
ax.set_ylabel("обсяг, тис. BTC"); ax.set_title("Місячний обсяг торгів (агрегація $group по candles_1m)"); commas(ax); fig.tight_layout(); fig.savefig(f"{FIG}/fig_monthly_volume.png", dpi=200); plt.close(fig)

h = pd.read_csv(f"{RES}/pz4_agg_volume_by_hour.csv")
fig, ax = plt.subplots(figsize=(6.3, 3.4)); ax.bar(h.hour, h.avg_volume_per_min, color=C2)
ax.set_xticks(range(0, 24, 2)); commas(ax); ax.set_xlabel("година доби (UTC)"); ax.set_ylabel("середній обсяг за хвилину, BTC")
ax.set_title("Обсяг за годиною доби, 2018 р. і пізніше ($hour у конвеєрі)"); fig.tight_layout(); fig.savefig(f"{FIG}/fig_volume_by_hour.png", dpi=200); plt.close(fig)

a = pd.read_csv(f"{RES}/pz4_anom_per_year.csv")
fig, ax = plt.subplots(figsize=(6.3, 3.4)); ax.bar(a.year, a.n, color=C1)
for x, v in zip(a.year, a.n): ax.text(x, v, str(v), ha="center", va="bottom", fontsize=9)
ax.set_xticks(a.year[::2]); ax.set_ylabel("кількість аномальних хвилин"); ax.set_title("Аномалії ПЗ3 у колекції anomalies за роками (агрегація MongoDB)")
fig.tight_layout(); fig.savefig(f"{FIG}/fig_anomalies_per_year.png", dpi=200); plt.close(fig)

s = pd.read_csv(f"{RES}/pz4_storage.csv").set_index("collection")
fig, ax = plt.subplots(1, 2, figsize=(6.3, 3.4))
cs = ["candles_1m", "candles_1m_ts"]; x = np.arange(2); w = .27
# для TS-колекції size вже відображає стиснені бакети (не "дані без стиснення"); індекс TS - meta_1_ts_1, а не _id
for i, (c, l, col) in enumerate((("size_MiB", "size (BSON)", "#999"), ("storageSize_MiB", "storageSize (диск)", C2), ("totalIndexSize_MiB", "індекси", C1))):
    ax[0].bar(x + (i - 1) * w, s.loc[cs, c], w, label=l, color=col)
    for xx, v in zip(x + (i - 1) * w, s.loc[cs, c]): ax[0].text(xx, v, dec(v, 0 if v >= 10 else 2), ha="center", va="bottom", fontsize=9)
ax[0].set_xticks(x); ax[0].set_xticklabels(["плоска", "time-series\n(індекс meta_1_ts_1)"]); ax[0].set_ylabel("МіБ"); ax[0].legend(fontsize=9); ax[0].set_title("Хвилинні свічки, 7,8 млн док.", fontsize=10); ax[0].set_ylim(top=ax[0].get_ylim()[1] * 1.15); commas(ax[0])
cs = ["candles_1h", "candles_1d_buckets"]
for i, (c, l, col) in enumerate((("size_MiB", "size (BSON)", "#999"), ("storageSize_MiB", "storageSize (диск)", C2))):
    ax[1].bar(np.arange(2) + (i - .5) * .35, s.loc[cs, c], .35, label=l, color=col)
    for xx, v in zip(np.arange(2) + (i - .5) * .35, s.loc[cs, c]): ax[1].text(xx, v, dec(v, 1), ha="center", va="bottom", fontsize=9)
ax[1].set_xticks(range(2)); ax[1].set_xticklabels(["плоска\n(130 276 док.)", "bucket «доба»\n(5 429 док.)"]); ax[1].set_ylabel("МіБ"); ax[1].legend(fontsize=9); ax[1].set_title("Годинні свічки", fontsize=10); ax[1].set_ylim(top=ax[1].get_ylim()[1] * 1.3); commas(ax[1])
fig.tight_layout(); fig.savefig(f"{FIG}/fig_storage.png", dpi=200); plt.close(fig)

# M4: плоска проти time-series (медіана з 5, wall-clock клієнта)
import os
if os.path.exists(f"{RES}/pz4_flat_vs_ts.csv"):
    ft = pd.read_csv(f"{RES}/pz4_flat_vs_ts.csv")
    lab = ["1 доба\n(1 440 хв)", "1 місяць\n(44 640 хв)", "місячний OHLC\nза 2021 рік"]
    fig, ax = plt.subplots(figsize=(6.3, 3.4)); x = np.arange(len(ft)); w = .38
    r1 = ax.bar(x - w / 2, ft.flat_ms_median, w, color=C2, label="плоска candles_1m (індекс ts_1)")
    r2 = ax.bar(x + w / 2, ft.ts_ms_median, w, color=C1, label="time-series candles_1m_ts")
    for r, v in list(zip(r1, ft.flat_ms_median)) + list(zip(r2, ft.ts_ms_median)): ax.text(r.get_x() + r.get_width() / 2, v * 1.1, dec(v, 1), ha="center", fontsize=9)
    ax.set_yscale("log"); ax.set_ylim(top=ft[["flat_ms_median", "ts_ms_median"]].values.max() * 6); ax.set_xticks(x); ax.set_xticklabels(lab)
    ax.set_ylabel("час у Python, мс (лог.)"); ax.set_title("Три запити: плоска і time-series колекції (медіана з 5)"); ax.legend(loc="upper left"); commas(ax)
    fig.tight_layout(); fig.savefig(f"{FIG}/fig_flat_vs_ts.png", dpi=200); plt.close(fig)

v = pd.read_csv(f"{RES}/pz4_agg_yearly_volatility.csv")
fig, ax = plt.subplots(figsize=(6.3, 3.4)); ax.bar(v.year, v.avg_rv, color=C2)
ax.set_xticks(v.year[::2]); commas(ax); ax.set_ylabel("середня rv"); ax.set_title("Річна волатильність (агрегація по spark_daily_features, ПЗ2)")
fig.tight_layout(); fig.savefig(f"{FIG}/fig_yearly_volatility.png", dpi=200); plt.close(fig)
print("ok")
