# -*- coding: utf-8 -*-
"""Рисунки ПЗ3 (підписи українською, dpi 150)."""
import os, sys
import numpy as np, pandas as pd
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
P = "/home/user/AVD/work/pz3"; R = P + "/results"; F = P + "/figures"
os.makedirs(F, exist_ok=True)
sys.path.insert(0, P + "/code")
plt.rcParams.update({"font.size": 10, "axes.grid": True, "grid.alpha": .3, "axes.spines.top": False, "axes.spines.right": False})
C1, C2, C3 = "#1f77b4", "#d62728", "#2ca02c"

# Рисунки аномалій (правило v2) – у make_figures_anomaly.py; версія v1 – make_figures_v1.py.

# 3. тривалість мікробатчів і швидкість обробки
pg = pd.read_csv(R + "/stream/progress.csv", parse_dates=["timestamp"])
fig, ax = plt.subplots(2, 1, figsize=(9, 6), sharex=True)
NM = {"anomaly": "аномалії", "sink_win1h": "вікно 1 год (parquet)", "sink_win15m": "вікно 15 хв (parquet)", "mem_daily": "добова статистика"}
for q, col in (("anomaly", C2), ("sink_win1h", C1), ("sink_win15m", C3), ("mem_daily", "#9467bd")):
    g = pg[(pg["query"] == q) & (pg.num_input_rows > 0)].sort_values("timestamp")
    if g.empty: continue
    t = (g["timestamp"] - pg["timestamp"].min()).dt.total_seconds()
    ax[0].plot(t, g["trigger_execution_ms"] / 1000, "o-", ms=3, lw=1, color=col, label=NM[q])
    ax[1].plot(t, g["processed_rows_per_s"], "o-", ms=3, lw=1, color=col, label=NM[q])
ax[0].axhline(10, color="k", ls=":", lw=1, label="інтервал тригера 10 с"); ax[0].set_ylabel("тривалість мікробатчу, с"); ax[0].legend(fontsize=8, ncol=3)
ax[0].set_title("Тривалість мікробатчів і швидкість обробки (за даними прогресу запиту)")
ax[1].set_ylabel("оброблено рядків/с"); ax[1].set_xlabel("секунди від початку першого мікробатчу")
fig.tight_layout(); fig.savefig(F + "/pz3_batch_duration_throughput.png", dpi=150); plt.close(fig)
# 3b. вхідна швидкість vs оброблена + кількість рядків
g = pg[(pg["query"] == "anomaly") & (pg.num_input_rows > 0)].sort_values("timestamp"); t = (g["timestamp"] - pg["timestamp"].min()).dt.total_seconds()
fig, ax = plt.subplots(figsize=(9, 3.6)); ax.bar(t, g["num_input_rows"], width=6, color=C1, alpha=.7, label="рядків у мікробатчі")
ax.set_ylabel("рядків у мікробатчі"); ax.set_xlabel("секунди від початку"); a2 = ax.twinx()
a2.plot(t, g["input_rows_per_s"], color=C2, label="вхідна швидкість, рядків/с"); a2.plot(t, g["processed_rows_per_s"], color=C3, label="оброблена швидкість, рядків/с"); a2.set_ylabel("рядків/с"); a2.grid(False)
h1, l1 = ax.get_legend_handles_labels(); h2, l2 = a2.get_legend_handles_labels(); ax.legend(h1 + h2, l1 + l2, fontsize=8, loc="upper right")
ax.set_title("Запит anomaly: вхідна та оброблена швидкість"); fig.tight_layout(); fig.savefig(F + "/pz3_input_vs_processed.png", dpi=150); plt.close(fig)

# 4. OHLC за вікнами (свічки): 15-хв і 1-год, 12.03.2020
def candles(ax, d, width, title):
    up = d.close >= d.open
    for _, r in d.iterrows():
        c = C3 if r.close >= r.open else C2
        ax.plot([r.mid, r.mid], [r.low, r.high], color=c, lw=.9)
        ax.add_patch(plt.Rectangle((mdates.date2num(r.mid) - width / 2, min(r.open, r.close)), width, max(abs(r.close - r.open), 1), color=c))
    ax.set_title(title, fontsize=10); ax.set_ylabel("USD"); ax.xaxis_date(); ax.xaxis.set_major_formatter(mdates.DateFormatter("%d.%m %H:%M"))
w15 = pd.read_csv(R + "/stream/win15m_final.csv", parse_dates=["win_start", "win_end"]); w1h = pd.read_csv(R + "/stream/win1h_final.csv", parse_dates=["win_start", "win_end"])
fig, ax = plt.subplots(3, 1, figsize=(9, 8))
for a, d, wd, ttl, lo, hi in ((ax[0], w15, 15 / 1440, "Вікна 15 хв (tumbling), 12.03.2020 UTC", "2020-03-12", "2020-03-13"),
                              (ax[1], w1h, 1 / 24, "Вікна 1 год (tumbling), 9–16.03.2020 UTC", "2020-03-09", "2020-03-17")):
    d = d.copy(); d["mid"] = d["win_start"] + (d["win_end"] - d["win_start"]) / 2
    d = d[(d.win_start >= lo) & (d.win_start < hi)]
    candles(a, d, wd * .8, ttl); a.set_xlim(d.mid.min() - pd.Timedelta(minutes=30), d.mid.max() + pd.Timedelta(minutes=30))
d = w1h[(w1h.win_start >= "2020-03-12") & (w1h.win_start < "2020-03-14")]
ax[2].bar(d.win_start + pd.Timedelta(minutes=30), d.volume, width=1 / 24 * .8, color=C1); ax[2].set_ylabel("Обсяг, BTC"); ax[2].set_title("Обсяг за 1-год вікнами, 12–13.03.2020", fontsize=10)
ax[2].xaxis_date(); ax[2].xaxis.set_major_formatter(mdates.DateFormatter("%d.%m %H:%M"))
for a in ax: plt.setp(a.get_xticklabels(), rotation=30, ha="right")
fig.tight_layout(); fig.savefig(F + "/pz3_window_ohlc.png", dpi=150); plt.close(fig)

print("figures done", os.listdir(F))
