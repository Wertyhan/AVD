# -*- coding: utf-8 -*-
"""Рисунки ПЗ3: метрики потоку, OHLC за вікнами, watermark (підписи українською, стиль figstyle.py; дані лише з results/ цього прогону)."""
import os, sys
import numpy as np, pandas as pd
import matplotlib.dates as mdates
from pathlib import Path
P = str(Path(__file__).resolve().parents[1]); R = P + "/results"; F = P + "/figures"
os.makedirs(F, exist_ok=True); sys.path.insert(0, P + "/code")
from figstyle import *

pg = pd.read_csv(R + "/stream/progress.csv", parse_dates=["timestamp", "watermark"])
t0 = pg["timestamp"].min()
NM = {"anomaly": "anomaly (foreachBatch)", "sink_win1h": "вікно 1 год", "sink_win15m": "вікно 15 хв", "mem_daily": "добова статистика"}
# 1. тривалість мікробатчів і швидкість обробки
fig, ax = plt.subplots(2, 1, figsize=(W, H2), sharex=True)
for q, col in (("anomaly", C2), ("sink_win1h", C1), ("sink_win15m", C3), ("mem_daily", C5)):
    g = pg[(pg["query"] == q) & (pg.num_input_rows > 0)].sort_values("timestamp")
    t = (g["timestamp"] - t0).dt.total_seconds()
    ax[0].plot(t, g["trigger_execution_ms"] / 1000, "o-", ms=2.5, lw=1, color=col, label=NM[q])
    ax[1].plot(t, g["processed_rows_per_s"], "o-", ms=2.5, lw=1, color=col, label=NM[q])
ax[0].axhline(10, color="k", ls=":", lw=1, label="тригер 10 с"); ax[0].set_ylabel("тривалість, с"); ax[0].legend(ncol=3, fontsize=9, loc="upper right")
ax[0].set_title("Тривалість мікробатчів і швидкість обробки")
ax[1].set_ylabel("оброблено, рядків/с"); ax[1].set_xlabel("секунди від початку першого мікробатчу"); commaize(*ax)
save(fig, F + "/pz3_batch_duration_throughput.png")
# 2. вхідна та оброблена швидкість запиту anomaly
g = pg[(pg["query"] == "anomaly") & (pg.num_input_rows > 0)].sort_values("timestamp"); t = (g["timestamp"] - t0).dt.total_seconds()
fig, ax = plt.subplots(figsize=(W, H1)); ax.bar(t, g["num_input_rows"], width=6, color=C1, alpha=.6, label="рядків у мікробатчі")
ax.set_ylabel("рядків у мікробатчі"); ax.set_xlabel("секунди від початку"); a2 = ax.twinx()
a2.plot(t, g["input_rows_per_s"], color=C2, label="вхідна, рядків/с"); a2.plot(t, g["processed_rows_per_s"], color=C3, label="оброблена, рядків/с"); a2.set_ylabel("рядків/с"); a2.grid(False)
h1, l1 = ax.get_legend_handles_labels(); h2, l2 = a2.get_legend_handles_labels(); ax.legend(h1 + h2, l1 + l2, fontsize=9, loc="upper center", ncol=3, bbox_to_anchor=(.5, 1.0))
ax.set_ylim(0, g["num_input_rows"].max() * 1.35); ax.set_title("Запит anomaly: вхідна та оброблена швидкість"); commaize(ax, a2)
save(fig, F + "/pz3_input_vs_processed.png")

# 3. OHLC за вікнами (12.03.2020, 15 хв; 9–16.03.2020, 1 год) і обсяг
def candles(ax, d, width, title):
    for _, r in d.iterrows():
        c = C3 if r.close >= r.open else C2
        ax.plot([r.mid, r.mid], [r.low, r.high], color=c, lw=.9)
        ax.add_patch(plt.Rectangle((mdates.date2num(r.mid) - width / 2, min(r.open, r.close)), width, max(abs(r.close - r.open), 1), color=c))
    ax.set_title(title); ax.set_ylabel("USD"); ax.xaxis_date(); ax.xaxis.set_major_formatter(mdates.DateFormatter("%d.%m %H:%M"))
w15 = pd.read_csv(R + "/stream/win15m_final.csv", parse_dates=["win_start", "win_end"]); w1h = pd.read_csv(R + "/stream/win1h_final.csv", parse_dates=["win_start", "win_end"])
fig, ax = plt.subplots(2, 1, figsize=(W, H2))
for a, d, wd, ttl, lo, hi in ((ax[0], w15, 15 / 1440, "Вікна 15 хв, 12.03.2020 (UTC)", "2020-03-12", "2020-03-13"),
                              (ax[1], w1h, 1 / 24, "Вікна 1 год, 9–16.03.2020 (UTC)", "2020-03-09", "2020-03-17")):
    d = d.copy(); d["mid"] = d["win_start"] + (d["win_end"] - d["win_start"]) / 2; d = d[(d.win_start >= lo) & (d.win_start < hi)]
    candles(a, d, wd * .8, ttl); a.set_xlim(d.mid.min() - pd.Timedelta(minutes=30), d.mid.max() + pd.Timedelta(minutes=30))
    plt.setp(a.get_xticklabels(), rotation=0); a.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x:,.0f}".replace(",", " ")))
ax[0].xaxis.set_major_locator(mdates.HourLocator(interval=4)); ax[1].xaxis.set_major_formatter(mdates.DateFormatter("%d.%m")); ax[1].xaxis.set_major_locator(mdates.DayLocator())
save(fig, F + "/pz3_window_ohlc.png")
d = w1h[(w1h.win_start >= "2020-03-12") & (w1h.win_start < "2020-03-14")]
fig, ax = plt.subplots(figsize=(W, H1)); ax.bar(d.win_start + pd.Timedelta(minutes=30), d.volume, width=1 / 24 * .8, color=C1)
ax.set_ylabel("обсяг, BTC"); ax.set_title("Обсяг за 1-годинними вікнами, 12–13.03.2020 (UTC)"); ax.xaxis_date(); ax.xaxis.set_major_formatter(mdates.DateFormatter("%d.%m %H:%M"))
ax.xaxis.set_major_locator(mdates.HourLocator(interval=8)); ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x:,.0f}".replace(",", " ")))
save(fig, F + "/pz3_window_volume.png")

# 4. watermark: event time max і watermark за мікробатчами (sink_win15m) та розрив час обробки – watermark за запитами
g = pg[(pg["query"] == "sink_win15m") & pg["watermark"].notna() & (pg["watermark"] > "1971-01-01") & pg["event_time_max"].notna()].sort_values("batch_id")
fig, ax = plt.subplots(2, 1, figsize=(W, H2), sharex=True)
ax[0].plot(g["batch_id"], pd.to_datetime(g["event_time_max"], utc=True), color=C1, label="max event time у мікробатчі")
ax[0].plot(g["batch_id"], g["watermark"], color=C2, label="watermark (max event time – 10 хв)")
ax[0].yaxis.set_major_formatter(mdates.DateFormatter("%d.%m.%Y")); ax[0].legend(fontsize=9); ax[0].set_title("Watermark запиту sink_win15m"); ax[0].set_ylabel("час події (UTC)")
for q, col in (("sink_win15m", C1), ("sink_win1h", C3), ("mem_daily", C5), ("console_1h", C4)):
    gg = pg[(pg["query"] == q) & pg["watermark"].notna() & (pg["watermark"] > "1971-01-01")].sort_values("batch_id")
    ax[1].plot(gg["batch_id"], gg["watermark_gap_s"] / 1e8, color=col, lw=1, label=q)
ax[1].set_ylabel("розрив, 10⁸ с"); ax[1].set_xlabel("номер мікробатчу"); ax[1].legend(fontsize=9, ncol=2); commaize(ax[1])
save(fig, F + "/pz3_watermark.png")
print("figures done (make_figures)")
