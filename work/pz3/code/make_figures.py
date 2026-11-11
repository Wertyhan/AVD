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
from anomaly_rule import K_SEL, KV_SEL
plt.rcParams.update({"font.size": 10, "axes.grid": True, "grid.alpha": .3, "axes.spines.top": False, "axes.spines.right": False})
C1, C2, C3 = "#1f77b4", "#d62728", "#2ca02c"

# 1. ціна та аномалії для кожного періоду
st = pd.read_parquet(R + "/stream/scored.parquet"); st["dt"] = pd.to_datetime(st["ts"], unit="s", utc=True)
names = {"2020-03": "9–16 березня 2020 (обвал COVID-19)", "2021-05": "17–22 травня 2021 (обвал 19.05)", "2022-11": "6–12 листопада 2022 (крах FTX)"}
st["period"] = st["dt"].dt.strftime("%Y-%m")
for p, g in st.groupby("period"):
    fig, (a1, a2) = plt.subplots(2, 1, figsize=(9, 5.6), sharex=True, gridspec_kw={"height_ratios": [3, 1.3]})
    a1.plot(g["dt"], g["close"], lw=.8, color=C1, label="ціна закриття (потік)")
    an = g[g["anomaly"]]
    a1.scatter(an["dt"], an["close"], s=22, c=C2, zorder=3, label=f"аномалії (k={K_SEL}, k_v={KV_SEL}): {len(an)}")
    a1.set_ylabel("Close, USD"); a1.set_title(f"Потік і виявлені аномалії: {names.get(p, p)}"); a1.legend(loc="best")
    a2.plot(g["dt"], g["z_ret"], lw=.6, color="gray", label="z = |r|/(1,4826·MAD)")
    a2.axhline(K_SEL, color=C2, ls="--", lw=1, label=f"поріг k={K_SEL}")
    a2.set_ylabel("z"); a2.set_xlabel("час UTC"); a2.legend(loc="upper right")
    a2.xaxis.set_major_formatter(mdates.DateFormatter("%d.%m %H:%M")); fig.autofmt_xdate(); fig.tight_layout()
    fig.savefig(f"{F}/pz3_price_anomalies_{p}.png", dpi=150); plt.close(fig)

# 2. k проти кількості спрацювань
sel = pd.read_csv(R + "/pz3_threshold_selection.csv")
fig, ax = plt.subplots(1, 2, figsize=(10, 4))
for a, rule, ttl, ksel in ((ax[0], "return", "Правило за дохідністю: z > k", K_SEL), (ax[1], "volume", "Правило за обсягом: V/медіана > k_v", KV_SEL)):
    s = sel[sel.rule == rule]
    a.plot(s.k, s.share_2018plus_pct, "o-", color=C1, label="2018+ (ліквідна ера)")
    a.plot(s.k, s.share_full_pct, "s--", color="gray", label="уся історія")
    a.axhline(0.15, color=C2, ls=":", label="бюджет 0,15 %")
    a.axvline(ksel, color=C3, ls="--", label=f"обране = {ksel}")
    a.set_xscale("log"); a.set_yscale("log"); a.set_xlabel("поріг k" if rule == "return" else "поріг k_v")
    a.set_ylabel("частка позначених хвилин, %"); a.set_title(ttl, fontsize=10); a.legend(fontsize=8)
fig.tight_layout(); fig.savefig(F + "/pz3_k_vs_triggers.png", dpi=150); plt.close(fig)
# 2b. k = k_v ∈ {3,5,7,10}: офлайн та потік
off = pd.read_csv(R + "/pz3_trigger_counts_offline.csv"); sc = pd.read_csv(R + "/pz3_trigger_counts_stream.csv")
sc = sc[sc.k == sc.k_v]
fig, ax = plt.subplots(1, 2, figsize=(10, 4)); x = np.arange(4); w = .27
for a, d, ttl in ((ax[0], off, "Уся історія (7,1 млн оцінених хвилин)"), (ax[1], sc, "Відтворений потік (30 240 хв)")):
    a.bar(x - w, d.n_return_rule, w, label="за дохідністю", color=C1); a.bar(x, d.n_volume_rule, w, label="за обсягом", color="#ff7f0e")
    a.bar(x + w, d.n_union, w, label="об'єднання (АБО)", color=C3)
    a.set_xticks(x); a.set_xticklabels([f"k={k}" for k in d.k]); a.set_yscale("log"); a.set_ylabel("кількість хвилин"); a.set_title(ttl, fontsize=10); a.legend(fontsize=8)
fig.tight_layout(); fig.savefig(F + "/pz3_trigger_counts_grid.png", dpi=150); plt.close(fig)

# 3. тривалість мікробатчів і швидкість обробки
pg = pd.read_csv(R + "/stream/progress.csv", parse_dates=["timestamp"])
fig, ax = plt.subplots(2, 1, figsize=(9, 6), sharex=True)
for q, col in (("anomaly", C2), ("sink_win1h", C1), ("sink_win15m", C3), ("mem_daily", "#9467bd")):
    g = pg[(pg["query"] == q) & (pg.num_input_rows > 0)].sort_values("timestamp")
    if g.empty: continue
    t = (g["timestamp"] - pg["timestamp"].min()).dt.total_seconds()
    ax[0].plot(t, g["trigger_execution_ms"] / 1000, "o-", ms=3, lw=1, color=col, label=q)
    ax[1].plot(t, g["processed_rows_per_s"], "o-", ms=3, lw=1, color=col, label=q)
ax[0].axhline(10, color="k", ls=":", lw=1, label="інтервал тригера 10 с"); ax[0].set_ylabel("тривалість мікробатчу, с"); ax[0].legend(fontsize=8, ncol=3)
ax[0].set_title("Тривалість мікробатчів і швидкість обробки (query.recentProgress)")
ax[1].set_ylabel("оброблено рядків/с (processedRowsPerSecond)"); ax[1].set_xlabel("секунди від початку першого мікробатчу")
fig.tight_layout(); fig.savefig(F + "/pz3_batch_duration_throughput.png", dpi=150); plt.close(fig)
# 3b. вхідна швидкість vs оброблена + кількість рядків
g = pg[(pg["query"] == "anomaly") & (pg.num_input_rows > 0)].sort_values("timestamp"); t = (g["timestamp"] - pg["timestamp"].min()).dt.total_seconds()
fig, ax = plt.subplots(figsize=(9, 3.6)); ax.bar(t, g["num_input_rows"], width=6, color=C1, alpha=.7, label="рядків у мікробатчі")
ax.set_ylabel("рядків у мікробатчі"); ax.set_xlabel("секунди від початку"); a2 = ax.twinx()
a2.plot(t, g["input_rows_per_s"], color=C2, label="inputRowsPerSecond"); a2.plot(t, g["processed_rows_per_s"], color=C3, label="processedRowsPerSecond"); a2.set_ylabel("рядків/с"); a2.grid(False)
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
ax[2].bar(d.win_start + pd.Timedelta(minutes=30), d.volume, width=1 / 24 * .8, color=C1); ax[2].set_ylabel("Volume, BTC"); ax[2].set_title("Обсяг за 1-год вікнами, 12–13.03.2020", fontsize=10)
ax[2].xaxis_date(); ax[2].xaxis.set_major_formatter(mdates.DateFormatter("%d.%m %H:%M"))
for a in ax: plt.setp(a.get_xticklabels(), rotation=30, ha="right")
fig.tight_layout(); fig.savefig(F + "/pz3_window_ohlc.png", dpi=150); plt.close(fig)

# 5. закономірності: день тижня, година, роки
wd = pd.read_csv(R + "/pz3_anomalies_by_weekday_2018plus.csv"); hr = pd.read_csv(R + "/pz3_anomalies_by_hour_2018plus.csv"); yr = pd.read_csv(R + "/pz3_anomalies_by_year.csv")
fig, ax = plt.subplots(1, 3, figsize=(12, 3.8))
ax[0].bar(["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Нд"], wd.share_pct, color=C1); ax[0].set_title("За днем тижня (2018+)", fontsize=10); ax[0].set_ylabel("частка позначених хвилин, %")
ax[1].bar(hr.hour, hr.share_pct, color=C1); ax[1].set_title("За годиною доби UTC (2018+)", fontsize=10); ax[1].set_xlabel("година UTC")
ax[2].bar(yr.year.astype(str), yr.share_pct, color="#ff7f0e"); ax[2].set_title("За роками (уся історія)", fontsize=10); plt.setp(ax[2].get_xticklabels(), rotation=60)
fig.tight_layout(); fig.savefig(F + "/pz3_patterns.png", dpi=150); plt.close(fig)
print("figures done", os.listdir(F))
