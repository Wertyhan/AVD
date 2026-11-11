# -*- coding: utf-8 -*-
"""Рисунки ПЗ3 для правила аномалій v2 (підписи українською, dpi 150). Віконні рисунки – у make_figures.py."""
import os, sys
import numpy as np, pandas as pd
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
P = "/home/user/AVD/work/pz3"; R = P + "/results"; F = P + "/figures"
sys.path.insert(0, P + "/code")
from anomaly_rule import K_SEL
plt.rcParams.update({"font.size": 10, "axes.grid": True, "grid.alpha": .3, "axes.spines.top": False, "axes.spines.right": False})
C1, C2, C3, C4 = "#1f77b4", "#d62728", "#2ca02c", "#ff7f0e"
dec = lambda x, n=2: f"{x:.{n}f}".replace(".", ",")

# 1. ціна та аномалії в потоці для кожного періоду
st = pd.read_parquet(R + "/stream_anomaly_v2/scored.parquet"); st["dt"] = pd.to_datetime(st["ts"], unit="s", utc=True)
names = {"2020-03": "9–16 березня 2020 (обвал COVID-19)", "2021-05": "17–22 травня 2021 (обвал 19.05)", "2022-11": "6–12 листопада 2022 (крах FTX)"}
for p, g in st.groupby("key"):
    fig, (a1, a2) = plt.subplots(2, 1, figsize=(9, 5.6), sharex=True, gridspec_kw={"height_ratios": [3, 1.4]})
    a1.plot(g["dt"], g["close"], lw=.8, color=C1, label="ціна закриття (потік)")
    an = g[g["anomaly"]]
    a1.scatter(an["dt"], an["close"], s=20, c=C2, zorder=3, label=f"аномалії (k={K_SEL}, z_vol>0): {len(an)}")
    a1.set_ylabel("Ціна закриття, USD"); a1.set_title(f"Потік і виявлені аномалії: {names.get(p, p)}"); a1.legend(loc="best")
    a2.plot(g["dt"], g["z_ret"], lw=.6, color="gray", label="z_ret = |r| / (Q90₇д|r| / 1,6449)")
    a2.axhline(K_SEL, color=C2, ls="--", lw=1, label=f"поріг k={K_SEL}")
    a2.set_yscale("symlog", linthresh=10); a2.set_ylabel("z_ret"); a2.set_xlabel("час UTC"); a2.legend(loc="upper right", fontsize=8)
    a2.xaxis.set_major_formatter(mdates.DateFormatter("%d.%m")); fig.autofmt_xdate(); fig.tight_layout()
    fig.savefig(f"{F}/pz3_price_anomalies_{p}.png", dpi=150); plt.close(fig)

# 2. k проти кількості спрацювань і lift (сітка k)
sel = pd.read_csv(R + "/pz3_threshold_selection.csv")
fig, ax = plt.subplots(1, 2, figsize=(10.5, 4))
a = ax[0]
a.plot(sel.k, sel.share_2018plus_pct, "o-", color=C1, label="2018+ (ліквідна ера)")
a.plot(sel.k, sel.share_2012_2016_pct, "s--", color="gray", label="2012–2016")
a.plot(sel.k, sel.share_event_days_pct, "^-", color=C2, label="5 днів відомих подій")
a.axhline(0.10, color=C4, ls=":", label="ціль 0,10 % (≈1,4 тривоги/добу)")
a.axvline(K_SEL, color=C3, ls="--", label=f"обране k = {K_SEL}")
a.set_xscale("log"); a.set_yscale("log"); a.set_xticks(sel.k); a.set_xticklabels(sel.k)
a.set_xlabel("поріг k"); a.set_ylabel("частка позначених активних хвилин, %"); a.set_title("Частка спрацювань залежно від k", fontsize=10); a.legend(fontsize=7.5)
a = ax[1]
a.plot(sel.k, sel.lift_event_vs_base, "o-", color=C2, label="хвилини")
a.plot(sel.k, sel.lift_1h_windows, "s--", color=C1, label="вікна 1 год (≥1 позначка)")
a.axvline(K_SEL, color=C3, ls="--", label=f"обране k = {K_SEL}")
a.set_xscale("log"); a.set_xticks(sel.k); a.set_xticklabels(sel.k)
a.set_xlabel("поріг k"); a.set_ylabel("lift = частка в днях подій / частка в базові дні 2018+"); a.set_title("Lift днів подій залежно від k", fontsize=10); a.legend(fontsize=8)
fig.tight_layout(); fig.savefig(F + "/pz3_k_vs_triggers.png", dpi=150); plt.close(fig)

# 3. lift по днях подій і періодах (хвилини, 15 хв, 1 год)
lf = pd.read_csv(R + "/pz3_lift.csv")
groups = lf["group"].unique().tolist()
fig, ax = plt.subplots(figsize=(10, 4.2)); x = np.arange(len(groups)); w = .27
for i, (lvl, nm, col) in enumerate((("minute", "хвилини", C2), ("15m_window", "вікна 15 хв", C4), ("1h_window", "вікна 1 год", C1))):
    d = lf[lf.level == lvl].set_index("group").reindex(groups)
    b = ax.bar(x + (i - 1) * w, d["lift"], w, color=col, label=nm)
    for xi, val in zip(x + (i - 1) * w, d["lift"]):
        ax.text(xi, val, dec(val, 1), ha="center", va="bottom", fontsize=6.5)
ax.axhline(1, color="k", lw=.8, ls=":")
ax.set_xticks(x); ax.set_xticklabels([g.split(" (")[0].replace("період ", "період\n").replace("усі 5 днів подій", "усі 5 днів\nподій") for g in groups], fontsize=8)
ax.set_ylabel("lift (база: дні 2018+ без подій)"); ax.set_title(f"Концентрація аномалій у днях відомих подій (k={K_SEL})", fontsize=10); ax.legend(fontsize=8)
fig.tight_layout(); fig.savefig(F + "/pz3_lift.png", dpi=150); plt.close(fig)

# 4. порівняння варіантів правила за однакового бюджету 0,10 %
dc = pd.read_csv(R + "/pz3_design_comparison.csv")
dc = dc[dc.combination == "return"].copy()
lbl = {"MAD0=Q50|r|/0,6745": "MAD", "IQR(r)/1,349": "IQR", "Q90|r|/1,6449": "Q90", "Q99|r|/2,5758": "Q99"}
dc["lab"] = [("7д, та сама година, Q90" if "same_hour" in v else f"{ {60: '60 хв', 1440: '24 год', 10080: '7 діб'}[b]}, {lbl.get(s, s)}")
             for v, b, s in zip(dc.variant, dc.baseline_min, dc.scale)]
fig, ax = plt.subplots(1, 2, figsize=(11, 4.4), sharey=True); y = np.arange(len(dc))
ax[0].barh(y, dc.lift_event_vs_2018plus, color=[C3 if "7 діб, Q90" == l else C1 for l in dc.lab]); ax[0].set_yticks(y); ax[0].set_yticklabels(dc.lab, fontsize=8)
ax[0].set_xlabel("lift днів подій"); ax[0].set_title("Lift за частки тривог 0,10 % (2018+)", fontsize=10); ax[0].invert_yaxis()
ax[1].barh(y, dc.rate_2012_2016_pct, color=[C3 if "7 діб, Q90" == l else "gray" for l in dc.lab]); ax[1].set_xscale("log")
ax[1].axvline(0.1, color=C4, ls=":", label="0,10 % (як у 2018+)"); ax[1].legend(fontsize=8)
ax[1].set_xlabel("частка позначених хвилин 2012–2016, %"); ax[1].set_title("Хибні тривоги тонкого ринку", fontsize=10)
fig.tight_layout(); fig.savefig(F + "/pz3_design_comparison.png", dpi=150); plt.close(fig)

# 5. k = 3,5,7,10 і обране: офлайн та потік
off = pd.read_csv(R + "/pz3_trigger_counts_offline.csv"); sc = pd.read_csv(R + "/pz3_trigger_counts_stream.csv")
fig, ax = plt.subplots(1, 2, figsize=(10, 4)); w = .38
for a, d, ttl in ((ax[0], off, f"Уся історія ({off.n_valid_minutes.iloc[0]:,} оцінених хвилин)".replace(",", " ")),
                  (ax[1], sc, f"Відтворений потік ({sc.n_scored.iloc[0]:,} оцінених хвилин)".replace(",", " "))):
    x = np.arange(len(d))
    a.bar(x - w / 2, d.n_return_only, w, label="z_ret > k", color=C1); a.bar(x + w / 2, d.n_flags, w, label="z_ret > k і z_vol > 0", color=C3)
    a.set_xticks(x); a.set_xticklabels([f"k={k}" for k in d.k]); a.set_yscale("log"); a.set_ylabel("кількість хвилин"); a.set_title(ttl, fontsize=10); a.legend(fontsize=8)
fig.tight_layout(); fig.savefig(F + "/pz3_trigger_counts_grid.png", dpi=150); plt.close(fig)

# 6. закономірності: роки (з часткою активних хвилин), день тижня, година
wd = pd.read_csv(R + "/pz3_anomalies_by_weekday_2018plus.csv"); hr = pd.read_csv(R + "/pz3_anomalies_by_hour_2018plus.csv"); yr = pd.read_csv(R + "/pz3_anomalies_by_year.csv")
fig, ax = plt.subplots(1, 3, figsize=(12, 3.8))
ax[0].bar(["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Нд"], wd.share_pct, color=C1); ax[0].set_title("За днем тижня (2018+)", fontsize=10); ax[0].set_ylabel("частка позначених хвилин, %")
ax[1].bar(hr.hour, hr.share_pct, color=C1); ax[1].set_title("За годиною доби UTC (2018+)", fontsize=10); ax[1].set_xlabel("година UTC")
ax[2].bar(yr.year.astype(str), yr.share_pct, color=C4); ax[2].axhline(0.1, color=C2, ls=":", lw=1); ax[2].set_title("За роками (уся історія)", fontsize=10); plt.setp(ax[2].get_xticklabels(), rotation=60)
fig.tight_layout(); fig.savefig(F + "/pz3_patterns.png", dpi=150); plt.close(fig)
print("figures done", sorted(os.listdir(F)))
