# -*- coding: utf-8 -*-
"""Рисунки ПЗ3 для правила аномалій v2 (українські підписи, стиль figstyle.py). Дані – з results/ цього прогону."""
import os, sys
import numpy as np, pandas as pd
import matplotlib.dates as mdates
P = "/home/user/AVD/work/pz3"; R = P + "/results"; F = P + "/figures"
sys.path.insert(0, P + "/code")
from anomaly_rule import K_SEL
from figstyle import *

# 1. ціна та аномалії в потоці для кожного періоду
st = pd.read_parquet(R + "/stream/scored.parquet"); st["dt"] = pd.to_datetime(st["ts"], unit="s", utc=True)
names = {"2020-03": "9–16.03.2020 (обвал COVID-19)", "2021-05": "17–22.05.2021 (обвал 19.05)", "2022-11": "6–12.11.2022 (крах FTX)"}
for p, g in st.groupby("key"):
    fig, (a1, a2) = plt.subplots(2, 1, figsize=(W, H2), sharex=True, gridspec_kw={"height_ratios": [3, 1.6]})
    a1.plot(g["dt"], g["close"], lw=.8, color=C1, label="ціна закриття")
    an = g[g["anomaly"]]
    a1.scatter(an["dt"], an["close"], s=12, c=C2, zorder=3, label=f"аномалії: {len(an)}")
    a1.set_ylabel("USD"); a1.set_title(f"Потік і аномалії: {names.get(p, p)}"); a1.legend(loc="best", fontsize=9)
    a1.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x:,.0f}".replace(",", " ")))
    a2.plot(g["dt"], g["z_ret"], lw=.6, color="gray", label="z_ret"); a2.axhline(K_SEL, color=C2, ls="--", lw=1, label=f"k = {K_SEL}")
    a2.set_yscale("symlog", linthresh=10); a2.set_ylabel("z_ret"); a2.set_xlabel("дата (UTC)"); a2.legend(loc="upper right", fontsize=9)
    a2.xaxis.set_major_formatter(mdates.DateFormatter("%d.%m")); a2.xaxis.set_major_locator(mdates.DayLocator())
    save(fig, f"{F}/pz3_price_anomalies_{p}.png")

# 2. k проти частки спрацювань і lift
sel = pd.read_csv(R + "/pz3_threshold_selection.csv")
fig, ax = plt.subplots(2, 1, figsize=(W, H2), sharex=True)
a = ax[0]
a.plot(sel.k, sel.share_2018plus_pct, "o-", color=C1, ms=4, label="2018+"); a.plot(sel.k, sel.share_2012_2016_pct, "s--", color="gray", ms=4, label="2012–2016")
a.plot(sel.k, sel.share_event_days_pct, "^-", color=C2, ms=4, label="5 днів подій"); a.axhline(0.10, color=C4, ls=":", label="ціль 0,10 %")
a.axvline(K_SEL, color=C3, ls="--", label=f"k = {K_SEL}"); a.set_xscale("log"); a.set_yscale("log"); a.set_ylabel("частка позначених, %")
a.set_title("Частка спрацювань і lift залежно від k"); a.legend(fontsize=9, ncol=3, loc="lower left")
a = ax[1]
a.plot(sel.k, sel.lift_event_vs_base, "o-", color=C2, ms=4, label="хвилини"); a.plot(sel.k, sel.lift_1h_windows, "s--", color=C1, ms=4, label="1-год вікна")
a.axvline(K_SEL, color=C3, ls="--"); a.set_xscale("log"); a.set_xticks(sel.k); a.set_xticklabels(sel.k); a.minorticks_off()
a.set_xlabel("поріг k"); a.set_ylabel("lift"); a.legend(fontsize=9)
save(fig, F + "/pz3_k_vs_triggers.png")

# 3. lift по днях подій і періодах
lf = pd.read_csv(R + "/pz3_lift.csv"); groups = lf["group"].unique().tolist()
fig, ax = plt.subplots(figsize=(W, H1 + .4)); x = np.arange(len(groups)); w = .27
for i, (lvl, nm, col) in enumerate((("minute", "хвилини", C2), ("15m_window", "вікна 15 хв", C4), ("1h_window", "вікна 1 год", C1))):
    d = lf[lf.level == lvl].set_index("group").reindex(groups); ax.bar(x + (i - 1) * w, d["lift"], w, color=col, label=nm)
ax.axhline(1, color="k", lw=.8, ls=":"); ax.set_xticks(x)
lab = {"усі 5 днів подій": "5 днів", "2020-03-12": "12.03.20", "2020-03-13": "13.03.20", "2021-05-19": "19.05.21", "2022-11-08": "08.11.22", "2022-11-09": "09.11.22"}
ax.set_xticklabels([lab[g] if g in lab else "період\n" + g.split(" ")[1] for g in groups], fontsize=9)
ax.set_ylabel("lift"); ax.set_title(f"Концентрація аномалій у днях подій (k = {K_SEL})"); ax.legend(fontsize=9); commaize(ax, axis="y")
save(fig, F + "/pz3_lift.png")

# 4. порівняння варіантів за однакового бюджету 0,10 %
dc = pd.read_csv(R + "/pz3_design_comparison.csv"); dc = dc[dc.combination == "return"].copy()
lbl = {"MAD0=Q50|r|/0,6745": "MAD", "IQR(r)/1,349": "IQR", "Q90|r|/1,6449": "Q90", "Q99|r|/2,5758": "Q99"}
dc["lab"] = [("7 діб, та сама година, Q90" if "same_hour" in v else f"{ {60: '60 хв', 1440: '24 год', 10080: '7 діб'}[b]}, {lbl.get(s, s)}") for v, b, s in zip(dc.variant, dc.baseline_min, dc.scale)]
fig, ax = plt.subplots(2, 1, figsize=(W, H2 + 1.0)); y = np.arange(len(dc)); isel = [l == "7 діб, Q90" for l in dc.lab]
ax[0].bar(y, dc.lift_event_vs_2018plus, color=[C3 if s else C1 for s in isel]); ax[0].set_ylabel("lift днів подій"); ax[0].set_title("Варіанти правила за бюджету 0,10 % (2018+)"); ax[0].set_xticks(y); ax[0].set_xticklabels([])
ax[1].bar(y, dc.rate_2012_2016_pct, color=[C3 if s else "gray" for s in isel]); ax[1].set_yscale("log"); ax[1].axhline(0.1, color=C4, ls=":", label="0,10 % (як 2018+)")
ax[1].set_ylabel("частка 2012–2016, %"); ax[1].set_xticks(y); ax[1].set_xticklabels(dc.lab, rotation=40, ha="right", fontsize=9); ax[1].legend(fontsize=9)
save(fig, F + "/pz3_design_comparison.png")

# 5. k = 3,5,7,10 і обране: офлайн та потік
off = pd.read_csv(R + "/pz3_trigger_counts_offline.csv"); sc = pd.read_csv(R + "/pz3_trigger_counts_stream.csv")
fig, ax = plt.subplots(2, 1, figsize=(W, H2), sharex=False); w = .38
for a, d, ttl in ((ax[0], off, f"Уся історія ({off.n_valid_minutes.iloc[0]:,} оцінених хвилин)".replace(",", " ")), (ax[1], sc, f"Відтворений потік ({sc.n_scored.iloc[0]:,} оцінених хвилин)".replace(",", " "))):
    x = np.arange(len(d)); a.bar(x - w / 2, d.n_return_only, w, label="z_ret > k", color=C1); a.bar(x + w / 2, d.n_flags, w, label="z_ret > k і z_vol > 0", color=C3)
    a.set_xticks(x); a.set_xticklabels([f"k = {int(k)}" for k in d.k]); a.set_yscale("log"); a.set_ylabel("хвилин"); a.set_title(ttl); a.legend(fontsize=9)
save(fig, F + "/pz3_trigger_counts_grid.png")

# 6. закономірності (завдання 8): година×день тижня, маргінальні розподіли, роки, напрям топ-50
hw = pd.read_csv(R + "/pz3_patterns_hour_weekday_2018plus.csv"); wd = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Нд"]
pv = hw.pivot(index="dow", columns="hour", values="n_anomalies")
fig, ax = plt.subplots(figsize=(W, H1)); im = ax.imshow(pv.values, aspect="auto", cmap="YlOrRd"); ax.grid(False)
ax.set_yticks(range(7)); ax.set_yticklabels(wd); ax.set_xticks(range(0, 24, 2)); ax.set_xlabel("година доби (UTC)"); ax.set_title("Кількість аномалій: день тижня × година (2018+)")
cb = fig.colorbar(im, ax=ax); cb.set_label("кількість"); save(fig, F + "/pz3_patterns_hour_weekday.png")
wk = pd.read_csv(R + "/pz3_anomalies_by_weekday_2018plus.csv"); hr = pd.read_csv(R + "/pz3_anomalies_by_hour_2018plus.csv")
fig, ax = plt.subplots(2, 1, figsize=(W, H2))
ax[0].bar(wd, wk.n_anomalies, color=C1); ax[0].set_ylabel("аномалій"); ax[0].set_title("За днем тижня (2018+)")
ax[1].bar(hr.hour, hr.n_anomalies, color=C1); ax[1].set_ylabel("аномалій"); ax[1].set_xlabel("година доби (UTC)"); ax[1].set_title("За годиною доби (2018+)"); ax[1].set_xticks(range(0, 24, 2))
for a in ax: a.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x:,.0f}".replace(",", " ")))
save(fig, F + "/pz3_patterns.png")
yr = pd.read_csv(R + "/pz3_anomalies_by_year.csv")
fig, ax = plt.subplots(figsize=(W, H1)); ax.bar(yr.year.astype(str), yr.share_pct, color=C4); ax.axhline(0.1, color=C2, ls=":", lw=1, label="0,10 %")
ax.set_ylabel("частка позначених хвилин, %"); ax.set_title("Частка аномалій за роками"); plt.setp(ax.get_xticklabels(), rotation=60); ax.legend(fontsize=9); commaize(ax, axis="y")
save(fig, F + "/pz3_patterns_year.png")
t = pd.read_csv(R + "/pz3_top50_dates_direction.csv")
fig, ax = plt.subplots(figsize=(W, H1)); cols = [C2 if v < 0 else C3 for v in t.day_logret]
ax.bar(range(len(t)), t.n_anomalies, color=cols); ax.set_xlabel("ранг дати за кількістю аномалій"); ax.set_ylabel("аномалій за добу"); ax.set_title("Топ-50 днів: обвали та зростання")
from matplotlib.patches import Patch
ax.legend(handles=[Patch(color=C2, label=f"обвал: {int((t.day_logret < 0).sum())}"), Patch(color=C3, label=f"зростання: {int((t.day_logret >= 0).sum())}")], fontsize=9)
save(fig, F + "/pz3_top50_direction.png")

# 7. out-of-sample
loo = pd.read_csv(R + "/pz3_oos_loo.csv"); ts = pd.read_csv(R + "/pz3_oos_timesplit.csv")
fig, ax = plt.subplots(2, 1, figsize=(W, H2 + .6))
l1 = loo[loo.unit == "день"]; x = np.arange(len(l1)); w = .38
ax[0].bar(x - w / 2, l1.insample_lift_chosen_all_5_days, w, color="gray", label="in-sample (усі 5 днів)"); ax[0].bar(x + w / 2, l1.heldout_lift_chosen, w, color=C3, label="пропущений день")
ax[0].set_xticks(x); ax[0].set_xticklabels([f"{d[8:10]}.{d[5:7]}.{d[2:4]}" for d in l1.held_out]); ax[0].set_ylabel("lift"); ax[0].legend(fontsize=9); ax[0].set_title("Leave-one-event-out: lift обраного варіанта")
t2 = ts.head(8); x = np.arange(len(t2))
ax[1].bar(x - w / 2, t2.train_lift, w, color=C1, label="навчання 2013–2019"); ax[1].bar(x + w / 2, t2.test_lift_5_known_days, w, color=C3, label="тест 2020+ (5 днів)")
ax[1].set_xticks(x); ax[1].set_xticklabels([v.replace("W10080_", "7д ").replace("zr_and_zv>", "z_ret∧z_vol>").replace("Q90|r|/1,6449", "Q90")[:16] for v in t2.variant], rotation=35, ha="right", fontsize=9)
ax[1].set_ylabel("lift"); ax[1].legend(fontsize=9); ax[1].set_title("Часовий поділ: вибір на 2013–2019, оцінка на 2020+")
save(fig, F + "/pz3_oos_eval.png")
print("figures done (anomaly)")
