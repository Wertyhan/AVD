# -*- coding: utf-8 -*-
"""Рисунки до ПЗ2 з файлів results/ (matplotlib, dpi 150, підписи українською)."""
import os, numpy as np, pandas as pd, matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
np.random.seed(42)
BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
R, FG = f"{BASE}/results", f"{BASE}/figures"
import sys as _s; _s.path.insert(0, '/home/user/AVD/work/tools_report'); import plotstyle  # правило 2b
plt.rcParams.update({"font.size": 9, "axes.labelsize": 10, "axes.titlesize": 11, "legend.fontsize": 10, "xtick.labelsize": 9, "ytick.labelsize": 9, "axes.grid": True, "grid.alpha": .3, "axes.spines.top": False, "axes.spines.right": False})
C = ["#1f77b4", "#d95f02", "#2a9d8f", "#7b2d8e"]
from matplotlib.ticker import ScalarFormatter
def _fmt(v, _=None):
    return f"{v:,.0f}".replace(",", " ") if abs(v) >= 1e4 else f"{v:.10g}".replace(".", ",")
def out(n):
    """Десяткова кома на всіх лінійних осях; збереження рисунка."""
    fig = plt.gcf()
    for ax in fig.axes:
        for axis in (ax.xaxis, ax.yaxis):
            if isinstance(axis.get_major_formatter(), ScalarFormatter) and axis.get_scale() == "linear":
                axis.set_major_formatter(FuncFormatter(_fmt))
    import textwrap
    for ax in fig.axes:
        t = ax.get_title()
        if t: ax.set_title("\n".join(textwrap.wrap(t, 62)), fontsize=11)
    plt.tight_layout(); plt.savefig(f"{FG}/{n}.png", dpi=200); plt.close()
def cm(x, d=2): return f"{x:.{d}f}".replace(".", ",")
daily = pd.read_parquet(f"{R}/pz2_daily_features.parquet"); hourly = pd.read_parquet(f"{R}/pz2_hourly_features.parquet")

# 1. Ціна закриття (лог-шкала) + обсяг
fig, ax = plt.subplots(2, 1, figsize=(6.3, 4.7), sharex=True, gridspec_kw={"height_ratios": [3, 1]})
ax[0].semilogy(daily.date, daily.close, color=C[0], lw=.9, label="Close (кінець доби)")
ax[0].semilogy(daily.date, daily.ma_30, color=C[1], lw=.9, label="ковзне середнє 30 діб")
ax[0].set_ylabel("Ціна, USD (лог. шкала)"); ax[0].legend(); ax[0].set_title("Ціна закриття BTC/USD, добові дані зі Spark")
ax[1].bar(daily.date, daily.volume, width=1, color=C[2]); ax[1].set_ylabel("Обсяг за добу, BTC"); ax[1].set_xlabel("Дата (UTC)")
out("fig01_close_log")

# 2. Розподіл дохідностей vs нормальний (лог. вісь Y)
from math import erf
h = pd.read_csv(f"{R}/pz2_hist_ret_1m_active.csv"); mk = pd.read_csv(f"{R}/pz2_skew_kurtosis.csv")
fig, ax = plt.subplots(2, 2, figsize=(6.3, 4.7)); ax = ax.ravel(); ax[3].axis("off")
def panel(a, x, bw, title, xlab, lim):
    x = x[np.isfinite(x)]; mu, sd = x.mean(), x.std()
    bins = np.arange(-lim, lim + bw, bw); cnt, e = np.histogram(x, bins=bins); ce = (e[1:] + e[:-1]) / 2
    dens = cnt / (len(x) * bw); ok = cnt > 0
    a.semilogy(ce[ok], dens[ok], ".", color=C[0], ms=3, label="емпіричний")
    g = np.linspace(-lim, lim, 400); a.semilogy(g, np.exp(-(g - mu) ** 2 / (2 * sd ** 2)) / (sd * np.sqrt(2 * np.pi)), color=C[1], label=f"нормальний (σ вибірки = {cm(sd,4)})")
    a.set_title(title); a.set_xlabel(xlab); a.set_ylabel("Щільність (лог. шкала)"); a.legend(); a.set_ylim(bottom=1e-3)
x1 = np.repeat(h.bin_center.values, h["count"].values)
panel(ax[0], x1, 1e-4, "1 хвилина (свічки з торгами, обрізана вибірка |r|<0,01)", "Логарифмічна дохідність", 0.01)
panel(ax[1], hourly.ret_1h.dropna().values, 0.002, "1 година", "Логарифмічна дохідність", 0.1)
panel(ax[2], daily.ret_1d.dropna().values, 0.01, "1 доба", "Логарифмічна дохідність", 0.4)
out("fig02_returns_vs_normal")

# 3. Обсяг за годиною доби
vh = pd.read_csv(f"{R}/pz2_volume_by_hour.csv")
fig, ax = plt.subplots(figsize=(6.3, 3.4)); ax.bar(vh.hour, vh.share_pct, color=C[0])
ax.set_xlabel("Година доби (UTC)"); ax.set_ylabel("Частка сумарного обсягу, %"); ax.set_xticks(range(24))
ax.set_title("Розподіл торгового обсягу за годиною доби, 2012–2026"); out("fig03_volume_by_hour")

# 4. Волатильність у часі
fig, ax = plt.subplots(2, 1, figsize=(6.3, 4.7), sharex=True)
ax[0].plot(daily.date, daily.rv, color=C[0], lw=.5, label="добова реалізована волатильність rv")
ax[0].plot(daily.date, daily.vol_30d, color=C[1], lw=1.2, label="σ добових дохідностей, вікно 30 діб")
ax[0].set_yscale("log"); ax[0].set_ylabel("Волатильність (лог. шкала)"); ax[0].legend()
ax[1].plot(daily.date, daily.vol60_mean, color=C[2], lw=.7); ax[1].set_yscale("log")
ax[1].set_ylabel("Середня за добу σ(60 хв)"); ax[1].set_xlabel("Дата (UTC)")
ax[0].set_title("Волатильність BTC/USD: добова rv та ковзна σ за 60 хвилин"); out("fig04_volatility")

# 5. Обсяг за роками + частка flat
y = pd.read_csv(f"{R}/pz2_yearly_volume_flat.csv")
fig, ax = plt.subplots(figsize=(6.3, 3.4)); ax.bar(y.year, y.volume / 1e6, color=C[0]); ax.set_xlabel("Рік"); ax.set_ylabel("Обсяг, млн BTC")
a2 = ax.twinx(); a2.plot(y.year, y.flat_share_pct, color=C[1], marker="o"); a2.set_ylabel("Частка хвилин з нульовим обсягом, %"); a2.grid(False)
ax.set_xticks(y.year); ax.tick_params(axis="x", rotation=45); ax.set_title("Річний обсяг і частка заповнених (flat) хвилин"); out("fig05_yearly_volume")

# 6. Топ-10 днів
t = pd.read_csv(f"{R}/pz2_top10_rv.csv"); t2 = pd.read_csv(f"{R}/pz2_top10_hl_rel_active.csv")
fig, ax = plt.subplots(2, 1, figsize=(6.3, 4.7))
ax[0].barh(t.date[::-1], t.rv[::-1], color=C[0]); ax[0].set_xlabel("Реалізована волатильність rv"); ax[0].set_title("Топ-10 за rv (повні доби)")
ax[1].barh(t2.date[::-1], t2.hl_rel[::-1] * 100, color=C[1]); ax[1].set_xlabel("(High–Low)/Open, %"); ax[1].set_title("Топ-10 за розмахом (flat < 50 %)")
out("fig06_top_days")

# 7. Погодинна й добова сезонність (теплова карта день тижня × година)
q = pd.read_csv(f"{R}/pz2_sql_q3_dow_hour_volume.csv").pivot(index="dow", columns="hour", values="avg_volume_per_minute")
fig, ax = plt.subplots(figsize=(6.3, 3.4)); im = ax.imshow(q.values, aspect="auto", cmap="viridis")
ax.set_yticks(range(7)); ax.set_yticklabels(["нд", "пн", "вт", "ср", "чт", "пт", "сб"]); ax.set_xticks(range(24)); ax.grid(False)
ax.set_xlabel("Година доби (UTC)"); ax.set_ylabel("День тижня"); plt.colorbar(im, label="Середній обсяг на хвилину, BTC")
ax.set_title("Середній хвилинний обсяг, 2018–2026 (Spark SQL)"); out("fig07_dow_hour_heatmap")

# 8. Порівняння рушіїв: медіана з 3 запусків, відрізок = мін–макс
s = pd.read_csv(f"{R}/pz2_engine_comparison_summary.csv"); c = pd.read_csv(f"{R}/pz2_cache_experiment.csv")
fig, ax = plt.subplots(figsize=(6.3, 3.4))
tasks = ["year_count_volume", "daily_ohlc"]; names = {"year_count_volume": "кількість+обсяг за роками", "daily_ohlc": "денний OHLC"}
w = .26
for i, (e, lab) in enumerate([("pandas", "pandas (1 потік)"), ("spark_local[*]", "Spark local[*], з CSV"), ("spark_cached_df", "Spark, кеш у пам'яті")]):
    r_ = [s[(s.task == k) & (s.engine == e)].iloc[0] for k in tasks]
    v = [x["median"] for x in r_]; lo = [x["median"] - x["min"] for x in r_]; hi = [x["max"] - x["median"] for x in r_]
    b = ax.bar(np.arange(2) + (i - 1) * w, v, w, yerr=[lo, hi], capsize=3, label=lab, color=C[i])
    for rr, vv in zip(b, v): ax.text(rr.get_x() + rr.get_width() / 2, vv, cm(vv, 1), ha="center", va="bottom")
ax.set_xticks(range(2)); ax.set_xticklabels([names[k] for k in tasks]); ax.set_ylabel("Час, с (медіана, мін–макс)"); ax.legend()
ax.set_title("Spark проти pandas на тому самому CSV (запуски чергувалися)"); out("fig08_engines")

# 9. Кешування: кеш із 16 розділів
fig, ax = plt.subplots(2, 1, figsize=(6.3, 4.7))
u = c[c["mode"] == "without_cache"].seconds.values; cc_ = c[(c["mode"] == "with_cache") & (~c.run.str.startswith("materialize"))].seconds.values
mat = c[c.run.str.startswith("materialize")].seconds.values
ax[0].bar(np.arange(3) - .2, u, .4, color=C[0], label="без cache()"); ax[0].bar(np.arange(3) + .2, cc_, .4, color=C[1], label="з cache()")
ax[0].set_xticks(range(3)); ax[0].set_xticklabels(["раунд 1", "раунд 2", "раунд 3"]); ax[0].set_ylabel("Час двох запитів, с"); ax[0].legend()
ax[0].set_title(f"Запит без кешу та з кешем (медіани: {cm(np.median(u),1)} с і {cm(np.median(cc_),1)} с)")
ax[1].bar(range(3), mat, color=C[2]); ax[1].set_xticks(range(3)); ax[1].set_xticklabels(["раунд 1", "раунд 2", "раунд 3"]); ax[1].set_ylabel("Час, с")
ax[1].set_title(f"Матеріалізація кешу (медіана {cm(np.median(mat),1)} с)"); out("fig09_cache")

# 10. Частка flat-хвилин за добу
fig, ax = plt.subplots(figsize=(6.3, 3.4)); ax.plot(daily.date, daily.flat_share * 100, color=C[3], lw=.8)
ax.set_ylabel("Частка хвилин із нульовим обсягом за добу, %"); ax.set_xlabel("Дата (UTC)"); ax.set_title("Частка хвилин із нульовим обсягом за добу"); out("fig10_flat_share")

# 11. Ледачі обчислення: ціна різних дій над одним DataFrame
a_ = pd.read_csv(f"{R}/pz2_lazy_vs_action_summary.csv")
lab = {"transformations_build_only": "лише трансформації\n(план без дії)", "count": "count()\n(вікна обрізано)", "agg_sum_vol60_logret": "sum(vol60), sum(log_ret)\n(вікна потрібні)", "noop_write": "write.format(\"noop\")\n(усі стовпці)"}
fig, ax = plt.subplots(figsize=(6.3, 3.4)); x = np.arange(len(a_))
ax.bar(x, a_["median"], color=[C[2], C[0], C[1], C[1]], yerr=[a_["median"] - a_["min"], a_["max"] - a_["median"]], capsize=3)
for xi, v in zip(x, a_["median"]): ax.text(xi, v, cm(v, 2), ha="center", va="bottom", fontsize=9)
ax.set_xticks(x); ax.set_xticklabels([lab[k] for k in a_.action]); ax.set_ylabel("Час, с (медіана з 3)")
ax.set_title("Трансформації та дії: count() не обчислює вікон, повні дії – обчислюють"); out("fig11_lazy_actions")

# 12. Доби зі сплеском обсягу (Q7): сплеск без кризи теж трапляється
q7 = pd.read_csv(f"{R}/pz2_sql_q7_volume_spike_days.csv")
fig, ax = plt.subplots(figsize=(6.3, 3.4))
m = q7.in_q6_list == 1
ax.scatter(q7[~m].volume_x_prev30_mean, q7[~m].ret_pct, color=C[0], s=28, label="інші доби зі сплеском")
ax.scatter(q7[m].volume_x_prev30_mean, q7[m].ret_pct, color=C[1], s=40, marker="D", label="6 відомих кризових діб (Q6)")
ax.axhline(0, color="gray", lw=.6); ax.axhline(10, color="gray", lw=.6, ls=":"); ax.axhline(-10, color="gray", lw=.6, ls=":")
ax.set_xlabel("Обсяг / середній обсяг попередніх 30 діб"); ax.set_ylabel("Дохідність за добу, %"); ax.legend()
ax.set_title("Усі доби з 2018 року зі сплеском обсягу (порогові значення – Q7)"); out("fig12_volume_spikes")
print("ok")
