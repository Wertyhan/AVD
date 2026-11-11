# -*- coding: utf-8 -*-
"""Рисунки до ПЗ2 з файлів results/ (matplotlib, dpi 150, підписи українською)."""
import os, numpy as np, pandas as pd, matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
np.random.seed(42)
BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
R, FG = f"{BASE}/results", f"{BASE}/figures"
plt.rcParams.update({"font.size": 10, "axes.grid": True, "grid.alpha": .3, "axes.spines.top": False, "axes.spines.right": False})
C = ["#1f77b4", "#d95f02", "#2a9d8f", "#7b2d8e"]
def out(n): plt.tight_layout(); plt.savefig(f"{FG}/{n}.png", dpi=150); plt.close()
daily = pd.read_parquet(f"{R}/pz2_daily_features.parquet"); hourly = pd.read_parquet(f"{R}/pz2_hourly_features.parquet")

# 1. Ціна закриття (лог-шкала) + обсяг
fig, ax = plt.subplots(2, 1, figsize=(10, 6), sharex=True, gridspec_kw={"height_ratios": [3, 1]})
ax[0].semilogy(daily.date, daily.close, color=C[0], lw=.9, label="Close (кінець доби)")
ax[0].semilogy(daily.date, daily.ma_30, color=C[1], lw=.9, label="ковзне середнє 30 діб")
ax[0].set_ylabel("Ціна, USD (лог. шкала)"); ax[0].legend(); ax[0].set_title("Ціна закриття BTC/USD, добові дані зі Spark")
ax[1].bar(daily.date, daily.volume, width=1, color=C[2]); ax[1].set_ylabel("Обсяг за добу, BTC"); ax[1].set_xlabel("Дата (UTC)")
out("fig01_close_log")

# 2. Розподіл дохідностей vs нормальний (лог. вісь Y)
from math import erf
h = pd.read_csv(f"{R}/pz2_hist_ret_1m_active.csv"); mk = pd.read_csv(f"{R}/pz2_skew_kurtosis.csv")
fig, ax = plt.subplots(1, 3, figsize=(13, 4))
def panel(a, x, bw, title, xlab, lim):
    x = x[np.isfinite(x)]; mu, sd = x.mean(), x.std()
    bins = np.arange(-lim, lim + bw, bw); cnt, e = np.histogram(x, bins=bins); ce = (e[1:] + e[:-1]) / 2
    dens = cnt / (len(x) * bw); ok = cnt > 0
    a.semilogy(ce[ok], dens[ok], ".", color=C[0], ms=4, label="емпіричний")
    g = np.linspace(-lim, lim, 400); a.semilogy(g, np.exp(-(g - mu) ** 2 / (2 * sd ** 2)) / (sd * np.sqrt(2 * np.pi)), color=C[1], label=f"нормальний (σ={sd:.4f})")
    a.set_title(title); a.set_xlabel(xlab); a.set_ylabel("Щільність (лог. шкала)"); a.legend(fontsize=8); a.set_ylim(bottom=1e-3)
x1 = np.repeat(h.bin_center.values, h["count"].values)
panel(ax[0], x1, 1e-4, "1 хвилина (свічки з торгами, |r|<0,01)", "Логарифмічна дохідність", 0.01)
panel(ax[1], hourly.ret_1h.dropna().values, 0.002, "1 година", "Логарифмічна дохідність", 0.1)
panel(ax[2], daily.ret_1d.dropna().values, 0.01, "1 доба", "Логарифмічна дохідність", 0.4)
out("fig02_returns_vs_normal")

# 3. Обсяг за годиною доби
vh = pd.read_csv(f"{R}/pz2_volume_by_hour.csv")
fig, ax = plt.subplots(figsize=(9, 4)); ax.bar(vh.hour, vh.share_pct, color=C[0])
ax.set_xlabel("Година доби (UTC)"); ax.set_ylabel("Частка сумарного обсягу, %"); ax.set_xticks(range(24))
ax.set_title("Розподіл торгового обсягу за годиною доби, 2012–2026"); out("fig03_volume_by_hour")

# 4. Волатильність у часі
fig, ax = plt.subplots(2, 1, figsize=(10, 6), sharex=True)
ax[0].plot(daily.date, daily.rv, color=C[0], lw=.5, label="добова реалізована волатильність rv")
ax[0].plot(daily.date, daily.vol_30d, color=C[1], lw=1.2, label="σ добових дохідностей, вікно 30 діб")
ax[0].set_yscale("log"); ax[0].set_ylabel("Волатильність (лог. шкала)"); ax[0].legend(fontsize=8)
ax[1].plot(daily.date, daily.vol60_mean, color=C[2], lw=.7); ax[1].set_yscale("log")
ax[1].set_ylabel("Середня за добу σ(60 хв)"); ax[1].set_xlabel("Дата (UTC)")
ax[0].set_title("Волатильність BTC/USD: добова rv та ковзна σ за 60 хвилин"); out("fig04_volatility")

# 5. Обсяг за роками + частка flat
y = pd.read_csv(f"{R}/pz2_yearly_volume_flat.csv")
fig, ax = plt.subplots(figsize=(9, 4.2)); ax.bar(y.year, y.volume / 1e6, color=C[0]); ax.set_xlabel("Рік"); ax.set_ylabel("Обсяг, млн BTC")
a2 = ax.twinx(); a2.plot(y.year, y.flat_share_pct, color=C[1], marker="o"); a2.set_ylabel("Частка хвилин з нульовим обсягом, %"); a2.grid(False)
ax.set_xticks(y.year); ax.tick_params(axis="x", rotation=45); ax.set_title("Річний обсяг і частка заповнених (flat) хвилин"); out("fig05_yearly_volume")

# 6. Топ-10 днів
t = pd.read_csv(f"{R}/pz2_top10_rv.csv"); t2 = pd.read_csv(f"{R}/pz2_top10_hl_rel_active.csv")
fig, ax = plt.subplots(1, 2, figsize=(12, 4.5))
ax[0].barh(t.date[::-1], t.rv[::-1], color=C[0]); ax[0].set_xlabel("Реалізована волатильність rv"); ax[0].set_title("Топ-10 за rv (повні доби)")
ax[1].barh(t2.date[::-1], t2.hl_rel[::-1] * 100, color=C[1]); ax[1].set_xlabel("(High–Low)/Open, %"); ax[1].set_title("Топ-10 за розмахом (flat < 50 %)")
out("fig06_top_days")

# 7. Погодинна й добова сезонність (теплова карта день тижня × година)
q = pd.read_csv(f"{R}/pz2_sql_q3_dow_hour_volume.csv").pivot(index="dow", columns="hour", values="avg_volume_per_minute")
fig, ax = plt.subplots(figsize=(10, 3.6)); im = ax.imshow(q.values, aspect="auto", cmap="viridis")
ax.set_yticks(range(7)); ax.set_yticklabels(["нд", "пн", "вт", "ср", "чт", "пт", "сб"]); ax.set_xticks(range(24)); ax.grid(False)
ax.set_xlabel("Година доби (UTC)"); ax.set_ylabel("День тижня"); plt.colorbar(im, label="Середній обсяг на хвилину, BTC")
ax.set_title("Середній хвилинний обсяг, 2018–2026 (Spark SQL)"); out("fig07_dow_hour_heatmap")

# 8. Час виконання: Spark vs pandas, кеш
s = pd.read_csv(f"{R}/pz2_engine_comparison_summary.csv"); c = pd.read_csv(f"{R}/pz2_cache_experiment.csv")
fig, ax = plt.subplots(1, 2, figsize=(12, 4))
tasks = ["year_count_volume", "daily_ohlc"]; names = {"year_count_volume": "кількість+обсяг за роками", "daily_ohlc": "денний OHLC"}
w = .26
for i, (e, lab) in enumerate([("pandas", "pandas"), ("spark_local[*]", "Spark local[*]"), ("spark_cached_df", "Spark, кеш у пам'яті")]):
    v = [s[(s.task == k) & (s.engine == e)]["median"].iloc[0] for k in tasks]
    b = ax[0].bar(np.arange(2) + (i - 1) * w, v, w, label=lab, color=C[i])
    for r_, vv in zip(b, v): ax[0].text(r_.get_x() + r_.get_width() / 2, vv, f"{vv:.1f}", ha="center", va="bottom", fontsize=8)
ax[0].set_xticks(range(2)); ax[0].set_xticklabels([names[k] for k in tasks]); ax[0].set_ylabel("Час, с (медіана з 3)"); ax[0].legend(fontsize=8)
ax[0].set_title("Той самий CSV: Spark vs pandas (Hadoop – після ПЗ1)")
cc = c[c["run"] != "materialize_count"].copy(); cc["lab"] = cc["mode"].map({"without_cache": "без cache()", "with_cache": "з cache()"})
for i, m in enumerate(["without_cache", "with_cache"]):
    v = cc[cc["mode"] == m].seconds.values; ax[1].bar(np.arange(3) + (i - .5) * .38, v, .38, color=C[i], label=cc[cc["mode"] == m].lab.iloc[0])
mat = c[c["run"] == "materialize_count"].seconds.iloc[0]
ax[1].set_xticks(range(3)); ax[1].set_xticklabels(["запуск 1", "запуск 2", "запуск 3"]); ax[1].set_ylabel("Час, с"); ax[1].legend(fontsize=8)
ax[1].set_title(f"Кешування (матеріалізація кешу: {mat:.0f} с)"); out("fig08_timings")

# 9. Об'ємна модель: залежність волатильності від рівня flat (чому flat слід враховувати)
fig, ax = plt.subplots(figsize=(8, 4)); ax.plot(daily.date, daily.flat_share * 100, color=C[3], lw=.8)
ax.set_ylabel("Частка flat-хвилин за добу, %"); ax.set_xlabel("Дата (UTC)"); ax.set_title("Частка хвилин із нульовим обсягом за добу"); out("fig09_flat_share")
print("ok")
