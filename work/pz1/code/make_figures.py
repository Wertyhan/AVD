#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""make_figures.py - рисунки до ПЗ1 (matplotlib, підписи українською, dpi 200) за результатами MapReduce/Hive/бенчмарку."""
from pathlib import Path
import pandas as pd, numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

R = Path('/home/user/AVD/work/pz1/results'); F = Path('/home/user/AVD/work/pz1/figures'); F.mkdir(exist_ok=True)
import sys; sys.path.insert(0, '/home/user/AVD/work/tools_report')
import plotstyle  # правило 2b: 6,3 x 3,4 дюйма, шрифти >= 9 pt, dpi 200, десяткова кома
C1, C2, C3, C4 = '#2a6fdb', '#e8890c', '#2a9d5c', '#8a4fbf'
fmt_space = FuncFormatter(lambda x, _: f'{x:,.0f}'.replace(',', ' '))

# 1. обсяг торгів за роками (результат MapReduce)
y = pd.read_csv(R / 'mr_yearly_stats.csv')
fig, ax = plt.subplots(figsize=(6.3, 3.4))
ax.bar(y.year.astype(str), y.volume / 1e6, color=C1)
ax.set_xlabel('Рік (UTC)'); ax.set_ylabel('Сумарний обсяг, млн BTC'); ax.set_title('Сумарний обсяг торгів за роками (Hadoop Streaming)')
for i, v in enumerate(y.volume / 1e6): ax.text(i, v, f'{v:.1f}'.replace('.', ','), ha='center', va='bottom', fontsize=9)
plt.xticks(rotation=45); fig.tight_layout(); fig.savefig(F / 'fig_mr_yearly_volume.png', dpi=200); plt.close(fig)

# 2. кількість свічок за роками
fig, ax = plt.subplots(figsize=(6.3, 3.4))
ax.bar(y.year.astype(str), y.candles / 1e3, color=C2)
ax.set_xlabel('Рік (UTC)'); ax.set_ylabel('Кількість хвилинних свічок, тис.'); ax.set_title('Кількість 1-хвилинних свічок за роками (Hadoop Streaming)')
for i, v in enumerate(y.candles / 1e3): ax.text(i, v, f'{v:.0f}', ha='center', va='bottom', fontsize=9)
plt.xticks(rotation=45); fig.tight_layout(); fig.savefig(F / 'fig_mr_yearly_candles.png', dpi=200); plt.close(fig)

# 3. добова ціна закриття з результату MapReduce, логарифмічна шкала
d = pd.read_csv(R / 'mr_daily_ohlc_full.csv', parse_dates=['day'])
fig, ax = plt.subplots(figsize=(6.3, 3.4))
ax.plot(d.day, d.close, color=C1, lw=1)
ax.set_yscale('log'); ax.set_xlabel('Дата (UTC)'); ax.set_ylabel('Ціна закриття доби, USD (лог. шкала)')
ax.set_title(f'Добова ціна закриття BTC/USD за результатом MapReduce ({len(d)} діб)')
ax.yaxis.set_major_formatter(fmt_space); fig.tight_layout(); fig.savefig(F / 'fig_mr_daily_close_log.png', dpi=200); plt.close(fig)

# 4. порівняння часу виконання (медіана, діапазон min-max)
S = pd.read_csv(R / 'timing_summary.csv')
tools = ['Hadoop Streaming', 'Hive (MR)', 'Pandas', 'PySpark (local[*])']
labels = ['Hadoop\nStreaming', 'Hive\n(MR)', 'Pandas', 'PySpark\n(local[*])']
cols = [C1, C2, C3, C4]
fig, axs = plt.subplots(1, 2, figsize=(6.3, 3.4))
for ax, task, title in zip(axs, ['yearly', 'daily_ohlc'], ['Свічки й обсяг за роками', 'Добові OHLC']):
    s = S[S.task == task].set_index('tool').loc[tools]
    err = np.vstack([s['median'] - s['min'], s['max'] - s['median']])
    ax.bar(labels, s['median'], color=cols, yerr=err, capsize=5)
    for i, (v, mx) in enumerate(zip(s['median'], s['max'])): ax.text(i, mx * 1.02, f'{v:.1f}'.replace('.', ','), ha='center', va='bottom', fontsize=9, fontweight='bold')
    ax.set_ylim(0, s['max'].max() * 1.12)
    ax.set_title(title); ax.set_ylabel('Час, с (медіана з 3)')
fig.suptitle('Порівняння часу: 7,8 млн рядків CSV, 4 vCPU / 15 GiB', y=1.0); fig.tight_layout()
fig.savefig(F / 'fig_timing_comparison.png', dpi=200); plt.close(fig)

# 5. топ-10 діб за обсягом та за амплітудою High-Low (результат Hive)
tv = pd.read_csv(R / 'hive_top10_volume_days.csv'); tr = pd.read_csv(R / 'hive_top10_range_days.csv')
fig, axs = plt.subplots(1, 2, figsize=(6.3, 4.0))
axs[0].barh(tv.trade_date.astype(str)[::-1], tv.volume[::-1] / 1e3, color=C1)
axs[0].set_xlabel('Обсяг за добу, тис. BTC'); axs[0].set_title('Топ-10 за обсягом (Hive)')
axs[1].barh(tr.trade_date.astype(str)[::-1], tr.price_range[::-1], color=C2)
axs[1].set_xlabel('High − Low за добу, USD'); axs[1].set_title('Топ-10 за High − Low (Hive)')
for a in axs: a.xaxis.set_major_formatter(fmt_space)
fig.tight_layout(); fig.savefig(F / 'fig_top_days.png', dpi=200); plt.close(fig)

# 6. щомісячний обсяг (Hive GROUP BY рік, місяць)
m = pd.read_csv(R / 'hive_monthly_stats.csv')
m['date'] = pd.to_datetime(dict(year=m.yr, month=m.mn, day=1))
fig, ax = plt.subplots(figsize=(6.3, 3.4))
ax.plot(m.date, m.total_volume / 1e3, color=C3, lw=1.3)
ax.set_xlabel('Місяць (UTC)'); ax.set_ylabel('Обсяг за місяць, тис. BTC'); ax.set_title('Місячний обсяг торгів BTC (Hive, GROUP BY рік, місяць)')
fig.tight_layout(); fig.savefig(F / 'fig_hive_monthly_volume.png', dpi=200); plt.close(fig)
print('figures:', sorted(p.name for p in F.glob('*.png')))
