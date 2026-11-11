# -*- coding: utf-8 -*-
"""Спільний стиль рисунків ПЗ3 (RULES 2b): розмір для друку 6,3×3,4 дюйма (два рядки панелей 6,3×4,7), шрифти ≥ 9 pt
(тіки 9, підписи й легенда 10, заголовок 11), dpi 200, десяткова кома на осях. Якщо є work/tools_report/plotstyle.py – застосовується він,
далі локальні значення (вони діють як мінімальні)."""
import sys
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
sys.path.insert(0, "/home/user/AVD/work/tools_report")
try:
    import plotstyle  # noqa: F401  (спільний хелпер координатора, якщо вже доданий)
except Exception:
    pass
plt.rcParams.update({"font.size": 10, "axes.titlesize": 11, "axes.labelsize": 10, "xtick.labelsize": 9, "ytick.labelsize": 9, "legend.fontsize": 10,
                     "axes.grid": True, "grid.alpha": .3, "axes.spines.top": False, "axes.spines.right": False, "savefig.dpi": 200, "figure.dpi": 100,
                     "axes.unicode_minus": False})
W, H1, H2 = 6.3, 3.4, 4.7
C1, C2, C3, C4, C5 = "#1f77b4", "#d62728", "#2ca02c", "#ff7f0e", "#9467bd"
dec = lambda x, n=2: f"{x:.{n}f}".replace(".", ",")
COMMA = FuncFormatter(lambda x, _: f"{x:g}".replace(".", ","))


def commaize(*axes, axis="both"):
    for a in axes:
        if axis in ("both", "x"):
            a.xaxis.set_major_formatter(COMMA)
        if axis in ("both", "y"):
            a.yaxis.set_major_formatter(COMMA)


def save(fig, path):
    fig.tight_layout(); fig.savefig(path, dpi=200); plt.close(fig)
