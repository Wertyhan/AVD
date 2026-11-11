# -*- coding: utf-8 -*-
"""Спільний стиль рисунків (RULES.md, правило 2b): друкований розмір, шрифти >= 9 pt, dpi 200, десяткова кома.
Використання: sys.path.insert(0, '/home/user/AVD/work/tools_report'); import plotstyle  (стиль застосовується при імпорті).
Розміри: plotstyle.SINGLE = (6.3, 3.4), plotstyle.TWO_PANEL = (6.3, 4.7)."""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, ScalarFormatter

SINGLE = (6.3, 3.4)
TWO_PANEL = (6.3, 4.7)

plt.rcParams.update({
    "font.family": "DejaVu Sans", "font.size": 9, "axes.labelsize": 10, "axes.titlesize": 11,
    "xtick.labelsize": 9, "ytick.labelsize": 9, "legend.fontsize": 10,
    "axes.spines.top": False, "axes.spines.right": False, "axes.grid": True, "grid.alpha": .3,
    "figure.figsize": SINGLE, "figure.dpi": 200, "savefig.dpi": 200, "axes.unicode_minus": False,
})


def dec(v, d=None):
    """Число -> рядок з десятковою комою (d знаків або %g)."""
    t = f"{v:.{d}f}" if d is not None else f"{v:g}"
    return t.replace(".", ",")


comma_fmt = FuncFormatter(lambda v, _: dec(v))
space_fmt = FuncFormatter(lambda v, _: f"{v:,.0f}".replace(",", " "))


def commas(ax, which="y"):
    (ax.yaxis if which == "y" else ax.xaxis).set_major_formatter(comma_fmt)


# десяткова кома у стандартних (ScalarFormatter) підписах осей
_orig_call = ScalarFormatter.__call__
def _call(self, x, pos=None):
    return _orig_call(self, x, pos).replace(".", ",")
ScalarFormatter.__call__ = _call
