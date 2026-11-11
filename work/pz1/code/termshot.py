# -*- coding: utf-8 -*-
"""termshot.py - тонка обгортка над спільним рендером /home/user/AVD/work/tools_report/term_png.py
(1600 px, 90 колонок, <=35 рядків, перенос з «↪»). Зберігає старий інтерфейс render_terminal_png(cmd, text, path)."""
import sys
sys.path.insert(0, '/home/user/AVD/work/tools_report')
import term_png


def align_tabs(text):
    """Вирівнює табуляції (вивід Hive/Hadoop розділений \\t) у колонки; зміст рядків не змінюється, лише пробіли."""
    lines = text.rstrip('\n').split('\n')
    rows = [ln.split('\t') for ln in lines]
    tab = [r for r in rows if len(r) > 1]
    if len(tab) < 2:
        return text.expandtabs(8)
    ncol = max(len(r) for r in tab)
    w = [max((len(r[i]) for r in tab if i < len(r)), default=0) for i in range(ncol)]
    return '\n'.join('  '.join(c.ljust(w[i]) for i, c in enumerate(r)).rstrip() if len(r) > 1 else r[0] for r in rows)


def render_terminal_png(cmd, text, path, note=None, **kw):
    return term_png.render_terminal_png(cmd, align_tabs(text), str(path), note=note)
