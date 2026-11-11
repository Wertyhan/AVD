# -*- coding: utf-8 -*-
"""(ПЗ4) Рендер реального виводу програми (screenshots/txt/*.txt) у PNG спільним рендером
tools_report/term_png.py (1600 px, 90 колонок, <=35 рядків, перенос з «↪»). Формат txt: рядок 1 "$ команда",
необов'язковий рядок 2 "# примітка", далі вивід; кінцевий рядок "… (ще N рядків)" означає скорочення на етапі генерації."""
import glob, os, re, sys
sys.path.insert(0, "/home/user/AVD/work/tools_report")
from term_png import render_terminal_png
BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
for p in sorted(glob.glob(f"{BASE}/screenshots/txt/*.txt")):
    lines = open(p, encoding="utf-8").read().rstrip("\n").split("\n")
    cmd = lines[0][2:] if lines[0].startswith("$ ") else lines[0]
    rest = lines[1:]
    note = None
    if rest and rest[0].startswith("# "):
        note, rest = rest[0][2:], rest[1:]
    extra = 0
    if rest:
        m = re.match(r"^… \(ще (\d+) рядків\)$", rest[-1])
        if m:
            extra, rest = int(m.group(1)), rest[:-1]
    out = f"{BASE}/screenshots/term_{os.path.basename(p)[:-4]}.png"
    render_terminal_png(cmd, "\n".join(rest), out, note=note, tail=0 if extra else 10, extra_skipped=extra)
    print(out)
