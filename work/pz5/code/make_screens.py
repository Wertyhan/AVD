# -*- coding: utf-8 -*-
"""Рендер РЕАЛЬНОГО виводу програм (screenshots/txt/*.txt) у PNG через спільний tools_report/term_png.py.
Формат txt: рядок 1 – '$ python code/<скрипт>' (справжня команда), рядок 2 – '# коментар', далі – захоплений вивід."""
import glob, os, sys
sys.path.insert(0, "/home/user/AVD/work/tools_report"); sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from term_png import render_terminal_png
from pz5_common import SHOTS, TXT
for p in sorted(glob.glob(f"{TXT}/*.txt")):
    L = open(p, encoding="utf-8").read().rstrip("\n").split("\n")
    assert L[0].startswith("$ ") and L[1].startswith("# "), p
    cmd, note, body = L[0][2:], L[1][2:], "\n".join(L[2:])
    out = f"{SHOTS}/term_{os.path.basename(p)[:-4]}.png"
    print(out, render_terminal_png(cmd, body, out, note=note))
