# -*- coding: utf-8 -*-
"""Рендер реального виводу команди у PNG (темний термінал, рядок '$ команда' та фактичний вивід).
Використання: term_shot.py OUT.png --cmd "команда" [--from-file лог] [--head N] [--tail N] [--width 120]
  --from-file: показати збережений вивід (лог раніше виконаної команди) під рядком '$ cmd'."""
import argparse, subprocess, textwrap, os, sys
from PIL import Image, ImageDraw, ImageFont
ap = argparse.ArgumentParser()
ap.add_argument("out"); ap.add_argument("--cmd", required=True); ap.add_argument("--from-file", default="")
ap.add_argument("--head", type=int, default=0); ap.add_argument("--tail", type=int, default=0)
ap.add_argument("--grep", default=""); ap.add_argument("--width", type=int, default=125)
ap.add_argument("--comment", default="")
ap.add_argument("--save", default="", help="зберегти сирий вивід команди у файл")
ap.add_argument("--run", action="store_true", help="виконати --cmd у shell і зняти вивід")
a = ap.parse_args()
if a.from_file:
    txt = open(a.from_file, errors="replace").read()
else:
    env = dict(os.environ); env.pop("JAVA_TOOL_OPTIONS", None)
    txt = subprocess.run(a.cmd, shell=True, capture_output=True, text=True, env=env, timeout=120, executable="/bin/bash")
    txt = txt.stdout + txt.stderr
if a.save:
    open(a.save, "w").write(txt)
lines = txt.replace("\t", "    ").splitlines()
if a.grep:
    import re
    lines = [l for l in lines if re.search(a.grep, l)]
if a.head: lines = lines[:a.head]
if a.tail: lines = lines[-a.tail:]
# Рендер термінального знімка (90 колонок на всю ширину 1600 px, <= 35 рядків, перенос довгих рядків маркером)
sys.path.insert(0, os.environ.get("TERM_PNG_DIR", "."))   # каталог із term_png.py
from term_png import render_terminal_png
if a.save:
    open(a.save + ".cmd", "w").write(a.cmd)
size = render_terminal_png(a.cmd, "\n".join(lines), a.out, note=a.comment or None)
print("saved", a.out, size)
