# -*- coding: utf-8 -*-
"""Рендер реального виводу команди у PNG (темний термінал, рядок '$ команда' та фактичний вивід).
Використання: term_shot.py OUT.png --cmd "команда" [--from-file лог] [--head N] [--tail N] [--width 120]
  --from-file: показати збережений вивід (лог раніше виконаної команди) під рядком '$ cmd'."""
import argparse, subprocess, textwrap, os
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
# Правило читабельності: рядки <= 90 символів (довші обрізаються з «…»), разом <= 35 рядків (середина згортається «…»)
COLS, MAXL = 90, 35
lines = [l if len(l) <= COLS else l[:COLS - 1] + "…" for l in lines]
head = textwrap.wrap("$ " + a.cmd, COLS, subsequent_indent="    ", drop_whitespace=False) or ["$"]
extra = [("# " + a.comment) if a.comment else None]
extra = [e[:COLS - 1] + "…" if e and len(e) > COLS else e for e in extra if e]
room = MAXL - len(head) - len(extra)
if len(lines) > room:
    keep_h = room // 2; keep_t = room - keep_h - 1
    lines = lines[:keep_h] + ["…"] + lines[len(lines) - keep_t:]
out = head + extra + lines
SZ = 30
font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf", SZ)
bold = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf", SZ)
cw, lh = font.getlength("M"), int(SZ * 1.3)
PAD, TOP = 20, 50
W, H = int(cw * COLS + 2 * PAD), lh * len(out) + TOP + 20
img = Image.new("RGB", (W, H), (30, 30, 30)); d = ImageDraw.Draw(img)
d.rectangle([0, 0, W, 40], fill=(60, 60, 60))
for i, c in enumerate([(255, 95, 86), (255, 189, 46), (39, 201, 63)]):
    d.ellipse([14 + i * 32, 10, 34 + i * 32, 30], fill=c)
for i, l in enumerate(out):
    if i < len(head): col, f = (120, 220, 120), bold
    elif l.startswith("# фрагмент") or (extra and i == len(head)): col, f = (140, 140, 140), font
    else: col, f = (225, 225, 225), font
    d.text((PAD, TOP + i * lh), l, font=f, fill=col)
img.save(a.out); print("saved", a.out, W, H, len(out), "рядків")
