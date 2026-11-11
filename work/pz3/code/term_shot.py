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
ap.add_argument("--run", action="store_true", help="виконати --cmd у shell і зняти вивід")
a = ap.parse_args()
if a.from_file:
    txt = open(a.from_file, errors="replace").read()
else:
    env = dict(os.environ); env.pop("JAVA_TOOL_OPTIONS", None)
    txt = subprocess.run(a.cmd, shell=True, capture_output=True, text=True, env=env, timeout=120, executable="/bin/bash")
    txt = txt.stdout + txt.stderr
lines = txt.replace("\t", "    ").splitlines()
if a.grep:
    import re
    lines = [l for l in lines if re.search(a.grep, l)]
if a.head: lines = lines[:a.head]
if a.tail: lines = lines[-a.tail:]
out = []
for i, l in enumerate(["$ " + a.cmd] + lines):
    w = textwrap.wrap(l, a.width, subsequent_indent="    ", drop_whitespace=False, replace_whitespace=False) or [""]
    out += w
font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf", 15)
bold = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf", 15)
cw, lh = font.getlength("M"), 21
W, H = int(cw * a.width + 40), lh * len(out) + 50
img = Image.new("RGB", (W, H), (30, 30, 30)); d = ImageDraw.Draw(img)
d.rectangle([0, 0, W, 26], fill=(60, 60, 60))
for i, c in enumerate([(255, 95, 86), (255, 189, 46), (39, 201, 63)]):
    d.ellipse([10 + i * 20, 8, 22 + i * 20, 20], fill=c)
d.text((W // 2 - 40, 5), "bash", font=font, fill=(200, 200, 200))
for i, l in enumerate(out):
    col = (120, 220, 120) if i == 0 else (225, 225, 225)
    d.text((20, 36 + i * lh), l, font=bold if i == 0 else font, fill=col)
img.save(a.out); print("saved", a.out, W, H)
