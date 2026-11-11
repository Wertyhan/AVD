# -*- coding: utf-8 -*-
"""Рендер реального виводу програми (screenshots/txt/*.txt) у PNG: темний фон, моноширинний шрифт."""
import glob, os
from PIL import Image, ImageDraw, ImageFont
BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
FONT = "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf"
BOLD = "/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf"
W, PAD, MAXLINES, MAXCOLS = 1600, 20, 35, 90
def fonts(sz): return ImageFont.truetype(FONT, sz), ImageFont.truetype(BOLD, sz)
for p in sorted(glob.glob(f"{BASE}/screenshots/txt/*.txt")):
    lines = open(p, encoding="utf-8").read().rstrip("\n").split("\n")
    lines = [l.expandtabs(4) for l in lines]
    note = None
    if len(lines) > MAXLINES:
        note = f"... (перші {MAXLINES - 1} рядків із {len(lines)}; повний вивід у results/)"
        lines = lines[:MAXLINES - 1]
    lines = [(l[:MAXCOLS - 1] + "…") if len(l) > MAXCOLS else l for l in lines]
    if note: lines.append(note)
    cols = min(MAXCOLS, max(40, max(len(l) for l in lines)))
    SZ = min(30, int((W - 2 * PAD) / (cols * 0.602)))        # шрифт: cols символів заповнюють ширину ~1600 px
    f, fb = fonts(SZ); cw = f.getlength("M"); lh = SZ + 6
    W_ = int(cols * cw + 2 * PAD)
    h = lh * (len(lines)) + 2 * PAD + 34
    im = Image.new("RGB", (W_, h), (22, 24, 29)); d = ImageDraw.Draw(im)
    d.rectangle([0, 0, im.width, 30], fill=(48, 52, 62))
    for i, c in enumerate([(255, 95, 86), (255, 189, 46), (39, 201, 63)]):
        d.ellipse([12 + i * 22, 9, 24 + i * 22, 21], fill=c)
    d.text((100, 6), "bash – pz2", font=fonts(18)[0], fill=(190, 195, 205))
    y = 30 + PAD
    for i, l in enumerate(lines):
        if i == 0 and l.startswith("$"):
            d.text((PAD, y), "$", font=fb, fill=(120, 220, 120)); d.text((PAD + cw * 2, y), l[2:], font=fb, fill=(240, 240, 240))
        elif i == 1 and l.startswith("# фрагмент виводу"):
            d.text((PAD, y), l, font=f, fill=(110, 118, 130))            # приглушений коментар
        else:
            d.text((PAD, y), l, font=f, fill=(205, 210, 220))
        y += lh
    out = f"{BASE}/screenshots/term_{os.path.basename(p)[:-4]}.png"
    im.save(out); print(out)
