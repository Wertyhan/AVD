# -*- coding: utf-8 -*-
"""term_png.py - спільний рендер РЕАЛЬНОГО виводу команди у PNG (ПЗ1-ПЗ5).
Ширина 1600 px, моноширинний шрифт підібрано так, що 90 колонок займають усю ширину.
Рядки > 90 символів переносяться з маркером «↪», вивід > 35 рядків скорочується
рядком «… (N рядків пропущено)»; висота зображення підганяється під вміст."""
import os
from PIL import Image, ImageDraw, ImageFont

COLS, MAX_LINES, WIDTH, PAD = 90, 35, 1600, 20
_FONTS = ['/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf',
          '/usr/share/fonts/truetype/liberation/LiberationMono-Regular.ttf']
_BOLD = '/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf'
BG, BAR, PROMPT, CMD, DIM, TXT = (24, 26, 31), (52, 56, 66), (120, 230, 130), (245, 245, 245), (130, 140, 155), (220, 222, 228)


def _font(path, size):
    return ImageFont.truetype(path, size)


def _fit_size(path):
    """Розмір шрифту, при якому COLS символів займають WIDTH - 2*PAD."""
    target = (WIDTH - 2 * PAD) / COLS
    size = 30.0
    for _ in range(20):
        adv = _font(path, size).getlength('M')
        size *= target / adv
    return size


def wrap(line, cols=COLS, marker='↪ '):
    line = line.expandtabs(8).rstrip('\r')
    if len(line) <= cols:
        return [line]
    out = [line[:cols]]
    rest = line[cols:]
    w = cols - len(marker)
    while rest:
        out.append(marker + rest[:w]); rest = rest[w:]
    return out


def render_terminal_png(cmd, text, path, note=None, max_lines=MAX_LINES, tail=10):
    """cmd - команда (без '$'), text - вивід, note - підпис «# фрагмент виводу: …» (необов'язково)."""
    path_f = next(p for p in _FONTS if os.path.exists(p))
    size = _fit_size(path_f)
    f = _font(path_f, size)
    fb = _font(_BOLD, size) if os.path.exists(_BOLD) else f
    adv = f.getlength('M')
    lh = int(round(size * 1.28))
    # заголовок: "$ cmd" з переносом
    c = ' '.join(cmd.split('\n'))
    head, first, w = [], True, COLS - 2
    while c:
        head.append(c[:w]); c = c[w:]
    body = []
    for ln in text.rstrip('\n').split('\n'):
        body += wrap(ln)
    if len(body) > max_lines:
        keep_t = min(tail, max_lines - 2)
        keep_h = max_lines - 1 - keep_t
        cut = len(body) - keep_h - keep_t
        body = body[:keep_h] + [f'… ({cut} рядків пропущено)'] + body[len(body) - keep_t:]
    nl = len(head) + (1 if note else 0) + len(body)
    bar, gap = 34, 12
    H = bar + PAD + nl * lh + gap + PAD
    img = Image.new('RGB', (WIDTH, H), BG)
    d = ImageDraw.Draw(img)
    d.rectangle([0, 0, WIDTH, bar], fill=BAR)
    for i, col in enumerate([(255, 95, 86), (255, 189, 46), (39, 201, 63)]):
        d.ellipse([14 + i * 26, 9, 30 + i * 26, 25], fill=col)
    y = bar + PAD
    for i, ln in enumerate(head):
        if i == 0:
            d.text((PAD, y), '$', font=fb, fill=PROMPT)
        d.text((PAD + 2 * adv, y), ln, font=fb, fill=CMD); y += lh
    if note:
        d.text((PAD, y), ('# фрагмент виводу: ' + note)[:COLS], font=f, fill=DIM); y += lh
    for ln in body:
        d.text((PAD, y), ln, font=f, fill=DIM if ln.startswith('… (') else TXT); y += lh
    img.save(path)
    return img.size
