# -*- coding: utf-8 -*-
"""termshot.py - рендер РЕАЛЬНОГО виводу команди у PNG-«вікно термінала» (темний фон, моноширинний шрифт, рядок '$ команда')."""
import os
def _font(size):
    from PIL import ImageFont
    cands = ['/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf', '/usr/share/fonts/dejavu/DejaVuSansMono.ttf']
    try:
        import matplotlib
        cands.append(os.path.join(matplotlib.get_data_path(), 'fonts/ttf/DejaVuSansMono.ttf'))
    except Exception:
        pass
    for c in cands:
        if os.path.exists(c):
            return ImageFont.truetype(c, size)
    return ImageFont.load_default()

def align_tabs(text):
    """Вирівнює табуляції (вивід Hive/Hadoop розділений \\t) у колонки; зміст рядків не змінюється, лише пробіли."""
    lines = text.rstrip('\n').split('\n')
    rows = [ln.split('\t') for ln in lines]
    tab = [r for r in rows if len(r) > 1]
    if len(tab) < 2:
        return text.expandtabs(8)
    ncol = max(len(r) for r in tab)
    w = [max((len(r[i]) for r in tab if i < len(r)), default=0) for i in range(ncol)]
    out = []
    for r in rows:
        out.append('  '.join(c.ljust(w[i]) for i, c in enumerate(r)).rstrip() if len(r) > 1 else r[0])
    return '\n'.join(out)


def render_terminal_png(cmd, text, path, max_lines=40, cols=112):
    """Рендерить РЕАЛЬНИЙ вивід команди як вікно термінала (темний фон, моноширинний шрифт, рядок '$ команда')."""
    from PIL import Image, ImageDraw
    wrap = lambda s: [s[i:i + cols] for i in range(0, max(len(s), 1), cols)]
    head = []
    for i, part in enumerate(wrap('$ ' + cmd.replace('\n', ' '))):
        head.append(part if i == 0 else '  ' + part)
    body = []
    for ln in align_tabs(text).rstrip('\n').split('\n'):
        body += wrap(ln)
    if len(body) > max_lines:
        h, t = max_lines - 12, 11
        body = body[:h] + [f'... [пропущено {len(body) - h - t} рядків / {len(body) - h - t} lines omitted] ...'] + body[-t:]
    f = _font(15)
    lh, pad, bar = 20, 14, 30
    w = 9 * cols + 2 * pad
    hgt = bar + pad + lh * (len(head) + len(body)) + pad
    img = Image.new('RGB', (w, hgt), (24, 26, 31))
    d = ImageDraw.Draw(img)
    d.rectangle([0, 0, w, bar], fill=(52, 56, 66))
    for i, c in enumerate([(255, 95, 86), (255, 189, 46), (39, 201, 63)]):
        d.ellipse([12 + i * 22, 9, 24 + i * 22, 21], fill=c)
    d.text((90, 7), 'root@localhost: ~', font=f, fill=(190, 195, 205))
    y = bar + pad
    for ln in head:
        d.text((pad, y), ln, font=f, fill=(120, 230, 130)); y += lh
    for ln in body:
        d.text((pad, y), ln, font=f, fill=(220, 222, 228)); y += lh
    img.save(path)

