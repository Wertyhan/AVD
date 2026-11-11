# -*- coding: utf-8 -*-
"""Рендер реального виводу програми (screenshots/txt/*.txt) у PNG: темний фон, моноширинний шрифт."""
import glob, os
from PIL import Image, ImageDraw, ImageFont
BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
FONT = "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf"
BOLD = "/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf"
SZ, PAD, MAXLINES, MAXCOLS = 16, 18, 48, 150
f, fb = ImageFont.truetype(FONT, SZ), ImageFont.truetype(BOLD, SZ)
DIM = (125, 130, 140)
SCRIPT = "python code/pz5_spark.py"   # усі фрагменти надруковані цим скриптом
DESC = {"01_env": "версії ПЗ та ресурси машини", "02_schema": "схема та перші рядки вхідного parquet (ПЗ2)",
        "03_period": "частка плоских годин і rv за роками (вибір періоду моделювання)", "04_split": "хронологічний поділ 80/20 і частка класу 1",
        "05_acf": "автокореляція дохідностей і модуля дохідностей", "06_pipeline": "склад ML Pipeline для LR, RF і GBT",
        "07_tuning": "підбір гіперпараметрів (часова крос-валідація, PR-AUC)", "08_metrics": "метрики на тесті: базові лінії та моделі",
        "09_importance": "важливість ознак (RF, GBT, коефіцієнти LR)", "10_random_vs_chrono": "випадковий і хронологічний поділ",
        "11_direction": "контрольний експеримент: прогноз напряму", "12_model_saved": "збереження та завантаження найкращого pipeline"}
cw = f.getlength("M"); lh = SZ + 5
for p in sorted(glob.glob(f"{BASE}/screenshots/txt/*.txt")):
    lines = open(p, encoding="utf-8").read().rstrip("\n").split("\n")
    key = os.path.basename(p)[:-4]
    lines = [l.expandtabs(4) for l in lines[1:]]   # перший рядок txt – службовий заголовок; тіло – реальний захоплений вивід
    lines = [f"$ {SCRIPT}", f"# фрагмент виводу: {DESC[key]}"] + lines
    note = None
    if len(lines) > MAXLINES:
        note = f"... (виведено перші {MAXLINES - 1} рядків із {len(lines)}; повний вивід у results/)"
        lines = lines[:MAXLINES - 1]
    lines = [(l[:MAXCOLS - 1] + "…") if len(l) > MAXCOLS else l for l in lines]
    if note: lines.append(note)
    w = int(max(len(l) for l in lines) * cw + 2 * PAD) + 10
    h = lh * (len(lines)) + 2 * PAD + 34
    im = Image.new("RGB", (max(w, 700), h), (22, 24, 29)); d = ImageDraw.Draw(im)
    d.rectangle([0, 0, im.width, 30], fill=(48, 52, 62))
    for i, c in enumerate([(255, 95, 86), (255, 189, 46), (39, 201, 63)]):
        d.ellipse([12 + i * 22, 9, 24 + i * 22, 21], fill=c)
    d.text((100, 6), "bash – pz5", font=f, fill=(190, 195, 205))
    y = 30 + PAD
    for i, l in enumerate(lines):
        if i == 1:
            d.text((PAD, y), l, font=f, fill=DIM)
        elif i == 0:
            d.text((PAD, y), "$", font=fb, fill=(120, 220, 120)); d.text((PAD + cw * 2, y), l[2:], font=fb, fill=(240, 240, 240))
        else:
            d.text((PAD, y), l, font=f, fill=(205, 210, 220))
        y += lh
    out = f"{BASE}/screenshots/term_{os.path.basename(p)[:-4]}.png"
    im.save(out); print(out)
