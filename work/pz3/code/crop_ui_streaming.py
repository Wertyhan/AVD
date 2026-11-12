"""Фрагмент таблиці Spark UI (ПЗ3, рис. 3): вирізає з оригінального знімка колонки ID, Run ID і Start Time
і склеює решту (Name, Status, Duration, Avg Input, Avg Process, Latest Batch) без зміни значень.
Використання: crop_ui_streaming.py SRC_PNG OUT_PNG"""
import sys
from PIL import Image

src, out = sys.argv[1], sys.argv[2]
im = Image.open(src).convert("RGB")
w, h = im.size
x1, x2 = 358, 1637  # межі колонок ID..Start Time (піксели оригіналу 2494 px)
a, b = im.crop((0, 0, x1, h)), im.crop((x2, 0, w, h))
res = Image.new("RGB", (a.width + b.width, h), "white")
res.paste(a, (0, 0)); res.paste(b, (a.width, 0))
res.save(out)
print(out, res.size)
