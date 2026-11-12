# -*- coding: utf-8 -*-
"""Підготовка двох знімків Spark UI для друку (кроп і розкладка; вміст пікселів не змінюється):
 - spark_ui_storage_cols.png: фрагмент таблиці Storage без стовпців ID і RDD Name (довгий план);
 - spark_ui_sql_detail_3col.png: вузький довгий граф SQL розкладено на три колонки.
Вхід: screenshots/spark_ui_storage.png і screenshots/spark_ui_sql_detail.png (їх створює pz2_spark.py)."""
import os, sys
from PIL import Image
BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
OUT = sys.argv[1] if len(sys.argv) > 1 else f"{BASE}/screenshots"
S = f"{BASE}/screenshots"

st = Image.open(f"{S}/spark_ui_storage.png").convert("RGB")
st.crop((1168, 0, st.width, 430)).save(f"{OUT}/spark_ui_storage_cols.png")

g = Image.open(f"{S}/spark_ui_sql_detail.png").convert("RGB")
H, GAP, N = 812, 40, 3
canvas = Image.new("RGB", (N * g.width + (N - 1) * GAP, H), (255, 255, 255))
starts = [0, 805, 1617]                 # початок кожної колонки у вихідному графі (рядки), колонки трохи перекриваються
for k, y in enumerate(starts):
    canvas.paste(g.crop((0, y, g.width, min(y + H, g.height))), (k * (g.width + GAP), 0))
canvas.save(f"{OUT}/spark_ui_sql_detail_3col.png")
