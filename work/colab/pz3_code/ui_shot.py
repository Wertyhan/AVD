# -*- coding: utf-8 -*-
"""Знімок Spark UI через headless Chromium (playwright). Використання: ui_shot.py URL OUT.png"""
import sys
from playwright.sync_api import sync_playwright
url, out = sys.argv[1], sys.argv[2]
with sync_playwright() as p:
    b = p.chromium.launch(args=["--no-sandbox"])
    pg = b.new_page(viewport={"width": 1400, "height": 900})
    pg.goto(url, wait_until="networkidle", timeout=45000)
    pg.screenshot(path=out, full_page=True)
    b.close()
print("saved", out)
