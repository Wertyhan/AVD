# -*- coding: utf-8 -*-
"""Знімки Spark UI запиту anomaly (правило v2) під час його роботи: Executors, Structured Streaming, сторінка запиту.
Використання: ui_shot_v2.py OUT_DIR http://host:PORT   (viewport 1280x800, scale 2, знімається лише вміст сторінки)"""
import sys
from playwright.sync_api import sync_playwright
out, base = sys.argv[1], sys.argv[2].rstrip("/")
with sync_playwright() as p:
    b = p.chromium.launch(executable_path="/opt/pw-browsers/chromium-1243/chrome-linux64/chrome", args=["--no-sandbox"])
    pg = b.new_page(viewport={"width": 1280, "height": 800}, device_scale_factor=2)
    def shot(name, maxh=1400):
        box = pg.evaluate("()=>{const e=document.querySelector('body > .container-fluid')||document.body;const r=e.getBoundingClientRect();return [r.x,r.y,r.width,r.height]}")
        h = min(box[3] + box[1] + 10, maxh)
        pg.screenshot(path=f"{out}/{name}.png", full_page=True, clip={"x": 0, "y": 0, "width": 1280, "height": h}); print("saved", name, h, flush=True)
    pg.goto(base + "/executors/", wait_until="load"); pg.wait_for_selector("#active-executors-table tbody tr", timeout=30000); pg.wait_for_timeout(1500)
    shot("ui_executors")
    pg.goto(base + "/StreamingQuery/", wait_until="load"); pg.wait_for_selector("table tbody tr", timeout=30000); pg.wait_for_timeout(1500)
    shot("ui_streaming_tab")
    href = pg.eval_on_selector_all("a[href*='StreamingQuery/statistics']", "e=>e.map(x=>[x.textContent.trim(),x.href])")
    print(href, flush=True)
    link = [h for n, h in href if n == "anomaly"][0]
    pg.goto(link, wait_until="load"); pg.wait_for_timeout(4000)
    shot("ui_query_anomaly", 2400)
    b.close()
