# -*- coding: utf-8 -*-
"""Знімки Spark UI (:4040) через headless Chromium: Jobs, Structured Streaming, сторінка запиту, Executors/SQL.
Використання: ui_shot.py OUT_DIR  (запускати, поки працює Spark-застосунок)"""
import sys, re
from playwright.sync_api import sync_playwright
out = sys.argv[1]; base = sys.argv[2].rstrip("/")   # напр. http://localhost:4042
with sync_playwright() as p:
    b = p.chromium.launch(executable_path="/opt/pw-browsers/chromium-1243/chrome-linux64/chrome", args=["--no-sandbox"])
    pg = b.new_page(viewport={"width": 1500, "height": 950})
    def shot(path, name):
        pg.goto(base + path, wait_until="load", timeout=45000); pg.wait_for_timeout(1500)
        pg.screenshot(path=f"{out}/{name}.png", full_page=True); print("saved", name, flush=True)
    shot("/jobs/", "ui_jobs")
    shot("/StreamingQuery/", "ui_streaming_tab")
    links = pg.eval_on_selector_all("a[href*='StreamingQuery/statistics']", "e=>e.map(x=>[x.textContent.trim(),x.href])")
    print(links, flush=True)
    for nm, href in links:
        if nm in ("anomaly", "sink_win1h", "console_1h", "mem_win15m", "sink_win15m"):
            pg.goto(href, wait_until="load", timeout=45000); pg.wait_for_timeout(3000)
            pg.screenshot(path=f"{out}/ui_query_{nm}.png", full_page=True); print("saved query", nm, flush=True)
    shot("/stages/", "ui_stages")
    shot("/executors/", "ui_executors")
    b.close()
