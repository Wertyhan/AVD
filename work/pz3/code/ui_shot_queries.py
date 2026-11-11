# -*- coding: utf-8 -*-
"""Знімки сторінок окремих streaming-запитів Spark UI (Structured Streaming -> Run ID). Використання: ui_shot_queries.py OUT_DIR BASE_URL"""
import sys
from playwright.sync_api import sync_playwright
out, base = sys.argv[1], sys.argv[2].rstrip("/")
with sync_playwright() as p:
    b = p.chromium.launch(executable_path="/opt/pw-browsers/chromium-1243/chrome-linux64/chrome", args=["--no-sandbox"])
    pg = b.new_page(viewport={"width": 1500, "height": 950})
    pg.goto(base + "/StreamingQuery/", wait_until="load"); pg.wait_for_timeout(1500)
    rows = pg.eval_on_selector_all("table tbody tr", "e=>e.map(r=>[...r.querySelectorAll('td')].map(t=>t.textContent.trim()).slice(0,2).concat([(r.querySelector('a')||{}).href]))")
    print(rows, flush=True)
    for name, rid, href in rows:
        if name in ("anomaly", "sink_win1h"):
            pg.goto(href, wait_until="load"); pg.wait_for_timeout(4000)
            pg.screenshot(path=f"{out}/ui_query_{name}.png", full_page=True); print("saved", name, flush=True)
    b.close()
