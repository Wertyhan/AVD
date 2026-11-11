# -*- coding: utf-8 -*-
"""Знімки Spark UI під час роботи ПОВНОГО конвеєра (7 запитів): Structured Streaming (таблиця активних запитів),
сторінка віконного запиту, сторінка запиту anomaly, Jobs, Executors. Viewport 1280x800, масштаб 2, кадр обмежено вмістом.
Використання: ui_shot_run.py OUT_DIR http://localhost:PORT [window_query_name]"""
import sys, json
from playwright.sync_api import sync_playwright
out, base = sys.argv[1], sys.argv[2].rstrip("/")
winq = sys.argv[3] if len(sys.argv) > 3 else "sink_win1h"
EXE = "/opt/pw-browsers/chromium-1243/chrome-linux64/chrome"


def clip_of(pg, js_selector_fn, pad=8, maxh=2600):
    """Обмежувальний прямокутник вмісту (в CSS-пікселях сторінки) для кадрування."""
    box = pg.evaluate(js_selector_fn)
    x, y, w, h = box
    return {"x": max(x - pad, 0), "y": max(y - pad, 0), "width": min(w + 2 * pad, 1280), "height": min(h + 2 * pad, maxh)}


with sync_playwright() as p:
    b = p.chromium.launch(executable_path=EXE, args=["--no-sandbox"])
    pg = b.new_page(viewport={"width": 1280, "height": 800}, device_scale_factor=2)
    info = {}

    def snap(name, clip):
        pg.screenshot(path=f"{out}/{name}.png", full_page=True, clip=clip)
        info[name] = clip
        print("saved", name, clip, flush=True)

    # 1. Structured Streaming: заголовок + таблиця активних запитів
    pg.goto(base + "/StreamingQuery/", wait_until="load"); pg.wait_for_selector("table tbody tr", timeout=30000); pg.wait_for_timeout(1500)
    n_act = pg.evaluate("()=>document.querySelectorAll('table')[0].querySelectorAll('tbody tr').length")
    print("активних запитів у таблиці:", n_act, flush=True)
    snap("ui_streaming_tab", clip_of(pg, """()=>{
        const t=document.querySelectorAll('table')[0]; let h=t.previousElementSibling; while(h && !/Active Streaming Queries/.test(h.textContent)) h=h.previousElementSibling;
        const r1=(h||t).getBoundingClientRect(), r2=t.getBoundingClientRect();
        return [Math.min(r1.x,r2.x)+window.scrollX, r1.y+window.scrollY, Math.max(r1.width,r2.width), r2.bottom-r1.top]}"""))
    links = pg.eval_on_selector_all("table tbody tr", "e=>e.map(r=>[r.querySelector('td').textContent.trim(),(r.querySelector('a[href*=\\'StreamingQuery/statistics\\']')||{}).href])")
    print(links, flush=True)
    info["queries"] = [l[0] for l in links]
    # 2. сторінки запитів (статистика: input rate, process rate, batch duration, state)
    for nm, href in links:
        if nm in (winq, "anomaly"):
            pg.goto(href, wait_until="load"); pg.wait_for_timeout(5000)
            snap("ui_query_" + nm, clip_of(pg, """()=>{
                const e=document.querySelector('body > .container-fluid')||document.body; const r=e.getBoundingClientRect();
                return [r.x+window.scrollX, r.y+window.scrollY, r.width, r.height]}""", maxh=2600))
    # 3. Jobs (активні/завершені завдання)
    pg.goto(base + "/jobs/", wait_until="load"); pg.wait_for_timeout(2000)
    snap("ui_jobs", clip_of(pg, """()=>{
        const e=document.querySelector('body > .container-fluid')||document.body; const r=e.getBoundingClientRect();
        return [r.x+window.scrollX, r.y+window.scrollY, r.width, r.height]}""", maxh=1500))
    # 4. Executors: таблиця Summary + Executors; якщо колонки не вміщуються, розширюємо viewport
    pg.goto(base + "/executors/", wait_until="load"); pg.wait_for_selector("#active-executors-table tbody tr", timeout=30000); pg.wait_for_timeout(1500)
    sw = pg.evaluate("()=>Math.max(document.documentElement.scrollWidth, document.querySelector('#active-executors-table').scrollWidth)")
    print("scrollWidth executors:", sw, flush=True)
    pg.set_viewport_size({"width": max(1280, sw + 80), "height": 800}); pg.wait_for_timeout(1000)
    sw2 = pg.evaluate("()=>Math.max(document.documentElement.scrollWidth, document.querySelector('#active-executors-table').scrollWidth)")
    wv = pg.evaluate("()=>window.innerWidth")
    print("viewport", wv, "scrollWidth after", sw2, flush=True)
    c = clip_of(pg, """()=>{
        const e=document.querySelector('body > .container-fluid')||document.body; const r=e.getBoundingClientRect();
        return [r.x+window.scrollX, r.y+window.scrollY, r.width, r.height]}""", maxh=1100)
    c["width"] = min(wv, max(c["width"], sw2) + 16)
    c["height"] = min(c["height"], 560)
    snap("ui_executors", c)
    info["executors_viewport_width"] = wv
    json.dump(info, open(f"{out}/ui_shot_run.json", "w"), indent=1)
    b.close()
