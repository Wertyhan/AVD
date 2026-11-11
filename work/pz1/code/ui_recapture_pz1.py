# -*- coding: utf-8 -*-
"""ui_recapture_pz1.py - повторний захват веб-інтерфейсів Hadoop (ПЗ1) для друку:
viewport 1280x800, device_scale_factor=2, знімається лише змістовна область (clip по таблицях).
У YARN-таблиці застосунків приховано стовпці, що завжди N/A для завершених задач (CSS display:none); дані не змінюються.
Імена файлів збережено; 35 (дублікат 36) видалено."""
import sys
from pathlib import Path
ONLY_JH = 'jh' in sys.argv  # RM після перезапуску не зберігає застосунки: YARN-сторінки не перезнімаються
from playwright.sync_api import sync_playwright
SH = Path('/home/user/AVD/work/pz1/screenshots')
nn, rm, jh = 'http://localhost:9870', 'http://localhost:8088', 'http://localhost:19888'
APP = {'word': '1794418481996_0001', 'mr_yearly': '1794418481996_0002', 'mr_daily_ohlc': '1794418481996_0003'}
HIDE_APPS = [4, 6, 8, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 23]   # 0-based індекси N/A-стовпців таблиці #apps

def box(pg, first, last=None, pad=10, nth_last=False):
    js = """([a,b,nl])=>{const A=document.querySelector(a);const L=b?(nl?[...document.querySelectorAll(b)].pop():document.querySelector(b)):A;
      const r1=A.getBoundingClientRect(),r2=L.getBoundingClientRect();
      return {x:Math.min(r1.x,r2.x),y:r1.y+scrollY,r:Math.max(r1.right,r2.right),b:r2.bottom+scrollY}}"""
    o = pg.evaluate(js, [first, last, nth_last])
    return {'x': max(o['x'] - pad, 0), 'y': max(o['y'] - pad, 0), 'width': o['r'] - o['x'] + 2 * pad, 'height': o['b'] - o['y'] + 2 * pad}

def shot(b, url, name, first, last=None, wait=3500, css=None, nth_last=False, hide_apps=False):
    try:
        _shot(b, url, name, first, last, wait, css, nth_last, hide_apps)
    except Exception as e:
        print('SKIP', name, str(e)[:60])

def _shot(b, url, name, first, last=None, wait=3500, css=None, nth_last=False, hide_apps=False):
    pg = b.new_page(viewport={'width': 1280, 'height': 800}, device_scale_factor=2)
    pg.goto(url, wait_until='domcontentloaded'); pg.wait_for_selector(first, timeout=20000); pg.wait_for_timeout(wait)
    if hide_apps:
        pg.add_style_tag(content=','.join(f'#apps thead th:nth-child({i + 1}),#apps tbody td:nth-child({i + 1})' for i in HIDE_APPS) + '{display:none !important}')
        pg.wait_for_timeout(500)
    c = box(pg, first, last, nth_last=nth_last)
    pg.screenshot(path=str(SH / f'{name}.png'), full_page=True, clip=c)
    print(name, round(c['width']), round(c['height'])); pg.close()

with sync_playwright() as p:
    b = p.chromium.launch(executable_path='/opt/pw-browsers/chromium-1243/chrome-linux64/chrome', args=['--no-sandbox'])
    shot(b, f'{nn}/dfshealth.html#tab-overview', '31_ui_namenode_overview', '#tab-overview .page-header', '#tab-overview table:nth-of-type(2)')
    shot(b, f'{nn}/dfshealth.html#tab-datanode', '32_ui_namenode_datanodes', '#tab-datanode', '#tab-datanode table:last-of-type')
    shot(b, f'{nn}/explorer.html#/user/root/btc', '33_ui_namenode_browse_btc', '.page-header', '#table-explorer')
    shot(b, f'{nn}/explorer.html#/user/root/btc/raw', '34_ui_namenode_browse_raw', '.page-header', '#table-explorer')
    if ONLY_JH: pass
    else: shot(b, f'{rm}/cluster/apps', '36_ui_yarn_applications', 'table#metricsoverview', 'table#apps', hide_apps=True)
    shot(b, f'{rm}/cluster/nodes', '37_ui_yarn_nodes', 'table#metricsoverview', 'table#nodes')
    for k, (n1, n2, n3) in {'word': (38, 39, 40), 'mr_daily_ohlc': (41, 42, 43), 'mr_yearly': (44, 45, 46)}.items():
        i = APP[k]
        if not ONLY_JH: shot(b, f'{rm}/cluster/app/application_{i}', f'{n1}_ui_yarn_app_{k}', 'table#info', 'table#attempts')
        shot(b, f'{jh}/jobhistory/job/job_{i}', f'{n2}_ui_jobhistory_{k}', 'table#info', 'table#job', nth_last=True)
        shot(b, f'{jh}/jobhistory/jobcounters/job_{i}', f'{n3}_ui_jobhistory_counters_{k}', 'table#counters')
    shot(b, f'{jh}/jobhistory', '47_ui_jobhistory_list', 'table#jobs')
    b.close()
