# -*- coding: utf-8 -*-
"""ui_panels_pz1.py - повторний захват веб-UI Hadoop для багатопанельних рисунків (ПЗ1): viewport 720x800, scale 2
(текст >= 8 pt при ширині рисунка 16 см), clip по таблицях. Приховуються лише зайві рядки/стовпці (CSS), значення не змінюються.
Збирає figures/ui_hdfs_panel.png і figures/ui_yarn_panel.png; значення з DOM/REST -> results/ui_values.csv."""
import json, urllib.request, csv
from pathlib import Path
from playwright.sync_api import sync_playwright
from PIL import Image, ImageDraw, ImageFont
P = Path('/home/user/AVD/work/pz1'); SRC = P / 'screenshots/ui_panel_src'; FIG = P / 'figures'
nn, rm, jh = 'http://localhost:9870', 'http://localhost:8088', 'http://localhost:19888'
JOB = 'job_1794418481996_0003'   # mr_daily_ohlc
VALS = []

def get(u): return json.load(urllib.request.urlopen(u))

def snap(b, url, name, sel, css='', js='', wait=3500, nth=0):
    pg = b.new_page(viewport={'width': 720, 'height': 800}, device_scale_factor=2)
    pg.goto(url, wait_until='domcontentloaded'); pg.wait_for_selector(sel, timeout=20000); pg.wait_for_timeout(wait)
    if css: pg.add_style_tag(content=css)
    if js: pg.evaluate(js)
    pg.wait_for_timeout(500)
    el = pg.query_selector_all(sel)[nth]
    el.screenshot(path=str(SRC / f'{name}.png')); txt = el.inner_text(); pg.close(); return txt

HIDE_SUM = """
const keep=['Configured Capacity','DFS Used','DFS Remaining','Live Nodes','Dead Nodes','Number of Under-Replicated Blocks'];
document.querySelectorAll('#tab-overview table')[1].querySelectorAll('tr').forEach(r=>{const t=r.cells[0].innerText.trim();
 if(!keep.some(k=>t.startsWith(k))) r.style.display='none';});"""
HIDE_YARN = """
const keep=['ID','Name','Application Type','State','FinalStatus','Elapsed Time'];
const th=[...document.querySelectorAll('#apps thead th')]; const idx=th.map((t,i)=>keep.includes(t.innerText.trim())?-1:i).filter(i=>i>=0);
const css=idx.map(i=>`#apps thead th:nth-child(${i+1}),#apps tbody td:nth-child(${i+1}),#apps tfoot th:nth-child(${i+1})`).join(',');
const s=document.createElement('style'); s.textContent=css+'{display:none !important} #apps td{max-width:260px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap} #apps,#apps_wrapper,#apps_wrapper table{width:715px !important;table-layout:fixed} #apps td,#apps th{font-size:11px !important;padding:3px 3px !important} #apps td:nth-child(1){font-size:10px !important}'; document.head.appendChild(s);
const W={'ID':175,'Name':235,'Application Type':95,'State':75,'FinalStatus':105}; th.forEach(t=>{const k=t.innerText.trim(); if(W[k]) t.style.width=W[k]+'px';});"""
CNT_CSS = '#counters{width:670px !important} #counters>tbody>tr>td:first-child{width:90px !important} #counters table,.dt-counters,.dataTables_wrapper{width:100% !important;table-layout:fixed} .dt-counters th:nth-child(1){width:46% !important} #counters{table-layout:fixed} #counters td,#counters th{font-size:13px}'
KEEP_CNT = ['Launched map tasks','Data-local map tasks','Map input records','Map output records','Reduce input groups','Shuffled Maps','Spilled Records']
HIDE_CNT = """(keep)=>{document.querySelectorAll('#counters table.dt-counters tbody tr').forEach(r=>{const t=r.cells[0]?r.cells[0].innerText.trim():'';
 if(!keep.includes(t)) r.style.display='none';});
 document.querySelectorAll('#counters > tbody > tr').forEach(r=>{const inner=r.querySelectorAll('table tbody tr');const vis=[...inner].some(x=>x.style.display!=='none'); if(!vis) r.style.display='none';});
 document.querySelectorAll('#counters > tbody > tr').forEach(r=>{const g=r.cells[0].innerText.trim(); if(['Shuffle Errors','File Input Format Counters','File Output Format Counters'].includes(g)) r.style.display='none';});}"""

with sync_playwright() as p:
    b = p.chromium.launch(executable_path='/opt/pw-browsers/chromium-1243/chrome-linux64/chrome', args=['--no-sandbox'])
    t1 = snap(b, f'{nn}/dfshealth.html#tab-overview', 'nn_summary', '#tab-overview table:nth-of-type(2)', js=HIDE_SUM)
    t2 = snap(b, f'{nn}/dfshealth.html#tab-datanode', 'nn_datanodes', '#tab-datanode table:last-of-type')
    t3 = snap(b, f'{nn}/explorer.html#/user/root/btc/raw', 'nn_browse_raw', '#table-explorer')
    t4 = snap(b, f'{rm}/cluster/apps', 'yarn_apps', '#apps', js=HIDE_YARN, css='#apps_wrapper{width:715px !important;overflow:hidden}')
    pg_js = "(" + HIDE_CNT + ")(" + json.dumps(KEEP_CNT) + ")"
    t5 = snap(b, f'{jh}/jobhistory/jobcounters/{JOB}', 'jh_counters', 'table#counters', js=pg_js, css=CNT_CSS)
    b.close()
print(t1); print(t3); print(t5)
# значення -> csv (DOM + REST)
for line in t1.split('\n'):
    if '\t' in line: k, v = line.split('\t', 1); VALS.append(('NameNode UI Summary', k.strip(), v.strip()))
for line in t3.split('\n'):
    if 'btcusd' in line: VALS.append(('NameNode UI Browse /user/root/btc/raw', 'row', ' | '.join(x.strip() for x in line.split('\t') if x.strip())))
c = get(f'{jh}/ws/v1/history/mapreduce/jobs/{JOB}/counters')['jobCounters']['counterGroup']
for g in c:
    for ct in g['counter']:
        if ct['name'] in ('SHUFFLED_MAPS', 'TOTAL_LAUNCHED_MAPS', 'DATA_LOCAL_MAPS', 'MAP_INPUT_RECORDS', 'REDUCE_INPUT_GROUPS'):
            VALS.append((f'JobHistory REST {JOB} (mr_daily_ohlc)', ct['name'], ct['totalCounterValue']))
VALS.append(('JobHistory UI counters ' + JOB, 'Shuffled Maps (UI text)', [l for l in t5.split('\n') if 'Shuffled Maps' in l]))
fi = get(f'{nn}/jmx?qry=Hadoop:service=NameNode,name=FSNamesystem'); 
with open(P / 'results/ui_values.csv', 'w', newline='') as f:
    w = csv.writer(f); w.writerow(['source', 'name', 'value']); w.writerows(VALS)

def panel(items, out, width=1440):
    F = ImageFont.truetype('/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf', 30)
    ims = []
    for label, fn in items:
        im = Image.open(SRC / f'{fn}.png').convert('RGB')
        r = width / im.width
        if r < 1: im = im.resize((width, int(im.height * r)), Image.LANCZOS)
        ims.append((label, im))
    H = sum(im.height + 50 for _, im in ims) + 10
    W = max(im.width for _, im in ims)
    cv = Image.new('RGB', (W, H), 'white'); d = ImageDraw.Draw(cv); y = 5
    for label, im in ims:
        d.text((10, y + 5), label, font=F, fill=(20, 20, 20)); y += 50
        cv.paste(im, (0, y)); y += im.height
    cv.save(FIG / out); print(out, cv.size)
panel([('(a) NameNode: Summary', 'nn_summary'), ('(b) NameNode: Datanodes', 'nn_datanodes'), ('(c) NameNode: Browse Directory /user/root/btc/raw', 'nn_browse_raw')], 'ui_hdfs_panel.png')
panel([('(a) YARN: Applications (Hive-запити)', 'yarn_apps'), ('(b) JobHistory: лічильники mr_daily_ohlc', 'jh_counters')], 'ui_yarn_panel.png')
