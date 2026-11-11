#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ui_shots_pz1.py - скріншоти веб-інтерфейсів (NameNode :9870, YARN :8088, JobHistory :19888) через playwright/Chromium
та термінальні скріншоти команд yarn/mapred/hdfs, що характеризують виконані задачі."""
import os, re, subprocess, sys, json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from termshot import render_terminal_png
from playwright.sync_api import sync_playwright
W = Path('/home/user/AVD/work'); SH = W / 'pz1/screenshots'; RES = W / 'pz1/results'
HADOOP = W / 'tools/hadoop-3.3.6'; JDK8 = W / 'mamba/envs/jdk8'
os.environ.pop('JAVA_TOOL_OPTIONS', None)
os.environ.update(JAVA_HOME=str(JDK8), HADOOP_HOME=str(HADOOP), PATH=f'{HADOOP}/bin:{JDK8}/bin:' + os.environ['PATH'])
n = [len(list(SH.glob('*.png')))]
def term(cmd, name, **kw):
    out = subprocess.run(cmd, shell=True, executable='/bin/bash', capture_output=True, text=True).stdout
    n[0] += 1; p = SH / f'{n[0]:02d}_{name}.png'
    render_terminal_png(cmd, out, p, **kw); print('term', p.name); return out
def path(name):
    n[0] += 1; return str(SH / f'{n[0]:02d}_{name}.png')

apps = term('yarn application -list -appStates ALL 2>/dev/null', 'yarn_application_list', max_lines=45, cols=150)
term('jps', 'jps_final')
term('hdfs dfs -ls -R /user/root/btc/out | grep -v "/_" | head -30', 'hdfs_ls_outputs')
term('hdfs dfs -ls /user/hive/warehouse/btc.db', 'hdfs_ls_warehouse')
ids = {}   # назва -> application id (перший успішний запуск)
for line in apps.splitlines():
    m = re.match(r'(application_\d+_\d+)\s+(\S+)\s+', line)
    if m and m.group(2) not in ids and ('mr_' in m.group(2) or 'word' in m.group(2).lower()):
        ids[m.group(2)] = m.group(1)
print(ids)
nn, rm, jh = 'http://localhost:9870', 'http://localhost:8088', 'http://localhost:19888'
shots = [
    (f'{nn}/dfshealth.html#tab-overview', 'ui_namenode_overview', None, True),
    (f'{nn}/dfshealth.html#tab-datanode', 'ui_namenode_datanodes', None, False),
    (f'{nn}/explorer.html#/user/root/btc', 'ui_namenode_browse_btc', '#table-explorer', False),
    (f'{nn}/explorer.html#/user/root/btc/raw', 'ui_namenode_browse_raw', '#table-explorer', False),
    (f'{rm}/cluster', 'ui_yarn_cluster', None, True),
    (f'{rm}/cluster/apps', 'ui_yarn_applications', '#apps', True),
    (f'{rm}/cluster/nodes', 'ui_yarn_nodes', None, False),
]
for name, aid in ids.items():
    shots.append((f'{rm}/cluster/app/{aid}', f'ui_yarn_app_{name}', None, True))
    jid = aid.replace('application_', 'job_')
    shots.append((f'{jh}/jobhistory/job/{jid}', f'ui_jobhistory_{name}', None, True))
    shots.append((f'{jh}/jobhistory/jobcounters/{jid}', f'ui_jobhistory_counters_{name}', None, True))
shots.append((f'{jh}/jobhistory', 'ui_jobhistory_list', None, True))
log = []
with sync_playwright() as p:
    b = p.chromium.launch(executable_path='/opt/pw-browsers/chromium-1243/chrome-linux64/chrome', args=['--no-sandbox'])
    pg = b.new_page(viewport={'width': 1400, 'height': 900})
    for url, name, sel, full in shots:
        try:
            pg.goto(url, timeout=30000, wait_until='domcontentloaded')
            if sel:
                try: pg.wait_for_selector(sel, timeout=15000)
                except Exception: pass
            pg.wait_for_timeout(4000)
            f = path(name); pg.screenshot(path=f, full_page=full, timeout=30000)
            log.append((name, url, f, 'OK')); print('OK', f)
        except Exception as e:
            log.append((name, url, '', 'FAIL ' + str(e)[:100])); print('FAIL', name, e)
    b.close()
import pandas as pd
pd.DataFrame(log, columns=['name', 'url', 'file', 'status']).to_csv(RES / 'ui_screenshots.csv', index=False)
