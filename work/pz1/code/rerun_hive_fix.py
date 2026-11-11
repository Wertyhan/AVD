# -*- coding: utf-8 -*-
"""rerun_hive_fix.py - повторний запуск усіх Hive-кроків ПЗ1 після виправлення TBLPROPERTIES
(skip.header.line.count) + крок LOAD DATA (M4). Результати -> results/*.csv, журнали -> logs/08_hive_*.log (дописуються),
скріншоти рендеряться окремо (rerender_pz1.py 19-25, render_hive_extra.py)."""
import io, os, subprocess, sys, time
from pathlib import Path
import numpy as np, pandas as pd
W = Path('/home/user/AVD/work'); P = W / 'pz1'; RES = P / 'results'; LOG = P / 'logs'; CODE = P / 'code'
HM = W / 'hadoop_runtime/hive_meta'
os.environ.pop('JAVA_TOOL_OPTIONS', None)
os.environ.update(JAVA_HOME=str(W / 'mamba/envs/jdk8'), HADOOP_HOME=str(W / 'tools/hadoop-3.3.6'))
os.environ['PATH'] = f"{W}/tools/hadoop-3.3.6/bin:{W}/tools/apache-hive-3.1.3-bin/bin:{W}/mamba/envs/jdk8/bin:" + os.environ['PATH']

def run(cmd, tag=None):
    p = subprocess.run(cmd, shell=True, executable='/bin/bash', capture_output=True, text=True, cwd=str(HM))
    if tag:
        with open(LOG / f'08_hive_{tag}.log', 'a') as f: f.write(f'\n$ {cmd}\n{p.stdout}')
        with open(LOG / f'08_hive_{tag}.err.log', 'a') as f: f.write(p.stderr)
    assert p.returncode == 0, (cmd, p.stderr[-2000:])
    return p.stdout
def hive(f, tag): return run(f'hive -S -f {CODE}/{f}', tag)
def df(o): return pd.read_csv(io.StringIO(o), sep='\t', float_precision='round_trip')

hive('hive_01_create_table.hql', 'create_table')
run(f"hive -S -f {CODE}/hive_01b_describe.hql | sed '/^# Storage Information/,$d'", 'describe')
o = hive('hive_02_select.hql', 'select'); print(o)
lines = o.strip().split('\n'); raw_rows = int(lines[-3].strip()); cnt = int(lines[-1].strip())
assert 'NULL' not in lines[1], 'header NULL row still present'
N = 7816537
pd.DataFrame([dict(hive_count_view=cnt, hive_count_table=raw_rows, csv_rows=N, equal_view=cnt == N, equal_table=raw_rows == N)]).to_csv(RES / 'hive_count_check.csv', index=False)
Y = df(hive('hive_03a_yearly.hql', 'yearly')); Y.to_csv(RES / 'hive_yearly_stats.csv', index=False)
M = df(hive('hive_03b_monthly.hql', 'monthly')); M.to_csv(RES / 'hive_monthly_stats.csv', index=False)
hive('hive_04_daily_struct.hql', 'daily_struct')
hive('hive_06_daily_count.hql', 'daily_count')
V = df(hive('hive_05a_top_volume.hql', 'top_volume')); V.to_csv(RES / 'hive_top10_volume_days.csv', index=False)
o = hive('hive_05b_top_range.hql', 'top_range'); R = df(o[o.index('trade_date\thigh'):]); R.to_csv(RES / 'hive_top10_range_days.csv', index=False)
txt = run('hdfs dfs -cat /user/hive/warehouse/btc.db/daily_ohlc/*')
D = pd.read_csv(io.StringIO(txt), sep='\x01', header=None, float_precision='round_trip', names=['day', 'open', 'high', 'low', 'close', 'volume', 'candles']).sort_values('day').reset_index(drop=True)
pd.DataFrame([dict(hive_daily_rows=len(D))]).to_csv(RES / 'hive_daily_rowcount.csv', index=False)
D.to_csv('/tmp/hive_daily_new.csv', index=False)
# звірка Hive vs MR (лише Hive-MR)
MRY = pd.read_csv(RES / 'mr_yearly_stats.csv'); MRD = pd.read_csv(RES / 'mr_daily_ohlc_full.csv')
m = Y.merge(MRY, left_on='yr', right_on='year')
ok = len(m) == len(MRY) == len(Y) and (m.candles_x == m.candles_y).all() and np.allclose(m.total_volume, m.volume, rtol=1e-9)
pd.DataFrame([dict(check='hive_yearly_vs_mapreduce', rows_hive=len(Y), rows_mr=len(MRY), match=bool(ok))]).to_csv(RES / 'crosscheck_hive_vs_mr_yearly.csv', index=False)
m = D.merge(MRD, on='day', suffixes=('_h', '_m'))
ok2 = len(m) == len(D) == len(MRD) and (m.candles_h == m.candles_m).all() and np.allclose(m.volume_h, m.volume_m, rtol=1e-9)
for k in ('open', 'high', 'low', 'close'): ok2 = ok2 and np.allclose(m[k + '_h'], m[k + '_m'], rtol=1e-12)
pd.DataFrame([dict(check='hive_daily_vs_mapreduce_daily', rows_hive=len(D), rows_mr=len(MRD), match=bool(ok2))]).to_csv(RES / 'crosscheck_hive_vs_mr_daily.csv', index=False)
print('yearly match', ok, 'daily match', ok2, 'counts', raw_rows, cnt, 'daily rows', len(D))
