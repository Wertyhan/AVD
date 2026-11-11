#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
bench_pz1.py - порівняння часу виконання ТИХ САМИХ агрегацій на ОДНАКОВОМУ CSV (7 816 537 рядків):
  (а) кількість свічок і сумарний обсяг за роками;  (б) добові OHLC.
Інструменти: Hadoop Streaming (YARN), Hive (рушій MR), Pandas, PySpark (local[*]).
Кожна пара інструмент-задача виконується 3 рази; у підсумку наведено медіану, мінімум і максимум.
Вимірюється повний «настінний» час від запуску команди до готового результату (включно зі стартом JVM/контейнерів і читанням CSV).
Результати: results/timings.csv (усі запуски), results/timing_summary.csv (медіани),
results/crosscheck_spark_vs_pandas.csv, results/spark_*.csv.
"""
import os, re, io, sys, time, shlex, subprocess, statistics, json
from pathlib import Path
import numpy as np, pandas as pd

W = Path('/home/user/AVD/work'); OUT = W / 'pz1'; RES = OUT / 'results'; CODE = OUT / 'code'
HADOOP_HOME = W / 'tools/hadoop-3.3.6'; HIVE_HOME = W / 'tools/apache-hive-3.1.3-bin'
JDK8 = W / 'mamba/envs/jdk8'; JDK21 = '/usr/lib/jvm/java-21-openjdk-amd64'
RT = W / 'hadoop_runtime'; HIVE_META = RT / 'hive_meta'
CSV = W / 'data/raw/btcusd_1-min_data.csv'
HDFS_RAW = '/user/root/btc/raw'
RUNS = 3
os.environ.pop('JAVA_TOOL_OPTIONS', None)
env = dict(os.environ, JAVA_HOME=str(JDK8), HADOOP_HOME=str(HADOOP_HOME), HIVE_HOME=str(HIVE_HOME),
           HADOOP_CONF_DIR=str(HADOOP_HOME / 'etc/hadoop'), HADOOP_MAPRED_HOME=str(HADOOP_HOME),
           PATH=f'{HADOOP_HOME}/bin:{HIVE_HOME}/bin:{JDK8}/bin:' + os.environ['PATH'])
STREAM_JAR = next((HADOOP_HOME / 'share/hadoop/tools/lib').glob('hadoop-streaming-*.jar'))

def sh(cmd, cwd=None):
    t0 = time.perf_counter()
    p = subprocess.run(cmd, shell=True, executable='/bin/bash', env=env, cwd=cwd, capture_output=True, text=True)
    return p.returncode, p.stdout, p.stderr, time.perf_counter() - t0

rows = []
def rec(tool, task, run, secs, note=''):
    rows.append(dict(tool=tool, task=task, run=run, seconds=round(secs, 2), note=note))
    print(f'{tool:22s} {task:16s} run{run}: {secs:8.2f} s  {note}', flush=True)
    pd.DataFrame(rows).to_csv(RES / 'timings_partial.csv', index=False)

# ---- умови вимірювання: навантаження машини ----
la = os.getloadavg()
ps = subprocess.run("ps -eo pid,pcpu,rss,args --sort=-pcpu | head -6 | cut -c1-150", shell=True, capture_output=True, text=True).stdout
print('loadavg перед стартом:', la); print(ps)
cond = dict(loadavg_1m=la[0], loadavg_5m=la[1], loadavg_15m=la[2], nproc=os.cpu_count(),
            mem_available_gb=round(int(re.search(r'MemAvailable:\s+(\d+)', open('/proc/meminfo').read()).group(1)) / 1048576, 2))
pd.DataFrame([cond]).to_csv(RES / 'timing_conditions.csv', index=False)

# ---- Hadoop Streaming ----
def streaming(name, mapper, reducer, combiner=None):
    outp = f'/user/root/btc/out/bench_{name}'
    sh(f'hdfs dfs -rm -r -f {outp}')
    files = ','.join(str(CODE / f) for f in dict.fromkeys([mapper, reducer] + ([combiner] if combiner else [])))
    comb = f"-combiner 'python3 {combiner}'" if combiner else ''
    cmd = (f"hadoop jar {STREAM_JAR} -D mapreduce.job.name=bench_{name} -D mapreduce.job.reduces=1 -files {files} "
           f"-mapper 'python3 {mapper}' {comb} -reducer 'python3 {reducer}' -input {HDFS_RAW} -output {outp}")
    rc, o, e, t = sh(cmd)
    assert rc == 0, e[-2000:]
    return t
for r in range(1, RUNS + 1):
    rec('Hadoop Streaming', 'yearly', r, streaming('yearly', 'mapper_year.py', 'reducer_year.py', 'reducer_year.py'), 'map+combiner+1 reduce, YARN')
for r in range(1, RUNS + 1):
    rec('Hadoop Streaming', 'daily_ohlc', r, streaming('daily', 'mapper_ohlc.py', 'reducer_ohlc.py'), 'in-mapper combining+1 reduce, YARN')

# ---- Hive (MR engine) ----
def hive(f):
    rc, o, e, t = sh(f'hive -S -f {CODE / f}', cwd=str(HIVE_META))
    assert rc == 0, e[-2000:]
    return t
for r in range(1, RUNS + 1):
    rec('Hive (MR)', 'yearly', r, hive('hive_03a_yearly.hql'), 'hive -f: старт CLI + компіляція + MR-задачі')
for r in range(1, RUNS + 1):
    rec('Hive (MR)', 'daily_ohlc', r, hive('hive_04_daily_struct.hql'), 'CTAS daily_ohlc (min/max над struct(ts,значення))')

# ---- Pandas ----
PD = {}
for r in range(1, RUNS + 1):
    t0 = time.perf_counter()
    df = pd.read_csv(CSV, float_precision='round_trip').dropna()
    t_read = time.perf_counter() - t0
    t0 = time.perf_counter()
    years = pd.to_datetime(df['Timestamp'], unit='s').dt.year
    PD_YEAR = df.groupby(years.rename('year')).agg(candles=('Volume', 'size'), volume=('Volume', 'sum')).reset_index()
    t_y = time.perf_counter() - t0
    t0 = time.perf_counter()
    d = df.sort_values('Timestamp', kind='stable')
    day = (d['Timestamp'] // 86400).rename('day_n')
    PD_DAILY = d.groupby(day).agg(open=('Open', 'first'), high=('High', 'max'), low=('Low', 'min'), close=('Close', 'last'),
                                  volume=('Volume', 'sum'), candles=('Volume', 'size')).reset_index()
    PD_DAILY['day'] = pd.to_datetime(PD_DAILY['day_n'] * 86400, unit='s').dt.strftime('%Y-%m-%d')
    PD_DAILY = PD_DAILY[['day', 'open', 'high', 'low', 'close', 'volume', 'candles']]
    t_d = time.perf_counter() - t0
    rec('Pandas', 'read_csv', r, t_read, 'read_csv+dropna (частина обох задач)')
    rec('Pandas', 'yearly_agg', r, t_y, 'лише groupby, без читання')
    rec('Pandas', 'daily_agg', r, t_d, 'лише sort+groupby, без читання')
    rec('Pandas', 'yearly', r, t_read + t_y, 'читання CSV + агрегація')
    rec('Pandas', 'daily_ohlc', r, t_read + t_d, 'читання CSV + агрегація')
    del df, d
PD_YEAR.to_csv(RES / 'pandas_yearly_stats.csv', index=False); PD_DAILY.to_csv(RES / 'pandas_daily_ohlc_full.csv', index=False)

# ---- PySpark (JDK 21, Spark 4.x) ----
os.environ['JAVA_HOME'] = JDK21
os.environ['PATH'] = f'{JDK21}/bin:' + os.environ['PATH']
import pyspark
from pyspark.sql import SparkSession, functions as F
t0 = time.perf_counter()
spark = (SparkSession.builder.master('local[*]').appName('pz1_btc').config('spark.driver.memory', '3g')
         .config('spark.sql.session.timeZone', 'UTC').config('spark.sql.shuffle.partitions', '8')
         .config('spark.ui.enabled', 'false').getOrCreate())
spark.sparkContext.setLogLevel('ERROR')
rec('PySpark (local[*])', 'session_start', 1, time.perf_counter() - t0, f'pyspark {pyspark.__version__}')
schema = 'Timestamp DOUBLE, Open DOUBLE, High DOUBLE, Low DOUBLE, Close DOUBLE, Volume DOUBLE'
def load():
    s = spark.read.csv(f'file://{CSV}', header=True, schema=schema).na.drop()
    return s.withColumn('t', F.timestamp_seconds(F.col('Timestamp').cast('long')))
for r in range(1, RUNS + 1):
    t0 = time.perf_counter()
    SP_YEAR = load().groupBy(F.year('t').alias('year')).agg(F.count('*').alias('candles'), F.sum('Volume').alias('volume')).orderBy('year').toPandas()
    rec('PySpark (local[*])', 'yearly', r, time.perf_counter() - t0, 'читання CSV + агрегація + toPandas')
for r in range(1, RUNS + 1):
    t0 = time.perf_counter()
    SP_DAILY = (load().withColumn('day', F.date_format('t', 'yyyy-MM-dd')).groupBy('day')
                .agg(F.min_by('Open', 'Timestamp').alias('open'), F.max('High').alias('high'), F.min('Low').alias('low'),
                     F.max_by('Close', 'Timestamp').alias('close'), F.sum('Volume').alias('volume'), F.count('*').alias('candles'))
                .orderBy('day')).toPandas()
    rec('PySpark (local[*])', 'daily_ohlc', r, time.perf_counter() - t0, 'читання CSV + агрегація + toPandas')
spark.stop()
SP_YEAR.to_csv(RES / 'spark_yearly_stats.csv', index=False); SP_DAILY.to_csv(RES / 'spark_daily_ohlc_full.csv', index=False)
m = SP_YEAR.merge(PD_YEAR, on='year', suffixes=('_sp', '_pd'))
ok_y = len(m) == len(PD_YEAR) == len(SP_YEAR) and (m.candles_sp == m.candles_pd).all() and np.allclose(m.volume_sp, m.volume_pd, rtol=1e-9)
m = SP_DAILY.merge(PD_DAILY, on='day', suffixes=('_sp', '_pd'))
ok_d = len(m) == len(PD_DAILY) == len(SP_DAILY) and (m.candles_sp == m.candles_pd).all() and np.allclose(m.volume_sp, m.volume_pd, rtol=1e-9)
for k in ('open', 'high', 'low', 'close'):
    ok_d = ok_d and np.allclose(m[k + '_sp'], m[k + '_pd'], rtol=1e-12)
pd.DataFrame([dict(check='spark_vs_pandas_yearly', match=bool(ok_y)), dict(check='spark_vs_pandas_daily_ohlc', match=bool(ok_d))]).to_csv(RES / 'crosscheck_spark_vs_pandas.csv', index=False)
print('crosscheck spark vs pandas:', ok_y, ok_d)

# ---- підсумок ----
T = pd.DataFrame(rows)
T['machine'] = f"{os.cpu_count()} vCPU / 15 GiB RAM"
T.to_csv(RES / 'timings.csv', index=False)
S = T.groupby(['tool', 'task'], sort=False).seconds.agg(median='median', min='min', max='max', runs='count').round(2).reset_index()
S.to_csv(RES / 'timing_summary.csv', index=False)
print(S.to_string(index=False))
os.remove(RES / 'timings_partial.csv')
