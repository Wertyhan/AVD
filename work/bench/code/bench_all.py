#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
bench_all.py - єдина точка входу: чесне порівняння двох агрегацій у чотирьох рушіях.
  T1: кількість свічок і сумарний обсяг за роками;  T2: добові OHLC (first/max/min/last/sum за Timestamp).
  Рушії: Hadoop Streaming (YARN), Hive (рушій MR), pandas, PySpark local[*].
Запуск:  python3 bench_all.py            (повний прогін: очікування тиші, 1 прогрів + 5 вимірюваних кіл)
         python3 bench_all.py --smoke    (одне коло для перевірки, результати у bench/smoke, не рахуються)
Службовий режим:  bench_all.py --job <engine> <T1|T2|start> <outdir>  (виконується у дочірньому процесі).
"""
import os, sys, re, time, subprocess, shutil, json
from pathlib import Path

W = Path(__file__).resolve().parents[2]; B = W / 'bench'; CODE = B / 'code'
CSV = W / 'data/raw/btcusd_1-min_data.csv'
SCR = Path('/tmp/bench_scratch')            # локальний каталог виводу pandas/Spark
HDFS_RAW = '/user/root/btc/raw'; HDFS_OUT = '/user/root/btc/out'; HDFS_TINY = '/user/root/btc/bench_tiny'
HH = W / 'tools/hadoop-3.3.6'; HV = W / 'tools/apache-hive-3.1.3-bin'
JDK8 = W / 'mamba/envs/jdk8'; JDK21 = '/usr/lib/jvm/java-21-openjdk-amd64'
HIVE_META = W / 'hadoop_runtime/hive_meta'
SELF = str(CODE / 'bench_all.py')
ROUNDS = 5

# ------------------------------------------------------------------ дочірні завдання
def job_pandas(task, out):
    import pandas as pd
    df = pd.read_csv(CSV, float_precision='round_trip').dropna()
    if task == 'T1':
        y = pd.to_datetime(df['Timestamp'], unit='s').dt.year.rename('year')
        r = df.groupby(y).agg(candles=('Volume', 'size'), volume=('Volume', 'sum')).reset_index()
    else:
        d = df.sort_values('Timestamp', kind='stable')
        day = (d['Timestamp'] // 86400).rename('day_n')
        r = d.groupby(day).agg(open=('Open', 'first'), high=('High', 'max'), low=('Low', 'min'), close=('Close', 'last'),
                               volume=('Volume', 'sum'), candles=('Volume', 'size')).reset_index()
        r['day_n'] = pd.to_datetime(r['day_n'] * 86400, unit='s').dt.strftime('%Y-%m-%d')
    r.to_csv(Path(out) / 'result.csv', index=False, header=False)

def spark_session():
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.master('local[*]').appName('bench').config('spark.driver.memory', '3g')
         .config('spark.sql.session.timeZone', 'UTC').config('spark.sql.shuffle.partitions', '8')
         .config('spark.ui.enabled', 'false').getOrCreate())
    s.sparkContext.setLogLevel('ERROR')
    return s

def job_spark(task, out):
    from pyspark.sql import functions as F
    spark = spark_session()
    if task == 'start':
        spark.stop(); return
    schema = 'Timestamp DOUBLE, Open DOUBLE, High DOUBLE, Low DOUBLE, Close DOUBLE, Volume DOUBLE'
    s = spark.read.csv(f'file://{CSV}', header=True, schema=schema).na.drop()
    s = s.withColumn('t', F.timestamp_seconds(F.col('Timestamp').cast('long')))
    if task == 'T1':
        r = s.groupBy(F.year('t').alias('year')).agg(F.count('*').alias('candles'), F.sum('Volume').alias('volume'))
    else:
        r = (s.withColumn('day', F.date_format('t', 'yyyy-MM-dd')).groupBy('day')
             .agg(F.min_by('Open', 'Timestamp'), F.max('High'), F.min('Low'), F.max_by('Close', 'Timestamp'),
                  F.sum('Volume'), F.count('*')))
    r.coalesce(1).write.mode('overwrite').option('header', 'false').csv(f'file://{out}/result')
    spark.stop()

if len(sys.argv) > 1 and sys.argv[1] == '--job':
    eng, task, out = sys.argv[2:5]
    {'pandas': job_pandas, 'spark': job_spark}[eng](task, out)
    sys.exit(0)

# ------------------------------------------------------------------ головний процес
import numpy as np, pandas as pd
os.environ.pop('JAVA_TOOL_OPTIONS', None)
ENV8 = dict(os.environ, JAVA_HOME=str(JDK8), HADOOP_HOME=str(HH), HIVE_HOME=str(HV), HADOOP_CONF_DIR=str(HH / 'etc/hadoop'),
            HADOOP_MAPRED_HOME=str(HH), PATH=f'{HH}/bin:{HV}/bin:{JDK8}/bin:' + os.environ['PATH'])
ENV21 = dict(os.environ, JAVA_HOME=JDK21, PATH=f'{JDK21}/bin:' + os.environ['PATH'])
STREAM_JAR = next((HH / 'share/hadoop/tools/lib').glob('hadoop-streaming-*.jar'))
PY = sys.executable

def sh(cmd, env=ENV8, cwd=None):
    t0 = time.perf_counter()
    p = subprocess.run(cmd, shell=True, executable='/bin/bash', env=env, cwd=cwd, capture_output=True, text=True)
    return p.returncode, p.stdout, p.stderr, time.perf_counter() - t0

def streaming_cmd(task):
    out = f'{HDFS_OUT}/bench_hs_{task}'
    if task == 'T1':
        m, r, c = 'mapper_year.py', 'reducer_year.py', 'reducer_year.py'
    else:
        m, r, c = 'mapper_ohlc.py', 'reducer_ohlc.py', None
    P1 = W / 'pz1/code'
    files = ','.join(str(P1 / f) for f in dict.fromkeys([m, r] + ([c] if c else [])))
    comb = f"-combiner 'python3 {c}'" if c else ''
    return out, (f"hadoop jar {STREAM_JAR} -D mapreduce.job.name=bench_{task} -D mapreduce.job.reduces=1 -files {files} "
                 f"-mapper 'python3 {m}' {comb} -reducer 'python3 {r}' -input {HDFS_RAW} -output {out}")

def prepare(engine, task):
    """Очищення вихідних каталогів (поза вимірюваним часом)."""
    if engine in ('pandas', 'spark'):
        d = SCR / f'{engine}_{task}'; shutil.rmtree(d, ignore_errors=True); d.mkdir(parents=True)
    elif engine == 'hadoop':
        sh(f'hdfs dfs -rm -r -f {HDFS_OUT}/bench_hs_{task}')
    elif engine == 'hive':
        sh(f'hdfs dfs -rm -r -f {HDFS_OUT}/bench_hive_{task}')

def command(engine, task):
    """Команда, яка вимірюється (від запуску процесу до готового результату)."""
    if engine == 'pandas':
        if task == 'start': return f'{PY} -c "import pandas"', ENV8, None
        return f'{PY} {SELF} --job pandas {task} {SCR}/pandas_{task}', ENV8, None
    if engine == 'spark':
        return f'{PY} {SELF} --job spark {task} {SCR}/spark_{task}', ENV21, None
    if engine == 'hadoop':
        if task == 'start':
            return (f"hadoop jar {STREAM_JAR} -D mapreduce.job.reduces=0 -mapper cat -input {HDFS_TINY} "
                    f"-output {HDFS_OUT}/bench_hs_start", ENV8, None)
        return streaming_cmd(task)[1], ENV8, None
    if engine == 'hive':
        if task == 'start': return 'hive -S -e "select 1"', ENV8, str(HIVE_META)
        return f'hive -S -f {CODE}/bench_{"yearly" if task == "T1" else "daily"}.hql', ENV8, str(HIVE_META)

def fetch(engine, task):
    """Результат у DataFrame без назв стовпців (поза вимірюваним часом)."""
    if engine in ('pandas', 'spark'):
        d = SCR / f'{engine}_{task}'
        f = d / 'result.csv' if engine == 'pandas' else next((d / 'result').glob('part-*.csv'))
        return pd.read_csv(f, header=None, float_precision='round_trip')
    d = f'{HDFS_OUT}/bench_hs_{task}' if engine == 'hadoop' else f'{HDFS_OUT}/bench_hive_{task}'
    rc, o, e, _ = sh(f'hdfs dfs -cat {d}/*')
    assert rc == 0, e[-500:]
    import io
    return pd.read_csv(io.StringIO(o), sep='\t', header=None, float_precision='round_trip')

def normalise(df, task):
    if task == 'T1':
        df = df.iloc[:, :3].copy(); df.columns = ['year', 'candles', 'volume']
        return df.sort_values('year').reset_index(drop=True)
    df = df.iloc[:, :7].copy(); df.columns = ['day', 'open', 'high', 'low', 'close', 'volume', 'candles']
    return df.sort_values('day').reset_index(drop=True)

REF = {}
def verify(engine, task):
    cur = normalise(fetch(engine, task), task)
    if engine == 'pandas' and task not in REF:
        REF[task] = cur
        return 'reference'
    ref = REF[task]
    ok = len(cur) == len(ref)
    if ok:
        ok = bool((cur.iloc[:, 0].astype(str).values == ref.iloc[:, 0].astype(str).values).all())
        ok = ok and bool((cur['candles'].astype('int64').values == ref['candles'].astype('int64').values).all())
        ok = ok and bool(np.allclose(cur['volume'], ref['volume'], rtol=1e-6, atol=0))
        if task == 'T2':
            for k in ('open', 'high', 'low', 'close'):
                ok = ok and bool((cur[k].values == ref[k].values).all())
    return 'ok' if ok else 'MISMATCH'

def mem_avail_gb():
    return round(int(re.search(r'MemAvailable:\s+(\d+)', open('/proc/meminfo').read()).group(1)) / 1048576, 2)

def run_once(engine, task, rnd, warm, rows, runs_csv):
    prepare(engine, task)
    if engine == 'hadoop' and task == 'start': sh(f'hdfs dfs -rm -r -f {HDFS_OUT}/bench_hs_start')
    cmd, env, cwd = command(engine, task)
    l1, mem, ts = os.getloadavg()[0], mem_avail_gb(), time.strftime('%Y-%m-%d %H:%M:%S')
    rc, o, e, t = sh(cmd, env, cwd)
    l2 = os.getloadavg()[0]
    status = 'rc=%d' % rc
    if rc == 0 and task != 'start':
        status = verify(engine, task)
    row = dict(round=rnd, warmup=warm, engine=engine, task=task, seconds=round(t, 3), load1_before=l1, load1_after=l2,
               mem_available_gb=mem, timestamp=ts, check=status)
    print(row, flush=True)
    if rc != 0: print(e[-1500:], flush=True)
    rows.append(row); pd.DataFrame(rows).to_csv(runs_csv, index=False)
    return row

def quiet_wait(log, max_min=90):
    """Опитування щохвилини: load1 < 1,5 тричі поспіль і жодних сторонніх java/spark-процесів."""
    streak, quiet, polls = 0, False, []
    for i in range(max_min):
        la = os.getloadavg()[0]
        ps = subprocess.run("ps -eo pid,args", shell=True, capture_output=True, text=True).stdout.splitlines()
        others = []
        for l in ps:
            if re.search(r'java|spark-submit|pyspark|pz\d?_spark', l) and 'grep' not in l and 'bench_all' not in l:
                if re.search(r'org\.apache\.hadoop|kafka\.Kafka|kafka_2\.13|\bps -eo', l): continue   # демони Hadoop, Kafka (лише записується)
                others.append(l.strip()[:120])
        ok = la < 1.5 and not others
        streak = streak + 1 if ok else 0
        polls.append(dict(poll=i, time=time.strftime('%H:%M:%S'), load1=la, other_procs=len(others), streak=streak))
        print(polls[-1], flush=True)
        if streak >= 3: quiet = True; break
        time.sleep(60)
    pd.DataFrame(polls).to_csv(log, index=False)
    return quiet, len(polls)

def conditions(quiet, npolls, resdir):
    ver = lambda c, env=ENV8: subprocess.run(c, shell=True, env=env, capture_output=True, text=True).stdout.strip().splitlines()[:1]
    ps = subprocess.run("ps -eo comm,args", shell=True, capture_output=True, text=True).stdout
    import pyspark
    c = dict(start_time=time.strftime('%Y-%m-%d %H:%M:%S'), quiet_reached=quiet, polls=npolls,
             load1_start=os.getloadavg()[0], load5_start=os.getloadavg()[1], nproc=os.cpu_count(), mem_available_gb=mem_avail_gb(),
             kafka_running=bool(re.search(r'kafka\.Kafka|kafka_2\.13', ps)), mongod_running='mongod' in ps,
             pandas=pd.__version__, pyspark=pyspark.__version__, python=sys.version.split()[0],
             hadoop='3.3.6', hive='3.1.3 (MR)', jdk_hadoop_hive='8u472', jdk_spark='21',
             spark_cfg='local[*], driver 3g, shuffle.partitions 8, UI off', hadoop_cfg='1 reducer, 1 node YARN/HDFS (pseudo-distributed)')
    pd.DataFrame([c]).to_csv(resdir / 'bench_conditions.csv', index=False)

def summarise(runs, resdir):
    r = runs[~runs.warmup]
    ok = r[r.task.isin(['T1', 'T2'])]
    st = r[r.task == 'start'].groupby('engine').seconds.median()
    out = []
    for (e, t), g in ok.groupby(['engine', 'task'], sort=False):
        s = g.seconds; sm = st[e]; p = s - sm
        q1, q3 = s.quantile(.25), s.quantile(.75)
        out.append(dict(engine=e, task=t, median_total_s=round(s.median(), 2), median_startup_s=round(sm, 2),
                        median_processing_s=round(s.median() - sm, 2), min=round(s.min(), 2), max=round(s.max(), 2),
                        iqr=round(q3 - q1, 2), n=len(s), min_processing_s=round(p.min(), 2), max_processing_s=round(p.max(), 2),
                        all_checks_ok=bool((g.check.isin(['ok', 'reference'])).all())))
    S = pd.DataFrame(out); S.to_csv(resdir / 'bench_summary.csv', index=False)
    return S

def figure(S, figdir):
    sys.path.insert(0, str(W / 'tools_report')); import plotstyle
    import matplotlib.pyplot as plt
    names = {'hadoop': 'Hadoop Streaming', 'hive': 'Hive (MR)', 'pandas': 'pandas', 'spark': 'PySpark'}
    fig, axs = plt.subplots(2, 1, figsize=plotstyle.TWO_PANEL, sharex=True)
    for ax, (task, ttl) in zip(axs, [('T1', 'Т1: свічки та обсяг за роками'), ('T2', 'Т2: добові OHLC')]):
        g = S[S.task == task].set_index('engine').loc[list(names)]
        x = np.arange(len(g)); w = .38
        for off, col, lo, hi, lab, c in [(-w / 2, 'median_total_s', 'min', 'max', 'Загальний час (зі стартом)', '#3b6ea5'),
                                         (w / 2, 'median_processing_s', 'min_processing_s', 'max_processing_s', 'Без старту (обробка)', '#e08a2c')]:
            v = g[col].values
            ax.bar(x + off, v, w, color=c, label=lab, yerr=[v - g[lo].values, g[hi].values - v], capsize=3, error_kw=dict(lw=1))
            for xi, vi in zip(x + off, v): ax.text(xi, vi * 1.04 + 1, plotstyle.dec(vi, 1), ha='center', fontsize=9)
        ax.set_title(ttl); ax.set_ylabel('Час, с'); ax.set_ylim(0, g['max'].max() * 1.25)
        plotstyle.commas(ax)
    axs[0].legend(loc='upper right', frameon=False)
    axs[1].set_xticks(x); axs[1].set_xticklabels([names[e] for e in g.index])
    fig.tight_layout(); fig.savefig(figdir / 'fig_bench.png'); plt.close(fig)

def main():
    smoke = '--smoke' in sys.argv
    res = B / ('smoke' if smoke else 'results'); res.mkdir(parents=True, exist_ok=True)
    if not smoke: quiet, npolls = quiet_wait(res / 'bench_quiet_polls.csv')
    else: quiet, npolls = False, 0
    # підготовка: крихітний вхід для порожнього MR-завдання; перевірка заголовка таблиці Hive
    sh(f'hdfs dfs -mkdir -p {HDFS_TINY} && echo x | hdfs dfs -put -f - {HDFS_TINY}/one.txt')
    rc, o, e, _ = sh('hive -S -e "select count(*) from btc.candles"', cwd=str(HIVE_META))
    assert rc == 0 and o.strip().splitlines()[-1] == '7816537', (o, e[-500:])
    conditions(quiet, npolls, res)
    combos = [(e, t) for e in ('hadoop', 'hive', 'pandas', 'spark') for t in ('T1', 'T2')] + [(e, 'start') for e in ('hadoop', 'hive', 'pandas', 'spark')]
    # pandas T1/T2 першими в нульовому колі, щоб мати еталон для звірки
    combos.sort(key=lambda c: 0 if c[0] == 'pandas' and c[1] != 'start' else 1)
    # чергування: кожне коло проходить усі рушії та задачі; порядок рушіїв циклічно зсувається
    rows, runs_csv = [], res / 'bench_runs.csv'
    for rnd in range(0, 1 if smoke else ROUNDS + 1):
        order = combos[:]
        if rnd > 0:
            k = rnd % len(order); order = order[k:] + order[:k]
        else:
            order = combos
        for e, t in order:
            run_once(e, t, rnd, rnd == 0, rows, runs_csv)
    runs = pd.DataFrame(rows)
    if smoke: return
    S = summarise(runs, res); print(S.to_string(index=False))
    figure(S, B / 'figures')
    for p in (W / 'pz1/results', W / 'pz2/results'): shutil.copy(res / 'bench_summary.csv', p / 'engine_benchmark.csv')

main()
