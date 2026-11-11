#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""run_pz1.py - ПЗ1: HDFS, MapReduce (Hadoop Streaming), Hive на повному наборі Bitcoin (1-хв свічки).
Локальний запуск (pseudo-distributed Hadoop 3.3.6 + Hive 3.1.3). Адаптовано з Colab-ноутбука.
"""
import os, sys, re, io, json, glob, math, time, shlex, shutil, zipfile, platform, traceback, subprocess, datetime
from pathlib import Path
from types import SimpleNamespace

# ---------- шляхи / paths ----------
DRIVE_BASE = Path('/nonexistent_drive')   # на Colab тут був Google Drive; локально синхронізація не потрібна
OUT_DRIVE  = DRIVE_BASE / 'colab_results' / 'pz1'
OUT   = Path('/home/user/AVD/work/pz1')       # каталог результатів ПЗ1
SHOTS = OUT / 'screenshots'
RES   = OUT / 'results'
LOGS  = OUT / 'logs'
CODE  = OUT / 'code'                           # mapper/reducer/hql
DATA  = Path('/home/user/AVD/work/data/raw')
for d in (SHOTS, RES, LOGS, CODE):
    d.mkdir(parents=True, exist_ok=True)

# ---------- версії та середовище ----------
HADOOP_VER, HIVE_VER = '3.3.6', '3.1.3'
HADOOP_HOME, HIVE_HOME = '/home/user/AVD/work/tools/hadoop-3.3.6', '/home/user/AVD/work/tools/apache-hive-3.1.3-bin'
RT = '/home/user/AVD/work/hadoop_runtime'      # дані NameNode/DataNode, pid-файли, метасховище Hive
JAVA_HOME = '/home/user/AVD/work/mamba/envs/jdk8'   # JDK 8 (потрібен Hive 3.1.3; Hadoop 3.3.6 теж працює на ній)
HDFS_BASE = '/user/root/btc'
os.environ.pop('JAVA_TOOL_OPTIONS', None)   # проксі-налаштування сесії не потрібні Hadoop
os.environ.update({
    'JAVA_HOME': JAVA_HOME, 'HADOOP_HOME': HADOOP_HOME, 'HADOOP_CONF_DIR': f'{HADOOP_HOME}/etc/hadoop',
    'HADOOP_MAPRED_HOME': HADOOP_HOME, 'HIVE_HOME': HIVE_HOME,
    'HDFS_NAMENODE_USER': 'root', 'HDFS_DATANODE_USER': 'root', 'HDFS_SECONDARYNAMENODE_USER': 'root',
    'YARN_RESOURCEMANAGER_USER': 'root', 'YARN_NODEMANAGER_USER': 'root', 'MAPRED_HISTORYSERVER_USER': 'root',
    'PYTHONHASHSEED': '42',
})
os.environ['PATH'] = ':'.join([f'{HADOOP_HOME}/bin', f'{HADOOP_HOME}/sbin', f'{HIVE_HOME}/bin',
                               f'{JAVA_HOME}/bin', os.environ['PATH']])

STATE = {}          # прапорці готовності: hdfs_up, csv, ...
RUNTIMES = []       # (крок, секунди)
TIMINGS = []        # порівняння інструментів
CHECK = {}          # чекліст: ключ -> 'OK' / 'FAIL: ...'
TASKS = [           # (ключ, опис)
    ('data',         'Дані: archive.zip -> CSV (Data prepared)'),
    ('arch',         'Завдання 1: архітектура Hadoop / HDFS (dfsadmin, fsck, jps)'),
    ('install',      'Завдання 2: Java 8 + Hadoop 3.3 встановлено і налаштовано'),
    ('daemons',      'Завдання 2: NameNode/DataNode/ResourceManager/NodeManager запущено'),
    ('hdfs',         'Завдання 3: CSV завантажено в HDFS (ls/du/cat/fsck)'),
    ('selftest',     'Попередня перевірка mapper/reducer на вибірці (vs Pandas)'),
    ('wordcount',    'Завдання 4: WordCount (hadoop-mapreduce-examples)'),
    ('mr_year',      'Завдання 4: власна Streaming-задача, свічки та обсяг за роками'),
    ('mr_ohlc',      'Завдання 4: власна Streaming-задача, добові OHLC'),
    ('hive_install', 'Завдання 5: Hive 3.1.3 + Derby metastore'),
    ('hive_table',   'Завдання 5: зовнішня таблиця Hive над CSV у HDFS'),
    ('hive_queries', 'Завдання 6: вибірка, групування за роком/місяцем'),
    ('hive_daily',   'Завдання 6: добова таблиця, топ-10 за обсягом і за High-Low'),
    ('pandas',       'Завдання 7: Pandas (ті самі агрегації) + звірка з MapReduce'),
    ('spark',        'Завдання 7: PySpark (ті самі агрегації) + звірка'),
    ('timing',       'Завдання 7: таблиця порівняння часу'),
    ('ui_shots',     'Веб-інтерфейси NameNode / YARN / Spark (скріншоти)'),
    ('save',         'Збереження версій, логів, runtimes'),
]
for k, _ in TASKS:
    CHECK[k] = 'FAIL: не виконано / not executed'

def banner(t):
    print('\n' + '=' * 78 + f'\n== {t}\n' + '=' * 78, flush=True)

def sync():
    """Копіює локальні результати на Drive (викликається після кожного кроку)."""
    try:
        if not DRIVE_BASE.exists():
            return
        OUT_DRIVE.mkdir(parents=True, exist_ok=True)
        pd_ = __import__('pandas')
        pd_.DataFrame(RUNTIMES, columns=['step', 'seconds']).to_csv(OUT / 'runtimes.csv', index=False)
        shutil.copytree(OUT, OUT_DRIVE, dirs_exist_ok=True)
        shutil.copytree(CODE, OUT_DRIVE / 'code', dirs_exist_ok=True)
    except Exception as e:
        print('  [sync] попередження / warning:', e)

class step:
    """Контекст кроку: заголовок, час, перехоплення помилок, запис у чекліст. Run all не зупиняється."""
    def __init__(self, key, title):
        self.key, self.title = key, title
    def __enter__(self):
        banner(f'[{self.key}] {self.title}')
        self.t0 = time.time()
        return self
    def __exit__(self, et, ev, tb):
        dt = time.time() - self.t0
        RUNTIMES.append((self.key, round(dt, 1)))
        if et is not None and issubclass(et, Exception):
            msg = f'{et.__name__}: {ev}'
            CHECK[self.key] = 'FAIL: ' + msg.replace('\n', ' ')[:300]
            print('\n!!! ПОМИЛКА / FAILED:', self.key, '\n' + msg)
            traceback.print_exception(et, ev, tb, limit=-3)
            sync()
            return True
        if CHECK.get(self.key, '').startswith('FAIL: не виконано'):
            CHECK[self.key] = 'OK'
        print(f'--- [{self.key}] завершено за {dt:.1f} с / done in {dt:.1f} s ---')
        sync()
        return False

def check(cond, ua, en):
    """Перевірка з двомовним повідомленням / bilingual assertion."""
    if not cond:
        raise AssertionError(f'{ua} | {en}')

def need(flag, ua, en):
    check(STATE.get(flag), f'{ua} (попередній крок не виконано)', f'{en} (previous step did not finish)')

def strip_ansi(s):
    return re.sub(r'\x1b\[[0-9;?]*[A-Za-z]', '', s).replace('\r', '')

def sh(cmd, log=None, check_rc=True, timeout=None, quiet=False, tail=25, cwd=None, err_to=None, echo=True):
    """Виконує bash-команду, друкує вивід (або лише хвіст при quiet), пише лог. Повертає (rc, out, secs)."""
    if echo:
        print('\n$ ' + (cmd if len(cmd) < 400 else cmd[:400] + ' ...'), flush=True)
    full = cmd
    if timeout:
        full = f'timeout {int(timeout)} bash -c {shlex.quote(cmd)}'
    t0 = time.time()
    errf = open(err_to, 'a') if err_to else None
    p = subprocess.Popen(full, shell=True, executable='/bin/bash', stdout=subprocess.PIPE,
                         stderr=errf if errf else subprocess.STDOUT, text=True, errors='replace',
                         cwd=cwd, env=os.environ)
    lines = []
    lf = open(LOGS / log, 'a') if log else None
    if lf:
        lf.write(f'\n$ {cmd}\n')
    for ln in p.stdout:
        lines.append(ln)
        if lf:
            lf.write(ln)
        if not quiet:
            sys.stdout.write(ln)
    p.wait()
    if lf: lf.close()
    if errf: errf.close()
    out = strip_ansi(''.join(lines))
    secs = time.time() - t0
    if quiet and tail:
        print('\n'.join(out.rstrip('\n').split('\n')[-tail:]))
    if p.returncode != 0 and check_rc:
        last = '\n'.join(out.rstrip('\n').split('\n')[-30:])
        raise RuntimeError(f'Команда завершилась з кодом {p.returncode} / command failed (rc={p.returncode}): {cmd[:200]}\n{last}')
    return SimpleNamespace(rc=p.returncode, out=out, secs=secs)

# ---------- «скріншоти» терміналу / terminal screenshots ----------
SHOT_N = [0]
sys.path.insert(0, str(Path(__file__).resolve().parent))
from termshot import render_terminal_png   # рендер терміналу в PNG (див. termshot.py)

def shot(cmd, text, name):
    SHOT_N[0] += 1
    p = SHOTS / f'{SHOT_N[0]:02d}_{name}.png'
    try:
        render_terminal_png(cmd, text, p)
        print(f'  [скріншот/screenshot] {p.name}')
    except Exception as e:
        print('  [скріншот не створено / screenshot failed]', name, e)
    return p

def run_shot(cmd, name, log=None, **kw):
    """Виконує команду, показує вивід і зберігає його як PNG-«термінал»."""
    r = sh(cmd, log=log, **kw)
    shot(cmd, r.out, name)
    return r

def save_csv(df, name, **kw):
    p = RES / name
    df.to_csv(p, index=False, **kw)
    print(f'  [csv] {p.name}: {len(df)} рядків / rows')
    return p

print('Службові функції готові / helpers ready. Python', sys.version.split()[0])

with step('data', 'Перевірка вхідного CSV'):
    CSV = DATA / 'btcusd_1-min_data.csv'
    check(CSV.exists(), 'CSV не знайдено', 'CSV not found')
    header = open(CSV).readline().strip()
    print('Заголовок / header:', header)
    check(header.lower().replace(' ', '') == 'timestamp,open,high,low,close,volume', f'Несподівані стовпці CSV: {header}', 'Unexpected CSV columns')
    N_LINES = sum(1 for _ in open(CSV, 'rb'))
    N_ROWS_RAW = N_LINES - 1
    print(f'Файл: {CSV.name}, {CSV.stat().st_size / 1e6:.1f} MB, рядків даних: {N_ROWS_RAW:,}')
    check(N_ROWS_RAW == 7816537, 'Кількість рядків не збігається з README', 'Row count differs from README')
    STATE['csv'] = True

def download(urls, dest):
    """Пробує кілька дзеркал по черзі (dlcdn -> archive.apache.org)."""
    dest = Path(dest)
    for u in urls:
        r = sh(f'curl -fL --retry 3 --connect-timeout 25 -o "{dest}" "{u}" && gzip -t "{dest}"',
               check_rc=False, quiet=True, tail=3)
        if r.rc == 0:
            print('  завантажено / downloaded:', u, f'({dest.stat().st_size / 1e6:.0f} MB)')
            return
        print('  не вдалося / failed:', u)
    raise RuntimeError(f'Не вдалося завантажити {dest.name} з жодного дзеркала / could not download from any mirror')

def write_xml(path, props):
    body = ''.join(f'  <property><name>{k}</name><value>{v}</value></property>\n' for k, v in props.items())
    Path(path).write_text('<?xml version="1.0"?>\n<configuration>\n' + body + '</configuration>\n')



with step('install', 'Налаштування Hadoop ' + HADOOP_VER + ' (pseudo-distributed)'):
    need('csv', 'Немає даних', 'No data')
    check(Path(JAVA_HOME, 'bin/java').exists(), 'JDK 8 не знайдено', 'JDK 8 not found')
    sh('java -version')
    check(Path(HADOOP_HOME, 'bin/hadoop').exists(), 'Hadoop не розпаковано', 'Hadoop not unpacked')
    for sub in ('nn', 'dn', 'pids', 'tmp'):
        Path(RT, sub).mkdir(parents=True, exist_ok=True)
    conf = Path(HADOOP_HOME, 'etc/hadoop')
    envf = conf / 'hadoop-env.sh'
    txt = envf.read_text()
    txt = txt.split('# ---- PZ1 local ----')[0]
    txt += f"""# ---- PZ1 local ----
export JAVA_HOME={JAVA_HOME}
export HADOOP_PID_DIR={RT}/pids
export HDFS_NAMENODE_OPTS="-Xmx768m"
export HDFS_DATANODE_OPTS="-Xmx512m"
export YARN_RESOURCEMANAGER_OPTS="-Xmx768m"
export YARN_NODEMANAGER_OPTS="-Xmx768m"
export MAPRED_HISTORYSERVER_OPTS="-Xmx512m"
"""
    envf.write_text(txt)
    write_xml(conf / 'core-site.xml', {
        'fs.defaultFS': 'hdfs://localhost:9000',
        'hadoop.tmp.dir': RT + '/tmp'})
    write_xml(conf / 'hdfs-site.xml', {
        'dfs.replication': '1',
        'dfs.namenode.name.dir': 'file://' + RT + '/nn',
        'dfs.datanode.data.dir': 'file://' + RT + '/dn',
        'dfs.permissions.enabled': 'false',
        'dfs.webhdfs.enabled': 'true'})
    write_xml(conf / 'mapred-site.xml', {
        'mapreduce.framework.name': 'yarn',
        'mapreduce.application.classpath': '$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*',
        'yarn.app.mapreduce.am.env': 'HADOOP_MAPRED_HOME=' + HADOOP_HOME,
        'mapreduce.map.env': 'HADOOP_MAPRED_HOME=' + HADOOP_HOME,
        'mapreduce.reduce.env': 'HADOOP_MAPRED_HOME=' + HADOOP_HOME,
        'yarn.app.mapreduce.am.resource.mb': '1024',
        'yarn.app.mapreduce.am.command-opts': '-Xmx768m',
        'mapreduce.map.memory.mb': '1024',
        'mapreduce.map.java.opts': '-Xmx768m',
        'mapreduce.reduce.memory.mb': '1536',
        'mapreduce.reduce.java.opts': '-Xmx1024m',
        'mapreduce.task.io.sort.mb': '128',
        'mapreduce.jobhistory.address': 'localhost:10020',
        'mapreduce.jobhistory.webapp.address': 'localhost:19888',
        'mapreduce.jobhistory.intermediate-done-dir': RT + '/history/intermediate',
        'mapreduce.jobhistory.done-dir': RT + '/history/done'})
    write_xml(conf / 'yarn-site.xml', {
        'yarn.resourcemanager.hostname': 'localhost',
        'yarn.nodemanager.aux-services': 'mapreduce_shuffle',
        'yarn.nodemanager.env-whitelist': 'JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_MAPRED_HOME',
        'yarn.nodemanager.resource.memory-mb': '5120',
        'yarn.nodemanager.resource.cpu-vcores': '4',
        'yarn.scheduler.minimum-allocation-mb': '256',
        'yarn.scheduler.maximum-allocation-mb': '4096',
        'yarn.nodemanager.vmem-check-enabled': 'false',
        'yarn.nodemanager.pmem-check-enabled': 'false',
        'yarn.nodemanager.local-dirs': RT + '/nm-local',
        'yarn.nodemanager.log-dirs': RT + '/nm-logs',
        'yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage': '98.5',
        'yarn.log-aggregation-enable': 'false'})
    cs = conf / 'capacity-scheduler.xml'
    t = cs.read_text()
    t = re.sub(r'(<name>yarn.scheduler.capacity.maximum-am-resource-percent</name>\s*<value>)[^<]*(</value>)', r'\g<1>0.8\g<2>', t)
    cs.write_text(t)
    r = sh('hadoop version', log='01_install_hadoop.log')
    check('Hadoop ' + HADOOP_VER in r.out, 'Hadoop не встановлено коректно', 'Hadoop was not installed correctly')
    STATE['hadoop_installed'] = True

def jps_names():
    return set(re.findall(r'^\d+\s+(\w+)', sh('jps', echo=False, check_rc=False).out, flags=re.M))

with step('daemons', 'Запуск HDFS + YARN (pseudo-distributed)'):
    need('hadoop_installed', 'Hadoop не встановлено', 'Hadoop is not installed')
    nn_dir = Path(RT + '/nn/current')
    if not nn_dir.exists():
        sh('hdfs namenode -format -force -nonInteractive', log='02_format.log', quiet=True, tail=6)
    names = jps_names()
    if 'NameNode' not in names:
        sh('hdfs --daemon start namenode', log='02_daemons.log')
    if 'DataNode' not in names:
        sh('hdfs --daemon start datanode', log='02_daemons.log')
    if 'ResourceManager' not in names:
        sh('yarn --daemon start resourcemanager', log='02_daemons.log')
    if 'NodeManager' not in names:
        sh('yarn --daemon start nodemanager', log='02_daemons.log')
    sh('mapred --daemon start historyserver', log='02_daemons.log', check_rc=False)   # необов'язково (сторінка job)

    ok = False
    for i in range(60):                                   # до ~2 хв очікування
        rep = sh('hdfs dfsadmin -report', echo=False, check_rc=False).out
        nodes = sh('yarn node -list', echo=False, check_rc=False).out
        if 'Live datanodes (1)' in rep and 'RUNNING' in nodes:
            ok = True; break
        time.sleep(2)
    if not ok:
        for f in glob.glob(f'{HADOOP_HOME}/logs/*namenode*.log') + glob.glob(f'{HADOOP_HOME}/logs/*datanode*.log'):
            print('--- хвіст логу', f); print(sh(f'tail -n 25 {f}', echo=False).out)
    check(ok, 'NameNode/DataNode/NodeManager не запустились за 2 хв (див. логи вище)',
          'NameNode/DataNode/NodeManager did not come up within 2 min (see logs above)')
    sh('hdfs dfsadmin -safemode wait')
    names = jps_names()
    for need_ in ('NameNode', 'DataNode', 'ResourceManager', 'NodeManager'):
        check(need_ in names, f'Демон {need_} не працює', f'Daemon {need_} is not running')
    STATE['hdfs_up'] = True
    run_shot('jps', 'jps_daemons', log='02_daemons.log')
    sh(f'hdfs dfs -mkdir -p {HDFS_BASE}/raw {HDFS_BASE}/wc_in {HDFS_BASE}/out /tmp /user/hive/warehouse && hdfs dfs -chmod -R 777 /tmp /user/hive')
    run_shot('yarn node -list', 'yarn_node_list', log='02_daemons.log')
with step('arch', 'Конфігурація кластера: dfsadmin -report, getconf'):
    need('hdfs_up', 'HDFS не запущено', 'HDFS is not running')
    run_shot('hdfs dfsadmin -report', 'hdfs_dfsadmin_report', log='03_arch.log')
    run_shot('for k in fs.defaultFS dfs.replication dfs.blocksize dfs.namenode.name.dir; do echo -n "$k = "; hdfs getconf -confKey $k; done',
             'hdfs_getconf', log='03_arch.log')
    run_shot('yarn node -list -all', 'yarn_nodes', log='03_arch.log')
    import pandas as pd
    rep = sh('hdfs dfsadmin -report', echo=False).out
    cap = re.search(r'Configured Capacity: (\d+)', rep)
    arch = pd.DataFrame([
        ('fs.defaultFS', sh('hdfs getconf -confKey fs.defaultFS', echo=False).out.strip()),
        ('dfs.replication', sh('hdfs getconf -confKey dfs.replication', echo=False).out.strip()),
        ('dfs.blocksize_bytes', sh('hdfs getconf -confKey dfs.blocksize', echo=False).out.strip()),
        ('live_datanodes', '1' if 'Live datanodes (1)' in rep else '?'),
        ('configured_capacity_bytes', cap.group(1) if cap else '?')], columns=['parameter', 'value'])
    save_csv(arch, 'hdfs_cluster_config.csv')
with step('hdfs', 'HDFS: каталоги, put, ls, du, cat, fsck'):
    need('hdfs_up', 'HDFS не запущено', 'HDFS is not running')
    need('csv', 'Немає CSV', 'No CSV')
    import pandas as pd
    HDFS_CSV = f'{HDFS_BASE}/raw/{CSV.name}'
    run_shot(f'hdfs dfs -mkdir -p {HDFS_BASE}/raw {HDFS_BASE}/wc_in {HDFS_BASE}/out && hdfs dfs -ls -R /user/root', 'hdfs_mkdir_ls', log='04_hdfs.log')
    r = sh(f'hdfs dfs -put -f "{CSV}" {HDFS_CSV}', log='04_hdfs.log')
    put_secs = r.secs
    print(f'hdfs put: {put_secs:.1f} с')
    run_shot(f'hdfs dfs -ls -h {HDFS_BASE}/raw', 'hdfs_ls_raw', log='04_hdfs.log')
    run_shot(f'hdfs dfs -du -h {HDFS_BASE}', 'hdfs_du', log='04_hdfs.log')
    run_shot(f'hdfs dfs -cat {HDFS_CSV} | head -n 6', 'hdfs_cat_head', log='04_hdfs.log', check_rc=False)
    run_shot(f'hdfs dfs -tail {HDFS_CSV}', 'hdfs_tail', log='04_hdfs.log', check_rc=False)
    fs = run_shot(f'hdfs fsck {HDFS_BASE}/raw -files -blocks -locations', 'hdfs_fsck', log='04_hdfs.log')
    check('is HEALTHY' in fs.out, 'fsck: файлова система не HEALTHY', 'fsck: filesystem is not HEALTHY')
    size_hdfs = int(sh(f'hdfs dfs -stat %b {HDFS_CSV}', echo=False).out.strip())
    check(size_hdfs == CSV.stat().st_size, f'Розмір у HDFS ({size_hdfs}) != локальний ({CSV.stat().st_size})',
          'HDFS file size differs from local file size')
    blocks = int(re.search(r'Total blocks \(validated\):\s+(\d+)', fs.out).group(1))
    save_csv(pd.DataFrame([dict(file=CSV.name, size_bytes=size_hdfs, size_mb=round(size_hdfs / 1e6, 1),
                                hdfs_blocks=blocks, replication=1, rows_data=N_ROWS_RAW, put_seconds=round(put_secs, 1))]),
             'hdfs_file_info.csv')
    TIMINGS.append(dict(tool='HDFS', task='put CSV', seconds=round(put_secs, 1), note='hdfs dfs -put'))
    STATE['hdfs_data'] = True

def write_code(name, text):
    pass

with step('selftest', 'Локальна перевірка mapper/reducer на вибірці (cat | mapper | sort | reducer)'):
    need('csv', 'Немає CSV', 'No CSV')
    import pandas as pd, numpy as np
    smp = Path(RT + '/sample_200k.csv')
    sh(f'head -n 200001 "{CSV}" > {smp}', echo=False)
    S = "LC_ALL=C sort -t $'\\t' -k1,1 -s"
    y_out = sh(f'cat {smp} | python3 {CODE}/mapper_year.py | {S} | python3 {CODE}/reducer_year.py', quiet=True, tail=3).out
    o_out = sh(f'cat {smp} | python3 {CODE}/mapper_ohlc.py | {S} | python3 {CODE}/reducer_ohlc.py', quiet=True, tail=3).out
    sm = pd.read_csv(smp, float_precision='round_trip').dropna()
    sm['dt'] = pd.to_datetime(sm['Timestamp'], unit='s')
    y = pd.read_csv(io.StringIO(y_out), sep='\t', header=None, names=['year', 'n', 'vol'], float_precision='round_trip')
    g = sm.groupby(sm.dt.dt.year).agg(n=('Volume', 'size'), vol=('Volume', 'sum')).reset_index(names='year')
    mm = y.merge(g, on='year', suffixes=('_mr', '_pd'))
    check(len(mm) == len(g) == len(y) and (mm.n_mr == mm.n_pd).all() and np.allclose(mm.vol_mr, mm.vol_pd, rtol=1e-9),
          'Selftest: річні підсумки mapper/reducer не збігаються з Pandas', 'Selftest: yearly mapper/reducer output differs from Pandas')
    o = pd.read_csv(io.StringIO(o_out), sep='\t', header=None, names=['day', 'open', 'high', 'low', 'close', 'vol', 'n'], float_precision='round_trip')
    sm = sm.sort_values('Timestamp')
    d = sm.groupby(sm.dt.dt.strftime('%Y-%m-%d')).agg(open=('Open', 'first'), high=('High', 'max'), low=('Low', 'min'),
                                                      close=('Close', 'last'), vol=('Volume', 'sum'), n=('Volume', 'size')).reset_index(names='day')
    mo = o.merge(d, on='day', suffixes=('_mr', '_pd'))
    ok = len(mo) == len(d) == len(o) and (mo.n_mr == mo.n_pd).all() and np.allclose(mo.vol_mr, mo.vol_pd, rtol=1e-9)
    for k in ('open', 'high', 'low', 'close'):
        ok = ok and np.allclose(mo[k + '_mr'], mo[k + '_pd'], rtol=1e-12)
    check(ok, 'Selftest: добові OHLC mapper/reducer не збігаються з Pandas', 'Selftest: daily OHLC mapper/reducer differs from Pandas')
    print(f'Selftest OK: роки={len(y)}, доби={len(o)}')
with step('wordcount', 'WordCount (hadoop-mapreduce-examples) - перевірка кластера'):
    need('hdfs_up', 'HDFS не запущено', 'HDFS is not running')
    import pandas as pd
    ex_jar = glob.glob(f'{HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar')[0]
    sh(f'hdfs dfs -put -f {HADOOP_HOME}/LICENSE.txt {HADOOP_HOME}/NOTICE.txt {HDFS_BASE}/wc_in/')
    sh(f'hdfs dfs -rm -r -f {HDFS_BASE}/out/wordcount')
    cmd = f'hadoop jar {ex_jar} wordcount {HDFS_BASE}/wc_in {HDFS_BASE}/out/wordcount'
    r = run_shot(cmd, 'wordcount_run', log='05_wordcount.log', quiet=True, tail=40, timeout=900)
    check(sh(f'hdfs dfs -test -e {HDFS_BASE}/out/wordcount/_SUCCESS', check_rc=False, echo=False).rc == 0,
          'WordCount не завершився успішно (_SUCCESS відсутній)', 'WordCount failed (_SUCCESS missing)')
    TIMINGS.append(dict(tool='Hadoop (examples jar)', task='wordcount', seconds=round(r.secs, 1), note='LICENSE.txt+NOTICE.txt'))
    run_shot(f'hdfs dfs -ls {HDFS_BASE}/out/wordcount', 'wordcount_ls', log='05_wordcount.log')
    run_shot(f'hdfs dfs -cat {HDFS_BASE}/out/wordcount/part-r-00000 | sort -k2 -nr | head -n 20', 'wordcount_top20',
             log='05_wordcount.log', check_rc=False)
    wc = pd.read_csv(io.StringIO(sh(f'hdfs dfs -cat {HDFS_BASE}/out/wordcount/part-r-*', echo=False, quiet=True, tail=0).out),
                     sep='\t', header=None, names=['word', 'count'], quoting=3)
    check(len(wc) > 100, 'WordCount: підозріло мало слів у результаті', 'WordCount: suspiciously few words')
    save_csv(wc.sort_values('count', ascending=False).head(50), 'wordcount_top50.csv')
    print('Унікальних слів / distinct words:', len(wc), ' Усього / total:', int(wc['count'].sum()))
import pandas as pd, numpy as np
STREAM_JAR = (glob.glob(f'{HADOOP_HOME}/share/hadoop/tools/lib/hadoop-streaming-*.jar') or [''])[0]
JOBIDS = {}

def counters(text):
    """Витягує лічильники MapReduce з логу задачі."""
    want = ['Map input records', 'Map output records', 'Combine input records', 'Combine output records',
            'Reduce input groups', 'Reduce input records', 'Reduce output records', 'Launched map tasks', 'Launched reduce tasks']
    out = {}
    for w in want:
        m = re.search(r'^\s*' + re.escape(w) + r'=(\d+)', text, flags=re.M)
        if m: out[w] = int(m.group(1))
    return out

def run_streaming(name, mapper, reducer, combiner=None, reduces=1):
    """Запускає Hadoop Streaming задачу над CSV у HDFS. Повертає (секунди, лічильники, вивід)."""
    outp = f'{HDFS_BASE}/out/{name}'
    sh(f'hdfs dfs -rm -r -f {outp}', echo=False)
    comb = f"-combiner 'python3 {combiner}'" if combiner else ''
    cmd = (f"hadoop jar {STREAM_JAR} -D mapreduce.job.name={name} -D mapreduce.job.reduces={reduces} "
           f"-files {CODE}/{mapper},{CODE}/{reducer}"
           + (f",{CODE}/{combiner}" if combiner and combiner not in (reducer,) else '') +
           f" -mapper 'python3 {mapper}' {comb} -reducer 'python3 {reducer}' -input {HDFS_BASE}/raw -output {outp}")
    r = sh(cmd, log=f'06_{name}.log', quiet=True, tail=40, timeout=1800, check_rc=False)
    check(r.rc == 0, f'Задача {name} завершилась помилкою (rc={r.rc}), див. logs/06_{name}.log',
          f'Streaming job {name} failed (rc={r.rc}), see logs/06_{name}.log')
    check(sh(f'hdfs dfs -test -e {outp}/_SUCCESS', check_rc=False, echo=False).rc == 0,
          f'Задача {name}: немає _SUCCESS', f'Job {name}: _SUCCESS is missing')
    m = re.search(r'Running job: (job_\d+_\d+)', r.out)
    if m: JOBIDS[name] = m.group(1)
    cnt = counters(r.out)
    check(cnt.get('Map input records', 0) in (N_LINES, N_LINES + 1),
          f'Задача {name}: Map input records={cnt.get("Map input records")} не збігається з кількістю рядків файлу {N_LINES}',
          f'Job {name}: Map input records does not match number of lines in file')
    shot(cmd, r.out, f'{name}_job_output')
    save_csv(pd.DataFrame(sorted(cnt.items()), columns=['counter', 'value']), f'mr_counters_{name}.csv')
    return r.secs, cnt, outp

with step('mr_year', 'Hadoop Streaming: кількість свічок і сумарний обсяг за роками'):
    need('hdfs_data', 'CSV не в HDFS', 'CSV is not in HDFS')
    check(STREAM_JAR, 'hadoop-streaming jar не знайдено', 'hadoop-streaming jar not found')
    t, cnt, outp = run_streaming('mr_yearly', 'mapper_year.py', 'reducer_year.py', combiner='reducer_year.py')
    print('Лічильники / counters:', cnt)
    txt = sh(f'hdfs dfs -cat {outp}/part-*', echo=False, quiet=True, tail=0).out
    MR_YEAR = pd.read_csv(io.StringIO(txt), sep='\t', header=None, names=['year', 'candles', 'volume'], float_precision='round_trip')
    MR_YEAR = MR_YEAR.sort_values('year').reset_index(drop=True)
    shot(f'hdfs dfs -cat {outp}/part-*', txt, 'mr_yearly_result')
    print(MR_YEAR.to_string(index=False))
    check(MR_YEAR['candles'].sum() == N_ROWS_RAW, f'Сума свічок MR ({MR_YEAR.candles.sum()}) != рядків у CSV ({N_ROWS_RAW})',
          'MR candle total differs from CSV row count')
    save_csv(MR_YEAR, 'mr_yearly_stats.csv')
    TIMINGS.append(dict(tool='Hadoop Streaming', task='yearly', seconds=round(t, 1), note='map+combiner+reduce, YARN pseudo-distributed'))

with step('mr_ohlc', 'Hadoop Streaming: добові OHLC зі свічок 1 хв'):
    need('hdfs_data', 'CSV не в HDFS', 'CSV is not in HDFS')
    t, cnt, outp = run_streaming('mr_daily_ohlc', 'mapper_ohlc.py', 'reducer_ohlc.py')
    print('Лічильники / counters:', cnt)
    txt = sh(f'hdfs dfs -cat {outp}/part-*', echo=False, quiet=True, tail=0).out
    MR_DAILY = pd.read_csv(io.StringIO(txt), sep='\t', header=None, names=['day', 'open', 'high', 'low', 'close', 'volume', 'candles'],
                           float_precision='round_trip').sort_values('day').reset_index(drop=True)
    shot(f'hdfs dfs -cat {outp}/part-* | head', '\n'.join(txt.split('\n')[:12]), 'mr_daily_ohlc_result')
    check(MR_DAILY['candles'].sum() == N_ROWS_RAW, 'Сума свічок у добових OHLC != рядків CSV', 'Daily OHLC candle total differs from CSV rows')
    check((MR_DAILY.high >= MR_DAILY[['open', 'close', 'low']].max(axis=1)).all() and (MR_DAILY.low <= MR_DAILY[['open', 'close', 'high']].min(axis=1)).all(),
          'Некоректні OHLC (High<max(O,C) або Low>min(O,C))', 'Invalid OHLC (High<max(O,C) or Low>min(O,C))')
    save_csv(MR_DAILY, 'mr_daily_ohlc_full.csv')
    save_csv(pd.concat([MR_DAILY.head(15), MR_DAILY.tail(15)]), 'mr_daily_ohlc_sample.csv')
    save_csv(pd.DataFrame([dict(daily_rows=len(MR_DAILY), first_day=MR_DAILY.day.min(), last_day=MR_DAILY.day.max(),
                                candles_total=int(MR_DAILY.candles.sum()))]), 'mr_daily_ohlc_rowcount.csv')
    TIMINGS.append(dict(tool='Hadoop Streaming', task='daily_ohlc', seconds=round(t, 1), note='in-mapper combining, 1 reducer'))
    print(MR_DAILY.head(5).to_string(index=False))
HIVE_META = Path(RT + '/hive_meta')

with step('hive_install', f'Hive {HIVE_VER} + Derby (embedded metastore)'):
    need('hdfs_up', 'HDFS не запущено', 'HDFS is not running')
    HIVE_META.mkdir(exist_ok=True)
    if not Path(HIVE_HOME, 'bin/hive').exists():
        tgz = Path(f'/content/hive-{HIVE_VER}.tar.gz')
        download([f'https://archive.apache.org/dist/hive/hive-{HIVE_VER}/apache-hive-{HIVE_VER}-bin.tar.gz',
                  f'https://dlcdn.apache.org/hive/hive-{HIVE_VER}/apache-hive-{HIVE_VER}-bin.tar.gz'], tgz)
        sh(f'tar -xzf {tgz} -C /opt && rm -rf {HIVE_HOME} && mv /opt/apache-hive-{HIVE_VER}-bin {HIVE_HOME} && rm -f {tgz}')
    # відомий конфлікт guava: Hive 3.1.3 постачається з guava-19, Hadoop 3.3 потребує guava-27+
    h_guava = glob.glob(f'{HADOOP_HOME}/share/hadoop/common/lib/guava-[0-9]*.jar')
    check(h_guava, 'guava не знайдено в Hadoop', 'guava jar not found in Hadoop')
    for g in glob.glob(f'{HIVE_HOME}/lib/guava-[0-9]*.jar'):
        os.remove(g)
    shutil.copy(h_guava[0], f'{HIVE_HOME}/lib/')
    print('guava замінено на / guava replaced with:', Path(h_guava[0]).name)
    write_xml(Path(HIVE_HOME, 'conf/hive-site.xml'), {
        'javax.jdo.option.ConnectionURL': f'jdbc:derby:;databaseName={HIVE_META}/metastore_db;create=true',
        'javax.jdo.option.ConnectionDriverName': 'org.apache.derby.jdbc.EmbeddedDriver',
        'hive.metastore.warehouse.dir': '/user/hive/warehouse',
        'hive.metastore.schema.verification': 'false',
        'datanucleus.schema.autoCreateAll': 'false',
        'hive.execution.engine': 'mr',
        'hive.exec.scratchdir': '/tmp/hive',
        'hive.exec.local.scratchdir': f'{HIVE_META}/scratch',
        'hive.downloaded.resources.dir': f'{HIVE_META}/resources',
        'hive.querylog.location': f'{HIVE_META}/querylog',
        'hive.server2.logging.operation.log.location': f'{HIVE_META}/oplog',
        'hive.exec.mode.local.auto': 'false'})
    if not (HIVE_META / 'metastore_db').exists():
        sh('schematool -dbType derby -initSchema', log='07_hive_install.log', cwd=str(HIVE_META), quiet=True, tail=8)
    r = sh('hive --version 2>/dev/null | head -n 3', log='07_hive_install.log', cwd=str(HIVE_META), check_rc=False)
    check('Hive' in r.out, 'Hive не запускається (перевірте guava / schematool у logs/07_hive_install.log)',
          'Hive does not start (check guava / schematool in logs/07_hive_install.log)')
    STATE['hive_ok'] = True

def hive_f(fname, tag, timeout=1800):
    """Запускає .hql файл: stdout (результат) повертається, stderr (прогрес MR) іде в лог."""
    errlog = LOGS / f'08_hive_{tag}.err.log'
    r = sh(f'hive -S -f {CODE}/{fname}', log=f'08_hive_{tag}.log', err_to=str(errlog), cwd=str(HIVE_META),
           timeout=timeout, check_rc=False, quiet=True, tail=0)
    if r.rc != 0:
        print('--- хвіст stderr Hive / Hive stderr tail ---')
        print(sh(f'tail -n 25 {errlog}', echo=False, check_rc=False).out)
    return r

def hive_df(out):
    return pd.read_csv(io.StringIO(out), sep='\t', float_precision='round_trip')

with step('hive_table', 'Hive: CREATE EXTERNAL TABLE над CSV у HDFS + вибірка'):
    need('hive_ok', 'Hive не встановлено', 'Hive is not installed')
    need('hdfs_data', 'CSV не в HDFS', 'CSV is not in HDFS')
    r = hive_f('hive_01_create_table.hql', 'create_table')
    check(r.rc == 0, 'CREATE TABLE у Hive завершився помилкою', 'Hive CREATE TABLE failed')
    shot('hive -f hive_01_create_table.hql', r.out, 'hive_create_table')
    r = hive_f('hive_02_select.hql', 'select')
    check(r.rc == 0, 'SELECT у Hive завершився помилкою', 'Hive SELECT failed')
    shot('hive -f hive_02_select.hql', r.out, 'hive_select')
    print(r.out)
    cnt = int(r.out.strip().split('\n')[-1].strip())
    HIVE_COUNT = cnt
    check(cnt == N_ROWS_RAW, f'Hive COUNT(*)={cnt} != рядків у CSV {N_ROWS_RAW} (skip.header?)',
          'Hive COUNT(*) differs from CSV data rows')
    save_csv(pd.DataFrame([dict(hive_count=cnt, csv_rows=N_ROWS_RAW, equal=cnt == N_ROWS_RAW)]), 'hive_count_check.csv')
with step('hive_queries', 'Hive: GROUP BY рік / рік+місяць'):
    need('hive_ok', 'Hive не встановлено', 'Hive is not installed')
    r = hive_f('hive_03a_yearly.hql', 'yearly')
    check(r.rc == 0, 'Запит за роками у Hive завершився помилкою', 'Hive yearly query failed')
    shot('hive -f hive_03a_yearly.hql', r.out, 'hive_yearly')
    HIVE_YEAR = hive_df(r.out)
    hive_year_secs = r.secs
    print(HIVE_YEAR.to_string(index=False))
    check(int(HIVE_YEAR.candles.sum()) == N_ROWS_RAW, 'Hive: сума свічок за роками != рядків CSV', 'Hive yearly candle total differs from CSV rows')
    save_csv(HIVE_YEAR, 'hive_yearly_stats.csv')
    TIMINGS.append(dict(tool='Hive (MR engine)', task='yearly', seconds=round(hive_year_secs, 1), note='hive -f, включає старт JVM/компіляцію'))
    r = hive_f('hive_03b_monthly.hql', 'monthly')
    check(r.rc == 0, 'Запит за рік+місяць у Hive завершився помилкою', 'Hive year-month query failed')
    HIVE_MONTH = hive_df(r.out)
    shot('hive -f hive_03b_monthly.hql', r.out, 'hive_monthly')
    check(int(HIVE_MONTH.candles.sum()) == N_ROWS_RAW, 'Hive: сума свічок за місяцями != рядків CSV', 'Hive monthly candle total differs from CSV rows')
    save_csv(HIVE_MONTH, 'hive_monthly_stats.csv')
    print('місяців / months:', len(HIVE_MONTH))
    # звірка Hive vs MapReduce (за роками)
    if 'MR_YEAR' in globals():
        m = HIVE_YEAR.merge(MR_YEAR, left_on='yr', right_on='year')
        ok = len(m) == len(MR_YEAR) == len(HIVE_YEAR) and (m.candles_x == m.candles_y).all() and np.allclose(m.total_volume, m.volume, rtol=1e-9)
        save_csv(pd.DataFrame([dict(check='hive_yearly_vs_mapreduce', rows_hive=len(HIVE_YEAR), rows_mr=len(MR_YEAR), match=bool(ok))]),
                 'crosscheck_hive_vs_mr_yearly.csv')
        check(ok, 'Hive та MapReduce дають різні річні підсумки', 'Hive and MapReduce yearly results differ')

with step('hive_daily', 'Hive: добова таблиця OHLC, топ-10 діб за обсягом і за High-Low'):
    need('hive_ok', 'Hive не встановлено', 'Hive is not installed')
    r = hive_f('hive_04_daily_struct.hql', 'daily_struct')
    hive_daily_secs = r.secs
    if r.rc != 0:
        print('Варіант зі struct не спрацював, запасний варіант з вікнами / struct variant failed, using window functions')
        r = hive_f('hive_04_daily_window.hql', 'daily_window')
        hive_daily_secs += r.secs
    check(r.rc == 0, 'Створення добової таблиці Hive завершилось помилкою', 'Hive daily table creation failed')
    TIMINGS.append(dict(tool='Hive (MR engine)', task='daily_ohlc', seconds=round(hive_daily_secs, 1), note='CTAS daily_ohlc'))
    r = hive_f('hive_06_daily_count.hql', 'daily_count')
    check(r.rc == 0, 'Запит до daily_ohlc не виконано', 'Query on daily_ohlc failed')
    shot('hive -f hive_06_daily_count.hql', r.out, 'hive_daily_table')
    print(r.out)
    r = hive_f('hive_05a_top_volume.hql', 'top_volume')
    check(r.rc == 0, 'Топ-10 за обсягом: помилка', 'Top-10 by volume failed')
    shot('hive -f hive_05a_top_volume.hql', r.out, 'hive_top10_volume')
    HIVE_TOPV = hive_df(r.out); print(HIVE_TOPV.to_string(index=False))
    save_csv(HIVE_TOPV, 'hive_top10_volume_days.csv')
    r = hive_f('hive_05b_top_range.hql', 'top_range')
    check(r.rc == 0, 'Топ-10 за High-Low: помилка', 'Top-10 by High-Low failed')
    # CREATE VIEW у Hive при print.header=true виводить зайвий рядок заголовка подання; лишаємо тільки результат SELECT
    out_r = r.out[r.out.index('trade_date\thigh'):]
    shot('hive -f hive_05b_top_range.hql', out_r, 'hive_top10_range')
    HIVE_TOPR = hive_df(out_r); print(HIVE_TOPR.to_string(index=False))
    save_csv(HIVE_TOPR, 'hive_top10_range_days.csv')
    # вміст добової таблиці (роздільник Hive за замовчуванням \001) і звірка з MapReduce
    txt = sh('hdfs dfs -cat /user/hive/warehouse/btc.db/daily_ohlc/*', echo=False, quiet=True, tail=0).out
    HIVE_DAILY = pd.read_csv(io.StringIO(txt), sep='\x01', header=None, float_precision='round_trip',
                             names=['day', 'open', 'high', 'low', 'close', 'volume', 'candles']).sort_values('day').reset_index(drop=True)
    save_csv(pd.DataFrame([dict(hive_daily_rows=len(HIVE_DAILY))]), 'hive_daily_rowcount.csv')
    if 'MR_DAILY' in globals():
        m = HIVE_DAILY.merge(MR_DAILY, on='day', suffixes=('_h', '_m'))
        ok = len(m) == len(HIVE_DAILY) == len(MR_DAILY) and (m.candles_h == m.candles_m).all() and np.allclose(m.volume_h, m.volume_m, rtol=1e-9)
        for k in ('open', 'high', 'low', 'close'):
            ok = ok and np.allclose(m[k + '_h'], m[k + '_m'], rtol=1e-12)
        save_csv(pd.DataFrame([dict(check='hive_daily_vs_mapreduce_daily', rows_hive=len(HIVE_DAILY), rows_mr=len(MR_DAILY), match=bool(ok))]),
                 'crosscheck_hive_vs_mr_daily.csv')
        check(ok, 'Добові OHLC у Hive і MapReduce відрізняються', 'Hive and MapReduce daily OHLC differ')
with step('pandas', 'Pandas: ті самі агрегації + звірка з MapReduce/Hive'):
    need('csv', 'Немає CSV', 'No CSV')
    t0 = time.time()
    df = pd.read_csv(CSV, float_precision='round_trip').dropna()
    df['Timestamp'] = df['Timestamp'].astype('int64')
    t_read = time.time() - t0
    check(len(df) == N_ROWS_RAW, f'Pandas прочитав {len(df)} рядків, а в CSV {N_ROWS_RAW}', f'Pandas row count {len(df)} differs from CSV {N_ROWS_RAW}')
    t0 = time.time()
    years = pd.to_datetime(df['Timestamp'], unit='s').dt.year
    PD_YEAR = df.groupby(years.rename('year')).agg(candles=('Volume', 'size'), volume=('Volume', 'sum')).reset_index()
    t_year = time.time() - t0
    t0 = time.time()
    d = df.sort_values('Timestamp', kind='stable')
    day = (d['Timestamp'] // 86400).rename('day_n')
    PD_DAILY = d.groupby(day).agg(open=('Open', 'first'), high=('High', 'max'), low=('Low', 'min'), close=('Close', 'last'),
                                  volume=('Volume', 'sum'), candles=('Volume', 'size')).reset_index()
    PD_DAILY['day'] = pd.to_datetime(PD_DAILY['day_n'] * 86400, unit='s').dt.strftime('%Y-%m-%d')
    PD_DAILY = PD_DAILY[['day', 'open', 'high', 'low', 'close', 'volume', 'candles']]
    t_daily = time.time() - t0
    PD_T = dict(read=t_read, year=t_year, daily=t_daily)
    print(f'Pandas: читання {t_read:.1f} с, річна агрегація {t_year:.1f} с, добові OHLC {t_daily:.1f} с')
    TIMINGS.append(dict(tool='Pandas', task='read_csv', seconds=round(t_read, 1), note='read_csv+dropna'))
    TIMINGS.append(dict(tool='Pandas', task='yearly', seconds=round(t_year, 1), note='groupby (без читання)'))
    TIMINGS.append(dict(tool='Pandas', task='daily_ohlc', seconds=round(t_daily, 1), note='sort+groupby (без читання)'))
    save_csv(PD_YEAR, 'pandas_yearly_stats.csv')
    save_csv(PD_DAILY, 'pandas_daily_ohlc_full.csv')
    PD_DAILY.assign(price_range=PD_DAILY.high - PD_DAILY.low).nlargest(10, 'volume').pipe(lambda x: save_csv(x, 'pandas_top10_volume_days.csv'))
    PD_DAILY.assign(price_range=PD_DAILY.high - PD_DAILY.low).nlargest(10, 'price_range').pipe(lambda x: save_csv(x, 'pandas_top10_range_days.csv'))
    shot('python: pandas yearly (count, volume)', PD_YEAR.to_string(index=False), 'pandas_yearly')

    rows, problems = [], []
    def cmp(name, a, b, ok, extra=''):
        rows.append(dict(check=name, left_rows=len(a), right_rows=len(b), match=bool(ok), note=extra))
        if not ok: problems.append(name)
    if 'MR_YEAR' in globals():
        m = MR_YEAR.merge(PD_YEAR, on='year', suffixes=('_mr', '_pd'))
        rel = float(np.max(np.abs(m.volume_mr - m.volume_pd) / np.maximum(1e-12, np.abs(m.volume_pd))))
        cmp('mapreduce_vs_pandas_yearly', MR_YEAR, PD_YEAR, len(m) == len(MR_YEAR) == len(PD_YEAR) and (m.candles_mr == m.candles_pd).all() and rel < 1e-9, f'max_rel_diff_volume={rel:.2e}')
    if 'MR_DAILY' in globals():
        m = MR_DAILY.merge(PD_DAILY, on='day', suffixes=('_mr', '_pd'))
        ok = len(m) == len(MR_DAILY) == len(PD_DAILY) and (m.candles_mr == m.candles_pd).all()
        bad = {k: int((~np.isclose(m[k + '_mr'], m[k + '_pd'], rtol=1e-12, atol=0)).sum()) for k in ('open', 'high', 'low', 'close')}
        relv = float(np.max(np.abs(m.volume_mr - m.volume_pd) / np.maximum(1e-12, np.abs(m.volume_pd))))
        cmp('mapreduce_vs_pandas_daily_ohlc', MR_DAILY, PD_DAILY, ok and sum(bad.values()) == 0 and relv < 1e-9, f'mismatch_OHLC={bad}; max_rel_diff_volume={relv:.2e}')
    if 'HIVE_YEAR' in globals():
        m = HIVE_YEAR.merge(PD_YEAR, left_on='yr', right_on='year')
        cmp('hive_vs_pandas_yearly', HIVE_YEAR, PD_YEAR, len(m) == len(PD_YEAR) and (m.candles_x == m.candles_y).all() and np.allclose(m.total_volume, m.volume, rtol=1e-9))
    if 'HIVE_DAILY' in globals():
        cmp('hive_vs_pandas_daily_rows', HIVE_DAILY, PD_DAILY, len(HIVE_DAILY) == len(PD_DAILY))
    if 'HIVE_TOPV' in globals():
        pv = set(PD_DAILY.nlargest(10, 'volume').day); hv = set(HIVE_TOPV.trade_date.astype(str))
        cmp('hive_vs_pandas_top10_volume_days', HIVE_TOPV, PD_DAILY.nlargest(10, 'volume'), pv == hv)
    if 'HIVE_TOPR' in globals():
        pr = set(PD_DAILY.assign(r=PD_DAILY.high - PD_DAILY.low).nlargest(10, 'r').day); hr = set(HIVE_TOPR.trade_date.astype(str))
        cmp('hive_vs_pandas_top10_range_days', HIVE_TOPR, PD_DAILY, pr == hr)
    CROSS = pd.DataFrame(rows)
    save_csv(CROSS, 'crosscheck_mr_hive_vs_pandas.csv')
    print(CROSS.to_string(index=False))
    check(CROSS.shape[0] > 0 and not problems, f'Розбіжність результатів: {problems} (рядків: Hadoop/Hive vs Pandas)',
          f'Result mismatch between Hadoop/Hive and Pandas: {problems}')
    del df, d
with step('save', 'versions.txt, логи, runtimes.csv'):
    import pandas as pd
    def first(cmd):
        return sh(cmd, echo=False, check_rc=False).out.strip().split('\n')[0]
    ver = {
        'Java': first('java -version 2>&1'), 'Hadoop': first('hadoop version'), 'Hive': first('cd '+str(HIVE_META)+' && hive --version 2>/dev/null | head -n 1'),
        'Python': sys.version.split()[0], 'pandas': pd.__version__, 'numpy': np.__version__,
        'pyspark': first('pip show pyspark 2>/dev/null | grep -i ^Version'), 'Hadoop JAVA_HOME': JAVA_HOME, 'OS': platform.platform(),
        'CPU': first("lscpu | grep 'Model name' | sed 's/.*: *//'"), 'nproc': first('nproc'),
        'RAM': first("free -h | awk '/Mem/{print $2}'"), 'Disk': first("df -h / | tail -n 1"),
        'Date UTC': datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
    }
    lines = [f'{k}: {v}' for k, v in ver.items()]
    lines += ['', '--- nproc / free -h ---', sh('nproc; free -h', echo=False).out]
    (OUT / 'versions.txt').write_text('\n'.join(lines))
    save_csv(pd.DataFrame(list(ver.items()), columns=['component', 'version']), 'env_versions.csv')
    print('\n'.join(lines))
    for f in glob.glob(f'{HADOOP_HOME}/logs/*.log'):
        shutil.copy(f, LOGS / ('daemon_' + Path(f).name))
    for f in glob.glob(f'{HIVE_META}/*.log') + glob.glob(f'{HIVE_META}/querylog/*'):
        shutil.copy(f, LOGS / ('hive_' + Path(f).name))
TASKS = [t for t in TASKS if t[0] not in ('spark','timing','ui_shots')]
banner('ПІДСУМКОВИЙ ЧЕКЛІСТ / FINAL CHECKLIST')
lines = []
for k, title in TASKS:
    st = CHECK.get(k, 'FAIL: не виконано')
    mark = 'OK  ' if st == 'OK' else 'FAIL'
    lines.append(f'[{mark}] {title}' + ('' if st == 'OK' else f'  <- {st[6:]}'))
n_ok = sum(1 for k, _ in TASKS if CHECK.get(k) == 'OK')
lines.append('')
lines.append(f'Разом: {n_ok} з {len(TASKS)} OK. Результати: {OUT}')
lines.append('Скріншоти: ' + str(len(list(SHOTS.glob('*.png')))) + ' файлів; CSV: ' + str(len(list(RES.glob('*.csv')))))
summary = '\n'.join(lines)
print(summary)
(OUT / 'summary.txt').write_text(summary)
sync()
print('\nРезультати збережено у', OUT)