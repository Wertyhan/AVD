# -*- coding: utf-8 -*-
"""rerender_pz1.py - перерендер усіх термінальних скріншотів ПЗ1 спільним рендером term_png.
Вивід беретьcя з ОСТАННЬОГО блоку відповідної команди в logs/*.log (поточний запуск) або з повторного
виконання read-only команд (hdfs dfs -ls/du/cat, yarn ..., python3 code/pandas_yearly.py). Імена файлів збережено."""
import os, re, subprocess, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from termshot import render_terminal_png
W = Path('/home/user/AVD/work'); P = W / 'pz1'; SH = P / 'screenshots'; LOG = P / 'logs'
HADOOP = W / 'tools/hadoop-3.3.6'; JDK8 = W / 'mamba/envs/jdk8'
os.environ.pop('JAVA_TOOL_OPTIONS', None)
os.environ.update(JAVA_HOME=str(JDK8), HADOOP_HOME=str(HADOOP), PATH=f'{HADOOP}/bin:{JDK8}/bin:' + os.environ['PATH'])

def last_block(log, prefix):
    txt = (LOG / log).read_text(errors='replace')
    blocks = re.split(r'\n(?=\$ )', '\n' + txt)
    hit = [b for b in blocks if b.startswith('$ ' + prefix)]
    first, _, body = hit[-1].partition('\n')
    return first[2:], body.strip('\n')

def live(cmd, cwd=None):
    return subprocess.run(cmd, shell=True, executable='/bin/bash', capture_output=True, text=True, cwd=cwd).stdout

GREP19 = "grep -vE '^(OwnerType|Owner|CreateTime|LastAccessTime|Retention):|bucketing|transient'"
HQL = lambda n: f'hive -S -f {n}'
def trim_top_range(o):
    return o[o.index('trade_date\thigh'):]
# (png, відображувана команда або None=з логу, джерело, примітка)
CMD27 = "yarn application -list -appStates ALL 2>/dev/null | tr '\\n\\t' '  ' | grep -oE 'application_[0-9]+_[0-9]+|(FINISHED|FAILED|KILLED) +(SUCCEEDED|FAILED|KILLED)' | tr -s ' ' | paste - -"
J = [
 ('01_jps_daemons', 'jps', ('02_daemons.log', 'jps'), None),
 ('02_yarn_node_list', 'yarn node -list', ('02_daemons.log', 'yarn node -list'), None),
 ('03_hdfs_dfsadmin_report', None, ('03_arch.log', 'hdfs dfsadmin -report'), None),
 ('04_hdfs_getconf', None, ('03_arch.log', 'for k in'), None),
 ('05_yarn_nodes', None, ('03_arch.log', 'yarn node -list -all'), None),
 ('06_hdfs_mkdir_ls', 'hdfs dfs -mkdir -p /user/root/btc/raw /user/root/btc/wc_in /user/root/btc/out && hdfs dfs -ls -R /user/root | awk \'{print $1, $5, $8}\'', 'LIVE:hdfs dfs -mkdir -p /user/root/btc/raw /user/root/btc/wc_in /user/root/btc/out && hdfs dfs -ls -R /user/root | awk \'{print $1, $5, $8}\'', 'права, розмір, шлях (поточний стан HDFS)'),
 ('07_hdfs_ls_raw', None, ('04_hdfs.log', 'hdfs dfs -ls -h'), None),
 ('08_hdfs_du', None, ('04_hdfs.log', 'hdfs dfs -du'), None),
 ('09_hdfs_cat_head', None, ('04_hdfs.log', 'hdfs dfs -cat'), None),
 ('10_hdfs_tail', None, ('04_hdfs.log', 'hdfs dfs -tail'), None),
 ('11_hdfs_fsck', None, ('04_hdfs.log', 'hdfs fsck'), 'початок і кінець звіту fsck'),
 ('12_wordcount_run', 'hadoop jar hadoop-mapreduce-examples-3.3.6.jar wordcount /user/root/btc/wc_in /user/root/btc/out/wordcount', ('05_wordcount.log', 'hadoop jar'), 'початок і кінець журналу задачі'),
 ('13_wordcount_ls', None, ('05_wordcount.log', 'hdfs dfs -ls'), None),
 ('14_wordcount_top20', None, ('05_wordcount.log', 'hdfs dfs -cat'), None),
 ('15_mr_yearly_job_output', 'hadoop jar hadoop-streaming-3.3.6.jar -D mapreduce.job.name=mr_yearly -files mapper_year.py,reducer_year.py -mapper \'python3 mapper_year.py\' -combiner \'python3 reducer_year.py\' -reducer \'python3 reducer_year.py\' -input /user/root/btc/raw -output /user/root/btc/out/mr_yearly', ('06_mr_yearly.log', 'hadoop jar'), 'початок і кінець журналу задачі'),
 ('16_mr_yearly_result', 'hdfs dfs -cat /user/root/btc/out/mr_yearly/part-*', 'LIVE:hdfs dfs -cat /user/root/btc/out/mr_yearly/part-*', None),
 ('17_mr_daily_ohlc_job_output', 'hadoop jar hadoop-streaming-3.3.6.jar -D mapreduce.job.name=mr_daily_ohlc -files mapper_ohlc.py,reducer_ohlc.py -mapper \'python3 mapper_ohlc.py\' -reducer \'python3 reducer_ohlc.py\' -input /user/root/btc/raw -output /user/root/btc/out/mr_daily_ohlc', ('06_mr_daily_ohlc.log', 'hadoop jar'), 'початок і кінець журналу задачі'),
 ('18_mr_daily_ohlc_result', 'hdfs dfs -cat /user/root/btc/out/mr_daily_ohlc/part-* | head -n 12', 'LIVE:hdfs dfs -cat /user/root/btc/out/mr_daily_ohlc/part-* | head -n 12', None),
 ('19_hive_create_table', "hive -S -f hive_01b_describe.hql | sed '/^# Storage Information/,$d' | " + GREP19, ('08_hive_describe.log', 'hive -S'), 'DESCRIBE FORMATTED candles (без Storage Information)'),
 ('20_hive_select', HQL('hive_02_select.hql'), ('08_hive_select.log', 'hive -S'), None),
 ('21_hive_yearly', HQL('hive_03a_yearly.hql'), ('08_hive_yearly.log', 'hive -S'), None),
 ('22_hive_monthly', HQL('hive_03b_monthly.hql'), ('08_hive_monthly.log', 'hive -S'), 'початок і кінець результату (усього 179 місяців)'),
 ('23_hive_daily_table', HQL('hive_06_daily_count.hql'), ('08_hive_daily_count.log', 'hive -S'), None),
 ('24_hive_top10_volume', HQL('hive_05a_top_volume.hql'), ('08_hive_top_volume.log', 'hive -S'), None),
 ('25_hive_top10_range', HQL('hive_05b_top_range.hql'), ('08_hive_top_range.log', 'hive -S'), None),
 ('26_pandas_yearly', 'python3 code/pandas_yearly.py', 'LIVE:python3 code/pandas_yearly.py', None),
 ('27_yarn_application_list', CMD27, 'LIVE:' + CMD27, 'Application-Id, State, Final-State'),
 ('28_jps_final', 'jps', 'LIVE:jps', None),
 ('29_hdfs_ls_outputs', 'hdfs dfs -ls -R /user/root/btc/out | grep -v "/_" | head -30', 'LIVE:hdfs dfs -ls -R /user/root/btc/out | grep -v "/_" | head -30', None),
 ('30_hdfs_ls_warehouse', 'hdfs dfs -ls /user/hive/warehouse/btc.db', 'LIVE:hdfs dfs -ls /user/hive/warehouse/btc.db', None),

 ('48_hdfs_check_ls_R', 'hdfs dfs -ls -R -h /user/root/btc/raw /user/root/btc/wc_in /user/root/btc/out/wordcount', 'LIVE:hdfs dfs -ls -R -h /user/root/btc/raw /user/root/btc/wc_in /user/root/btc/out/wordcount', None),
 ('49_hdfs_check_du_cat', 'hdfs dfs -du -h /user/root/btc/raw; hdfs dfs -cat /user/root/btc/raw/btcusd_1-min_data.csv | head -n 5', 'LIVE:hdfs dfs -du -h /user/root/btc/raw; hdfs dfs -cat /user/root/btc/raw/btcusd_1-min_data.csv | head -n 5', None),
 ('50_wordcount_cat_head', 'hdfs dfs -cat /user/root/btc/out/wordcount/part-r-00000 | head -n 15', 'LIVE:hdfs dfs -cat /user/root/btc/out/wordcount/part-r-00000 | head -n 15', None),
 ('51_hive_load_data', HQL('hive_07_load_data.hql') + ' | head -n 2', ('08_hive_load_data.log', 'hive -S'), 'CREATE TABLE candles_managed + LOAD DATA INPATH + COUNT(*)'),
 ('52_hdfs_after_load', 'hdfs dfs -ls -h /user/root/btc/raw /user/root/btc/load_src /user/hive/warehouse/btc.db/candles_managed', 'LIVE:hdfs dfs -ls -h /user/root/btc/raw /user/root/btc/load_src /user/hive/warehouse/btc.db/candles_managed', 'raw: файл на місці; load_src: порожній (LOAD перемістив файл); candles_managed: файл у сховищі'),
]
only = sys.argv[1:]
for png, shown, src, note in J:
    if only and not any(png.startswith(o) for o in only): continue
    if isinstance(src, tuple):
        cmd, out = last_block(*src)
        if src[0] == '08_hive_top_range.log': out = trim_top_range(out)
        if png.startswith('51_'):  # head -n 2 з показаної команди
            out = '\n'.join(out.split('\n')[:2])
        if png.startswith('19_'):  # той самий grep, що й у показаній команді, застосований до виводу з журналу
            out = subprocess.run(GREP19[:5] + ' ' + GREP19[5:], shell=True, input=out, capture_output=True, text=True).stdout
        if shown is None: shown = cmd
    else:
        out = live(src[5:], cwd=str(P))
    size = render_terminal_png(shown, out, SH / f'{png}.png', note=note)
    print(png, size)
