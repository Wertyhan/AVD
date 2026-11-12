# -*- coding: utf-8 -*-
"""Пік пам'яті (RSS) для pandas і Spark на тих самих двох задачах, що й у pz2_spark.py.
Кожне вимірювання – окремий процес; psutil опитує RSS кожні 50 мс (процес + діти, для Spark – JVM драйвера).
3 раунди, у кожному раунді конфігурації йдуть у чергуванні pandas/Spark; підсумок – медіана.
Додатково: базовий рівень простої сесії Spark (spark.range(1).count()), Spark із driver 2g, pandas з read_csv(chunksize)."""
import os, sys, time, subprocess, psutil, csv, statistics
RAW = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "data", "raw", "btcusd_1-min_data.csv"))
RES = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "results"))
WORKER = r'''
import sys, pandas as pd
RAW, eng, task, mem = sys.argv[1:5]
if eng == "pandas":
    if task == "year_count_volume_chunked":
        parts = []
        for ch in pd.read_csv(RAW, chunksize=500_000):
            y = pd.to_datetime(ch["Timestamp"], unit="s").dt.year
            parts.append(ch.groupby(y).agg(n=("Volume", "size"), vol=("Volume", "sum")))
        r = pd.concat(parts).groupby(level=0).sum()
    else:
        d = pd.read_csv(RAW)
        if task == "year_count_volume":
            y = pd.to_datetime(d["Timestamp"], unit="s").dt.year
            r = d.groupby(y).agg(n=("Volume", "size"), vol=("Volume", "sum"))
        else:
            dt = pd.to_datetime(d["Timestamp"], unit="s").dt.date
            r = d.groupby(dt).agg(open=("Open","first"), high=("High","max"), low=("Low","min"), close=("Close","last"), volume=("Volume","sum"))
else:
    from pyspark.sql import SparkSession, functions as F
    from pyspark.sql.types import *
    spark = (SparkSession.builder.master("local[*]").config("spark.driver.memory", mem)
             .config("spark.sql.shuffle.partitions","16").config("spark.sql.session.timeZone","UTC")
             .config("spark.ui.enabled","false").getOrCreate())
    SCHEMA = StructType([StructField("Timestamp", LongType())] + [StructField(c, DoubleType()) for c in ["Open","High","Low","Close","Volume"]])
    if task == "idle":
        r = spark.range(1).count()
    else:
        d = spark.read.csv(RAW, header=True, schema=SCHEMA)
        if task == "year_count_volume":
            r = d.groupBy(F.year(F.timestamp_seconds("Timestamp")).alias("year")).agg(F.count("*").alias("n"), F.sum("Volume").alias("vol")).orderBy("year").toPandas()
        else:
            d = d.withColumn("date", F.to_date(F.timestamp_seconds("Timestamp")))
            r = d.groupBy("date").agg(F.min_by("Open","Timestamp").alias("open"), F.max("High").alias("high"), F.min("Low").alias("low"),
                  F.max_by("Close","Timestamp").alias("close"), F.sum("Volume").alias("volume")).orderBy("date").toPandas()
    spark.stop()
'''
def run(eng, task, mem="8g"):
    p = subprocess.Popen([sys.executable, "-I", "-c", WORKER, RAW, eng, task, mem], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    ps = psutil.Process(p.pid); peak = {"py": 0, "jvm": 0}
    while p.poll() is None:
        try:
            peak["py"] = max(peak["py"], ps.memory_info().rss); jvm = 0
            for c in ps.children(recursive=True):
                try:
                    if "java" in c.name(): jvm = max(jvm, c.memory_info().rss)
                except psutil.Error: pass
            peak["jvm"] = max(peak["jvm"], jvm)
        except psutil.Error: pass
        time.sleep(0.05)
    return peak
mb = lambda b: round(b / 2**20, 1)
CONF = [("pandas", "year_count_volume", "-"), ("spark", "year_count_volume", "8g"),
        ("pandas", "daily_ohlc", "-"), ("spark", "daily_ohlc", "8g"),
        ("pandas", "year_count_volume_chunked", "-"), ("spark", "idle", "8g"),
        ("spark", "year_count_volume", "2g"), ("spark", "daily_ohlc", "2g")]
runs = []
for rnd in (1, 2, 3):
    conf = CONF if rnd % 2 else CONF[::-1]          # порядок конфігурацій чергується між раундами
    for eng, task, mem in conf:
        pk = run(eng, task, mem if mem != "-" else "8g")
        if eng == "pandas":
            runs.append((rnd, "pandas" if task != "year_count_volume_chunked" else "pandas_chunksize500k", task, "-", mb(pk["py"])))
        else:
            runs.append((rnd, "spark_jvm", task, mem, mb(pk["jvm"]))); runs.append((rnd, "spark_python_client", task, mem, mb(pk["py"])))
        print(runs[-1], flush=True)
with open(f"{RES}/pz2_memory_runs.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f); w.writerow(["round", "engine", "task", "driver_memory", "peak_rss_mb"]); w.writerows(runs)
keys = sorted({(r[1], r[2], r[3]) for r in runs})
with open(f"{RES}/pz2_memory.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f); w.writerow(["engine", "task", "driver_memory", "peak_mb_median", "peak_mb_min", "peak_mb_max", "method"])
    for k in keys:
        v = [r[4] for r in runs if (r[1], r[2], r[3]) == k]
        w.writerow([*k, statistics.median(v), min(v), max(v), "peak RSS, psutil 50 ms polling, median of 3 separate processes; spark_jvm = java child of the driver"])
