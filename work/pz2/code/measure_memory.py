# -*- coding: utf-8 -*-
"""Пік пам'яті (RSS) для pandas і Spark на тих самих двох задачах, що й у pz2_spark.py.
Кожне вимірювання – окремий процес; psutil опитує RSS кожні 50 мс (процес + діти, для Spark – JVM драйвера)."""
import os, sys, time, subprocess, threading, psutil, csv
RAW = "/home/user/AVD/work/data/raw/btcusd_1-min_data.csv"
RES = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "results"))
WORKER = r'''
import sys, pandas as pd
RAW, eng, task = sys.argv[1:4]
if eng == "pandas":
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
    spark = (SparkSession.builder.master("local[*]").config("spark.driver.memory","8g")
             .config("spark.sql.shuffle.partitions","16").config("spark.sql.session.timeZone","UTC")
             .config("spark.ui.enabled","false").getOrCreate())
    SCHEMA = StructType([StructField("Timestamp", LongType())] + [StructField(c, DoubleType()) for c in ["Open","High","Low","Close","Volume"]])
    d = spark.read.csv(RAW, header=True, schema=SCHEMA)
    if task == "year_count_volume":
        r = d.groupBy(F.year(F.timestamp_seconds("Timestamp")).alias("year")).agg(F.count("*").alias("n"), F.sum("Volume").alias("vol")).orderBy("year").toPandas()
    else:
        d = d.withColumn("date", F.to_date(F.timestamp_seconds("Timestamp")))
        r = d.groupBy("date").agg(F.min_by("Open","Timestamp").alias("open"), F.max("High").alias("high"), F.min("Low").alias("low"),
              F.max_by("Close","Timestamp").alias("close"), F.sum("Volume").alias("volume")).orderBy("date").toPandas()
    spark.stop()
print("rows", len(r))
'''
def run(eng, task):
    p = subprocess.Popen([sys.executable, "-I", "-c", WORKER, RAW, eng, task], stdout=subprocess.PIPE, text=True)
    ps = psutil.Process(p.pid); peak = {"py": 0, "jvm": 0, "tot": 0}
    while p.poll() is None:
        try:
            py = ps.memory_info().rss; jvm = 0
            for c in ps.children(recursive=True):
                try:
                    if "java" in c.name(): jvm = max(jvm, c.memory_info().rss)
                except psutil.Error: pass
            peak["py"] = max(peak["py"], py); peak["jvm"] = max(peak["jvm"], jvm); peak["tot"] = max(peak["tot"], py + jvm)
        except psutil.Error: pass
        time.sleep(0.05)
    return peak
rows = []
for eng in ["pandas", "spark"]:
    for task in ["year_count_volume", "daily_ohlc"]:
        pk = run(eng, task); mb = lambda b: round(b / 2**20, 1)
        if eng == "pandas":
            rows.append((eng, task, mb(pk["py"]), "peak RSS of Python process (psutil, 50 ms polling)"))
        else:
            rows.append((eng, task, mb(pk["jvm"]), "peak RSS of driver JVM (java child process, psutil, 50 ms polling), local[*], driver 8g"))
            rows.append(("spark_python_client", task, mb(pk["py"]), "peak RSS of PySpark Python process (excluding JVM)"))
        print(rows[-1], flush=True)
with open(f"{RES}/pz2_memory.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f); w.writerow(["engine", "task", "peak_mb", "method"]); w.writerows(rows)
