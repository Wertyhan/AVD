# -*- coding: utf-8 -*-
"""Експеримент: що буде з вікнами 15 хв, якщо тема вже повна (backlog) і всі партиції читаються одними мікробатчами
(Trigger.AvailableNow + maxOffsetsPerTrigger = 3000, watermark 10 хв, той самий запит, що в streaming_job.py).
Очікування: партиція 0 (2022-11) просуває глобальний watermark до листопада 2022, рядки партиції 1 (2020, 2021) відкидаються як запізнілі.
Результат: results/pz3_watermark_backlog_test.csv. Використання: watermark_backlog_test.py OUT_DIR"""
import json, os, sys, tempfile
import pandas as pd
from pyspark.sql import SparkSession, functions as F, types as T
R = sys.argv[1]
spark = (SparkSession.builder.appName("PZ3-watermark-backlog").master("local[2]").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0")
         .config("spark.sql.shuffle.partitions", "4").config("spark.sql.session.timezone", "UTC").config("spark.ui.enabled", "false").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
pol = spark.conf.get("spark.sql.streaming.multipleWatermarkPolicy")
schema = T.StructType([T.StructField("ts", T.LongType()), T.StructField("open", T.DoubleType()), T.StructField("high", T.DoubleType()),
                       T.StructField("low", T.DoubleType()), T.StructField("close", T.DoubleType()), T.StructField("volume", T.DoubleType())])
res = []
for label, maxoff in (("backlog, maxOffsetsPerTrigger=3000", 3000), ("backlog, без ліміту (один мікробатч)", None)):
    r = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "bigdata-topic").option("startingOffsets", "earliest")
    if maxoff: r = r.option("maxOffsetsPerTrigger", maxoff)
    df = r.load().select(F.from_json(F.col("value").cast("string"), schema).alias("m")).select("m.*").withColumn("event_time", F.col("ts").cast("timestamp"))
    w = (df.withWatermark("event_time", "10 minutes").groupBy(F.window("event_time", "15 minutes"))
         .agg(F.count(F.lit(1)).alias("n_candles")))
    name = "bk" + str(len(res))
    q = w.writeStream.queryName(name).outputMode("update").format("memory").trigger(availableNow=True).option("checkpointLocation", tempfile.mkdtemp()).start()
    q.awaitTermination(300)
    prog = q.recentProgress
    t = spark.table(name).toPandas().sort_values("window").drop_duplicates("window", keep="last")
    dropped = sum(o.get("numRowsDroppedByWatermark", 0) for p in prog for o in p.get("stateOperators", []))
    res.append({"scenario": label, "micro_batches": len(prog), "messages_in_topic": 30240, "rows_in_windows": int(t["n_candles"].sum()),
                "rows_lost": 30240 - int(t["n_candles"].sum()), "numRowsDroppedByWatermark_sum": int(dropped), "windows": len(t),
                "final_watermark": (prog[-1].get("eventTime") or {}).get("watermark"), "multipleWatermarkPolicy": pol})
    print(res[-1], flush=True)
pd.DataFrame(res).to_csv(os.path.join(R, "pz3_watermark_backlog_test.csv"), index=False)
spark.stop()
