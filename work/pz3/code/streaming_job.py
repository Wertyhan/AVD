# -*- coding: utf-8 -*-
"""Spark Structured Streaming: Kafka -> розбір JSON -> віконна агрегація -> аномалії -> parquet (ПЗ3, завдання 3-7)."""
import argparse, glob, json, os, sys, time
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession, functions as F, types as T

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from anomaly_rule import StreamScorer, K_SEL, KV_SEL

ap = argparse.ArgumentParser()
ap.add_argument("--bootstrap", default="localhost:9092")
ap.add_argument("--topic", default="bigdata-topic")
ap.add_argument("--out", required=True)
ap.add_argument("--ckpt", required=True)
ap.add_argument("--done-flag", required=True)
ap.add_argument("--kafka-pkg", required=True)
ap.add_argument("--watermark", default="10 minutes")
ap.add_argument("--trigger-seconds", type=int, default=10)
ap.add_argument("--max-offsets", type=int, default=3000)
ap.add_argument("--k", type=float, default=K_SEL)
ap.add_argument("--kv", type=float, default=KV_SEL)
ap.add_argument("--max-seconds", type=int, default=1200)
a = ap.parse_args()

os.makedirs(a.out, exist_ok=True)
PARTS = os.path.join(a.out, "scored_parts"); os.makedirs(PARTS, exist_ok=True)

spark = (SparkSession.builder.appName("PZ3-bitcoin-streaming").master("local[2]")
         .config("spark.jars.packages", a.kafka_pkg)
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.sql.session.timeZone", "UTC")     # вікна вирівнюються по UTC
         .config("spark.driver.memory", "2g")
         .config("spark.ui.port", "4040")
         .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
print("[job] Spark UI:", spark.sparkContext.uiWebUrl, flush=True)
print("[job] Spark", spark.version, "| пакет", a.kafka_pkg, flush=True)

# ---------- Завдання 3: читання з Kafka ----------
raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", a.bootstrap)
       .option("subscribe", a.topic)
       .option("startingOffsets", "earliest")
       .option("maxOffsetsPerTrigger", a.max_offsets)   # обмеження розміру мікробатчу
       .option("failOnDataLoss", "false")
       .load())

# ---------- Завдання 4: декодування, типи, фільтрація ----------
schema = T.StructType([                       # явна схема JSON-повідомлення
    T.StructField("ts", T.LongType()), T.StructField("open", T.DoubleType()),
    T.StructField("high", T.DoubleType()), T.StructField("low", T.DoubleType()),
    T.StructField("close", T.DoubleType()), T.StructField("volume", T.DoubleType())])
parsed = (raw.select(F.col("key").cast("string").alias("key"),                # bytes -> string
                     F.col("value").cast("string").alias("json"),
                     F.col("timestamp").cast("double").alias("kafka_epoch"),  # час запису в Kafka (CreateTime)
                     F.col("offset").alias("kafka_offset"))
          .withColumn("m", F.from_json("json", schema))                       # string -> struct
          .select("key", "kafka_epoch", "kafka_offset", "m.*")
          .withColumn("event_time", F.col("ts").cast("timestamp"))            # ЧАС ПОДІЇ (unix с), а не час обробки
          .withColumn("valid", F.col("ts").isNotNull() & F.col("open").isNotNull() & F.col("close").isNotNull()
                      & F.col("high").isNotNull() & F.col("low").isNotNull() & F.col("volume").isNotNull()
                      & (F.col("high") >= F.col("low")) & (F.col("close") > 0) & (F.col("volume") >= 0)))
valid = parsed.filter("valid")                                                # фільтрація некоректних записів

# watermark: допустиме запізнення подій 10 хв (докладніше у ноутбуці)
events = valid.withWatermark("event_time", a.watermark)

# ---------- Завдання 5: віконні агрегації (tumbling) ----------
def win_agg(df, dur):
    g = df.groupBy(F.window("event_time", dur)).agg(
        F.min(F.struct("event_time", "open")).getField("open").alias("open"),    # open = значення з найранішим event_time
        F.max("high").alias("high"), F.min("low").alias("low"),
        F.max(F.struct("event_time", "close")).getField("close").alias("close"), # close = значення з найпізнішим event_time
        F.sum("volume").alias("volume"), F.count(F.lit(1)).alias("n_candles"))
    return g.select(F.col("window.start").alias("win_start"), F.col("window.end").alias("win_end"),
                    "open", "high", "low", "close", "volume", "n_candles")

w15, w1h = win_agg(events, "15 minutes"), win_agg(events, "1 hour")
# групування + статистичне агрегування по календарній добі (без вікна)
daily = (events.groupBy(F.to_date("event_time").alias("trade_date"))
         .agg(F.count(F.lit(1)).alias("n"), F.avg("close").alias("avg_close"), F.stddev("close").alias("std_close"),
              F.min("low").alias("min_low"), F.max("high").alias("max_high"), F.sum("volume").alias("volume")))

trig = {"processingTime": f"{a.trigger_seconds} seconds"}
Q = {}
# ---------- Завдання 7: parquet-приймачі (append: вікно записується після закриття водяною позначкою) ----------
for name, df in (("win15m", w15), ("win1h", w1h)):
    Q[f"sink_{name}"] = (df.writeStream.queryName(f"sink_{name}").outputMode("append").format("parquet")
                         .option("path", os.path.join(a.out, "parquet", name))
                         .option("checkpointLocation", os.path.join(a.ckpt, name)).trigger(**trig).start())
# memory-приймачі (update): повний поточний стан вікон для перевірки та скріншотів
Q["mem_win15m"] = w15.writeStream.queryName("win15m_mem").outputMode("update").format("memory").trigger(**trig).start()
Q["mem_win1h"] = w1h.writeStream.queryName("win1h_mem").outputMode("update").format("memory").trigger(**trig).start()
Q["mem_daily"] = daily.writeStream.queryName("daily_mem").outputMode("update").format("memory").trigger(**trig).start()
# console-приймач для демонстрації (вивід у лог)
Q["console_1h"] = (w1h.writeStream.queryName("console_1h").outputMode("update").format("console")
                   .option("truncate", "false").option("numRows", 6).trigger(**trig).start())

# ---------- Завдання 6: аномалії у foreachBatch зі станом (буфер останніх 61 хвилини) ----------
scorer = StreamScorer(k=a.k, kv=a.kv)
BATCH_LOG = []
def process_batch(bdf, batch_id):
    t0 = time.time()
    pdf = bdf.select("ts", "close", "volume", "valid", "kafka_epoch").toPandas()
    n_raw, n_valid = len(pdf), int(pdf["valid"].sum())
    lat = (time.time() - pdf["kafka_epoch"]).to_numpy() if n_raw else np.array([np.nan])
    res = scorer.update(pdf[pdf["valid"]]) if n_valid else pd.DataFrame()
    n_anom = 0
    if len(res):
        res = res.copy()
        res["batch_id"] = batch_id
        res.to_parquet(os.path.join(PARTS, f"part-{batch_id:05d}.parquet"), index=False)  # ідемпотентний запис
        n_anom = int(res["anomaly"].sum())
    rec = {"batch_id": batch_id, "n_raw": n_raw, "n_invalid": n_raw - n_valid, "n_valid": n_valid,
           "n_anomalies": n_anom, "latency_mean_s": float(np.nanmean(lat)),
           "latency_p95_s": float(np.nanpercentile(lat, 95)), "latency_max_s": float(np.nanmax(lat)),
           "foreachbatch_s": round(time.time() - t0, 3)}
    BATCH_LOG.append(rec)
    print(f"[anomaly] batch {batch_id}: рядків {n_raw}, аномалій {n_anom}, "
          f"затримка середня {rec['latency_mean_s']:.1f} с", flush=True)

Q["anomaly"] = (parsed.writeStream.queryName("anomaly").foreachBatch(process_batch)
                .option("checkpointLocation", os.path.join(a.ckpt, "anomaly")).trigger(**trig).start())
print("STREAMING_STARTED", [q.name for q in Q.values()], flush=True)

# ---------- моніторинг: збір прогресу, умова зупинки ----------
PROG = {}
def collect():
    for key, q in Q.items():
        for p in q.recentProgress:
            PROG[(key, p["batchId"])] = {
                "query": key, "batch_id": p["batchId"], "timestamp": p["timestamp"],
                "num_input_rows": p["numInputRows"], "input_rows_per_s": p.get("inputRowsPerSecond"),
                "processed_rows_per_s": p.get("processedRowsPerSecond"),
                "trigger_execution_ms": p["durationMs"].get("triggerExecution"),
                "add_batch_ms": p["durationMs"].get("addBatch"),
                "get_batch_ms": p["durationMs"].get("getBatch"),
                "watermark": (p.get("eventTime") or {}).get("watermark")}
def totals():
    t = {k: 0 for k in Q}
    for (k, _), v in PROG.items():
        t[k] += v["num_input_rows"] or 0
    return t

t_start, status, expected = time.time(), "timeout", None
while time.time() - t_start < a.max_seconds:
    time.sleep(5)
    collect()
    for k, q in Q.items():
        if q.exception():
            print("ПОМИЛКА у запиті", k, q.exception(), flush=True); status = "query_failed"
    if status == "query_failed":
        break
    if expected is None and os.path.exists(a.done_flag):
        try:
            expected = json.load(open(a.done_flag))["sent"]
        except Exception:
            expected = None
    tt = totals()
    print(f"[job] t={time.time()-t_start:.0f}с прочитано(anomaly)={tt['anomaly']} очікується={expected}", flush=True)
    if expected is not None and all(v >= expected for v in tt.values()):
        time.sleep(2 * a.trigger_seconds)   # дати завершитися останнім мікробатчам
        collect(); status = "ok"
        break

# ---------- збереження результатів ----------
summary = {"status": status, "expected_messages": expected, "input_totals": totals(),
           "seconds": round(time.time() - t_start, 1)}
if status in ("ok", "timeout"):
    for tbl, fn in (("win15m_mem", "win15m_all"), ("win1h_mem", "win1h_all"), ("daily_mem", "daily_stats")):
        d = spark.table(tbl).toPandas()
        d = d.sort_values(d.columns[0]).reset_index(drop=True)
        d.to_csv(os.path.join(a.out, fn + ".csv"), index=False)
        d.to_parquet(os.path.join(a.out, fn + ".parquet"), index=False)
        summary[fn + "_rows"] = len(d)
        print(f"[job] {tbl}: {len(d)} рядків -> {fn}.csv", flush=True)
        if tbl == "win1h_mem":
            print(d.head(8).to_string(index=False), flush=True)
pd.DataFrame(list(PROG.values())).sort_values(["query", "batch_id"]).to_csv(os.path.join(a.out, "progress.csv"), index=False)
pd.DataFrame(BATCH_LOG).to_csv(os.path.join(a.out, "batch_stats.csv"), index=False)
parts = sorted(glob.glob(os.path.join(PARTS, "part-*.parquet")))
if parts:
    sc = pd.concat([pd.read_parquet(p) for p in parts], ignore_index=True).sort_values("ts").reset_index(drop=True)
    sc["datetime_utc"] = pd.to_datetime(sc["ts"], unit="s", utc=True).dt.strftime("%Y-%m-%d %H:%M:%S")
    sc.to_parquet(os.path.join(a.out, "scored.parquet"), index=False)
    sc.to_csv(os.path.join(a.out, "scored.csv"), index=False)
    an = sc[sc["anomaly"]]
    an.to_parquet(os.path.join(a.out, "anomalies.parquet"), index=False)
    an.to_csv(os.path.join(a.out, "anomalies.csv"), index=False)
    summary.update(scored_rows=len(sc), anomalies_rows=len(an), k=a.k, kv=a.kv)
    print(f"[job] scored={len(sc)} anomalies={len(an)}", flush=True)
json.dump(summary, open(os.path.join(a.out, "job_summary.json"), "w"), indent=1)
for q in Q.values():
    try: q.stop()
    except Exception: pass
spark.stop()
print("JOB_DONE", json.dumps(summary), flush=True)
sys.exit(0 if status == "ok" else 3)
