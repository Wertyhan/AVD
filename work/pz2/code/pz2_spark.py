# -*- coding: utf-8 -*-
"""ПЗ2. Apache Spark (PySpark + Spark SQL) на хвилинних свічках BTC/USD.
Запуск: python3 pz2_spark.py   (результати -> ../results, скріншоти -> ../screenshots)
"""
import os, sys, io, time, platform, subprocess, contextlib, statistics, math
import numpy as np
import pandas as pd

SEED = 42
np.random.seed(SEED)
BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
RES, SHOTS, TXT = f"{BASE}/results", f"{BASE}/screenshots", f"{BASE}/screenshots/txt"
RAW = "/home/user/AVD/work/data/raw/btcusd_1-min_data.csv"
for d in (RES, SHOTS, TXT):
    os.makedirs(d, exist_ok=True)

from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.types import (StructType, StructField, LongType, DoubleType)
from pyspark import StorageLevel
import pyspark

# ---------- допоміжні засоби: тайминги та захоплення виводу для скріншотів ----------
TIMINGS = []
def rec(step, secs, note=""):
    TIMINGS.append((step, round(secs, 3), note))
    print(f"[timing] {step}: {secs:.2f} с {note}")

@contextlib.contextmanager
def timed(step, note=""):
    t = time.perf_counter(); yield
    rec(step, time.perf_counter() - t, note)

class Tee(io.StringIO):
    def write(self, s):
        sys.__stdout__.write(s); return super().write(s)

@contextlib.contextmanager
def cap(name, cmd):
    """Виконує блок, дублює stdout у файл screenshots/txt/<name>.txt з заголовком '$ команда'."""
    buf = Tee()
    with contextlib.redirect_stdout(buf):
        yield
    with open(f"{TXT}/{name}.txt", "w", encoding="utf-8") as f:
        f.write(f"$ {cmd}\n" + buf.getvalue())

def save(df, name):
    df.to_csv(f"{RES}/{name}.csv", index=False, encoding="utf-8")

def explain_str(df, mode="formatted"):
    return df._sc._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), mode)

# ---------- Завдання 1. Середовище та SparkSession ----------
t0_all = time.perf_counter()
CORES = os.cpu_count()
spark = (SparkSession.builder.appName("PZ2_BTC_Spark")
         .master("local[*]")                       # усі ядра однієї машини
         .config("spark.driver.memory", "8g")      # 15 ГБ RAM на машині; решта – ОС і Python
         .config("spark.sql.shuffle.partitions", str(CORES * 4))  # 16: ~4 розділи на ядро, 200 за замовч. надмір для 7,8 млн рядків
         .config("spark.sql.session.timeZone", "UTC")             # усі часові операції в UTC
         .config("spark.ui.port", "4040")
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())
sc = spark.sparkContext
sc.setLogLevel("ERROR")

def sh(c):
    return subprocess.run(c, shell=True, capture_output=True, text=True).stdout.strip()
ver_rows = [
    ("Java", sh("java -version 2>&1 | grep -v Picked | head -1")),
    ("Spark", spark.version), ("PySpark", pyspark.__version__),
    ("Python", platform.python_version()), ("pandas", pd.__version__),
    ("numpy", np.__version__), ("OS", platform.platform()),
    ("CPU", sh("lscpu | grep 'Model name' | sed 's/.*: *//'")),
    ("CPU_logical_cores", str(CORES)),
    ("RAM_GB", sh("free -g | awk '/Mem:/{print $2}'")),
    ("spark.master", sc.master), ("spark.defaultParallelism", str(sc.defaultParallelism)),
    ("spark.driver.memory", spark.conf.get("spark.driver.memory")),
    ("spark.sql.shuffle.partitions", spark.conf.get("spark.sql.shuffle.partitions")),
    ("spark.sql.session.timeZone", spark.conf.get("spark.sql.session.timeZone")),
    ("spark.ui.url", sc.uiWebUrl), ("seed", str(SEED)),
]
pd.DataFrame(ver_rows, columns=["parameter", "value"]).to_csv(f"{RES}/env_versions.csv", index=False)
with cap("01_env", "python3 -c \"import pyspark; ...\"  # версії та конфігурація SparkSession"):
    for k, v in ver_rows:
        print(f"{k:32s} {v}")

# ---------- Завдання 2. Завантаження даних ----------
SCHEMA = StructType([StructField("Timestamp", LongType()), StructField("Open", DoubleType()),
                     StructField("High", DoubleType()), StructField("Low", DoubleType()),
                     StructField("Close", DoubleType()), StructField("Volume", DoubleType())])

# 2а. Виведення схеми Spark-ом (inferSchema потребує додаткового проходу по файлу – вимірюємо)
with timed("load_csv_inferSchema_plus_count", "inferSchema=True, count()"):
    raw_inf = spark.read.csv(RAW, header=True, inferSchema=True)
    n_rows = raw_inf.count()
# 2б. Явна схема: без додаткового проходу
with timed("load_csv_explicit_schema_plus_count", "явна схема, count()"):
    raw = spark.read.csv(RAW, header=True, schema=SCHEMA)
    n_rows2 = raw.count()
assert n_rows == n_rows2

with cap("02_schema", "df = spark.read.csv(RAW, header=True, inferSchema=True); df.printSchema(); df.show(5)"):
    raw_inf.printSchema()
    print("Кількість записів :", n_rows)
    print("Кількість атрибутів:", len(raw_inf.columns))
    print("Назви полів       :", raw_inf.columns)
    print("Типи даних        :", raw_inf.dtypes)
    print("Кількість розділів (partitions) DataFrame:", raw_inf.rdd.getNumPartitions())
    raw_inf.show(5)

with timed("missing_values_per_column"):
    nulls = raw.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in raw.columns]).first().asDict()
    nans = raw.select([F.sum(F.isnan(F.col(c)).cast("int")).alias(c) for c in raw.columns if c != "Timestamp"]).first().asDict()
schema_tbl = pd.DataFrame({"field": raw.columns,
                           "spark_type_inferred": [t for _, t in raw_inf.dtypes],
                           "spark_type_explicit": [t for _, t in raw.dtypes],
                           "null_count": [nulls[c] for c in raw.columns],
                           "nan_count": [nans.get(c, 0) for c in raw.columns]})
save(schema_tbl, "pz2_schema_nulls")
pd.DataFrame([("rows", n_rows), ("columns", len(raw.columns)),
              ("raw_partitions_inferSchema", raw_inf.rdd.getNumPartitions()),
              ("raw_partitions_explicit", raw.rdd.getNumPartitions())],
             columns=["metric", "value"]).to_csv(f"{RES}/pz2_load_summary.csv", index=False)
with cap("03_nulls", "df.select([sum(col(c).isNull()) ...]).show()  # пропущені значення"):
    print(schema_tbl.to_string(index=False))

# ---------- Завдання 3. Попередня обробка ----------
def build():
    """Повертає (ледачий) DataFrame хвилинних свічок з похідними атрибутами."""
    d = spark.read.csv(RAW, header=True, schema=SCHEMA)
    d = d.dropna()                                              # пропущених немає, але крок виконується
    d = d.dropDuplicates(["Timestamp"])                         # дублікати за ключем часу
    d = d.withColumn("datetime", F.timestamp_seconds("Timestamp"))   # unix с -> timestamp (UTC за конфігурацією сесії)
    d = (d.withColumn("date", F.to_date("datetime"))
          .withColumn("year", F.year("datetime")).withColumn("month", F.month("datetime"))
          .withColumn("hour", F.hour("datetime"))
          .withColumn("is_flat", (F.col("Volume") == 0).cast("int"))      # нульовий обсяг = заповнена (flat) свічка
          .withColumn("range", F.col("High") - F.col("Low"))               # діапазон свічки
          .withColumn("rel_range", (F.col("High") - F.col("Low")) / F.col("Open")))
    w = Window.orderBy("Timestamp")
    d = d.withColumn("log_ret", F.log("Close") - F.log(F.lag("Close").over(w)))   # логарифмічна дохідність
    # ковзна волатильність: ст. відхилення log_ret за 60 хвилин (сітка без пропусків, тому 60 рядків = 60 хв)
    d = d.withColumn("vol60", F.stddev("log_ret").over(w.rowsBetween(-59, 0)))
    return d

# 3.1 Демонстрація дублікатів/пропусків
with timed("dedup_check"):
    n_dedup = raw.dropna().dropDuplicates(["Timestamp"]).count()
pd.DataFrame([("rows_raw", n_rows), ("rows_after_dropna", raw.dropna().count()),
              ("rows_after_dropDuplicates_Timestamp", n_dedup),
              ("duplicates_removed", n_rows - n_dedup)], columns=["metric", "value"]).to_csv(f"{RES}/pz2_cleaning.csv", index=False)

# ---------- Ледача обчислюваність: трансформації та дії ----------
t = time.perf_counter()
df = build()
t_transform = time.perf_counter() - t
rec("lazy_build_transformations", t_transform, "лише побудова плану, без обчислень")
t = time.perf_counter()
n_proc = df.count()                      # дія: запускає виконання всього плану
t_action = time.perf_counter() - t
rec("first_action_count_uncached", t_action, "count() запускає читання+вікна")
pd.DataFrame([("transformations_build_s", t_transform), ("first_action_count_s", t_action),
              ("rows", n_proc)], columns=["metric", "value"]).to_csv(f"{RES}/pz2_lazy_vs_action.csv", index=False)
with cap("04_lazy", "df = build(); # трансформації (мс)  ->  df.count() # дія; df.explain('formatted')"):
    print(f"Трансформації (read, dropna, dropDuplicates, withColumn x10, Window lag/stddev): {t_transform*1000:.1f} мс – обчислень немає")
    print(f"Дія count() = {n_proc} рядків за {t_action:.2f} с – лише тепер Spark виконує план")
    print()
    print(explain_str(df.select('date', 'log_ret', 'vol60').where('year = 2021'), "formatted")[:3800])
with open(f"{RES}/logs/explain_full.txt", "w") as f:
    f.write(explain_str(df, "extended"))

# ---------- Кешування: той самий запит без cache() та з cache() ----------
def heavy_query(d):
    """Два аналітичні запити (дві дії): топ-10 днів за реалізованою волатильністю та обсяг за годиною доби."""
    a = (d.groupBy("date").agg(F.sqrt(F.sum(F.col("log_ret") ** 2)).alias("rv"))
          .orderBy(F.desc("rv")).limit(10).collect())
    b = d.groupBy("hour").agg(F.sum("Volume").alias("v")).orderBy("hour").collect()
    return a, b

cache_rows = []
df.unpersist()
for i in range(1, 4):
    t = time.perf_counter(); heavy_query(df); dt = time.perf_counter() - t
    cache_rows.append(("without_cache", f"run{i}", round(dt, 3))); rec(f"cache_exp_without_cache_run{i}", dt)
df.persist(StorageLevel.MEMORY_AND_DISK)
t = time.perf_counter(); df.count(); dt = time.perf_counter() - t       # матеріалізація кешу
cache_rows.append(("with_cache", "materialize_count", round(dt, 3))); rec("cache_exp_materialize", dt)
for i in range(1, 4):
    t = time.perf_counter(); heavy_query(df); dt = time.perf_counter() - t
    cache_rows.append(("with_cache", f"run{i}", round(dt, 3))); rec(f"cache_exp_with_cache_run{i}", dt)
cache_df = pd.DataFrame(cache_rows, columns=["mode", "run", "seconds"])
save(cache_df, "pz2_cache_experiment")
with cap("05_cache", "df.unpersist(); heavy_query(df) x3; df.cache(); df.count(); heavy_query(df) x3"):
    print(cache_df.to_string(index=False))
    m_no = cache_df[(cache_df['mode']=='without_cache')]['seconds'].median()
    m_c = cache_df[(cache_df['mode']=='with_cache') & (cache_df['run']!='materialize_count')]['seconds'].median()
    print(f"\nмедіана без кешу: {m_no:.2f} с; з кешем: {m_c:.2f} с; прискорення x{m_no/m_c:.1f}")
    print("Storage level:", df.storageLevel)
pd.DataFrame([("median_without_cache_s", m_no), ("median_with_cache_s", m_c), ("speedup", m_no / m_c)],
             columns=["metric", "value"]).to_csv(f"{RES}/pz2_cache_summary.csv", index=False)

# ---------- Опис змінних та контроль якості похідних атрибутів ----------
with timed("describe_derived"):
    desc = df.select("Open", "High", "Low", "Close", "Volume", "range", "rel_range", "log_ret", "vol60").describe().toPandas()
save(desc, "pz2_describe")
flat = df.agg(F.sum("is_flat").alias("flat"), F.count("*").alias("n")).first()
flat_by_year = (df.groupBy("year").agg(F.count("*").alias("n_minutes"), F.sum("is_flat").alias("n_flat"),
                                       F.sum("Volume").alias("volume"), F.avg("Close").alias("avg_close"))
                  .withColumn("flat_share_pct", F.round(100 * F.col("n_flat") / F.col("n_minutes"), 2))
                  .orderBy("year").toPandas())
save(flat_by_year, "pz2_yearly_volume_flat")
pd.DataFrame([("flat_minutes", flat["flat"]), ("rows", flat["n"]), ("flat_share_pct", round(100 * flat["flat"] / flat["n"], 2))],
             columns=["metric", "value"]).to_csv(f"{RES}/pz2_flat_summary.csv", index=False)
with cap("06_derived", "df.select('Timestamp','datetime','Close','log_ret','range','rel_range','vol60').show(8); df.describe()"):
    df.select("Timestamp", "datetime", "Open", "High", "Low", "Close", "Volume", "log_ret", "range", "rel_range", "vol60", "year", "month", "hour").filter("year = 2021 and month = 5").show(8, truncate=False)
    print(desc.to_string(index=False))
    print(f"\nНульовий обсяг: {flat['flat']} з {flat['n']} = {100*flat['flat']/flat['n']:.2f} %")

# ---------- Завдання 4. Розподілена аналітика ----------
# 4.1 Денний агрегат (групування + агрегація) з похідними ознаками
with timed("daily_aggregation"):
    d1 = (df.groupBy("date").agg(
        F.min_by("Open", "Timestamp").alias("open"), F.max("High").alias("high"), F.min("Low").alias("low"),
        F.max_by("Close", "Timestamp").alias("close"), F.sum("Volume").alias("volume"),
        F.count("*").alias("n_minutes"), F.sum("is_flat").alias("n_flat"),
        F.sqrt(F.sum(F.col("log_ret") ** 2)).alias("rv"),            # реалізована волатильність за добу
        F.stddev("log_ret").alias("std_ret_1m"), F.avg("vol60").alias("vol60_mean"),
        F.max("rel_range").alias("max_rel_range_1m")))
    wd = Window.orderBy("date")
    d1 = (d1.withColumn("ret_1d", F.log("close") - F.log(F.lag("close").over(wd)))
            .withColumn("hl_rel", (F.col("high") - F.col("low")) / F.col("open"))
            .withColumn("flat_share", F.col("n_flat") / F.col("n_minutes"))
            .withColumn("is_complete", (F.col("n_minutes") == 1440).cast("int"))
            .withColumn("log_volume", F.log1p("volume"))
            .withColumn("vol_7d", F.stddev("ret_1d").over(wd.rowsBetween(-6, 0)))
            .withColumn("vol_30d", F.stddev("ret_1d").over(wd.rowsBetween(-29, 0)))
            .withColumn("ma_7", F.avg("close").over(wd.rowsBetween(-6, 0)))
            .withColumn("ma_30", F.avg("close").over(wd.rowsBetween(-29, 0)))
            .withColumn("dow", F.dayofweek("date")).withColumn("year", F.year("date")).withColumn("month", F.month("date")))
    daily = d1.orderBy("date").toPandas()
daily["date"] = pd.to_datetime(daily["date"])
daily.to_parquet(f"{RES}/pz2_daily_features.parquet", index=False)
save(daily, "pz2_daily_features")

# 4.2 Найволатильніші дні (сортування + фільтрація)
comp = d1.filter("is_complete = 1")
with timed("top10_volatile_days"):
    top_rv = comp.orderBy(F.desc("rv")).limit(10).toPandas()
    top_hl = comp.orderBy(F.desc("hl_rel")).limit(10).toPandas()
    # варіант без «розріджених» днів: щонайменше 50 % хвилин з торгами (щоб виключити 2012–2013 з частковими заповненнями)
    top_rv_active = comp.filter("flat_share < 0.5").orderBy(F.desc("rv")).limit(10).toPandas()
    top_hl_active = comp.filter("flat_share < 0.5").orderBy(F.desc("hl_rel")).limit(10).toPandas()
cols = ["date", "open", "high", "low", "close", "volume", "rv", "hl_rel", "ret_1d", "flat_share"]
save(top_rv[cols], "pz2_top10_rv"); save(top_hl[cols], "pz2_top10_hl_rel")
save(top_rv_active[cols], "pz2_top10_rv_active"); save(top_hl_active[cols], "pz2_top10_hl_rel_active")
with cap("07_top_days", "comp.orderBy(desc('rv')).limit(10); comp.orderBy(desc('hl_rel')).limit(10)"):
    pd.set_option("display.width", 200)
    print("Топ-10 за реалізованою волатильністю rv = sqrt(sum(log_ret^2)), повні доби:")
    print(top_rv[cols].round(4).to_string(index=False))
    print("\nТоп-10 за (High-Low)/Open:")
    print(top_hl[cols].round(4).to_string(index=False))
    print("\nТоп-10 за rv, flat_share < 0,5:")
    print(top_rv_active[cols].round(4).to_string(index=False))

# 4.3 Обсяг за годиною доби (UTC) та за роками
with timed("volume_by_hour"):
    vh = (df.groupBy("hour").agg(F.sum("Volume").alias("volume_total"),
                                 F.avg("Volume").alias("volume_mean_per_minute"),
                                 F.avg(F.when(F.col("is_flat") == 0, F.col("Volume"))).alias("volume_mean_active_minute"),
                                 F.avg("rel_range").alias("rel_range_mean"),
                                 F.stddev("log_ret").alias("std_ret_1m"))
            .withColumn("share_pct", 100 * F.col("volume_total") / F.sum("volume_total").over(Window.partitionBy())).orderBy("hour").toPandas())
save(vh, "pz2_volume_by_hour")
save(flat_by_year, "pz2_volume_by_year")
with cap("08_volume", "df.groupBy('hour').agg(sum('Volume')...); df.groupBy('year').agg(...)"):
    print(vh.round(5).to_string(index=False)); print(); print(flat_by_year.round(2).to_string(index=False))

# 4.4 Погодинна таблиця ознак
with timed("hourly_features"):
    hh = (df.groupBy(F.date_trunc("hour", "datetime").alias("datetime_utc")).agg(
        F.min_by("Open", "Timestamp").alias("open"), F.max("High").alias("high"), F.min("Low").alias("low"),
        F.max_by("Close", "Timestamp").alias("close"), F.sum("Volume").alias("volume"),
        F.count("*").alias("n_minutes"), F.sum("is_flat").alias("n_flat"),
        F.sqrt(F.sum(F.col("log_ret") ** 2)).alias("rv"), F.stddev("log_ret").alias("std_ret_1m")))
    wh = Window.orderBy("datetime_utc")
    hh = (hh.withColumn("ret_1h", F.log("close") - F.log(F.lag("close").over(wh)))
            .withColumn("range", F.col("high") - F.col("low")).withColumn("rel_range", (F.col("high") - F.col("low")) / F.col("open"))
            .withColumn("flat_share", F.col("n_flat") / F.col("n_minutes"))
            .withColumn("log_volume", F.log1p("volume"))
            .withColumn("vol_24h", F.stddev("ret_1h").over(wh.rowsBetween(-23, 0)))
            .withColumn("hour", F.hour("datetime_utc")).withColumn("dow", F.dayofweek("datetime_utc"))
            .withColumn("year", F.year("datetime_utc")))
    hourly = hh.orderBy("datetime_utc").toPandas()
hourly.to_parquet(f"{RES}/pz2_hourly_features.parquet", index=False)

# 4.5 Асиметрія та ексцес дохідностей на різних масштабах
def moments(name, d, col, filt=None):
    x = d.filter(filt) if filt else d
    r = x.filter(F.col(col).isNotNull()).agg(F.count(col).alias("n"), F.avg(col).alias("mean"), F.stddev(col).alias("std"),
                                              F.skewness(col).alias("skew"), F.kurtosis(col).alias("excess_kurtosis"),
                                              F.min(col).alias("min"), F.max(col).alias("max")).first().asDict()
    r["series"] = name; return r
with timed("skew_kurtosis"):
    rows = [moments("1 хв, усі свічки", df, "log_ret"),
            moments("1 хв, лише з торгами (Volume>0)", df, "log_ret", "is_flat = 0"),
            moments("1 год, усі", d1.sparkSession.createDataFrame(hourly[["ret_1h"]].dropna()), "ret_1h"),
            moments("1 год, flat_share<0,5", d1.sparkSession.createDataFrame(hourly[hourly.flat_share < 0.5][["ret_1h"]].dropna()), "ret_1h"),
            moments("1 доба, усі", d1.sparkSession.createDataFrame(daily[["ret_1d"]].dropna()), "ret_1d"),
            moments("1 доба, flat_share<0,5", d1.sparkSession.createDataFrame(daily[daily.flat_share < 0.5][["ret_1d"]].dropna()), "ret_1d")]
mom = pd.DataFrame(rows)[["series", "n", "mean", "std", "skew", "excess_kurtosis", "min", "max"]]
save(mom, "pz2_skew_kurtosis")
with cap("09_moments", "F.skewness('log_ret'), F.kurtosis('log_ret')  # 1 хв / 1 год / 1 доба"):
    print(mom.to_string(index=False, float_format=lambda v: f"{v:.6g}"))

# гістограма хвилинних дохідностей (лише з торгами) для порівняння з нормальним розподілом
BW = 0.0001
with timed("minute_return_histogram"):
    hist = (df.filter("is_flat = 0 and log_ret is not null and abs(log_ret) < 0.01")
              .withColumn("bin", F.floor(F.col("log_ret") / BW)).groupBy("bin").count().orderBy("bin").toPandas())
hist["bin_center"] = (hist["bin"] + 0.5) * BW
save(hist, "pz2_hist_ret_1m_active")

# ---------- Завдання 5. Spark SQL ----------
df.createOrReplaceTempView("btc")
d1.createOrReplaceTempView("btc_daily")
SQLQ = {
 "q1_year_stats": """SELECT year, COUNT(*) AS minutes, ROUND(AVG(Close),2) AS avg_close, MAX(High) AS max_high, MIN(Low) AS min_low,
                          ROUND(SUM(Volume),0) AS volume, ROUND(100*AVG(is_flat),2) AS flat_pct
                     FROM btc GROUP BY year ORDER BY year""",
 "q2_top_days_range": """SELECT date, open, high, low, ROUND((high-low)/open,4) AS hl_rel, ROUND(volume,0) AS volume, ROUND(flat_share,3) AS flat_share
                     FROM btc_daily WHERE is_complete = 1 AND flat_share < 0.5 ORDER BY hl_rel DESC LIMIT 10""",
 "q3_dow_hour_volume": """SELECT dayofweek(datetime) AS dow, hour, ROUND(AVG(Volume),4) AS avg_volume_per_minute,
                          ROUND(SUM(Volume),0) AS volume_total
                     FROM btc WHERE year >= 2018 GROUP BY dow, hour ORDER BY dow, hour""",
 "q4_month_seasonality": """SELECT month, ROUND(AVG(rv),5) AS avg_daily_rv, ROUND(AVG(hl_rel),4) AS avg_hl_rel, ROUND(AVG(volume),1) AS avg_daily_volume, COUNT(*) AS days
                     FROM btc_daily WHERE year >= 2018 AND is_complete = 1 GROUP BY month ORDER BY month""",
 "q5_max_volume_day_per_year": """SELECT year, date, ROUND(volume,0) AS volume, close FROM (
                         SELECT year, date, volume, close, ROW_NUMBER() OVER (PARTITION BY year ORDER BY volume DESC) AS rn FROM btc_daily)
                     WHERE rn = 1 ORDER BY year""",
 "q6_crash_days_vs_30d_mean": """SELECT date, ROUND(100*ret_1d,2) AS ret_pct, ROUND(rv,4) AS rv, ROUND(volume,0) AS volume,
                          ROUND(volume / AVG(volume) OVER (ORDER BY date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING),2) AS volume_x_prev30_mean
                     FROM btc_daily WHERE date IN (DATE'2020-03-12', DATE'2020-03-13', DATE'2021-05-19', DATE'2022-11-08', DATE'2022-11-09', DATE'2022-06-13') ORDER BY date""",
}
# q6: вікно рахується всередині відфільтрованого набору, тому рахуємо у підзапиті над усіма днями
SQLQ["q6_crash_days_vs_30d_mean"] = """SELECT * FROM (
    SELECT date, ROUND(100*ret_1d,2) AS ret_pct, ROUND(rv,4) AS rv, ROUND(volume,0) AS volume,
           ROUND(volume / AVG(volume) OVER (ORDER BY date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING),2) AS volume_x_prev30_mean
    FROM btc_daily) WHERE date IN (DATE'2020-03-12', DATE'2020-03-13', DATE'2021-05-19', DATE'2022-06-13', DATE'2022-11-08', DATE'2022-11-09') ORDER BY date"""
for i, (k, q) in enumerate(SQLQ.items(), 1):
    with timed(f"sql_{k}"):
        r = spark.sql(q).toPandas()
    save(r, f"pz2_sql_{k}")
    with cap(f"10_sql_{k}", "spark.sql(\"" + " ".join(q.split())[:150] + " ...\").show()"):
        print(r.head(25).to_string(index=False))
with open(f"{RES}/pz2_sql_queries.sql", "w", encoding="utf-8") as f:
    for k, q in SQLQ.items():
        f.write(f"-- {k}\n{' '.join(q.split())};\n\n")
with cap("11_sql_explain", "spark.sql('SELECT year, ... FROM btc GROUP BY year').explain()"):
    print(explain_str(spark.sql(SQLQ["q1_year_stats"]), "formatted")[:3500])

# ---------- Завдання 6. Spark vs pandas vs (Hadoop) на тому самому CSV ----------
def spark_year_count_volume():
    d = spark.read.csv(RAW, header=True, schema=SCHEMA)
    return d.groupBy(F.year(F.timestamp_seconds("Timestamp")).alias("year")).agg(F.count("*").alias("n"), F.sum("Volume").alias("vol")).orderBy("year").toPandas()
def spark_daily_ohlc():
    d = spark.read.csv(RAW, header=True, schema=SCHEMA).withColumn("date", F.to_date(F.timestamp_seconds("Timestamp")))
    return (d.groupBy("date").agg(F.min_by("Open", "Timestamp").alias("open"), F.max("High").alias("high"), F.min("Low").alias("low"),
                                  F.max_by("Close", "Timestamp").alias("close"), F.sum("Volume").alias("volume")).orderBy("date").toPandas())
def pandas_year_count_volume():
    d = pd.read_csv(RAW); y = pd.to_datetime(d["Timestamp"], unit="s").dt.year
    return d.groupby(y).agg(n=("Volume", "size"), vol=("Volume", "sum"))
def pandas_daily_ohlc():
    d = pd.read_csv(RAW); dt = pd.to_datetime(d["Timestamp"], unit="s").dt.date
    return d.groupby(dt).agg(open=("Open", "first"), high=("High", "max"), low=("Low", "min"), close=("Close", "last"), volume=("Volume", "sum"))
cmp_rows = []
chk = {}
for task, fs, fp in (("year_count_volume", spark_year_count_volume, pandas_year_count_volume),
                     ("daily_ohlc", spark_daily_ohlc, pandas_daily_ohlc)):
    for engine, fn in (("spark_local[*]", fs), ("pandas", fp)):
        for run in (1, 2, 3):
            t = time.perf_counter(); out = fn(); dt = time.perf_counter() - t
            cmp_rows.append((task, engine, run, round(dt, 3), "читання CSV + агрегація"))
            chk[(task, engine)] = out
    # перевірка однаковості результатів
    a, b = chk[(task, "spark_local[*]")], chk[(task, "pandas")]
    if task == "year_count_volume":
        assert (a["n"].values == b["n"].values).all() and np.allclose(a["vol"].values, b["vol"].values)
    else:
        assert len(a) == len(b) and np.allclose(a["close"].values, b["close"].values) and np.allclose(a["volume"].values, b["volume"].values)
# Spark на кешованому DataFrame (дані вже в пам'яті, без читання CSV)
for run in (1, 2, 3):
    t = time.perf_counter()
    df.groupBy("year").agg(F.count("*"), F.sum("Volume")).orderBy("year").collect(); dt1 = time.perf_counter() - t
    cmp_rows.append(("year_count_volume", "spark_cached_df", run, round(dt1, 3), "лише агрегація над кешем"))
    t = time.perf_counter()
    df.groupBy("date").agg(F.min_by("Open", "Timestamp"), F.max("High"), F.min("Low"), F.max_by("Close", "Timestamp"), F.sum("Volume")).collect(); dt2 = time.perf_counter() - t
    cmp_rows.append(("daily_ohlc", "spark_cached_df", run, round(dt2, 3), "лише агрегація над кешем"))
cmp_rows.append(("year_count_volume", "hadoop_streaming", "", "[ЗАПОВНИТИ ПІСЛЯ ЗАПУСКУ]", "значення з ПЗ1 (Colab)"))
cmp_rows.append(("daily_ohlc", "hadoop_streaming", "", "[ЗАПОВНИТИ ПІСЛЯ ЗАПУСКУ]", "значення з ПЗ1 (Colab)"))
cmp = pd.DataFrame(cmp_rows, columns=["task", "engine", "run", "seconds", "note"])
save(cmp, "pz2_engine_comparison")
num = cmp[pd.to_numeric(cmp["seconds"], errors="coerce").notna()].copy(); num["seconds"] = num["seconds"].astype(float)
summ = num.groupby(["task", "engine"])["seconds"].agg(["median", "min", "max"]).round(3).reset_index()
save(summ, "pz2_engine_comparison_summary")
for _, r in summ.iterrows():
    rec(f"engine_{r['task']}_{r['engine']}", r["median"], "медіана з 3 запусків")
rec("engine_year_count_volume_hadoop_streaming", float("nan"), "[ЗАПОВНИТИ ПІСЛЯ ЗАПУСКУ]")
rec("engine_daily_ohlc_hadoop_streaming", float("nan"), "[ЗАПОВНИТИ ПІСЛЯ ЗАПУСКУ]")
with cap("12_engines", "spark vs pandas: рік (count+volume) та денний OHLC, 3 запуски, той самий CSV"):
    print(cmp.to_string(index=False)); print(); print(summ.to_string(index=False))

# ---------- Знімки Spark UI, поки сесія жива ----------
def ui_screens():
    os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", "/opt/pw-browsers")
    from playwright.sync_api import sync_playwright
    pages = {"jobs": "jobs/", "stages": "stages/", "sql": "SQL/", "executors": "executors/", "storage": "storage/", "environment": "environment/"}
    with sync_playwright() as p:
        b = p.chromium.launch(executable_path="/opt/pw-browsers/chromium_headless_shell-1243/chrome-headless-shell-linux64/chrome-headless-shell")
        pg = b.new_page(viewport={"width": 1500, "height": 950})
        for k, u in pages.items():
            pg.goto(f"http://localhost:4040/{u}", wait_until="load"); pg.wait_for_timeout(1200)
            pg.screenshot(path=f"{SHOTS}/spark_ui_{k}.png")
        # деталі найважчого SQL-запиту (план виконання)
        pg.goto("http://localhost:4040/SQL/", wait_until="load")
        link = pg.locator("table a").first
        if link.count():
            link.click(); pg.wait_for_timeout(1500); pg.screenshot(path=f"{SHOTS}/spark_ui_sql_detail.png", full_page=False)
        b.close()
try:
    ui_screens()
except Exception as e:
    print("UI screenshots failed:", repr(e))
    open(f"{RES}/logs/ui_error.txt", "w").write(repr(e))

rec("TOTAL_script_runtime", time.perf_counter() - t0_all)
pd.DataFrame(TIMINGS, columns=["step", "seconds", "note"]).to_csv(f"{RES}/timings.csv", index=False)
spark.stop()
print("OK")
