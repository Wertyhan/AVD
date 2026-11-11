# -*- coding: utf-8 -*-
"""ПЗ2. Apache Spark (PySpark + Spark SQL) на хвилинних свічках BTC/USD.
Запуск з каталогу pz2:  python code/pz2_spark.py   (результати -> results, скріншоти -> screenshots)
Увесь конвеєр, знімки Spark UI і txt для терміналних скріншотів створюються одним запуском.
"""
import os, sys, io, time, platform, subprocess, contextlib, json, urllib.request
import numpy as np
import pandas as pd

SEED = 42
np.random.seed(SEED)
BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
RES, SHOTS, TXT = f"{BASE}/results", f"{BASE}/screenshots", f"{BASE}/screenshots/txt"
RAW = "/home/user/AVD/work/data/raw/btcusd_1-min_data.csv"
for d in (RES, f"{RES}/logs", SHOTS, TXT):
    os.makedirs(d, exist_ok=True)

from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.types import StructType, StructField, LongType, DoubleType
from pyspark import StorageLevel
import pyspark

# ---------- допоміжні засоби: вимірювання часу та захоплення виводу для скріншотів ----------
TIMINGS = []
sc = None
def rec(step, secs, note=""):
    TIMINGS.append((step, round(secs, 3), note))
    print(f"[timing] {step}: {secs:.2f} с {note}")

@contextlib.contextmanager
def timed(step, note=""):
    if sc: sc.setJobDescription(f"pz2_spark.py: {step}")   # підпис у вкладках Jobs і SQL у Spark UI
    t = time.perf_counter(); yield
    rec(step, time.perf_counter() - t, note)

def jobdesc(text):
    if sc: sc.setJobDescription(f"pz2_spark.py: {text}")

class Tee(io.StringIO):
    def write(self, s):
        sys.__stdout__.write(s); return super().write(s)

@contextlib.contextmanager
def cap(name, what):
    """Виконує блок і дублює реальний stdout у screenshots/txt/<name>.txt.
    Перший рядок – справжня команда запуску, другий – коментар про фрагмент, далі – вивід без змін."""
    buf = Tee()
    with contextlib.redirect_stdout(buf):
        yield
    with open(f"{TXT}/{name}.txt", "w", encoding="utf-8") as f:
        f.write(f"$ python code/pz2_spark.py\n# фрагмент виводу: {what}\n" + buf.getvalue())

def save(df, name):
    df.to_csv(f"{RES}/{name}.csv", index=False, encoding="utf-8")

def explain_str(df, mode="formatted"):
    return df._sc._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), mode)

def rest(path):
    with urllib.request.urlopen(f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/{path}") as r:
        return json.loads(r.read())

def cache_info():
    """Стан кешу за REST API Spark UI: кількість розділів, закешованих розділів, пам'ять і диск."""
    out = [(x["name"][:40], x["numPartitions"], x["numCachedPartitions"], x["memoryUsed"], x["diskUsed"]) for x in rest("storage/rdd")]
    return out

def sh(c):
    return subprocess.run(c, shell=True, capture_output=True, text=True).stdout.strip()

# ---------- Завдання 1. Середовище та SparkSession ----------
t0_all = time.perf_counter()
CORES = os.cpu_count()
spark = (SparkSession.builder.appName("PZ2_BTC_Spark")
         .master("local[*]")                       # усі ядра однієї машини
         .config("spark.driver.memory", "8g")      # 15 ГБ RAM на машині; решта – ОС і Python
         .config("spark.sql.shuffle.partitions", str(CORES * 4))  # 16: ~4 розділи на ядро, 200 за замовч. надмір для 7,8 млн рядків
         # AQE зливає малі розділи шафла; вимкнено злиття, щоб після вікон із partitionBy кеш мав усі 16 розділів
         .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
         .config("spark.sql.session.timeZone", "UTC")             # усі часові операції в UTC
         .config("spark.ui.port", "4040")
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())
sc = spark.sparkContext
sc.setLogLevel("ERROR")

import matplotlib, psutil
ver_rows = [
    ("Java", sh("java -version 2>&1 | grep -v Picked | head -1")),
    ("Spark", spark.version), ("PySpark", pyspark.__version__),
    ("Python", platform.python_version()), ("pandas", pd.__version__),
    ("numpy", np.__version__), ("matplotlib", matplotlib.__version__), ("psutil", psutil.__version__),
    ("playwright", sh("pip show playwright | awk '/^Version/{print $2}'")),
    ("Chromium", sh("/opt/pw-browsers/chromium-1243/chrome-linux64/chrome --version 2>/dev/null")),
    ("OS", platform.platform()),
    ("CPU", sh("lscpu | grep 'Model name' | sed 's/.*: *//'")),
    ("CPU_logical_cores", str(CORES)),
    ("RAM_GB", sh("free -g | awk '/Mem:/{print $2}'")),
    ("spark.master", sc.master), ("spark.defaultParallelism", str(sc.defaultParallelism)),
    ("spark.driver.memory", spark.conf.get("spark.driver.memory")),
    ("spark.sql.shuffle.partitions", spark.conf.get("spark.sql.shuffle.partitions")),
    ("spark.sql.adaptive.coalescePartitions.enabled", spark.conf.get("spark.sql.adaptive.coalescePartitions.enabled")),
    ("spark.sql.session.timeZone", spark.conf.get("spark.sql.session.timeZone")),
    ("spark.ui.url", sc.uiWebUrl), ("seed", str(SEED)),
    ("run_started", time.strftime("%Y-%m-%d %H:%M:%S")),
    ("loadavg_at_start", sh("cat /proc/loadavg")),
]
pd.DataFrame(ver_rows, columns=["parameter", "value"]).to_csv(f"{RES}/env_versions.csv", index=False)
with cap("01_env", "версії програмного забезпечення та конфігурація SparkSession"):
    for k, v in ver_rows:
        print(f"{k:46s} {v}")

# ---------- Завдання 2. Завантаження даних ----------
SCHEMA = StructType([StructField("Timestamp", LongType()), StructField("Open", DoubleType()),
                     StructField("High", DoubleType()), StructField("Low", DoubleType()),
                     StructField("Close", DoubleType()), StructField("Volume", DoubleType())])
def read_inferred(): return spark.read.csv(RAW, header=True, inferSchema=True)
def read_explicit(): return spark.read.csv(RAW, header=True, schema=SCHEMA)
def parse_all(d):
    """Дія, що розбирає ВСІ шість стовпців: суми всіх стовпців (на відміну від count(), який стовпці не читає)."""
    return d.agg(*[F.sum(c).alias(c) for c in d.columns]).first().asDict()

# 2а. Експеримент inferSchema vs явна схема (M3): гріємо файловий кеш ОС, потім 3 пари запусків із чергуванням порядку
mem_before = sh("free -m | awk '/Mem:/{print $6}'")
with open(RAW, "rb") as fh:
    while fh.read(1 << 24): pass                       # прогрів файлового кешу ОС: один повний послідовний прохід
mem_after = sh("free -m | awk '/Mem:/{print $6}'")
schema_rows, sums = [], {}
for i in (1, 2, 3):
    order = ("inferSchema", "explicit") if i % 2 else ("explicit", "inferSchema")   # порядок чергується
    for kind in order:
        jobdesc(f"schema_experiment_{kind}_run{i}")
        t = time.perf_counter()
        s = parse_all(read_inferred() if kind == "inferSchema" else read_explicit())
        dt = time.perf_counter() - t
        schema_rows.append((kind, i, order.index(kind) + 1, round(dt, 3))); sums[kind] = s
        rec(f"schema_experiment_{kind}_run{i}", dt, f"read+sum усіх 6 стовпців, позиція в парі {order.index(kind)+1}")
assert all(abs(sums["inferSchema"][c] - sums["explicit"][c]) <= 1e-9 * abs(sums["explicit"][c]) for c in sums["explicit"])
schema_exp = pd.DataFrame(schema_rows, columns=["variant", "run", "position_in_pair", "seconds"])
schema_exp["note"] = f"warm OS file cache: file read once before the runs; free -m buff/cache {mem_before} -> {mem_after} MB; action = sum of all 6 columns"
save(schema_exp, "pz2_schema_experiment")
schema_sum = schema_exp.groupby("variant")["seconds"].agg(["median", "min", "max"]).reset_index()
schema_sum["ratio_vs_explicit"] = schema_sum["median"] / schema_sum.loc[schema_sum.variant == "explicit", "median"].iloc[0]
save(schema_sum, "pz2_schema_experiment_summary")
with cap("02a_schema_experiment", "inferSchema проти явної схеми: сума всіх 6 стовпців, 3 запуски, порядок чергується"):
    print(f"Файловий кеш ОС прогріто (buff/cache, МБ: {mem_before} -> {mem_after})")
    print(schema_exp.drop(columns="note").to_string(index=False)); print(); print(schema_sum.round(3).to_string(index=False))

raw_inf = read_inferred()
raw = read_explicit()
with timed("count_explicit_schema", "count() без розбору стовпців"):
    n_rows = raw.count()
with cap("02_schema", "printSchema(), кількість записів/атрибутів, назви та типи полів, show(5)"):
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
with cap("03_nulls", "кількість пропущених значень і NaN за стовпцями, типи"):
    print(schema_tbl.to_string(index=False))

# ---------- Завдання 3. Попередня обробка ----------
# Ключ секціонування вікон: (year, month). Вікно без partitionBy зводить усі 7,8 млн рядків в один розділ
# (Exchange SinglePartition). Із partitionBy вікна рахуються паралельно, а кеш має 16 розділів.
# Ціна: у першому рядку кожного розділу-місяця log_ret = NULL, а перші 59 хв місяця мають менше 60 точок у vol60.
PART = ["year", "month"]
def build(partitioned=True):
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
    w = Window.partitionBy(*PART).orderBy("Timestamp") if partitioned else Window.orderBy("Timestamp")
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

# ---------- Завдання 6 (частина). Spark vs pandas: чергування рушіїв, ДО кешування (сесія без кешу) ----------
def spark_year_count_volume():
    d = read_explicit()
    return d.groupBy(F.year(F.timestamp_seconds("Timestamp")).alias("year")).agg(F.count("*").alias("n"), F.sum("Volume").alias("vol")).orderBy("year").toPandas()
def spark_daily_ohlc():
    d = read_explicit().withColumn("date", F.to_date(F.timestamp_seconds("Timestamp")))
    return (d.groupBy("date").agg(F.min_by("Open", "Timestamp").alias("open"), F.max("High").alias("high"), F.min("Low").alias("low"),
                                  F.max_by("Close", "Timestamp").alias("close"), F.sum("Volume").alias("volume"),
                                  F.count("*").alias("n")).orderBy("date").toPandas())
def pandas_year_count_volume():
    d = pd.read_csv(RAW); y = pd.to_datetime(d["Timestamp"], unit="s").dt.year.rename("year")
    return d.groupby(y).agg(n=("Volume", "size"), vol=("Volume", "sum")).reset_index()
def pandas_daily_ohlc():
    d = pd.read_csv(RAW)
    assert d["Timestamp"].is_monotonic_increasing, "first/last у pandas вимагають відсортованого за часом файлу"
    dt = pd.to_datetime(d["Timestamp"], unit="s").dt.date.rename("date")
    return d.groupby(dt).agg(open=("Open", "first"), high=("High", "max"), low=("Low", "min"), close=("Close", "last"),
                             volume=("Volume", "sum"), n=("Volume", "size")).reset_index()

def rel_diff(a, b):
    a, b = np.asarray(a, float), np.asarray(b, float)
    return float(np.max(np.abs(a - b) / np.maximum(np.abs(b), 1e-300)))
def check_equal(task, a, b):
    """Повна звірка результатів двох рушіїв; повертає рядки для pz2_engine_equality.csv і перериває при розбіжності."""
    rows = []
    def chk(name, ok, val): rows.append((task, name, bool(ok), val)); assert ok, f"{task}: {name} {val}"
    chk("row_count_equal", len(a) == len(b), f"{len(a)} vs {len(b)}")
    if task == "year_count_volume":
        chk("year_equal", (a["year"].values == b["year"].values).all(), "")
        chk("count_equal", (a["n"].values == b["n"].values).all(), "")
        d = rel_diff(a["vol"], b["vol"]); chk("volume_rel_diff<=1e-6", d <= 1e-6, f"max rel diff {d:.3g}")
    else:
        chk("date_equal", (pd.to_datetime(a["date"]).values == pd.to_datetime(b["date"]).values).all(), "")
        chk("minutes_per_day_equal", (a["n"].values == b["n"].values).all(), "")
        for c in ("open", "high", "low", "close"):
            d = rel_diff(a[c], b[c]); chk(f"{c}_rel_diff<=1e-9", d <= 1e-9, f"max rel diff {d:.3g}")
        d = rel_diff(a["volume"], b["volume"]); chk("volume_rel_diff<=1e-6", d <= 1e-6, f"max rel diff {d:.3g}")
    return rows

cmp_rows, eq_rows = [], []
for task, fs, fp in (("year_count_volume", spark_year_count_volume, pandas_year_count_volume),
                     ("daily_ohlc", spark_daily_ohlc, pandas_daily_ohlc)):
    last = {}
    for run in (1, 2, 3):                      # порядок S, P, S, P, S, P
        for engine, fn in (("spark_local[*]", fs), ("pandas", fp)):
            jobdesc(f"engine_{task}_{engine}_run{run}")
            t = time.perf_counter(); out = fn(); dt = time.perf_counter() - t
            cmp_rows.append((task, engine, run, round(dt, 3), "читання CSV + агрегація; Spark включає toPandas(), pandas – один потік"))
            last[engine] = out
    eq_rows += check_equal(task, last["spark_local[*]"], last["pandas"])
pd.DataFrame(eq_rows, columns=["task", "check", "passed", "detail"]).to_csv(f"{RES}/pz2_engine_equality.csv", index=False)

# ---------- Ледача обчислюваність: трансформації, дії, explain() (C1) ----------
t = time.perf_counter()
df = build()
t_transform = time.perf_counter() - t
rec("lazy_build_transformations", t_transform, "лише побудова плану, без обчислень")
plan_count = explain_str(df.groupBy().count(), "formatted")
plan_vol = explain_str(df.agg(F.sum("vol60").alias("sum_vol60")), "formatted")
has_win_count, has_win_vol = "Window" in plan_count, "Window" in plan_vol
with open(f"{RES}/logs/explain_count.txt", "w") as f: f.write(plan_count)
with open(f"{RES}/logs/explain_sum_vol60.txt", "w") as f: f.write(plan_vol)
with open(f"{RES}/logs/explain_full.txt", "w") as f: f.write(explain_str(df, "extended"))

ACTIONS = {
    "count": lambda: df.count(),                                                       # стовпці вікон не потрібні -> обрізаються
    "agg_sum_vol60_logret": lambda: df.agg(F.sum("vol60"), F.sum("log_ret")).first(),  # потребує обох вікон
    "noop_write": lambda: df.write.format("noop").mode("overwrite").save(),            # обчислює всі стовпці, нічого не пише
}
names = list(ACTIONS)
act_rows = []
for i in range(3):
    for k in names[i % 3:] + names[:i % 3]:     # порядок дій ротується між раундами
        jobdesc(f"lazy_action_{k}_run{i+1}")
        t = time.perf_counter(); ACTIONS[k](); dt = time.perf_counter() - t
        act_rows.append((k, i + 1, round(dt, 3))); rec(f"lazy_action_{k}_run{i+1}", dt)
act = pd.DataFrame(act_rows, columns=["action", "run", "seconds"])
act["plan_contains_Window"] = act["action"].map({"count": has_win_count, "agg_sum_vol60_logret": has_win_vol, "noop_write": True})
save(act, "pz2_lazy_vs_action")
act_sum = act.groupby("action")["seconds"].agg(["median", "min", "max"]).reset_index()
act_sum = pd.concat([pd.DataFrame([("transformations_build_only", t_transform, t_transform, t_transform)], columns=act_sum.columns), act_sum])
save(act_sum, "pz2_lazy_vs_action_summary")
with cap("04_lazy", "трансформації (мс) проти дій; explain() для count() і для sum(vol60)"):
    print(f"Трансформації (read, dropna, dropDuplicates, withColumn x10, Window lag/stddev): {t_transform*1000:.1f} мс – обчислень немає\n")
    print(act_sum.round(3).to_string(index=False))
    print(f"\nПлан count() містить вузол Window: {has_win_count}; план sum(vol60): {has_win_vol}\n")
    print("== df.groupBy().count().explain('formatted') ==")
    print(plan_count[:1800])
    print("== df.agg(sum('vol60')).explain('formatted'), фрагмент ==")
    print("\n".join(l for l in plan_vol.splitlines() if "Window" in l or "Exchange" in l or "Sort" in l or l.startswith("== ")))

# ---------- Кешування (M4): розділи, матеріалізація, кешований vs некешований запит ----------
def heavy_query(d):
    """Два аналітичні запити (дві дії): топ-10 днів за реалізованою волатильністю та обсяг за годиною доби."""
    a = (d.groupBy("date").agg(F.sqrt(F.sum(F.col("log_ret") ** 2)).alias("rv"))
          .orderBy(F.desc("rv")).limit(10).collect())
    b = d.groupBy("hour").agg(F.sum("Volume").alias("v")).orderBy("hour").collect()
    return a, b

# Порядок у раундах чергується: 1) U,M,C  2) M,C,U  3) U,M,C  (U = без кешу, M = матеріалізація кешу, C = з кешем)
cache_rows, part_rows = [], []
def run_U(i):
    jobdesc(f"cache_exp_uncached_run{i}")
    t = time.perf_counter(); heavy_query(df); dt = time.perf_counter() - t
    cache_rows.append(("without_cache", f"run{i}", round(dt, 3))); rec(f"cache_exp_without_cache_run{i}", dt)
def run_M(i):
    df.unpersist(blocking=True); df.persist(StorageLevel.MEMORY_AND_DISK)
    jobdesc(f"cache_exp_materialize_run{i}")
    t = time.perf_counter(); df.count(); dt = time.perf_counter() - t       # count() матеріалізує КЕШ усіх стовпців плану
    cache_rows.append(("with_cache", f"materialize_run{i}", round(dt, 3))); rec(f"cache_exp_materialize_run{i}", dt)
    ci = cache_info(); part_rows.append((i, *ci[0]) if ci else (i, "", 0, 0, 0, 0))
def run_C(i):
    jobdesc(f"cache_exp_cached_run{i}")
    t = time.perf_counter(); heavy_query(df); dt = time.perf_counter() - t
    cache_rows.append(("with_cache", f"run{i}", round(dt, 3))); rec(f"cache_exp_with_cache_run{i}", dt)
df.unpersist(blocking=True)
run_U(1); run_M(1); run_C(1)                 # раунд 1: U -> C
run_M(2); run_C(2); df.unpersist(blocking=True); run_U(2)    # раунд 2: C -> U
run_U(3); run_M(3); run_C(3)                 # раунд 3: U -> C; наприкінці кеш матеріалізовано (для Storage у Spark UI)
cache_df = pd.DataFrame(cache_rows, columns=["mode", "run", "seconds"])
save(cache_df, "pz2_cache_experiment")
save(pd.DataFrame(part_rows, columns=["round", "rdd_name", "numPartitions", "numCachedPartitions", "memoryUsed_bytes", "diskUsed_bytes"]), "pz2_cache_partitions")
m_no = cache_df[cache_df["mode"] == "without_cache"]["seconds"].median()
m_c = cache_df[(cache_df["mode"] == "with_cache") & (~cache_df["run"].str.startswith("materialize"))]["seconds"].median()
m_mat = cache_df[cache_df["run"].str.startswith("materialize")]["seconds"].median()
pd.DataFrame([("median_without_cache_s", m_no), ("median_with_cache_s", m_c), ("median_materialize_s", m_mat),
              ("speedup", m_no / m_c), ("break_even_runs", m_mat / (m_no - m_c)),
              ("cached_partitions_last", part_rows[-1][3]), ("total_partitions_last", part_rows[-1][2]),
              ("cache_memory_bytes_last", part_rows[-1][4]), ("cache_disk_bytes_last", part_rows[-1][5])],
             columns=["metric", "value"]).to_csv(f"{RES}/pz2_cache_summary.csv", index=False)
with cap("05_cache", "кеш: некешований запит, матеріалізація, кешований запит (3 раунди, порядок чергується)"):
    print(cache_df.to_string(index=False))
    print(f"\nмедіана без кешу: {m_no:.2f} с; з кешем: {m_c:.2f} с; прискорення x{m_no/m_c:.1f}; матеріалізація (медіана): {m_mat:.2f} с")
    print("Розділів кешу (всього / закешовано) та пам'ять, байт:", [(r[2], r[3], r[4]) for r in part_rows])
    print("Storage level:", df.storageLevel)

# Розподіл рядків за розділами кешу та крайовий ефект вікон
with timed("partition_sizes_and_window_boundary"):
    ps = df.groupBy(F.spark_partition_id().alias("partition_id")).count().orderBy("partition_id").toPandas()
    w60 = Window.partitionBy(*PART).orderBy("Timestamp").rowsBetween(-59, 0)
    bnd = df.withColumn("n60", F.count("log_ret").over(w60))
    n_null_ret = df.filter("log_ret is null").count()
    n_incomplete = bnd.filter("n60 < 60").count()
    n_vol_null = df.filter("vol60 is null").count()
    n_keys = df.select(*PART).distinct().count()
save(ps, "pz2_partition_sizes")
n_rows_proc = df.count()
save(pd.DataFrame([("window_partition_key", "+".join(PART)), ("partition_groups", n_keys), ("rows", n_rows_proc),
                   ("rdd_partitions", df.rdd.getNumPartitions()), ("non_empty_partitions", len(ps)),
                   ("log_ret_null_rows", n_null_ret), ("log_ret_null_rows_unpartitioned_window", 1),
                   ("rows_with_vol60_window_lt_60_points", n_incomplete), ("rows_with_vol60_window_lt_60_points_unpartitioned", 60),
                   ("rows_vol60_null", n_vol_null), ("share_rows_vol60_incomplete_pct", round(100 * n_incomplete / n_rows_proc, 4))],
                  columns=["metric", "value"]), "pz2_window_boundary")

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
with cap("06_derived", "похідні атрибути (травень 2021) та describe()"):
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
    wd = Window.orderBy("date")             # ~5,4 тис. добових рядків: один розділ не є проблемою
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

# C3: розподіл добової rv після 2023 року (для коректного цитування у звіті)
def rv_dist(sub, label):
    x = sub["rv"].dropna()
    return (label, len(x), x.min(), x.quantile(.05), x.quantile(.25), x.median(), x.quantile(.75), x.quantile(.95), x.max())
rvd = pd.DataFrame([rv_dist(daily[daily.date >= "2023-01-01"], "all days >= 2023-01-01"),
                    rv_dist(daily[(daily.date >= "2023-01-01") & (daily.is_complete == 1)], "complete days >= 2023-01-01"),
                    rv_dist(daily[(daily.date >= "2018-01-01")], "all days >= 2018-01-01")],
                   columns=["subset", "n", "min", "p05", "p25", "median", "p75", "p95", "max"])
save(rvd, "pz2_rv_after2023")

# 4.2 Найволатильніші дні (сортування + фільтрація)
comp = d1.filter("is_complete = 1")
with timed("top10_volatile_days"):
    top_rv = comp.orderBy(F.desc("rv")).limit(10).toPandas()
    top_hl = comp.orderBy(F.desc("hl_rel")).limit(10).toPandas()
    # варіант без «розріджених» днів: щонайменше 50 % хвилин з торгами (щоб виключити 2012 і частину 2013 з майже суцільними заповненнями)
    top_rv_active = comp.filter("flat_share < 0.5").orderBy(F.desc("rv")).limit(10).toPandas()
    top_hl_active = comp.filter("flat_share < 0.5").orderBy(F.desc("hl_rel")).limit(10).toPandas()
cols = ["date", "open", "high", "low", "close", "volume", "rv", "hl_rel", "ret_1d", "flat_share"]
save(top_rv[cols], "pz2_top10_rv"); save(top_hl[cols], "pz2_top10_hl_rel")
save(top_rv_active[cols], "pz2_top10_rv_active"); save(top_hl_active[cols], "pz2_top10_hl_rel_active")
with cap("07_top_days", "топ-10 найволатильніших діб (rv та (High-Low)/Open)"):
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
with cap("08_volume", "обсяг за годиною доби (UTC) і за роками"):
    print(vh.round(5).to_string(index=False)); print(); print(flat_by_year.round(2).to_string(index=False))

# 4.4 Погодинна таблиця ознак
with timed("hourly_features"):
    hh = (df.groupBy(F.date_trunc("hour", "datetime").alias("datetime_utc")).agg(
        F.min_by("Open", "Timestamp").alias("open"), F.max("High").alias("high"), F.min("Low").alias("low"),
        F.max_by("Close", "Timestamp").alias("close"), F.sum("Volume").alias("volume"),
        F.count("*").alias("n_minutes"), F.sum("is_flat").alias("n_flat"),
        F.sqrt(F.sum(F.col("log_ret") ** 2)).alias("rv"), F.stddev("log_ret").alias("std_ret_1m")))
    wh = Window.orderBy("datetime_utc")     # ~130 тис. погодинних рядків: один розділ допустимий
    hh = (hh.withColumn("ret_1h", F.log("close") - F.log(F.lag("close").over(wh)))
            .withColumn("range", F.col("high") - F.col("low")).withColumn("rel_range", (F.col("high") - F.col("low")) / F.col("open"))
            .withColumn("flat_share", F.col("n_flat") / F.col("n_minutes"))
            .withColumn("log_volume", F.log1p("volume"))
            .withColumn("vol_24h", F.stddev("ret_1h").over(wh.rowsBetween(-23, 0)))
            .withColumn("hour", F.hour("datetime_utc")).withColumn("dow", F.dayofweek("datetime_utc"))
            .withColumn("year", F.year("datetime_utc")))
    hourly = hh.orderBy("datetime_utc").toPandas()
hourly.to_parquet(f"{RES}/pz2_hourly_features.parquet", index=False)

# 4.5 Асиметрія та ексцес дохідностей на різних масштабах + стійкість до викидів (M7)
def moments(name, d, col, filt=None):
    x = d.filter(filt) if filt is not None else d
    r = x.filter(F.col(col).isNotNull()).agg(F.count(col).alias("n"), F.avg(col).alias("mean"), F.stddev(col).alias("std"),
                                              F.skewness(col).alias("skew"), F.kurtosis(col).alias("excess_kurtosis"),
                                              F.min(col).alias("min"), F.max(col).alias("max")).first().asDict()
    r["series"] = name; return r
with timed("skew_kurtosis"):
    ext = (df.filter("log_ret is not null").withColumn("abs_ret", F.abs("log_ret")).orderBy(F.desc("abs_ret")).limit(10)
             .select("Timestamp", "datetime", "Open", "High", "Low", "Close", "Volume", "log_ret").toPandas())
    top2, top10 = ext["Timestamp"].tolist()[:2], ext["Timestamp"].tolist()
    keep2, keep10 = ~F.col("Timestamp").isin(top2), ~F.col("Timestamp").isin(top10)
    rows = [moments("1 хв, усі свічки", df, "log_ret"),
            moments("1 хв, без 2 найбільших |r|", df, "log_ret", keep2),
            moments("1 хв, без 10 найбільших |r|", df, "log_ret", keep10),
            moments("1 хв, лише з торгами (Volume>0)", df, "log_ret", "is_flat = 0"),
            moments("1 хв, лише з торгами, без 10 найбільших |r|", df, "log_ret", (F.col("is_flat") == 0) & keep10),
            moments("1 год, усі", hh, "ret_1h"),
            moments("1 год, flat_share<0,5", hh, "ret_1h", "flat_share < 0.5"),
            moments("1 доба, усі", d1, "ret_1d"),
            moments("1 доба, flat_share<0,5", d1, "ret_1d", "flat_share < 0.5")]
mom = pd.DataFrame(rows)[["series", "n", "mean", "std", "skew", "excess_kurtosis", "min", "max"]]
save(mom, "pz2_skew_kurtosis")
save(ext, "pz2_top10_abs_minute_returns")
with cap("09_moments", "асиметрія й ексцес на 1 хв / 1 год / 1 добу та стійкість до викидів"):
    print(mom.to_string(index=False, float_format=lambda v: f"{v:.6g}"))
    print("\n10 найбільших за модулем хвилинних дохідностей:")
    print(ext.to_string(index=False, float_format=lambda v: f"{v:.6g}"))

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
Q6_DATES = "DATE'2020-03-12', DATE'2020-03-13', DATE'2021-05-19', DATE'2022-06-13', DATE'2022-11-08', DATE'2022-11-09'"
RATIO = ("ROUND(volume / AVG(volume) OVER (ORDER BY date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING),2)")
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
 # вікно рахується в підзапиті над усіма днями, а відбір дат – зовні (інакше вікно бачило б лише відібрані дати)
 "q6_crash_days_vs_30d_mean": f"""SELECT * FROM (
    SELECT date, ROUND(100*ret_1d,2) AS ret_pct, ROUND(rv,4) AS rv, ROUND(volume,0) AS volume, {RATIO} AS volume_x_prev30_mean
    FROM btc_daily) WHERE date IN ({Q6_DATES}) ORDER BY date""",
}
res_sql = {}
def run_sql(k, q):
    with timed(f"sql_{k}"):
        r = spark.sql(q).toPandas()
    res_sql[k] = r; save(r, f"pz2_sql_{k}")
    with cap(f"10_sql_{k}", f"результат запиту {k} (spark.sql)"):
        print(" ".join(q.split())[:300] + "\n"); print(r.head(25).to_string(index=False))
for k, q in SQLQ.items(): run_sql(k, q)

# Q7 (M6): УСІ доби з 2018 року, де обсяг перевищує поріг k × середній обсяг попередніх 30 діб (те саме правило, що в Q6)
K = float(res_sql["q6_crash_days_vs_30d_mean"]["volume_x_prev30_mean"].min())      # k = найменше відношення серед 6 відомих кризових діб
SQLQ["q7_volume_spike_days"] = f"""SELECT * FROM (
    SELECT date, ROUND(100*ret_1d,2) AS ret_pct, ROUND(rv,4) AS rv, ROUND(volume,0) AS volume, {RATIO} AS volume_x_prev30_mean,
           CASE WHEN date IN ({Q6_DATES}) THEN 1 ELSE 0 END AS in_q6_list
    FROM btc_daily) WHERE date >= DATE'2018-01-01' AND volume_x_prev30_mean >= {K} ORDER BY date"""
run_sql("q7_volume_spike_days", SQLQ["q7_volume_spike_days"])
q7 = res_sql["q7_volume_spike_days"]
n_days_2018 = int((daily.date >= "2018-01-01").sum())
big = q7["ret_pct"].abs() >= 10
pd.DataFrame([("k_threshold_min_ratio_of_q6_dates", K), ("days_since_2018_total", n_days_2018), ("spike_days", len(q7)),
              ("spike_days_share_of_all_days_pct", round(100 * len(q7) / n_days_2018, 2)),
              ("spike_days_with_abs_ret_ge_10pct", int(big.sum())), ("spike_days_with_abs_ret_lt_10pct", int((~big).sum())),
              ("spike_days_with_abs_ret_lt_5pct", int((q7["ret_pct"].abs() < 5).sum())),
              ("share_spike_days_with_abs_ret_ge_10pct", round(float(big.mean()), 4)),
              ("q6_list_dates_in_q7", int(q7["in_q6_list"].sum())),
              ("max_ratio", float(q7["volume_x_prev30_mean"].max())), ("median_ratio", float(q7["volume_x_prev30_mean"].median()))],
             columns=["metric", "value"]).to_csv(f"{RES}/pz2_sql_q7_summary.csv", index=False)

with open(f"{RES}/pz2_sql_queries.sql", "w", encoding="utf-8") as f:
    for k, q in SQLQ.items():
        f.write(f"-- {k}\n{' '.join(q.split())};\n\n")
with cap("11_sql_explain", "план виконання запиту q1 (explain formatted)"):
    print(explain_str(spark.sql(SQLQ["q1_year_stats"]), "formatted")[:3500])

# ---------- Завдання 6. Spark на кешованому DataFrame; підсумок порівняння рушіїв ----------
# (S/P-запуски без кешу виконано вище; тут – агрегація над даними в пам'яті, без читання CSV)
for run in (1, 2, 3):
    jobdesc(f"engine_cached_run{run}")
    t = time.perf_counter()
    df.groupBy("year").agg(F.count("*"), F.sum("Volume")).orderBy("year").collect(); dt1 = time.perf_counter() - t
    cmp_rows.append(("year_count_volume", "spark_cached_df", run, round(dt1, 3), "лише агрегація над кешем (16 розділів)"))
    t = time.perf_counter()
    df.groupBy("date").agg(F.min_by("Open", "Timestamp"), F.max("High"), F.min("Low"), F.max_by("Close", "Timestamp"), F.sum("Volume")).collect(); dt2 = time.perf_counter() - t
    cmp_rows.append(("daily_ohlc", "spark_cached_df", run, round(dt2, 3), "лише агрегація над кешем (16 розділів)"))
cmp_rows.append(("year_count_volume", "hadoop_streaming", "", "[ЗАПОВНИТИ ПІСЛЯ ЗАПУСКУ]", "значення з окремого бенчмарку Hadoop"))
cmp_rows.append(("daily_ohlc", "hadoop_streaming", "", "[ЗАПОВНИТИ ПІСЛЯ ЗАПУСКУ]", "значення з окремого бенчмарку Hadoop"))
cmp = pd.DataFrame(cmp_rows, columns=["task", "engine", "run", "seconds", "note"])
save(cmp, "pz2_engine_comparison")
num = cmp[pd.to_numeric(cmp["seconds"], errors="coerce").notna()].copy(); num["seconds"] = num["seconds"].astype(float)
summ = num.groupby(["task", "engine"])["seconds"].agg(["median", "min", "max"]).round(3).reset_index()
save(summ, "pz2_engine_comparison_summary")
for _, r in summ.iterrows():
    rec(f"engine_{r['task']}_{r['engine']}", r["median"], "медіана з 3 запусків")
rec("engine_year_count_volume_hadoop_streaming", float("nan"), "[ЗАПОВНИТИ ПІСЛЯ ЗАПУСКУ]")
rec("engine_daily_ohlc_hadoop_streaming", float("nan"), "[ЗАПОВНИТИ ПІСЛЯ ЗАПУСКУ]")
with cap("12_engines", "Spark проти pandas (порядок S,P,S,P,S,P), медіани, звірка результатів"):
    print(cmp.to_string(index=False)); print(); print(summ.to_string(index=False))
    print("\nЗвірка результатів двох рушіїв (assert):"); print(pd.read_csv(f"{RES}/pz2_engine_equality.csv").to_string(index=False))

# ---------- Знімки Spark UI, поки SparkSession жива ----------
def ui_screens():
    from playwright.sync_api import sync_playwright
    pages = {"jobs": "jobs/", "stages": "stages/", "storage": "storage/", "sql": "SQL/", "executors": "executors/", "environment": "environment/"}
    exe = "/opt/pw-browsers/chromium-1243/chrome-linux64/chrome"
    sql_list = rest("sql?details=false&length=1000")
    def exec_id(label):
        for e in sql_list:
            if label in (e.get("description") or ""): return e["id"]
    with sync_playwright() as p:
        b = p.chromium.launch(executable_path=exe, args=["--no-sandbox"])
        pg = b.new_page(viewport={"width": 1500, "height": 950})
        for k, u in pages.items():
            pg.goto(f"{sc.uiWebUrl}/{u}", wait_until="load"); pg.wait_for_timeout(1500)
            pg.screenshot(path=f"{SHOTS}/spark_ui_{k}.png")
        # деталі SQL-запиту: DAG обрізається до області графа та знімається з подвійним масштабом
        pg2 = b.new_page(viewport={"width": 1600, "height": 1000}, device_scale_factor=2)
        for label, fn in (("lazy_action_noop_write_run1", "spark_ui_sql_detail"), ("sql_q5_max_volume_day_per_year", "spark_ui_sql_detail_q5")):
            i = exec_id(label)
            if i is None: print("SQL execution not found:", label); continue
            pg2.goto(f"{sc.uiWebUrl}/SQL/execution/?id={i}", wait_until="load")
            pg2.wait_for_selector("#plan-viz-graph svg", timeout=30000); pg2.wait_for_timeout(2500)
            pg2.locator("#plan-viz-graph").screenshot(path=f"{SHOTS}/{fn}_dag.png")
            pg2.screenshot(path=f"{SHOTS}/{fn}.png", full_page=False)
        b.close()
ui_screens()

rec("TOTAL_script_runtime", time.perf_counter() - t0_all)
pd.DataFrame(TIMINGS, columns=["step", "seconds", "note"]).to_csv(f"{RES}/timings.csv", index=False)
spark.stop()
print("OK")
