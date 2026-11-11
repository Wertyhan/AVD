# -*- coding: utf-8 -*-
"""ПЗ5. Apache Spark MLlib: прогноз «чи буде наступна година високоволатильною» (BTC/USD).
Запуск: python3 pz5_spark.py   (результати -> ../results, скріншоти -> ../screenshots)
"""
import os, sys, io, time, platform, subprocess, contextlib, itertools, json
import numpy as np
import pandas as pd

SEED = 42
np.random.seed(SEED)
BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
RES, SHOTS, TXT = f"{BASE}/results", f"{BASE}/screenshots", f"{BASE}/screenshots/txt"
SRC = "/home/user/AVD/work/pz2/results/pz2_hourly_features.parquet"   # результат попередньої обробки (ПЗ2)
for d in (RES, SHOTS, TXT, f"{RES}/model"):
    os.makedirs(d, exist_ok=True)

from pyspark.sql import SparkSession, Window, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import (Imputer, StringIndexer, OneHotEncoder, VectorAssembler,
                                StandardScaler, SQLTransformer)
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.functions import vector_to_array
import pyspark

# ---------- допоміжні засоби ----------
TIMINGS = []
def rec(step, secs, note=""):
    TIMINGS.append((step, round(secs, 3), note)); print(f"[timing] {step}: {secs:.2f} с {note}")

class Tee(io.StringIO):
    def write(self, s):
        sys.__stdout__.write(s); return super().write(s)

@contextlib.contextmanager
def cap(name, cmd):
    """Дублює stdout у screenshots/txt/<name>.txt із заголовком '$ команда' (для рендеру PNG)."""
    buf = Tee()
    with contextlib.redirect_stdout(buf):
        yield
    with open(f"{TXT}/{name}.txt", "w", encoding="utf-8") as f:
        f.write(f"$ {cmd}\n" + buf.getvalue())

def save(df, name):
    df.to_csv(f"{RES}/{name}.csv", index=False, encoding="utf-8")

T_ALL = time.perf_counter()
spark = (SparkSession.builder.appName("PZ5_BTC_MLlib").master("local[*]")
         .config("spark.driver.memory", "8g")
         .config("spark.sql.shuffle.partitions", "16")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.port", "4040").config("spark.ui.showConsoleProgress", "false")
         .config("spark.sql.ansi.enabled", "false")
         .getOrCreate())
sc = spark.sparkContext; sc.setLogLevel("ERROR")

def sh(c):
    return subprocess.run(c, shell=True, capture_output=True, text=True).stdout.strip()
env = [("Java", sh("java -version 2>&1 | grep -v Picked | head -1")), ("Spark", spark.version),
       ("PySpark", pyspark.__version__), ("Python", platform.python_version()), ("pandas", pd.__version__),
       ("numpy", np.__version__), ("OS", platform.platform()),
       ("CPU", sh("lscpu | grep 'Model name' | sed 's/.*: *//'")), ("CPU_logical_cores", str(os.cpu_count())),
       ("RAM_GB", sh("free -g | awk '/Mem:/{print $2}'")), ("spark.master", sc.master),
       ("spark.driver.memory", "8g"), ("spark.sql.shuffle.partitions", "16"), ("seed", str(SEED)),
       ("spark.ui.url", sc.uiWebUrl)]
pd.DataFrame(env, columns=["parameter", "value"]).to_csv(f"{RES}/env_versions.csv", index=False)
with cap("01_env", "python3 -c 'import pyspark; print(pyspark.__version__)'; java -version; nproc; free -g"):
    for k, v in env: print(f"{k:<22}{v}")

# ---------- Крок 1. Завантаження результатів попередньої обробки (ПЗ2) ----------
t = time.perf_counter()
raw = spark.read.parquet(SRC).orderBy("datetime_utc")
n_raw = raw.count()
rec("load_parquet_pz2", time.perf_counter() - t, f"{n_raw} годинних рядків")
with cap("02_schema", "df = spark.read.parquet('pz2_hourly_features.parquet'); df.printSchema(); df.show(3)"):
    raw.printSchema(); print("rows:", n_raw)
    raw.select("datetime_utc", "close", "volume", "n_minutes", "rv", "ret_1h", "flat_share", "year").show(3, False)

# ---------- Крок 2. Обґрунтування вибору періоду та задачі (з даних) ----------
flat_year = (raw.groupBy("year").agg(F.count("*").alias("hours"), F.round(100 * F.sum("n_flat") / F.sum("n_minutes"), 2).alias("flat_share_pct"),
                                     F.round(F.avg("rv"), 5).alias("mean_rv"), F.round(F.expr("percentile_approx(rv,0.5)"), 5).alias("median_rv"),
                                     F.sum((F.col("volume") == 0).cast("int")).alias("zero_vol_hours")).orderBy("year").toPandas())
save(flat_year, "pz5_flat_share_by_year")
START_YEAR = 2018   # з 2018 р. частка «пласких» хвилин < 4 % щороку (2017: 8,04 %, 2016: 33,19 %)
with cap("03_period", "df.groupBy('year').agg(flat share, mean rv)  # вибір періоду моделювання"):
    print(flat_year.to_string(index=False)); print(f"\nПеріод моделювання: з {START_YEAR} р.")

# ---------- Крок 3. Формування ознак і цільової змінної (лише з минулого) ----------
HI_WIN, PCT = 720, 0.75     # поріг «висока волатильність» = 75-й перцентиль rv за останні 720 год (30 діб), включно з поточною годиною
EMB = 24                    # ембарго (год) між навчальною та валід./тестовою частинами
w_all = Window.orderBy("datetime_utc")
def roll(n): return w_all.rowsBetween(-(n - 1), 0)
EPS = 1e-5
d = raw.withColumn("log_rv", F.log(F.col("rv") + EPS))
for k in (1, 2, 5, 23): d = d.withColumn(f"log_rv_l{k}", F.lag("log_rv", k).over(w_all))
d = d.withColumn("log_rv_l0", F.col("log_rv"))
for n in (3, 6, 24, 168): d = d.withColumn(f"log_rv_m{n}", F.log(F.avg("rv").over(roll(n)) + EPS))
d = (d.withColumn("thr_rv", F.expr(f"percentile_approx(rv, {PCT}, 10000)").over(roll(HI_WIN)))   # лише рядки <= t
       .withColumn("log_rv_over_thr", F.log(F.col("rv") + EPS) - F.log(F.col("thr_rv") + EPS))
       .withColumn("abs_ret_1h", F.abs("ret_1h"))
       .withColumn("ret_6h", F.log(F.col("close") / F.lag("close", 6).over(w_all)))
       .withColumn("log_rel_range", F.log(F.col("rel_range") + EPS))
       .withColumn("vol_z168", (F.col("log_volume") - F.avg("log_volume").over(roll(168))) / (F.stddev("log_volume").over(roll(168)) + 1e-9))
       .withColumn("next_rv", F.lead("rv", 1).over(w_all))
       .withColumn("next_ret", F.lead("ret_1h", 1).over(w_all))
       .withColumn("label", (F.col("next_rv") > F.col("thr_rv")).cast("int"))
       .withColumn("label_dir", F.when(F.col("next_ret") > 0, 1).when(F.col("next_ret") < 0, 0))
       .withColumn("persist", (F.col("rv") > F.col("thr_rv")).cast("double")))
NUM = ["log_rv_l0", "log_rv_l1", "log_rv_l2", "log_rv_l5", "log_rv_l23", "log_rv_m3", "log_rv_m6", "log_rv_m24", "log_rv_m168",
       "log_rv_over_thr", "ret_1h", "abs_ret_1h", "ret_6h", "log_rel_range", "log_volume", "vol_z168", "flat_share"]
base_cols = ["datetime_utc", "year", "hour", "dow", "n_minutes", "label", "label_dir", "persist", "rv", "next_rv", "thr_rv", "next_ret"] + NUM
t = time.perf_counter()
m = d.select(*base_cols)
# кількість пропусків ДО фільтрації (розігрів вікон на початку історії)
nulls_before = m.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in NUM + ["label"]]).toPandas().T.reset_index()
nulls_before.columns = ["column", "nulls_full_history"]
m = (m.filter((F.col("year") >= START_YEAR) & (F.col("n_minutes") == 60) & F.col("label").isNotNull())   # неповні годинні бари та остання година виключені
       .withColumn("rn", F.row_number().over(Window.orderBy("datetime_utc"))).cache())
N = m.count()
rec("feature_engineering", time.perf_counter() - t, f"{N} рядків моделювання")
nulls_after = m.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in NUM + ["label"]]).toPandas().T.reset_index()
nulls_after.columns = ["column", "nulls_model_rows"]
save(nulls_before.merge(nulls_after, on="column"), "pz5_missing_values")

# ---------- Chronological split 80/20 ----------
n_train = int(N * 0.8)
bounds = m.filter(F.col("rn").isin(1, n_train, n_train + 1, N)).select("rn", "datetime_utc").orderBy("rn").toPandas()
train = m.filter(F.col("rn") <= n_train - EMB).cache()      # ембарго: останні 24 год навчальної частини вилучено
test = m.filter(F.col("rn") > n_train).cache()
n_tr, n_te = train.count(), test.count()
sp = pd.DataFrame([("rows_model_total", N), ("train_rows", n_tr), ("embargo_rows", EMB), ("test_rows", n_te),
                   ("train_share_pct", round(100 * (n_train) / N, 2)), ("train_start", str(bounds.datetime_utc[0])),
                   ("train_end_nominal", str(bounds.datetime_utc[1])), ("test_start", str(bounds.datetime_utc[2])), ("test_end", str(bounds.datetime_utc[3]))],
                  columns=["parameter", "value"])
save(sp, "pz5_split")
lab = {}
for nm, df in (("train", train), ("test", test)):
    r = df.agg(F.avg("label").alias("pos"), F.avg("label_dir").alias("up")).first(); lab[nm] = (r["pos"], r["up"])
# Дрейф: частка позитивних за роками при ковзному порозі (основний) і при глобальному пороговому значенні з навчальної вибірки
glob_thr = train.approxQuantile("next_rv", [PCT], 1e-4)[0]
yr = (m.withColumn("label_global", (F.col("next_rv") > glob_thr).cast("int")).groupBy("year")
        .agg(F.count("*").alias("hours"), F.round(F.avg("label"), 4).alias("pos_share_rolling_thr"),
             F.round(F.avg("label_global"), 4).alias("pos_share_global_thr"), F.round(F.avg("label_dir"), 4).alias("up_share_next_hour")).orderBy("year").toPandas())
save(yr, "pz5_target_share_by_year")
tgt_over_time = m.select("datetime_utc", "year", "label", "rv", "next_rv", "thr_rv").toPandas()
tgt_over_time.to_csv(f"{RES}/pz5_target_series.csv", index=False)
with cap("04_split", "chronological split 80/20 + target; train.count(), test.count()"):
    print(sp.to_string(index=False))
    print(f"\nДоля позитивного класу: train={lab['train'][0]:.4f}  test={lab['test'][0]:.4f}")
    print(f"Глобальний поріг rv (75-й перцентиль train) = {glob_thr:.5f}")
    print(yr.to_string(index=False))

# ---------- Чому не напрям ціни: ACF та базові лінії ----------
pdf = m.select("datetime_utc", "ret_1h", "label_dir", "next_ret").toPandas().sort_values("datetime_utc")
r = pdf.ret_1h.values; ar = np.abs(r)
def acf(x, k):
    x = x - x.mean(); return float((x[:-k] * x[k:]).sum() / (x * x).sum())
nn = len(r); conf = 1.96 / np.sqrt(nn)
acf_df = pd.DataFrame({"lag_h": range(1, 73), "acf_ret": [acf(r, k) for k in range(1, 73)], "acf_abs_ret": [acf(ar, k) for k in range(1, 73)]})
acf_df["conf95"] = conf
save(acf_df, "pz5_acf")
dirv = pdf.dropna(subset=["label_dir"])
up_share = dirv.label_dir.mean()
sign_persist_acc = float(((np.sign(dirv.ret_1h) > 0).astype(int) == dirv.label_dir)[dirv.ret_1h != 0].mean())
dir_base = pd.DataFrame([("n_hours", nn), ("conf95_bound", conf), ("acf_ret_lag1", acf_df.acf_ret[0]), ("acf_abs_ret_lag1", acf_df.acf_abs_ret[0]),
                         ("acf_abs_ret_lag24", acf_df.acf_abs_ret[23]), ("acf_abs_ret_lag168_not_shown", acf(ar, 168)),
                         ("up_share_next_hour", up_share), ("majority_accuracy_direction", max(up_share, 1 - up_share)),
                         ("sign_persistence_accuracy_direction", sign_persist_acc)], columns=["metric", "value"])
with cap("05_acf", "acf(ret_1h) vs acf(|ret_1h|), baseline of the direction task"):
    print(dir_base.to_string(index=False))
    print("\nlag  ACF(ret)  ACF(|ret|)"); print(acf_df.head(6).round(4).to_string(index=False))

# ---------- Пайплайни ----------
# SQLTransformer: рядкові (без вікон) похідні ознаки всередині конвеєра: циклічний час доби, рядкова мітка дня тижня
SQLT = SQLTransformer(statement="SELECT *, sin(2*pi()*hour/24.0) AS hour_sin, cos(2*pi()*hour/24.0) AS hour_cos, "
                                "CAST(dow AS STRING) AS dow_str, CASE WHEN dow IN (1,7) THEN 1.0 ELSE 0.0 END AS is_weekend FROM __THIS__")
CYC = ["hour_sin", "hour_cos", "is_weekend"]
FEATS = NUM
def stages(model, scale):
    st = [SQLT,
          Imputer(inputCols=FEATS, outputCols=[c + "_imp" for c in FEATS], strategy="median"),     # пропуски -> медіана train
          StringIndexer(inputCol="dow_str", outputCol="dow_idx", handleInvalid="keep"),
          OneHotEncoder(inputCols=["dow_idx"], outputCols=["dow_ohe"], handleInvalid="keep", dropLast=False),  # категоріальна ознака
          VectorAssembler(inputCols=[c + "_imp" for c in FEATS] + CYC + ["dow_ohe"], outputCol="features_raw", handleInvalid="keep")]
    if scale:
        st.append(StandardScaler(inputCol="features_raw", outputCol="features", withMean=True, withStd=True))
    else:
        st.append(SQLTransformer(statement="SELECT *, features_raw AS features FROM __THIS__"))
    return st + [model]
def mk(name, p):
    if name == "LR":
        return Pipeline(stages=stages(LogisticRegression(featuresCol="features", labelCol="label", maxIter=200, regParam=p["regParam"],
                                                         elasticNetParam=p["elasticNetParam"], standardization=False), True))
    if name == "RF":
        return Pipeline(stages=stages(RandomForestClassifier(featuresCol="features", labelCol="label", seed=SEED, numTrees=p["numTrees"],
                                                             maxDepth=p["maxDepth"], minInstancesPerNode=p["minInstancesPerNode"], subsamplingRate=0.8), False))
    return Pipeline(stages=stages(GBTClassifier(featuresCol="features", labelCol="label", seed=SEED, maxIter=p["maxIter"], maxDepth=p["maxDepth"],
                                                stepSize=p["stepSize"], subsamplingRate=0.8), False))
GRIDS = {"LR": {"regParam": [0.0, 0.001, 0.01, 0.1], "elasticNetParam": [0.0, 0.5]},
         "RF": {"numTrees": [100, 200], "maxDepth": [4, 8], "minInstancesPerNode": [20, 100]},
         "GBT": {"maxIter": [50, 100], "maxDepth": [3, 5], "stepSize": [0.05, 0.1]}}
def combos(g): return [dict(zip(g, v)) for v in itertools.product(*g.values())]
with cap("06_pipeline", "pipeline = Pipeline(stages=[SQLTransformer, Imputer, StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler, LogisticRegression])"):
    for nm in ("LR", "RF", "GBT"):
        pl = mk(nm, combos(GRIDS[nm])[0])
        print(f"{nm}: " + " -> ".join(type(s).__name__ for s in pl.getStages()))
    print(f"\nОзнаки (числові, {len(FEATS)}): {FEATS}\nЦиклічні/бінарні: {CYC}; категоріальна: dow -> OneHot(7)")

# ---------- Метрики ----------
ev_roc = BinaryClassificationEvaluator(rawPredictionCol="p1", labelCol="label", metricName="areaUnderROC")
ev_pr = BinaryClassificationEvaluator(rawPredictionCol="p1", labelCol="label", metricName="areaUnderPR")
def with_p1(pred):
    return pred.withColumn("p1", vector_to_array("probability")[1])
def metrics(df, thr=0.5):
    """df має p1 (оцінка класу 1) та label. Повертає всі метрики; precision/recall/F1 – для класу «висока волатильність» (1)."""
    q = df.withColumn("yhat", (F.col("p1") >= thr).cast("int"))
    c = q.groupBy("label", "yhat").count().toPandas()
    g = lambda a, b: int(c[(c.label == a) & (c.yhat == b)]["count"].sum())
    tn, fp, fn, tp = g(0, 0), g(0, 1), g(1, 0), g(1, 1)
    n = tn + fp + fn + tp
    prec = tp / (tp + fp) if tp + fp else 0.0; rec_ = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * prec * rec_ / (prec + rec_) if prec + rec_ else 0.0
    spec = tn / (tn + fp) if tn + fp else 0.0
    mcc_d = np.sqrt(float(tp + fp) * (tp + fn) * (tn + fp) * (tn + fn))
    return dict(threshold=thr, accuracy=(tp + tn) / n, precision=prec, recall=rec_, f1=f1, balanced_accuracy=(rec_ + spec) / 2,
                mcc=((tp * tn - fp * fn) / mcc_d) if mcc_d else 0.0, roc_auc=ev_roc.evaluate(df), pr_auc=ev_pr.evaluate(df),
                tn=tn, fp=fp, fn=fn, tp=tp, n=n)

# ---------- Часова валідація: розширюване вікно (3 фолди) з ембарго ----------
FOLDS = [(0.50, 0.667), (0.667, 0.833), (0.833, 1.0)]
cuts = [(int(n_tr * a), int(n_tr * b)) for a, b in FOLDS]
def fold_data(i):
    a, b = cuts[i]
    return train.filter(F.col("rn") <= a - EMB), train.filter((F.col("rn") > a) & (F.col("rn") <= b))
tune_rows, best = [], {}
t_tune = time.perf_counter()
for nm in ("LR", "RF", "GBT"):
    for p in combos(GRIDS[nm]):
        sc_roc, sc_pr = [], []
        t = time.perf_counter()
        for i in range(3):
            tr_i, va_i = fold_data(i)
            pr_ = with_p1(mk(nm, p).fit(tr_i).transform(va_i))
            sc_roc.append(ev_roc.evaluate(pr_)); sc_pr.append(ev_pr.evaluate(pr_))
        tune_rows.append(dict(model=nm, **{k: p.get(k) for k in ("regParam", "elasticNetParam", "numTrees", "maxDepth", "minInstancesPerNode", "maxIter", "stepSize")},
                              cv_roc_auc=np.mean(sc_roc), cv_pr_auc=np.mean(sc_pr), cv_pr_auc_std=np.std(sc_pr), seconds=time.perf_counter() - t))
        print(f"[tune] {nm} {p} PR-AUC={np.mean(sc_pr):.4f} ROC-AUC={np.mean(sc_roc):.4f}")
rec("hyperparameter_tuning_all", time.perf_counter() - t_tune, "3 моделі, 3 фолди, ембарго 24 год")
tune = pd.DataFrame(tune_rows); save(tune, "pz5_tuning")
for nm in ("LR", "RF", "GBT"):
    sub = tune[tune.model == nm].sort_values("cv_pr_auc", ascending=False).iloc[0]
    best[nm] = {k: (int(sub[k]) if k in ("numTrees", "maxDepth", "minInstancesPerNode", "maxIter") else float(sub[k]))
                for k in GRIDS[nm]}
json.dump(best, open(f"{RES}/pz5_best_params.json", "w"), indent=1)
with cap("07_tuning", "tuning: expanding-window 3-fold (PR-AUC), best params per model"):
    for nm in ("LR", "RF", "GBT"):
        s = tune[tune.model == nm].sort_values("cv_pr_auc", ascending=False)
        print(f"\n{nm}: найкраще {best[nm]}"); print(s.head(3).drop(columns=["seconds"]).round(4).to_string(index=False))

# ---------- Вибір порогу рішення на останньому валідаційному фолді (без доступу до test) ----------
thr_grid = np.round(np.arange(0.05, 0.96, 0.01), 2)
chosen_thr, thr_rows = {}, []
tr_v, va_v = fold_data(2)
for nm in ("LR", "RF", "GBT"):
    pv = with_p1(mk(nm, best[nm]).fit(tr_v).transform(va_v)).select("label", "p1").toPandas()
    f1s = []
    for th in thr_grid:
        yh = (pv.p1 >= th).astype(int); tp = ((yh == 1) & (pv.label == 1)).sum(); fp = ((yh == 1) & (pv.label == 0)).sum(); fn = ((yh == 0) & (pv.label == 1)).sum()
        pr = tp / (tp + fp) if tp + fp else 0; rc = tp / (tp + fn) if tp + fn else 0
        f1s.append(2 * pr * rc / (pr + rc) if pr + rc else 0)
        thr_rows.append(dict(model=nm, threshold=th, val_f1=f1s[-1], val_precision=pr, val_recall=rc))
    chosen_thr[nm] = float(thr_grid[int(np.argmax(f1s))])
save(pd.DataFrame(thr_rows), "pz5_threshold_validation")

# ---------- Остаточне навчання на train і оцінка на test ----------
models, preds, rows, timing_models = {}, {}, [], []
for nm in ("LR", "RF", "GBT"):
    t = time.perf_counter(); pm = mk(nm, best[nm]).fit(train); t_fit = time.perf_counter() - t
    t = time.perf_counter(); pr_ = with_p1(pm.transform(test)).cache(); n_ = pr_.count(); t_inf = time.perf_counter() - t
    models[nm] = pm; preds[nm] = pr_
    timing_models.append(dict(model=nm, fit_seconds=t_fit, inference_seconds_test=t_inf, test_rows=n_, inference_rows_per_sec=n_ / t_inf))
    rec(f"fit_{nm}", t_fit, f"train {n_tr} рядків"); rec(f"inference_{nm}", t_inf, f"test {n_te} рядків")
save(pd.DataFrame(timing_models), "pz5_model_runtimes")
# Базові лінії
base_test = test.withColumn("p1", F.lit(0.0))
persist_test = test.withColumn("p1", F.col("persist"))
ratio_test = test.withColumn("p1", F.col("log_rv_over_thr"))
major_train = 0 if lab["train"][0] < 0.5 else 1
all_rows = []
def add(name, df, thr, kind):
    mt = metrics(df, thr); mt.update(model=name, kind=kind); all_rows.append(mt)
add("Мажоритарний клас (завжди 0)", base_test, 0.5, "baseline")
add("Персистентність (rv_t > порогу)", persist_test, 0.5, "baseline")
add("Ранжування за log(rv_t/поріг), поріг 0", ratio_test, 0.0, "baseline")
for nm in ("LR", "RF", "GBT"):
    add(f"{nm} (поріг 0,5)", preds[nm], 0.5, "model")
    add(f"{nm} (поріг {chosen_thr[nm]:.2f}, з валідації)", preds[nm], chosen_thr[nm], "model_tuned_thr")
res = pd.DataFrame(all_rows)[["model", "kind", "threshold", "accuracy", "precision", "recall", "f1", "balanced_accuracy", "mcc", "roc_auc", "pr_auc", "tn", "fp", "fn", "tp", "n"]]
save(res, "pz5_metrics_test")
with cap("08_metrics", "evaluate(test): accuracy, precision, recall, F1, ROC-AUC, PR-AUC for baselines and models"):
    print(f"Частка класу 1 у test: {lab['test'][0]:.4f}; пороги з валідації: {chosen_thr}")
    print(res.drop(columns=["kind", "n"]).round(4).to_string(index=False))
    print("\nЧас, с:"); print(pd.DataFrame(timing_models).round(3).to_string(index=False))

# ---------- Збереження прогнозів ----------
pcols = test.select("datetime_utc", "year", "hour", "dow", "label", "persist", "log_rv_over_thr", "log_rv_l0", "rv", "next_rv", "thr_rv").toPandas()
for nm in ("LR", "RF", "GBT"):
    pcols[f"p_{nm}"] = preds[nm].select("datetime_utc", "p1").toPandas().set_index("datetime_utc").loc[pcols.datetime_utc, "p1"].values
pcols = pcols.sort_values("datetime_utc")
pcols.to_csv(f"{RES}/pz5_predictions_test_full.csv", index=False)
pcols.head(200).to_csv(f"{RES}/pz5_predictions_sample.csv", index=False)
json.dump(chosen_thr, open(f"{RES}/pz5_chosen_thresholds.json", "w"))

# ---------- Важливість ознак / коефіцієнти ----------
def feat_names(pm):
    sch = pm.transform(test.limit(5)).schema["features_raw"].metadata["ml_attr"]["attrs"]
    items = [x for grp in sch.values() for x in grp]; items.sort(key=lambda x: x["idx"])
    return [x["name"].replace("_imp", "") for x in items]
names = feat_names(models["RF"])
imp = pd.DataFrame({"feature": names})
imp["RF_importance"] = models["RF"].stages[-1].featureImportances.toArray()
imp["GBT_importance"] = models["GBT"].stages[-1].featureImportances.toArray()
imp["LR_coef_standardized"] = models["LR"].stages[-1].coefficients.toArray()
imp["LR_abs_coef"] = imp.LR_coef_standardized.abs()
save(imp.sort_values("RF_importance", ascending=False), "pz5_feature_importance")
with cap("09_importance", "model.stages[-1].featureImportances / coefficients"):
    print(imp.sort_values("RF_importance", ascending=False).round(4).to_string(index=False))
    print("LR intercept:", round(models["LR"].stages[-1].intercept, 4))

# ---------- Випадковий поділ проти хронологічного (ілюстрація витоку) ----------
rnd_tr, rnd_te = m.randomSplit([0.8, 0.2], seed=SEED)
cmp_rows = []
for nm in ("LR", "RF", "GBT"):
    pm = mk(nm, best[nm]).fit(rnd_tr); pr_ = with_p1(pm.transform(rnd_te))
    r_ = metrics(pr_, 0.5); r_.update(model=nm, split="random 80/20 (shuffle)", n_test=r_["n"]); cmp_rows.append(r_)
    c_ = metrics(preds[nm], 0.5); c_.update(model=nm, split="chronological 80/20", n_test=c_["n"]); cmp_rows.append(c_)
cmp = pd.DataFrame(cmp_rows)[["model", "split", "accuracy", "precision", "recall", "f1", "roc_auc", "pr_auc", "n_test"]]
save(cmp, "pz5_random_vs_chrono")
with cap("10_random_vs_chrono", "random split vs chronological split (same models and hyperparameters)"):
    print(cmp.round(4).to_string(index=False))

# ---------- Перевірка «напрямок ціни» (лише LR, для обґрунтування вибору задачі) ----------
dir_tr, dir_te = train.filter(F.col("label_dir").isNotNull()), test.filter(F.col("label_dir").isNotNull())
ev_d = BinaryClassificationEvaluator(rawPredictionCol="p1", labelCol="label_dir", metricName="areaUnderROC")
st_ = stages(LogisticRegression(featuresCol="features", labelCol="label_dir", maxIter=200, regParam=0.01, standardization=False), True)
pd_ = with_p1(Pipeline(stages=st_).fit(dir_tr).transform(dir_te))
acc_d = pd_.select(F.avg(((F.col("p1") >= 0.5).cast("int") == F.col("label_dir")).cast("double"))).first()[0]
maj_d = dir_te.agg(F.avg("label_dir")).first()[0]
dir_res = pd.DataFrame([("LR direction: ROC-AUC", ev_d.evaluate(pd_)), ("LR direction: accuracy", acc_d),
                        ("majority accuracy (test)", max(maj_d, 1 - maj_d)), ("up share test", maj_d)], columns=["metric", "value"])
save(pd.concat([dir_base, dir_res]), "pz5_direction_check")
with cap("11_direction", "LogisticRegression on label = (next-hour return > 0): control experiment"):
    print(dir_res.round(4).to_string(index=False))

# ---------- Збереження найкращої моделі ----------
best_name = res[res.kind != "baseline"].sort_values("pr_auc", ascending=False).iloc[0]["model"].split(" ")[0]
t = time.perf_counter(); models[best_name].write().overwrite().save(f"{RES}/model/best_pipeline_{best_name}")
rec("save_best_pipeline", time.perf_counter() - t, best_name)
from pyspark.ml import PipelineModel
t = time.perf_counter(); reloaded = PipelineModel.load(f"{RES}/model/best_pipeline_{best_name}")
chk = with_p1(reloaded.transform(test)).agg(F.max("p1")).first()[0]; rec("load_best_pipeline", time.perf_counter() - t, f"max p1={chk:.4f}")
pd.DataFrame([("best_model_by_test_pr_auc", best_name)], columns=["parameter", "value"]).to_csv(f"{RES}/pz5_best_model.csv", index=False)
with cap("12_model_saved", f"model.write().save('results/model/best_pipeline_{best_name}'); PipelineModel.load(...)"):
    print("збережено:", f"results/model/best_pipeline_{best_name}"); print(sh(f"ls {RES}/model/best_pipeline_{best_name}"))
    print("етапи:", [type(s).__name__ for s in reloaded.stages])

# ---------- Таймінги та Spark UI ----------
rec("total_script", time.perf_counter() - T_ALL)
pd.DataFrame(TIMINGS, columns=["step", "seconds", "note"]).to_csv(f"{RES}/timings.csv", index=False)
try:
    from playwright.sync_api import sync_playwright
    pages = {"jobs": "jobs/", "stages": "stages/", "sql": "SQL/", "executors": "executors/", "environment": "environment/", "storage": "storage/"}
    with sync_playwright() as p:
        b = p.chromium.launch(executable_path="/opt/pw-browsers/chromium-1243/chrome-linux64/chrome", args=["--no-sandbox"])
        pg = b.new_page(viewport={"width": 1500, "height": 950})
        for k, u in pages.items():
            pg.goto(f"http://localhost:4040/{u}", wait_until="load"); pg.wait_for_timeout(1200)
            pg.screenshot(path=f"{SHOTS}/spark_ui_{k}.png")
        b.close()
except Exception as e:
    print("Spark UI screenshot failed:", e)
spark.stop()
