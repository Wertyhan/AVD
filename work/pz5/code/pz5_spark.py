# -*- coding: utf-8 -*-
"""ПЗ5. Apache Spark MLlib: прогноз «чи буде наступна година високоволатильною» (BTC/USD).
Запуск (з каталогу pz5):  python code/pz5_spark.py      (seed 42)
Результати -> results/, скріншоти терміналів (txt) -> screenshots/txt, знімки Spark UI -> screenshots/
"""
import os, sys, time, platform, subprocess, itertools, json, shutil
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pz5_common import BASE, RES, SHOTS, TXT, SEED, cap as _cap, save
np.random.seed(SEED)
SRC = "/home/user/AVD/work/pz2/results/pz2_hourly_features.parquet"   # результат попередньої обробки (ПЗ2, чистий перезапуск)
SCRIPT = "pz5_spark.py"
def cap(name, desc): return _cap(name, SCRIPT, desc)
os.makedirs(f"{RES}/model", exist_ok=True)

from pyspark.sql import SparkSession, Window, functions as F
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import (Imputer, StringIndexer, OneHotEncoder, VectorAssembler,
                                StandardScaler, SQLTransformer)
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.functions import vector_to_array
import pyspark

# ---------- допоміжні засоби ----------
TIMINGS, LOAD = [], []
def rec(step, secs, note=""):
    TIMINGS.append((step, round(secs, 3), note)); print(f"[timing] {step}: {secs:.2f} с {note}")
def sh(c):
    return subprocess.run(c, shell=True, capture_output=True, text=True).stdout.strip()
def loadavg(step):
    l1, l5, l15 = open("/proc/loadavg").read().split()[:3]
    top = sh("ps -eo pcpu,comm --sort=-pcpu --no-headers | head -4 | awk '{printf \"%s:%s \", $2, $1}'")
    LOAD.append((step, time.strftime("%Y-%m-%d %H:%M:%S"), float(l1), float(l5), float(l15), os.cpu_count(), top.strip()))

T_ALL = time.perf_counter()
loadavg("start")
spark = (SparkSession.builder.appName("PZ5_BTC_MLlib").master("local[*]")
         .config("spark.driver.memory", "8g")
         .config("spark.sql.shuffle.partitions", "16")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.port", "4040").config("spark.ui.showConsoleProgress", "false")
         .config("spark.sql.ansi.enabled", "false")
         .getOrCreate())
sc = spark.sparkContext; sc.setLogLevel("ERROR")

env = [("Java", sh("java -version 2>&1 | grep -v Picked | head -1")), ("Spark", spark.version),
       ("PySpark", pyspark.__version__), ("Python", platform.python_version()), ("pandas", pd.__version__),
       ("numpy", np.__version__), ("playwright", sh("pip show playwright | awk '/^Version/{print $2}'")),
       ("OS", platform.platform()),
       ("CPU", sh("lscpu | grep 'Model name' | sed 's/.*: *//'")), ("CPU_logical_cores", str(os.cpu_count())),
       ("RAM_GB", sh("free -g | awk '/Mem:/{print $2}'")), ("spark.master", sc.master),
       ("spark.driver.memory", "8g"), ("spark.sql.shuffle.partitions", "16"), ("seed", str(SEED)),
       ("spark.ui.url", sc.uiWebUrl), ("run_started", time.strftime("%Y-%m-%d %H:%M:%S")),
       ("loadavg_at_start", open("/proc/loadavg").read().strip()), ("input_parquet", SRC)]
pd.DataFrame(env, columns=["parameter", "value"]).to_csv(f"{RES}/env_versions.csv", index=False)
with cap("01_env", "версії ПЗ, ресурси машини та конфігурація SparkSession"):
    for k, v in env: print(f"{k:<20}{v}")

# ---------- Крок 1. Завантаження результатів попередньої обробки (ПЗ2) ----------
t = time.perf_counter()
raw = spark.read.parquet(SRC).orderBy("datetime_utc")
n_raw = raw.count()
rec("load_parquet_pz2", time.perf_counter() - t, f"{n_raw} годинних рядків")
last_raw = raw.orderBy(F.col("datetime_utc").desc()).select("datetime_utc", "n_minutes").first()
with cap("02_schema", "схема та перші рядки вхідного parquet (результат ПЗ2)"):
    raw.printSchema(); print("rows:", n_raw)
    raw.select("datetime_utc", "close", "volume", "n_minutes", "rv", "ret_1h").show(3, False)

# ---------- Крок 2. Обґрунтування вибору періоду та задачі (з даних) ----------
flat_year = (raw.groupBy("year").agg(F.count("*").alias("hours"), F.round(100 * F.sum("n_flat") / F.sum("n_minutes"), 2).alias("flat_share_pct"),
                                     F.round(F.avg("rv"), 5).alias("mean_rv"), F.round(F.expr("percentile_approx(rv,0.5)"), 5).alias("median_rv"),
                                     F.sum((F.col("volume") == 0).cast("int")).alias("zero_vol_hours")).orderBy("year").toPandas())
save(flat_year, "pz5_flat_share_by_year")
START_YEAR = 2018   # з 2018 р. частка «пласких» хвилин < 4 % щороку (див. pz5_flat_share_by_year.csv)
with cap("03_period", "частка пласких хвилин і rv за роками (вибір періоду моделювання)"):
    print(flat_year.to_string(index=False)); print(f"\nПеріод моделювання: з {START_YEAR} р.")

# ---------- Крок 3. Формування ознак і цільової змінної (лише з минулого) ----------
HI_WIN, PCT = 720, 0.75     # поріг «висока волатильність» = 75-й перцентиль rv за останні 720 год (30 діб), включно з поточною годиною
EMB = 24                    # ембарго (год) між навчальною та тестовою частинами
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
       .withColumn("next_nmin", F.lead("n_minutes", 1).over(w_all))
       # мітка визначена лише якщо НАСТУПНА година повна (60 хв): неповна остання година мітку не формує
       .withColumn("label", F.when(F.col("next_nmin") == 60, (F.col("next_rv") > F.col("thr_rv")).cast("int")))
       .withColumn("label_dir", F.when(F.col("next_nmin") == 60, F.when(F.col("next_ret") > 0, 1).when(F.col("next_ret") < 0, 0)))
       .withColumn("persist", (F.col("rv") > F.col("thr_rv")).cast("double")))
NUM = ["log_rv_l0", "log_rv_l1", "log_rv_l2", "log_rv_l5", "log_rv_l23", "log_rv_m3", "log_rv_m6", "log_rv_m24", "log_rv_m168",
       "log_rv_over_thr", "ret_1h", "abs_ret_1h", "ret_6h", "log_rel_range", "log_volume", "vol_z168", "flat_share"]
base_cols = ["datetime_utc", "year", "hour", "dow", "n_minutes", "label", "label_dir", "persist", "rv", "next_rv", "thr_rv", "ret_1h", "next_ret"] + NUM
base_cols = list(dict.fromkeys(base_cols))
t = time.perf_counter()
m = d.select(*base_cols)
# кількість пропусків ДО фільтрації (розігрів вікон на початку історії)
nulls_before = m.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in NUM + ["label"]]).toPandas().T.reset_index()
nulls_before.columns = ["column", "nulls_full_history"]
m = (m.filter((F.col("year") >= START_YEAR) & (F.col("n_minutes") == 60) & F.col("label").isNotNull())   # неповні бари (поточний і наступний) виключені
       .withColumn("rn", F.row_number().over(Window.orderBy("datetime_utc"))).cache())
N = m.count()
rec("feature_engineering", time.perf_counter() - t, f"{N} рядків моделювання")
nulls_after = m.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in NUM + ["label"]]).toPandas().T.reset_index()
nulls_after.columns = ["column", "nulls_model_rows"]
save(nulls_before.merge(nulls_after, on="column"), "pz5_missing_values")

# ---------- Хронологічний поділ 80/20 ----------
n_train = int(N * 0.8)
bounds = m.filter(F.col("rn").isin(1, n_train - EMB, n_train, n_train + 1, N)).select("rn", "datetime_utc").orderBy("rn").toPandas()
train = m.filter(F.col("rn") <= n_train - EMB).cache()      # ембарго: останні 24 год навчальної частини вилучено
test = m.filter(F.col("rn") > n_train).cache()
n_tr, n_te = train.count(), test.count()
TEST_START = pd.Timestamp(bounds.datetime_utc[3])
sp = pd.DataFrame([("rows_model_total", N), ("train_rows", n_tr), ("embargo_rows", EMB), ("test_rows", n_te),
                   ("train_share_pct", round(100 * n_train / N, 2)), ("train_start", str(bounds.datetime_utc[0])),
                   ("train_end", str(bounds.datetime_utc[1])), ("embargo_end_nominal_train_boundary", str(bounds.datetime_utc[2])),
                   ("test_start", str(bounds.datetime_utc[3])), ("test_end", str(bounds.datetime_utc[4])),
                   ("raw_last_hour", str(last_raw["datetime_utc"])), ("raw_last_hour_n_minutes", int(last_raw["n_minutes"])),
                   ("raw_hourly_rows", n_raw)], columns=["parameter", "value"])
save(sp, "pz5_split")
lab = {}
for nm, df in (("train", train), ("test", test)):
    r = df.agg(F.avg("label").alias("pos"), F.avg("label_dir").alias("up")).first(); lab[nm] = (r["pos"], r["up"])
# Дрейф: частка позитивних за роками при ковзному порозі (основний) і при глобальному порозі з навчальної вибірки
glob_thr = train.approxQuantile("next_rv", [PCT], 1e-4)[0]
yr = (m.withColumn("label_global", (F.col("next_rv") > glob_thr).cast("int")).groupBy("year")
        .agg(F.count("*").alias("hours"), F.round(F.avg("label"), 4).alias("pos_share_rolling_thr"),
             F.round(F.avg("label_global"), 4).alias("pos_share_global_thr"), F.round(F.avg("label_dir"), 4).alias("up_share_next_hour")).orderBy("year").toPandas())
save(yr, "pz5_target_share_by_year")
ts = m.select("datetime_utc", "year", "label", "rv", "next_rv", "thr_rv", "ret_1h", "next_ret", "label_dir", "rn").toPandas().sort_values("datetime_utc")
ts["split"] = np.where(ts.rn <= n_train - EMB, "train", np.where(ts.rn > n_train, "test", "embargo"))
ts.drop(columns=["rn"]).to_csv(f"{RES}/pz5_target_series.csv", index=False)
with cap("04_split", "хронологічний поділ 80/20 з ембарго, розміри вибірок і частка класу 1"):
    print(sp.to_string(index=False, header=False))
    print(f"\nЧастка класу 1: train={lab['train'][0]:.4f}  test={lab['test'][0]:.4f}")
    print(f"Глобальний поріг rv (75-й перцентиль train) = {glob_thr:.5f}")
    print(yr.to_string(index=False))

# ---------- Чому не напрям ціни: ACF та базові лінії ----------
pdf = ts[["datetime_utc", "ret_1h", "label_dir", "next_ret"]]
r = pdf.ret_1h.values; ar = np.abs(r)
def acf(x, k):
    x = x - x.mean(); return float((x[:-k] * x[k:]).sum() / (x * x).sum())
nn = len(r); conf = 1.96 / np.sqrt(nn)
acf_df = pd.DataFrame({"lag_h": range(1, 73), "acf_ret": [acf(r, k) for k in range(1, 73)], "acf_abs_ret": [acf(ar, k) for k in range(1, 73)]})
acf_df["conf95"] = conf
save(acf_df, "pz5_acf")
dirv = pdf.dropna(subset=["label_dir"])
up_share = dirv.label_dir.mean()
same_sign_acc = float(((np.sign(dirv.ret_1h) > 0).astype(int) == dirv.label_dir)[dirv.ret_1h != 0].mean())   # правило «знак як у поточній годині»
dir_base = pd.DataFrame([("n_hours", nn), ("conf95_bound", conf), ("acf_ret_lag1", acf_df.acf_ret[0]), ("acf_abs_ret_lag1", acf_df.acf_abs_ret[0]),
                         ("acf_abs_ret_lag24", acf_df.acf_abs_ret[23]), ("acf_abs_ret_lag168_not_shown", acf(ar, 168)),
                         ("up_share_next_hour", up_share), ("majority_accuracy_direction", max(up_share, 1 - up_share)),
                         ("same_sign_rule_accuracy_all_hours", same_sign_acc), ("opposite_sign_rule_accuracy_all_hours", 1 - same_sign_acc)],
                        columns=["metric", "value"])
with cap("05_acf", "ACF дохідностей і модуля дохідностей; базові лінії задачі «напрям»"):
    print(dir_base.to_string(index=False))
    print("\nlag  ACF(ret)  ACF(|ret|)"); print(acf_df.head(6).round(4).to_string(index=False))

# ---------- Пайплайни ----------
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
import textwrap
with cap("06_pipeline", "склад ML Pipeline для LR, RF і GBT; вектор ознак"):
    for nm in ("LR", "RF", "GBT"):
        pl = mk(nm, combos(GRIDS[nm])[0])
        print(f"{nm}: " + " -> ".join(type(s).__name__ for s in pl.getStages()))
    print(f"\nЧислові ознаки ({len(FEATS)}):"); print("\n".join(textwrap.wrap(", ".join(FEATS), 86)))
    print(f"Циклічні/бінарні: {', '.join(CYC)}; категоріальна: dow -> OneHot(7)")

# ---------- Метрики ----------
ev_roc = BinaryClassificationEvaluator(rawPredictionCol="p1", labelCol="label", metricName="areaUnderROC")
ev_pr = BinaryClassificationEvaluator(rawPredictionCol="p1", labelCol="label", metricName="areaUnderPR")
def with_p1(pred):
    return pred.withColumn("p1", vector_to_array("probability")[1])
def metrics(df, thr=0.5, auc=True):
    """df має p1 (оцінка класу 1) та label. precision/recall/F1 – для класу «висока волатильність» (1)."""
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
                mcc=((tp * tn - fp * fn) / mcc_d) if mcc_d else 0.0,
                roc_auc=ev_roc.evaluate(df) if auc else np.nan, pr_auc=ev_pr.evaluate(df) if auc else np.nan,
                tn=tn, fp=fp, fn=fn, tp=tp, n=n)

# ---------- Часова валідація: розширюване вікно (3 фолди) з ембарго ----------
FOLDS = [(0.50, 0.667), (0.667, 0.833), (0.833, 1.0)]
cuts = [(int(n_tr * a), int(n_tr * b)) for a, b in FOLDS]
def fold_data(i):
    a, b = cuts[i]
    return train.filter(F.col("rn") <= a - EMB), train.filter((F.col("rn") > a) & (F.col("rn") <= b))
fold_rows = []
for i in range(3):
    a, b = cuts[i]; tri, vai = fold_data(i)
    fold_rows.append(dict(fold=i + 1, train_rows=tri.count(), embargo=EMB, val_rows=vai.count(), val_rn_from=a + 1, val_rn_to=b,
                          val_pos_share=vai.agg(F.avg("label")).first()[0]))
save(pd.DataFrame(fold_rows), "pz5_cv_folds")
tune_rows, best = [], {}
loadavg("before_tuning")
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
                              cv_roc_auc=np.mean(sc_roc), cv_pr_auc=np.mean(sc_pr), cv_pr_auc_std=np.std(sc_pr),
                              cv_pr_f1=sc_pr[0], cv_pr_f2=sc_pr[1], cv_pr_f3=sc_pr[2],
                              cv_roc_f1=sc_roc[0], cv_roc_f2=sc_roc[1], cv_roc_f3=sc_roc[2], seconds=time.perf_counter() - t))
        print(f"[tune] {nm} {p} PR-AUC={np.mean(sc_pr):.4f} ROC-AUC={np.mean(sc_roc):.4f}")
rec("hyperparameter_tuning_all", time.perf_counter() - t_tune, "3 моделі, 3 фолди, ембарго 24 год")
loadavg("after_tuning")
tune = pd.DataFrame(tune_rows); save(tune, "pz5_tuning")
tb = {}
for nm in ("LR", "RF", "GBT"):
    sub = tune[tune.model == nm].sort_values("cv_pr_auc", ascending=False).iloc[0]
    tb[nm] = sub
    best[nm] = {k: (int(sub[k]) if k in ("numTrees", "maxDepth", "minInstancesPerNode", "maxIter") else float(sub[k]))
                for k in GRIDS[nm]}
json.dump(best, open(f"{RES}/pz5_best_params.json", "w"), indent=1)
# ВИБІР МОДЕЛІ – лише за середнім PR-AUC часової крос-валідації (test у виборі не бере участі)
sel = max(("LR", "RF", "GBT"), key=lambda k: tb[k].cv_pr_auc)
sel_df = pd.DataFrame([dict(model=k, cv_pr_auc=tb[k].cv_pr_auc, cv_pr_auc_std_between_folds=tb[k].cv_pr_auc_std, cv_roc_auc=tb[k].cv_roc_auc,
                            selected_by_cv=(k == sel)) for k in ("LR", "RF", "GBT")])
save(sel_df, "pz5_model_selection_cv")
with cap("07_tuning", "підбір гіперпараметрів: часова CV (3 фолди), найкращі конфігурації та вибір моделі"):
    for nm in ("LR", "RF", "GBT"):
        s = tune[tune.model == nm].sort_values("cv_pr_auc", ascending=False).head(3)
        s = s.dropna(axis=1, how="all").rename(columns={"cv_roc_auc": "cv_roc", "cv_pr_auc": "cv_pr", "cv_pr_auc_std": "std"})
        s = s[[c for c in s.columns if not c.startswith(("cv_pr_f", "cv_roc_f")) and c not in ("seconds", "model")]]
        print(f"\n{nm}: найкраще {best[nm]}"); print(s.round(4).to_string(index=False))
    print("\nВибір моделі за CV PR-AUC (test не використано):"); print(sel_df.round(4).to_string(index=False))

# ---------- Поріг рішення: середній F1 за 3 валідаційними фолдами (без доступу до test) ----------
thr_grid = np.round(np.arange(0.05, 0.96, 0.01), 2)
chosen_thr, thr_rows, thr_detail = {}, [], []
for nm in ("LR", "RF", "GBT"):
    F1 = np.zeros((3, len(thr_grid))); PR = np.zeros_like(F1); RC = np.zeros_like(F1)
    for i in range(3):
        tr_v, va_v = fold_data(i)
        pv = with_p1(mk(nm, best[nm]).fit(tr_v).transform(va_v)).select("label", "p1").toPandas()
        for j, th in enumerate(thr_grid):
            yh = (pv.p1 >= th).astype(int); tp = ((yh == 1) & (pv.label == 1)).sum(); fp = ((yh == 1) & (pv.label == 0)).sum(); fn = ((yh == 0) & (pv.label == 1)).sum()
            PR[i, j] = tp / (tp + fp) if tp + fp else 0; RC[i, j] = tp / (tp + fn) if tp + fn else 0
            F1[i, j] = 2 * PR[i, j] * RC[i, j] / (PR[i, j] + RC[i, j]) if PR[i, j] + RC[i, j] else 0
        thr_detail.append(dict(model=nm, fold=i + 1, best_threshold=float(thr_grid[F1[i].argmax()]), best_f1=float(F1[i].max())))
    chosen_thr[nm] = float(thr_grid[int(np.argmax(F1.mean(axis=0)))])
    for j, th in enumerate(thr_grid):
        thr_rows.append(dict(model=nm, threshold=th, f1_fold1=F1[0, j], f1_fold2=F1[1, j], f1_fold3=F1[2, j],
                             val_f1=F1[:, j].mean(), val_precision=PR[:, j].mean(), val_recall=RC[:, j].mean()))
    thr_detail.append(dict(model=nm, fold="mean", best_threshold=chosen_thr[nm], best_f1=float(F1.mean(axis=0).max())))
save(pd.DataFrame(thr_rows), "pz5_threshold_validation")
save(pd.DataFrame(thr_detail), "pz5_threshold_per_fold")
json.dump(chosen_thr, open(f"{RES}/pz5_chosen_thresholds.json", "w"))

# ---------- Остаточне навчання на train і оцінка на test ----------
models, preds, timing_models = {}, {}, []
for nm in ("LR", "RF", "GBT"):
    t = time.perf_counter(); pm = mk(nm, best[nm]).fit(train); t_fit = time.perf_counter() - t
    t = time.perf_counter(); pr_ = with_p1(pm.transform(test)).cache(); n_ = pr_.count(); t_inf = time.perf_counter() - t
    models[nm] = pm; preds[nm] = pr_
    timing_models.append(dict(model=nm, fit_seconds=t_fit, inference_seconds_test=t_inf, test_rows=n_, inference_rows_per_sec=n_ / t_inf))
    rec(f"fit_{nm}", t_fit, f"train {n_tr} рядків"); rec(f"inference_{nm}", t_inf, f"test {n_te} рядків")
save(pd.DataFrame(timing_models), "pz5_model_runtimes")
base_test = test.withColumn("p1", F.lit(0.0))
persist_test = test.withColumn("p1", F.col("persist"))
ratio_test = test.withColumn("p1", F.col("log_rv_over_thr"))
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
def short(n): return (n.replace("Мажоритарний клас (завжди 0)", "Мажоритарний").replace("Персистентність (rv_t > порогу)", "Персистентність")
                       .replace("Ранжування за log(rv_t/поріг), поріг 0", "log(rv/поріг)").replace(", з валідації", "").replace("(поріг ", "@").replace(")", "").replace(",", "."))
with cap("08_metrics", "метрики на тесті: базові лінії та моделі (поріг 0,5 і поріг з валідації)"):
    print(f"Частка класу 1 у test: {lab['test'][0]:.4f}; n={n_te}; пороги з валідації: {chosen_thr}")
    q = res.drop(columns=["kind", "n", "balanced_accuracy", "tn", "fp", "fn", "tp"]).copy(); q["model"] = q.model.map(short)
    q = q.rename(columns={"model": "модель", "threshold": "thr", "accuracy": "acc", "precision": "prec", "recall": "rec", "roc_auc": "ROC", "pr_auc": "PR"})
    print(q.round(3).to_string(index=False))
    print("\nЧас, с:"); print(pd.DataFrame(timing_models).drop(columns=["test_rows"]).round(2).to_string(index=False))

# ---------- Збереження прогнозів ----------
pcols = test.select("datetime_utc", "year", "hour", "dow", "label", "persist", "log_rv_over_thr", "log_rv_l0", "rv", "next_rv", "thr_rv").toPandas()
for nm in ("LR", "RF", "GBT"):
    pcols[f"p_{nm}"] = preds[nm].select("datetime_utc", "p1").toPandas().set_index("datetime_utc").loc[pcols.datetime_utc, "p1"].values
pcols = pcols.sort_values("datetime_utc")
pcols.to_csv(f"{RES}/pz5_predictions_test_full.csv", index=False)
pcols.head(200).to_csv(f"{RES}/pz5_predictions_sample.csv", index=False)

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
imp = imp.sort_values("RF_importance", ascending=False)
save(imp, "pz5_feature_importance")
with cap("09_importance", "model.stages[-1].featureImportances / coefficients (перші 14 з 29 за RF)"):
    print(imp.head(14).drop(columns=["LR_abs_coef"]).rename(columns={"LR_coef_standardized": "LR_coef"}).round(4).to_string(index=False))
    print(f"... ще {len(imp) - 14} рядків у results/pz5_feature_importance.csv; LR intercept:", round(models['LR'].stages[-1].intercept, 4))

# ---------- Випадковий поділ проти хронологічного: порівняння на ОДНАКОВИХ годинах (MAJOR-1) ----------
#  A  – хронологічна модель (train до ембарго) -> хронологічний test (n_te)
#  B  – випадкова вибірка 80 % передтестового періоду (train) -> ТОЙ САМИЙ хронологічний test (ті самі рядки, що й A)
#  C  – випадковий поділ 80/20 усього періоду -> його випадковий test (вся історія 2018–2026)
#  C* – C, обмежений годинами з періоду хронологічного test (>= test_start): модель випадкового поділу, тест лише на цих годинах
#  A* – хронологічна модель на тих самих годинах, що й C*
loadavg("before_random_vs_chrono")
rnd_pre = train.sample(False, 0.8, seed=SEED)
n_B = rnd_pre.count()
rnd_tr, rnd_te = m.randomSplit([0.8, 0.2], seed=SEED)
n_Ctr, n_Cte = rnd_tr.count(), rnd_te.count()
rnd_te = rnd_te.cache(); rnd_te_star = rnd_te.filter(F.col("datetime_utc") >= F.lit(str(TEST_START))).cache()
n_Cs = rnd_te_star.count()
star_keys = rnd_te_star.select("datetime_utc")
cmp_rows, pred_B, pred_C = [], {}, {}
def row(nm, split, df, thr_tuned):
    r_ = metrics(df, 0.5); r2 = metrics(df, thr_tuned, auc=False)
    r_.update(model=nm, split=split, n_test=r_["n"], f1_at_cv_threshold=r2["f1"], cv_threshold=thr_tuned); cmp_rows.append(r_)
for nm in ("LR", "RF", "GBT"):
    th = chosen_thr[nm]
    row(nm, "A chrono -> chrono test", preds[nm], th)
    row(nm, "A* chrono -> test hours of C*", preds[nm].join(star_keys, "datetime_utc"), th)
    pmB = mk(nm, best[nm]).fit(rnd_pre); pB = with_p1(pmB.transform(test)).cache(); pred_B[nm] = pB
    row(nm, "B random 80% of pre-test -> chrono test", pB, th)
    pmC = mk(nm, best[nm]).fit(rnd_tr); pC = with_p1(pmC.transform(rnd_te)).cache(); pred_C[nm] = pC
    row(nm, "C random 80/20 whole period -> random test", pC, th)
    row(nm, "C* random 80/20 -> random test, hours >= test_start", pC.join(star_keys, "datetime_utc"), th)
cmp = pd.DataFrame(cmp_rows)[["model", "split", "n_test", "accuracy", "precision", "recall", "f1", "f1_at_cv_threshold", "cv_threshold", "roc_auc", "pr_auc"]]
save(cmp, "pz5_random_vs_chrono")
def collect_preds(pd_, keycols):
    out = pd_["LR"].select("datetime_utc", "label").toPandas().set_index("datetime_utc")
    for nm in ("LR", "RF", "GBT"):
        out[f"p_{nm}"] = pd_[nm].select("datetime_utc", "p1").toPandas().set_index("datetime_utc").p1.reindex(out.index)
    return out.sort_index().reset_index()
collect_preds(pred_B, None).to_csv(f"{RES}/pz5_pred_random_pretest_on_chrono_test.csv", index=False)
collect_preds(pred_C, None).to_csv(f"{RES}/pz5_pred_random_whole_test.csv", index=False)
rs = pd.DataFrame([("A_chrono_train_rows", n_tr), ("A_chrono_test_rows", n_te), ("B_random_pretest_train_rows", n_B), ("B_test_rows", n_te),
                   ("C_random_train_rows", n_Ctr), ("C_random_test_rows", n_Cte), ("C_star_test_rows(>=test_start)", n_Cs), ("test_start", str(TEST_START))],
                  columns=["parameter", "value"])
save(rs, "pz5_random_split_sizes")
with cap("10_random_vs_chrono", "випадковий і хронологічний поділ: ті самі моделі, ті самі години тесту (поріг 0,5)"):
    print(rs.to_string(index=False, header=False))
    q = cmp.drop(columns=["accuracy", "precision", "recall", "f1_at_cv_threshold", "cv_threshold"]).copy()
    q["split"] = q.split.str.split(" ").str[0]
    print(q.rename(columns={"n_test": "n", "roc_auc": "ROC", "pr_auc": "PR"}).round(3).to_string(index=False))

# ---------- Перевірка «напрямок ціни» (лише LR, для обґрунтування вибору задачі) ----------
dir_tr, dir_te = train.filter(F.col("label_dir").isNotNull()), test.filter(F.col("label_dir").isNotNull())
ev_d = BinaryClassificationEvaluator(rawPredictionCol="p1", labelCol="label_dir", metricName="areaUnderROC")
st_ = stages(LogisticRegression(featuresCol="features", labelCol="label_dir", maxIter=200, regParam=0.01, standardization=False), True)
pd_ = with_p1(Pipeline(stages=st_).fit(dir_tr).transform(dir_te)).cache()
acc_d = pd_.select(F.avg(((F.col("p1") >= 0.5).cast("int") == F.col("label_dir")).cast("double"))).first()[0]
n_dir = pd_.count()
maj_d = dir_te.agg(F.avg("label_dir")).first()[0]
dir_res = pd.DataFrame([("LR direction: ROC-AUC", ev_d.evaluate(pd_)), ("LR direction: accuracy", acc_d), ("LR direction: n_test", n_dir),
                        ("LR direction: n_correct", round(acc_d * n_dir)),
                        ("majority accuracy (test)", max(maj_d, 1 - maj_d)), ("up share test", maj_d)], columns=["metric", "value"])
save(pd.concat([dir_base, dir_res]), "pz5_direction_check")
pd_.select("datetime_utc", "label_dir", "p1", "ret_1h", "next_ret").toPandas().sort_values("datetime_utc").to_csv(f"{RES}/pz5_direction_pred_test.csv", index=False)
with cap("11_direction", "LogisticRegression на label = (дохідність наступної години > 0): контрольний експеримент"):
    print(dir_res.round(4).to_string(index=False))

# ---------- Вибір та збереження моделі: за CV (не за test) ----------
best_name = sel
for stale in os.listdir(f"{RES}/model"):
    if stale != f"best_pipeline_{best_name}": shutil.rmtree(f"{RES}/model/{stale}", ignore_errors=True)
t = time.perf_counter(); models[best_name].write().overwrite().save(f"{RES}/model/best_pipeline_{best_name}")
rec("save_best_pipeline", time.perf_counter() - t, best_name)
t = time.perf_counter(); reloaded = PipelineModel.load(f"{RES}/model/best_pipeline_{best_name}")
chk = with_p1(reloaded.transform(test)).agg(F.max("p1")).first()[0]; rec("load_best_pipeline", time.perf_counter() - t, f"max p1={chk:.4f}")
pd.DataFrame([("best_model_selected_by", "mean CV PR-AUC (3 expanding folds), test not used"), ("best_model", best_name),
              ("cv_pr_auc_LR", tb["LR"].cv_pr_auc), ("cv_pr_auc_RF", tb["RF"].cv_pr_auc), ("cv_pr_auc_GBT", tb["GBT"].cv_pr_auc)],
             columns=["parameter", "value"]).to_csv(f"{RES}/pz5_best_model.csv", index=False)
with cap("12_model_saved", f"model.write().save('results/model/best_pipeline_{best_name}'); PipelineModel.load(...)"):
    print("модель обрано за CV PR-AUC:", best_name); print("збережено:", f"results/model/best_pipeline_{best_name}")
    print(sh(f"ls {RES}/model/best_pipeline_{best_name}")); print("етапи:", [type(s).__name__ for s in reloaded.stages])

# ---------- Таймінги та знімки Spark UI (поки сесія Spark працює) ----------
rec("total_script_before_ui", time.perf_counter() - T_ALL)
loadavg("end")
try:
    from playwright.sync_api import sync_playwright
    def hide_fixed(pg):
        pg.evaluate("document.querySelectorAll('*').forEach(e=>{if(getComputedStyle(e).position=='fixed')e.style.display='none'})")
    def snap(pg, sel, path, maxh=520):
        el = pg.locator(sel).first; el.wait_for(timeout=30000)
        bb = el.bounding_box()
        pg.screenshot(path=path, full_page=True, clip={"x": max(bb["x"] - 4, 0), "y": max(bb["y"] - 4, 0),
                      "width": min(bb["width"] + 8, 1280), "height": min(bb["height"] + 8, maxh)})
    with sync_playwright() as p:
        b = p.chromium.launch(executable_path="/opt/pw-browsers/chromium-1243/chrome-linux64/chrome", args=["--no-sandbox"])
        pg = b.new_page(viewport={"width": 1280, "height": 800}, device_scale_factor=2)
        pages = {"jobs": ("jobs/", "#completedJob-table"), "stages": ("stages/", "#completedStage-table"),
                 "storage": ("storage/", "#storage-by-rdd-table"), "sql": ("SQL/", "table:has(th:has-text('Description'))"),
                 "executors": ("executors/", "#summary-execs-table"), "environment": ("environment/", "#runtime-table")}
        for k, (u, sel_) in pages.items():
            try:
                pg.goto(f"{sc.uiWebUrl}/{u}", wait_until="load"); pg.wait_for_timeout(2500)
                hide_fixed(pg); pg.evaluate("document.body.style.zoom = 1.5"); pg.wait_for_timeout(500)
                snap(pg, sel_, f"{SHOTS}/spark_ui_{k}.png")
            except Exception as e:
                print(f"Spark UI {k} failed:", e)
        b.close()
except Exception as e:
    print("Spark UI screenshots failed:", e)
try:
    ui_jobs = len(json.loads(sh(f"curl -s {sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/jobs?length=100000")) or [])
    ui_stages = len(json.loads(sh(f"curl -s {sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/stages?length=100000")) or [])
    rec("spark_ui_totals", 0.0, f"jobs={ui_jobs} stages={ui_stages}")
except Exception as e:
    print("Spark UI REST failed:", e)
rec("total_script", time.perf_counter() - T_ALL)
pd.DataFrame(TIMINGS, columns=["step", "seconds", "note"]).to_csv(f"{RES}/timings.csv", index=False)
pd.DataFrame(LOAD, columns=["moment", "time", "load1", "load5", "load15", "cpu_cores", "top_processes_pcpu"]).to_csv(f"{RES}/loadavg.csv", index=False)
spark.stop()
