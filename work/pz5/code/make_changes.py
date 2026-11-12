# -*- coding: utf-8 -*-
"""Формує results/CHANGES_vs_old_run.md: «було (_old_run) -> стало (поточний запуск)» для кожного числа поточного report.md.
Старі значення читаються з _old_run/results, нові – з results/. Запуск: python code/make_changes.py"""
import os, sys, json
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pz5_common import BASE, RES
OLD = f"{BASE}/_old_run/results"
def rd(d, f): return pd.read_csv(f"{d}/{f}.csv")
def kv(d, f): x = rd(d, f); return dict(zip(x.iloc[:, 0], x.iloc[:, 1]))
def fm(v, d=4):
    if v is None or (isinstance(v, float) and np.isnan(v)): return "–"
    if isinstance(v, str): return v
    if isinstance(v, (int, np.integer)) or float(v) == int(v) and d == 0: return f"{int(v):,}".replace(",", " ")
    return f"{v:.{d}f}".replace(".", ",")
rows = []
def add(sec, what, o, n, d=4, note=""):
    ov, nv = fm(o, d), fm(n, d)
    rows.append((sec, what, ov, nv, "=" if ov == nv else "змінилося", note))

# ---- джерела ----
ospl, nspl = kv(OLD, "pz5_split"), kv(RES, "pz5_split")
odc, ndc = kv(OLD, "pz5_direction_check"), kv(RES, "pz5_direction_check")
oacf, nacf = rd(OLD, "pz5_acf"), rd(RES, "pz5_acf")
oflat, nflat = rd(OLD, "pz5_flat_share_by_year").set_index("year"), rd(RES, "pz5_flat_share_by_year").set_index("year")
otgt, ntgt = rd(OLD, "pz5_target_share_by_year").set_index("year"), rd(RES, "pz5_target_share_by_year").set_index("year")
otun, ntun = rd(OLD, "pz5_tuning"), rd(RES, "pz5_tuning")
om, nm_ = rd(OLD, "pz5_metrics_test"), rd(RES, "pz5_metrics_test")
otm, ntm = rd(OLD, "timings").set_index("step").seconds, rd(RES, "timings").set_index("step").seconds
ort, nrt = rd(OLD, "pz5_model_runtimes").set_index("model"), rd(RES, "pz5_model_runtimes").set_index("model")
oimp, nimp = rd(OLD, "pz5_feature_importance").set_index("feature"), rd(RES, "pz5_feature_importance").set_index("feature")
othr, nthr = json.load(open(f"{OLD}/pz5_chosen_thresholds.json")), json.load(open(f"{RES}/pz5_chosen_thresholds.json"))
omiss, nmiss = rd(OLD, "pz5_missing_values").set_index("column"), rd(RES, "pz5_missing_values").set_index("column")

# ---- Розділ 1, таблиця 1 ----
S = "1 / табл. 1"
add(S, "годин моделювання (підпис табл. 1, розд. 2.1, висн. 2)", ospl["rows_model_total"], nspl["rows_model_total"], 0, "остання година 02:00 більше не має мітки: наступний бар 03:00 неповний (38 хв)")
for lag, c in ((1, 0), (2, 1), (24, 23)):
    add(S, f"ACF дохідності, лаг {lag}", oacf.acf_ret[c], nacf.acf_ret[c])
for lag, c in ((1, 0), (24, 23)):
    add(S, f"ACF |дохідності|, лаг {lag}", oacf.acf_abs_ret[c], nacf.acf_abs_ret[c])
add(S, "ACF |дохідності|, лаг 168", odc["acf_abs_ret_lag168_not_shown"], ndc["acf_abs_ret_lag168_not_shown"])
add(S, "95 % межа", odc["conf95_bound"], ndc["conf95_bound"])
add(S, "частка годин зі зростанням", odc["up_share_next_hour"], ndc["up_share_next_hour"])
add(S, "правило «знак як у поточній годині» (було sign_persistence…, стало same_sign_rule…, усі години)", odc["sign_persistence_accuracy_direction"], ndc["same_sign_rule_accuracy_all_hours"], 4, "перейменовано; те саме правило «той самий знак»")
add(S, "правило «протилежний знак», усі години (нове)", None, ndc["opposite_sign_rule_accuracy_all_hours"], 4, "нове: 1 − той самий знак")
add(S, "LR напрям: ROC-AUC", odc["LR direction: ROC-AUC"], ndc["LR direction: ROC-AUC"])
add(S, "LR напрям: accuracy", odc["LR direction: accuracy"], ndc["LR direction: accuracy"])
add(S, "мажоритарний клас для напряму (test)", odc["majority accuracy (test)"], ndc["majority accuracy (test)"])
dt = rd(RES, "pz5_direction_test").iloc[0]
add(S, "LR напрям: z проти 0,5 (нове)", None, dt.z, 2, f"n={int(dt.n)}, k={int(dt.k_correct)}, p(біном.)={dt.p_binom_two_sided:.2e}")
sr = rd(RES, "pz5_direction_sign_rules").set_index("subset")
for ss in ("train", "test"):
    add(S, f"правило «той самий знак», {ss} (нове)", None, sr.loc[ss, "same_sign_accuracy"], 4, f"n={int(sr.loc[ss, 'n_used'])}")
    add(S, f"правило «протилежний знак», {ss} (нове)", None, sr.loc[ss, "opposite_sign_accuracy"], 4, "")

# ---- Розділ 2.1, табл. 2 ----
for y in range(2017, 2027):
    S = "2.1 / табл. 2"
    add(S, f"{y}: flat, %", oflat.flat_share_pct[y], nflat.flat_share_pct[y], 2)
    add(S, f"{y}: медіана rv", oflat.median_rv[y], nflat.median_rv[y], 5)
    if y >= 2018:
        add(S, f"{y}: клас 1, ковзний поріг", otgt.pos_share_rolling_thr[y], ntgt.pos_share_rolling_thr[y])
        add(S, f"{y}: клас 1, глобальний поріг", otgt.pos_share_global_thr[y], ntgt.pos_share_global_thr[y])
S = "2.1–2.2"
add(S, "7 816 537 хвилинних записів / 130 276 годинних рядків (вхід)", 130276, nspl["raw_hourly_rows"], 0, "вхідний parquet ПЗ2 перезапущено; кількість рядків та n_minutes однакові")
add(S, "початок / кінець періоду моделювання (останній рядок)", ospl["test_end"], nspl["test_end"], 0, "текст «11.11.2026 02:00»")
# глобальний поріг з навчальної вибірки – з run.log (txt)
def glob_thr(path):
    for l in open(path, encoding="utf-8"):
        if "Глобальний поріг" in l or "Global" in l: return float(l.split("=")[-1])
add(S, "глобальний поріг rv (75-й перцентиль train)", 0.00794, glob_thr(f"{BASE}/screenshots/txt/04_split.txt"), 5, "старе значення з тексту звіту/скріншота")
add(S, "частка класу 1 на всіх годинах (текст «0,248»)", 0.2480, rd(RES, "pz5_target_series").label.mean(), 4)
# ---- Таблиця 3: пропуски ----
for c in ("log_rv_l1", "log_rv_l2", "log_rv_l5", "log_rv_l23", "ret_1h", "abs_ret_1h", "ret_6h", "vol_z168"):
    add("2.3 / табл. 3", f"пропуски в історії: {c}", int(omiss.nulls_full_history[c]), int(nmiss.nulls_full_history[c]), 0)
add("2.3 / табл. 3", "пропуски у рядках моделі (усі ознаки, сума)", int(omiss.nulls_model_rows.sum()), int(nmiss.nulls_model_rows.sum()), 0)
add("2.3", "довжина вектора ознак (позицій)", 29, len(nimp), 0)
# ---- Розділ 3, табл. 4 ----
S = "3 / табл. 4"
add(S, "train_rows", ospl["train_rows"], nspl["train_rows"], 0, "N·0,8 − 24; N зменшилося на 1 рядок")
add(S, "test_rows", ospl["test_rows"], nspl["test_rows"], 0)
add(S, "ембарго, год", ospl["embargo_rows"], nspl["embargo_rows"], 0)
for k in ("train_start", "train_end", "embargo_end_nominal_train_boundary", "test_start", "test_end"):
    add(S, k, ospl[k], nspl[k], 0)
ob, nb = pd.Series(dtype=float), pd.Series(dtype=float)
# частка класу 1 train/test – зі скріншота 04_split
def share(path):
    for l in open(path, encoding="utf-8"):
        if l.startswith("Частка класу 1") or l.startswith("Доля"): return l
add(S, "частка класу 1 train / test", "0,2478 / 0,2489", share(f"{BASE}/screenshots/txt/04_split.txt").split(":")[1].strip().replace("train=", "").replace("test=", "").replace("  ", " / ").replace(".", ","), 0)
# ---- Розділ 4, табл. 5 ----
S = "4.1 / табл. 5"
def bestrow(t, m): return t[t.model == m].sort_values("cv_pr_auc", ascending=False).iloc[0]
for m in ("LR", "RF", "GBT"):
    o, n = bestrow(otun, m), bestrow(ntun, m)
    add(S, f"{m}: CV ROC-AUC", o.cv_roc_auc, n.cv_roc_auc); add(S, f"{m}: CV PR-AUC", o.cv_pr_auc, n.cv_pr_auc); add(S, f"{m}: σ між блоками", o.cv_pr_auc_std, n.cv_pr_auc_std)
    keys = [k for k in ("regParam", "elasticNetParam", "numTrees", "maxDepth", "minInstancesPerNode", "maxIter", "stepSize") if not pd.isna(o[k])]
    add(S, f"{m}: найкраща конфігурація", ", ".join(f"{k}={o[k]:g}" for k in keys), ", ".join(f"{k}={n[k]:g}" for k in keys), 0)
lr_o = otun[(otun.model == "LR") & (otun.regParam == 0.1)].cv_pr_auc.max(); lr_n = ntun[(ntun.model == "LR") & (ntun.regParam == 0.1)].cv_pr_auc.max()
add(S, "LR regParam 0,1: CV PR-AUC", lr_o, lr_n)
for nm, tb in (("o", otun), ("n", ntun)):
    pass
def rf(t, d): return t[(t.model == "RF") & (t.maxDepth == d) & (t.numTrees == 200)].cv_pr_auc.max()
add(S, "RF глибина 8 / 4 (200 дерев): CV PR-AUC", f"{fm(rf(otun, 8))} / {fm(rf(otun, 4))}", f"{fm(rf(ntun, 8))} / {fm(rf(ntun, 4))}", 0)
def gspread(t): s = t[t.model == "GBT"].sort_values("cv_pr_auc", ascending=False).head(3).cv_pr_auc; return s.max() - s.min()
add(S, "GBT: розкид трійки найкращих (макс − мін)", gspread(otun), gspread(ntun))
add(S, "тривалість повної сітки, с", otm["hyperparameter_tuning_all"], ntm["hyperparameter_tuning_all"], 3, "залежить від навантаження машини (див. loadavg.csv)")
ovf = rd(OLD, "pz5_threshold_validation"); nvf = rd(RES, "pz5_threshold_validation")
for m in ("LR", "RF", "GBT"):
    add("4.2", f"{m}: поріг рішення", othr[m], nthr[m], 2, "було: один фолд (3-й); стало: середній F1 за 3 фолдами")
    add("4.2", f"{m}: макс. F1 на валідації", ovf[ovf.model == m].val_f1.max(), nvf[nvf.model == m].val_f1.max(), 4)
# ---- Розділ 5 ----
S = "5"
add(S, "збереження моделі, с", otm["save_best_pipeline"], ntm["save_best_pipeline"], 3); add(S, "завантаження моделі, с", otm["load_best_pipeline"], ntm["load_best_pipeline"], 3)
bo = kv(OLD, "pz5_best_model"); bn = kv(RES, "pz5_best_model")
add(S, "збережена модель", bo["best_model_by_test_pr_auc"], bn["best_model"], 0, "було: за test PR-AUC; стало: за CV PR-AUC")
add(S, "весь сценарій pz5_spark.py, с", otm["total_script"], ntm["total_script"], 3, "залежить від навантаження машини")
add(S, "віконні ознаки (feature_engineering), с", otm["feature_engineering"], ntm["feature_engineering"], 3)
nt = rd(RES, "timings"); jn = nt[nt.step == "spark_ui_totals"].note.iloc[0] if (nt.step == "spark_ui_totals").any() else "–"
add(S, "джоби / етапи Spark UI", "11 364 / 20 036", jn.replace("jobs=", "").replace(" stages=", " / "), 0)
# ---- Розділ 6/7, табл. 6 ----
def mrow(t, name): return t[t.model == name].iloc[0]
NAMES = ["Мажоритарний клас (завжди 0)", "Персистентність (rv_t > порогу)", "Ранжування за log(rv_t/поріг), поріг 0"]
for m in ("LR", "RF", "GBT"): NAMES += [f"{m} (поріг 0,5)", None]
for nme in NAMES:
    on = nme
    if nme is None:
        m = NAMES[NAMES.index(None) - 1].split()[0] if False else None
tuned = {m: (f"{m} (поріг {othr[m]:.2f}, з валідації)", f"{m} (поріг {nthr[m]:.2f}, з валідації)") for m in ("LR", "RF", "GBT")}
pairs = [(n, n) for n in NAMES[:3]] + [item for m in ("LR", "RF", "GBT") for item in ((f"{m} (поріг 0,5)", f"{m} (поріг 0,5)"), tuned[m])]
for o_name, n_name in pairs:
    o, n = mrow(om, o_name), mrow(nm_, n_name)
    lab = short = o_name.replace(", з валідації", "") + ("" if o_name == n_name else f" -> {n_name.replace(', з валідації', '')}")
    for c, cn in (("accuracy", "Accuracy"), ("precision", "Precision"), ("recall", "Recall"), ("f1", "F1"), ("roc_auc", "ROC-AUC"), ("pr_auc", "PR-AUC"), ("mcc", "MCC")):
        add("7.1 / табл. 6", f"{lab}: {cn}", o[c], n[c])
for m in ("LR", "RF", "GBT"):
    add("7.1 / табл. 6", f"{m}: навчання, с", ort.fit_seconds[m], nrt.fit_seconds[m], 3, "залежить від навантаження")
    add("7.1", f"{m}: інференс 15 534 рядків, с", ort.inference_seconds_test[m], nrt.inference_seconds_test[m], 3, "залежить від навантаження")
P_o, P_n = mrow(om, "Персистентність (rv_t > порогу)"), mrow(nm_, "Персистентність (rv_t > порогу)")
add("6", "персистентність: FPR", P_o.fp / (P_o.fp + P_o.tn), P_n.fp / (P_n.fp + P_n.tn), 3); add("6", "персистентність: TPR", P_o.tp / (P_o.tp + P_o.fn), P_n.tp / (P_n.tp + P_n.fn), 3)
def ginfo(t, nm_t):
    g, p, r = mrow(t, nm_t[0]), mrow(t, "Персистентність (rv_t > порогу)"), mrow(t, "Ранжування за log(rv_t/поріг), поріг 0")
    return dict(dF1=g.f1 - p.f1, dMCC=g.mcc - p.mcc, dROC=g.roc_auc - r.roc_auc, dPR=g.pr_auc - r.pr_auc)
go, gn = ginfo(om, [tuned["GBT"][0]]), ginfo(nm_, [tuned["GBT"][1]])
add("7.1", "GBT (тюнінг) − персистентність: F1", go["dF1"], gn["dF1"], 3); add("7.1", "GBT − персистентність: MCC", go["dMCC"], gn["dMCC"], 3)
add("7.1", "GBT − log(rv/поріг): ROC-AUC", go["dROC"], gn["dROC"], 3); add("7.1", "GBT − log(rv/поріг): PR-AUC", go["dPR"], gn["dPR"], 3)
# ---- 7.2, табл. 7 ----
S = "7.2 / табл. 7"
orc, nrc = rd(OLD, "pz5_random_vs_chrono"), rd(RES, "pz5_random_vs_chrono")
nrs = kv(RES, "pz5_random_split_sizes")
for m in ("LR", "RF", "GBT"):
    for oname, nname, lab in (("random 80/20 (shuffle)", "C random 80/20 whole period -> random test", "випадковий (було) -> C: випадковий, весь період"),
                              ("chronological 80/20", "A chrono -> chrono test", "хронологічний (було) -> A")):
        o, n = orc[(orc.model == m) & (orc.split == oname)].iloc[0], nrc[(nrc.model == m) & (nrc.split == nname)].iloc[0]
        add(S, f"{m}, {lab}: n тестових рядків", o.n_test, n.n_test, 0)
        for c, cn in (("f1", "F1"), ("roc_auc", "ROC-AUC"), ("pr_auc", "PR-AUC")):
            add(S, f"{m}, {lab}: {cn}", o[c], n[c])
    for sp_, lab in (("B random 80% of pre-test -> chrono test", "B (нове): випадкові 80 % передтесту → той самий test"),
                     ("A* chrono -> test hours of C*", "A* (нове): хроно на годинах C*"), ("C* random 80/20 -> random test, hours >= test_start", "C* (нове): випадковий поділ, лише години >= test_start")):
        n = nrc[(nrc.model == m) & (nrc.split == sp_)].iloc[0]
        add(S, f"{m}, {lab}: n / PR-AUC / ROC-AUC / F1", None, f"{int(n.n_test)} / {fm(n.pr_auc)} / {fm(n.roc_auc)} / {fm(n.f1)}", 0, "нове порівняння на однакових годинах")
# ---- 8.1 важливості ----
S = "8.1"
for f, col, lab in (("log_rv_over_thr", "RF_importance", "log_rv_over_thr: RF"), ("log_rv_over_thr", "GBT_importance", "log_rv_over_thr: GBT"), ("log_rv_over_thr", "LR_coef_standardized", "log_rv_over_thr: LR коеф."),
                    ("log_rel_range", "RF_importance", "log_rel_range: RF"), ("vol_z168", "RF_importance", "vol_z168: RF"), ("log_rv_l0", "RF_importance", "log_rv_l0: RF"),
                    ("hour_cos", "GBT_importance", "hour_cos: GBT"), ("hour_sin", "GBT_importance", "hour_sin: GBT"), ("log_rv_l0", "LR_coef_standardized", "log_rv_l0: LR коеф."),
                    ("flat_share", "RF_importance", "flat_share: RF")):
    add(S, lab, oimp.loc[f, col], nimp.loc[f, col])
add(S, "hour_cos + hour_sin (GBT)", oimp.loc["hour_cos", "GBT_importance"] + oimp.loc["hour_sin", "GBT_importance"], nimp.loc["hour_cos", "GBT_importance"] + nimp.loc["hour_sin", "GBT_importance"])
# ---- 8.2 помилки ----
S = "8.2 / рис. 8"
for m in ("LR", "RF", "GBT"):
    o, n = mrow(om, tuned[m][0]), mrow(nm_, tuned[m][1])
    add(S, f"{m}: помилок (fp+fn)", o.fp + o.fn, n.fp + n.fn, 0); add(S, f"{m}: FN", o.fn, n.fn, 0); add(S, f"{m}: FP", o.fp, n.fp, 0)
oen, nen = rd(OLD, "pz5_errors_by_current_regime"), rd(RES, "pz5_errors_by_current_regime")
bm = bn["best_model"]
add("8.2 / табл. 8", "модель в аналізі помилок", "GBT", bm, 0, "було: за test; стало: обрана за CV")
for ch, lab in ((0, "поточна година нижче порогу"), (1, "поточна година вище порогу")):
    o, n = oen[oen.cur_high == ch].iloc[0], nen[nen.cur_high == ch].iloc[0]
    for c, cn, d in (("n", "n", 0), ("pos_share", "частка класу 1", 4), ("error_rate", "частка помилок", 4), ("recall", "recall", 4), ("precision", "precision", 4)):
        add("8.2 / табл. 8", f"{lab}: {cn}", o[c], n[c], d)
oet, net = rd(OLD, "pz5_errors_by_transition").set_index("transition"), rd(RES, "pz5_errors_by_transition").set_index("transition")
for tname, lab in (("persist", "режим зберігається"), ("onset (0->1)", "початок сплеску"), ("decay (1->0)", "завершення сплеску")):
    add("8.2 / табл. 8", f"{lab}: n", oet.n[tname], net.n[tname], 0); add("8.2 / табл. 8", f"{lab}: частка помилок", oet.error_rate[tname], net.error_rate[tname])
oey, ney = rd(OLD, "pz5_errors_by_year").set_index("year"), rd(RES, "pz5_errors_by_year").set_index("year")
for y in (2025, 2026):
    for c, cn, d in (("n", "n", 0), ("pos_share", "частка класу 1", 4), ("error_rate", "частка помилок", 4), ("recall", "recall", 4), ("precision", "precision", 4)):
        add("8.2 / табл. 8", f"{y}: {cn}", oey.loc[y, c], ney.loc[y, c], d)
oeh, neh = rd(OLD, "pz5_errors_by_hour").set_index("hour"), rd(RES, "pz5_errors_by_hour").set_index("hour")
for h, c, cn in ((3, "error_rate", "частка помилок, рядок 03:00"), (12, "error_rate", "частка помилок, рядок 12:00"), (12, "pos_share", "частка класу 1, рядок 12:00"), (12, "recall", "recall, рядок 12:00"),
                 (13, "pos_share", "частка класу 1, рядок 13:00"), (13, "error_rate", "частка помилок, рядок 13:00")):
    add("8.2 / рис. 9", cn, oeh.loc[h, c], neh.loc[h, c])
# FP при поточній годині вище порогу (808 з 970) – із прогнозів
def fp_high(d):
    P = pd.read_csv(f"{d}/pz5_predictions_test_full.csv"); m = bm if d == RES else "GBT"; t = (nthr if d == RES else othr)[m]
    yh = P[f"p_{m}"] >= t; fp = yh & (P.label == 0); return int((fp & (P.persist == 1)).sum()), int(fp.sum())
fo, fn_ = fp_high(OLD), fp_high(RES)
add("8.2", "хибні тривоги при поточній rv > порогу / всього", f"{fo[0]} / {fo[1]}", f"{fn_[0]} / {fn_[1]}", 0)
# ---- Нові блоки (немає в старому звіті) ----
ci = rd(RES, "pz5_bootstrap_ci"); dd = rd(RES, "pz5_bootstrap_diff")
for m in ("LR", "RF", "GBT", "Персистентність", "log(rv/поріг)"):
    for c in ("pr_auc", "roc_auc", "f1"):
        r = ci[(ci.model == m) & (ci.metric == c)].iloc[0]
        add("нове: 95 % ДІ", f"{m}: {c}", None, f"{fm(r.estimate)} [{fm(r.ci_lo)}; {fm(r.ci_hi)}]", 0, "блочний бутстреп, тижневі блоки, 1000, seed 42")
for _, r in dd.iterrows():
    add("нове: різниці", f"{r.comparison}: {r.metric}", None, f"{r['diff']:+.4f} [{r.ci_lo:+.4f}; {r.ci_hi:+.4f}]".replace(".", ","), 0, f"ДІ не містить 0: {'так' if r.ci_excludes_0 else 'ні'}")
sel_ = rd(RES, "pz5_model_selection_cv")
for _, r in sel_.iterrows():
    add("нове: вибір за CV", f"{r.model}: середній CV PR-AUC", None, r.cv_pr_auc, 4, "обрано" if r.selected_by_cv else "")

df = pd.DataFrame(rows, columns=["Розділ / таблиця", "Величина", "Було", "Стало", "Статус", "Примітка"])
df.to_csv(f"{RES}/CHANGES_vs_old_run.csv", index=False, encoding="utf-8")
md = ["# ПЗ5: зміни чисел відносно `_old_run` (було -> стало)", "",
      "Старе значення береться з `_old_run/results` (запуск до рев'ю), нове – з `results/` поточного запуску (`bash code/run_all.sh`, seed 42). Таблиця охоплює кожне число основної частини поточного `report.md`; рядки «нове» стосуються блоків, яких у старому звіті не було. Секундні виміри залежать від навантаження машини (`results/loadavg.csv`).", "",
      "Причини змін детермінованих величин: (1) вхід ПЗ2 перезапущено, `rv` змінилася у 137 годинах першого дня місяця (`pz5_input_diff.csv`; 105 з них з 2018 р.); (2) остання година ряду з неповним наступним баром більше не отримує мітку (N = 77 666 замість 77 667); (3) поріг рішення тепер обирається за середнім F1 трьох валідаційних фолдів, а не за останнім; (4) модель обирається за CV PR-AUC; (5) порівняння випадкового й хронологічного поділу переписано (однакові години тесту, ДІ).", "",
      "| " + " | ".join(df.columns) + " |", "|" + "---|" * len(df.columns)]
for r in df.itertuples(index=False):
    md.append("| " + " | ".join(str(x).replace("|", "\\|") for x in r) + " |")
open(f"{RES}/CHANGES_vs_old_run.md", "w", encoding="utf-8").write("\n".join(md) + "\n")
print(len(df), "рядків;", (df["Статус"] == "змінилося").sum(), "змінилося;", (df["Статус"] == "=").sum(), "без змін")
