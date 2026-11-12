# -*- coding: utf-8 -*-
"""ПЗ5: статистика поверх прогнозів pz5_spark.py (numpy/pandas, без Spark; seed 42).
 1. блочний бутстреп (тижневі блоки, 1000 повторів) 95 % ДІ для PR-AUC, ROC-AUC, F1 і різниць GBT-LR, GBT-RF, GBT-персистентність;
 2. випадковий і хронологічний поділ на ОДНАКОВИХ годинах (ДІ та парні різниці);
 3. напрям ціни: біноміальний тест, z, правила «той самий / протилежний знак»;
 4. перевірка ембарго (ACF мітки).
Запуск (з каталогу pz5):  python code/pz5_stats.py"""
import os, sys, json, math, time
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pz5_common import RES, SEED, cap as _cap, save
SCRIPT = "pz5_stats.py"
def cap(name, desc): return _cap(name, SCRIPT, desc)
NBOOT, BLOCK_H = 1000, 168     # 1000 повторів; блок = 1 тиждень (168 год)
T0 = time.perf_counter()

# ---------- метрики (numpy) ----------
def roc_auc(y, s):
    r = pd.Series(s).rank().values; n1 = int(y.sum()); n0 = len(y) - n1
    return float((r[y == 1].sum() - n1 * (n1 + 1) / 2) / (n1 * n0))
def _curve(y, s):
    o = np.argsort(-s, kind="mergesort"); ys, ss = y[o], s[o]
    tp = np.cumsum(ys); fp = np.cumsum(1 - ys)
    keep = np.r_[np.where(np.diff(ss) != 0)[0], len(ss) - 1]
    tp, fp = tp[keep], fp[keep]
    return tp / ys.sum(), tp / (tp + fp)
def pr_auc(y, s):
    """Площа під PR-кривою трапеціями (як areaUnderPR у Spark: перша точка (0; precision першого порога))."""
    rc, pr = _curve(y, s); return float(np.trapezoid(np.r_[pr[0], pr], np.r_[0, rc]))
def avg_prec(y, s):
    """Average precision (ступінчаста інтерполяція, sklearn-подібна)."""
    rc, pr = _curve(y, s); return float(np.sum(np.diff(np.r_[0, rc]) * pr))
def f1_at(y, s, thr):
    yh = s >= thr; tp = int((yh & (y == 1)).sum()); fp = int((yh & (y == 0)).sum()); fn = int((~yh & (y == 1)).sum())
    return 2 * tp / (2 * tp + fp + fn) if 2 * tp + fp + fn else 0.0
def all_metrics(y, s, thrs):
    d = {"roc_auc": roc_auc(y, s), "pr_auc": pr_auc(y, s), "avg_precision": avg_prec(y, s)}
    for k, th in thrs.items(): d[k] = f1_at(y, s, th)
    return d

def week_groups(dt):
    wk = ((dt - dt.min()).dt.days // 7).values
    return [np.where(wk == w)[0] for w in np.unique(wk)]
def boot_metrics(groups, y, scores, thrs, seed, nboot=NBOOT):
    """scores: {name: array}; thrs: {name: {metric_name: thr}}. Повертає {(name, metric): array(nboot)}.
    Блоки = календарні тижні; повторна вибірка блоків із поверненням (кількість блоків зберігається)."""
    rng = np.random.default_rng(seed); nb = len(groups); out = {}
    for b in range(nboot):
        idx = np.concatenate([groups[g] for g in rng.integers(0, nb, nb)])
        yb = y[idx]
        for nm, s in scores.items():
            for k, v in all_metrics(yb, s[idx], thrs[nm]).items():
                out.setdefault((nm, k), np.empty(nboot))[b] = v
    return out
def ci(a): return float(np.percentile(a, 2.5)), float(np.percentile(a, 97.5))
def pboot(d): return float(min(1.0, 2 * min((d <= 0).mean(), (d >= 0).mean())))

# =============== 1. Бутстреп на хронологічному test ===============
P = pd.read_csv(f"{RES}/pz5_predictions_test_full.csv", parse_dates=["datetime_utc"]).sort_values("datetime_utc").reset_index(drop=True)
thr = json.load(open(f"{RES}/pz5_chosen_thresholds.json"))
M = pd.read_csv(f"{RES}/pz5_metrics_test.csv")
y = P.label.values.astype(int)
score = {"LR": P.p_LR.values, "RF": P.p_RF.values, "GBT": P.p_GBT.values, "Персистентність": P.persist.values, "log(rv/поріг)": P.log_rv_over_thr.values}
TH = {"LR": {"f1": thr["LR"], "f1_05": 0.5}, "RF": {"f1": thr["RF"], "f1_05": 0.5}, "GBT": {"f1": thr["GBT"], "f1_05": 0.5},
      "Персистентність": {"f1": 0.5}, "log(rv/поріг)": {"f1": 0.0}}
groups = week_groups(P.datetime_utc)
est = {nm: all_metrics(y, s, TH[nm]) for nm, s in score.items()}
bs = boot_metrics(groups, y, score, TH, SEED)
rows = []
for (nm, k), a in bs.items():
    lo, hi = ci(a); rows.append(dict(set="chrono test", model=nm, metric=k, estimate=est[nm][k], ci_lo=lo, ci_hi=hi, n=len(y), n_blocks=len(groups), block_hours=BLOCK_H, n_boot=NBOOT, seed=SEED))
cidf = pd.DataFrame(rows); save(cidf, "pz5_bootstrap_ci")
# звірка власних ROC/PR із Spark-значеннями (pz5_metrics_test.csv)
chk = []
mname = {"LR": "LR (поріг 0,5)", "RF": "RF (поріг 0,5)", "GBT": "GBT (поріг 0,5)", "Персистентність": "Персистентність (rv_t > порогу)", "log(rv/поріг)": "Ранжування за log(rv_t/поріг), поріг 0"}
for nm, mn in mname.items():
    r = M[M.model == mn].iloc[0]
    chk.append(dict(model=nm, spark_roc=r.roc_auc, numpy_roc=est[nm]["roc_auc"], spark_pr=r.pr_auc, numpy_pr=est[nm]["pr_auc"],
                    abs_diff_roc=abs(r.roc_auc - est[nm]["roc_auc"]), abs_diff_pr=abs(r.pr_auc - est[nm]["pr_auc"])))
save(pd.DataFrame(chk), "pz5_metric_check_spark_vs_numpy")
drows = []
for other in ("LR", "RF", "Персистентність", "log(rv/поріг)"):
    for k in ("pr_auc", "roc_auc", "f1"):
        d = bs[("GBT", k)] - bs[(other, k)]; lo, hi = ci(d)
        drows.append(dict(comparison=f"GBT - {other}", metric=k, diff=est["GBT"][k] - est[other][k], ci_lo=lo, ci_hi=hi, p_boot=pboot(d),
                          ci_excludes_0=bool(lo > 0 or hi < 0), n=len(y), n_blocks=len(groups), n_boot=NBOOT, seed=SEED))
# якщо за CV обрано не GBT – додаємо порівняння обраної моделі
sel = pd.read_csv(f"{RES}/pz5_best_model.csv").set_index("parameter").value["best_model"]
if sel != "GBT":
    for other in [m_ for m_ in ("LR", "RF", "GBT", "Персистентність", "log(rv/поріг)") if m_ != sel]:
        for k in ("pr_auc", "roc_auc", "f1"):
            d = bs[(sel, k)] - bs[(other, k)]; lo, hi = ci(d)
            drows.append(dict(comparison=f"{sel} - {other}", metric=k, diff=est[sel][k] - est[other][k], ci_lo=lo, ci_hi=hi, p_boot=pboot(d),
                              ci_excludes_0=bool(lo > 0 or hi < 0), n=len(y), n_blocks=len(groups), n_boot=NBOOT, seed=SEED))
ddf = pd.DataFrame(drows); save(ddf, "pz5_bootstrap_diff")
with cap("13_bootstrap", f"блочний бутстреп (тижневі блоки, {NBOOT} повторів, seed {SEED}): 95 % ДІ на test, n={len(y)}"):
    t_ = cidf[cidf.metric.isin(["pr_auc", "roc_auc", "f1"])].copy()
    t_["95% ДІ"] = t_.apply(lambda r: f"[{r.ci_lo:.4f}; {r.ci_hi:.4f}]", axis=1)
    print(t_[["model", "metric", "estimate", "95% ДІ"]].round(4).to_string(index=False))
    print("\nРізниці (парний бутстреп на тих самих блоках):")
    q = ddf.copy(); q["95% ДІ"] = q.apply(lambda r: f"[{r.ci_lo:+.4f}; {r.ci_hi:+.4f}]", axis=1)
    print(q[["comparison", "metric", "diff", "95% ДІ", "p_boot"]].round(4).to_string(index=False))

# =============== 2. Випадковий і хронологічний поділ на однакових годинах ===============
A = P[["datetime_utc", "label", "p_LR", "p_RF", "p_GBT"]]
B = pd.read_csv(f"{RES}/pz5_pred_random_pretest_on_chrono_test.csv", parse_dates=["datetime_utc"])
C = pd.read_csv(f"{RES}/pz5_pred_random_whole_test.csv", parse_dates=["datetime_utc"])
spl = pd.read_csv(f"{RES}/pz5_split.csv").set_index("parameter").value
TEST_START = pd.Timestamp(spl["test_start"])
MODELS = ("LR", "RF", "GBT")
def th2(nm): return {"f1": thr[nm], "f1_05": 0.5}
def mset(df, pref, seed):
    sc_ = {nm: df[f"{pref}{nm}"].values for nm in MODELS}
    yv = df.label.values.astype(int)
    return yv, sc_, boot_metrics(week_groups(df.datetime_utc), yv, sc_, {nm: th2(nm) for nm in MODELS}, seed), {nm: all_metrics(yv, sc_[nm], th2(nm)) for nm in MODELS}, len(week_groups(df.datetime_utc))
rows, drows = [], []
def put(variant, nm, k, e, a, n, nbk):
    lo, hi = ci(a); rows.append(dict(variant=variant, model=nm, metric=k, estimate=e, ci_lo=lo, ci_hi=hi, n=n, n_blocks=nbk))
# A: хроно -> хроно test; B: випадкові 80 % передтестового -> той самий test (парно)
AB = A.merge(B, on="datetime_utc", suffixes=("_A", "_B")); assert (AB.label_A == AB.label_B).all()
dA = AB.rename(columns={"label_A": "label"})
yv = dA.label.values.astype(int); g = week_groups(dA.datetime_utc)
scA = {nm: dA[f"p_{nm}_A"].values for nm in MODELS}; scB = {nm: dA[f"p_{nm}_B"].values for nm in MODELS}
sc_all = {**{f"A{nm}": scA[nm] for nm in MODELS}, **{f"B{nm}": scB[nm] for nm in MODELS}}
th_all = {**{f"A{nm}": th2(nm) for nm in MODELS}, **{f"B{nm}": th2(nm) for nm in MODELS}}
bsAB = boot_metrics(g, yv, sc_all, th_all, SEED)
for nm in MODELS:
    for k in ("pr_auc", "roc_auc", "f1", "f1_05"):
        eA, eB = all_metrics(yv, scA[nm], th2(nm))[k], all_metrics(yv, scB[nm], th2(nm))[k]
        put("A хроно -> хроно test", nm, k, eA, bsAB[(f"A{nm}", k)], len(yv), len(g)); put("B випадкові 80% передтестового -> той самий test", nm, k, eB, bsAB[(f"B{nm}", k)], len(yv), len(g))
        d = bsAB[(f"B{nm}", k)] - bsAB[(f"A{nm}", k)]; lo, hi = ci(d)
        drows.append(dict(comparison="B - A (ті самі рядки test)", model=nm, metric=k, diff=eB - eA, ci_lo=lo, ci_hi=hi, p_boot=pboot(d), n=len(yv), n_blocks=len(g)))
# C* vs A*: випадкова модель на випадковому тесті, обмеженому годинами >= test_start, проти хронологічної на ТИХ САМИХ годинах
Cs = C[C.datetime_utc >= TEST_START]
AC = A.merge(Cs, on="datetime_utc", suffixes=("_A", "_C")); assert (AC.label_A == AC.label_C).all()
dS = AC.rename(columns={"label_A": "label"}); yS = dS.label.values.astype(int); gS = week_groups(dS.datetime_utc)
scS = {**{f"A{nm}": dS[f"p_{nm}_A"].values for nm in MODELS}, **{f"C{nm}": dS[f"p_{nm}_C"].values for nm in MODELS}}
thS = {**{f"A{nm}": th2(nm) for nm in MODELS}, **{f"C{nm}": th2(nm) for nm in MODELS}}
bsS = boot_metrics(gS, yS, scS, thS, SEED)
for nm in MODELS:
    for k in ("pr_auc", "roc_auc", "f1", "f1_05"):
        eA, eC = all_metrics(yS, scS[f"A{nm}"], th2(nm))[k], all_metrics(yS, scS[f"C{nm}"], th2(nm))[k]
        put("A* хроно на тих самих годинах, що C*", nm, k, eA, bsS[(f"A{nm}", k)], len(yS), len(gS))
        put("C* випадковий поділ, тест лише >= test_start", nm, k, eC, bsS[(f"C{nm}", k)], len(yS), len(gS))
        d = bsS[(f"C{nm}", k)] - bsS[(f"A{nm}", k)]; lo, hi = ci(d)
        drows.append(dict(comparison="C* - A* (ті самі години)", model=nm, metric=k, diff=eC - eA, ci_lo=lo, ci_hi=hi, p_boot=pboot(d), n=len(yS), n_blocks=len(gS)))
# C: випадковий поділ усього періоду на власному тесті (інші рядки, ніж A) – незалежні бутстрепи
yC, scC, bsC, estC, nbC = mset(C.sort_values("datetime_utc").reset_index(drop=True), "p_", SEED + 1)
for nm in MODELS:
    for k in ("pr_auc", "roc_auc", "f1", "f1_05"):
        put("C випадковий поділ усього періоду -> власний випадковий test", nm, k, estC[nm][k], bsC[(nm, k)], len(yC), nbC)
        d = bsC[(nm, k)] - bs[(nm, k)]; lo, hi = ci(d); eA = est[nm][k] if k in est[nm] else all_metrics(y, score[nm], th2(nm))[k]
        drows.append(dict(comparison="C - A (різні рядки, незалежні бутстрепи)", model=nm, metric=k, diff=estC[nm][k] - eA, ci_lo=lo, ci_hi=hi, p_boot=pboot(d), n=len(yC), n_blocks=nbC))
rv = pd.DataFrame(rows); rv.to_csv(f"{RES}/pz5_random_vs_chrono_ci.csv", index=False)
rd = pd.DataFrame(drows); rd["ci_excludes_0"] = (rd.ci_lo > 0) | (rd.ci_hi < 0); rd.to_csv(f"{RES}/pz5_random_vs_chrono_diff.csv", index=False)
# «експозиція» витоку: яка частка випадкових тест-годин має обох сусідів (t-1, t+1) у випадковому train
allh = pd.read_csv(f"{RES}/pz5_target_series.csv", parse_dates=["datetime_utc"]).datetime_utc
test_set = set(C.datetime_utc); h = pd.Timedelta(hours=1)
nb_in_train = lambda t: ((t - h) in set_all and (t - h) not in test_set) and ((t + h) in set_all and (t + h) not in test_set)
set_all = set(allh)
expo = pd.DataFrame([("C_test_rows", len(C)), ("C_test_rows_>=test_start", len(Cs)),
                     ("share_C_test_with_both_neighbours_in_random_train", float(np.mean([nb_in_train(t) for t in C.datetime_utc]))),
                     ("share_C*_test_with_both_neighbours_in_random_train", float(np.mean([nb_in_train(t) for t in Cs.datetime_utc]))),
                     ("share_A_test_with_both_neighbours_in_train", 0.0)], columns=["parameter", "value"])
save(expo, "pz5_random_split_exposure")
with cap("14_random_same_hours", "випадковий проти хронологічного поділу на однакових годинах: PR-AUC, 95 % ДІ"):
    q = rv[rv.metric == "pr_auc"].copy(); q["variant"] = q.variant.str.split(" ").str[0]
    q["PR-AUC [95 % ДІ]"] = q.apply(lambda r: f"{r.estimate:.3f} [{r.ci_lo:.3f}; {r.ci_hi:.3f}]", axis=1)
    print(q[["variant", "model", "n", "PR-AUC [95 % ДІ]"]].to_string(index=False))
    print("\nРізниці PR-AUC (парні, ті самі години):")
    q = rd[(rd.metric == "pr_auc")].copy(); q["95% ДІ"] = q.apply(lambda r: f"[{r.ci_lo:+.3f}; {r.ci_hi:+.3f}]", axis=1)
    print(q[["comparison", "model", "n", "diff", "95% ДІ"]].round(3).to_string(index=False))

# =============== 3. Напрям ціни: біноміальний тест і правила знаку ===============
def binom_two_sided(k, n):
    m = min(k, n - k); lp = [math.lgamma(n + 1) - math.lgamma(i + 1) - math.lgamma(n - i + 1) - n * math.log(2) for i in range(m + 1)]
    mx = max(lp); return min(1.0, 2 * math.exp(mx) * sum(math.exp(v - mx) for v in lp))
def wilson(k, n, z=1.96):
    p = k / n; den = 1 + z * z / n; c = (p + z * z / (2 * n)) / den; hw = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / den
    return c - hw, c + hw
D = pd.read_csv(f"{RES}/pz5_direction_pred_test.csv", parse_dates=["datetime_utc"]).sort_values("datetime_utc").reset_index(drop=True)
yd = D.label_dir.values.astype(int); pred = (D.p1.values >= 0.5).astype(int)
n_d = len(yd); k_d = int((pred == yd).sum()); acc_d = k_d / n_d
z_d = (k_d - n_d / 2) / math.sqrt(n_d / 4); p_d = binom_two_sided(k_d, n_d); lo_w, hi_w = wilson(k_d, n_d)
maj = max(yd.mean(), 1 - yd.mean())
gD = week_groups(D.datetime_utc); rng = np.random.default_rng(SEED); accs, aucs = [], []
for _ in range(NBOOT):
    idx = np.concatenate([gD[i] for i in rng.integers(0, len(gD), len(gD))])
    accs.append((pred[idx] == yd[idx]).mean()); aucs.append(roc_auc(yd[idx], D.p1.values[idx]))
auc_d = roc_auc(yd, D.p1.values)
dirt = pd.DataFrame([dict(test="LR: accuracy vs 0,5", n=n_d, k_correct=k_d, estimate=acc_d, ci_lo=ci(np.array(accs))[0], ci_hi=ci(np.array(accs))[1], z=z_d, p_binom_two_sided=p_d,
                          note=f"Wilson 95 %: [{lo_w:.4f}; {hi_w:.4f}]; SE={math.sqrt(0.25 / n_d):.4f}; majority(test)={maj:.4f}"),
                     dict(test="LR: ROC-AUC", n=n_d, k_correct=np.nan, estimate=auc_d, ci_lo=ci(np.array(aucs))[0], ci_hi=ci(np.array(aucs))[1], z=np.nan, p_binom_two_sided=np.nan, note="блочний бутстреп")])
S = pd.read_csv(f"{RES}/pz5_target_series.csv", parse_dates=["datetime_utc"])
def sign_rule(df, name):
    v = df[(df.ret_1h != 0) & df.label_dir.notna()]
    same = ((v.ret_1h > 0).astype(int) == v.label_dir); n = len(v); k = int(same.sum())
    return dict(subset=name, n_hours=len(df), n_cur_zero_ret=int((df.ret_1h == 0).sum()), n_next_zero_ret=int((df.label_dir.isna()).sum()), n_used=n,
                same_sign_accuracy=k / n, opposite_sign_accuracy=1 - k / n, z_vs_0_5=(k - n / 2) / math.sqrt(n / 4), p_binom_two_sided=binom_two_sided(k, n),
                spearman_ret_t_ret_t1=float(v.ret_1h.rank().corr(v.next_ret.rank())), pearson_ret_t_ret_t1=float(v.ret_1h.corr(v.next_ret)))
sr = [sign_rule(S, "всі години"), sign_rule(S[S.split == "train"], "train"), sign_rule(S[S.split == "test"], "test")]
sr += [sign_rule(S[S.year == yv_], f"рік {yv_}") for yv_ in sorted(S.year.unique())]
srdf = pd.DataFrame(sr); save(srdf, "pz5_direction_sign_rules")
save(dirt, "pz5_direction_test")
with cap("15_direction_test", "напрям ціни: біноміальний тест LR і правила «той самий / протилежний знак»"):
    print(f"LR напрям, test: n={n_d}, правильних={k_d}, accuracy={acc_d:.4f} (мажоритарний {maj:.4f})")
    print(f"z={z_d:.2f} (SE={math.sqrt(0.25 / n_d):.4f}), p(двосторонній біном.)={p_d:.2e}, ROC-AUC={auc_d:.4f}")
    print(f"Wilson 95 %: [{lo_w:.4f}; {hi_w:.4f}];  бутстреп-ДІ accuracy: [{ci(np.array(accs))[0]:.4f}; {ci(np.array(accs))[1]:.4f}]")
    print("\nПравило знаку поточної години (години з ненульовою дохідністю):")
    q = srdf[["subset", "n_used", "same_sign_accuracy", "opposite_sign_accuracy", "z_vs_0_5"]].copy()
    print(q.round(4).to_string(index=False))
    print(f"\nSpearman(ret_t, ret_t+1), всі: {srdf.spearman_ret_t_ret_t1[0]:.4f};  Pearson: {srdf.pearson_ret_t_ret_t1[0]:.4f}")

# =============== 4. Ембарго: ACF мітки ===============
lab_ = S.label.values.astype(float); lab_ = lab_ - lab_.mean(); n_l = len(lab_); bound = 1.96 / math.sqrt(n_l)
ac = {k: float((lab_[:-k] * lab_[k:]).sum() / (lab_ * lab_).sum()) for k in range(1, 721)}
emb = [dict(lag_h=k, acf_label=ac[k], conf95=bound) for k in (1, 2, 3, 6, 12, 24, 48, 72, 168, 336, 720)]
emb_df = pd.DataFrame(emb); save(emb_df, "pz5_embargo_check")
extra = pd.DataFrame([("n_rows", n_l), ("lags_1_720_with_abs_acf_above_bound", int(sum(abs(v) > bound for v in ac.values()))),
                      ("label_horizon_h(min_embargo)", 1), ("embargo_used_h", 24), ("train_labels_using_test_hours_if_no_embargo", 1)], columns=["parameter", "value"])
save(extra, "pz5_embargo_summary")
print("\nЕмбарго:", emb_df.round(4).to_string(index=False)); print(extra.to_string(index=False))
rec = pd.DataFrame([("pz5_stats_seconds", round(time.perf_counter() - T0, 2), f"bootstrap {NBOOT}, block {BLOCK_H} h")], columns=["step", "seconds", "note"])
tm = pd.read_csv(f"{RES}/timings.csv"); tm = tm[tm.step != "pz5_stats_seconds"]
pd.concat([tm, rec]).to_csv(f"{RES}/timings.csv", index=False)
