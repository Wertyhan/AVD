# -*- coding: utf-8 -*-
"""ПЗ5: аналіз помилок (csv) та рисунки (dpi 150, українські підписи) із results/*.csv."""
import os, json
import numpy as np, pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
R, FG = f"{BASE}/results", f"{BASE}/figures"
os.makedirs(FG, exist_ok=True)
plt.rcParams.update({"font.size": 10, "axes.grid": True, "grid.alpha": .3, "figure.dpi": 150, "savefig.dpi": 150,
                     "axes.spines.top": False, "axes.spines.right": False})
C = {"LR": "#1f77b4", "RF": "#2ca02c", "GBT": "#d62728", "persist": "#7f7f7f"}
def fig_save(name): plt.tight_layout(); plt.savefig(f"{FG}/{name}.png"); plt.close()

# 1. Цільова змінна в часі
s = pd.read_csv(f"{R}/pz5_target_series.csv", parse_dates=["datetime_utc"]).set_index("datetime_utc")
fig, ax = plt.subplots(2, 1, figsize=(10, 6), sharex=True)
d1 = s.resample("D").agg({"rv": "mean", "thr_rv": "mean", "label": "mean"})
ax[0].plot(d1.index, d1.rv, lw=.6, label="середня rv за добу"); ax[0].plot(d1.index, d1.thr_rv, lw=1.2, color="C3", label="поріг (75-й перцентиль за 720 год)")
ax[0].set_yscale("log"); ax[0].set_ylabel("Реалізована волатильність rv"); ax[0].legend(); ax[0].set_title("Волатильність і ковзний поріг цільової змінної")
m30 = s.label.resample("30D").mean()
ax[1].plot(m30.index, m30.values, color="C1"); ax[1].axhline(s.label.mean(), ls="--", color="k", lw=.8, label=f"середнє {s.label.mean():.3f}")
ax[1].set_ylabel("Частка класу 1 (30 діб)"); ax[1].legend(); ax[1].set_xlabel("Дата (UTC)")
fig_save("fig01_target_over_time")

# 2. ACF
a = pd.read_csv(f"{R}/pz5_acf.csv"); lags = a.lag_h.values
fig, ax = plt.subplots(1, 2, figsize=(10, 3.8), sharey=True)
for x, col, t in ((ax[0], "acf_ret", "ACF годинних дохідностей"), (ax[1], "acf_abs_ret", "ACF модуля дохідностей |r|")):
    x.bar(lags, a[col], color="C0"); x.axhline(a.conf95[0], ls="--", color="r", lw=.8); x.axhline(-a.conf95[0], ls="--", color="r", lw=.8, label="95 % межа")
    x.set_title(t); x.set_xlabel("Лаг, год")
ax[0].set_ylabel("Автокореляція"); ax[0].legend()
fig_save("fig02_acf_returns_vs_abs")

# Прогнози
P = pd.read_csv(f"{R}/pz5_predictions_test_full.csv", parse_dates=["datetime_utc"])
thr = json.load(open(f"{R}/pz5_chosen_thresholds.json"))
y = P.label.values
def roc_pr(score, y):
    o = np.argsort(-score, kind="mergesort"); ys = y[o]; sc = score[o]
    tp = np.cumsum(ys); fp = np.cumsum(1 - ys)
    keep = np.r_[np.where(np.diff(sc) != 0)[0], len(sc) - 1]
    tp, fp = tp[keep], fp[keep]
    tpr = np.r_[0, tp / y.sum()]; fpr = np.r_[0, fp / (1 - y).sum()]; prec = np.r_[1, tp / (tp + fp)]
    return fpr, tpr, tpr, prec
scores = {"Персистентність": P.persist.values, "LR": P.p_LR.values, "RF": P.p_RF.values, "GBT": P.p_GBT.values}
cols = {"Персистентність": C["persist"], "LR": C["LR"], "RF": C["RF"], "GBT": C["GBT"]}
M = pd.read_csv(f"{R}/pz5_metrics_test.csv")
fig, ax = plt.subplots(1, 2, figsize=(10, 4.3))
for k, sc in scores.items():
    fpr, tpr, rc, pr = roc_pr(sc, y)
    auc = float(np.trapezoid(tpr, fpr)); ap = float(-np.sum(np.diff(rc) * pr[1:]))
    ax[0].plot(fpr, tpr, color=cols[k], label=f"{k} (AUC={auc:.3f})"); ax[1].plot(rc, pr, color=cols[k], label=f"{k} (AP={ap:.3f})")
ax[0].plot([0, 1], [0, 1], "k--", lw=.8); ax[0].set_xlabel("FPR"); ax[0].set_ylabel("TPR"); ax[0].set_title("ROC-криві (test)"); ax[0].legend()
ax[1].axhline(y.mean(), ls="--", color="k", lw=.8, label=f"частка класу 1 = {y.mean():.3f}"); ax[1].set_xlabel("Recall"); ax[1].set_ylabel("Precision")
ax[1].set_title("PR-криві (test)"); ax[1].legend()
fig_save("fig03_roc_pr")

# 4. Матриці помилок
fig, ax = plt.subplots(1, 3, figsize=(11, 3.6))
for x, nm in zip(ax, ("LR", "RF", "GBT")):
    r = M[M.model == f"{nm} (поріг {thr[nm]:.2f}, з валідації)"].iloc[0]
    cm = np.array([[r.tn, r.fp], [r.fn, r.tp]])
    x.imshow(cm, cmap="Blues"); x.grid(False)
    for i in range(2):
        for j in range(2): x.text(j, i, f"{int(cm[i, j])}", ha="center", va="center", color="white" if cm[i, j] > cm.max() / 2 else "black")
    x.set_xticks([0, 1], ["прогноз 0", "прогноз 1"]); x.set_yticks([0, 1], ["факт 0", "факт 1"]); x.set_title(f"{nm}, поріг {thr[nm]:.2f}")
fig_save("fig04_confusion_matrices")

# 5. Важливість ознак
I = pd.read_csv(f"{R}/pz5_feature_importance.csv")
fig, ax = plt.subplots(1, 3, figsize=(12, 4.8))
for x, col, t, c in ((ax[0], "RF_importance", "Random Forest", C["RF"]), (ax[1], "GBT_importance", "GBT", C["GBT"]), (ax[2], "LR_coef_standardized", "LR: стандартизовані коефіцієнти", C["LR"])):
    q = I.reindex(I[col].abs().sort_values().index).tail(12)
    x.barh(q.feature, q[col], color=c); x.set_title(t)
fig_save("fig05_feature_importance")

# Аналіз помилок (за найкращою за PR-AUC моделлю з порогом із валідації)
best = M[M.kind == "model_tuned_thr"].sort_values("pr_auc", ascending=False).iloc[0].model.split(" ")[0]
P["yhat"] = (P[f"p_{best}"] >= thr[best]).astype(int); P["err"] = (P.yhat != P.label).astype(int)
P["fn"] = ((P.yhat == 0) & (P.label == 1)).astype(int); P["fp"] = ((P.yhat == 1) & (P.label == 0)).astype(int)
P["cur_high"] = P.persist.astype(int)
def grp(k):
    g = P.groupby(k).agg(n=("err", "size"), pos_share=("label", "mean"), error_rate=("err", "mean"), fn_rate=("fn", "sum"), fp=("fp", "sum"))
    g["recall"] = P[P.label == 1].groupby(k).yhat.mean(); g["precision"] = P[P.yhat == 1].groupby(k).label.mean()
    g["fn_rate"] = g.fn_rate / (g.n * g.pos_share); return g.reset_index()
ey, eh = grp("year"), grp("hour"); er = grp("cur_high")
tr = P.assign(transition=np.select([(P.cur_high == 0) & (P.label == 1), (P.cur_high == 1) & (P.label == 0), P.cur_high == P.label], ["onset (0->1)", "decay (1->0)", "persist"], "other"))
et = tr.groupby("transition").agg(n=("err", "size"), error_rate=("err", "mean")).reset_index()
ey.to_csv(f"{R}/pz5_errors_by_year.csv", index=False); eh.to_csv(f"{R}/pz5_errors_by_hour.csv", index=False)
er.to_csv(f"{R}/pz5_errors_by_current_regime.csv", index=False); et.to_csv(f"{R}/pz5_errors_by_transition.csv", index=False)
fig, ax = plt.subplots(1, 2, figsize=(11, 3.9))
ax[0].bar(ey.year.astype(str), ey.error_rate, color="C0"); ax[0].set_title(f"Частка помилок за роками (test, {best})"); ax[0].set_ylabel("Частка помилок")
for i, (e, n) in enumerate(zip(ey.error_rate, ey.n)): ax[0].text(i, e, f"n={n}", ha="center", va="bottom", fontsize=8)
ax[1].bar(eh.hour, eh.error_rate, color="C1"); ax[1].plot(eh.hour, eh.pos_share, "k.-", lw=.8, label="частка класу 1"); ax[1].legend()
ax[1].set_title("Частка помилок за годиною доби (UTC)"); ax[1].set_xlabel("Година")
fig_save("fig06_errors_year_hour")

# 7. Випадковий vs хронологічний
c = pd.read_csv(f"{R}/pz5_random_vs_chrono.csv")
fig, ax = plt.subplots(1, 3, figsize=(11, 3.8))
for x, met, t in zip(ax, ("roc_auc", "pr_auc", "f1"), ("ROC-AUC", "PR-AUC", "F1 (поріг 0,5)")):
    pv = c.pivot(index="model", columns="split", values=met).loc[["LR", "RF", "GBT"]]
    pv.plot.bar(ax=x, rot=0, color=["#4c78a8", "#e45756"]); x.set_title(t); x.set_xlabel(""); x.legend(fontsize=7)
    lo = pv.values.min(); x.set_ylim(max(0, lo - .08), pv.values.max() + .03)
fig_save("fig07_random_vs_chrono")

# 8. Порівняння моделей та час
tm = pd.read_csv(f"{R}/pz5_model_runtimes.csv")
mm = M[M.kind != "model"].copy()
fig, ax = plt.subplots(1, 2, figsize=(11, 4))
lab = [n.replace(" (поріг", "\n(поріг").replace("(rv_t > порогу)", "") for n in mm.model]
xx = np.arange(len(mm)); wd = .2
for i, (met, col) in enumerate((("f1", "C0"), ("roc_auc", "C2"), ("pr_auc", "C3"), ("accuracy", "C7"))):
    ax[0].bar(xx + (i - 1.5) * wd, mm[met], wd, label=met, color=col)
ax[0].set_xticks(xx, [l[:22] for l in lab], rotation=40, ha="right", fontsize=7); ax[0].legend(fontsize=7); ax[0].set_title("Метрики на test")
ax[1].bar(tm.model, tm.fit_seconds, label="навчання"); ax[1].bar(tm.model, tm.inference_seconds_test, bottom=tm.fit_seconds, label="інференс (test)")
ax[1].set_ylabel("с"); ax[1].set_title("Час виконання"); ax[1].legend()
fig_save("fig08_model_comparison")

# 9. Налаштування гіперпараметрів
T = pd.read_csv(f"{R}/pz5_tuning.csv")
fig, ax = plt.subplots(figsize=(8, 3.8))
for nm in ("LR", "RF", "GBT"):
    q = T[T.model == nm].sort_values("cv_pr_auc", ascending=False).reset_index(drop=True)
    ax.plot(q.index + 1, q.cv_pr_auc, "o-", color=C[nm], label=nm)
ax.set_xlabel("Ранг конфігурації"); ax.set_ylabel("Середній PR-AUC (3 фолди)"); ax.legend(); ax.set_title("Часова крос-валідація: сітка гіперпараметрів")
fig_save("fig09_tuning")

# 10. Вибір порогу
V = pd.read_csv(f"{R}/pz5_threshold_validation.csv")
fig, ax = plt.subplots(figsize=(7, 3.8))
for nm in ("LR", "RF", "GBT"):
    q = V[V.model == nm]; ax.plot(q.threshold, q.val_f1, color=C[nm], label=f"{nm} (макс. при {thr[nm]:.2f})"); ax.axvline(thr[nm], color=C[nm], ls=":", lw=.8)
ax.set_xlabel("Поріг ймовірності"); ax.set_ylabel("F1 на валідації"); ax.legend(); ax.set_title("Вибір порогу рішення (валідаційний фолд)")
fig_save("fig10_threshold")
print("ok", best)
