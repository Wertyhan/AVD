# -*- coding: utf-8 -*-
"""ПЗ5: аналіз помилок (csv) та рисунки з results/*.csv. Правило 2b: друкований розмір 6,3 x 3,4 / 4,7 дюйма,
шрифти >= 9 pt, dpi 200, українські підписи, десяткова кома."""
import os, sys, json
import numpy as np, pandas as pd
sys.path.insert(0, "/home/user/AVD/work/tools_report"); sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import plotstyle
from plotstyle import dec, comma_fmt, SINGLE, TWO_PANEL
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from pz5_common import RES as R, FIG as FG

C = {"LR": "#1f77b4", "RF": "#2ca02c", "GBT": "#d62728", "persist": "#7f7f7f", "ratio": "#9467bd"}
def fig_save(name): plt.tight_layout(); plt.savefig(f"{FG}/{name}.png"); plt.close()
def yc(ax): ax.yaxis.set_major_formatter(comma_fmt)
def xc(ax): ax.xaxis.set_major_formatter(comma_fmt)
thin = lambda v: f"{int(v):,}".replace(",", " ")     # тонкий пропуск між розрядами

FEAT_UA = {"log_rv_l0": "lg rv, ця година", "log_rv_l1": "lg rv, 1 год тому", "log_rv_l2": "lg rv, 2 год тому", "log_rv_l5": "lg rv, 5 год тому",
           "log_rv_l23": "lg rv, 23 год тому", "log_rv_m3": "ср. rv за 3 год", "log_rv_m6": "ср. rv за 6 год", "log_rv_m24": "ср. rv за 24 год",
           "log_rv_m168": "ср. rv за 168 год", "log_rv_over_thr": "lg(rv / поріг)", "ret_1h": "дохідність 1 год", "abs_ret_1h": "|дохідність| 1 год",
           "ret_6h": "дохідність 6 год", "log_rel_range": "lg відн. розмах", "log_volume": "lg обсяг", "vol_z168": "z-обсяг (168 год)",
           "flat_share": "частка пласких хв", "hour_sin": "година доби, sin", "hour_cos": "година доби, cos", "is_weekend": "вихідний день"}
def ua(f):
    if f.startswith("dow_ohe_"): return "день тижня, " + f.replace("dow_ohe_", "кат. ")
    return FEAT_UA.get(f, f)
GROUP = {**{k: "рівень rv (лаги, середні, відношення до порога)" for k in ("log_rv_l0", "log_rv_l1", "log_rv_l2", "log_rv_l5", "log_rv_l23", "log_rv_m3", "log_rv_m6", "log_rv_m24", "log_rv_m168", "log_rv_over_thr")},
         **{k: "дохідність" for k in ("ret_1h", "abs_ret_1h", "ret_6h")}, **{k: "розмах і обсяг" for k in ("log_rel_range", "log_volume", "vol_z168", "flat_share")},
         **{k: "календар" for k in ("hour_sin", "hour_cos", "is_weekend")}}
thr = json.load(open(f"{R}/pz5_chosen_thresholds.json"))
best = pd.read_csv(f"{R}/pz5_best_model.csv").set_index("parameter").value["best_model"]     # обрано за CV, не за test
M = pd.read_csv(f"{R}/pz5_metrics_test.csv")

# 1. Цільова змінна в часі
s = pd.read_csv(f"{R}/pz5_target_series.csv", parse_dates=["datetime_utc"]).set_index("datetime_utc")
fig, ax = plt.subplots(2, 1, figsize=TWO_PANEL, sharex=True)
d1 = s.resample("D").agg({"rv": "mean", "thr_rv": "mean", "label": "mean"})
ax[0].plot(d1.index, d1.rv, lw=.6, label="середня за добу погодинна rv"); ax[0].plot(d1.index, d1.thr_rv, lw=1.2, color="C3", label="поріг (75-й перцентиль за 720 год)")
ax[0].set_yscale("log"); ax[0].set_ylabel("rv"); ax[0].legend(loc="upper right", fontsize=9); ax[0].set_title("Волатильність і ковзний поріг цільової змінної")
m30 = s.label.resample("30D").mean()
ax[1].plot(m30.index, m30.values, color="C1"); ax[1].axhline(s.label.mean(), ls="--", color="k", lw=.8, label=f"середнє {dec(s.label.mean(), 3)}")
ax[1].set_ylabel("Частка класу 1 (30 діб)"); ax[1].legend(loc="upper right", fontsize=9); ax[1].set_xlabel("Дата (UTC)"); yc(ax[1])
fig_save("fig01_target_over_time")

# 2. ACF
a = pd.read_csv(f"{R}/pz5_acf.csv"); lags = a.lag_h.values
fig, ax = plt.subplots(1, 2, figsize=SINGLE, sharey=True)
for x, col, t in ((ax[0], "acf_ret", "ACF дохідностей"), (ax[1], "acf_abs_ret", "ACF модуля |r|")):
    x.bar(lags, a[col], color="C0", width=.8); x.axhline(a.conf95[0], ls="--", color="r", lw=.8); x.axhline(-a.conf95[0], ls="--", color="r", lw=.8, label="95 % межа")
    x.set_title(t); x.set_xlabel("Лаг, год"); yc(x)
ax[0].set_ylabel("Автокореляція"); ax[1].legend(loc="upper right", fontsize=9)
fig_save("fig02_acf_returns_vs_abs")

# Прогнози
P = pd.read_csv(f"{R}/pz5_predictions_test_full.csv", parse_dates=["datetime_utc"])
y = P.label.values
def roc_pr(score, y):
    o = np.argsort(-score, kind="mergesort"); ys = y[o]; sc = score[o]
    tp = np.cumsum(ys); fp = np.cumsum(1 - ys)
    keep = np.r_[np.where(np.diff(sc) != 0)[0], len(sc) - 1]
    tp, fp = tp[keep], fp[keep]
    tpr = np.r_[0, tp / y.sum()]; fpr = np.r_[0, fp / (1 - y).sum()]; prec = np.r_[1, tp / (tp + fp)]
    return fpr, tpr, tpr, prec
scores = {"Персист.": P.persist.values, "LR": P.p_LR.values, "RF": P.p_RF.values, "GBT": P.p_GBT.values}
cols = {"Персист.": C["persist"], "LR": C["LR"], "RF": C["RF"], "GBT": C["GBT"]}
MROW = {"Персист.": "Персистентність (rv_t > порогу)", "LR": "LR (поріг 0,5)", "RF": "RF (поріг 0,5)", "GBT": "GBT (поріг 0,5)"}
fig, ax = plt.subplots(1, 2, figsize=SINGLE)
for k, sc in scores.items():
    fpr, tpr, rc, pr = roc_pr(sc, y)
    row = M[M.model == MROW[k]].iloc[0]; auc, ap = float(row.roc_auc), float(row.pr_auc)
    assert abs(auc - float(np.trapezoid(tpr, fpr))) < 5e-3, (k, auc)      # звірка кривої зі значенням Spark
    ax[0].plot(fpr, tpr, color=cols[k], label=f"{k} {dec(auc, 3)}"); ax[1].plot(rc, pr, color=cols[k], label=f"{k} {dec(ap, 3)}")
ax[0].plot([0, 1], [0, 1], "k--", lw=.8); ax[0].set_xlabel("FPR"); ax[0].set_ylabel("TPR"); ax[0].set_title("ROC (test)"); ax[0].legend(loc="lower right", fontsize=9)
ax[1].axhline(y.mean(), ls="--", color="k", lw=.8); ax[1].set_xlabel("Recall"); ax[1].set_ylabel("Precision"); ax[1].set_title("PR (test)"); ax[1].legend(loc="lower left", fontsize=9)
for x in ax: xc(x); yc(x)
fig_save("fig03_roc_pr")

# 4. Матриці помилок (поріг з валідації)
fig, ax = plt.subplots(1, 3, figsize=SINGLE)
for x, nm in zip(ax, ("LR", "RF", "GBT")):
    r = M[M.model == f"{nm} (поріг {thr[nm]:.2f}, з валідації)"].iloc[0]
    cm = np.array([[r.tn, r.fp], [r.fn, r.tp]])
    x.imshow(cm, cmap="Blues"); x.grid(False)
    for i in range(2):
        for j in range(2): x.text(j, i, thin(cm[i, j]), ha="center", va="center", fontsize=9, color="white" if cm[i, j] > cm.max() / 2 else "black")
    x.set_xticks([0, 1], ["0", "1"]); x.set_yticks([0, 1], ["0", "1"]); x.set_xlabel("прогноз"); x.set_title(f"{nm}, {dec(thr[nm], 2)}")
ax[0].set_ylabel("факт")
fig_save("fig04_confusion_matrices")

# 5. Важливість ознак (імпурітна для RF і GBT, стандартизовані коефіцієнти LR) + групові суми
I = pd.read_csv(f"{R}/pz5_feature_importance.csv")
I["group"] = I.feature.map(GROUP).fillna("календар (день тижня)")
g = I.groupby("group")[["RF_importance", "GBT_importance"]].sum().reset_index(); g["LR_sum_abs_coef"] = I.groupby("group").LR_abs_coef.sum().values
g.to_csv(f"{R}/pz5_feature_importance_groups.csv", index=False)
fig, ax = plt.subplots(2, 1, figsize=TWO_PANEL)
q = I.assign(mean_imp=(I.RF_importance + I.GBT_importance) / 2).sort_values("mean_imp").tail(8)
yy = np.arange(len(q)); ax[0].barh(yy + .2, q.RF_importance, .4, color=C["RF"], label="RF"); ax[0].barh(yy - .2, q.GBT_importance, .4, color=C["GBT"], label="GBT")
ax[0].set_yticks(yy, [ua(f) for f in q.feature]); ax[0].set_xlabel("Важливість ознаки (імпурітна)"); ax[0].legend(loc="lower right", fontsize=9); xc(ax[0])
q = I.reindex(I.LR_coef_standardized.abs().sort_values().index).tail(8)
ax[1].barh([ua(f) for f in q.feature], q.LR_coef_standardized, color=C["LR"]); ax[1].set_xlabel("LR: стандартизований коефіцієнт"); ax[1].axvline(0, color="k", lw=.6); xc(ax[1])
fig_save("fig05_feature_importance")

# Аналіз помилок (модель, обрана за CV, з порогом із валідації)
P["yhat"] = (P[f"p_{best}"] >= thr[best]).astype(int); P["err"] = (P.yhat != P.label).astype(int)
P["fn"] = ((P.yhat == 0) & (P.label == 1)).astype(int); P["fp"] = ((P.yhat == 1) & (P.label == 0)).astype(int)
P["cur_high"] = P.persist.astype(int)
def grp(k):
    gg = P.groupby(k).agg(n=("err", "size"), pos_share=("label", "mean"), error_rate=("err", "mean"), fn_rate=("fn", "sum"), fp=("fp", "sum"))
    gg["recall"] = P[P.label == 1].groupby(k).yhat.mean(); gg["precision"] = P[P.yhat == 1].groupby(k).label.mean()
    gg["fn_rate"] = gg.fn_rate / (gg.n * gg.pos_share); return gg.reset_index()
ey, eh, er = grp("year"), grp("hour"), grp("cur_high")
tr = P.assign(transition=np.select([(P.cur_high == 0) & (P.label == 1), (P.cur_high == 1) & (P.label == 0), P.cur_high == P.label], ["onset (0->1)", "decay (1->0)", "persist"], "other"))
et = tr.groupby("transition").agg(n=("err", "size"), error_rate=("err", "mean")).reset_index()
for df_, nm in ((ey, "year"), (eh, "hour"), (er, "current_regime"), (et, "transition")):
    df_.assign(model=best, threshold=thr[best]).to_csv(f"{R}/pz5_errors_by_{nm}.csv", index=False)
fig, ax = plt.subplots(2, 1, figsize=TWO_PANEL)
ax[0].bar(ey.year.astype(str), ey.error_rate, color="C0"); ax[0].set_title(f"Частка помилок за роками (test, {best})"); ax[0].set_ylabel("Частка помилок"); yc(ax[0])
for i, e in enumerate(ey.error_rate): ax[0].text(i, e, dec(e, 2), ha="center", va="bottom", fontsize=9)
ax[0].set_ylim(0, ey.error_rate.max() * 1.18)
ax[1].bar(eh.hour, eh.error_rate, color="C1", label="частка помилок"); ax[1].plot(eh.hour, eh.pos_share, "k.-", lw=.8, label="частка класу 1"); ax[1].legend(loc="upper left", fontsize=9, ncol=2)
ax[1].set_title("За годиною доби (UTC)"); ax[1].set_xlabel("Година"); ax[1].set_xticks(range(0, 24, 3)); yc(ax[1]); ax[1].set_ylim(0, max(eh.error_rate.max(), eh.pos_share.max()) * 1.25)
fig_save("fig06_errors_year_hour")

# 7. Випадковий і хронологічний поділ на однакових годинах (ДІ: блочний бутстреп)
rv = pd.read_csv(f"{R}/pz5_random_vs_chrono_ci.csv")
VAR = [("A хроно -> хроно test", "A: хроно → test"), ("B випадкові 80% передтестового -> той самий test", "B: випадк. 80 % передтесту\n→ той самий test"),
       ("A* хроно на тих самих годинах, що C*", "A*: хроно, години C*"), ("C* випадковий поділ, тест лише >= test_start", "C*: випадк. поділ,\nлише години test"),
       ("C випадковий поділ усього періоду -> власний випадковий test", "C: випадк. поділ,\nвесь період")]
fig, ax = plt.subplots(1, 2, figsize=TWO_PANEL, sharey=True)
for x, met, t in zip(ax, ("pr_auc", "roc_auc"), ("PR-AUC", "ROC-AUC")):
    for j, (v, lab_) in enumerate(VAR):
        for k, nm in enumerate(("LR", "RF", "GBT")):
            r = rv[(rv.variant == v) & (rv.model == nm) & (rv.metric == met)].iloc[0]
            yp = -j + (1 - k) * .22
            x.errorbar(r.estimate, yp, xerr=[[r.estimate - r.ci_lo], [r.ci_hi - r.estimate]], fmt="o", ms=4, color=C[nm], capsize=2, lw=1.2, label=nm if j == 0 else None)
    x.set_xlabel(t); xc(x); x.set_title(t)
ax[0].set_yticks([-j for j in range(len(VAR))], [f"{l}\nn={thin(rv[(rv.variant == v)].n.iloc[0])}" for v, l in VAR]); ax[0].legend(loc="lower left", fontsize=9)
fig_save("fig07_random_vs_chrono")

# 8. Порівняння моделей: оцінки з 95 % ДІ + час виконання
ci = pd.read_csv(f"{R}/pz5_bootstrap_ci.csv"); tm = pd.read_csv(f"{R}/pz5_model_runtimes.csv")
names = [("Персистентність", "Персист."), ("log(rv/поріг)", "log(rv/пор.)"), ("LR", "LR"), ("RF", "RF"), ("GBT", "GBT")]
fig, ax = plt.subplots(2, 1, figsize=TWO_PANEL, gridspec_kw={"height_ratios": [2.2, 1]})
for k, (met, ln, col) in enumerate((("pr_auc", "PR-AUC", "C3"), ("roc_auc", "ROC-AUC", "C2"), ("f1", "F1", "C0"))):
    for i, (nm, lab_) in enumerate(names):
        r = ci[(ci.model == nm) & (ci.metric == met)].iloc[0]
        ax[0].errorbar(i + (k - 1) * .22, r.estimate, yerr=[[r.estimate - r.ci_lo], [r.ci_hi - r.estimate]], fmt="o", ms=4, color=col, capsize=2, label=ln if i == 0 else None)
ax[0].set_xticks(range(len(names)), [l for _, l in names]); ax[0].set_ylabel("Значення (95 % ДІ)"); ax[0].legend(loc="lower right", fontsize=9, ncol=3); yc(ax[0]); ax[0].set_title("Метрики на test з 95 % ДІ (блочний бутстреп)")
ax[1].bar(tm.model, tm.fit_seconds, color="C0", label="навчання"); ax[1].bar(tm.model, tm.inference_seconds_test, bottom=tm.fit_seconds, color="C1", label="інференс (test)")
ax[1].set_ylabel("Час, с"); ax[1].legend(loc="upper left", fontsize=9)
fig_save("fig08_model_comparison")

# 9. Налаштування гіперпараметрів
T = pd.read_csv(f"{R}/pz5_tuning.csv")
fig, ax = plt.subplots(figsize=SINGLE)
for nm in ("LR", "RF", "GBT"):
    qq = T[T.model == nm].sort_values("cv_pr_auc", ascending=False).reset_index(drop=True)
    ax.errorbar(qq.index + 1, qq.cv_pr_auc, yerr=qq.cv_pr_auc_std, fmt="o-", ms=4, lw=1, capsize=2, color=C[nm], label=nm, alpha=.9)
ax.set_xlabel("Ранг конфігурації"); ax.set_ylabel("Середній PR-AUC (3 фолди)"); ax.legend(loc="lower left", fontsize=9, ncol=3); yc(ax); ax.set_title("Часова CV: сітка гіперпараметрів (± σ між фолдами)")
fig_save("fig09_tuning")

# 10. Вибір порогу (середній F1 за 3 фолдами)
V = pd.read_csv(f"{R}/pz5_threshold_validation.csv")
fig, ax = plt.subplots(figsize=SINGLE)
for nm in ("LR", "RF", "GBT"):
    qq = V[V.model == nm]; ax.plot(qq.threshold, qq.val_f1, color=C[nm], label=f"{nm}, макс. при {dec(thr[nm], 2)}"); ax.axvline(thr[nm], color=C[nm], ls=":", lw=.8)
ax.set_xlabel("Поріг ймовірності"); ax.set_ylabel("Середній F1 (3 фолди)"); ax.legend(loc="lower center", fontsize=9); xc(ax); yc(ax); ax.set_title("Вибір порогу рішення")
fig_save("fig10_threshold")

# 11. Різниці GBT − інші моделі з 95 % ДІ
dd = pd.read_csv(f"{R}/pz5_bootstrap_diff.csv"); dd = dd[dd.comparison.str.startswith("GBT -")]
fig, ax = plt.subplots(figsize=SINGLE)
rows_ = [(c, m_) for c in ("GBT - LR", "GBT - RF", "GBT - Персистентність") for m_ in ("pr_auc", "roc_auc", "f1")]
lab_m = {"pr_auc": "PR-AUC", "roc_auc": "ROC-AUC", "f1": "F1"}; lab_c = {"GBT - LR": "GBT − LR", "GBT - RF": "GBT − RF", "GBT - Персистентність": "GBT − персист."}
for i, (c_, m_) in enumerate(rows_):
    r = dd[(dd.comparison == c_) & (dd.metric == m_)].iloc[0]
    ax.errorbar(r["diff"], -i, xerr=[[r["diff"] - r.ci_lo], [r.ci_hi - r["diff"]]], fmt="o", ms=4, capsize=2, color={"pr_auc": "C3", "roc_auc": "C2", "f1": "C0"}[m_])
ax.axvline(0, color="k", lw=.8); ax.set_yticks([-i for i in range(len(rows_))], [f"{lab_c[c_]}, {lab_m[m_]}" for c_, m_ in rows_]); ax.set_xlabel("Різниця метрики (95 % ДІ)"); xc(ax)
fig_save("fig11_differences")
print("ok; best (CV) =", best)
