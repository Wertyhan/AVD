# -*- coding: utf-8 -*-
"""Незалежна перевірка правила аномалій (ПЗ3, завдання 6/8). Потік і офлайн викликають ОДНУ функцію score(), тому їх збіг
(pz3_stream_vs_offline.csv) перевіряє буферизацію, прогрів і порядок, а не формулу. Тут формула перевіряється окремо:
 (1) для 200 випадкових позначених і 200 випадкових непозначених хвилин потоку z_ret, z_vol, прапорець перераховано
     прямою pandas/numpy-реалізацією, написаною окремо (без імпорту anomaly_rule.score) за сирими даними parquet;
 (2) тест на «заглядання вперед»: для 40 хвилин усі наступні свічки замінено випадковими, z у момент t не змінюється.
Використання: verify_anomalies_independent.py RESULTS_DIR HISTORY_PARQUET OFFLINE_SCORED_PARQUET"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
R, HIST, OFF = sys.argv[1:4]
W, K = 10_080, 9
h = pd.read_parquet(HIST, columns=["Timestamp", "Close", "Volume"])
TS, C, V = h["Timestamp"].to_numpy(np.int64), h["Close"].to_numpy(float), h["Volume"].to_numpy(float)
st = pd.read_parquet(os.path.join(R, "stream", "scored.parquet"))
off = pd.read_parquet(OFF, columns=["ts", "z_ret", "z_vol", "anomaly", "scored"]).set_index("ts")
st = st[st["scored"]]
rng = np.random.default_rng(42)
fl, un = st[st["anomaly"]], st[~st["anomaly"]]
pick = pd.concat([fl.iloc[rng.choice(len(fl), 200, replace=False)].assign(group="позначені"),
                  un.iloc[rng.choice(len(un), 200, replace=False)].assign(group="непозначені")])


def z_pandas(t):
    """Пряма реалізація: 10 080 попередніх записів, активні (V>0); |r| -> 90-й перцентиль / 1,6449; ln V -> медіана, IQR / 1,349."""
    pos = int(np.searchsorted(TS, t)); assert TS[pos] == t
    close = pd.Series(C[pos - W - 1: pos + 1]); vol = pd.Series(V[pos - W: pos + 1])
    r = np.log(close).diff().iloc[1:].reset_index(drop=True)            # W+1 дохідностей: останній – поточна
    r_win, r_t, v_win, v_t = r.iloc[:W], r.iloc[W], vol.iloc[:W], vol.iloc[W]
    act = v_win > 0
    sigma = r_win[act].abs().quantile(0.90) / 1.6449
    lv = np.log(v_win[act])
    scale = max((lv.quantile(0.75) - lv.quantile(0.25)) / 1.349, 0.05)
    zr, zv = abs(r_t) / sigma, (np.log(v_t) - lv.median()) / scale
    return zr, zv, bool(zr > K and zv > 0), int(act.sum())


rows = []
for t, g, zr_s, zv_s, an_s in zip(pick["ts"], pick["group"], pick["z_ret"], pick["z_vol"], pick["anomaly"]):
    zr, zv, an, nact = z_pandas(int(t))
    rows.append({"ts": int(t), "datetime_utc": pd.to_datetime(int(t), unit="s", utc=True).strftime("%Y-%m-%d %H:%M"), "group": g, "n_active_in_base": nact,
                 "z_ret_stream": zr_s, "z_ret_pandas": zr, "z_vol_stream": zv_s, "z_vol_pandas": zv, "z_ret_offline": off.at[int(t), "z_ret"],
                 "flag_stream": bool(an_s), "flag_pandas": an, "flag_offline": bool(off.at[int(t), "anomaly"])})
d = pd.DataFrame(rows)
d["abs_diff_z_ret"] = (d.z_ret_stream - d.z_ret_pandas).abs(); d["abs_diff_z_vol"] = (d.z_vol_stream - d.z_vol_pandas).abs()
d.to_csv(os.path.join(R, "pz3_independent_check_samples.csv"), index=False)
summ = [{"group": g, "n": len(x), "max_abs_diff_z_ret": x.abs_diff_z_ret.max(), "max_rel_diff_z_ret": (x.abs_diff_z_ret / x.z_ret_pandas).max(),
         "max_abs_diff_z_vol": x.abs_diff_z_vol.max(), "flag_mismatches_stream_vs_pandas": int((x.flag_stream != x.flag_pandas).sum()),
         "flag_mismatches_offline_vs_pandas": int((x.flag_offline != x.flag_pandas).sum())} for g, x in d.groupby("group")]
pd.DataFrame(summ).to_csv(os.path.join(R, "pz3_independent_check.csv"), index=False)
print(pd.DataFrame(summ).T.to_string())
# --- (2) тест на заглядання вперед: score() від зрізу до t включно проти score() на зрізі з випереджальними даними, підміненими на випадкові
from anomaly_rule import score
rows = []
ts_check = list(fl["ts"].iloc[rng.choice(len(fl), 20, replace=False)]) + list(un["ts"].iloc[rng.choice(len(un), 20, replace=False)])
for t in ts_check:
    pos = int(np.searchsorted(TS, t)); lo, hi = pos - W - 1, pos + 3000
    base = pd.DataFrame({"ts": TS[lo:hi], "close": C[lo:hi], "volume": V[lo:hi]})
    pert = base.copy(); n_after = len(pert) - (W + 2)               # рядки після t
    pert.loc[W + 2:, "close"] = pert["close"].iloc[W + 1] * np.exp(np.cumsum(rng.normal(0, 0.05, n_after)))
    pert.loc[W + 2:, "volume"] = rng.exponential(50, n_after)
    a = score(base.iloc[:W + 2]).iloc[-1]; b = score(pert).iloc[W + 1]; c = score(base).iloc[W + 1]
    rows.append({"ts": int(t), "z_ret_truncated": a.z_ret, "z_ret_future_replaced": b.z_ret, "z_ret_full": c.z_ret,
                 "z_vol_truncated": a.z_vol, "z_vol_future_replaced": b.z_vol, "equal": bool(a.z_ret == b.z_ret == c.z_ret and a.z_vol == b.z_vol)})
p = pd.DataFrame(rows); p.to_csv(os.path.join(R, "pz3_lookahead_perturbation.csv"), index=False)
print("заглядання вперед: збігів", int(p.equal.sum()), "з", len(p))
