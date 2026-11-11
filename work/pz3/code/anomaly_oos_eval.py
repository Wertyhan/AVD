# -*- coding: utf-8 -*-
"""Out-of-sample оцінка вибору правила аномалій (ПЗ3, завдання 6). Вхід: results/pz3_design_daily_counts.parquet (anomaly_design_eval.py).
(a) leave-one-event-out: для кожного з 5 днів подій варіант обирається за lift на решті 4 днів (критерій ANOMALY_DESIGN.md: однаковий бюджет
    0,10 % активних хвилин 2018+, максимум lift), після чого оцінюється lift і ранг пропущеного дня; те саме для 3 подій-кластерів
    (COVID 12–13.03.2020, 19.05.2021, FTX 08–09.11.2022), бо сусідні дні однієї події корельовані.
(b) часовий поділ: варіант і k обираються на 2013–2019 (події = дні з |денна лог-дохідність| >= TAU; k – бюджет 0,10 % активних хвилин 2013–2019),
    оцінка на 2020+ (5 відомих днів та всі дні 2020+ з |ret| >= TAU) з незмінним k.
Виходи: pz3_oos_loo.csv, pz3_oos_timesplit.csv, pz3_oos_timesplit_events.csv, pz3_oos_summary.csv. Використання: anomaly_oos_eval.py RESULTS HISTORY_PARQUET"""
import os, sys
import numpy as np, pandas as pd
R, HIST = sys.argv[1], sys.argv[2]
TAU = 0.10
EV = ["2020-03-12", "2020-03-13", "2021-05-19", "2022-11-08", "2022-11-09"]
CL = {"COVID 12–13.03.2020": ["2020-03-12", "2020-03-13"], "19.05.2021": ["2021-05-19"], "FTX 08–09.11.2022": ["2022-11-08", "2022-11-09"]}
d = pd.read_parquet(os.path.join(R, "pz3_design_daily_counts.parquet"))
variants = list(dict.fromkeys(d["variant"]))
PROD = "W10080_zr_and_zv>0"       # варіант, реалізований у v2 (z_ret > k і z_vol > 0)
h = pd.read_parquet(HIST, columns=["Timestamp", "Close"])
dt = pd.to_datetime(h["Timestamp"], unit="s", utc=True).dt.strftime("%Y-%m-%d")
dret = np.log(h["Close"].groupby(dt.to_numpy()).last()).diff()
T = {v: g.set_index("date").sort_index() for v, g in d.groupby("variant")}


def lift(v, days, base_days, col="n_flag"):
    g = T[v]
    e = g.loc[g.index.intersection(days)]; b = g.loc[g.index.intersection(base_days)]
    if e["n_ok"].sum() == 0 or b[col].sum() == 0:
        return np.nan
    return (e[col].sum() / e["n_ok"].sum()) / (b[col].sum() / b["n_ok"].sum())


def rank(v, day, col="n_flag"):
    c = T[v][col]
    return int((c > c.get(day, 0)).sum() + 1)


dates = sorted(set(d["date"]))
base_all = [x for x in dates if x >= "2018-01-01" and x not in EV]
rows = []
for unit, groups in (("день", {e: [e] for e in EV}), ("подія", CL)):
    for name, held in groups.items():
        train = [e for e in EV if e not in held]
        sc = {v: lift(v, train, base_all) for v in variants}
        best = max(sc, key=lambda v: (sc[v] if np.isfinite(sc[v]) else -1))
        full = {v: lift(v, EV, base_all) for v in variants}
        held_l = {v: lift(v, held, base_all) for v in variants}
        orc = max(held_l, key=lambda v: held_l[v] if np.isfinite(held_l[v]) else -1)
        rows.append({"unit": unit, "held_out": name, "chosen_variant": best, "train_lift_of_chosen": round(sc[best], 1),
                     "heldout_lift_chosen": round(held_l[best], 1), "heldout_flags_chosen": int(sum(T[best].at[x, "n_flag"] for x in held)),
                     "heldout_rank_chosen": ", ".join(str(rank(best, x)) for x in held), "n_dates": len(T[best]),
                     "heldout_lift_production_variant": round(held_l[PROD], 1), "heldout_rank_production": ", ".join(str(rank(PROD, x)) for x in held),
                     "heldout_lift_best_in_hindsight": round(held_l[orc], 1), "best_in_hindsight_variant": orc,
                     "heldout_lift_rank_of_chosen_among_variants": int(sum(1 for v in variants if held_l[v] > held_l[best]) + 1),
                     "insample_lift_chosen_all_5_days": round(full[best], 1)})
loo = pd.DataFrame(rows); loo.to_csv(os.path.join(R, "pz3_oos_loo.csv"), index=False)
print(loo.drop(columns=["n_dates"]).T.to_string())

# --- (b) часовий поділ
big = dret[dret.abs() >= TAU]
tr_ev = [x for x in big.index if "2013-01-01" <= x <= "2019-12-31"]
te_ev_big = [x for x in big.index if x >= "2020-01-01"]
pd.DataFrame({"date": list(big.index), "day_logret": big.values.round(4), "set": ["train 2013–2019" if "2013-01-01" <= x <= "2019-12-31" else ("test 2020+" if x >= "2020-01-01" else "поза діапазоном") for x in big.index],
              "known_event": [x in EV for x in big.index]}).to_csv(os.path.join(R, "pz3_oos_timesplit_events.csv"), index=False)
base_tr = [x for x in dates if "2013-01-01" <= x <= "2019-12-31" and x not in tr_ev]
base_te = [x for x in dates if x >= "2020-01-01" and x not in EV and x not in te_ev_big]
rows = []
for v in variants:
    g = T[v]
    te_base_rate = g.loc[g.index.intersection(base_te)]; te_base_rate = te_base_rate["n_flag_train"].sum() / te_base_rate["n_ok"].sum()
    rows.append({"variant": v, "k_train": round(float(g["k_train"].iloc[0]), 2), "train_lift": lift(v, tr_ev, base_tr, "n_flag_train"),
                 "test_lift_5_known_days": lift(v, EV, base_te, "n_flag_train"), "test_lift_all_big_move_days": lift(v, te_ev_big, base_te, "n_flag_train"),
                 "test_base_rate_pct": 100 * te_base_rate, "test_rank_known_days": ", ".join(str(rank(v, x, "n_flag_train")) for x in EV)})
ts = pd.DataFrame(rows).round(3).sort_values("train_lift", ascending=False)
ts["chosen_on_train"] = ts["variant"] == ts["variant"].iloc[0]
ts.to_csv(os.path.join(R, "pz3_oos_timesplit.csv"), index=False)
print(ts.to_string(index=False))
ch = ts.iloc[0]; pr = ts[ts.variant == PROD].iloc[0]
pd.DataFrame([{"tau_daily_logret": TAU, "n_train_events": len(tr_ev), "n_test_big_move_days": len(te_ev_big), "chosen_on_train": ch.variant,
               "chosen_k_train": ch.k_train, "chosen_train_lift": ch.train_lift, "chosen_test_lift_5_known": ch.test_lift_5_known_days,
               "chosen_test_lift_big_days": ch.test_lift_all_big_move_days, "chosen_test_base_rate_pct": ch.test_base_rate_pct,
               "production_variant_train_lift": pr.train_lift, "production_test_lift_5_known": pr.test_lift_5_known_days,
               "production_test_base_rate_pct": pr.test_base_rate_pct,
               "loo_days_mean_heldout_lift": loo[loo.unit == "день"].heldout_lift_chosen.mean(), "loo_days_same_variant_all_folds": loo[loo.unit == "день"].chosen_variant.nunique() == 1,
               "loo_events_mean_heldout_lift": loo[loo.unit == "подія"].heldout_lift_chosen.mean()}]).round(3).to_csv(os.path.join(R, "pz3_oos_summary.csv"), index=False)
print(pd.read_csv(os.path.join(R, "pz3_oos_summary.csv")).T.to_string())
