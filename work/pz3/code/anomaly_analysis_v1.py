# -*- coding: utf-8 -*-
"""Офлайн-калібрування правила аномалій на всій історії (ПЗ3, завдання 6, 8) і порівняння з потоковою реалізацією.
Фаза 1 (без --scored): оцінка всієї історії, таблиці кількості спрацювань, вибір k, k_v, топ-дати.
Фаза 2 (--scored scored.parquet): порівняння потік/офлайн, підсумкові файли для ПЗ4."""
import argparse, json, os, sys, time
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from anomaly_rule import score, K_GRID, KV_GRID, K_SEL, KV_SEL, WINDOW, MIN_OBS

ap = argparse.ArgumentParser()
ap.add_argument("--parquet", required=True)
ap.add_argument("--out", required=True)
ap.add_argument("--cache", required=True, help="каталог для проміжного файлу офлайн-оцінок")
ap.add_argument("--scored", default="")
ap.add_argument("--budget", type=float, default=0.0015, help="допустима частка хвилин (ліквідна ера 2018+) на одне правило, ~2 тривоги на добу")
a = ap.parse_args()
os.makedirs(a.out, exist_ok=True); os.makedirs(a.cache, exist_ok=True)
cache_f = os.path.join(a.cache, "offline_scored.parquet")
EVENTS = {"2020-03-12": "обвал COVID-19", "2020-03-13": "обвал COVID-19 (відскок)", "2021-05-19": "обвал 19.05.2021",
          "2022-11-08": "FTX (08.11)", "2022-11-09": "FTX (09.11)"}
PERIODS = {"2020-03": ("2020-03-09", "2020-03-16"), "2021-05": ("2021-05-17", "2021-05-22"), "2022-11": ("2022-11-06", "2022-11-12")}

# --- 1. повна історія
if os.path.exists(cache_f):
    sc = pd.read_parquet(cache_f)
else:
    t0 = time.time()
    df = pd.read_parquet(a.parquet, columns=["Timestamp", "Close", "Volume"]).rename(
        columns={"Timestamp": "ts", "Close": "close", "Volume": "volume"})
    n_all = len(df)
    df = df.dropna(); df = df[(df["close"] > 0) & (df["volume"] >= 0)]
    sc = score(df)
    sc["date"] = pd.to_datetime(sc["ts"], unit="s", utc=True).dt.strftime("%Y-%m-%d")
    sc.to_parquet(cache_f, index=False)
    pd.DataFrame([{"stage": "offline_score_full_history", "rows": len(sc), "seconds": round(time.time() - t0, 1)}]).to_csv(
        os.path.join(a.out, "offline_timing.csv"), index=False)
    print(f"[analysis] score() на {len(sc)} рядках: {time.time()-t0:.0f} с", flush=True)
v = sc[sc["scored"]]
nv = len(v)
ev_mask = v["date"].isin(EVENTS.keys())
mod = v["date"] >= "2018-01-01"   # ліквідна ера: усі відтворювані періоди належать їй
vm = v[mod]
print(f"[analysis] оцінено хвилин {nv} з {len(sc)}; пласких (V=0) {int((sc.volume==0).sum())}", flush=True)

if not a.scored:
    # --- 2. кількість спрацювань за k (правило за дохідністю) і k_v (правило за обсягом), та сітка k = k_v
    rows = []
    for rule, col, grid in (("return", "z_ret", K_GRID), ("volume", "vol_ratio", KV_GRID)):
        for k in grid:
            f, fm = (v[col] > k), (vm[col] > k)
            n = int(f.sum())
            rows.append({"rule": rule, "k": k, "n_full": n, "share_full_pct": round(100 * n / nv, 4),
                         "n_2018plus": int(fm.sum()), "share_2018plus_pct": round(100 * fm.mean(), 4),
                         "per_day_2018plus": round(fm.sum() / (len(vm) / 1440), 2),
                         "n_in_event_days": int(f[ev_mask].sum()), "share_event_days_pct": round(100 * f[ev_mask].mean(), 3),
                         "lift_event_vs_2018plus": round(f[ev_mask].mean() / max(fm.mean(), 1e-12), 1)})
    sel = pd.DataFrame(rows)
    sel.to_csv(os.path.join(a.out, "pz3_threshold_selection.csv"), index=False)
    rk = {}
    for rule, col in (("return", "k"), ("volume", "k")):
        s_ = sel[(sel.rule == rule) & (sel.share_2018plus_pct <= 100 * a.budget)]
        rk[rule] = float(s_["k"].min())
    print(sel.to_string(index=False), flush=True)
    print("[analysis] обрано за бюджетом", a.budget, rk, "| у коді K_SEL, KV_SEL =", K_SEL, KV_SEL, flush=True)
    assert (rk["return"], rk["volume"]) == (K_SEL, KV_SEL), "оновіть K_SEL/KV_SEL в anomaly_rule.py"
    # вимога завдання: k in {3,5,7,10}, k_v = k (об'єднання)
    rows = []
    for k in (3, 5, 7, 10):
        n_ret, n_vol = int((v["z_ret"] > k).sum()), int((v["vol_ratio"] > k).sum())
        n_u = int(((v["z_ret"] > k) | (v["vol_ratio"] > k)).sum())
        rows.append({"k": k, "k_v": k, "n_valid_minutes": nv, "n_return_rule": n_ret, "n_volume_rule": n_vol, "n_union": n_u,
                     "share_union_pct": round(100 * n_u / nv, 3), "per_day_avg": round(n_u / (nv / 1440), 2)})
    pd.DataFrame(rows).to_csv(os.path.join(a.out, "pz3_trigger_counts_offline.csv"), index=False)
    print(pd.DataFrame(rows).to_string(index=False), flush=True)
    # відсоток пласких і його вплив: сценарій без виключення пласких хвилин (для порівняння)
    # --- 3. обрані пороги: топ-дати, роки, відомі події
    fl = v[v["anomaly"]].copy()
    fl["datetime_utc"] = pd.to_datetime(fl["ts"], unit="s", utc=True).dt.strftime("%Y-%m-%d %H:%M:%S")
    by_date = (fl.groupby("date").agg(n_anomalies=("ts", "size"), max_z_ret=("z_ret", "max"), max_vol_ratio=("vol_ratio", "max"),
                                      max_abs_r=("r", lambda s: s.abs().max()), max_score=("score", "max"))
               .sort_values("n_anomalies", ascending=False))
    by_date.head(40).to_csv(os.path.join(a.out, "pz3_top_dates.csv"))
    print(by_date.head(15).to_string(), flush=True)
    fl.sort_values("score", ascending=False).head(40)[["datetime_utc", "close", "volume", "r", "z_ret", "vol_ratio", "score", "rule"]].to_csv(
        os.path.join(a.out, "pz3_top_minutes.csv"), index=False)
    fl["year"] = fl["date"].str[:4]
    by_year = fl.groupby("year").size().rename("n_anomalies").to_frame()
    by_year["scored_minutes"] = v.assign(year=v["date"].str[:4]).groupby("year").size()
    by_year["share_pct"] = (100 * by_year["n_anomalies"] / by_year["scored_minutes"]).round(3)
    by_year.to_csv(os.path.join(a.out, "pz3_anomalies_by_year.csv"))
    rank = by_date["n_anomalies"].rank(ascending=False, method="min")
    chk = []
    for d, nm in EVENTS.items():
        chk.append({"date": d, "event": nm, "n_anomalies": int(by_date.loc[d, "n_anomalies"]) if d in by_date.index else 0,
                    "rank_by_count": int(rank.loc[d]) if d in by_date.index else None, "n_dates_with_anomalies": len(by_date),
                    "max_z_ret": float(by_date.loc[d, "max_z_ret"]) if d in by_date.index else None})
    pd.DataFrame(chk).to_csv(os.path.join(a.out, "pz3_known_events_check.csv"), index=False)
    print(pd.DataFrame(chk).to_string(index=False), flush=True)
    # ефект виключення пласких хвилин: частка пласких серед усієї історії та серед вікон
    flat_info = {"flat_minutes": int((sc["volume"] == 0).sum()), "flat_share_pct": round(100 * (sc["volume"] == 0).mean(), 2),
                 "unscored_minutes": int((~sc["scored"]).sum()), "min_obs": MIN_OBS, "window": WINDOW}
    pd.DataFrame([flat_info]).to_csv(os.path.join(a.out, "pz3_flat_minutes_handling.csv"), index=False)
    print(flat_info, flush=True)
    # топ-дати у ліквідну еру (2018+) та показники трьох відтворюваних періодів
    f2 = fl[fl["date"] >= "2018-01-01"]
    t2 = f2.groupby("date").agg(n_anomalies=("ts", "size"), max_z_ret=("z_ret", "max"), max_vol_ratio=("vol_ratio", "max"),
                                max_abs_r=("r", lambda s: s.abs().max())).sort_values("n_anomalies", ascending=False)
    t2.head(30).to_csv(os.path.join(a.out, "pz3_top_dates_2018plus.csv"))
    print(t2.head(12).to_string(), flush=True)
    base_per_day = len(f2) / (len(vm) / 1440)
    prow = []
    for p, (d0, d1) in PERIODS.items():
        mk = (v["date"] >= d0) & (v["date"] <= d1)
        n_p = int(v.loc[mk, "anomaly"].sum())
        days = mk.sum() / 1440
        prow.append({"period": p, "from": d0, "to": d1, "scored_minutes": int(mk.sum()), "n_anomalies": n_p,
                     "per_day": round(n_p / days, 2), "baseline_per_day_2018plus": round(base_per_day, 2),
                     "ratio_to_baseline": round(n_p / days / base_per_day, 2),
                     "max_z_ret": float(v.loc[mk, "z_ret"].max()), "max_vol_ratio": float(v.loc[mk, "vol_ratio"].max())})
    pd.DataFrame(prow).to_csv(os.path.join(a.out, "pz3_periods_vs_baseline.csv"), index=False)
    print(pd.DataFrame(prow).to_string(index=False), flush=True)
    # закономірності: день тижня та година UTC (2018+), частка позначених від оцінених хвилин
    vv = vm.assign(dt=pd.to_datetime(vm["ts"], unit="s", utc=True)); vv["dow"] = vv["dt"].dt.dayofweek; vv["hour"] = vv["dt"].dt.hour
    for col, nm in (("dow", "weekday"), ("hour", "hour")):
        g = vv.groupby(col).agg(scored_minutes=("ts", "size"), n_anomalies=("anomaly", "sum"))
        g["share_pct"] = (100 * g["n_anomalies"] / g["scored_minutes"]).round(4)
        g.to_csv(os.path.join(a.out, f"pz3_anomalies_by_{nm}_2018plus.csv"))
        print(g.to_string(), flush=True)
    # результат для ПЗ4: повна історія, обрані пороги
    out = fl.rename(columns={"r": "log_return"})
    out["timestamp"] = out["datetime_utc"]
    out["k"], out["k_v"] = K_SEL, KV_SEL
    cols = ["timestamp", "ts", "close", "log_return", "volume", "z_ret", "vol_ratio", "score", "rule", "k", "k_v", "date"]
    out[cols].to_csv(os.path.join(a.out, "pz3_anomalies_offline_full.csv"), index=False)
    print("[analysis] FULL anomalies:", len(out), flush=True)
    print("ANALYSIS_PHASE1_DONE", flush=True)
    sys.exit(0)

# ---------------- фаза 2: порівняння потоку та офлайн ----------------
st = pd.read_parquet(a.scored)
m = st.merge(sc, on="ts", suffixes=("_stream", "_off"))
both = m[m["scored_stream"] & m["scored_off"]]
cmp_ = {"n_stream_rows": len(st), "n_stream_scored": int(st["scored"].sum()), "n_warmup_unscored": int((~st["scored"]).sum()),
        "n_compared": len(both),
        "max_abs_diff_z": float(np.nanmax(np.abs(both["z_ret_stream"] - both["z_ret_off"]))),
        "max_abs_diff_q": float(np.nanmax(np.abs(both["vol_ratio_stream"] - both["vol_ratio_off"]))),
        "n_anomalies_stream": int(both["anomaly_stream"].sum()), "n_anomalies_offline": int(both["anomaly_off"].sum()),
        "flag_mismatches": int((both["anomaly_stream"] != both["anomaly_off"]).sum()), "k": K_SEL, "k_v": KV_SEL}
pd.DataFrame([cmp_]).to_csv(os.path.join(a.out, "pz3_stream_vs_offline.csv"), index=False)
print("[analysis] потік проти офлайн:", cmp_, flush=True)
s2 = st[st["scored"]].copy()
s2["datetime_utc"] = pd.to_datetime(s2["ts"], unit="s", utc=True).dt.strftime("%Y-%m-%d %H:%M:%S")
s2["date"] = s2["datetime_utc"].str[:10]
s2["period"] = s2["date"].str[:7]
# кількість спрацювань у потоці за сіткою k (k=k_v), як вимагає завдання
rows = []
for k, kv in ((3, 3), (5, 5), (7, 7), (10, 10), (K_SEL, KV_SEL)):
    ur, uv = (s2["z_ret"] > k), (s2["vol_ratio"] > kv)
    row = {"k": k, "k_v": kv, "n_scored": len(s2), "n_return_rule": int(ur.sum()), "n_volume_rule": int(uv.sum()), "n_union": int((ur | uv).sum()),
           "share_union_pct": round(100 * (ur | uv).mean(), 3)}
    for p in PERIODS:
        mk = s2["period"] == p
        row["n_union_" + p] = int((ur | uv)[mk].sum())
    rows.append(row)
pd.DataFrame(rows).to_csv(os.path.join(a.out, "pz3_trigger_counts_stream.csv"), index=False)
print(pd.DataFrame(rows).to_string(index=False), flush=True)
an = s2[s2["anomaly"]].copy()
an_by_day = an.groupby("date").agg(n_anomalies=("ts", "size"), max_z_ret=("z_ret", "max"), max_vol_ratio=("vol_ratio", "max"),
                                   max_score=("score", "max")).sort_values("n_anomalies", ascending=False)
an_by_day.to_csv(os.path.join(a.out, "pz3_stream_anomalies_by_date.csv"))
print(an_by_day.head(12).to_string(), flush=True)
out = an.rename(columns={"r": "log_return"})
out["timestamp"] = out["datetime_utc"]; out["k"] = K_SEL; out["k_v"] = KV_SEL
cols = ["timestamp", "ts", "close", "log_return", "volume", "z_ret", "vol_ratio", "score", "rule", "k", "k_v", "date", "period"]
out[cols].to_csv(os.path.join(a.out, "pz3_anomalies_stream.csv"), index=False)
out[cols].to_parquet(os.path.join(a.out, "pz3_anomalies_stream.parquet"), index=False)
# файл для ПЗ4: повна історія (офлайн), з позначкою про потрапляння у відтворений потік
full = pd.read_csv(os.path.join(a.out, "pz3_anomalies_offline_full.csv"))
full["in_stream_replay"] = False
for d0, d1 in PERIODS.values():
    full["in_stream_replay"] |= full["date"].between(d0, d1)
full.to_csv(os.path.join(a.out, "pz3_anomalies.csv"), index=False)
full.to_parquet(os.path.join(a.out, "pz3_anomalies.parquet"), index=False)
print(f"[analysis] pz3_anomalies.csv: {len(full)} рядків; у відтворених періодах {int(full.in_stream_replay.sum())}", flush=True)
print("ANALYSIS_DONE", flush=True)
