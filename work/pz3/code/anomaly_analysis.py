# -*- coding: utf-8 -*-
"""Офлайн-калібрування правила аномалій v2 на всій історії (ПЗ3, завдання 6, 8) і порівняння з потоком.
Фаза 1 (без --scored): оцінка всієї історії, сітка k, вибір k, lift днів подій, топ-20 дат, ери, вікна 15 хв / 1 год.
Фаза 2 (--scored scored.parquet): порівняння потік/офлайн, підсумкові файли для ПЗ4."""
import argparse, os, sys, time
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from anomaly_rule import score, K_GRID, K_SEL, ZV_MIN, WINDOW, MIN_ACTIVE, P_SCALE

ap = argparse.ArgumentParser()
ap.add_argument("--parquet", required=True)
ap.add_argument("--out", required=True)
ap.add_argument("--cache", required=True, help="каталог для проміжного файлу офлайн-оцінок")
ap.add_argument("--scored", default="")
ap.add_argument("--target", type=float, default=0.0010, help="цільова частка тривог: 0,10 % активних хвилин 2018+ (~1,4 на добу)")
a = ap.parse_args()
os.makedirs(a.out, exist_ok=True); os.makedirs(a.cache, exist_ok=True)
cache_f = os.path.join(a.cache, "offline_scored_v2.parquet")
EVENTS = {"2020-03-12": "обвал COVID-19", "2020-03-13": "обвал COVID-19 (продовження)", "2021-05-19": "обвал 19.05.2021",
          "2022-11-08": "FTX (08.11)", "2022-11-09": "FTX (09.11)"}
PERIODS = {"2020-03": ("2020-03-09", "2020-03-16"), "2021-05": ("2021-05-17", "2021-05-22"), "2022-11": ("2022-11-06", "2022-11-12")}
W_OUT = lambda name: os.path.join(a.out, name)

# --- 1. повна історія
if os.path.exists(cache_f):
    sc = pd.read_parquet(cache_f)
else:
    t0 = time.time()
    df = pd.read_parquet(a.parquet, columns=["Timestamp", "Close", "Volume"]).rename(
        columns={"Timestamp": "ts", "Close": "close", "Volume": "volume"})
    sc = score(df)
    sc["date"] = pd.to_datetime(sc["ts"], unit="s", utc=True).dt.strftime("%Y-%m-%d")
    sc.to_parquet(cache_f, index=False)
    pd.DataFrame([{"stage": "offline_score_full_history_v2", "rows": len(sc), "seconds": round(time.time() - t0, 1)}]).to_csv(
        W_OUT("offline_timing.csv"), index=False)
    print(f"[analysis] score() на {len(sc)} рядках: {time.time()-t0:.0f} с", flush=True)
sc["year"] = sc["date"].str[:4].astype(int)
v = sc[sc["scored"]].copy()
nv = len(v)
ev_mask = v["date"].isin(EVENTS.keys())
m18 = v["year"] >= 2018
base = m18 & ~ev_mask                      # базові дні: 2018+ без днів подій
old = v["year"] <= 2016
print(f"[analysis] оцінено хвилин {nv} з {len(sc)}; пласких (V=0) {int((sc.volume==0).sum())}", flush=True)


def win_flags(frame, flag, freq_s):
    """Вікно (tumbling, вирівняне по UTC) позначене, якщо в ньому є хоча б одна позначена хвилина."""
    wid = frame["ts"].to_numpy() // freq_s
    g = pd.DataFrame({"wid": wid, "f": flag.to_numpy(), "date": frame["date"].to_numpy(), "year": frame["year"].to_numpy()})
    return g.groupby("wid").agg(f=("f", "max"), date=("date", "first"), year=("year", "first"))


if not a.scored:
    # --- 2. сітка k: кількість спрацювань, частки за ерами, lift днів подій; вікна 1 год
    rows = []
    for k in K_GRID:
        f_r = v["z_ret"] > k
        f = f_r & (v["z_vol"] > ZV_MIN)
        w1h = win_flags(v, f, 3600)
        wb = w1h[(w1h.year >= 2018) & ~w1h.date.isin(EVENTS.keys())]
        we = w1h[w1h.date.isin(EVENTS.keys())]
        rate18, rate_ev = f[base].mean(), f[ev_mask].mean()
        rows.append({"k": k, "n_return_only": int(f_r.sum()), "n_flags": int(f.sum()), "share_all_pct": round(100 * f.mean(), 4),
                     "n_2018plus": int(f[m18].sum()), "share_2018plus_pct": round(100 * f[m18].mean(), 4),
                     "per_day_2018plus": round(f[m18].sum() / (m18.sum() / 1440), 2),
                     "share_2012_2016_pct": round(100 * f[old].mean(), 4), "share_2017_pct": round(100 * f[v.year == 2017].mean(), 4),
                     "n_event_days": int(f[ev_mask].sum()), "share_event_days_pct": round(100 * rate_ev, 3),
                     "lift_event_vs_base": round(rate_ev / max(rate18, 1e-12), 1),
                     "share_1h_windows_2018plus_pct": round(100 * w1h[w1h.year >= 2018].f.mean(), 3),
                     "lift_1h_windows": round(we.f.mean() / max(wb.f.mean(), 1e-12), 1)})
    sel = pd.DataFrame(rows)
    ok_k = sel[sel.share_2018plus_pct <= 100 * a.target]
    k_chosen = int(ok_k["k"].min())
    sel["chosen"] = sel["k"] == k_chosen
    sel.to_csv(W_OUT("pz3_threshold_selection.csv"), index=False)
    print(sel.to_string(index=False), flush=True)
    print(f"[analysis] критерій: найменше k з часткою <= {100*a.target:.2f} % активних хвилин 2018+ -> k = {k_chosen}; у коді K_SEL = {K_SEL}", flush=True)
    assert k_chosen == K_SEL, "оновіть K_SEL в anomaly_rule.py"
    # вимога завдання: k in {3,5,7,10}
    tc = sel[sel.k.isin([3, 5, 7, 10, K_SEL])][["k", "n_return_only", "n_flags", "share_all_pct", "share_2018plus_pct", "per_day_2018plus"]].copy()
    tc.insert(1, "n_valid_minutes", nv)
    tc.to_csv(W_OUT("pz3_trigger_counts_offline.csv"), index=False)

    # --- 3. обраний поріг
    v["anomaly"] = (v["z_ret"] > K_SEL) & (v["z_vol"] > ZV_MIN)
    fl = v[v["anomaly"]].copy()
    fl["datetime_utc"] = pd.to_datetime(fl["ts"], unit="s", utc=True).dt.strftime("%Y-%m-%d %H:%M:%S")
    # денна зміна ціни для інтерпретації топ-дат (лог-дохідність доби за close)
    dclose = sc.groupby("date")["close"].last()
    dret = np.log(dclose / dclose.shift(1))
    by_date = (fl.groupby("date").agg(n_anomalies=("ts", "size"), max_z_ret=("z_ret", "max"), max_z_vol=("z_vol", "max"),
                                      max_abs_r=("r", lambda s: s.abs().max()), first_utc=("datetime_utc", "min"), last_utc=("datetime_utc", "max"))
               .sort_values(["n_anomalies", "max_z_ret"], ascending=False))
    by_date["day_logret"] = dret.reindex(by_date.index).round(4)
    by_date["rank"] = np.arange(1, len(by_date) + 1)
    by_date.head(20).to_csv(W_OUT("pz3_top_dates.csv"))
    print(by_date.head(20).to_string(), flush=True)
    t2 = by_date[by_date.index >= "2018-01-01"].copy(); t2["rank_2018plus"] = np.arange(1, len(t2) + 1)
    t2.head(20).to_csv(W_OUT("pz3_top_dates_2018plus.csv"))
    fl.sort_values("z_ret", ascending=False).head(40)[["datetime_utc", "close", "volume", "r", "sigma_ret", "z_ret", "z_vol", "vol_ratio", "score"]].to_csv(
        W_OUT("pz3_top_minutes.csv"), index=False)
    # ери та роки
    yr = v.groupby("year").agg(scored_minutes=("ts", "size"), n_anomalies=("anomaly", "sum"))
    yr["share_pct"] = (100 * yr.n_anomalies / yr.scored_minutes).round(4)
    yr["active_share_pct"] = (100 * sc.groupby("year")["volume"].apply(lambda s: (s > 0).mean())).round(1)
    yr.to_csv(W_OUT("pz3_anomalies_by_year.csv"))
    era = []
    top20_years = by_date.head(20).index.str[:4].astype(int)
    for nm, lo, hi in (("2012–2016", 2012, 2016), ("2017", 2017, 2017), ("2018+", 2018, 2100), ("2017+", 2017, 2100), ("усе", 2012, 2100)):
        mk = v.year.between(lo, hi)
        era.append({"era": nm, "scored_minutes": int(mk.sum()), "n_anomalies": int(v.anomaly[mk].sum()),
                    "share_pct": round(100 * v.anomaly[mk].mean(), 4), "per_day": round(v.anomaly[mk].sum() / (mk.sum() / 1440), 2),
                    "n_dates_with_anomalies": int(fl[fl.year.between(lo, hi)]["date"].nunique()),
                    "dates_in_top20": int(((top20_years >= lo) & (top20_years <= hi)).sum())})
    pd.DataFrame(era).to_csv(W_OUT("pz3_anomalies_by_era.csv"), index=False)
    print(pd.DataFrame(era).to_string(index=False), flush=True)
    # відомі події: кількість, ранги, вікна
    rank_all = by_date["rank"]; rank18 = t2["rank_2018plus"]
    w1 = win_flags(v, v["anomaly"], 3600); w15 = win_flags(v, v["anomaly"], 900)
    chk = []
    for d, nm in EVENTS.items():
        dm = v["date"] == d
        chk.append({"date": d, "event": nm, "scored_minutes": int(dm.sum()), "n_anomalies": int(v.anomaly[dm].sum()),
                    "rank_all_dates": int(rank_all.get(d, 0)) or None, "rank_2018plus": int(rank18.get(d, 0)) or None,
                    "n_dates_with_anomalies": len(by_date), "n_dates_2018plus": len(t2),
                    "flagged_1h_windows": int(w1[w1.date == d].f.sum()), "flagged_15m_windows": int(w15[w15.date == d].f.sum()),
                    "max_z_ret": round(float(v.z_ret[dm].max()), 2), "sigma_ret_median_day": float(v.sigma_ret[dm].median()),
                    "day_logret": round(float(dret.get(d)), 4),
                    "first_flag_utc": by_date.loc[d, "first_utc"] if d in by_date.index else "", "last_flag_utc": by_date.loc[d, "last_utc"] if d in by_date.index else ""})
    pd.DataFrame(chk).to_csv(W_OUT("pz3_known_events_check.csv"), index=False)
    print(pd.DataFrame(chk).to_string(index=False), flush=True)
    # lift: хвилини, вікна 15 хв, 1 год; дні подій та відтворені періоди проти базових днів 2018+
    lift = []
    for lvl, frame in (("minute", None), ("15m_window", w15), ("1h_window", w1)):
        if frame is None:
            fb, unit = v.anomaly[base], 1
            sel_ev = lambda mk: v.anomaly[mk(v)]
        else:
            fb = frame.f[(frame.year >= 2018) & ~frame.date.isin(EVENTS.keys())]
            sel_ev = lambda mk, fr=frame: fr.f[mk(fr)]
        rb = fb.mean()
        groups = [("усі 5 днів подій", lambda x: x["date"].isin(EVENTS.keys()))] + [(d, lambda x, d=d: x["date"] == d) for d in EVENTS] + \
                 [(f"період {p} ({d0}..{d1})", lambda x, d0=d0, d1=d1: x["date"].between(d0, d1)) for p, (d0, d1) in PERIODS.items()]
        for gname, mk in groups:
            fe = sel_ev(mk)
            lift.append({"level": lvl, "group": gname, "units": len(fe), "flagged": int(fe.sum()), "rate_pct": round(100 * fe.mean(), 3),
                         "base_rate_2018plus_pct": round(100 * rb, 4), "lift": round(fe.mean() / rb, 1)})
    lift = pd.DataFrame(lift); lift.to_csv(W_OUT("pz3_lift.csv"), index=False)
    print(lift.to_string(index=False), flush=True)
    # періоди відтворення
    base_per_day = v.anomaly[base].sum() / (base.sum() / 1440)
    prow = []
    for p, (d0, d1) in PERIODS.items():
        mk = v["date"].between(d0, d1)
        n_p = int(v.loc[mk, "anomaly"].sum()); days = mk.sum() / 1440
        prow.append({"period": p, "from": d0, "to": d1, "scored_minutes": int(mk.sum()), "n_anomalies": n_p, "per_day": round(n_p / days, 2),
                     "baseline_per_day_2018plus": round(base_per_day, 2), "ratio_to_baseline": round(n_p / days / base_per_day, 1),
                     "max_z_ret": round(float(v.loc[mk, "z_ret"].max()), 2)})
    pd.DataFrame(prow).to_csv(W_OUT("pz3_periods_vs_baseline.csv"), index=False)
    print(pd.DataFrame(prow).to_string(index=False), flush=True)
    # пласкі хвилини та мінімальна активність
    flat_info = {"total_minutes": len(sc), "flat_minutes": int((sc["volume"] == 0).sum()), "flat_share_pct": round(100 * (sc["volume"] == 0).mean(), 2),
                 "scored_minutes": nv, "unscored_active_minutes": int(((sc.volume > 0) & ~sc.scored).sum()),
                 "unscored_active_2012": int(((sc.volume > 0) & ~sc.scored & (sc.year == 2012)).sum()),
                 "window": WINDOW, "min_active": MIN_ACTIVE, "p_scale": P_SCALE}
    pd.DataFrame([flat_info]).to_csv(W_OUT("pz3_flat_minutes_handling.csv"), index=False)
    print(flat_info, flush=True)
    # закономірності 2018+: день тижня, година UTC
    vv = v[m18].assign(dt=lambda x: pd.to_datetime(x["ts"], unit="s", utc=True))
    vv["dow"] = vv["dt"].dt.dayofweek; vv["hour"] = vv["dt"].dt.hour
    for col, nm in (("dow", "weekday"), ("hour", "hour")):
        g = vv.groupby(col).agg(scored_minutes=("ts", "size"), n_anomalies=("anomaly", "sum"))
        g["share_pct"] = (100 * g["n_anomalies"] / g["scored_minutes"]).round(4)
        g.to_csv(W_OUT(f"pz3_anomalies_by_{nm}_2018plus.csv"))
    # результат для ПЗ4: повна історія, обраний поріг
    out = fl.rename(columns={"r": "log_return"})
    out["timestamp"] = out["datetime_utc"]; out["k"] = K_SEL; out["k_v"] = ZV_MIN
    cols = ["timestamp", "ts", "close", "log_return", "volume", "z_ret", "vol_ratio", "score", "rule", "k", "k_v", "date",
            "z_vol", "sigma_ret", "med_vol"]
    out[cols].to_csv(W_OUT("pz3_anomalies_offline_full.csv"), index=False)
    print("[analysis] FULL anomalies:", len(out), flush=True)
    print("ANALYSIS_PHASE1_DONE", flush=True)
    sys.exit(0)

# ---------------- фаза 2: порівняння потоку та офлайн ----------------
st = pd.read_parquet(a.scored)
m = st.merge(sc, on="ts", suffixes=("_stream", "_off"))
both = m[m["scored_stream"] & m["scored_off"]]
cmp_ = {"n_stream_rows": len(st), "n_stream_scored": int(st["scored"].sum()), "n_stream_unscored_flat": int((~st["scored"]).sum()),
        "n_unscored_flat_offline_same_ts": int((~m["scored_off"]).sum()), "n_compared": len(both),
        "max_abs_diff_z_ret": float(np.nanmax(np.abs(both["z_ret_stream"] - both["z_ret_off"]))),
        "max_abs_diff_z_vol": float(np.nanmax(np.abs(both["z_vol_stream"] - both["z_vol_off"]))),
        "max_abs_diff_sigma": float(np.nanmax(np.abs(both["sigma_ret_stream"] - both["sigma_ret_off"]))),
        "n_anomalies_stream": int(both["anomaly_stream"].sum()), "n_anomalies_offline": int(both["anomaly_off"].sum()),
        "flag_mismatches": int((both["anomaly_stream"] != both["anomaly_off"]).sum()),
        "scored_mismatches": int((m["scored_stream"] != m["scored_off"]).sum()), "k": K_SEL, "zv_min": ZV_MIN}
pd.DataFrame([cmp_]).to_csv(W_OUT("pz3_stream_vs_offline.csv"), index=False)
print("[analysis] потік проти офлайн:", cmp_, flush=True)
s2 = st[st["scored"]].copy()
s2["datetime_utc"] = pd.to_datetime(s2["ts"], unit="s", utc=True).dt.strftime("%Y-%m-%d %H:%M:%S")
s2["date"] = s2["datetime_utc"].str[:10]; s2["period"] = s2["date"].str[:7]
rows = []
for k in (3, 5, 7, 10, K_SEL):
    fr = s2["z_ret"] > k; f = fr & (s2["z_vol"] > ZV_MIN)
    row = {"k": k, "n_scored": len(s2), "n_return_only": int(fr.sum()), "n_flags": int(f.sum()), "share_pct": round(100 * f.mean(), 3)}
    for p in PERIODS:
        row["n_flags_" + p] = int(f[s2["period"] == p].sum())
    rows.append(row)
pd.DataFrame(rows).drop_duplicates("k").to_csv(W_OUT("pz3_trigger_counts_stream.csv"), index=False)
print(pd.DataFrame(rows).drop_duplicates("k").to_string(index=False), flush=True)
an = s2[s2["anomaly"]].copy()
an_by_day = an.groupby("date").agg(n_anomalies=("ts", "size"), max_z_ret=("z_ret", "max"), max_z_vol=("z_vol", "max"),
                                   first_utc=("datetime_utc", "min"), last_utc=("datetime_utc", "max")).sort_values("n_anomalies", ascending=False)
an_by_day.to_csv(W_OUT("pz3_stream_anomalies_by_date.csv"))
print(an_by_day.to_string(), flush=True)
out = an.rename(columns={"r": "log_return"})
out["timestamp"] = out["datetime_utc"]; out["k"] = K_SEL; out["k_v"] = ZV_MIN
cols = ["timestamp", "ts", "close", "log_return", "volume", "z_ret", "vol_ratio", "score", "rule", "k", "k_v", "date", "period",
        "z_vol", "sigma_ret", "med_vol"]
out[cols].to_csv(W_OUT("pz3_anomalies_stream.csv"), index=False)
out[cols].to_parquet(W_OUT("pz3_anomalies_stream.parquet"), index=False)
full = pd.read_csv(W_OUT("pz3_anomalies_offline_full.csv"))
full["in_stream_replay"] = False
for d0, d1 in PERIODS.values():
    full["in_stream_replay"] |= full["date"].between(d0, d1)
full["rule_version"] = "v2"
full.to_csv(W_OUT("pz3_anomalies.csv"), index=False)
full.to_parquet(W_OUT("pz3_anomalies.parquet"), index=False)
print(f"[analysis] pz3_anomalies.csv: {len(full)} рядків; у відтворених періодах {int(full.in_stream_replay.sum())}", flush=True)
print("ANALYSIS_DONE", flush=True)
