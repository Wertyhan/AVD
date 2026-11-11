# -*- coding: utf-8 -*-
"""Завдання 8: закономірності аномалій (офлайн-оцінка всієї історії правилом v2, k = 9) – за годиною доби UTC і днем тижня (2018+),
напрям (обвал/зростання) 50 найактивніших днів, кластеризація у часі. Виходи: pz3_patterns_hour_weekday_2018plus.csv,
pz3_top50_dates_direction.csv, pz3_patterns_summary.csv. Використання: anomaly_patterns.py RESULTS HISTORY_PARQUET OFFLINE_SCORED_PARQUET"""
import os, sys
import numpy as np, pandas as pd
R, HIST, OFF = sys.argv[1:4]
sc = pd.read_parquet(OFF, columns=["ts", "r", "z_ret", "z_vol", "scored", "anomaly", "date"])
sc = sc[sc["scored"]].copy()
dtm = pd.to_datetime(sc["ts"], unit="s", utc=True)
sc["year"] = dtm.dt.year; sc["hour"] = dtm.dt.hour; sc["dow"] = dtm.dt.dayofweek
v = sc[sc["year"] >= 2018]
hw = v.groupby(["dow", "hour"]).agg(scored_minutes=("ts", "size"), n_anomalies=("anomaly", "sum")).reset_index()
hw["share_pct"] = (100 * hw["n_anomalies"] / hw["scored_minutes"]).round(4)
hw["weekday"] = hw["dow"].map(dict(enumerate(["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Нд"])))
hw.to_csv(os.path.join(R, "pz3_patterns_hour_weekday_2018plus.csv"), index=False)
wd = v.groupby("dow").agg(n=("anomaly", "sum"), m=("ts", "size")); wd["share"] = wd.n / wd.m
hr = v.groupby("hour").agg(n=("anomaly", "sum"), m=("ts", "size")); hr["share"] = hr.n / hr.m
# напрям: денна лог-дохідність (за close) для топ-50 дат
h = pd.read_parquet(HIST, columns=["Timestamp", "Close"])
dc = h["Close"].groupby(pd.to_datetime(h["Timestamp"], unit="s", utc=True).dt.strftime("%Y-%m-%d").to_numpy()).last()
dret = np.log(dc).diff()
fl = sc[sc["anomaly"]]
bd = (fl.groupby("date").agg(n_anomalies=("ts", "size"), max_z_ret=("z_ret", "max"), n_down_minutes=("r", lambda s: int((s < 0).sum())))
      .sort_values(["n_anomalies", "max_z_ret"], ascending=False))
bd["day_logret"] = dret.reindex(bd.index).round(4)
bd["rank_all"] = np.arange(1, len(bd) + 1)
bd["direction"] = np.where(bd["day_logret"] < 0, "обвал", "зростання")
top = bd.head(50); top.to_csv(os.path.join(R, "pz3_top50_dates_direction.csv"))
b18 = bd[bd.index >= "2018-01-01"].copy(); b18["rank_2018plus"] = np.arange(1, len(b18) + 1); t18 = b18.head(50)
summ = {"flagged_total": len(fl), "flagged_2018plus": int(v["anomaly"].sum()),
        "weekday_mean_share_pct": 100 * wd.n.iloc[:5].sum() / wd.m.iloc[:5].sum(), "weekend_mean_share_pct": 100 * wd.n.iloc[5:].sum() / wd.m.iloc[5:].sum(),
        "weekday_to_weekend_ratio": (wd.n.iloc[:5].sum() / wd.m.iloc[:5].sum()) / (wd.n.iloc[5:].sum() / wd.m.iloc[5:].sum()),
        "max_weekday": wd.share.idxmax(), "max_weekday_share_pct": 100 * wd.share.max(), "min_weekday": wd.share.idxmin(), "min_weekday_share_pct": 100 * wd.share.min(),
        "max_hour_utc": hr.share.idxmax(), "max_hour_share_pct": 100 * hr.share.max(), "min_hour_utc": hr.share.idxmin(), "min_hour_share_pct": 100 * hr.share.min(),
        "max_to_min_hour_ratio": hr.share.max() / hr.share.min()}
for nm, t in (("top50_all", top), ("top50_2018plus", t18)):
    summ.update({f"{nm}_crash": int((t.direction == "обвал").sum()), f"{nm}_rally": int((t.direction == "зростання").sum()),
                 f"{nm}_crash_share_pct": 100 * float((t.direction == "обвал").mean()),
                 f"{nm}_flagged_minutes_down_share_pct": 100 * t.n_down_minutes.sum() / t.n_anomalies.sum(),
                 f"{nm}_median_abs_day_logret": float(t.day_logret.abs().median())})
summ["flagged_minutes_down_share_pct_all"] = 100 * float((fl["r"] < 0).mean())
tsf = fl[fl["year"] >= 2018]["ts"].to_numpy(); gap = np.diff(tsf)
summ["flagged_2018plus_with_neighbour_within_60min_pct"] = 100 * float(np.mean(np.r_[gap[0] <= 3600, (np.minimum(np.r_[np.inf, gap], np.r_[gap, np.inf]) <= 3600)[1:-1], gap[-1] <= 3600]))
summ["top50_2018plus_dates_share_of_2018plus_flags_pct"] = 100 * t18.n_anomalies.sum() / v["anomaly"].sum()
pd.DataFrame([summ]).round(4).T.rename(columns={0: "value"}).to_csv(os.path.join(R, "pz3_patterns_summary.csv"), index_label="metric")
print(pd.DataFrame([summ]).T.to_string())
