# -*- coding: utf-8 -*-
"""Офлайн-калібрування правила аномалій на всій історії та аналіз результатів потоку (ПЗ3, завдання 6 і 8)."""
import argparse, json, os, sys, time
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from anomaly_rule import score, K_GRID

ap = argparse.ArgumentParser()
ap.add_argument("--csv", required=True)
ap.add_argument("--out", required=True)
ap.add_argument("--scored", default="", help="scored.parquet з потокового застосунку")
ap.add_argument("--budget", type=float, default=0.01, help="допустима частка позначених хвилин")
a = ap.parse_args()
os.makedirs(a.out, exist_ok=True)
fig_dir = os.path.join(a.out, "figures"); os.makedirs(fig_dir, exist_ok=True)

# --- 1. завантаження повної історії
t0 = time.time()
df = pd.read_csv(a.csv, usecols=["Timestamp", "Close", "Volume"])
df.columns = ["ts", "close", "volume"]
n_all = len(df)
df = df.dropna()
df["ts"] = df["ts"].astype("int64")
df = df[(df["close"] > 0) & (df["volume"] >= 0)]
print(f"[analysis] рядків у CSV: {n_all}; коректних: {len(df)}; читання {time.time()-t0:.0f} с", flush=True)

# --- 2. оцінка правила на всій історії
t0 = time.time()
sc = score(df)
sc["date"] = pd.to_datetime(sc["ts"], unit="s", utc=True).dt.strftime("%Y-%m-%d")
print(f"[analysis] score() на {len(sc)} рядках: {time.time()-t0:.0f} с", flush=True)
v = sc[sc["scored"]]
nv = len(v)
rows = []
for k in K_GRID:
    n_ret, n_vol = int((v["z_ret"] > k).sum()), int((v["vol_ratio"] > k).sum())
    n_u = int(v[f"flag_k{k}"].sum())
    rows.append({"k": k, "n_valid_minutes": nv, "n_return_rule": n_ret, "n_volume_rule": n_vol, "n_union": n_u,
                 "share_union_pct": round(100 * n_u / nv, 4), "per_day_avg": round(n_u / (nv / 1440), 3)})
cnt = pd.DataFrame(rows)
cnt.to_csv(os.path.join(a.out, "anomaly_counts_per_k.csv"), index=False)
print(cnt.to_string(index=False), flush=True)
ok = cnt[cnt["share_union_pct"] <= 100 * a.budget]
k_sel = int(ok["k"].min()) if len(ok) else int(cnt["k"].max())
reason = (f"найменше k із сітки, при якому частка позначених хвилин <= {100*a.budget:.1f}%" if len(ok)
          else f"жодне k не вкладається у бюджет {100*a.budget:.1f}%, взято найбільше")
print(f"[analysis] обрано k = k_v = {k_sel} ({reason})", flush=True)
json.dump({"k": k_sel, "reason": reason, "budget": a.budget}, open(os.path.join(a.out, "chosen_k.json"), "w"))

# --- 3. на які реальні дати припадають аномалії
fl = v[v[f"flag_k{k_sel}"]].copy()
fl["score"] = np.fmax(fl["z_ret"].fillna(0) / k_sel, fl["vol_ratio"].fillna(0) / k_sel)
by_date = (fl.groupby("date").agg(n_anomalies=("ts", "size"), max_z_ret=("z_ret", "max"), max_vol_ratio=("vol_ratio", "max"),
                                  max_abs_r=("r", lambda s: s.abs().max())).sort_values("n_anomalies", ascending=False))
by_date.head(30).to_csv(os.path.join(a.out, "top_dates.csv"))
print("[analysis] топ-15 дат за кількістю аномалій:\n", by_date.head(15).to_string(), flush=True)
top_min = fl.sort_values("score", ascending=False).head(30).copy()
top_min["datetime_utc"] = pd.to_datetime(top_min["ts"], unit="s", utc=True).dt.strftime("%Y-%m-%d %H:%M")
top_min[["datetime_utc", "close", "volume", "r", "z_ret", "vol_ratio", "score"]].to_csv(
    os.path.join(a.out, "top_minutes.csv"), index=False)
fl["year"] = fl["date"].str[:4]
fl.groupby("year").size().rename("n_anomalies").to_csv(os.path.join(a.out, "anomalies_by_year.csv"))
chk = []
for d in ("2020-03-12", "2020-03-13", "2021-05-19", "2022-11-08", "2022-11-09"):   # перевірка відомих подій
    rank = int(by_date.index.get_loc(d)) + 1 if d in by_date.index else None
    chk.append({"date": d, "n_anomalies": int(by_date.loc[d, "n_anomalies"]) if d in by_date.index else 0, "rank_by_count": rank})
pd.DataFrame(chk).to_csv(os.path.join(a.out, "known_events_check.csv"), index=False)
print(pd.DataFrame(chk).to_string(index=False), flush=True)

# --- 4. порівняння потокової та офлайн-реалізацій на тих самих хвилинах
if a.scored and os.path.exists(a.scored):
    st = pd.read_parquet(a.scored)
    sub = score(df[df["ts"].isin(st["ts"])])
    m = st.merge(sub, on="ts", suffixes=("_stream", "_off"))
    both = m[m["scored_stream"] & m["scored_off"]]
    cmp_ = {"n_stream": len(st), "n_both_scored": len(both),
            "max_abs_diff_z": float(np.nanmax(np.abs(both["z_ret_stream"] - both["z_ret_off"]))) if len(both) else None,
            "max_abs_diff_q": float(np.nanmax(np.abs(both["vol_ratio_stream"] - both["vol_ratio_off"]))) if len(both) else None,
            "flag_mismatch_k%d" % k_sel: int((m[f"flag_k{k_sel}_stream"] != m[f"flag_k{k_sel}_off"]).sum())}
    pd.DataFrame([cmp_]).to_csv(os.path.join(a.out, "stream_vs_offline.csv"), index=False)
    print("[analysis] потік проти офлайн:", cmp_, flush=True)
    s2 = st[st["scored"]].copy()
    an = s2[s2[f"flag_k{k_sel}"]].copy()
    an["datetime_utc"] = pd.to_datetime(an["ts"], unit="s", utc=True).dt.strftime("%Y-%m-%d %H:%M:%S")
    an["date"] = an["datetime_utc"].str[:10]
    an.to_parquet(os.path.join(a.out, "anomalies_stream_final.parquet"), index=False)
    an.to_csv(os.path.join(a.out, "anomalies_stream_final.csv"), index=False)
    pd.DataFrame({"k": [k_sel], "n_stream_scored": [len(s2)], "n_anomalies": [len(an)],
                  "dates": [";".join(sorted(an["date"].unique()))]}).to_csv(os.path.join(a.out, "stream_anomaly_summary.csv"), index=False)
    print(f"[analysis] у відтвореному потоці при k={k_sel}: {len(an)} аномалій із {len(s2)} оцінених хвилин", flush=True)
    print(an.groupby("date").size().rename("n").to_string(), flush=True)

# --- 5. рисунки
try:
    import matplotlib; matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots(figsize=(7, 4))
    x = np.arange(len(cnt)); wdt = 0.27
    ax.bar(x - wdt, cnt["n_return_rule"], wdt, label="правило за дохідністю")
    ax.bar(x, cnt["n_volume_rule"], wdt, label="правило за обсягом")
    ax.bar(x + wdt, cnt["n_union"], wdt, label="об'єднання (АБО)")
    ax.set_xticks(x); ax.set_xticklabels([f"k={k}" for k in cnt["k"]]); ax.set_yscale("log")
    ax.set_ylabel("кількість хвилин"); ax.set_title("Кількість спрацювань залежно від k"); ax.legend()
    fig.tight_layout(); fig.savefig(os.path.join(fig_dir, "anomaly_counts_per_k.png"), dpi=150); plt.close(fig)
    if a.scored and os.path.exists(a.scored):
        st = pd.read_parquet(a.scored); st["dt"] = pd.to_datetime(st["ts"], unit="s", utc=True)
        periods = [g for _, g in st.groupby((st["ts"].diff() > 7200).cumsum())]
        fig, axs = plt.subplots(len(periods), 1, figsize=(9, 2.6 * len(periods)))
        axs = np.atleast_1d(axs)
        for ax_, g in zip(axs, periods):
            ax_.plot(g["dt"], g["close"], lw=0.8)
            an_ = g[g[f"flag_k{k_sel}"]]
            ax_.scatter(an_["dt"], an_["close"], s=8, c="red", label=f"аномалії k={k_sel}")
            ax_.set_ylabel("Close, USD"); ax_.legend(loc="upper left")
        fig.tight_layout(); fig.savefig(os.path.join(fig_dir, "stream_price_anomalies.png"), dpi=150); plt.close(fig)
except Exception as e:
    print("[analysis] рисунки не збережено:", e, flush=True)
print("ANALYSIS_DONE", flush=True)
