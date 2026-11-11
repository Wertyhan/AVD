# -*- coding: utf-8 -*-
"""Метрики ОДНОГО прогону (ПЗ3): тривалість мікробатчів, вхідна/оброблена швидкість за запитами, e2e-затримка (mean/p50/p95/max
за всіма 30 240 повідомленнями), час foreachBatch, loadavg. Використання: metrics.py RESULTS_DIR
Виходи: pz3_metrics_per_query.csv, pz3_latency_summary.csv, pz3_metrics_summary.csv (довга форма), load_summary.csv."""
import sys, json, os
import numpy as np, pandas as pd
R = sys.argv[1]
pr = json.load(open(os.path.join(R, "producer_stats.json")))
prog = pd.read_csv(os.path.join(R, "stream", "progress.csv"))
bs = pd.read_csv(os.path.join(R, "stream", "batch_stats.csv"))
lat = pd.read_parquet(os.path.join(R, "stream", "latency_per_message.parquet"))
q = lambda s, p: float(np.percentile(s, p))
rows = []
for name, g in prog.groupby("query"):
    g = g[g.num_input_rows > 0]
    rows.append({"query": name, "batches_with_data": len(g), "rows_total": int(g.num_input_rows.sum()),
                 "batch_ms_mean": g.trigger_execution_ms.mean(), "batch_ms_median": g.trigger_execution_ms.median(),
                 "batch_ms_p95": q(g.trigger_execution_ms.dropna(), 95), "batch_ms_max": g.trigger_execution_ms.max(),
                 "input_rows_per_s_mean": g.input_rows_per_s.mean(), "input_rows_per_s_median": g.input_rows_per_s.median(),
                 "processed_rows_per_s_mean": g.processed_rows_per_s.mean(), "processed_rows_per_s_median": g.processed_rows_per_s.median(),
                 "rows_per_batch_min": int(g.num_input_rows.min()), "rows_per_batch_median": float(g.num_input_rows.median()), "rows_per_batch_max": int(g.num_input_rows.max())})
pq = pd.DataFrame(rows).round(2); pq.to_csv(os.path.join(R, "pz3_metrics_per_query.csv"), index=False)
L = []
for col, nm in (("lat_to_score_start_s", "Kafka CreateTime -> початок оцінювання (foreachBatch)"), ("lat_to_result_s", "Kafka CreateTime -> готовий результат (кінець оцінювання)")):
    s = lat[col]
    L.append({"definition": nm, "n_messages": len(s), "mean_s": s.mean(), "p50_s": q(s, 50), "p95_s": q(s, 95), "p99_s": q(s, 99), "max_s": s.max()})
pd.DataFrame(L).round(3).to_csv(os.path.join(R, "pz3_latency_summary.csv"), index=False)
b = bs[bs.n_raw > 0]
summ = [("producer_messages", pr["sent"]), ("producer_seconds", pr["seconds"]), ("producer_msg_per_s", pr["msg_per_s"]), ("producer_delay_ms", pr["delay_ms"]),
        ("producer_delivery_errors", pr["delivery_errors"]), ("foreachbatch_batches", len(b)), ("foreachbatch_s_mean", b.foreachbatch_s.mean()),
        ("foreachbatch_s_median", b.foreachbatch_s.median()), ("foreachbatch_s_p95", q(b.foreachbatch_s, 95)), ("foreachbatch_s_max", b.foreachbatch_s.max()),
        ("foreachbatch_rows_per_batch_min", int(b.n_raw.min())), ("foreachbatch_rows_per_batch_median", float(b.n_raw.median())), ("foreachbatch_rows_per_batch_max", int(b.n_raw.max())),
        ("anomalies_total", int(b.n_anomalies.sum())), ("invalid_rows", int(b.n_invalid.sum()))]
pd.DataFrame(summ, columns=["metric", "value"]).round(3).to_csv(os.path.join(R, "pz3_metrics_summary.csv"), index=False)
# навантаження за інтервал від першого до останнього мікробатчу
ld = pd.read_csv(os.path.join(R, "load_during_run.csv"))
ld["t"] = ld["epoch"] - ld["epoch"].min()
pd.DataFrame([{"samples": len(ld), "duration_s": int(ld.t.max()), "load1_mean": ld.load1.mean(), "load1_min": ld.load1.min(), "load1_max": ld.load1.max(),
               "load5_mean": ld.load5.mean(), "load15_last": ld.load15.iloc[-1], "java_procs_mean": ld.java_procs.mean(), "java_procs_max": ld.java_procs.max(),
               "vcpu": os.cpu_count()}]).round(2).to_csv(os.path.join(R, "load_summary.csv"), index=False)
for f in ("pz3_metrics_per_query.csv", "pz3_latency_summary.csv", "pz3_metrics_summary.csv", "load_summary.csv"):
    print(f); print(pd.read_csv(os.path.join(R, f)).to_string(index=False))
