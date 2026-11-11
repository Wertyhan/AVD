# -*- coding: utf-8 -*-
"""Метрики (ПЗ3, завдання 4 метрик): пропускна здатність producer, тривалість мікробатчів, швидкість обробки, затримка."""
import sys, json, os
import numpy as np, pandas as pd
R = sys.argv[1]
pr = json.load(open(os.path.join(R, "producer_stats.json")))
prog = pd.read_csv(os.path.join(R, "stream", "progress.csv"))
bs = pd.read_csv(os.path.join(R, "stream", "batch_stats.csv"))
rows = [{"metric": "producer_messages", "value": pr["sent"]}, {"metric": "producer_seconds", "value": pr["seconds"]},
        {"metric": "producer_msg_per_s", "value": pr["msg_per_s"]}, {"metric": "producer_delay_ms", "value": pr["delay_ms"]},
        {"metric": "producer_delivery_errors", "value": pr["delivery_errors"]}]
for q, g in prog.groupby("query"):
    g = g[g.num_input_rows > 0]
    if g.empty: continue
    rows += [{"metric": f"{q}_batches_with_data", "value": len(g)},
             {"metric": f"{q}_rows_total", "value": int(g.num_input_rows.sum())},
             {"metric": f"{q}_trigger_ms_mean", "value": round(g.trigger_execution_ms.mean(), 1)},
             {"metric": f"{q}_trigger_ms_median", "value": round(g.trigger_execution_ms.median(), 1)},
             {"metric": f"{q}_trigger_ms_p95", "value": round(g.trigger_execution_ms.quantile(.95), 1)},
             {"metric": f"{q}_trigger_ms_max", "value": round(g.trigger_execution_ms.max(), 1)},
             {"metric": f"{q}_processed_rows_per_s_mean", "value": round(g.processed_rows_per_s.mean(), 1)},
             {"metric": f"{q}_processed_rows_per_s_median", "value": round(g.processed_rows_per_s.median(), 1)},
             {"metric": f"{q}_input_rows_per_s_mean", "value": round(g.input_rows_per_s.mean(), 1)}]
b = bs[bs.n_raw > 0]
w = b.n_raw.to_numpy()
rows += [{"metric": "e2e_latency_mean_s_weighted", "value": round(float(np.average(b.latency_mean_s, weights=w)), 3)},
         {"metric": "e2e_latency_median_of_batch_means_s", "value": round(float(b.latency_mean_s.median()), 3)},
         {"metric": "e2e_latency_p95_max_over_batches_s", "value": round(float(b.latency_p95_s.max()), 3)},
         {"metric": "e2e_latency_max_s", "value": round(float(b.latency_max_s.max()), 3)},
         {"metric": "foreachbatch_s_mean", "value": round(float(b.foreachbatch_s.mean()), 3)},
         {"metric": "foreachbatch_batches", "value": len(b)}]
pd.DataFrame(rows).to_csv(os.path.join(R, "pz3_metrics_summary.csv"), index=False)
print(pd.DataFrame(rows).to_string(index=False))
