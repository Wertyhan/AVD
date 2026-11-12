#!/usr/bin/env bash
# Обробка результатів ОДНОГО прогону run_pipeline.sh: перевірки, метрики, watermark, аномалії (офлайн, потік проти офлайн, незалежна перевірка,
# out-of-sample), закономірності, рисунки, скріншоти терміналу. Запускати після PIPELINE_DONE (Kafka має працювати).
set -u
P=$(cd "$(dirname "$0")/.." && pwd); W=$(dirname "$P");  export P; L=$P/run/logs; R=$P/results; SC=$P/screenshots
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 KAFKA_HOME=$W/tools/kafka_2.13-3.7.0
export PATH=$JAVA_HOME/bin:$KAFKA_HOME/bin:$PATH
cd $P
shot() { local n=$1 c=$2; shift 2; python3 -I code/term_shot.py $SC/$n.png --cmd "$c" --save $L/$n.out "$@" > $L/shot_$n.log 2>&1; }
HIST=$W/data/btc_1min.parquet; OFF=$P/run/offline_scored_v2.parquet
rm -f $OFF
python3 -I code/verify_windows.py $R/stream $W/data/raw/btcusd_1-min_data.csv > $L/verify_windows.log 2>&1
python3 -I code/metrics.py $R > $L/metrics.log 2>&1
python3 -I code/watermark_analysis.py $R bigdata-topic > $L/watermark_analysis.log 2>&1
env -u JAVA_TOOL_OPTIONS python3 -I code/watermark_backlog_test.py $R > $L/watermark_backlog_test.log 2>&1
python3 -I code/anomaly_analysis.py --parquet $HIST --out $R --cache $P/run > $L/analysis_phase1.log 2>&1
python3 -I code/anomaly_analysis.py --parquet $HIST --out $R --cache $P/run --scored $R/stream/scored.parquet > $L/analysis_phase2.log 2>&1
python3 -I code/thin_market_stats.py > $L/thin_market.log 2>&1
python3 -I code/anomaly_design_eval.py --parquet $HIST --v1-cache $P/run/offline_scored.parquet --out $R > $L/design_eval.log 2>&1
python3 -I code/anomaly_oos_eval.py $R $HIST > $L/oos_eval.log 2>&1
python3 -I code/verify_anomalies_independent.py $R $HIST $OFF > $L/independent_check.log 2>&1
python3 -I code/anomaly_patterns.py $R $HIST $OFF > $L/patterns.log 2>&1
python3 -I code/make_figures.py > $L/figures1.log 2>&1
python3 -I code/make_figures_anomaly.py > $L/figures2.log 2>&1
# скріншоти терміналу (команди виконуються зараз)
shot term_09_window_verification "python3 code/show_csv.py results/pz3_window_verification.csv --T" 
shot term_10_trigger_counts_stream_vs_offline "python3 code/show_csv.py results/pz3_stream_vs_offline.csv --T && python3 code/show_csv.py results/pz3_trigger_counts_stream.csv"
shot term_11_metrics "python3 code/show_csv.py results/pz3_metrics_per_query.csv --cols query,batches_with_data,batch_ms_mean,batch_ms_p95,input_rows_per_s_mean,processed_rows_per_s_mean && python3 code/show_csv.py results/pz3_latency_summary.csv --cols n_messages,mean_s,p50_s,p95_s,max_s"
shot term_12_threshold_selection "python3 code/show_csv.py results/pz3_threshold_selection.csv --cols k,n_flags,share_2018plus_pct,per_day_2018plus,lift_event_vs_base,chosen"
shot term_13_independent_check "python3 code/show_csv.py results/pz3_independent_check.csv --T && python3 code/show_csv.py results/pz3_oos_summary.csv --T"
shot term_14_watermark "python3 code/show_csv.py results/pz3_watermark_disorder.csv --cols scope,n_messages,n_out_of_order,max_lateness_s && python3 code/show_csv.py results/pz3_watermark_backlog_test.csv --cols micro_batches,rows_in_windows,rows_lost,numRowsDroppedByWatermark_sum"
# час
python3 - <<'PY'
import pandas as pd, json
import os; R = os.environ["P"] + "/results"
st = pd.read_csv(R + "/timings_stages.csv"); of = pd.read_csv(R + "/offline_timing.csv")
t = pd.concat([st[["stage", "seconds"]], pd.DataFrame({"stage": ["offline_score_full_history_%d_rows" % of.rows[0]], "seconds": [of.seconds[0]]}),
               pd.DataFrame({"stage": ["producer_send_loop_s"], "seconds": [json.load(open(R + "/producer_stats.json"))["seconds"]]})])
t.to_csv(R + "/timings.csv", index=False); print(t)
PY
echo POST_DONE
