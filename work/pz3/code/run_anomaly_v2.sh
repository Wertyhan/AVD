#!/usr/bin/env bash
# Повторний потоковий прогін правила аномалій v2: нова тема bigdata-topic-v2 (ті самі 3 періоди, той самий producer),
# лише запит anomaly (foreachBatch), прогрів 7-денної бази з parquet, потім порівняння з офлайн (фаза 2).
# Віконні результати першого прогону (results/stream/) не змінюються.
set -u
W=/home/user/AVD/work; P=$W/pz3; L=$P/run/logs_v2; O=$P/results/stream_anomaly_v2; mkdir -p $L
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 KAFKA_HOME=$W/tools/kafka_2.13-3.7.0
export PATH=$JAVA_HOME/bin:$KAFKA_HOME/bin:$PATH
BS=localhost:9092; TOPIC=bigdata-topic-v2
KN() { env -u JAVA_TOOL_OPTIONS "$@"; }
rm -rf $L/* $O $P/run/ckpt_v2 $P/run/done_v2.json
KN kafka-topics.sh --bootstrap-server $BS --delete --topic $TOPIC > /dev/null 2>&1; sleep 3
KN kafka-topics.sh --bootstrap-server $BS --create --topic $TOPIC --partitions 3 --replication-factor 1 > $L/topic_create.log 2>&1
KN kafka-topics.sh --bootstrap-server $BS --describe --topic $TOPIC > $L/topic_describe.log 2>&1
cd $P/code
s=$(date +%s.%N)
nohup python3 -I streaming_job.py --only-anomaly --topic $TOPIC --out $O --ckpt $P/run/ckpt_v2 --done-flag $P/run/done_v2.json \
  --history-parquet $W/data/btc_1min.parquet --kafka-pkg org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0 \
  --trigger-seconds 10 --max-offsets 3000 > $L/spark_job.log 2>&1 &
for i in $(seq 1 240); do grep -q STREAMING_STARTED $L/spark_job.log && break; sleep 1; done
grep -q STREAMING_STARTED $L/spark_job.log || { echo "Spark не стартував"; tail -30 $L/spark_job.log; exit 1; }
python3 -I producer.py --csv $W/data/raw/btcusd_1-min_data.csv --topic $TOPIC --delay-ms 10 \
  --stats $O/../producer_stats_v2.json --done-flag $P/run/done_v2.json > $L/producer.log 2>&1
while pgrep -f "streaming_job.py --only-anomaly" > /dev/null; do sleep 3; done
e=$(date +%s.%N)
KN kafka-get-offsets.sh --bootstrap-server $BS --topic $TOPIC --time -1 > $L/topic_offsets.log 2>&1
echo "stream_v2_wall_s,$(python3 -c "print(round($e-$s,1))")" > $L/wall.csv
python3 -I anomaly_analysis.py --parquet $W/data/btc_1min.parquet --out $P/results --cache $P/run --scored $O/scored.parquet > $L/analysis_phase2.log 2>&1
echo "RUN_V2_DONE rc=$?"
