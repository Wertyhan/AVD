#!/usr/bin/env bash
# Короткий повторний потоковий прогін правила v2 (тема bigdata-topic-v2-ui) для знімків Spark UI під час роботи запиту anomaly.
set -u
P=$(cd "$(dirname "$0")/.." && pwd); W=$(dirname "$P"); L=$P/run/logs_ui_v2; O=$P/run/ui_v2_out; mkdir -p $L
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 KAFKA_HOME=$W/tools/kafka_2.13-3.7.0
export PATH=$JAVA_HOME/bin:$KAFKA_HOME/bin:$PATH
BS=localhost:9092; TOPIC=bigdata-topic-v2-ui; PORT=${UI_PORT:-4040}
KN() { env -u JAVA_TOOL_OPTIONS "$@"; }
rm -rf $L/* $O $P/run/ckpt_ui_v2 $P/run/done_ui_v2.json
KN kafka-topics.sh --bootstrap-server $BS --delete --topic $TOPIC > /dev/null 2>&1; sleep 3
KN kafka-topics.sh --bootstrap-server $BS --create --topic $TOPIC --partitions 3 --replication-factor 1 > $L/topic_create.log 2>&1
cd $P/code
nohup python3 -I streaming_job.py --only-anomaly --topic $TOPIC --out $O --ckpt $P/run/ckpt_ui_v2 --done-flag $P/run/done_ui_v2.json \
  --history-parquet $W/data/btc_1min.parquet --kafka-pkg org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0 \
  --trigger-seconds 10 --max-offsets 3000 --ui-port $PORT > $L/spark_job.log 2>&1 &
for i in $(seq 1 240); do grep -q STREAMING_STARTED $L/spark_job.log && break; sleep 1; done
grep -q STREAMING_STARTED $L/spark_job.log || { echo "Spark не стартував"; tail -30 $L/spark_job.log; exit 1; }
grep -a "Spark UI:" $L/spark_job.log
nohup python3 -I producer.py --csv $W/data/raw/btcusd_1-min_data.csv --topic $TOPIC --delay-ms 10 --max-rows 15000 \
  --stats $L/producer_stats.json --done-flag $P/run/done_ui_v2.json > $L/producer.log 2>&1 &
echo started
