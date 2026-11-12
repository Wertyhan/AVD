#!/usr/bin/env bash
# Повний прогін ПЗ3 одним запуском: брокер -> свіжа тема -> Spark Structured Streaming (УСІ запити: parquet-приймачі 15 хв і 1 год,
# memory-приймачі, добова статистика, console, foreachBatch-аномалії v2 з прогрівом --history-parquet) -> producer (3 періоди)
# -> консольний споживач/групи -> знімки Spark UI (під час роботи, після >= 22 мікробатчів) -> перевірочний споживач -> run_post.sh.
# Вихід: results/ (CSV), screenshots/, логи run/logs/.
set -u
P=$(cd "$(dirname "$0")/.." && pwd); W=$(dirname "$P"); L=$P/run/logs; R=$P/results; SC=$P/screenshots
mkdir -p $L $R $SC $R/stream; rm -rf $L/* $P/run/ckpt $R/stream/* $P/run/done.json
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 KAFKA_HOME=$W/tools/kafka_2.13-3.7.0
export PATH=$JAVA_HOME/bin:$KAFKA_HOME/bin:$PATH
BS=localhost:9092; TOPIC=bigdata-topic
KN() { env -u JAVA_TOOL_OPTIONS "$@"; }
ts() { date +%s.%N; }
cd $P
shot() { # shot NAME "команда" [опції term_shot]: команда виконується ЗАРАЗ у shell, вивід зберігається в $L/NAME.out і рендериться у PNG
  local n=$1 c=$2; shift 2
  python3 -I code/term_shot.py $SC/$n.png --cmd "$c" --save $L/$n.out "$@" > $L/shot_$n.log 2>&1
}
T0=$(ts); echo "stage,start_epoch,end_epoch,seconds" > $R/timings_stages.csv
stage() { echo "$1,$2,$3,$(python3 -c "print(round($3-$2,2))")" >> $R/timings_stages.csv; }
python3 -I code/env_versions.py $R > $L/env_versions.log 2>&1

# навантаження хоста: loadavg + кількість java-процесів кожні 5 с
echo "epoch,load1,load5,load15,java_procs" > $R/load_during_run.csv
( while true; do echo "$(date +%s),$(cut -d' ' -f1-3 /proc/loadavg | tr ' ' ','),$(pgrep -c java)" >> $R/load_during_run.csv; sleep 5; done ) &
LOADPID=$!

# ---- 1. брокер (KRaft): чистий перезапуск
s=$(ts)
pkill -f kafka.Kafka; sleep 4
KN nohup $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/kraft/server.properties > $L/kafka_server.log 2>&1 &
for i in $(seq 1 60); do grep -q "Kafka Server started" $L/kafka_server.log && break; sleep 1; done
stage broker_start $s $(ts)
grep -E "KafkaRaftServer|Kafka Server started|process.roles|Awaiting socket|Registered broker|started \(kafka" $L/kafka_server.log | head -20 > $L/kafka_start_excerpt.log
shot term_01_broker_start "grep -E 'process.roles|Kafka Server started|Registered broker|Awaiting socket' run/logs/kafka_server.log | cut -c1-200"

# ---- 2. тема (свіжа): видалення старих тем, створення і опис
s=$(ts)
for t in $(KN kafka-topics.sh --bootstrap-server $BS --list 2>/dev/null | grep -v '^__'); do KN kafka-topics.sh --bootstrap-server $BS --delete --topic $t > /dev/null 2>&1; done; sleep 4
shot term_02_topic_create_describe "kafka-topics.sh --bootstrap-server $BS --create --topic $TOPIC --partitions 3 --replication-factor 1 && kafka-topics.sh --bootstrap-server $BS --describe --topic $TOPIC"
cp $L/term_02_topic_create_describe.out $L/topic_create_describe.log
stage topic_create $s $(ts)

# ---- 3. Spark Structured Streaming (усі запити) у фоні
s_spark=$(ts)
cd $P/code
nohup python3 -I streaming_job.py --out $R/stream --ckpt $P/run/ckpt --done-flag $P/run/done.json --topic $TOPIC \
  --history-parquet $W/data/btc_1min.parquet --kafka-pkg org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0 \
  --watermark "10 minutes" --trigger-seconds 10 --max-offsets 3000 > $L/spark_job.log 2>&1 &
for i in $(seq 1 240); do grep -q STREAMING_STARTED $L/spark_job.log && break; sleep 1; done
grep -q STREAMING_STARTED $L/spark_job.log || { echo "Spark не стартував"; tail -30 $L/spark_job.log; kill $LOADPID; exit 1; }
echo "spark started after $(python3 -c "print(round($(ts)-$s_spark,1))") s"
UI=$(grep -a "Spark UI:" $L/spark_job.log | head -1 | sed "s/.*Spark UI: //; s/[[:space:]]*$//; s#//[^:]*:#//localhost:#"); echo "UI=$UI"
cd $P

# ---- 4. producer (3 періоди, затримка 10 мс)
s_prod=$(ts)
python3 -I code/producer.py --csv $W/data/raw/btcusd_1-min_data.csv --topic $TOPIC --delay-ms 10 \
  --stats $R/producer_stats.json --done-flag $P/run/done.json > $L/producer.log 2>&1 &
PROD=$!
# ---- 5. паралельно: консольний споживач, групи, зсуви
for i in $(seq 1 120); do grep -q "надіслано 2000" $L/producer.log && break; sleep 1; done
shot term_04_console_consumer "kafka-console-consumer.sh --bootstrap-server $BS --topic $TOPIC --group pz3-console --from-beginning --max-messages 8 --property print.key=true --property print.partition=true --property print.offset=true --property print.timestamp=true"
shot term_05_consumer_groups "kafka-consumer-groups.sh --bootstrap-server $BS --describe --group pz3-console"
shot term_06_topic_offsets "kafka-get-offsets.sh --bootstrap-server $BS --topic $TOPIC --time -1"
# ---- 6. знімки Spark UI після >= 22 мікробатчів запиту anomaly (продюсер ще працює)
for i in $(seq 1 400); do grep -aq "\[anomaly\] batch 22:" $L/spark_job.log && break; sleep 1; done
echo "UI-знімки: $(grep -a '\[anomaly\] batch' $L/spark_job.log | tail -1)"
python3 -I code/ui_shot_run.py $SC $UI sink_win1h > $L/ui_shot.log 2>&1; tail -3 $L/ui_shot.log
wait $PROD; stage producer $s_prod $(ts)
echo "producer finished"
while pgrep -f streaming_job.py > /dev/null; do sleep 3; done
stage spark_job $s_spark $(ts)
tail -3 $L/spark_job.log | cut -c1-300
# ---- 7. скріншоти: producer-лог, console sink, foreachBatch
shot term_03_producer_log "sed -n '1,5p;\$p' run/logs/producer.log | cut -c1-300"
shot term_07_spark_console_batch "grep -a -A 7 -m1 '^Batch: 12\$' run/logs/spark_job.log"
shot term_08_spark_anomaly_foreachbatch "grep -a -E '\[anomaly\]' run/logs/spark_job.log | sed -n '1,4p;20,23p;48,50p'"
# ---- 8. перевірочний споживач (повне перечитування) та групи
KN timeout 120 kafka-console-consumer.sh --bootstrap-server $BS --topic $TOPIC --group pz3-verify --from-beginning --timeout-ms 15000 \
  --property print.partition=true > $L/verify_consumer_all.log 2>&1
wc -l < $L/verify_consumer_all.log > $L/verify_consumer_count.txt
shot term_05b_consumer_groups_list "kafka-consumer-groups.sh --bootstrap-server $BS --list"
shot term_05c_consumer_group_verify "kafka-consumer-groups.sh --bootstrap-server $BS --describe --group pz3-verify"
shot term_06b_topic_offsets_final "kafka-get-offsets.sh --bootstrap-server $BS --topic $TOPIC --time -1"
cp $L/term_06b_topic_offsets_final.out $L/topic_offsets_final.log
kill $LOADPID
echo "pipeline_wall_s,$(python3 -c "print(round($(ts)-$T0,1))")" > $L/wall.csv
echo "PIPELINE_DONE"
echo "далі: bash code/run_post.sh"
