#!/usr/bin/env bash
# Оркестрація ПЗ3: брокер -> тема -> Spark-застосунок -> producer -> консольний споживач -> знімки UI.
# Усі виводи зберігаються у pz3/run/logs та використовуються для скріншотів.
set -u
W=/home/user/AVD/work; P=$W/pz3; L=$P/run/logs; mkdir -p $L $P/results/stream; rm -f $L/*
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 KAFKA_HOME=$W/tools/kafka_2.13-3.7.0
export PATH=$JAVA_HOME/bin:$KAFKA_HOME/bin:$PATH
BS=localhost:9092; TOPIC=bigdata-topic
KAFKA_NOPROXY() { env -u JAVA_TOOL_OPTIONS "$@"; }
ts() { date +%s.%N; }
T0=$(ts); echo "stage,start_epoch,end_epoch,seconds" > $P/results/timings_stages.csv
stage() { echo "$1,$2,$3,$(python3 -c "print(round($3-$2,2))")" >> $P/results/timings_stages.csv; }

( while true; do echo "$(date +%s) $(cut -d' ' -f1-3 /proc/loadavg)" >> $L/loadavg.log; sleep 5; done ) &
LOADPID=$!
# ---- 1. брокер (KRaft): зупинка діючого та чистий запуск, лог запуску
s=$(ts)
pkill -f kafka.Kafka; sleep 4
rm -f $L/kafka_start.log
KAFKA_NOPROXY nohup $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/kraft/server.properties > $L/kafka_server.log 2>&1 &
for i in $(seq 1 60); do grep -q "Kafka Server started" $L/kafka_server.log && break; sleep 1; done
stage broker_start $s $(ts)
grep -E "KafkaRaftServer|Kafka Server started|process.roles|Awaiting socket|Registered broker|started \(kafka" $L/kafka_server.log | head -20 > $L/kafka_start_excerpt.log
KAFKA_NOPROXY kafka-broker-api-versions.sh --bootstrap-server $BS 2>&1 | head -3 > $L/broker_api.log

# ---- 2. тема
s=$(ts)
KAFKA_NOPROXY kafka-topics.sh --bootstrap-server $BS --delete --topic $TOPIC > /dev/null 2>&1; sleep 3
KAFKA_NOPROXY kafka-topics.sh --bootstrap-server $BS --create --topic $TOPIC --partitions 3 --replication-factor 1 > $L/topic_create.log 2>&1
KAFKA_NOPROXY kafka-topics.sh --bootstrap-server $BS --describe --topic $TOPIC > $L/topic_describe.log 2>&1
KAFKA_NOPROXY kafka-topics.sh --bootstrap-server $BS --list > $L/topic_list.log 2>&1
stage topic_create $s $(ts)

# ---- 3. Spark Structured Streaming (у фоні)
rm -rf $P/run/ckpt $P/results/stream/*; rm -f $P/run/done.json
s_spark=$(ts)
cd $P/code
nohup python3 -I streaming_job.py --out $P/results/stream --ckpt $P/run/ckpt --done-flag $P/run/done.json \
  --kafka-pkg org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0 --watermark "10 minutes" --trigger-seconds 10 --max-offsets 3000 \
  > $L/spark_job.log 2>&1 &
for i in $(seq 1 180); do grep -q STREAMING_STARTED $L/spark_job.log && break; sleep 1; done
grep -q STREAMING_STARTED $L/spark_job.log || { echo "Spark не стартував"; tail -30 $L/spark_job.log; exit 1; }
echo "spark started after $(python3 -c "print(round($(ts)-$s_spark,1))") s"

# ---- 4. producer
s_prod=$(ts)
python3 -I producer.py --csv $W/data/raw/btcusd_1-min_data.csv --topic $TOPIC --delay-ms 10 \
  --stats $P/results/producer_stats.json --done-flag $P/run/done.json > $L/producer.log 2>&1 &
PROD=$!
# ---- 5. паралельно: консольний споживач, групи, знімки UI
for i in $(seq 1 120); do grep -q "надіслано 2000" $L/producer.log && break; sleep 1; done
KAFKA_NOPROXY timeout 60 kafka-console-consumer.sh --bootstrap-server $BS --topic $TOPIC --group pz3-console --from-beginning --max-messages 8 \
  --property print.key=true --property print.timestamp=true --property print.partition=true --property print.offset=true > $L/console_consumer.log 2>&1
KAFKA_NOPROXY kafka-consumer-groups.sh --bootstrap-server $BS --describe --group pz3-console > $L/consumer_group_describe_live.log 2>&1
KAFKA_NOPROXY kafka-get-offsets.sh --bootstrap-server $BS --topic $TOPIC --time -1 > $L/topic_offsets_live.log 2>&1
sleep 20
UI=$(grep -a "Spark UI:" $L/spark_job.log | head -1 | sed "s/.*Spark UI: //; s/[[:space:]]*$//"); echo "UI=$UI"; python3 -I ui_shot.py $P/screenshots $UI > $L/ui_shot.log 2>&1
wait $PROD; stage producer $s_prod $(ts)
echo "producer finished"
while pgrep -f streaming_job.py > /dev/null; do sleep 3; done
stage spark_job $s_spark $(ts)
tail -5 $L/spark_job.log
# ---- 6. перевірочний споживач (повне перечитування) та групи
KAFKA_NOPROXY timeout 120 kafka-console-consumer.sh --bootstrap-server $BS --topic $TOPIC --group pz3-verify --from-beginning --timeout-ms 15000 \
  --property print.partition=true > $L/verify_consumer_all.log 2>&1
wc -l < $L/verify_consumer_all.log > $L/verify_consumer_count.txt
KAFKA_NOPROXY kafka-consumer-groups.sh --bootstrap-server $BS --list > $L/consumer_groups_list.log 2>&1
KAFKA_NOPROXY kafka-consumer-groups.sh --bootstrap-server $BS --describe --group pz3-verify > $L/consumer_group_describe_final.log 2>&1
KAFKA_NOPROXY kafka-consumer-groups.sh --bootstrap-server $BS --describe --group pz3-console > $L/consumer_group_describe_console.log 2>&1
KAFKA_NOPROXY kafka-get-offsets.sh --bootstrap-server $BS --topic $TOPIC --time -1 > $L/topic_offsets_final.log 2>&1
KAFKA_NOPROXY kafka-log-dirs.sh --bootstrap-server $BS --describe --topic-list $TOPIC 2>/dev/null | tail -1 | cut -c1-600 > $L/log_dirs.log
kill $LOADPID
echo "total $(python3 -c "print(round($(ts)-$T0,1))") s"
