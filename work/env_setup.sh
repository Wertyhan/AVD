#!/usr/bin/env bash
# Idempotent. Source it (". env_setup.sh") to get env vars; run with "start" to start mongod+kafka.
W=/home/user/AVD/work; T=$W/tools
export MAMBA_ROOT_PREFIX=$W/mamba
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64      # JDK 21 (system). Hive needs JDK8: $MAMBA_ROOT_PREFIX/envs/jdk8
export HADOOP_HOME=$T/hadoop-3.3.6 HIVE_HOME=$T/apache-hive-3.1.3-bin KAFKA_HOME=$T/kafka_2.13-3.7.0
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$KAFKA_HOME/bin:$MAMBA_ROOT_PREFIX/envs/mongo/bin:$PATH
export HIVE_JAVA_HOME=$MAMBA_ROOT_PREFIX/envs/jdk8   # use: JAVA_HOME=$HIVE_JAVA_HOME $HIVE_HOME/bin/hive ...
mkdir -p $T && cd $T || return 0 2>/dev/null
pip list 2>/dev/null | grep -qi '^pyspark ' || { pip install -q --ignore-installed wheel setuptools; pip install -q pyspark; }
pip install -q pymongo pyarrow pandas matplotlib python-docx pymupdf pillow playwright 2>&1 | grep -v WARN
[ -x $T/mm/bin/micromamba ] || { curl -sSfO https://conda.anaconda.org/conda-forge/linux-64/micromamba-2.9.0-0.tar.bz2 && mkdir -p mm && tar xjf micromamba-2.9.0-0.tar.bz2 -C mm; }
[ -x $MAMBA_ROOT_PREFIX/envs/mongo/bin/mongod ] || $T/mm/bin/micromamba create -y -q -n mongo -c conda-forge mongodb
[ -x $MAMBA_ROOT_PREFIX/envs/jdk8/bin/java ] || $T/mm/bin/micromamba create -y -q -n jdk8 -c conda-forge openjdk=8
dl(){ [ -d "$2" ] || { curl -sSfO "$1" && tar xzf "$(basename $1)"; }; }
dl https://archive.apache.org/dist/kafka/3.7.0/kafka_2.13-3.7.0.tgz $KAFKA_HOME
dl https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz $HADOOP_HOME
dl https://archive.apache.org/dist/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz $HIVE_HOME
if [ "$1" = start ]; then
  mkdir -p $W/mongo_data
  pgrep -x mongod >/dev/null || mongod --dbpath $W/mongo_data --bind_ip 127.0.0.1 --port 27017 --fork --logpath $W/mongo_data/mongod.log
  if ! pgrep -f kafka.Kafka >/dev/null; then
    $KAFKA_HOME/bin/kafka-storage.sh format -t $($KAFKA_HOME/bin/kafka-storage.sh random-uuid|tail -1) -c $KAFKA_HOME/config/kraft/server.properties --ignore-formatted
    nohup $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/kraft/server.properties >$W/kafka.log 2>&1 &
  fi
fi
