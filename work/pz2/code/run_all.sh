#!/usr/bin/env bash
# Повний відтворюваний запуск ПЗ2 одним командою:  bash code/run_all.sh   (з будь-якого каталогу)
# Після нього results/, figures/, screenshots/ створені лише цим кодом. Seed 42.
set -euo pipefail
cd "$(dirname "$0")/.."
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}
mkdir -p results/logs figures screenshots/txt
T0=$(date +%s)
python code/env_shots.py                 # скріншоти середовища (java, python, pyspark, jupyter, smoke-тест)
python code/pz2_spark.py 2>&1 | tee results/logs/run.log     # конвеєр Spark, Spark UI-знімки, txt для терміналів
python code/measure_memory.py            # пік пам'яті: pandas і Spark, окремі процеси (після завершення основної сесії)
python code/make_figures.py              # рисунки з results/
python code/make_screens.py              # рендер txt -> PNG терміналів
T1=$(date +%s)
echo "run_all.sh wall-clock: $((T1-T0)) s ($(date '+%F %T'))" | tee results/logs/run_all_runtime.txt
