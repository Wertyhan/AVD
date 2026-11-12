#!/usr/bin/env bash
# Повний відтворюваний запуск ПЗ5 однією командою:  bash code/run_all.sh   (з будь-якого каталогу). Seed 42.
# results/, figures/, screenshots/ створюються лише цим кодом.
set -euo pipefail
cd "$(dirname "$0")/.."
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}
export PATH=$JAVA_HOME/bin:$PATH
mkdir -p results figures screenshots/txt
echo "start $(date '+%F %T'); loadavg: $(cat /proc/loadavg); nproc: $(nproc)" | tee results/run_all_log.txt
T0=$(date +%s)
step() { local s=$(date +%s); echo "== $1 ($(date '+%T'), loadavg $(cut -d' ' -f1-3 /proc/loadavg))" | tee -a results/run_all_log.txt; shift; "$@"; echo "   $(( $(date +%s)-s )) s" | tee -a results/run_all_log.txt; }
step "1 вхідні дані: перевірка даних ПЗ2 та опорні числа ПЗ2" python code/input_diff.py
step "2 Spark MLlib (повний конвеєр, Spark UI)"           bash -c 'python code/pz5_spark.py 2>&1 | tee results/run.log'
step "3 статистика: бутстреп, поділи, напрям, ембарго"     bash -c 'python code/pz5_stats.py 2>&1 | tee results/stats.log'
step "4 рисунки"                                           python code/make_figures.py
step "5 рендер терміналів у PNG"                           python code/make_screens.py
T1=$(date +%s)
echo "run_all.sh wall-clock: $((T1-T0)) s (end $(date '+%F %T'); loadavg $(cat /proc/loadavg))" | tee -a results/run_all_log.txt
