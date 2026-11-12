#!/usr/bin/env bash
# Повний відтворюваний запуск ПЗ2 однією командою:  bash code/run_all.sh   (з будь-якого каталогу)
# Після нього results/, figures/, screenshots/ створені лише цим кодом. Seed 42.
set -euo pipefail
cd "$(dirname "$0")/.."
export JAVA_HOME=${JAVA_HOME:-$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")}   # JDK із PATH, якщо не задано
: "${CHROME_BIN:=$(command -v chromium chromium-browser chrome 2>/dev/null | head -1)}"; export CHROME_BIN   # Chromium для знімків Spark UI
mkdir -p results/logs figures screenshots/txt
T0=$(date +%s)
python code/env_shots.py                 # скріншоти середовища (java, python, pyspark, jupyter, smoke-тест)
python code/pz2_spark.py 2>&1 | tee results/logs/run.log     # конвеєр Spark, Spark UI-знімки, txt для терміналів
python code/measure_memory.py            # пік пам'яті: pandas і Spark, окремі процеси (після завершення основної сесії)
python code/crop_ui.py                  # кроп і розкладка двох знімків Spark UI для друку (рис. 4, 5)
python code/make_figures.py              # рисунки з results/
python code/make_screens.py              # рендер txt -> PNG терміналів
T1=$(date +%s)
echo "run_all.sh wall-clock: $((T1-T0)) s ($(date '+%F %T'))" | tee results/logs/run_all_runtime.txt
