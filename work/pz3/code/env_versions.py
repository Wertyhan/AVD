# -*- coding: utf-8 -*-
"""Версії ПЗ, конфігурація машини: results/env_versions.csv (запускається на початку run_pipeline.sh)."""
import os, subprocess, sys, platform
from importlib.metadata import version
R = sys.argv[1]
env = {k: v for k, v in os.environ.items() if k != "JAVA_TOOL_OPTIONS"}
sh = lambda c: subprocess.run(c, shell=True, capture_output=True, text=True, env=env, executable="/bin/bash").stdout.strip()
mem = [l for l in open("/proc/meminfo") if l.startswith("MemTotal")][0].split()[1]
rows = [("OS", f"{platform.freedesktop_os_release()['PRETTY_NAME']} / kernel {platform.release()}"),
        ("CPU", f"{os.cpu_count()} vCPU"), ("RAM", f"{int(mem)/1048576:.1f} GiB"), ("Python", platform.python_version()),
        ("Java", sh("java -version 2>&1 | head -1")),
        ("Apache Kafka", "3.7.0 (Scala 2.13), KRaft combined broker+controller, 1 broker"),
        ("Spark/PySpark", version("pyspark")), ("Spark Kafka connector", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0 (Maven Central)"),
        ("confluent-kafka (producer)", f"{version('confluent-kafka')} (librdkafka)"),
        ("pandas", version("pandas")), ("numpy", version("numpy")), ("pyarrow", version("pyarrow")), ("matplotlib", version("matplotlib")),
        ("playwright", f"{version('playwright')} / Chromium 153"),
        ("Spark master", "local[2], driver 2g, shuffle.partitions=4, session timezone UTC"),
        ("Seed", "42 (конвеєр детермінований; випадковість лише у скриптах перевірки)"),
        ("Load note", "хост спільний з іншими завданнями: loadavg під час запуску – results/load_during_run.csv, results/load_summary.csv")]
import pandas as pd
pd.DataFrame(rows, columns=["component", "value"]).to_csv(os.path.join(R, "env_versions.csv"), index=False)
print(pd.DataFrame(rows).to_string(index=False))
