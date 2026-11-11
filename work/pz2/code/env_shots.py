# -*- coding: utf-8 -*-
"""Реальні команди перевірки середовища: вивід записується в screenshots/txt (заголовок = справжня команда).
Запуск з каталогу pz2:  python code/env_shots.py"""
import os, subprocess
BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
TXT = f"{BASE}/screenshots/txt"; os.makedirs(TXT, exist_ok=True)
CMDS = [("13_java_version", "java -version 2>&1 | grep -v Picked"),
        ("14_python_version", "python --version"),
        ("15_pip_show_pyspark", "pip show pyspark | head -8"),
        ("16_jupyter_version", "jupyter --version"),
        ("17_pyspark_smoke", "python code/smoke_test.py 2>&1 | grep -v -e WARN -e Picked -e log4j -e 'setLogLevel' -e FutureWarning -e require_minimum")]
for name, cmd in CMDS:
    out = subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd=BASE).stdout
    shown = cmd.split(" 2>&1")[0].split(" | ")[0] if name != "17_pyspark_smoke" else "python code/smoke_test.py"
    with open(f"{TXT}/{name}.txt", "w", encoding="utf-8") as f:
        f.write(f"$ {shown}\n{out}")
    print(name, "ok")
