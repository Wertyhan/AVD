# -*- coding: utf-8 -*-
"""Спільні шляхи та захоплення виводу для ПЗ5. Файл screenshots/txt/<name>.txt:
рядок 1 – справжня команда ('$ python code/...'), рядок 2 – коментар ('# …'), далі – реальний вивід."""
import os, io, sys, contextlib
BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
RES, FIG, SHOTS, TXT = f"{BASE}/results", f"{BASE}/figures", f"{BASE}/screenshots", f"{BASE}/screenshots/txt"
for _d in (RES, FIG, SHOTS, TXT):
    os.makedirs(_d, exist_ok=True)
SEED = 42

class Tee(io.StringIO):
    def write(self, s):
        sys.__stdout__.write(s); return super().write(s)

@contextlib.contextmanager
def cap(name, script, desc):
    """Дублює stdout у screenshots/txt/<name>.txt: '$ python code/<script>' + '# desc' + реальний вивід."""
    buf = Tee()
    with contextlib.redirect_stdout(buf):
        yield
    with open(f"{TXT}/{name}.txt", "w", encoding="utf-8") as f:
        f.write(f"$ python code/{script}\n# {desc}\n" + buf.getvalue())

def save(df, name):
    df.to_csv(f"{RES}/{name}.csv", index=False, encoding="utf-8")
