# -*- coding: utf-8 -*-
"""Компактний друк CSV у термінал для скріншотів: show_csv.py FILE [--T] [--cols a,b,c] [--round N]"""
import argparse, pandas as pd
ap = argparse.ArgumentParser(); ap.add_argument("f"); ap.add_argument("--T", action="store_true"); ap.add_argument("--cols", default=""); ap.add_argument("--round", type=int, default=3)
a = ap.parse_args()
d = pd.read_csv(a.f)
if a.cols: d = d[a.cols.split(",")]
d = d.round(a.round)
pd.set_option("display.width", 200)
print((d.T if a.T else d).to_string(header=not a.T) if a.T else d.to_string(index=False))
