# -*- coding: utf-8 -*-
"""pandas_yearly.py - ПЗ1: кількість свічок і сумарний обсяг за роками (Pandas, повний CSV)."""
from pathlib import Path
import pandas as pd
CSV = Path(__file__).resolve().parents[2] / 'data/raw/btcusd_1-min_data.csv'   # work/data/raw
df = pd.read_csv(CSV, float_precision='round_trip').dropna()
df['Timestamp'] = df['Timestamp'].astype('int64')
years = pd.to_datetime(df['Timestamp'], unit='s').dt.year
print(df.groupby(years.rename('year')).agg(candles=('Volume', 'size'), volume=('Volume', 'sum')).reset_index().to_string(index=False))
