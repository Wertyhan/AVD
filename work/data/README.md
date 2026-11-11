# Bitcoin 1-minute dataset: facts and cleaning decisions

Source: https://www.kaggle.com/datasets/mczielinski/bitcoin-historical-data (obtained as archive.zip from Google Drive id 1OtA56aRQMUDnmtOJMMPRHZw7NQjlBfyS; the archive.zip file was not verified against Kaggle directly).
Original files: archive.zip (111,874,794 bytes = 111.9 MB) containing btcusd_1-min_data.csv (396,849,494 bytes = 396.8 MB). `unzip -t`: no errors.
Reproduce: `python3 -I scripts/prepare.py raw/btcusd_1-min_data.csv .`

## Facts (same as facts.csv)
- raw_file: btcusd_1-min_data.csv
- raw_csv_size_MB: 396.8
- archive_size_MB: 111.9
- raw_rows: 7816537
- raw_columns_dtypes: Timestamp:int64; Open:float64; High:float64; Low:float64; Close:float64; Volume:float64
- raw_nan_cells: 0
- raw_sorted: True
- duplicate_timestamps: 0
- rows_with_nan: 0
- off_minute_timestamps: 0
- first_timestamp_utc: 2012-01-01 00:01:00+00:00
- last_timestamp_utc: 2026-11-11 03:37:00+00:00
- grid_minutes_expected: 7816537
- missing_minutes: 0
- gap_count: 0
- zero_volume_candles: 1312419
- zero_volume_share_pct: 16.79
- invalid_high_lt_max_oc: 0
- invalid_low_gt_min_oc: 0
- invalid_high_lt_low: 0
- negative_values: 0
- nonpositive_price_rows: 0
- invalid_candles_total: 0
- final_rows_1min: 7816537
- sample_rows: 44640
- final_rows_1h: 130276
- final_rows_1d: 5429
- incomplete_1h_bins(n<60): 2
- incomplete_1d_bins(n<1440): 2
- max_close: 126202.0 at 2025-10-06 18:57:00+00:00
- min_close: 3.8 at 2012-01-27 01:58:00+00:00
- top1_daily_abs_logret: 2013-04-11 logret=-0.6639 close=83.4 n_minutes=1440
- top2_daily_abs_logret: 2020-03-12 logret=-0.4940 close=4841.67 n_minutes=1440
- top3_daily_abs_logret: 2012-08-19 logret=-0.3589 close=8.06 n_minutes=1440
- top4_daily_abs_logret: 2013-04-10 logret=-0.3461 close=162.0 n_minutes=1440
- top5_daily_abs_logret: 2013-11-18 logret=+0.3375 close=669.01 n_minutes=1440
- daily_logret_note: log(Close_d/Close_{d-1}) between consecutive available daily bins; includes gap days

## Cleaning decisions
- Timestamp (unix seconds) converted to UTC datetime (column datetime_utc in parquet files; CSVs keep unix seconds only).
- Sorted by Timestamp (raw was already sorted); duplicates would be dropped keeping first (0 found).
- Invalid candles (High < max(Open,Close), Low > min(Open,Close), High < Low, negatives, NaN) would be removed; 0 found, so no rows removed. Final 1-min rows = raw rows.
- IMPORTANT: this file has NO missing minutes over its whole span (complete 1-min grid), and 16.79% of candles have Volume = 0. These look like gap-filled (flat, previous-close) candles rather than real trades, so zero-volume candles were KEPT unchanged (not dropped, not flagged as invalid). Treat zero-volume bars as "no trading/filled" in any analysis. Note the earliest rows (2012) are mostly flat zero-volume bars.
- The last timestamp is the extraction time of the file; the final hour/day bin is partial (see incomplete bin counts).
- 1h/1d: resample by UTC bins (label = bin start): Open=first, High=max, Low=min, Close=last, Volume=sum; empty bins dropped; n_minutes = count of 1-min rows in the bin. Incomplete bins (first and last) are kept.
- Daily log return = ln(Close_d / Close_{d-1}) on the daily bins' closes.
- btc_sample_1min.csv = 2021-05-01 00:00 to 2021-05-31 23:59 UTC (44,640 rows).

## Files
btc_1min.parquet/.csv, btc_1h.parquet/.csv, btc_1d.parquet/.csv (CSV columns: Timestamp,Open,High,Low,Close,Volume[,n_minutes]), btc_sample_1min.csv, facts.csv, raw/, scripts/prepare.py
