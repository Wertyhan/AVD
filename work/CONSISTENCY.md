# Cross-report consistency check, PZ1–PZ5

Sources: work/pz1..pz5/report.md; title pages from the PDFs (deliverables/ for PZ1–PZ4, work/pz5/build/ for PZ5); reference facts from work/data/facts.csv and README.md; benchmark from work/bench/results/bench_summary.csv.
Checked against the data itself: row count 7 816 537, zero-volume 1 312 419, max close 126 202 (2025-10-06), max High 126 272, first candle 4.58, last timestamp 2026-11-11 03:37 UTC. All match facts.csv.

## 1. Dataset facts: mismatches
None. Every stated value matches facts.csv:
- 7 816 537 rows: PZ1 l.155 and code l.916/1699; PZ2 l.3, 31, 37; PZ3 l.3, 155; PZ4 l.31, 59, 213; PZ5 l.3.
- Zero-volume 1 312 419 (16,79 %): PZ2 l.49; PZ3 l.146, 618; PZ4 l.85, 213.
- 1h 130 276: PZ4 l.33, 165; PZ5 l.21.
- 1d 5 429: PZ2 l.105, 248; PZ4 l.34–36, 221.
- Start 01.01.2012 00:01 UTC: PZ1 l.155. First candle price 4,58 (PZ1 l.60, PZ2 l.31) matches the data.
- Min 3,80 (2012 Low): PZ1 l.177, PZ2 l.160.
- Daily top moves: 11.04.2013 −0,664 (PZ4 l.108 rounds the facts value −0,6639); 12.03.2020 −0,494 (PZ3 l.56, PZ4 l.108).
- Incomplete edge days: 01.01.2012 1 439 min and 11.11.2026 218 min (PZ4 l.108); PZ5 l.58 last hour 03:00 has 38 min. Consistent with facts.

Gaps (stated nowhere, so not a mismatch):
- Date range end 11.11.2026 is not stated as a dataset range in PZ1–PZ4. PZ2 l.3 and PZ3 l.3 say "2012–2026 роки". PZ5 uses 11.11.2026 only for the test end (l.107).
- Max close 126 202 at 2025-10-06 18:57 is not stated in any report. PZ2 l.173 gives the 2025 max High 126 272, which agrees with the data.
- Zero-volume count and share are absent from PZ1 (only "нульовий обсяг" at l.60).
- The 130 276 hourly row count is absent from PZ2 (it appears in PZ4 and PZ5 only).

## 2. Terminology (notable)
- Log return: "логарифмічна дохідність" (PZ2 l.37, 490; PZ3 l.617), "лог-дохідність" (PZ3 l.54, 845; PZ4 l.108, 184, 190; PZ5 l.21), and bare "дохідність" (PZ5 l.31, 32, 41, 76). Column name log_ret in PZ2. PZ1 has no log-return section. Suggest one term, e.g. "логарифмічна дохідність (log return)".
- Flat minutes: "flat-хвилини" (PZ2, PZ4, PZ5 l.47/58) vs "пласкі хвилини" (PZ3 l.146, 618; PZ5 l.3 and others). Same concept.
- Engine labels: PZ1 l.274–279 "Hive (рушій MR)", "Pandas", "PySpark local[*]" vs PZ2 l.197–199 "Hive (MR)", "pandas", "PySpark". Same numbers. "Pandas" vs "pandas" is mixed inside PZ1 and PZ2.
- Number format: PZ2 l.49, 105, 248 write "5429" and "1440" without spaces; elsewhere "5 429" and "1 440".
- Row noun: "свічка" (PZ1, PZ4), "запис" (PZ2 l.3, 31; PZ3 l.3), "хвилина" (PZ2, PZ3). Acceptable but could be unified.

## 3. Cross-report numbers
- Benchmark: PZ1 l.272–279 and PZ2 l.196–203 (median, IQR, startup, processing, min–max) match bench_summary.csv exactly. engine_benchmark.csv in pz1/results and pz2/results is identical to bench_summary.csv. PZ2 l.205 ratios 1,7–2,1 are correct (27,20/12,88 = 2,11; 24,08/14,04 = 1,71).
- PZ4 → PZ2: spark_daily_features 5 429 (PZ4 l.36, 184) ok. "добові OHLC за 4,547 с" (PZ4 l.205, 223) equals PZ2 l.221 (Т2 Spark, median of 3 runs, CSV read). pandas 4,884 s and cache 0,618 s also match PZ2 l.221–226. PZ2 labels 4,547 as "читання CSV", PZ4 as "добові OHLC": same measurement, different label.
- PZ4 → PZ3: 5 437 anomalies (PZ3 l.189, 280) and k = 9 (PZ3 l.152, 193) ok. The yearly values 2019 = 777 and 2023 = 749 (PZ4 l.190) match pz3/results/pz3_anomalies_by_year.csv, but PZ3 report.md does not state them.
- PZ5 → PZ2: flat share 8,04 % (2017) and max 3,95 % from 2018 (PZ5 l.58) match PZ2 l.165, 171. Kurtosis 118,11 (PZ5 l.76) matches PZ2 l.120. 98,1 % flat for 27.01.2012 (PZ5 l.47) matches pz2/results/pz2_top10_rv.csv (0,9806) but is not in PZ2 text. 77 666 hours and the test end 11.11.2026 01:00 (PZ5 l.58, 107) are internally consistent with the 1h data.
- Yearly table PZ1 l.177–191 (Hive) matches PZ2 Table 3 (counts, mean Close, max High, min Low) for all 15 years; volume differs only by rounding.

## 4. Title pages and cards
- All five title pages: Міністерство освіти і науки України / ХНУРЕ / Кафедра штучного інтелекту / Дисципліна «Аналітика великих даних»; Виконав ст. гр. ДМм-26-1 Зелінський В.С.; Прийняв Золотухін О.В.; Харків – 2026. Identical.
- Themes match cards/pz1..pz5.md: Hadoop (HDFS, MapReduce, Hive); Apache Spark distributed processing; Kafka and Spark Structured Streaming; MongoDB NoSQL; Spark MLlib.
