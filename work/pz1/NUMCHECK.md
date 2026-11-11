# NUMCHECK: PZ1 report.md, main body

Scope: every numeric value in sections 1–7, Висновки, Tables 1–7, figure captions and in-text numbers (report.md lines 1–273). Appendix A excluded. Placeholders `[ЗАПОВНИТИ ПІСЛЯ ЗАПУСКУ]` excluded by instruction.

Result: 0 value mismatches. Every checked value matches its source with the stated rounding. 3 defects (N1–N3: one source-record inconsistency, one wrong attribution in the text, one imprecise wording). N4–N5 are notes, not defects.

## Verified tables

| Table | Source | Result |
|---|---|---|
| 1 pseudo-distributed config | code/run_pz1.py lines 246–265, 273; logs/run_pz1.out 564–565 | all values match (5120/4, 1024/-Xmx768m, 1536/-Xmx1024m, vmem off, 0,8, 134 217 728); peaks 2 487 697 408 and 605 048 832 match |
| 2 HDFS blocks | logs/04_hdfs.log lines 390–392 | IDs blk_1073742503–505, lengths 134 217 728 / 134 217 728 / 128 414 038, sum 396 849 494 match |
| 3 Streaming counters | results/mr_counters_extended.csv | all 14 values match for both jobs |
| 4 Hive yearly | results/hive_yearly_stats.csv | all 15 rows match (candles, volume, avg Close, max High, min Low), rounded to 0,01 |
| 5 top-10 days | results/hive_top10_volume_days.csv, hive_top10_range_days.csv | both columns, all 10 rows, dates and values match |
| 6 cross-check | results/crosscheck_*.csv | row counts and match flags match; tolerances 1e-9 and 1e-12 match code/run_pz1.py lines 375, 384, 562; 1e-13 matches max_rel_diff_volume |
| 7 timings | results/timing_summary.csv | placeholders only, intentional |

## Verified prose values (selection)

- 7 816 537 rows, 396,8 МБ, 396 849 494 bytes, 378,47 MB (= 396 849 494 / 1 048 576): match data/README.md and logs.
- 270 553 174 016 bytes capacity, 1 live DataNode, 134 217 728 block size: match results/hdfs_cluster_config.csv.
- 396 857 686 bytes read, 3 splits, Launched map 4 / Killed 1: match mr_counters_extended.csv and mr_yearly job log.
- 2012: 527 039 candles; 2016/2020/2024: 527 040; 2026: 452 378 to 11.11.2026: match hive_yearly_stats.csv.
- 5 525 344,45 BTC (2015 maximum); 1,81 млн BTC (2021 maximum after 2021): match.
- 5 429 days, 1 439 candles (first day), 218 candles (last day), 03:37 UTC: match mr_daily_ohlc_rowcount.csv, mr_daily_ohlc_sample.csv, data/README.md.
- 5 024 184,243438596 (Hive) vs 5 024 184,243438736 (MR) for 2014: match hive_yearly_stats.csv and mr_yearly_stats.csv.
- 179 monthly groups: hive_monthly_stats.csv has 179 data rows.
- Nov 2013 965 986,91; Dec 2013 952 410,66; Nov 2015 887 623,85; Nov 2026 14 618 candles: match hive_monthly_stats.csv.
- 18.12.2013 Open 675,00, Low 382,21; 19.05.2021 High 43 601,63, Low 30 066,00, volume 32 468,86: match hive_top10_*.csv.
- 9 of the top-10 volume days in 2013–2015; top-10 range days all in 2021–2026: match Table 5.
- 114 lines of Python (mapper_ohlc 63 + reducer_ohlc 51) and 18 lines for the CTAS (hive_04_daily_struct.hql): match `wc -l`.
- 10 Hive applications in the final run (application_1794418481996_0004 to _0013): match logs/daemon_hadoop-root-resourcemanager-vm.log.
- Pre-check on 200 000 rows: code/run_pz1.py line 366 (`head -n 200001`) gives 200 000 data rows.
- Shuffled Maps 3 for MR jobs: logs/06_mr_yearly.log line 61, logs/06_mr_daily_ohlc.log lines 60 and later.
- Combiner 127 655 772 bytes to 567 bytes shuffle; 1 879 to 798 pairs (WordCount); 304 input lines: match counters files and logs/05_wordcount.log.

## Findings (wording or source-record issues, not value mismatches)

- N1. Table 6, row "Hive – Pandas, топ-10 за High − Low | 10 / 10". The report value is correct: the top-10 sets match. But results/crosscheck_mr_hive_vs_pandas.csv, row hive_vs_pandas_top10_range_days, records right_rows = 5429. Fix the CSV (right_rows should be 10) or add a note.
- N2. Line 266, "3 data-local map-задачі". For mr_yearly, the counter Data-local map tasks is 4 (includes the speculative attempt, see line 101); for mr_daily_ohlc it is 3. State the daily job explicitly or write 4 for the annual job.
- N3. Line 190, "після 2021 року місячний обсяг стабілізувався на рівні десятків тисяч BTC". hive_monthly_stats.csv gives a range of 16 983 BTC (2026-11, partial month) to 130 935 BTC (2026-02). 2022-06 is 108 871 BTC. Several months are above 100 тис. BTC, so "десятки тисяч" is imprecise. Suggest "переважно десятки тисяч BTC, з піками понад 100 тис.".
- N4. Line 29, "бібліотеку guava 19 з Hive замінено на версію з Hadoop". The replacement with guava-27.0-jre.jar is logged (logs/run_pz1.out line 598). The guava-19 version of Hive's jar is stated in code/run_pz1.py line 486 but not logged. Consistent, not directly evidenced.
- N5. Line 7, "типово 128 МБ". General HDFS default, not a measured value. Consistent with dfs.blocksize = 134 217 728 bytes (128 MiB).

## Notes

- Numeric labels inside figures (Figure 4 bar labels such as 5.0, 0.6, 5.5; Figure 5 title "5429 діб") are not in the body text. Figure 4 values match results/mr_yearly_stats.csv (rounded to 0,1 million BTC). Decimal-point labels are a style issue, see STYLECHECK.md.
- results/timing_summary.csv already holds the medians for Table 7 and the timing sentences (lines 253–258, 264, 268). The placeholders are intentional per the task, so they were left unchanged.
