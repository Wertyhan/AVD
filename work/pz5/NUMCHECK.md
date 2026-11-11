# NUMCHECK: /home/user/AVD/work/pz5/report.md

Scope: main body, lines 1–272 (from "# Мета роботи" through conclusion 8). Appendix A (lines 274+) excluded. Table cells included.

Sources used:
- PZ5: `pz5/results/*.csv`, `pz5/results/INDEX.md`, `pz5/screenshots/txt/*.txt`, `pz5/screenshots/spark_ui_jobs_top.png`, `pz5/screenshots/spark_ui_stages.png`, `pz5/code/pz5_spark.py` (constants and grids).
- Shared data: `data/README.md`, `data/facts.csv`.
- PZ2: `pz2/_old_run/results/` (see defect D4; `pz2/results/` contains only an empty `logs/` folder).

Method: every numeric token in the main body was extracted with a regex and matched to a source. Rounding is accepted to the displayed decimals. Dates and times count as values. Structural numbers are not checked: section, table, figure and list numbering, and the words F1, ROC-AUC, PR-AUC.

Status legend: OK = matches the source at the shown precision. MISMATCH = value differs from the source. NOT FOUND = no source in the project. NOTE = value is correct, but the label or wording is inaccurate.

## Results by location

| Location (report line) | Value(s) | Source | Status |
|---|---|---|---|
| L3 Мета | 7 816 537 | PZ2 `_old_run/results/pz2_cleaning.csv` rows_raw; `data/facts.csv` raw_rows | OK (PZ2) |
| L13 | 25 %, 75 % | pz5 `pz5_metrics_test.csv` majority acc 0.7511; split pos share 0.248 | OK |
| L21 | 130 276; 2018, 2026 | `pz5/results/timings.csv` (130276 rows); `data/facts.csv` final_rows_1h 130276 | OK |
| L27 Table 1 caption | 77 667 | `pz5_split.csv` rows_model_total 77667 | OK |
| T1 ACF return, L31 | −0,0213; −0,0288; −0,0304 (lags 1, 2, 24) | `pz5_acf.csv` lag 1 −0.021336, lag 2 −0.028777, lag 24 −0.030448 | OK |
| T1 ACF |return|, L32 | 0,2951; 0,1982; 0,1445 (lags 1, 24, 168) | `pz5_acf.csv` lag 1 0.295142, lag 24 0.198238; `pz5_direction_check.csv` acf_abs_ret_lag168 0.144482 | OK |
| T1 CI, L33 | ±0,0070 | `pz5_direction_check.csv` conf95_bound 0.007033 | OK |
| T1 up share, L34 | 0,5066 | `pz5_direction_check.csv` up_share_next_hour 0.506566 | OK |
| T1 sign accuracy, L35 | 0,4644 | `pz5_direction_check.csv` sign_persistence_accuracy 0.463944 | OK |
| T1 LR direction, L36 | 0,5267 (ROC-AUC); 0,5134 (acc) | `pz5_direction_check.csv` 0.526689; 0.513442 | OK |
| T1 majority, L37 | 0,5000 | `pz5_direction_check.csv` majority accuracy (test) 0.500032 | OK |
| L39 text | 0,0070; 0,0304; 0,1 %; 0,527; 0,513; 0,500; 0,2951; 0,1445 | Same as above. 0,0304 is the largest |ACF| over lags 1–72 (`pz5_acf.csv`); 0.0304² = 0.09 % | OK |
| L47 | 27.01.2012; 98,1 %; top-5 | PZ2 `pz2_top10_rv.csv`: 2012-01-27 flat_share 0.98056 (=98,1 %), 4th row by rv | OK (PZ2) |
| T2 rows 2018–2026 (36 values) and 2017 (2 values), L53–L62 | Flat %, median rv, class-1 share (rolling), class-1 share (global) | `pz5_flat_share_by_year.csv` (flat_share_pct, median_rv); `pz5_target_share_by_year.csv` (pos_share_rolling_thr, pos_share_global_thr); `screenshots/txt/03_period.txt`, `04_split.txt`. All 38 cells checked individually | OK |
| L64 | 3,95 %; 8,04 %; 77 667; 01.01.2018; 11.11.2026 02:00 | `pz5_flat_share_by_year.csv` (2023 = 3.95, 2017 = 8.04); `pz5_split.csv` (rows, train_start, test_end) | OK |
| L68, L72 (code and text) | 75 (percentile); 720 (window); 1 (lead) | `pz5_spark.py` L367 (PCT 0.75, HI_WIN 720), L373–L384 | OK |
| L76 | 75 %; 720; 30 діб; 0,00720; 0,00318; 0,00794; 0,0581; 0,4400; 0,2185; 0,2669 | 720/24 = 30; `pz5_flat_share_by_year.csv` median 2018 0.0072, 2026 0.00318; `screenshots/txt/04_split.txt` global thr 0.00794; `pz5_target_share_by_year.csv` 2023 global 0.0581, 2018 global 0.4400; rolling min 0.2185 (2018), max 0.2669 (2023) | OK |
| L80 | 0,248 | Mean of `label` over 77 667 hours (`pz5_target_series.csv`) = 0.2480 | OK |
| L84 | 10⁻⁵; 118,11 | `pz5_spark.py` EPS = 1e-5; PZ2 `pz2_skew_kurtosis.csv` "1 год, усі" excess_kurtosis 118.106 | OK (PZ2) |
| T3 gap counts, L90–L94 (21 values) | 0,1,2,5,23 / 0; 0 / 0 (×2); 1,1,6 / 0; 0,0,1,0 / 0; 0 / 0 | `pz5_missing_values.csv` nulls_full_history (l1 1, l2 2, l5 5, l23 23, ret 1, abs 1, ret_6h 6, vol_z168 1, rest 0) and nulls_model_rows (all 0) | OK |
| T3 windows and lags, L90–L95 (12 values incl. OneHot 7) | lags 1, 2, 5, 23; windows 3, 6, 24, 168; ret 1 h, 6 h; OneHot 7 | `pz5_spark.py` L373, L375, L381, L379; `screenshots/txt/06_pipeline.txt` (OneHot 7) | OK |
| L97 | 2012; 23; 29 | `pz5_missing_values.csv` (history starts 2012; log_rv_l23 nulls 23); feature vector: 17 numeric + 3 cyclic + 9 dow slots = 29 (`09_importance.txt`) | OK |
| L101 | 80/20; 24 | `pz5_split.csv` train_share 80.0; EMB = 24 (`pz5_spark.py` L368) | OK |
| L111 caption | 80/20; 24 год | As above | OK |
| T4 and L115–L118 (14 values: 8 dates, 62 109, 24, 15 534, 77 667, 0,2478, 0,2489) | 2018-01-01 00:00; 2025-01-31 20:00; 2025-01-31 21:00; 2025-02-01 20:00; 2025-02-01 21:00; 2026-11-11 02:00; 62 109; 24; 15 534; 77 667; 0,2478; 0,2489 | `pz5_split.csv` (train_start, train_end, embargo_end_nominal_train_boundary, test_start, test_end, rows); `screenshots/txt/04_split.txt` (train 0.2478, test 0.2489). The embargo start 21:00 is train_end + 1 h | OK |
| L120 | 80 %; 2025-02-01 20:00; 2025-01-31 20:00; 0,2478; 0,2489 | `pz5_split.csv` embargo_end_nominal 2025-02-01 20:00, train_end 2025-01-31 20:00; `04_split.txt` | OK |
| L126 | 50; 66,7 %; 83,3 %; 100 %; 24; 3 блоки | `pz5_spark.py` L508 FOLDS (0.50, 0.667, 0.833, 1.0), L368 EMB | OK |
| T5 grids (18 values), L128–L134 | LR regParam {0; 0,001; 0,01; 0,1} × elastic {0; 0,5}; RF {100; 200} × depth {4; 8} × minInst {20; 100}; GBT {50; 100} × depth {3; 5} × step {0,05; 0,1} | `pz5_spark.py` GRIDS L477–L479; `pz5_tuning.csv` (24 rows) | OK |
| T5 best configs (8 values) | LR 0,001 / 0; RF 200 / 8 / 20; GBT 100 / 3 / 0,1 | `pz5/results/pz5_best_params.json` | OK |
| T5 CV metrics (9 values) | LR 0,8625 / 0,7456 / 0,0338; RF 0,8534 / 0,7234 / 0,0379; GBT 0,8680 / 0,7468 / 0,0383 | `pz5_tuning.csv` rows LR(0.001,0) 0.86248 / 0.74559 / 0.03378; RF(200,8,20) 0.85345 / 0.72340 / 0.03794; GBT(100,3,0.1) 0.86796 / 0.74684 / 0.03833 | OK |
| L136 text (13 values) | 0,1; 0,7193; 8; 4; 0,7234; 0,6994; 200; 0,005; 0,038; 0,7468; 24; 3; 1501,399 с | `pz5_tuning.csv`: LR(0.1,0) PR 0.71928; RF(200,4,20) PR 0.69937; GBT top-3 PR 0.7468, 0.7463, 0.7425 (spread 0.0043 < 0.005); `timings.csv` hyperparameter_tuning_all 1501.399 | OK |
| L140 text (11 values) | 0,05; 0,95; 0,01; 0,6982 at 0,34; 0,6879 at 0,30; 0,7166 at 0,38; 0,25; 0,45 | `pz5_threshold_validation.csv`: max val F1 LR 0.698171 (0.34), RF 0.687870 (0.30), GBT 0.716590 (0.38); grid in `pz5_spark.py` L539. The phrase "криві пологі" is a wording issue, see D3 | OK (values); NOTE (wording) |
| L159 | 4,46 с; 3,083 с | `timings.csv` save_best_pipeline 4.46; load_best_pipeline 3.083 | OK |
| L163 | 33 хв; 11 364; 20 036; 4 ядра; 2,10 ГГц; 1954,59 с; 83,869 с | `spark_ui_jobs_top.png` (Uptime 33 min, Completed Jobs 11364); `spark_ui_stages.png` (Completed Stages 20036); `env_versions.csv` (4 logical cores, Xeon 2.10 GHz); `timings.csv` total_script 1954.59, feature_engineering 83.869 | OK; the word "завдання" is wrong, see D1 |
| L171 | 0,105; 0,680; 0,7877 | `pz5_metrics_test.csv` persistence: FPR = 1222/(1222+10445) = 0.105; TPR 0.6801; ROC-AUC 0.7877 | OK |
| T6 (76 values: 63 metrics, 6 times, 7 thresholds) plus caption 15 534 and 0,2489 (2) | All rows of the comparison table | `pz5_metrics_test.csv` (9 rows, all 7 metrics); `timings.csv`/`pz5_model_runtimes.csv` (8,614; 58,739; 43,102); thresholds from `pz5_chosen_thresholds.json`. All cells checked | OK |
| L193 text (23 values) | 0,7115; 0,6202; 0,9010; 0,8032; +0,03; 0,6814; +0,044; 0,877; 0,761; 0,024; 0,042; 0,7511; 0,6272; 0,6998; 58,739 с; 6,633 с; 15 534; 43,102 с; 1,278 с; 8,614 с; 1,527 с; 0,38 | `pz5_metrics_test.csv`; `pz5_model_runtimes.csv`. Differences: 0.7115−0.6814 = 0.030; 0.6202−0.5761 = 0.044; 0.9010−0.8773 = 0.024; 0.8032−0.7608 = 0.042 | OK |
| L197 (7 values) | 80/20; 0,2; 42; 77 667; 15 746; 15 534 | `pz5_spark.py` L609 randomSplit([0.8, 0.2], seed=42); `pz5_random_vs_chrono.csv` n_test 15746 (random), 15534 (chrono) | OK |
| T7 (24 values + threshold 0,5) | Test rows, F1, ROC-AUC, PR-AUC for LR/RF/GBT, random and chronological | `pz5_random_vs_chrono.csv`; `screenshots/txt/10_random_vs_chrono.txt`. All 18 metric cells checked | OK |
| L212 (4 values) | 0,872; 0,901; 0,8680; 0,9010 | `pz5_random_vs_chrono.csv` GBT random ROC 0.87177 → 0,872; GBT chrono 0.90101; `pz5_tuning.csv` GBT CV ROC 0.8680 | OK |
| L222 (12 values) | 0,4828; 0,4116; 2,341; 0,877; 0,1012; 0,0721; 0,0720; 0,0990; 0,0393; 0,138; −1,720; 0,0039; 14:00 UTC | `pz5_feature_importance.csv`: RF 0.482777, GBT 0.411607, LR 2.341099; RF log_rel_range 0.101233; vol_z168 0.072060; log_rv_l0 0.072049; GBT hour_cos 0.099012; hour_sin 0.039270; sum 0.138282; LR log_rv_l0 −1.719993; RF flat_share 0.003899. ROC 0.877 from `pz5_metrics_test.csv`. 14:00 UTC is the PZ2 volume maximum (`pz2_volume_by_hour.csv`, hour 14 = 2212768) | OK (14:00: PZ2) |
| L230 (10 values) | 2166; 2552; 15 534; 1085; 1196; 970; 1467; 497; 970; 1085 | `pz5_metrics_test.csv` with validation thresholds: LR 0.34 FP 1467 FN 1085; RF 0.30 FP 1110 FN 1188; GBT 0.38 FP 970 FN 1196. Errors LR 2552, GBT 2166. 1467 − 970 = 497. Test rows 15 534 | OK |
| T8 (26 values), L236–L242 | Group n, class-1 share, error rate, recall, precision; by regime, transition and year | `pz5_errors_by_current_regime.csv` (n 11682 / 3852; pos 0.1059 / 0.6828; err 0.1010 / 0.2560; recall 0.1770 / 0.9323; precision 0.5748 / 0.7521); `pz5_errors_by_transition.csv` (err 0.8230 / 0.6612 / 0.0260); `pz5_errors_by_year.csv` (2025: 7995, 0.2497, 0.1386, 0.6969, 0.7344; 2026: 7539, 0.2482, 0.1403, 0.6841, 0.7327). Transition counts 1237 and 1222 match the persistence FN and FP in `pz5_metrics_test.csv` | OK |
| L244 (8 values) | 2,6 %; 82,3 %; 0,1770; 808 з 970; 0,1386; 0,1403; 21 міс. | `pz5_errors_by_transition.csv`; `pz5_errors_by_current_regime.csv` fp 808 (cur_high = 1); `pz5_metrics_test.csv` GBT FP 970; 2025-02 → 2026-11 = 21 months | OK |
| L248 (6 values) | 0,0696 (03:00); 0,2411 (12:00); 0,5286; 0,6228; 0,6754 (13:00); 0,1283 | `pz5_errors_by_hour.csv`: hour 3 err 0.06956 (minimum), hour 12 err 0.24111, pos 0.52859, recall 0.62281, hour 13 pos 0.67543, err 0.12828 | OK |
| L248 time "13:30 UTC" (1 value) | 13:30 UTC (US session open) | No project source. External fact: 09:30 ET = 13:30 UTC only in US daylight time, and 14:30 UTC in winter. See D2 | NOT FOUND |
| L252 | 15 хв | Design proposal (the feature window), not a measured result | N/A (not a result) |
| L256 (3 values) | −0,021; 0,527; 0,295 | As L36 and L32 | OK |
| L258 (5 values) | 3,95 %; 77 667; 75; 720; 17 | As L64 and L68; 17 = len(NUM) in `pz5_spark.py` L387–L388 | OK |
| L260 (7 values) | 62 109; 15 534; 0,2478; 0,2489; 24; 80; 20 | As L101–L120 | OK |
| L268 (6 values) | 0,38; 0,7115; 0,9010; 0,8032; +0,03; 0,877 | As L191, L193 | OK |
| L270 | 82,3 % | As L244 | OK |
| Code literals L70–L73, L103–L107 (3 values) | 10000; 1; 0,8 | `pz5_spark.py` L376 (percentile accuracy 10000), L382 (lead 1), L404 (`int(N*0.8)`) | OK |

Totals: about 459 numeric values checked (dates, times and counts included). 0 MISMATCH. 1 NOT FOUND (13:30 UTC, external). 2 NOTE items (wording). 1 source-path defect (D4). The remaining items are OK.

## Defect list

- **D1 (wording, L163):** "11 364 завдання Spark" should read "11 364 jobs (завдання Spark)". The value is correct. `spark_ui_jobs_top.png` says "Completed Jobs: 11364". The 20 036 stages match `spark_ui_stages.png`. Tasks are a different count and were not reported.
- **D2 (NOT FOUND, L248):** "Відкриття американської сесії о 13:30 UTC". No project source. The time holds only during US daylight time (9:30 ET = 13:30 UTC). In winter it is 14:30 UTC. Fix: add a source, or write "близько 13:30–14:30 UTC залежно від літнього часу США".
- **D3 (wording, L140):** "Криві F1 пологі в діапазоні 0,25–0,45" is overstated. Drop from the peak at 0,45 is LR −0,031 (0,6671), RF −0,043 (0,6450), GBT −0,027 (0,6899). At 0,25 it is LR −0,020, RF −0,007, GBT −0,021. Fix: "F1 падає не більше ніж на 0,04 у діапазоні 0,25–0,45". Source: `pz5_threshold_validation.csv`.
- **D4 (provenance, not a number):** PZ2 values (7 816 537; 98,1 %; 118,11; 14:00 UTC) exist only under `pz2/_old_run/results/`. `pz2/results/` contains only an empty `logs/` folder. `pz5/code/pz5_spark.py` L291 reads `pz2/results/pz2_hourly_features.parquet`, which is absent, so the pipeline does not run as written. The file exists at `pz2/_old_run/results/pz2_hourly_features.parquet`. Fix: point SRC to `_old_run` or restore the file.

No numeric MISMATCH was found in the main body.
