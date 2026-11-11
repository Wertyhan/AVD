# PZ4 (MongoDB) – відповідність завдань методички та доказів
Середовище: results/env_versions.csv; часи: results/timings.csv. Код: code/ (запуск: pz4_load, pz4_schema, pz4_indexes, pz4_queries, pz4_aggs, pz4_anomalies, pz4_figures, pz4_screens_txt + make_screens). mongod 8.3.7 локально, mongosh 2.14.1 (npm), pymongo 4.19.0. Compass не встановлювався (GUI недоступний).

| Завдання | Докази |
|---|---|
| 1 Підготовка MongoDB | results/env_versions.csv; screenshots/term_s01_show_dbs.png |
| 2 Створення БД і колекцій | term_s01, s02_collections, s03_counts; code/pz4_load.py |
| 3 Імпорт, кількість, структура, типи, пропуски | results/pz4_load_throughput.csv (insert_many, батч 20000), pz4_storage.csv, pz4_field_types.csv, pz4_missing_values.csv, pz4_spark_nan_to_null.csv, pz4_sample_documents.json; term_s22_load_log, s04_sample_1m, s05_sample_spark, s06_types, s07_missing; fig_storage.png (плоска vs time-series vs bucket) |
| 4 Запити (8) | results/pz4_q1..q8*.csv, pz4_queries_summary.csv; term_s08_q1, s09_q2, s10_q3, s11_q5 |
| 5 Агрегації | results/pz4_agg_*.csv (місячний OHLC + перевірка проти pandas pz4_agg_A1_validation.csv, обсяг за годиною, річна волатильність, $bucket, річний обсяг), pz4_agg_A1_timing.csv, explain/A1_*; pz4_anom_lookup_*.csv ($lookup); term_s12..s16; fig_monthly_volume, fig_volume_by_hour, fig_yearly_volatility |
| 6 Індекси до/після | results/pz4_index_comparison.csv, pz4_anom_index_comparison.csv, pz4_index_sizes.csv, explain/*_before|after.json; term_s17_getindexes, s18..s20 (explain), s23_index_table; fig_idx_time, fig_idx_docs |
| 7 Імпорт результатів Spark/ПЗ3 | колекції spark_daily_features (ПЗ2), anomalies (ПЗ3); pz4_anom_*.csv; code/pz4_anomalies.py (перезапускний); fig_anomalies_per_year |
| 8 Аналіз | на основі таблиць вище (див. примітки) |

Примітки: (1) anomalies завантажено з pz3_anomalies_v1.csv (108 119 документів), бо pz3_anomalies.csv на момент запуску був відсутній (перегенерація). Перезапуск: `python3 code/pz4_anomalies.py` (за замовчуванням читає pz3_anomalies.csv); потім pz4_figures.py та pz4_screens_txt.py+make_screens.py (s16, s17, s20 залежать від anomalies). (2) Скріншоти s18-s20: «до» = hint({$natural:1}) (примусовий COLLSCAN), «після» = план за замовчуванням; таблиці results/*.csv отримано видаленням/створенням індексу. (3) Агрегація A1 з індексом ts_1 (116,7 с) та без (119,1 с) практично не відрізняється: домінує $group по 7,8 млн документів, а не сортування. (4) Для E1/E2 "до" містить SORT, бо сортування за ts без індексу. (5) У show dbs присутня стороння БД "t" з тестів середовища.
