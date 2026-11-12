# ПЗ2 – індекс результатів (завдання з методички -> файли)

Усі результати, рисунки і скріншоти створено одним запуском поточного коду (після виправлень за REVIEW.md); попередній запуск збережено в `_old_run/` (не для звіту).

Запуск: `bash code/run_all.sh` (з каталогу pz2), що послідовно виконує `code/env_shots.py`, `code/pz2_spark.py` (повний конвеєр, Spark UI-знімки робить цей самий скрипт, поки SparkSession жива), `code/measure_memory.py` (пам'ять), `code/make_figures.py` (стиль 6,3x3,4 дюйма, dpi 200, десяткова кома), `code/make_screens.py`. Тривалість: див. `logs/run_all_runtime.txt` і `TOTAL_script_runtime` у `timings.csv`. Seed 42. Версії та конфігурація: `env_versions.csv`. Допоміжний `code/smoke_test.py` викликає env_shots.py. Порівняння з попереднім запуском: `CHANGES_vs_old_run.md`.

| Завдання | Результати (results/) | Скріншоти / рисунки |
|---|---|---|
| 1. Середовище, SparkSession | env_versions.csv | screenshots/term_01_env, term_13_java_version, term_14_python_version, term_15_pip_show_pyspark, term_16_jupyter_version, term_17_pyspark_smoke, spark_ui_environment, spark_ui_executors |
| 2. Завантаження: записи, атрибути, поля, типи, пропуски; inferSchema проти явної схеми | pz2_load_summary.csv, pz2_schema_nulls.csv, pz2_schema_experiment.csv, pz2_schema_experiment_summary.csv | term_02_schema, term_02a_schema_experiment, term_03_nulls |
| 3. Попередня обробка (дублікати, типи, похідні; вікна partitionBy(year, month)) | pz2_cleaning.csv, pz2_describe.csv, pz2_flat_summary.csv, pz2_yearly_volume_flat.csv, pz2_window_boundary.csv (крайовий ефект вікон), pz2_partition_sizes.csv | term_06_derived, figures/fig10_flat_share |
| Ліниві обчислення, explain() (C1) | pz2_lazy_vs_action.csv, pz2_lazy_vs_action_summary.csv, logs/explain_count.txt, logs/explain_sum_vol60.txt, logs/explain_full.txt | term_04_lazy, spark_ui_jobs, spark_ui_stages, spark_ui_sql_detail (DAG повного проходу з Window), fig11_lazy_actions |
| Кешування, розділи кешу (M4) | pz2_cache_experiment.csv, pz2_cache_summary.csv, pz2_cache_partitions.csv | term_05_cache, spark_ui_storage, fig09_cache |
| 4. Аналітика: топ-10 діб, обсяг за годиною/роком, моменти, стійкість моментів (M7), rv після 2023 (C3) | pz2_top10_rv*.csv, pz2_top10_hl_rel*.csv, pz2_volume_by_hour.csv, pz2_volume_by_year.csv, pz2_skew_kurtosis.csv, pz2_top10_abs_minute_returns.csv, pz2_rv_after2023.csv, pz2_hist_ret_1m_active.csv, pz2_daily_features.parquet/.csv, pz2_hourly_features.parquet | term_07_top_days, term_08_volume, term_09_moments, fig01-fig06 |
| 5. Spark SQL (Q1-Q7; Q7 = усі доби зі сплеском обсягу, M6) | pz2_sql_q1..q7_*.csv, pz2_sql_q7_summary.csv, pz2_sql_queries.sql | term_10_sql_q1..q7, term_11_sql_explain, spark_ui_sql, spark_ui_sql_detail_q1, fig07_dow_hour_heatmap, fig12_volume_spikes |
| 6. MapReduce vs Spark (вимірювання Spark/pandas, чергування S,P; M8) | pz2_engine_comparison.csv, pz2_engine_comparison_summary.csv, pz2_engine_equality.csv (повна звірка), pz2_memory.csv, pz2_memory_runs.csv (code/measure_memory.py) | term_12_engines, fig08_engines |
| 7. Візуалізація | – | figures/fig01-fig12 (12 рисунків) |
| Часи кроків | timings.csv | |

Hadoop Streaming / Hive / pandas / Spark наскрізно (зі стартом процесу): окремий бенчмарк `bench/results/` (bench_summary.csv, bench_method.md); копія підсумку – `engine_benchmark.csv`, рисунок – `figures/fig_bench.png`.

Примітки до інтерпретації:
- Вікна рахуються з `partitionBy(year, month)`: перший рядок кожного місяця має `log_ret = NULL`, перші 59 хв місяця мають менше 60 точок у `vol60` (кількості в pz2_window_boundary.csv). Глобальне вікно зводило б усі 7,8 млн рядків в один розділ.
- `spark.sql.adaptive.coalescePartitions.enabled=false`, щоб кеш мав усі 16 розділів.
- `count()` не обчислює вікон (ColumnPruning); повний прохід вимірюється через `write.format("noop")` і `agg(sum(vol60), sum(log_ret))`.
- Порівняння рушіїв: час Spark включає `toPandas()`, pandas працює в одному потоці; запуски Spark і pandas чергувалися в сесії без кешу, рядки «spark_cached_df» виміряно після кешування.
- Пікова пам'ять RSS JVM відображає ліниве звільнення пам'яті збирачем сміття, а не мінімальну потребу (див. рядки idle і driver 2g у pz2_memory.csv).
- Скріншоти терміналів: заголовок `$ python code/pz2_spark.py`, приглушений рядок `# фрагмент виводу: ...`, далі справжній stdout; Spark UI – знімки області таблиці або графа (viewport 1280x800, масштаб 2).

- `timings.csv` і `pz2_engine_comparison.csv` після запуску один раз постобробляли: вилучено два рядки-заповнювачі Hadoop (`hadoop_streaming`). Поточний `code/pz2_spark.py` таких рядків уже не пише (Hadoop вимірює окремий бенчмарк), тому код і файли збігаються; числа не змінилися.
- Рисунки 4 і 5 звіту (`spark_ui_sql_detail_3col.png`, `spark_ui_storage_cols.png`) створює `code/crop_ui.py` зі знімків `spark_ui_sql_detail.png` і `spark_ui_storage.png`; результат піксель-в-піксель збігається з файлами, використаними у звіті.
