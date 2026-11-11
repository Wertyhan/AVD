# ПЗ2 – індекс результатів (завдання з методички -> файли)

Код: `code/pz2_spark.py` (повний конвеєр, ~5,5 хв), `code/make_figures.py`, `code/make_screens.py` (рендер реального виводу в PNG).
Запуск: `python3 -I pz2_spark.py && python3 -I make_figures.py && python3 -I make_screens.py`. Seed 42. Spark 4.3.0, local[*], 4 ядра, driver 8 ГБ (див. env_versions.csv).
Вихідні дані: оригінальний CSV 7 816 537 рядків читається Spark-ом напряму.

| Завдання | Результати (results/) | Скріншоти / рисунки |
|---|---|---|
| 1. Підготовка середовища, SparkSession | env_versions.csv (додано Jupyter Notebook 7.6.5, JupyterLab 4.6.6, ipykernel) | screenshots/term_01_env.png, term_13_java_version.png, term_14_python_version.png, term_15_pip_show_pyspark.png, term_16_jupyter_version.png, term_17_pyspark_smoke.png (smoke-тест PySpark, code/smoke_test.py), spark_ui_environment.png, spark_ui_executors.png |
| 2. Завантаження: кількість записів, атрибутів, назви, типи, пропуски | pz2_load_summary.csv, pz2_schema_nulls.csv; timings.csv (inferSchema vs явна схема) | term_02_schema.png, term_03_nulls.png |
| 3. Попередня обробка: пропуски, типи, дублікати, похідні (UTC datetime, log_ret, range, rel_range, year/month/hour, vol60) | pz2_cleaning.csv, pz2_describe.csv, pz2_flat_summary.csv (16,79 % flat), pz2_yearly_volume_flat.csv | term_06_derived.png, fig09_flat_share.png |
| Ліниві обчислення: трансформації vs дії, explain() | pz2_lazy_vs_action.csv, logs/explain_full.txt | term_04_lazy.png, spark_ui_jobs.png, spark_ui_stages.png |
| Кешування: той самий запит без cache() і з ним | pz2_cache_experiment.csv, pz2_cache_summary.csv | term_05_cache.png, spark_ui_storage.png, fig08_timings.png |
| 4. Аналітика: топ-10 волатильних днів (rv і High-Low/Open), обсяг за годиною (UTC) та роком, асиметрія й ексцес 1 хв/1 год/1 доба, добовий і погодинний агрегат | pz2_top10_rv.csv, pz2_top10_hl_rel.csv, pz2_top10_rv_active.csv, pz2_top10_hl_rel_active.csv, pz2_volume_by_hour.csv, pz2_volume_by_year.csv, pz2_skew_kurtosis.csv, pz2_hist_ret_1m_active.csv, pz2_daily_features.parquet/.csv (5429 рядків), pz2_hourly_features.parquet (130276 рядків) | term_07_top_days.png, term_08_volume.png, term_09_moments.png, fig02-fig06 |
| 5. Spark SQL (6 запитів над temp views btc, btc_daily) | pz2_sql_q1..q6_*.csv, pz2_sql_queries.sql | term_10_sql_q*.png, term_11_sql_explain.png, spark_ui_sql.png (список SQL-запитів), spark_ui_sql_detail.png (DAG одного запиту),  fig07_dow_hour_heatmap.png |
| 6. Порівняння MapReduce і Spark (вимірювання) | pz2_memory.csv (пік RSS, code/measure_memory.py: pandas 744/882 МБ, JVM драйвера Spark 1279/1478 МБ для рік/кількість/обсяг і денного OHLC), pz2_engine_comparison.csv, pz2_engine_comparison_summary.csv (Spark vs pandas; Hadoop – рядки-заповнювачі [ЗАПОВНИТИ ПІСЛЯ ЗАПУСКУ]) | term_12_engines.png, fig08_timings.png |
| 7. Візуалізація | – | figures/fig01_close_log, fig02_returns_vs_normal, fig03_volume_by_hour, fig04_volatility, fig05_yearly_volume, fig06_top_days, fig07_dow_hour_heatmap, fig08_timings, fig09_flat_share (.png, 150 dpi) |
| Часи кроків | timings.csv | |

Примітки до чесної інтерпретації:
- Схема inferSchema визначає Timestamp як int (значення < 2^31), у явній схемі використано bigint.
- Матеріалізація кешу (MEMORY_AND_DISK) тривала близько 80 с; запит прискорюється ~7,7 разів (18,3 с -> 2,4 с), окупність настає після ~5 повторів.
- Кількість `rv` у 2012 р. спотворена: 94,95 % хвилин flat, тому top-10 подано і для днів з flat_share < 0,5.
- Spark на цьому одному 4-ядерному комп'ютері лише трохи швидший за pandas (JVM, шафл); перевага з'являється на кешованих даних. Hadoop-час треба вписати з Colab.
- Скріншоти Spark UI зроблено наприкінці скрипта, поки сесія жива (jobs/stages/storage/SQL/executors/environment). Вкладку SQL і деталь запиту (DAG) перезнято окремим скриптом code/spark_ui_sql_shots.py (3 запити з pz2_sql_queries.sql, wait_for_selector + 3 с); у колонці Description видно ім'я цього скрипта. Пам'ять виміряно окремими процесами, час у timings.csv не змінено.
- Скріншоти term_*.png відрендерено з файлів screenshots/txt/*.txt, що записані з реального stdout скрипта; заголовок "$ ..." показує фрагмент коду, який виконано в pz2_spark.py.
