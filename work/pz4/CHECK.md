# Перевірка повноти ПЗ4 (MongoDB)

Джерело вимог: `work/cards/pz4.md` (сирий текст `work/cards/raw/pz4.txt`). Шлях до результатів: `work/pz4/`.

## Таблиця: завдання → докази → статус

| № | Завдання / крок методички | Докази | Статус |
|---|---|---|---|
| 1 | Встановити MongoDB Community, PyMongo | `results/env_versions.csv` (mongod v8.3.7, mongosh 2.14.1, pymongo 4.19.0) | OK |
| 1 | Встановити MongoDB Compass | `env_versions.csv`: «не встановлювався (GUI недоступний)»; жодного артефакту | GAP-1 |
| 2 (порядок 2–3) | Запуск сервера, підключення (PyMongo або Compass) | `screenshots/term_s01_show_dbs.png`; `code/pz4_common.py` (MongoClient, DB_NAME=`bigdata_db`) | OK |
| 2 (порядок 4) | Створити базу `bigdata_db` | `term_s01_show_dbs.png` (bigdata_db 761 MiB); `code/pz4_common.py:13`, `code/pz4_load.py` | OK |
| 2 (порядок 5) | Створити колекції | `term_s02_collections.png` (7 колекцій + `candles_1m_ts` як time-series); `code/pz4_load.py:31`; `results/pz4_storage.csv` | OK |
| 3 | Імпорт, кількість документів | `results/pz4_load_throughput.csv`; `results/pz4_storage.csv` (count); `term_s03_counts.png`; `results/load.log`; `term_s22_load_log.png` | OK (скрін. див. GAP-5) |
| 3 | Структура документів | `results/pz4_sample_documents.json`; `term_s04_sample_1m.png`, `term_s05_sample_spark.png` | OK (скрін. див. GAP-5) |
| 3 | Типи полів | `results/pz4_field_types.csv` (bson_type по всіх полях); `term_s06_types.png` | OK (скрін. див. GAP-5) |
| 3 | Пропущені значення | `results/pz4_missing_values.csv`; `results/pz4_spark_nan_to_null.csv`; `term_s07_missing.png` | OK (скрін. див. GAP-5) |
| 4 | ≥5 запитів (пошук, фільтр, сортування, limit, projection) | 8 запитів `results/pz4_q1…q8*.csv`, `pz4_queries_summary.csv`; `code/pz4_queries.py`; `term_s08`–`s11` (q1, q2, q3, q5). Покриття: q3 діапазон + sort + limit, q8 `$in` + `$and`, projection `_id:0` у всіх | OK |
| 5 | ≥3 конвеєри агрегації | A1–A5 (`results/pz4_agg_*.csv`), ANOM `$lookup` (`pz4_anom_lookup_*.csv`); `code/pz4_aggs.py`; перевірка A1 проти pandas `pz4_agg_A1_validation.csv`; `term_s12`–`s16`; `fig_monthly_volume.png`, `fig_volume_by_hour.png`, `fig_yearly_volatility.png` | OK (s16 — anomaly v1, див. примітку) |
| 6 | ≥2 індекси | `results/pz4_index_sizes.csv`; `pz4_index_params.json`; індекси: candles_1m `ts_1`, `volume_-1`; candles_1d `date_1`, `year_1_volume_-1`; candles_1h `ts_1`; buckets/spark `date_1` | OK |
| 6 | explain executionStats до/після | `results/explain/E1…E5b_*_before|after.json` (16 файлів) | OK |
| 6 | Таблиця (час, docs examined, COLLSCAN vs IXSCAN) | `results/pz4_index_comparison.csv` (executionTimeMillis, wall_ms, totalDocsExamined, winning_plan); `term_s23_index_table.png` (скрін. див. GAP-5); `fig_idx_time.png`, `fig_idx_docs.png` | OK (скрін. див. GAP-5) |
| 6 | Чесне порівняння (той самий запит, медіана) | `code/pz4_common.py` (warm-up + 5 вимірювань, медіана); `code/pz4_indexes.py` (docstring умов); `explain_ms_runs`/`wall_ms_runs` у CSV | OK |
| 7 | Імпорт результатів ПЗ2 (Spark) | колекція `spark_daily_features` (5 429 док.); `code/pz4_load.py:88` читає `work/pz2/results/pz2_daily_features.parquet`, якого немає (є лише в `pz2/_old_run/results/`) | GAP-2 |
| 7 | Імпорт результатів ПЗ3 (аномалії) | колекція `anomalies` із `pz3_anomalies_v1.csv` (108 119 док.); `pz4_anom_*.csv`; `explain/ANOM*`; `fig_anomalies_per_year.png` | OK з примічанням (v1, див. нижче) |
| 7 | Аналітичні запити над імпортованими результатами (порядок 13) | q5, q6, q8, A3, A5, ANOM lookups; `pz4_anom_days_vs_rest.csv` | OK |
| 8 | Аналіз можливостей MongoDB для великих даних | Лише `results/INDEX.md` (відповідність завдань) і таблиці. Текстового аналізу немає | GAP-3 |
| 14 | Проаналізувати результати | Письмового аналізу немає (див. GAP-3) | GAP-3 |
| 15 | Підготувати звіт | `pz4/report.md` відсутній (очікується за RULES.md) | GAP-4 |
| — | `results/timings.csv` | Є: 12 етапів (завантаження, агрегації, аномалії) з примітками | OK |
| — | `results/env_versions.csv` | Є: сервер, mongosh, pymongo, Python, pandas, ОС, CPU, RAM 15,7 ГіБ, WiredTiger, Compass | OK |
| — | Коментарі українською у коді | `code/pz4_load.py`, `pz4_aggs.py`, `pz4_indexes.py`, `pz4_anomalies.py`, `pz4_schema.py`, `pz4_common.py`, `pz4_figures.py`, `pz4_screens_txt.py`, `make_screens.py` мають українські коментарі. `pz4_queries.py` має український docstring, але жодного коментаря в коді | OK, крім GAP-6 |
| — | PNG: читабельність, українські підписи | 7 графіків `figures/*.png`: українські підписи, читабельні. 23 термінальні PNG відкриваються, читабельні | OK |

## Перевірка скриншотів: заголовок «$» має бути реальною командою

| Файл | Заголовок | Результат |
|---|---|---|
| s01, s02, s08, s09, s10, s11, s12, s13, s14, s15, s16 | реальна команда | OK (11) |
| s17 | реальна команда, але mongosh виводить лише останній `getIndexes()` (candles_1d); індекси candles_1m на скрині не видно | GAP-5 (часткове) |
| s04, s05, s21 | команда без обгортки `printjson` / спрощений вигляд | GAP-5 (дрібне) |
| s03 | скорочено `forEach(n => print(n, db[n].countDocuments({})))`, фактично `…` | GAP-5 |
| s06, s07 | скорочено `...` наприкінці | GAP-5 |
| s18, s19, s20 | псевдокоманда `q.hint(...) vs q.explain(...)`, не виконувана; s20 з `{...2021}` | GAP-5 |
| s22 | `python3 code/pz4_load.py (фрагмент виводу)`: відфільтровані рядки журналу, не вивід команди | GAP-5 |
| s23 | `python3 -c "print(... [[...]])"`: псевдокоманда; таблиця створена `pz4_screens_txt.py` | GAP-5 |

## GAP-список

- GAP-1 (Завдання 1): MongoDB Compass не встановлено (`env_versions.csv`). Зробити: встановити Compass, зробити скрін підключення або явно зазначити, що GUI недоступний. PyMongo та mongosh при цьому є.
- GAP-2 (Завдання 7): джерело імпорту `pz2_daily_features.parquet` відсутнє в `work/pz2/results/`; `code/pz4_load.py:88` не запуститься повторно. Зробити: відновити файл з `pz2/_old_run/results/` або змінити шлях, перевірити, що він ідентичний тому, що імпортовано (5 429 рядків).
- GAP-3 (Завдання 8, крок 14): немає письмового аналізу можливостей MongoDB для великих даних (ми маємо лише таблиці та INDEX.md). Зробити: написати аналіз на основі `pz4_index_comparison.csv`, `pz4_storage.csv`, `fig_storage.png`.
- GAP-4 (Крок 15): немає `pz4/report.md` (звіт за RULES.md). Зробити після закриття GAP-1–3.
- GAP-5 (скріншоти): 8 термінальних PNG мають заголовок, що не є реальною командою (s03, s06, s07, s18, s19, s20, s22, s23), 3 мають спрощений заголовок (s04, s05, s21), а s17 не показує індекси candles_1m. Зробити: перегенерувати через `code/pz4_screens_txt.py` так, щоб заголовок збігався з виконаним рядком (або вивести команди у shell-обгортці), і зробити s17 з окремими викликами для кожної колекції; потім `make_screens.py`.
- GAP-6 (код): `code/pz4_queries.py` без українських коментарів усередині коду (лише docstring). Зробити: додати коментарі до `run()` та до кожного запиту.

## Примітки (не GAP)

- Anomaly v1 (відомо, перезапуск потрібен, не рахується як GAP): `anomalies` завантажено з `pz3_anomalies_v1.csv` (108 119 док.), бо `pz3_anomalies.csv` відсутній. Залежні артефакти: `results/pz4_anom_*.csv`, `explain/ANOM1_*`, `explain/ANOM2_*`, `pz4_anom_index_comparison.csv`, `figures/fig_anomalies_per_year.png`, `screenshots/term_s16_agg_lookup.png`, `term_s20_explain_anom.png`.
- Порівняння A1 з індексом `ts_1` та без нього (`pz4_agg_A1_timing.csv`, 116,65 с проти 119,12 с) має по одному запуску, без медіани. Це не порівняння запиту з Завдання 6 (там медіана з 5 запусків), тому не GAP. Варто зазначити це у звіті.
- Сторонню БД `t` видно у `show dbs` (`term_s01_show_dbs.png`). Це не впливає на завдання.
