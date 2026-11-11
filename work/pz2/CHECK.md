# ПЗ2 – перевірка повноти

Перевірено: картка `/home/user/AVD/work/cards/pz2.md`, сирий текст `/home/user/AVD/work/cards/raw/pz2.txt`, правила `/home/user/AVD/work/RULES.md`, вихід у `/home/user/AVD/work/pz2/`. Усі 32 PNG (9 рисунків, 23 скріншоти) відкрито і переглянуто; файли непорожні та валідні PNG.

## Таблиця 1. Завдання і кроки методички → докази → статус

| Завдання / крок | Докази (файли) | Статус |
|---|---|---|
| Завдання 1. Підготовка середовища (Python, Spark, PySpark, Jupyter або VS Code) | results/env_versions.csv (Java 21, Spark/PySpark 4.3.0, Python 3.13.18); screenshots/term_01_env.png; screenshots/spark_ui_environment.png. Перевірки Jupyter Notebook або VS Code немає ні в коді, ні в результатах | GAP |
| Завдання 2. Завантаження: записи, атрибути, назви, типи, пропуски | results/pz2_load_summary.csv (7 816 537 рядків, 6 стовпців); results/pz2_schema_nulls.csv (типи, 0 null і NaN); screenshots/term_02_schema.png; screenshots/term_03_nulls.png; results/timings.csv (inferSchema і явна схема) | OK |
| Завдання 3. Попередня обробка: пропуски, типи, дублікати, похідні атрибути | results/pz2_cleaning.csv (dropna, dropDuplicates: 0 видалено); results/pz2_describe.csv; results/pz2_flat_summary.csv (16,79 % flat); results/pz2_yearly_volume_flat.csv; screenshots/term_06_derived.png; figures/fig09_flat_share.png | OK |
| Завдання 4. Фільтрація, групування, сортування, агрегація, статистика | Фільтрація: results/pz2_top10_*_active.csv, pz2_sql_q2 (flat_share < 0,5). Групування та агрегація: results/pz2_volume_by_hour.csv, pz2_volume_by_year.csv, pz2_daily_features.csv, pz2_hourly_features.parquet. Сортування: results/pz2_top10_rv.csv, pz2_top10_hl_rel.csv. Статистика: results/pz2_skew_kurtosis.csv, pz2_hist_ret_1m_active.csv; screenshots/term_07_top_days.png, term_08_volume.png, term_09_moments.png; figures/fig02–fig06 | OK |
| Завдання 5. Тимчасове представлення і не менше 3 SQL-запитів | Представлення btc і btc_daily: code/pz2_spark.py:324–325. Запити: results/pz2_sql_queries.sql (6 запитів) і results/pz2_sql_q1…q6_*.csv; screenshots/term_10_sql_q1…q6_*.png; screenshots/term_11_sql_explain.png; figures/fig07_dow_hour_heatmap.png. Скріншот Spark UI SQL (spark_ui_sql.png) порожній, див. таблицю 2 | OK |
| Завдання 6. Порівняння MapReduce і Spark за моделлю, пам'яттю, продуктивністю, зручністю, повторним використанням | Виміряно лише Spark проти pandas: results/pz2_engine_comparison.csv, pz2_engine_comparison_summary.csv, results/timings.csv; screenshots/term_12_engines.png; figures/fig08_timings.png. Hadoop MapReduce не виміряно (рядки «[ЗАПОВНИТИ ПІСЛЯ ЗАПУСКУ]»). Немає вимірювання пам'яті. Немає текстового порівняння за п'ятьма критеріями. Ефект кешу (results/pz2_cache_summary.csv, figures/fig08) частково покриває повторне використання | GAP |
| Завдання 7. Не менше двох графіків | figures/fig01_close_log.png … fig09_flat_share.png (9 рисунків, перевірено візуально) | OK |
| Крок 1. Підготувати середовище Spark | results/env_versions.csv; screenshots/term_01_env.png | OK |
| Крок 2. Завантажити датасет з ПЗ1 | results/pz2_load_summary.csv; results/logs/explain_full.txt (шлях btcusd_1-min_data.csv, Scan csv) | OK |
| Крок 3. Створити SparkSession | code/pz2_spark.py:52–60 (master local[*], driver 8g, shuffle.partitions 16, timeZone UTC); screenshots/spark_ui_environment.png; results/env_versions.csv (spark.* параметри) | OK |
| Крок 4. Завантажити дані у DataFrame | code/pz2_spark.py (read.csv, явна схема); screenshots/term_02_schema.png; results/timings.csv | OK |
| Крок 5. Дослідити структуру даних | screenshots/term_02_schema.png; results/pz2_describe.csv; results/pz2_schema_nulls.csv | OK |
| Крок 6. Попередня обробка | results/pz2_cleaning.csv; screenshots/term_06_derived.png; code/pz2_spark.py:127–140 | OK |
| Крок 7. Аналітичні операції | screenshots/term_07_top_days.png, term_08_volume.png, term_09_moments.png; results/pz2_top10_*.csv, pz2_volume_by_*.csv, pz2_skew_kurtosis.csv | OK |
| Крок 8. SQL-запити | results/pz2_sql_queries.sql; results/pz2_sql_q1…q6_*.csv; screenshots/term_10_sql_q1…q6_*.png | OK |
| Крок 9. Отримати результати | results/*.csv (усі числа звітів); results/INDEX.md (відповідність завдання → файли) | OK |
| Крок 10. Побудувати графіки | figures/fig01…fig09 (150 dpi) | OK |
| Крок 11. Проаналізувати результати | Письмового аналізу немає: відсутній report.md (у ньому мав бути розділ «Результати» з інтерпретацією кожного рисунка й таблиці) | GAP |
| Крок 12. Підготувати звіт | Відсутні: report.md, build/ (docx/pdf), defense.md. Є лише results/INDEX.md і CHECK.md | GAP |
| Вимоги до звіту (титульна сторінка, мета, теорія, лістинг з коментарями, висновки) | Лістинг є у code/pz2_spark.py, make_figures.py, make_screens.py. Звіту, титулу, теорії, розділу «Висновки» і Додатка А немає | GAP |

Підсумок за таблицею 1: OK 15, GAP 5 (завдання 1 і 6; кроки 11 і 12; вимоги до звіту).

## Таблиця 2. Правила RULES.md

| Вимога | Докази | Статус |
|---|---|---|
| Не менше 8 рисунків, включно зі скріншотами | 9 рисунків (figures/) і 23 скріншоти (screenshots/) | OK |
| Дані для не менше 5 таблиць | 30+ CSV у results/, зокрема load, cleaning, describe, top10, volume, sql, engines | OK |
| Не менше 3 запитів Spark SQL | 6 запитів: results/pz2_sql_queries.sql, pz2_sql_q1…q6 | OK |
| results/timings.csv | Є, 35 кроків з часом і примітками | OK |
| results/env_versions.csv | Є: Java, Spark, PySpark, Python, pandas, CPU (4 ядра), RAM (15 ГБ), spark.* | OK |
| Seed 42 там, де є випадковість | code/pz2_spark.py:9–10 (SEED = 42, np.random.seed); code/make_figures.py:7 | OK |
| Коментарі в коді українською | pz2_spark.py: 26 з 27 рядків коментарів містять кирилицю (решта – заголовок кодування); make_figures.py: 9 з 10 | OK |
| Скріншоти реальні, без редагування виводу | term_*.png відрендерено з screenshots/txt/*.txt (stdout скрипта). spark_ui_*.png знято з UI. Але spark_ui_sql.png показує лише спінер «завантаження», без SQL-запитів | GAP |
| Без заповнювачів у результатах | results/timings.csv, pz2_engine_comparison.csv, screenshots/term_12_engines.png, figures/fig08_timings.png: Hadoop-рядки лишилися «[ЗАПОВНИТИ ПІСЛЯ ЗАПУСКУ]» | GAP |
| report.md, defense.md, build/ (docx/pdf) | Відсутні | GAP |

Підсумок за таблицею 2: OK 7, GAP 3.

## Перелік GAP (що саме відсутнє)

1. Завдання 1 / Вимоги: немає перевірки Jupyter Notebook або VS Code (ні в env_versions.csv, ні в скріншотах, ні в коді). Потрібно додати версію IDE і її запуск, або зафіксувати, що середовище перевірено через CLI.
2. Завдання 6: немає вимірювання Hadoop MapReduce. Потрібно запустити ті самі дві задачі (рік/кількість/обсяг і денний OHLC) на Hadoop Streaming (наприклад, Colab із ПЗ1), заповнити рядки hadoop_streaming у results/pz2_engine_comparison.csv, results/timings.csv, перегенерувати term_12_engines.png і fig08_timings.png. Заголовок fig08 зараз містить «Hadoop – після ПЗ1».
3. Завдання 6: немає вимірювання використання оперативної пам'яті (ні Spark, ні pandas, ні MapReduce). Немає текстового порівняння за п'ятьма критеріями: модель обчислень, пам'ять, продуктивність, зручність програмування, повторне використання.
4. Скріншот screenshots/spark_ui_sql.png порожній (спінер). Потрібно перезняти вкладку SQL / DataFrame з виконаними запитами, поки сесія жива (код скрипта робить знімки в кінці), або видалити файл і вказати це в INDEX.md.
5. Крок 11 «Проаналізувати результати» і крок 12 «Підготувати звіт»: немає report.md з розділами «Результати» (кожен рисунок і таблиця згадані в тексті до появи, з висновком після), «Висновки» за планом методички, Додаток А з лістингом, титульною сторінкою і теорією. Немає defense.md і build/ (docx/pdf).
