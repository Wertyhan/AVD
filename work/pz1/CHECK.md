# CHECK ПЗ1 (HDFS, MapReduce, Hive): перевірка повноти

Перевірено: картка `work/cards/pzN.md`, сирий текст `work/cards/raw/pz1.txt`, правила `work/RULES.md`, каталог `work/pz1/` (code, results, figures, screenshots, logs). Відкрито і переглянуто всі 53 PNG (30 терміналь, 17 веб-UI, 6 рисунків): усі непорожні та читабельні.

## Завдання методички → докази → статус

| # | Завдання / крок методички | Докази | Статус |
|---|---|---|---|
| П1 | Завантажити та встановити Hadoop 3.x | logs/01_install_hadoop.log (Hadoop 3.3.6), versions.txt, env_versions.csv | OK |
| П2 | Налаштувати pseudo-distributed | screenshots/04 (getconf: fs.defaultFS=hdfs://localhost:9000, dfs.replication=1), 03 (dfsadmin -report, 1 live DataNode), results/hdfs_cluster_config.csv. Файли core-site/hdfs-site не скріншотились | OK |
| П3 | Запустити HDFS і YARN | screenshots/01 і 28 (jps), 02 і 05 (yarn node -list), logs/02_daemons.log. Демони запущені через `--daemon start`, а не start-dfs.sh/start-yarn.sh (еквівалент) | OK |
| 1 | Архітектура Hadoop, принципи HDFS | screenshots/03 (dfsadmin), 04 (getconf), 11 (fsck: блоки, репліки, HEALTHY); results/hdfs_file_info.csv (3 блоки). Письмового пояснення архітектури немає (див. G1) | OK (докази), текст GAP |
| 2 | Установка pseudo-distributed | = П1 і П2 і П3 вище; UI screenshots/31, 32, 35, 37 | OK |
| 3 | Завантажити набір даних у HDFS | screenshots/06 (mkdir, ls -R), 07 (ls -h), 08 (du), 09 (cat | head), 10 (tail), 11 (fsck), 29, 30; UI 33, 34 (Browse Directory); results/hdfs_file_info.csv (396 849 494 B, 7 816 537 рядків, 17 с put). Розбіжність 06 з logs/04 (див. G3) | OK, з застереженням G3 |
| 4a | MapReduce: WordCount | screenshots/12 (run, completed successfully), 13 (ls, _SUCCESS), 14 (top-20 слів); UI 38, 39, 40 (counters); results/wordcount_top50.csv | OK |
| 4b | Перегляд результатів у HDFS | screenshots/13, 14, 29 | OK |
| 4c | Власна Streaming-задача: роки (свічки, обсяг) | code/mapper_year.py, reducer_year.py (з combiner), screenshots/15, 16; UI 44, 45, 46; results/mr_yearly_stats.csv, mr_counters_mr_yearly.csv, рисунки fig_mr_yearly_candles/volume | OK |
| 4d | Власна Streaming-задача: добові OHLC | code/mapper_ohlc.py (in-mapper combining), reducer_ohlc.py (first/max/min/last/sum), screenshots/17, 18; UI 41, 42, 43; results/mr_daily_ohlc_full.csv, mr_daily_ohlc_sample.csv, mr_daily_ohlc_rowcount.csv (5 429 діб), fig_mr_daily_close_log | OK |
| 5a | Встановити Hive та підключити до Hadoop | logs/07_hive_install.log (schematool derby, hive --version 3.1.3), versions.txt | OK |
| 5b | Створити таблицю над даними в HDFS | code/hive_01_create_table.hql (EXTERNAL TABLE, LOCATION /user/root/btc/raw), screenshots/19 (DESCRIBE FORMATTED: EXTERNAL_TABLE, totalSize 396849494), results/hive_count_check.csv (7 816 537 = CSV) | OK |
| 5c | Завантажити дані в таблицю | Окремого LOAD немає: external-таблиця читає каталог HDFS напряму (обґрунтовано в hql). Пояснення має бути в звіті | OK (обґрунтовано), пояснення GAP (G1) |
| 6a | Hive: вибірка | code/hive_02_select.hql, screenshots/20 (SELECT LIMIT 5, WHERE volume>1000 ORDER BY LIMIT 10, COUNT). У виводі є рядок NULL (див. G9) | OK, з G9 |
| 6b | Hive: групування та агрегація (рік) | code/hive_03a_yearly.hql, screenshots/21, results/hive_yearly_stats.csv (2012–2026) | OK |
| 6c | Hive: групування рік+місяць | code/hive_03b_monthly.hql, screenshots/22, results/hive_monthly_stats.csv (179 рядків), fig_hive_monthly_volume | OK |
| 6d | Hive: добова таблиця OHLC (CTAS) | code/hive_04_daily_struct.hql (+ запасний hive_04_daily_window.hql), screenshots/23 (COUNT=5429 і перші 5 діб), results/hive_daily_rowcount.csv, logs/08_hive_daily_*.log, screenshots/27 і 30 (YARN-задачі, warehouse) | OK |
| 6e | Hive: топ-10 діб за обсягом | code/hive_05a_top_volume.hql, screenshots/24, results/hive_top10_volume_days.csv; збігається з Pandas (перевірено) | OK |
| 6f | Hive: топ-10 діб за High-Low | code/hive_05b_top_range.hql, screenshots/25, results/hive_top10_range_days.csv; збігається з Pandas (перевірено). Вивід обрізано (див. G4) | OK, з G4 |
| 7a | Порівняння з Pandas | code/run_pz1.py (крок pandas), bench_pz1.py; results/pandas_*.csv, timings.csv (3 запуски), timing_summary.csv; screenshots/26 з помилковим заголовком (див. G2) | OK, з G2 |
| 7b | Порівняння зі Spark | code/bench_pz1.py (PySpark 4.3.0, local[*]), results/spark_*.csv, timings.csv, crosscheck_spark_vs_pandas.csv (True, True) | OK |
| 7c | Умови вимірювання часу | results/timing_conditions.csv (loadavg 1m=4,06 на 4 vCPU, RAM доступна 9 ГБ), env_versions.csv, INDEX.md (застереження про навантаження) | OK, з G5 |
| 7d | Висновки: переваги, обмеження, порівняння з Pandas/Spark | Цифри є (timing_summary, fig_timing_comparison, fig_top_days). Самого тексту висновків немає: report.md відсутній | GAP (G1) |

## Додаткові вимоги

| Вимога | Докази | Статус |
|---|---|---|
| HDFS-операції (mkdir, put, ls, du, cat, tail, fsck) | screenshots 06–11, 29, 30 | OK |
| WordCount | screenshots 12–14; UI 38–40 | OK |
| Власні Streaming: річна і добова OHLC | code/mapper_year.py, mapper_ohlc.py, reducer_*.py; screenshots 15–18; UI 41–46; CSV | OK |
| Hive: зовнішня таблиця, запити, топ-дні | hql 01–06; screenshots 19–25; CSV hive_* | OK |
| Крос-перевірки | crosscheck_hive_vs_mr_yearly.csv (True), crosscheck_hive_vs_mr_daily.csv (True, 5 429 рядків), crosscheck_mr_hive_vs_pandas.csv (усі True, OHLC без розбіжностей), crosscheck_spark_vs_pandas.csv (True, True). Примітка: hive_vs_pandas_daily перевіряє лише кількість рядків, значення перевірено через MR | OK |
| Таймінги з умовами | timings.csv, timing_summary.csv, timing_conditions.csv | OK, з G5 |
| env_versions.csv | Java 1.8.0_472, Hadoop 3.3.6, Hive 3.1.3, Python 3.13.18, pandas 3.0.5, numpy 2.5.3, pyspark 4.3.0, OS, CPU, RAM 15Gi, nproc 4, дата | OK |
| Українські коментарі в коді | run_pz1.py, mapper/reducer, make_figures.py, hql: українською. bench_pz1.py: 4 розділювальні коментарі англійською | OK, мінорно G7 |
| Скріншоти реальні, веб-UI з контентом | Усі 53 PNG відкрито; UI показує реальні дані (без спінерів і помилок); терміналь має справжні команди, крім 26 | OK, з G2, G3, G4 |
| Рисунки з українськими підписами | 6 рисунків, усі підписи українською, читабельні | OK |

## GAP (список)

G1. Відсутні report.md, defense.md і build/ (PDF/DOCX). Не написано: теорія, пояснення архітектури, пояснення «завантаження даних у таблицю» через external table, висновки з порівнянням з Pandas/Spark, перевагами Hadoop і обмеженнями (завдання 1, 5c, 7d), лістинг у Додатку А. Це головний пробіл. Вимоги до звіту (картка): титул, мета, теорія, результати, висновки.

G2. screenshots/26_pandas_yearly.png: заголовок `$ python: pandas yearly (count, volume)` не є командою, яка створила вивід (run_pz1.py, рядок shot(...)). Потрібно перерендерити зі справжньою командою (наприклад, запуск скрипта через python3) або з реальним виводом Pandas.

G3. screenshots/06_hdfs_mkdir_ls.png показує листинг з часами 14:16–14:19 і каталогами bench_*, mr_daily_ohlc, mr_yearly, wordcount. logs/04_hdfs.log (той самий кроці) містить інший стан (13:36, без цих каталогів). Також logs/05_wordcount.log містить job_1794404180673 (13:37), тоді як screenshots/12 і 14 мають job_1794418481996 (17:36). Логи дописувались з кількох запусків. Потрібно: або перерендерити 06 з журналу, у якому він виник, або у звіті вказати, який запуск дав кожен артефакт.

G4. screenshots/25_hive_top10_range.png: у run_pz1.py вивід обрізано (`out_r = r.out[r.out.index('trade_date\thigh'):]`), прибрано рядки перед заголовком, тобто вивід не повністю неправлений. RULES забороняє редагований вивід. Потрібно перерендерити з повним виводом або явно позначити обрізку.

G5. Умови вимірювання: loadavg зафіксовано один раз на початку bench (4,06 при 4 vCPU, тобто система вже була навантажена). Для кожного запуску навантаження не записано. Потрібно: у звіті прямо вказати перешкоди (INDEX.md це робить) і, бажано, писати loadavg перед кожним запуском.

G6. screenshots/35_ui_yarn_cluster.png і 36_ui_yarn_applications.png показують одну і ту саму сторінку All Applications. Мінорно: можна замінити 35 на сторінку /cluster з Cluster Metrics або прибрати дубль.

G7. code/bench_pz1.py: 4 розділювальні коментарі англійською (рядки 48, 64, 74, 100). Мінорно, для однорідності українською.

G8. results/summary.txt: рядок «Скріншоти: 26 файлів» застарілий (фактично 47 + 6 рисунків). Потрібно перегенерувати після фінального запуску.

G9. screenshots/20_hive_select.png: `SELECT * FROM candles LIMIT 5` повертає рядок NULL (заголовок CSV прочитано як дані, skip.header.line.number на fetch-запиті не спрацьовує). Агрегати правильні, бо candles_v відфільтровує NULL, і COUNT=7 816 537 збігається з CSV. Потрібно пояснити в звіті або виправити (наприклад, відфільтрувати в запиті).

G10. Картка pz1.md: у розділі «Проблеми» зазначено, що команди з методички не витягнуті (порожні місця після «Створити директорію…» тощо). Вимоги перевірялися за текстом завдань, а не за точними командами методички. Це обмеження картки, не виходу ПЗ1; за можливості варто дістати PDF і звірити команди.

## Підсумок
Технічні кроки (HDFS, WordCount, Streaming yearly і daily OHLC, Hive external table, 6 запитів, топ-дні, крос-перевірки, env, таймінги) мають докази. Основний пробіл: звіт з висновками (G1). Далі треба виправити скріншоти 26, 06, 25 (G2–G4) і зафіксувати умови таймінгів (G5).
