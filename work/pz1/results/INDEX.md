# ПЗ1 – індекс доказів (Hadoop: HDFS, MapReduce, Hive), повний набір Bitcoin 1 хв, 7 816 537 рядків

Середовище: Hadoop 3.3.6 pseudo-distributed (JDK 8), Hive 3.1.3 (рушій MR, Derby), 4 vCPU / 15 GiB, версії в `env_versions.csv`.
Код: `../code/` (run_pz1.py – основний конвеєр; bench_pz1.py – порівняння часу; ui_shots_pz1.py; make_figures.py; termshot.py; mapper/reducer *.py; hive_*.hql).
Скріншоти терміналу = реальний вивід команд (render у PNG); веб-UI = playwright/Chromium. Журнали: `../logs/`.

| Завдання методички | Докази |
|---|---|
| 1. Архітектура Hadoop, принципи HDFS | screenshots 03 (dfsadmin -report), 04 (getconf: fs.defaultFS, replication, blocksize), 11 (fsck з блоками); `hdfs_cluster_config.csv`, `hdfs_file_info.csv` |
| 2. Встановлення pseudo-distributed | screenshots 01 (jps), 02/05 (yarn node), 28 (jps фінальний); UI 31 (NameNode overview), 32 (Datanodes), 37 (YARN nodes); конфіг у `run_pz1.py` (крок install); `env_versions.csv` |
| 3. Завантаження даних у HDFS | screenshots 06 (mkdir/ls), 07 (ls -h), 08 (du), 09 (cat head), 10 (tail), 11 (fsck), 29, 30; UI 33, 34 (Browse Directory); `hdfs_file_info.csv` (розмір, блоки, час put) |
| 4. MapReduce | WordCount: screenshots 12–14, UI 38–40, `wordcount_top50.csv`. Streaming річні: 15, 16, UI 44–46, `mr_yearly_stats.csv`, `mr_counters_mr_yearly.csv`. Streaming добові OHLC: 17, 18, UI 41–43, `mr_daily_ohlc_full.csv`, `mr_daily_ohlc_sample.csv`, `mr_daily_ohlc_rowcount.csv`, `mr_counters_mr_daily_ohlc.csv`. Список задач: 27, UI 36, 47 |
| 5. Hive: таблиця над даними в HDFS | screenshots 19 (CREATE EXTERNAL TABLE + DESCRIBE), 20 (SELECT, COUNT); `hive_count_check.csv` |
| 6. SQL-запити Hive | 21 (GROUP BY рік), 22 (рік+місяць), 23 (daily_ohlc), 24 (топ-10 за обсягом), 25 (топ-10 за High-Low); `hive_yearly_stats.csv`, `hive_monthly_stats.csv`, `hive_daily_rowcount.csv`, `hive_top10_volume_days.csv`, `hive_top10_range_days.csv` |
| Звірка MR / Hive / Pandas / Spark | `crosscheck_hive_vs_mr_yearly.csv`, `crosscheck_hive_vs_mr_daily.csv`, `crosscheck_mr_hive_vs_pandas.csv`, `crosscheck_spark_vs_pandas.csv` (усі match=True); `pandas_*`, `spark_*` csv |
| 7. Висновки: порівняння з Pandas/Spark | `timings.csv` (усі запуски, 3 на пару), `timing_summary.csv` (медіана/min/max), `timing_conditions.csv` (навантаження), figures/fig_timing_comparison.png |

Рисунки (`../figures/`): fig_mr_yearly_volume, fig_mr_yearly_candles, fig_mr_daily_close_log, fig_timing_comparison, fig_top_days, fig_hive_monthly_volume.

## Умови вимірювання часу (чесне застереження)
Вимірювався повний настінний час від команди до результату (Streaming/Hive – з запуском JVM/контейнерів YARN; Pandas/Spark – з читанням CSV; Spark – з toPandas; створення SparkSession 14,52 с окремо). Hive-запит `yearly` виконується двома MR-стадіями. Під час вимірювань на тій самій 4-ядерній машині працювали сторонні процеси (loadavg 1 хв = 4,06 на старті, див. `timing_conditions.csv`; поруч виконувався Spark-сеанс іншого завдання), тому абсолютні значення, особливо Pandas/Spark, мають похибку; розкид наведено в min/max. Перший запуск PySpark yearly (23,9 с) містить холодний прогрів JVM.

Примітка (рендер для друку): термінальні PNG перерендерено спільним рендером tools_report/term_png.py (code/rerender_pz1.py); 26 – реальна команда python3 code/pandas_yearly.py; 06 – поточний стан HDFS; UI 35_ui_yarn_cluster.png видалено як дублікат 36_ui_yarn_applications.png (вміст однаковий). Веб-UI 31-34, 37, 39-40, 42-43, 45-47 перезняті з clip (code/ui_recapture_pz1.py); 36, 38, 41, 44 – обрізані (crop) попередні реальні знімки RM, бо після перезапуску ResourceManager втратив список застосунків.

## Доповнення після рецензії (див. `CHANGES_hive_fix.md`)
- C1: `skip.header.line.count` у `hive_01_create_table.hql`; Hive-кроки перезапущено; `SELECT ... LIMIT 5` без рядка NULL; `hive_count_check.csv` (COUNT по `candles` і `candles_v` = 7 816 537); csv Hive побайтно збігаються зі старими (`_old_before_hive_fix/`), звірки Hive vs MR = True. Скріншоти 19-25 перезняті.
- M4: `code/hive_07_load_data.hql` (керована таблиця + LOAD DATA INPATH), скріншоти 51, 52; `code/hive_01b_describe.hql` для скріншота 19.
- M1: скріншоти 48 (ls -R -h), 49 (du -h, cat | head), 50 (WordCount part-r-00000 | head).
- M2/C2: `figures/ui_hdfs_panel.png`, `figures/ui_yarn_panel.png` (джерела в `screenshots/ui_panel_src/`, код `code/ui_panels_pz1.py`), `ui_values.csv` (378.47 MB, Shuffled Maps = 3, DFS Used тощо).
- Застосунки Hive у YARN: `hive_yarn_applications.csv`, `hive_yarn_app_count.csv` (12).
- Код: `code/rerun_hive_fix.py` (повторний запуск Hive-кроків і звірка Hive vs MR), `code/rerender_pz1.py` (записи 19, 48-52).

Примітка до звірки: у `crosscheck_mr_hive_vs_pandas.csv` для `hive_vs_pandas_top10_range_days` right_rows = 5429 – це довжина переданої в журнал повної добової таблиці Pandas (run_pz1.py:661); match порівнює множини 10 дат; збіг 10/10 підтверджують `hive_top10_range_days.csv` і `pandas_top10_range_days.csv`.
Джерело частки нульового обсягу (16,79 