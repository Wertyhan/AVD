# NUMCHECK: перевірка чисел у звіті ПЗ2

Звіт: `/home/user/AVD/work/pz2/report.md` (основний текст, розділи 1–7, Висновки; Додаток А не перевірявся). Джерела: `results/*.csv` (рядок = номер рядка файлу з заголовком як рядок 1), `screenshots/txt/*.txt`, `data/README.md`, `data/facts.csv`. PNG-скріншоти та рисунки не перевірялися, де немає txt-джерела (позначено окремо).

**Підсумок:** OK = 462, MISMATCH = 1, NOT FOUND = 15 (числа рахуються по значеннях; у рядку таблиці вказано кількість значень у колонці «n»).

## Таблиця перевірки

| Місце в звіті (рядок) | Значення у звіті | Джерело (файл:рядок/колонка) | Статус | n |
|---|---|---|---|---|
| L3 | 7 816 537 записів; роки 2012 і 2026 | results/pz2_load_summary.csv:row 2 (rows); data/facts.csv first/last_timestamp_utc | OK | 3 |
| L7 | 4 логічні ядра / 4 потоки | results/env_versions.csv:row 10 (CPU_logical_cores=4) | OK | 1 |
| L9 | 396,8 МБ; 4 розділи | data/facts.csv:raw_csv_size_MB=396.8; results/pz2_load_summary.csv:row 4 (raw_partitions_inferSchema=4) | OK | 2 |
| L23 | Xeon 2,10 ГГц; 4 ядра; 15 ГБ RAM | results/env_versions.csv:row 9 (CPU), row 10, row 11 (RAM_GB=15) | OK | 3 |
| Табл. 1, L29–L39 | Java 21.0.12.1; Spark 4.3.0; PySpark 4.3.0; Python 3.13.18; pandas 3.0.5; numpy 2.5.3; Jupyter 7.6.5; JupyterLab 4.6.6; ipykernel 7.4.0; defaultParallelism 4; driver 8g; shuffle.partitions 16; seed 42 | results/env_versions.csv:rows 2–22 (col value); screenshots/txt/01_env.txt | OK | 13 |
| L41 | 8 ГБ з 15 ГБ; 16 розділів (4 на ядро); 7,8 млн рядків | env_versions.csv:rows 11, 14, 15; pz2_load_summary.csv:row 2 | OK | 5 |
| L41 | 200 розділів (типове значення Spark) | не в results/; Spark default, згадано лише в коментарі code/pz2_spark.py (Додаток А) | NOT FOUND | 1 |
| L58 | spark.defaultParallelism 4 | results/env_versions.csv:row 13 | OK | 1 |
| L76 | 4,58 USD; Volume 0; 1 січня 2012 | screenshots/txt/02_schema.txt (перший рядок: Timestamp 1325376060 = 2012-01-01 00:01 UTC, Close 4.58, Volume 0.0) | OK | 3 |
| Табл. 2, L82–L87 | null = 0 і NaN = 0 для 6 полів; типи int/bigint, double | results/pz2_schema_nulls.csv:rows 2–7 (null_count, nan_count, spark_type_*) | OK | 12 |
| L91 | 10,236 с (inferSchema); 1,045 с (явна схема); ~10 разів | results/timings.csv:row 2 (10.236), row 3 (1.045); однократні запуски | OK | 3 |
| L97 | 7 816 537 рядків після dropna і dropDuplicates; 0 дублікатів | results/pz2_cleaning.csv:rows 2–5 (rows_after_dropna, rows_after_dropDuplicates_Timestamp, duplicates_removed=0) | OK | 3 |
| L109 | 130 276 інтервалів (погодинний агрегат) | data/facts.csv:final_rows_1h=130276 | OK | 1 |
| Табл. 3, L115–L119 | count 7 816 537/7 816 536/7 816 535; mean, stddev, min, max для Volume, range, rel_range, log_ret, vol60 (25 значень) | results/pz2_describe.csv:rows 2–6, cols Volume, range, rel_range, log_ret, vol60 | OK | 25 |
| L121 | 1,24·10⁻⁶; 0,001733; ≈0,17 %; ±0,617; 0 некоректних свічок | pz2_describe.csv:row 3 (mean log_ret), row 4 (stddev), row 5/6 (min/max); data/facts.csv:invalid_candles_total=0 | OK | 5 |
| L125 | 1 312 419 хвилин; 16,79 %; 7 816 537 | results/pz2_flat_summary.csv:rows 2–4 (flat_minutes, rows, flat_share_pct) | OK | 3 |
| L129 | 2012: 94,95 %; 2013: 39,16 %; 2016: 33,19 %; з 2018 не більше 3,95 %; 100 % у 2015; доби 6–8.01.2015; 1440 хвилин | results/pz2_yearly_volume_flat.csv:rows 2, 3, 6, 9–16 (flat_share_pct); pz2_daily_features.csv: 2015-01-06..08 flat_share=1.0, n_flat=1440 | OK | 9 |
| L131 | 94,95 %; 24,24 %; 39,16 %; 4876 діб | pz2_yearly_volume_flat.csv:rows 2–5; pz2_daily_features.csv: count(flat_share<0,5)=4876 | OK | 4 |
| L135 | 0,245 с; 7,829 с | results/timings.csv:row 6 (0.245), row 7 (7.829); pz2_lazy_vs_action.csv:rows 2–3 | OK | 2 |
| L145 | вузли (13) Filter year=2021 і (3) Project; hashpartitioning 16 | screenshots/txt/04_lazy.txt (Physical Plan: Filter (13), Project (3); Exchange hashpartitioning(Timestamp, 16)) | OK | 3 |
| Табл. 4, L165–L167 | без cache: 19,774 / 17,565 / 18,349, медіана 18,349; з cache: 3,184 / 2,376 / 2,329, медіана 2,376; materialize 82,293 | results/pz2_cache_experiment.csv:rows 2–8 (seconds); pz2_cache_summary.csv:rows 2–3 (median); timings.csv:rows 8–14 | OK | 9 |
| L169 | у 7,7 разу; 82,293 с; ≈5 повторів | pz2_cache_summary.csv:row 4 (speedup 7.72); derived 82,293/(18,349−2,376)=5,2 | OK | 3 |
| L173 | 330,477 с (весь конвеєр) | results/timings.csv:row 36 (TOTAL_script_runtime) | OK | 1 |
| L173 | 16 стовпців у кешованому DataFrame | code/pz2_spark.py build(): 6 вхідних + 10 обчислених стовпців (Додаток А.1) | OK | 1 |
| L173 | Cached Partitions = 1 | тільки PNG (screenshots/spark_ui_storage.png); немає в screenshots/txt | NOT FOUND | 1 |
| L190 | 5429 діб; 5427 повних; 1440 хвилин; 130 276 рядків | results/pz2_daily_features.csv (5429 рядків; is_complete=1: 5427); data/facts.csv:final_rows_1h | OK | 4 |
| Табл. 5, L200–L205 | n, std, асиметрія, ексцес, min, max для 6 рядів (36 значень) | results/pz2_skew_kurtosis.csv:rows 2–7 (n, std, skew, excess_kurtosis, min, max) | OK | 36 |
| L207 | ексцес 7161,59; 5958,64; ±0,617 (≈356 σ); 1,3 млн flat-хвилин | pz2_skew_kurtosis.csv:rows 2–3; derived 0,617/0,001733; pz2_flat_summary.csv:row 2 | OK | 4 |
| L209 | 3,92; −1,23; −1,31; 118,11; 25,46; 5428 діб; 11.04.2013 −0,664; 12.03.2020 −0,494 | pz2_skew_kurtosis.csv:rows 2, 4, 6 (skew, excess_kurtosis, n); pz2_top10_rv.csv:row 2 (ret_1d −0,66395); pz2_sql_q6_crash_days_vs_30d_median.csv:row 2 (−49,4 %) | OK | 9 |
| L219 | rv 0,211 і vol30 0,094 (12.03.2020); rv 0,191 і vol30 0,053 (19.05.2021); доби 2013 з rv>0,3: 13; після 2023 ≈90 % з rv у [0,01; 0,05] | pz2_daily_features.csv: рядки 2020-03-12, 2021-05-19 (rv, vol_30d); похідні підрахунки | OK | 5 |
| Табл. 6, L237–L246 | rv, hl_rel, ret_1d, flat_share для 10 діб (40 значень) | results/pz2_top10_rv.csv:rows 2–11 (rv, hl_rel, ret_1d, flat_share) | OK | 40 |
| L248 | 98,1 % і 97,2 % flat (27.01 і 31.01.2012) | results/pz2_top10_rv.csv:rows 5 і 7 (flat_share 0,9806 і 0,9715) | OK | 2 |
| L248 | 4-те і 6-те місця | results/pz2_top10_rv.csv:rows 5 і 7 (rank = рядок−1) | OK | 2 |
| L248 | мінімум 3,8 USD (27.01.2012) | results/pz2_top10_rv.csv:row 5 (low 3.8) | OK | 1 |
| L248 | ціна з 259,34 до 45 USD | results/pz2_top10_rv.csv:row 4 (high 259.34, 2013-04-10), row 3 (low 45.0, 2013-04-12) | OK | 2 |
| L248 | flat 0,005 і 0,085 (18.12.2013, 20.11.2013) після фільтра | results/pz2_top10_rv_active.csv:rows 10 і 11 (flat_share 0,00486 і 0,08472) | OK | 2 |
| L248 | «Сім із десяти діб» припадають на квітень 2013 | results/pz2_top10_rv.csv:rows 2, 3, 4, 6, 9, 10 (квітень 2013) = 6 з 10 | MISMATCH | 1 |
| L252 | розмах 44,21 % (13.03.2020) | results/pz2_top10_hl_rel.csv:row 8 (hl_rel 0,44207) | OK | 1 |
| L260 | 2,85 % (03:00 UTC); 5,79 % (14:00 UTC); rel_range 0,00067 (05:00); 0,00109 (14:00) | results/pz2_volume_by_hour.csv:rows 5 (hour 3), 16 (hour 14), 7 (hour 5); cols share_pct, rel_range_mean | OK | 4 |
| L264 | 5,53 млн (2015); 0,64 млн (2025); середня ціна 272,25 → 101 639,98 USD; 567 949 BTC (2012); 94,95 % | results/pz2_volume_by_year.csv:rows 5 (2015), 15 (2025), 2 (2012) | OK | 6 |
| L281 | від 1,678 до 2,816 с | results/timings.csv:row 26 (sql_q5 1.678), row 23 (sql_q2 2.816) | OK | 2 |
| Табл. 7, L291–L305 | 15 рядків × 6 значень: хвилин, середня ціна, max High, min Low, обсяг, flat % | results/pz2_sql_q1_year_stats.csv:rows 2–16 (minutes, avg_close, max_high, min_low, volume, flat_pct) | OK | 90 |
| L307 | 525 600 і 527 040 хв; 126 272 USD (max High 2025); 3,8 USD (2012); 12,77–1163 USD (2013); 2012 з 00:01; 2026 до 11 листопада | results/pz2_sql_q1_year_stats.csv:rows 2–16; data/facts.csv:first/last_timestamp_utc | OK | 8 |
| L309 | Q4: 0,03977 (січ), 0,03912 (бер), 0,02342 (жовт); Q5: 137 070 BTC (18.12.2013), 58 513 BTC (12.03.2020) | results/pz2_sql_q4_month_seasonality.csv:rows 2, 4, 11 (avg_daily_rv); pz2_sql_q5_max_volume_day_per_year.csv:rows 3 (2013), 10 (2020) | OK | 5 |
| L313 | 6,1661 BTC/хв (середа 14:00 UTC); 1,532 BTC/хв (неділя 02:00 UTC) | results/pz2_sql_q3_dow_hour_volume.csv:line 88 (dow 4 = Wed, hour 14, 6.1661) і line 4 (dow 1 = Sun, hour 2, 1.532); max/min по всіх 168 рядках | OK | 3 |
| Табл. 8, L319–L324 | лог. дохідність, rv, обсяг, обсяг/середній за 30 діб для 6 діб (24 значення) | results/pz2_sql_q6_crash_days_vs_30d_median.csv:rows 2–7 (ret_pct, rv, volume, volume_x_prev30_mean) | OK | 24 |
| L326 | 4,63–9,27 разу; падіння 10,43 % (08.11.2022); обсяг у 8,45 разу | pz2_sql_q6_...csv:rows 2–7 (volume_x_prev30_mean; ret_pct −10,43 у row 6) | OK | 4 |
| L334 | три етапи (граф Spark UI) | тільки PNG (screenshots/spark_ui_sql_detail.png); немає в txt | NOT FOUND | 1 |
| L334 | 4 розділи; 12 місяців | results/pz2_load_summary.csv:row 4 (4 partitions); pz2_sql_q4_month_seasonality.csv (12 рядків) | OK | 2 |
| L335 | 16 розділів обміну (AQEShuffleRead) | тільки PNG (screenshots/spark_ui_sql_detail.png); 11_sql_explain.txt не містить кількості | NOT FOUND | 1 |
| L350 | опитування RSS кожні 50 мс | code/measure_memory.py (Додаток А.4): time.sleep(0.05) | OK | 1 |
| Табл. 9, рядок «Рік, pandas» | 4,733; 4,553–4,760; 744,4 | results/pz2_engine_comparison_summary.csv:row 5 (median, min, max); results/pz2_memory.csv:row 2 | OK | 4 |
| Табл. 9, рядок «Рік, Spark local» | 3,897; 3,159–4,638; 1279,2 (JVM) + 127,4 (Python) | pz2_engine_comparison_summary.csv:row 7; pz2_memory.csv:rows 4, 5 | OK | 5 |
| Табл. 9, рядок «Рік, Spark кеш» | 1,460; 1,410–1,791 | pz2_engine_comparison_summary.csv:row 6 | OK | 3 |
| Табл. 9, рядок «Рік, Hadoop» | 3 клітинки «[ЗАПОВНИТИ ПІСЛЯ ЗАПУСКУ]» | results/pz2_engine_comparison.csv:hadoop_streaming рядки (порожні); timings.csv:row 34 | NOT FOUND | 3 |
| Табл. 9, рядок «OHLC, pandas» | 5,734; 5,458–6,243; 881,6 | pz2_engine_comparison_summary.csv:row 2; pz2_memory.csv:row 3 | OK | 4 |
| Табл. 9, рядок «OHLC, Spark local» | 7,493; 5,593–12,414; 1478,1 (JVM) + 131,8 (Python) | pz2_engine_comparison_summary.csv:row 4; pz2_memory.csv:rows 6, 7 | OK | 5 |
| Табл. 9, рядок «OHLC, Spark кеш» | 2,190; 2,121–2,266 | pz2_engine_comparison_summary.csv:row 3 | OK | 3 |
| Табл. 9, рядок «OHLC, Hadoop» | 3 клітинки «[ЗАПОВНИТИ ПІСЛЯ ЗАПУСКУ]» | pz2_engine_comparison.csv:hadoop_streaming (порожні); timings.csv:row 35 | NOT FOUND | 3 |
| L369 | різниця 0,836 с; 3,897 / 4,733; 7,493 / 5,734; 5,593–12,414; pandas ≈0,8 с | pz2_engine_comparison_summary.csv:rows 4, 5, 7; derived 4,733−3,897=0,836 | OK | 7 |
| L371 | 1,460 і 2,190 с над кешем | pz2_engine_comparison_summary.csv:rows 3, 6 | OK | 2 |
| L375 | 2,7–3,4 разу; JVM 1279,2–1478,1 МБ; pandas 744,4–881,6 МБ | derived 3,897/1,460=2,67; 7,493/2,190=3,42 (summary rows 4,3,7,6); pz2_memory.csv:rows 2–4, 6 | OK | 6 |
| L386 | JVM драйвера 1279,2–1478,1 МБ | results/pz2_memory.csv:rows 4, 6 | OK | 2 |
| L386 | пікова пам'ять Hadoop: [ЗАПОВНИТИ] | немає значення в results/ | NOT FOUND | 1 |
| L387 | 3,897; 7,493 с (CSV); 1,460; 2,190 с (кеш) | pz2_engine_comparison_summary.csv:rows 3, 4, 6, 7 | OK | 4 |
| L387 | час Hadoop: [ЗАПОВНИТИ] | немає значення в results/ | NOT FOUND | 1 |
| L389 | 7,7 разу; 18,349 → 2,376 с | results/pz2_cache_summary.csv:rows 2–4 | OK | 3 |
| L391 | кількісний висновок Hadoop: [ЗАПОВНИТИ] | немає значення в results/ | NOT FOUND | 1 |
| L395 | 5429 діб; 130 276 годин; 24 години; 168 комірок; 7,8 млн рядків; 9 графіків | results/pz2_daily_features.csv (5429); data/facts.csv; results/pz2_sql_q3_dow_hour_volume.csv (168 рядків); figures/ (fig01–fig09) | OK | 6 |
| L398 | 5429 рядків замість 7 816 537 (коментар у коді) | pz2_daily_features.csv (5429); pz2_describe.csv row 2 (7816537) | OK | 2 |
| L407 | 3,8 USD; 126 202 USD (максимум закриття) | results/pz2_describe.csv:row 5 (Close min 3.8), row 6 (Close max 126202.0); data/facts.csv:max_close | OK | 2 |
| L411 | 4 ядра; 15 ГБ; 16 розділів; 8 ГБ; Java 21; Python 3.13; Jupyter 7.6.5 | results/env_versions.csv:rows 2, 5, 10, 11, 14, 15, 19 | OK | 7 |
| L413 | 7 816 537 записів; 6 атрибутів; 0 пропусків; 1,045 і 10,236 с | pz2_load_summary.csv:row 2; pz2_schema_nulls.csv; timings.csv:rows 2–3 | OK | 5 |
| L415 | 16,79 %; 94,95 %; 97–98 % flat; 2 доби | results/pz2_flat_summary.csv:row 4; pz2_yearly_volume_flat.csv:row 2; pz2_top10_rv.csv:rows 5, 7 | OK | 4 |
| L417 | ексцес 7161,59; 25,46 | results/pz2_skew_kurtosis.csv:rows 2 і 6 | OK | 2 |
| L419 | шість SQL-запитів; 4,63–9,27 разу; 12.03.2020, 19.05.2021, 08–09.11.2022 | results/pz2_sql_queries.sql (6 запитів); pz2_sql_q6_...csv:rows 2–7 | OK | 3 |
| L421 | 1279,2–1478,1 МБ; 744,4–881,6 МБ; 3,897 / 4,733 с; 7,493 / 5,734 с; 7,7 разу; 82,293 с | pz2_memory.csv:rows 2–4, 6; pz2_engine_comparison_summary.csv:rows 4, 5, 7; pz2_cache_summary.csv:row 4; timings.csv:row 11 | OK | 10 |
| L421 | пікова пам'ять і час MapReduce: [ЗАПОВНИТИ] | немає значення в results/ | NOT FOUND | 2 |
| L423 | дев'ять графіків | figures/ fig01–fig09 (9 файлів) | OK | 1 |

## Дефекти (з точними правильними значеннями)

- **D1, MISMATCH, L248.** «Сім із десяти діб припадають на квітень 2013 року». Правильно: **«Шість із десяти діб»**. У `results/pz2_top10_rv.csv` (рейтинг rv без фільтра) квітень 2013 мають 6 діб: 11.04, 12.04, 10.04, 16.04, 13.04, 17.04 (ранги 1, 2, 3, 5, 8, 9).
- **D2, NOT FOUND, 11 місць (L363, L367, L386, L387, L391, L421 ×2, Таблиці 9 і 10).** Значення Hadoop Streaming ще не внесено: `[ЗАПОВНИТИ ПІСЛЯ ЗАПУСКУ]`. У `results/pz2_engine_comparison.csv` і `results/timings.csv` (рядки 34–35) рядки hadoop_streaming порожні. Потрібно: час обох задач (медіана з 3 запусків, мінімум–максимум), пікова пам'ять MapReduce, і після цього оновити висновки в L391 і L421. Самі числа в звіті придумати не можна.
- **D3, методика, Таблиця 9 (L356) і L350.** Підпис «медіана з трьох запусків» стосується лише часу. Колонка «Пікова пам'ять, МБ» береться з `results/pz2_memory.csv`, який отримано **одним запуском** на пару рушій/задача (`code/measure_memory.py`, цикл без повторів; у файлі немає номера запуску). Значення в таблиці збігаються з файлом (744,4; 881,6; 1279,2; 127,4; 1478,1; 131,8). Правильний підпис: «Час – медіана з трьох запусків, мінімум–максимум; пікова пам'ять – один запуск (пік RSS)». У L350 варто додати, що пам'ять вимірювалася окремим скриптом, а не тими самими запусками, що й час.
- **D4, методика, L91, L135, L173.** Числа 10,236 с і 1,045 с (L91), 0,245 с і 7,829 с (L135), 330,477 с (L173) – однократні значення з `results/timings.csv` (рядки 2, 3, 6, 7, 36). Вони правильні, але текст не каже, що це один запуск, тоді як решта часів у звіті – медіани з трьох запусків. Правильно: додати «(один запуск)» або позначити це в тексті.
- **D5, назва файлу, L326.** Файл `results/pz2_sql_q6_crash_days_vs_30d_median.csv` (і псевдонім `q6_crash_days_vs_30d_median` у `results/pz2_sql_queries.sql`) називається «median», але запит обчислює **середнє** `AVG(volume) OVER (... 30 PRECEDING AND 1 PRECEDING)` (колонка `volume_x_prev30_mean`). Текст звіту це пояснює, тож числа в Таблиці 8 правильні. Правильно: перейменувати файл і псевдонім на `..._30d_mean`, щоб назва не суперечила змісту.
- **D6, точність, L307.** «Максимум ціни 126 272 USD припадає на 2025 рік» – це максимум колонки High (Таблиця 7, `max_high`). Максимум ціни закриття – **126 202 USD** (`results/pz2_describe.csv`, рядок 6, колонка Close; `data/facts.csv:max_close`), саме його використано в L407. Правильно: «Максимум High 126 272 USD (максимальне закриття 126 202 USD)».
- **D7, неоднозначність, L389 vs Таблиця 9.** Прискорення в 7,7 разу (18,349 → 2,376 с) береться з `heavy_query` із кешем (Таблиця 4, `results/pz2_cache_summary.csv`). У Таблиці 9 рядки «Spark, кеш» (1,460 і 2,190 с) – інший запит, лише агрегація над кешем. Числа правильні, але варто явно написати, що це різні запити, щоб читач не порівнював їх як одне й те саме.
- **D8, не перевірено (лише PNG), L173, L334, L335.** «Cached Partitions = 1», «граф складається з трьох етапів» і «16 розділів обміну» є лише на скриншотах `spark_ui_storage.png` і `spark_ui_sql_detail.png`, у `screenshots/txt/` їх немає. Потрібно переглянути PNG вручну або посилатися на txt-вивід.
- **D9, інформаційно, `results/INDEX.md`.** INDEX каже «матеріалізація кешу близько 80 с», звіт і `timings.csv` дають 82,293 с. Це округлення в індексі, звіт правильний; змінювати звіт не потрібно.

## Перевірка синхронності часів і пам'яті між файлами

- Медіани з трьох запусків узгоджено між `results/timings.csv` (рядки 28–33), `results/pz2_engine_comparison_summary.csv`, `results/pz2_engine_comparison.csv`, `screenshots/txt/12_engines.txt` і звітом (Таблиці 4 і 9, L369–L389). Мінімуми й максимуми в звіті збігаються з `pz2_engine_comparison_summary.csv`.
- Кеш: `results/pz2_cache_experiment.csv`, `pz2_cache_summary.csv`, `timings.csv` (рядки 8–14) і `screenshots/txt/05_cache.txt` узгоджуються (18,349 / 2,376 / 82,293 с; прискорення 7,72).
- Пам'ять: `results/pz2_memory.csv` і `code/measure_memory.py` – один запуск на пару; у звіті це не сказано (див. D3).
- Однократні часи (`timings.csv` рядки 2, 3, 6, 7, 36) не позначені як однократні (див. D4). Звіт не змішує їх із медіанами в одній таблиці.
- Жодного числового протиріччя між файлами `results/` і `screenshots/txt/` не знайдено: `env_versions.csv` = `01_env.txt`; `pz2_sql_q1..q6` = `10_sql_*.txt`; `pz2_skew_kurtosis.csv` = `09_moments.txt`; `pz2_volume_by_hour.csv` = `08_volume.txt`; `pz2_daily_features.csv` = `07_top_days.txt` (top-10 та флаги).

## Обсяг перевірки і виключення

- Перевірено всі числа в розділах 1–7 та Висновках, включно з клітинками таблиць 1–10 (Таблиця 6: 40 значень; Таблиця 7: 90 значень; Таблиця 3: 25 значень; Таблиця 5: 36 значень).
- Не перевірялися: Додаток А (код), числа в підписах коду, як-от `rowsBetween(-59, 0)`, `CORES * 4`, `2^31` (це параметри коду, не твердження про результати). Виняток: L398 (коментар з числами 5429 і 7 816 537), його перевірено.
- Рисунки (PNG) і скриншоти, які немає в `screenshots/txt/`, позначено NOT FOUND, а не OK.
- Округлення: показані знаки відповідають джерелу (наприклад, 0,0000012 з 1,2391·10⁻⁶; 3,9 з 3,92). Десятковий розділювач у звіті – кома.
- Нові файли: лише `/home/user/AVD/work/pz2/NUMCHECK.md`. Звіт `report.md` і результати не змінювалися.
