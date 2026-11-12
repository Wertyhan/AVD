# ПЗ2: зміни чисел відносно _old_run (старе -> нове)

Нове значення береться з results/ поточного запуску (`bash code/run_all.sh`), старе – з `_old_run/results`. Секундні виміри змінюються від запуску до запуску; зміни детермінованих величин спричинені двома змінами коду: вікна з `partitionBy(year, month)` (замість глобального вікна) і вимкненим злиттям розділів AQE.

## 1. Числа, які використовує поточний report.md

| Розділ / таблиця | Величина | Було | Стало | Примітка |
|---|---|---|---|---|
| 2 | inferSchema: завантаження+дія, с | 10,236 | 10,744 | було count() (без розбору стовпців), стало сума 6 стовпців, медіана з 3, прогрітий файловий кеш |
| 2 | явна схема: завантаження+дія, с | 1,045 | 5,014 | те саме; відношення infer/explicit було ×9,8, тепер ×2,143 |
| 3.3 | побудова плану (build), с | 0,245 | 0,174 |  |
| 3.3 | перша дія count(), с | 7,829 | 6,653 | count() не обчислює вікон (plan без Window) |
| 3.3 | повна дія (noop write), с | – | 24,537 | нове: повний прохід із вікнами |
| 3.3 | agg(sum vol60, sum log_ret), с | – | 20,301 | нове |
| 3.3 т.3 | без cache(), запуск 1, с | 19,774 | 16,811 |  |
| 3.3 т.3 | без cache(), запуск 2, с | 17,565 | 15,484 |  |
| 3.3 т.3 | без cache(), запуск 3, с | 18,349 | 14,012 |  |
| 3.3 т.3 | з cache(), запуск 1, с | 3,184 | 1,640 |  |
| 3.3 т.3 | з cache(), запуск 2, с | 2,376 | 1,363 |  |
| 3.3 т.3 | з cache(), запуск 3, с | 2,329 | 0,927 |  |
| 3.3 т.3 | матеріалізація кешу, с (було 1 запуск; тепер медіана з 3) | 82,293 | 28,950 | вікна з partitionBy(year, month): паралельно |
| 3.3 | медіана без кешу, с | 18,349 | 15,484 |  |
| 3.3 | медіана з кешем, с | 2,376 | 1,363 |  |
| 3.3 | прискорення кешу, разів | 7,723 | 11,360 |  |
| 3.3 | окупність, повторів | 5,152 | 2,050 |  |
| 3.3 | розділів кешу | 1 (SinglePartition) | 16 з 16 |  |
| 4.2 т.4 | 1 хв, усі свічки: n | 7816536,000 | 7816358,000 |  |
| 4.2 т.4 | 1 хв, усі свічки: std | 0,002 | 0,002 |  |
| 4.2 т.4 | 1 хв, усі свічки: skew | 3,920 | 3,920 |  |
| 4.2 т.4 | 1 хв, усі свічки: excess_kurtosis | 7161,590 | 7161,620 |  |
| 4.2 т.4 | 1 хв, лише з торгами (Volume>0): n | 6504118,000 | 6503968,000 |  |
| 4.2 т.4 | 1 хв, лише з торгами (Volume>0): std | 0,002 | 0,002 |  |
| 4.2 т.4 | 1 хв, лише з торгами (Volume>0): skew | 3,575 | 3,575 |  |
| 4.2 т.4 | 1 хв, лише з торгами (Volume>0): excess_kurtosis | 5958,635 | 5958,658 |  |
| 4.2 | rv після 2023 р. | «0,01–0,05» (хибно) | мін 0,003, p05 0,009, медіана 0,020, p95 0,041, макс 0,105 | C3 |
| 6.1 т.7 | daily_ohlc / pandas: median, с | 5,734 | 4,884 | запуски чергувалися S,P,S,P,S,P |
| 6.1 т.7 | daily_ohlc / pandas: min, с | 5,458 | 4,851 | запуски чергувалися S,P,S,P,S,P |
| 6.1 т.7 | daily_ohlc / pandas: max, с | 6,243 | 5,449 | запуски чергувалися S,P,S,P,S,P |
| 6.1 т.7 | daily_ohlc / spark_cached_df: median, с | 2,190 | 0,618 | кеш із 16 розділів |
| 6.1 т.7 | daily_ohlc / spark_cached_df: min, с | 2,121 | 0,563 | кеш із 16 розділів |
| 6.1 т.7 | daily_ohlc / spark_cached_df: max, с | 2,266 | 0,986 | кеш із 16 розділів |
| 6.1 т.7 | daily_ohlc / spark_local[*]: median, с | 7,493 | 4,547 | запуски чергувалися S,P,S,P,S,P |
| 6.1 т.7 | daily_ohlc / spark_local[*]: min, с | 5,593 | 4,220 | запуски чергувалися S,P,S,P,S,P |
| 6.1 т.7 | daily_ohlc / spark_local[*]: max, с | 12,414 | 4,841 | запуски чергувалися S,P,S,P,S,P |
| 6.1 т.7 | year_count_volume / pandas: median, с | 4,733 | 3,556 | запуски чергувалися S,P,S,P,S,P |
| 6.1 т.7 | year_count_volume / pandas: min, с | 4,553 | 3,524 | запуски чергувалися S,P,S,P,S,P |
| 6.1 т.7 | year_count_volume / pandas: max, с | 4,760 | 3,707 | запуски чергувалися S,P,S,P,S,P |
| 6.1 т.7 | year_count_volume / spark_cached_df: median, с | 1,460 | 0,549 | кеш із 16 розділів |
| 6.1 т.7 | year_count_volume / spark_cached_df: min, с | 1,410 | 0,428 | кеш із 16 розділів |
| 6.1 т.7 | year_count_volume / spark_cached_df: max, с | 1,791 | 0,729 | кеш із 16 розділів |
| 6.1 т.7 | year_count_volume / spark_local[*]: median, с | 3,897 | 3,588 | запуски чергувалися S,P,S,P,S,P |
| 6.1 т.7 | year_count_volume / spark_local[*]: min, с | 3,159 | 3,023 | запуски чергувалися S,P,S,P,S,P |
| 6.1 т.7 | year_count_volume / spark_local[*]: max, с | 4,638 | 3,944 | запуски чергувалися S,P,S,P,S,P |
| 6.1 т.7 | пам'ять pandas, year_count_volume, МБ | 744,400 | 783,100 | медіана з 3 процесів |
| 6.1 т.7 | пам'ять Spark JVM, year_count_volume, МБ | 1279,200 | 1483,300 | медіана з 3; додатково idle JVM 480.1 МБ, driver 2g: 923.3 МБ |
| 6.1 т.7 | пам'ять Spark Python-клієнт, year_count_volume, МБ | 127,400 | 124,700 |  |
| 6.1 т.7 | пам'ять pandas, daily_ohlc, МБ | 881,600 | 879,000 | медіана з 3 процесів |
| 6.1 т.7 | пам'ять Spark JVM, daily_ohlc, МБ | 1478,100 | 1496,700 | медіана з 3; додатково idle JVM 480.1 МБ, driver 2g: 1026.1 МБ |
| 6.1 т.7 | пам'ять Spark Python-клієнт, daily_ohlc, МБ | 131,800 | 128,900 |  |

## 2. Детерміновані результати: порівняння CSV (old -> new)

Файли зі змінами:
- `env_versions.csv`: форма (22, 2) -> (24, 2)
- `pz2_daily_features.csv`: 484 комірок відрізняється (напр. rv[517]: 0.0789532855948513 -> 0.0789445657883871; rv[790]: 0.0935040353864481 -> 0.0934824451226908; std_ret_1m[31]: 0.0043158711168395 -> 0.0043173710874741)
- `pz2_describe.csv`: 6 комірок відрізняється (напр. log_ret[0]: 7816536.0 -> 7816358.0; log_ret[1]: 1.2391253045571997e-06 -> 1.236018769628706e-06; vol60[0]: 7816535.0 -> 7816179.0)
- `pz2_hist_ret_1m_active.csv`: 46 комірок відрізняється (напр. count[43]: 1661 -> 1659; count[55]: 3653 -> 3652)
- `pz2_skew_kurtosis.csv`: форма (6, 8) -> (9, 8)
- `pz2_sql_q4_month_seasonality.csv`: 1 комірок відрізняється (напр. avg_daily_rv[10]: 0.03163 -> 0.03162)
- `pz2_volume_by_hour.csv`: 1 комірок відрізняється (напр. std_ret_1m[0]: 0.0016559101347242 -> 0.0016560692487397)

Файли без змін (збігаються з точністю 1e-6): pz2_cleaning.csv, pz2_flat_summary.csv, pz2_load_summary.csv, pz2_schema_nulls.csv, pz2_sql_q1_year_stats.csv, pz2_sql_q2_top_days_range.csv, pz2_sql_q3_dow_hour_volume.csv, pz2_sql_q5_max_volume_day_per_year.csv, pz2_sql_q6_crash_days_vs_30d_mean.csv, pz2_top10_hl_rel.csv, pz2_top10_hl_rel_active.csv, pz2_top10_rv.csv, pz2_top10_rv_active.csv, pz2_volume_by_year.csv, pz2_yearly_volume_flat.csv

## 3. Нові файли результатів

`pz2_cache_partitions.csv`, `pz2_engine_equality.csv`, `pz2_lazy_vs_action_summary.csv`, `pz2_memory_runs.csv`, `pz2_partition_sizes.csv`, `pz2_rv_after2023.csv`, `pz2_schema_experiment.csv`, `pz2_schema_experiment_summary.csv`, `pz2_sql_q7_summary.csv`, `pz2_sql_q7_volume_spike_days.csv`, `pz2_top10_abs_minute_returns.csv`, `pz2_window_boundary.csv`

## 4. Перейменовані рисунки і скріншоти

fig08_timings -> fig08_engines + fig09_cache; fig09_flat_share -> fig10_flat_share; нові fig11_lazy_actions, fig12_volume_spikes; term_10_sql_q6_crash_days_vs_30d_median -> term_10_sql_q6_crash_days_vs_30d_mean; нові term_02a_schema_experiment, term_10_sql_q7_volume_spike_days, spark_ui_sql_detail_q1; spark_ui_sql_shots.py видалено (знімки робить основний скрипт).
