# ПЗ5 (Spark MLlib): індекс результатів чистого перезапуску

Запуск: `bash code/run_all.sh` (seed 42; кроки: `input_diff.py` -> `pz5_spark.py` -> `pz5_stats.py` -> `make_figures.py` -> `make_screens.py`; потім `python code/make_changes.py`). Попередній запуск збережено в `_old_run/`. Машина: 4 vCPU Intel Xeon 2,10 ГГц, 15 GiB, Spark/PySpark 4.3.0, Java 21.0.12, Python 3.13.18 (`env_versions.csv`). Час: `timings.csv` (pz5_spark.py 1429,7 с, pz5_stats.py 74,2 с), `run_all_log.txt` (весь run_all.sh 1510 с). Навантаження машини (інші процеси: mongod, фоновий java): `loadavg.csv` (load1 1,15–1,70 при 4 ядрах; на початку load5 1,33).

Вхід: `pz2/results/pz2_hourly_features.parquet` (новий запуск ПЗ2). Порівняння зі старим: `pz5_input_diff.csv` (137 годин зі зміненою `rv`, усі на початку місяця, 105 з 2018 р.; решта 17 стовпців збігається; n_minutes, ціна, обсяг однакові). Опорні числа ПЗ2: `pz2_reference_numbers.csv`.

| Завдання / виправлення | Докази |
|---|---|
| 1 Задача | pz5_acf.csv, pz5_direction_check.csv, pz5_direction_test.csv (биномний тест, z), pz5_direction_sign_rules.csv (правила «той самий / протилежний знак» за train/test/роками), pz5_flat_share_by_year.csv, pz5_target_share_by_year.csv; fig01, fig02; term_03, term_05, term_11, term_15 |
| 2 Підготовка | pz5_missing_values.csv, env_versions.csv; term_01, term_02, term_06 |
| 3 Вибірки | pz5_split.csv (розміри; остання неповна година відкинута), pz5_embargo_check.csv і pz5_embargo_summary.csv (ACF мітки, обґрунтування ембарго); term_04 |
| 3 Випадковий проти хронологічного (MAJOR-1) | pz5_random_vs_chrono.csv (Spark), pz5_random_vs_chrono_ci.csv і pz5_random_vs_chrono_diff.csv (ДІ та парні різниці на однакових годинах), pz5_random_split_sizes.csv, pz5_random_split_exposure.csv, прогнози pz5_pred_random_*.csv; fig07; term_10, term_14 |
| 4 Моделі, вибір за CV (MAJOR-2) | pz5_tuning.csv (з метриками по фолдах), pz5_cv_folds.csv, pz5_model_selection_cv.csv, pz5_best_params.json, pz5_best_model.csv (вибір за CV, test не використано), pz5_threshold_validation.csv і pz5_threshold_per_fold.csv (поріг за середнім F1 трьох фолдів), pz5_chosen_thresholds.json; fig09, fig10; term_07 |
| 5 ML Pipeline | code/pz5_spark.py, results/model/best_pipeline_GBT (у .gitignore); term_06, term_12; spark_ui_*.png (знято скриптом під час роботи Spark, viewport 1280x800, scale 2, кліп таблиці) |
| 6 Метрики | pz5_metrics_test.csv, pz5_metric_check_spark_vs_numpy.csv (власні ROC/PR збігаються зі Spark), pz5_bootstrap_ci.csv (95 % ДІ PR-AUC, ROC-AUC, average precision, F1; тижневі блоки, 1000, seed 42); fig03, fig04, fig08; term_08, term_13 |
| 7 Порівняння | pz5_bootstrap_diff.csv (GBT-LR, GBT-RF, GBT-персистентність, GBT-log(rv/поріг)), pz5_model_runtimes.csv, timings.csv; fig08, fig11 |
| 8 Аналіз | pz5_feature_importance.csv, pz5_feature_importance_groups.csv, pz5_errors_by_year/hour/current_regime/transition.csv (модель, обрана за CV), pz5_predictions_sample.csv, pz5_predictions_test_full.csv; fig05, fig06; term_09 |
| Зміни проти старого запуску | CHANGES_vs_old_run.md (і .csv) |

Скріншоти терміналів (term_01–term_15): справжня команда `$ python code/pz5_spark.py` або `pz5_stats.py` + приглушений коментар, вивід з `screenshots/txt/*.txt`, рендер `tools_report/term_png.py` (1600 px, 90 колонок, <= 35 рядків). Рисунки (fig01–fig11): `tools_report/plotstyle.py`, 6,3 x 3,4 / 4,7 дюйма, dpi 200, десяткова кома.

Підсумкові числа (test, n = 15 534; обрано GBT за CV PR-AUC 0,7467 проти LR 0,7456 і RF 0,7241, розкид між фолдами 0,034–0,038): GBT PR-AUC 0,8024 [0,7630; 0,8363], ROC-AUC 0,9010 [0,8896; 0,9113], F1 0,7105 [0,6797; 0,7390] (поріг 0,33). Різниці GBT-LR: PR-AUC +0,0203 [+0,0144; +0,0280], GBT-RF +0,0197 [+0,0142; +0,0261], GBT-персистентність +0,1888 [+0,1785; +0,1990], GBT-log(rv/поріг) +0,0415 [+0,0333; +0,0515]; ДІ не містять 0. Випадковий поділ на тих самих годинах: B проти A різниця PR-AUC <= 0,002 (n = 15 534), C* проти A* +0,005…+0,013 (n = 3 180); C на власному тесті (n = 15 746) нижчий на 0,04–0,06 PR-AUC, бо рядки інші.

Примітки: (1) Spark UI зберігає лише ~1000 останніх завдань, тому загальні 12 663 завдань і 48 483 етапи в `timings.csv` взято як максимальний id + 1; (2) `pz5_input_diff.csv`: перші два стовпці для рядків `rows_*` містять кількість рядків (усі роки / з 2018 р.); (3) модель в аналізі помилок і в збереженні – обрана за CV (GBT).
