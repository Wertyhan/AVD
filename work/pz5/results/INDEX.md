# ПЗ5 (Spark MLlib): відповідність завдань методички та доказів

Задача: бінарна класифікація «наступна година високоволатильна» (rv наступної години > 75-го перцентиля rv за останні 720 год, лише минуле). Період 2018-01-01 – 2026-11-11 (77 667 годин); chronological split 80/20 з ембарго 24 год. Seed 42. Запуск: code/pz5_spark.py, потім code/make_figures.py і code/make_screens.py.

| Завдання | Докази |
|---|---|
| 1 Визначення задачі | pz5_acf.csv, pz5_direction_check.csv (ACF ret лаг1 = -0,021; ACF abs лаг1 = 0,295; напрям: LR AUC 0,527, acc 0,513 при majority 0,500), pz5_flat_share_by_year.csv (вибір періоду з 2018), pz5_target_share_by_year.csv; fig01, fig02; term_03_period, term_05_acf, term_11_direction |
| 2 Підготовка даних | pz5_missing_values.csv, env_versions.csv; term_01_env, term_02_schema, term_06_pipeline (Imputer, StringIndexer+OneHotEncoder, VectorAssembler, StandardScaler) |
| 3 Вибірки 80/20 | pz5_split.csv, pz5_random_vs_chrono.csv; term_04_split, term_10_random_vs_chrono; fig07 |
| 4 Моделі | LR, RF, GBT: pz5_best_params.json, pz5_tuning.csv, pz5_threshold_validation.csv, pz5_chosen_thresholds.json; term_07_tuning; fig09, fig10 |
| 5 ML Pipeline | code/pz5_spark.py (mk, stages); results/model/best_pipeline_GBT (gitignored); term_06_pipeline, term_12_model_saved; spark_ui_*.png |
| 6 Метрики | pz5_metrics_test.csv (accuracy, precision, recall, F1, ROC-AUC, PR-AUC, MCC, матриці); fig03, fig04; term_08_metrics |
| 7 Порівняння | pz5_metrics_test.csv, pz5_model_runtimes.csv, timings.csv; fig08 |
| 8 Аналіз | pz5_feature_importance.csv (fig05, term_09_importance); pz5_errors_by_year/hour/current_regime/transition.csv (fig06); pz5_predictions_sample.csv, pz5_predictions_test_full.csv |

Примітки: випадковий поділ НЕ завищив метрики (хронологічний вищий: GBT ROC-AUC 0,901 проти 0,872), бо ознаки не містять міток; у звіті це подати чесно як відсутність помітного витоку для цих ознак. Spark UI знято наприкінці того самого запуску (run.log).
