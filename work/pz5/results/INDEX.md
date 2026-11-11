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

Додаткові примітки (G4–G8): найкращу модель (GBT) обрано за test PR-AUC (pz5_best_model.csv), що є методологічно слабшим вибором; проте переможцем часової крос-валідації (pz5_tuning.csv, cv_pr_auc) також є GBT (найкраща конфігурація depth 3, iter 100, step 0,1: 0,7468; LR 0,7456; RF 0,7234), тобто вибір не залежить від test. Порівняння випадкового й хронологічного поділу виконано на різних n_test (15746 і 15534), обидва значення в pz5_random_vs_chrono.csv (колонка n_test). pz5_split.csv: train_end = останній фактичний навчальний рядок (2025-01-31 20:00), межа ембарго подана окремим рядком embargo_end_nominal_train_boundary (2025-02-01 20:00); скріншот term_04 зберігає початковий вивід із міткою train_end_nominal. Заголовки term_* – реальний скрипт (python code/pz5_spark.py) і коментар про фрагмент виводу.
