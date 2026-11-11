# CHECK: повнота ПЗ5 (Spark MLlib) відносно методичних вказівок

Джерело вимог: `/home/user/AVD/work/cards/pz5.md` (повний текст: `/home/user/AVD/work/cards/raw/pz5.txt`), правила: `/home/user/AVD/work/RULES.md`. Виходи: `/home/user/AVD/work/pz5/`.

Перевірено: усі CSV у `results/`, `INDEX.md`, `run.log`, `timings.csv`, `env_versions.csv`, код `code/*.py`, усі 10 рисунків у `figures/` і всі 18 PNG у `screenshots/` (відкрито та переглянуто кожен).

## 1. Завдання та кроки методички

| № | Завдання / крок (порядок методички) | Докази | Статус |
|---|---|---|---|
| 1 | Завд. 1: визначення задачі та обґрунтування | results/pz5_acf.csv, pz5_direction_check.csv, pz5_flat_share_by_year.csv, pz5_target_share_by_year.csv; figures/fig01, fig02; screenshots/term_03, term_05, term_11 | OK |
| 2a | Завд. 2: відбір ознак | code/pz5_spark.py (список NUM, 17 ознак); screenshots/term_06_pipeline.png; results/pz5_feature_importance.csv | OK |
| 2b | Завд. 2: обробка пропущених значень | results/pz5_missing_values.csv (NULL до і після фільтрації); Imputer(median) у pipeline (term_06) | OK |
| 2c | Завд. 2: кодування категоріальних ознак | StringIndexer + OneHotEncoder для dow (term_06); dow_ohe_* у pz5_feature_importance.csv | OK |
| 2d | Завд. 2: формування цільової змінної | label = rv_{t+1} > 75-й перцентиль за 720 год; results/pz5_target_share_by_year.csv; fig01; term_04 | OK |
| 2e | Завд. 2: формування вектора ознак | VectorAssembler (term_06, features_raw) | OK |
| 3 | Завд. 3: вибірки 80/20 (хронологічно) | results/pz5_split.csv (train 62109, embargo 24, test 15534); term_04; spark_ui_storage.png (фільтр rn <= 62109). Дефект підпису: див. G8 | OK |
| 4 | Завд. 4: дві моделі (тут три: LR, RF, GBT) | results/pz5_best_params.json, pz5_tuning.csv, pz5_threshold_validation.csv, pz5_chosen_thresholds.json; term_07; fig09, fig10 | OK |
| 5 | Завд. 5: ML Pipeline Data → Preprocessing → FE → Model → Prediction → Evaluation | code/pz5_spark.py (функція stages/mk); term_06 (заголовок неправильний, див. G7); term_12; results/model/best_pipeline_GBT (gitignored, файли є); results/pz5_predictions_test_full.csv (Prediction); 6 spark_ui_*.png (реальний Spark UI) | OK |
| 6 | Завд. 6: метрики Accuracy, Precision, Recall, F1, ROC-AUC | results/pz5_metrics_test.csv (усі 5 метрик плюс PR-AUC, MCC, balanced accuracy); term_08; fig03 (дефект підпису AP, див. G4) | OK |
| 6b | Завд. 6: матриця помилок | tn/fp/fn/tp у pz5_metrics_test.csv; figures/fig04_confusion_matrices.png (значення збігаються з CSV) | OK |
| 7 | Завд. 7: порівняння моделей у вигляді таблиці | results/pz5_metrics_test.csv, pz5_model_runtimes.csv, timings.csv; term_08; fig08 (обрізані підписи, англійська легенда, див. G5). Таблиці у звіті немає (G1) | GAP |
| 8a | Завд. 8: найінформативніші ознаки | results/pz5_feature_importance.csv (RF, GBT, коефіцієнти LR); figures/fig05; term_09 | OK |
| 8b | Завд. 8: які помилки допускають моделі | results/pz5_errors_by_year.csv, pz5_errors_by_hour.csv, pz5_errors_by_current_regime.csv, pz5_errors_by_transition.csv; figures/fig06; pz5_predictions_sample.csv | OK |
| 8c | Завд. 8: яка модель краща за метриками | results/pz5_metrics_test.csv, pz5_best_model.csv; fig03; fig08 | OK |
| 8d | Завд. 8: можливі шляхи вдосконалення | Немає ні тексту, ні файла з аналізом (лише числа) | GAP |
| S1 | Крок 1: завантажити результати ПЗ2 | term_02 (printSchema, show); spark_ui_jobs.png (job load parquet) | OK |
| S2 | Крок 2: визначити задачу Data Mining | results/INDEX.md; дані як у завд. 1 | OK |
| S3 | Крок 3: визначити цільову змінну | label, results/pz5_target_series.csv, term_04 | OK |
| S4 | Крок 4: визначити набір ознак | NUM у code/pz5_spark.py; term_06 | OK |
| S5 | Крок 5: підготувати дані | results/pz5_missing_values.csv; term_01, term_02, term_06 | OK |
| S6 | Крок 6: навчальна та тестова вибірки | results/pz5_split.csv; term_04 | OK |
| S7 | Крок 7: побудувати першу модель | LR: results/pz5_metrics_test.csv, pz5_tuning.csv; term_07 | OK |
| S8 | Крок 8: оцінити першу модель | pz5_metrics_test.csv (рядок LR); term_08 | OK |
| S9 | Крок 9: побудувати другу модель | RF і GBT: pz5_metrics_test.csv, pz5_tuning.csv; term_07 | OK |
| S10 | Крок 10: порівняти результати | pz5_metrics_test.csv, pz5_random_vs_chrono.csv; term_08, term_10 | OK |
| S11 | Крок 11: візуалізувати результати | 10 рисунків (fig01–fig10), дефекти в fig03, fig07, fig08 (G4, G5, G6) | GAP |
| S12 | Крок 12: проаналізувати результати | Текст аналізу відсутній (G2) | GAP |
| S13 | Крок 13: сформулювати висновки | Висновків немає (G3) | GAP |
| S14 | Крок 14: підготувати звіт | pz5/report.md, build/ (docx/pdf) відсутні (G1) | GAP |
| M1 | Метрики класифікації (Accuracy, Precision, Recall, F1, ROC-AUC) | results/pz5_metrics_test.csv | OK |
| M2 | Метрики регресії та силуетний коефіцієнт | Не застосовуються: обрано класифікацію (завд. 1) | N/A |
| R1 | Звіт: титульна сторінка, назва, мета, короткі теорія | Звіту немає (G1) | GAP |
| R2 | Звіт: лістинг програми з коментарями | code/pz5_spark.py, make_figures.py, make_screens.py з українськими коментарями є; у звіті (Додаток А) немає (G1) | GAP |
| R3 | Звіт: результати зі скріншотами | Скріншоти є; звіту немає (G1); заголовки term_* хибні (G7) | GAP |
| R4 | Звіт: висновки за планом | Немає (G3) | GAP |

## 2. Правила (RULES.md)

| № | Правило | Докази | Статус |
|---|---|---|---|
| P1 | Seed 42 | code/pz5_spark.py: SEED = 42, np.random.seed; seed=SEED у RF, GBT, randomSplit; results/env_versions.csv (seed 42) | OK |
| P2 | timings.csv (час кроків і машина) | results/timings.csv (14 кроків, total_script 1954,59 с); машина в env_versions.csv (Xeon 2,1 ГГц, 4 ядра, 15 ГБ) | OK |
| P3 | env_versions.csv (версії ПЗ, CPU, RAM) | results/env_versions.csv: Java 21, Spark 4.3.0, PySpark 4.3.0, Python 3.13.18, pandas, numpy, OS, CPU, RAM | OK |
| P4 | Українські коментарі в коді | code/pz5_spark.py (53 рядки з кирилицею), make_figures.py (37), make_screens.py (4) | OK |
| P5 | Хронологічний поділ без витоку | results/pz5_split.csv, Storage (spark_ui_storage.png); ембарго 24 год; ознаки лише з минулого (log_rv_over_thr, rolling-вікна, lag) | OK |
| P6 | Базові лінії | Мажоритарний клас, персистентність, ранжування log(rv/поріг) у pz5_metrics_test.csv | OK |
| P7 | Важливість ознак | results/pz5_feature_importance.csv; fig05; term_09 | OK |
| P8 | Матриця помилок | results/pz5_metrics_test.csv (tn, fp, fn, tp); fig04 | OK |
| P9 | Аналіз помилок | results/pz5_errors_*.csv; fig06 | OK |
| P10 | Скріншоти реальні (вивід команди з `$`) | Spark UI (6 PNG) реальні, прапорець 13:43:33 збігається з run.log. Термінальні PNG (term_01–term_12) містять вивід справжньої програми, але заголовок `$ ...` є псевдокомандою, а не командою, що дала вивід (G7) | GAP |
| P11 | Українські підписи на рисунках | fig01–fig06, fig09, fig10 українські; fig07 і fig08 мають англійські легенди (G6, G5) | GAP |
| P12 | Рисунки читабельні та коректні | fig03: від’ємний AP (G4); fig08: обрізані підписи (G5); решта перевірено, читабельно | GAP |
| P13 | Заповнювачів `[СКРІНШОТ]` / `[ЗАПОВНИТИ]` немає | Немає фабрикованих значень; усі числа зведені з CSV | OK |
| P14 | defense.md (Правила: звіт + захист) | Відсутній (G1) | GAP |

## 3. GAP-список (що саме бракує або виправити)

G1. Звіту немає. Потрібно створити `/home/user/AVD/work/pz5/report.md` (і `build/` з docx/pdf) з: титульною сторінкою, «Мета роботи», «Короткі теоретичні відомості» (до 350 слів), «Результати» з підрозділами 1–8 за завданнями, таблицями (порівняння моделей з pz5_metrics_test.csv), рисунками (8–14, не більше 12 рисунків; доступно 28 кандидатів: 10 рисунків плюс 18 скріншотів), «Висновки» і «Додаток А» з лістингом pz5_spark.py. Також відсутній `pz5/defense.md` за RULES.

G2. Немає текстового аналізу для завдання 1 (обґрунтування задачі), завдання 8 (найінформативніші ознаки, помилки, краща модель) і кроку 12. Є лише числа.

G3. Немає висновків за планом методички (крок 13).

G4. `figures/fig03_roc_pr.png`: у легенді PR-кривих AP від’ємний (LR -0,782, RF -0,785, GBT -0,803, персистентність -0,544). Причина: у `make_figures.py` `ap = -np.sum(np.diff(rc) * pr[1:])`, а `rc` зростає. Значення персистентності також не збігаються з pz5_metrics_test.csv (pr_auc 0,6134). Виправити знак і узгодити обчислення з PR-AUC зі Spark (або явно пояснити різницю в підписі).

G5. `figures/fig08_model_comparison.png`: підписи осі X обрізані функцією `l[:22]` (наприклад «Мажоритарний клас (зав», «Ранжування за log(rv_t»). Легенда лівої панелі англійська (f1, roc_auc, pr_auc, accuracy). Повернути повні підписи українською або перенести в ширший фігуру.

G6. `figures/fig07_random_vs_chrono.png`: легенда англійська («chronological 80/20», «random 80/20 (shuffle)»). Перекласти українською (наприклад «хронологічний 80/20», «випадковий 80/20»).

G7. Термінальні скріншоти `screenshots/term_01`–`term_12`: рядок `$ ...` є псевдокомандою або описом, а не командою, яка створила вивід. Приклади: term_01 показує `python3 -c 'import pyspark...'; java -version; nproc; free -g`, але вивід є таблицею, яку друкує pz5_spark.py; term_06 показує `Pipeline(stages=[..., StandardScaler, LogisticRegression])`, а вивід містить три пайплайни LR, RF, GBT. Потрібно або зняти вивід реальною командою (наприклад, `python3 code/pz5_spark.py ... | tee`), або змінити рядок `$` на команду, яка справді дала вивід, і повторно згенерувати PNG через make_screens.py.

G8. `results/pz5_split.csv` (і `term_04_split.png`): параметр `train_end_nominal` = 2025-02-01 20:00 є останнім рядком ембарго (rn 62133), а не кінцем навчальної вибірки. Останній навчальний рядок: 2025-01-31 20:00 (rn 62109, перевірено за pz5_target_series.csv). Перейменувати на `embargo_end` і додати `train_end_actual`, або виправити значення у скрипті.

## 4. Зауваження (не GAP, але розкрити у звіті)

- Найкраща модель обрана за PR-AUC на тесті (pz5_best_model.csv; pz5_spark.py рядок «best_name»; make_figures.py «best» за M.kind). Вибір за тестом є витоком для звіту. За часовою CV (pz5_tuning.csv) переможець той самий: GBT 0,7468 > LR 0,7456 > RF 0,7234. Висновок не змінюється, але це треба написати явно або обирати за CV.
- Випадковий поділ дає вищі похибки, ніж хронологічний (GBT ROC-AUC 0,872 проти 0,901). Це протилежне очікуваному. Пояснення в INDEX.md («ознаки не містять міток») недостатнє для звіту. Також n_test різний: 15746 (випадковий, `randomSplit`) проти 15534 (хронологічний). Порівняння не повністю рівноцінне.
- Figure 04 та fig08 використовують крапку в числах (0.34), правило вимагає кому в тексті та таблицях. Для підписів рисунків це не обов’язково, але узгодити зручно.
- Папка results/model/ є в git-ігноруванні; модель присутня на диску, але не в поставці репозиторію, якщо він буде здаватися через git.
- Рисунків і скріншотів 28 (10 + 18), правила дозволяють 8–14 і максимум 12 рисунків. Вибір для звіту є обов’язковим.
