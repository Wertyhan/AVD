# CHECK: ПЗ3 (Kafka + Spark Structured Streaming), перевірка повноти

Джерело вимог: `work/cards/pz3.md` (сирий текст `work/cards/raw/pz3.txt`). Файли з суфіксом `_v1` не використовувались як докази. Перевірено всі не-v1 PNG (27 шт.: 16 у `screenshots/`, 11 у `figures/`); усі читабельні, підписи українською, термінальні знімки мають заголовок `$`.

## Частина 1. Завдання і кроки (у порядку методички)

| # | Завдання / крок | Доказ | Статус |
|---|---|---|---|
| 1 | Встановити і запустити Kafka | `screenshots/term_01_broker_start.png` (KRaft, Kafka Server started); `results/env_versions.csv` (Kafka 3.7.0); `results/logs/kafka_start_excerpt.log`; `results/timings.csv` broker_start 10,11 с | OK (заголовок `$` є скороченим, а не точною командою, див. GAP 3) |
| 1.2 | Створити topic `bigdata-topic` | `results/logs/topic_create.log`, `topic_describe.log` (3 партиції, RF=1); `screenshots/term_02_topic_create_describe.png` | OK за логами; GAP за знімком (GAP 3) |
| 2 | Producer з затримкою | `code/producer.py` (затримка `--delay-ms 10`, коментарі українською); `screenshots/term_03_producer_log.png` (30 240 повід., 0 помилок, 327,041 с); `results/producer_stats.json` | OK |
| 2 | Затримка імітує потік | `producer.py` рядок `time.sleep(a.delay_ms/1000)`; 92,5 повід./с | OK |
| 3 | Spark Structured Streaming отримує з Kafka | `code/streaming_job.py` (readStream kafka, `subscribe`, `startingOffsets=earliest`); `screenshots/ui_jobs.png`, `ui_streaming_tab.png` (7 запитів RUNNING) | OK |
| 3 | Консольний споживач (перевірка тем) | `screenshots/term_04_console_consumer.png` (8 повідомлень з offset, partition, CreateTime); `term_05`, `term_06` | OK (знімки 05 і 06 складені, GAP 3) |
| 4 | Декодування (from_json, явна схема, cast bytes→string) | `streaming_job.py` блок «Завдання 4», `event_time = cast(ts as timestamp)` | OK |
| 4 | Перетворення типів | `ts` LongType → timestamp, JSON → Double; `kafka_epoch` | OK |
| 4 | Фільтрація | `valid` (некоректні 0 з 30 240, `results/stream/batch_stats.csv`) | OK |
| 4 | Групування і статистичне агрегування | `daily_stats_final.csv` (21 доба, 30 240 хв), `results/stream/daily_stats.csv` (avg/std/min/max/volume за датою) | OK |
| 5 | Віконна обробка, event time, watermark | `streaming_job.py` `withWatermark("event_time","10 minutes")`, `F.window` 15 хв і 1 год; `results/stream/win15m_final.csv` (2 016), `win1h_final.csv` (504); `results/pz3_window_verification.csv` (збіг з pandas, max Δ 0; volume ≤ 2,7e-12); `screenshots/term_09_window_verification.png`; `figures/pz3_window_ohlc.png` | OK |
| 5 | Watermark видно в UI | `screenshots/ui_query_sink_win1h.png`: Late Rows Dropped By Watermark = 0; Global Watermark Gap присутній | OK (див. GAP 7) |
| 6 | Правило виявлення аномалій (v2) | `code/anomaly_rule.py` (формула в docstring, `score()`, `StreamScorer`); `results/ANOMALY_DESIGN.md` (обґрунтування, 19 варіантів); `results/pz3_design_comparison.csv`; `figures/pz3_design_comparison.png` | OK |
| 6 | Калібрування порогу k | `results/pz3_threshold_selection.csv`, `pz3_trigger_counts_offline.csv`; `screenshots/term_12_threshold_selection_v2.png`; `figures/pz3_k_vs_triggers.png`, `pz3_trigger_counts_grid.png` | OK |
| 6 | Аномалії в потоці (foreachBatch) | `screenshots/term_08_spark_anomaly_foreachbatch.png` = вивід `run/logs_v2/spark_job.log` (перевірено: 26 рядків збігаються); `results/stream_anomaly_v2/job_summary.json` (30 240 оцінено, 272 аномалії); сума аномалій у логу = 272 | OK |
| 6 | Аномалії: графіки подій | `figures/pz3_price_anomalies_2020-03.png` (166), `_2021-05.png` (29), `_2022-11.png` (77) | OK |
| 7 | Збереження у файл/локальне сховище | parquet `results/stream/parquet/win15m|win1h`; `results/stream_anomaly_v2/anomalies.parquet|csv`, `scored.parquet`; `results/pz3_anomalies.csv` (5 437 рядків); `results/pz3_anomalies_stream.csv` (272) | OK (див. GAP 5) |
| 8 | Закономірності при потоковій обробці | `results/pz3_stream_vs_offline.csv` (max |Δz| = 0, 272 = 272); `pz3_lift.csv`, `figures/pz3_lift.png`; `pz3_known_events_check.csv`; `pz3_anomalies_by_hour_2018plus.csv`, `_by_weekday_2018plus.csv`, `_by_year.csv`; `figures/pz3_patterns.png`, `pz3_price_anomalies_*.png` | OK |
| Метрики | throughput, latency, тривалість мікробатчів: `results/pz3_metrics_summary.csv`, `results/stream/progress.csv`, `batch_stats.csv`; `screenshots/term_11_metrics.png`; `figures/pz3_batch_duration_throughput.png`, `pz3_input_vs_processed.png` | OK |
| Метрики | Spark UI: Jobs, Stages, Streaming, Query | `ui_jobs.png`, `ui_stages.png`, `ui_streaming_tab.png`, `ui_query_sink_win1h.png`, `ui_query_anomaly.png` (v1-прогін) | OK; GAP 2, 9 |
| Час | timings | `results/timings.csv`, `timings_stages.csv`, `timings_anomaly_v2.csv`, `results/load_during_run.csv` | OK |
| Середовище | версії, CPU/RAM | `results/env_versions.csv` (4 vCPU, 15 GiB, Python 3.13, Spark 4.3.0, connector 4.3.0) | OK |
| Код | коментарі українською | `producer.py`, `streaming_job.py`, `anomaly_rule.py`, `run_pipeline.sh`, `run_anomaly_v2.sh` | OK |
| Seed | seed 42 | Конвеєр детермінований, випадкових величин немає; seed 42 не записано в коді чи `env_versions.csv` | GAP 6 (мінор) |
| 13 | Підготувати звіт | У `pz3/` немає `report.md`, `defense.md`, `build/`; висновків і листингу Додатку А немає | GAP 1 |

## Частина 2. GAP-список

1. **Звіт (крок 13) відсутній.** Немає `work/pz3/report.md` (титул, мета, теорія, розділи 1–8, висновки, Додаток А з коментованим лістингом), `defense.md`, `build/`. Усі числа вже є в `results/`, тому дописати звіт можна без повторних запусків.
2. **`screenshots/ui_executors.png` порожній.** Показує лише «Loading...». Треба перезняти сторінку Executors після завантаження.
3. **Заголовки і вміст термінальних знімків складені вручну.** `term_02` (`--create ; ... --describe`, зайвий рядок `--- kafka-topics.sh --list`, сторонній topic `t1`), `term_05` (`--list ; ... --describe`), `term_06` (рядок «consumed by pz3-verify: 30243 lines» не є виводом команди), `term_10` (`cat run/logs_v2/derived/counts.txt`, файл зведений із CSV). Перезняти кожну команду окремо з точним рядком, як вона виконувалась. `term_01` і `term_03` мають скорочені заголовки (відносний шлях до CSV), що треба виправити.
4. **Немає точного повторного запуску команд з `run_pipeline.sh`.** Знімки не відтворюють `$W/data/raw/...` шляхи. Це мінор, але узгодити.
5. **Parquet-приймач не записує останнє вікно.** `win15m` 2 015 з 2 016 і `win1h` 503 з 504 (append + watermark). Пояснено в `INDEX.md`, повні дані є в `_final.csv` (memory sink). Для звіту треба явно написати про це.
6. **Seed 42 не задано.** RULES вимагає seed 42 скрізь. Пайплайн детермінований, але записати `seed=42` (або «не використовується») у `env_versions.csv` і в коментар коду.
7. **Global Watermark Gap ≈ 2·10⁸ с на `ui_query_sink_win1h.png`.** Значення відповідає різниці між поточним часом (2026) і подіями 2020 р., тобто не є помилкою обробки. Пояснити в звіті, інакше виглядає як аномалія.
8. **Spark UI для запиту аномалій v2 відсутній.** `ui_query_anomaly.png` знятий із першого прогону (v1). Для v2 є лише лог (`term_08`). Бажано знімок UI під час прогону `bigdata-topic-v2`.
9. **Порт UI.** `streaming_job.py` задає `spark.ui.port=4040`, фактично UI на 4041 (INDEX це пояснює). Узгодити текст звіту.
10. **Кількість ілюстрацій.** Є 27 не-v1 PNG (16 знімків і 11 фігур). Для звіту по RULES потрібно 8–14 рисунків; відбір вже можливий. Мітки легенди в `pz3_input_vs_processed.png` частково англійською (`inputRowsPerSecond`), у `pz3_trigger_counts_grid.png` порядок k неочевидний (k=9 після k=10). Мінор.
