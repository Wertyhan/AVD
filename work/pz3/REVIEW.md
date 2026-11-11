# REVIEW – ПЗ3 (Kafka + Spark Structured Streaming, BTC/USD 1 хв)

Reviewed: build/ПЗ3_АВД_Зелінський_ДМм-26-1.pdf (43 pp: body pp. 2–16, Appendix pp. 17–43), report.md, results/, code/, against cards/pz3.md + raw/pz3.txt, RULES.md, FORMAT.md.

Overall: all 8 guide tasks are present, in order, with evidence. The technical core is above average: event time is used rather than processing time, OHLC is computed with min/max(struct(event_time, x)) instead of first/last, the append last-window effect is explained and confirmed (final watermark in progress.csv is 2022‑11‑12T23:49), the 7‑day baseline uses `rolling(...).shift(1)` with prewarm `< ts_first` (no look-ahead), and stream and offline results match with zero differences. Numbers are consistent: I spot-checked 40+ values and found none invented. The main problems are one screenshot that contradicts its caption, metrics mixed across two runs and code versions, and an anomaly evaluation that is in-sample.

---

## CRITICAL

**C1. Figure 4 contradicts its caption and the text.** (§3, p. 6, `screenshots/ui_streaming_tab.png`)
The caption reads «Сім активних потокових запитів», and the text says «Усі запити мають статус RUNNING… 68,73–75,71 рядків/с». The image shows *Active Streaming Queries (1)*: only `anomaly`, app `PZ3-bitcoin-streaming-anomaly-v2`, 77,35 rows/s, latest batch 5. INDEX.md says it was taken during a separate UI-only run (`bigdata-topic-v2-ui`, 15 000 messages), which is not one of the analysed runs. It overwrote the correct shot, now saved as `ui_streaming_tab_v1.png` (7 queries, 68,73–75,71). The page also has about 40 % empty white space, which breaks RULES 2a.
*Fix:* put `ui_streaming_tab_v1.png` back as Figure 4, clipped to the table (element/clip screenshot, no empty area). Alternatively, re-shoot the 7-query run and update the numbers.

## MAJOR

**M1. Performance and latency mix two runs and two rule versions. The v2 metrics exist but are not used.** (§8.4, Table 8, §3, Conclusion 8)
Table 8, the latency figures 5,791 s mean and 12,354 s max, and foreachBatch 0,643 s all come from the v1 run (`results/stream/batch_stats.csv`, `pz3_metrics_summary.csv`), where `anomaly` ran rule v1 with a 60‑min buffer. The anomaly results in the report are v2, and `results/stream_anomaly_v2/batch_stats.csv` holds the matching metrics: weighted latency 5,28 s, max 11,41 s, foreachBatch 0,30 s per data batch. §3 then mixes runs the other way: «мікробатчі містили 950–980 рядків» is from v2 (305–980), while v1 had 188–962 with a median of 937. Using v1 metrics for the *window* queries is acceptable, because that code did not change. For the `anomaly` row and for latency it is not.
*Fix:* label Table 8 «перший прогін, віконні запити». Add a row «anomaly (v2, окремий прогін)» with numbers from `stream_anomaly_v2/progress.csv` and `batch_stats.csv`, and quote v2 latency and foreachBatch time in the text and in Conclusion 8. Make §3 refer to one run only. The cleanest fix is a single full re-run of all 7 queries with the v2 code, after which Table 3, Table 8, Figure 4 and the latency are regenerated from that run.

**M2. Appendix A does not reproduce the first run.** (А.2, А.11)
The results in Table 3, Table 8, Figures 1–4 and the latency come from `streaming_job_v1.py`, which is not in the appendix. А.11 `run_pipeline.sh` calls the current `streaming_job.py` without the required `--history-parquet`, so the listed script would stop at argparse.
*Fix:* do the single re-run from M1, or add `--history-parquet` to run_pipeline.sh and say in the text which file produced which table. Remove the comments that point to things that don't exist or reveal the internal workflow: «докладніше у ноутбуці» (there is no notebook) and «зафіксовано для відповідності RULES» (А.2).

**M3. The anomaly rule is chosen and evaluated on the same five event days, so the evaluation is in-sample.** (§6.2–6.3, Tables 4, 5, 7, Conclusion 6)
All 19 variants are ranked by lift on exactly 12–13.03.2020, 19.05.2021 and 08–09.11.2022. The same days are then the «evaluation» in Table 7. The budget k is calibrated on 2018–2026, which includes the replayed periods and data after them, so the threshold is chosen with hindsight. «Lift 38,4 проти 0,3» is therefore an optimistic, in-sample number.
*Fix (cheap, from existing outputs):*
- Leave-one-event-out: rank the variants by lift without each event in turn, and show that the 7d/Q90 choice is stable.
- Temporal split: calibrate the scale, base and k on 2013–2019, then report lift and ranks on 2020–2026 as a hold-out.
- Report ranks of independent known events not used in design, for example 2013‑04‑10, 2017‑09‑14, 2018‑11‑14/19, 2021‑01‑11, 2021‑12‑04, 2022‑06‑13, 2024‑08‑05. Each must be confirmed in results.
- Say explicitly in the text that the Table 4 numbers are design-set numbers.

**M4. The watermark choice and its semantics in this setup are not justified by data, and one fragility is not mentioned.** (§5)
- (a) The text says «реального запізнення немає», which means 0 min would work just as well. The 10 min value is not tied to any data. Measure event-time disorder at arrival, which is 0 because the producer sorts. Report `numRowsDroppedByWatermark` from `recentProgress`, which should be 0. Then justify 10 min as tolerance for a *live* feed, for example exchange reconnects or candle revisions, and state the cost: at 1× replay, append output lags by 10 min of *wall time*. The report only gives «≈0,1 с» for the 6000× replay.
- (b) The watermark is global per query, the max over all partitions. The periods 2020‑03 and 2021‑05 sit in partition 1 and 2022‑11 in partition 0. This works only because the producer sends the periods one after another and Spark consumes live. With a backlog, such as a restart or a topic that is already full, `maxOffsetsPerTrigger` reads both partitions in the same batch. The watermark then jumps to Nov 2022, and the remaining 2020/2021 rows are silently dropped as late. Discuss this and give the mitigation: sequential replay, a shifted contiguous event timeline, or one query per period.
- (c) The «Global Watermark Gap ≈ 2·10⁸ с» figure does not appear in any results file or screenshot (RULES integrity 1), and it varies from about 1,3·10⁸ to 2,1·10⁸ s depending on the period. Show it on a screenshot or remove it.

**M5. Task 8 «закономірності» is thin and partly misread.** (§8.3, Conclusions)
It is three sentences with no figure or table, although `figures/pz3_patterns.png` exists and is not used. The patterns come from offline scoring of the whole history, not from the stream; this needs to be said. The peak at 14 UTC is partly an artefact of a baseline with no seasonal adjustment: the 7‑day base ignores intraday volatility seasonality, and the tested `W7d_same_hour_Q90` variant exists. Discuss that. «Вдвічі рідші у вихідні» compares the minimum day with the maximum day: Wednesday is 2,6× the weekend, while the weekday average is about 2,1×. The rule ignores direction, yet the text speaks only of «обвали». In the top dates, 2019‑04‑02 (+0,169, rank 3), 2019‑10‑26, 2023‑01‑12 and 2020‑07‑27 are rallies. Streaming-specific patterns are missing: anomalies cluster in bursts within hours (Figure 7: batches 6–7 hold 146 of 272), and price and volume move together.
*Fix:* add the patterns figure (weekday/hour/year) with 2–3 sentences of interpretation. Add the cluster and burst observation and the rallies vs crashes split. Add one line on Task 8 to the conclusions.

**M6. The stream == offline check is presented as validation, but it only tests one thing.** (§8.1, Table 6)
Both sides call the same `score()`, and the stream buffer is prewarmed from the same parquet. Max |Δ| = 0 proves that results do not depend on how minutes are split into microbatches or keys, which is good to show. It does not show the formula is correct or that there is no look-ahead.
*Fix:* say this in one sentence. Add an independent check:
- a perturbation test: change all minutes after t and confirm that z at t is unchanged;
- a hand recomputation of 2–3 flagged minutes with numpy from the raw CSV (Q90 of the 10 080 preceding active |r|).

## MINOR

1. **Partitioning** (§1, Table 2): there is an empty partition, and the key `YYYY-MM` is a period-id hack. librdkafka's default partitioner is consistent_random (CRC32), not Java's murmur2, so the mapping depends on the client. *Fix:* set the partition explicitly per period, or use `partitioner=murmur2_random` and say so. The honest discussion is already good.
2. **Producer** (А.1): `acks=1` and no `enable.idempotence` mean a retry can duplicate or reorder messages within a partition. StreamScorer then silently drops any minute with `ts ≤ last`. *Fix:* set `enable.idempotence=true` and mention it in §7 as part of the exactly-once chain.
3. **Kafka source** (§3): `failOnDataLoss=false` (А.2, line 51) contradicts «жодне повідомлення не пропущено». `earliest` applies only on the first start; after that the checkpoint offsets win. Each of the 7 queries is a separate Kafka consumer, so the topic is read 7 times. That is not a «одне джерело».
4. **Daily query state** (§4, `daily_mem`): `groupBy(to_date(event_time))` does not use the watermarked column. The state is never evicted, and append mode would be rejected. *Fix:* use `window(event_time, "1 day")`.
5. **Recovery claim** (§7): «стан відновлюється повторним прогрівом з parquet» is true only because the parquet already contains the replayed minutes. A live stream would need its own state store, for example `applyInPandasWithState` or persisting the buffer per batch. Say so.
6. **Thin-market criterion** (§6.2, Figure 6): the right panel is labelled «Хибні тривоги тонкого ринку», but §8.2 calls the 2013 top dates «реальними обвалами». Rename the panel to «частка позначок 2012–2016» and justify the criterion more carefully.
7. **Volume confirmation is almost inert**: 5 489 → 5 437 flags, −1 %, per threshold_selection. State this; the text currently presents it as a design element.
8. **Budget 0,10 %** (§6.3) is not justified by data. Add one sentence, for example on operator capacity or the knee of the k-curve. Table 5 should include k = 8 (0,1321 %), because that row is what makes 9 «the smallest k».
9. **Baseline 1,3 vs 1,38 per day** (§8.1 vs §6.3): 1,3 is the rate on 2018+ days that are not event days (0,0905 %), and 1,38 is the rate on all 2018+ days. Name both.
10. **«виявлено повністю»** (§8.2): there is no minute-level ground truth. Use «виявлено з найвищими рангами».
11. **«незміщеними оцінками σ»** (§6.2): the right term is «спроможними (узгодженими) за нормальності». |r| has heavy tails, so this holds only under that assumption.
12. **Table 4 «Позначено 2012–2016, %»** is the *rate* of minutes flagged, but the text also uses «60,6 % позначок», which is the *share of flags*. Define both, or use one.
13. **Missing screenshots**: Tasks 4/5/7 have no screenshots. The console sink output (`term_07_spark_console_batch.png`), the verification (`term_09`) and the parquet output listing exist but are not used. There is room for them: 8 figures used, 12 allowed.
14. **Figure 1 caption** says «Створення та опис», but the image shows only `--describe`, and it is cut off at «Replicati…». Figure 2 is `cat` of a log file, not the producer command itself.
15. **Legibility**: tick labels on Figures 5 and 6 are about 5–6 pt at print size. Enlarge the fonts to 10+ pt, or use fewer panels per figure.
16. **Style**: «Позначимо r_t» is first person; use «Нехай r_t = …» or «Позначено». The English term «lift» needs a one-time Ukrainian gloss. Table 1 «confluent-kafka 2» gives an incomplete version.
17. **Appendix**: at 27 pages it is excessive. Screenshot tooling (А.13–А.15 term_shot/ui_shot) and figure scripts are not «лістинг програми» for the lab; move them out or shorten them.
18. **Conclusions**: item 3 is trivial. There is no item for Task 8 patterns, see M5. The final sentence matches the guide's «Висновки» plan, which is fine.

## Verified OK (spot-checks)

The following values match results/:
- 30 240 messages, 327,041 s, 92,465 msg/s; broker start 10,11 s
- partitions 10 080 / 20 160 / 0
- Table 3, all cells
- Table 4, all 9 rows
- Table 5, rows k = 3/5/7/9/10/20
- v1 9,9–27,1 % and 0,53–0,90
- thin-market zero-return shares 42,6 / 29,4 / 25,0 / 0,4 %
- z_vol p99,99 = 3,067, max 4,239; vol-only 60,6 %; z_vol > 2 lift 23,2
- Table 6, all cells, including 224 flat minutes
- Table 7, all cells, out of 1 572 dates
- top-20 split 13 / 1 / 6
- weekday, hour and year rates
- Table 8, all cells; loadavg 4,9–12,3
- latency 5,791 / 12,354, foreachBatch 0,643 (v1 run)
- 75,5 s, 340,7 s, 310,945 s
- hourly volume 6 000–9 000 BTC (8 925 at 12.03 11:00)

Style: prose about 2 380 words and theory 212 words, both within budget. No em dashes, no banned phrases, no bold.

**Grade estimate:** about 82/100 («добре») as submitted. C1 is an evidence defect an instructor will notice immediately. After C1 and M1–M3 are fixed: about 92–95 («відмінно»).
