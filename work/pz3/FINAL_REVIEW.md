# FINAL REVIEW – ПЗ3 (Kafka + Spark Structured Streaming, BTC/USD 1 хв)

Reviewed: build/ПЗ3_АВД_Зелінський_ДМм-26-1.pdf (52 pp: title p.1, body pp. 2–16, Appendix А pp. 17–52; every body page rendered and inspected, appendix pages sampled). I also reviewed report.md, results/ (INDEX.md, ANOMALY_DESIGN.md, CHANGES_vs_old_run.md, CSVs), code/, and checked them against cards/pz3.md, raw/pz3.txt, RULES.md, FORMAT.md, REVIEW.md and NUMCHECK.md.

## Verdict: FIX-FIRST (cheap fixes, about 1 hour; no re-run needed)

The content and technical part are now at «відмінно» level. Every previous CRITICAL and MAJOR item is fixed (see the status table). What remains is presentation: chart fonts in print are below the project's own legibility rule, the appendix leaks the internal workflow and is overlong, and Tasks 4 and 7 have no illustrating screenshot. None of these needs new computation.

Grade estimate: about 90/100 as is, and about 94–95 after the must-fix items.

---

## Status of previous CRITICAL / MAJOR items

| Item | Status | Evidence |
|---|---|---|
| C1. Figure 4 (UI) contradicted its caption | FIXED | Now Figure 3 (`ui_streaming_tab_crop.png`): 7 queries RUNNING at microbatch 22, Avg Input 85,75–87,56, Avg Process 314,64–814,77. These match the text on p. 6. The image is cropped to the table with no empty area. |
| M1. Metrics mixed two runs and two rule versions | FIXED | One run of all 7 queries with the current code (INDEX.md, CHANGES_vs_old_run.md). Table 8, latency (5,651 / 5,823 / p95 10,377 / max 12,445 s), foreachBatch 0,716 s and batch size 11–977 (median 949) all match `pz3_metrics_per_query.csv`, `pz3_latency_summary.csv`, `pz3_metrics_summary.csv` and `stream/batch_stats.csv`. |
| M2. Appendix did not reproduce the run | FIXED | All 23 listings А.1–А.23 are byte-identical to `code/*` (checked by script). `run_pipeline.sh` passes `--history-parquet`. The «ноутбук» and «RULES» comments are gone. See the new minor leak in D2. |
| M3. In-sample evaluation of the rule | FIXED | §6.5 adds leave-one-event-out (mean held-out lift 30,7 vs 38,4) and a 2013–2019 → 2020+ time split (k = 8,38, lift 42,5, base rate 0,094 %). Figure 8 and the Table 7 «Lift поза вибіркою» column are new. Table 4 is labelled «проєктна вибірка». The report honestly states that the scale changes across folds. Independent known events outside the design set were not added; the time split covers this need adequately. |
| M4. Watermark not justified, fragility not discussed | FIXED | Disorder 0 of 30 240 and `numRowsDroppedByWatermark` = 0 (`pz3_watermark_disorder.csv`, `pz3_watermark_gap.csv`). 10 min is justified as tolerance for a live feed, and its cost is stated. The global watermark is explained. The backlog experiment shows 14 080 of 30 240 rows kept with a 3 000 limit and 30 240 kept with no limit. The watermark gap now has a source (1,261–2,107·10⁸ s, median 1,730·10⁸). |
| M5. Task 8 patterns too thin | FIXED | §8.3 adds Figure 9, the weekday/weekend ratio 2,17, the hour profile 07 vs 14 UTC with a seasonality caveat, crashes vs rallies (30/20 and 25/25), and bursts (71,1 %). It also states that the patterns come from offline scoring. All of this is reflected in the conclusions. |
| M6. stream == offline presented as validation | FIXED | §8.1 states what that check proves. It adds an independent pandas recomputation (200 + 200 minutes, 0 mismatches) and a look-ahead perturbation test (40 of 40 equal), from `pz3_independent_check.csv` and `pz3_lookahead_perturbation.csv`. |

Previous minor items 1–8, 10–12, 14, 16 and 18 are fixed. Items 9, 13, 15 and 17 are still open; see below.

---

## Remaining defects

### MUST FIX (before submission)

**D1. Chart text in print is below the 8 pt rule (RULES 2b). This was previous minor 15 and it is still open.** The builder caps figure height at 8,5 cm, so the two-panel figures are shrunk in width:

| Figure | Printed width | Tick text in print |
|---|---|---|
| Figure 6 | 9,5 cm | ≈ 5 pt; rotated x-labels hard to read |
| Figure 8 | 10,2 cm | ≈ 6 pt |
| Figures 4, 5, 9 | 11,5 cm | ≈ 6,5 pt |

The figures were made at 6,3 in (16 cm) width.
*Fix:* insert two-panel figures at 16 cm width (height about 12 cm), or redraw them as single-row panels. Also, in Figure 8's bottom panel the legend covers the «7д IQR(r)/1,349» test bar. Move the legend outside the axes.

**D2. The appendix leaks the internal workflow and machine paths.**
- А.16 `figstyle.py` docstring and comment: «Якщо є work/tools_report/plotstyle.py…», «спільний хелпер координатора, якщо вже доданий».
- `sys.path.insert(0, "/home/user/AVD/work/tools_report")` appears in А.16 and А.19.
- Hard-coded `/home/user/AVD/work/...` paths appear in А.13, А.14, А.15 and А.21.

*Fix:* remove the «координатора» / tools_report wording. Replace absolute paths with command-line arguments or a relative `P = Path(__file__).parents[1]`. Re-sync the code files and the appendix so that Appendix A still equals the code.

**D3. The appendix is overlong and includes non-program tooling. This was previous minor 17 and it is worse now: 36 pages, up from 27.** The guide asks for «лістинг програми з коментарями». Screenshot and figure tooling are not that program:

| Listing | File |
|---|---|
| А.16 | figstyle.py |
| А.18 | ui_shot_run.py, with a Chromium path |
| А.19 | term_shot.py |
| А.20 | show_csv.py |
| А.23 | crop_ui_streaming.py |

*Fix:* drop these listings. Optionally also drop А.14 and А.15 (figure scripts). Add one sentence that the auxiliary scripts are in the project's code folder. Target 20–25 pages.

### SHOULD FIX

**D4. Tasks 4 and 7 have no illustrating screenshot. This was previous minor 13 and it is still open.** The guide requires results «проілюстровані скріншотами» per task. Task 4 (decoding, filter, grouping) has no output shown. Task 7 (saving) is text only. Nine figures are used of the 12 allowed.

*Fix:*
- Task 4: add the existing `term_07_spark_console_batch.png` (console sink output of windowed or daily aggregates).
- Task 7: add one terminal shot of the saved outputs, for example `ls stream/parquet/*` plus `head -3 results/pz3_anomalies.csv`, or `term_09_window_verification.png`.

**D5. Table 8 query names do not match the Spark names.** Table 8 uses the dict keys `mem_daily`, `mem_win15m` and `mem_win1h`. Spark's `queryName` values, shown in Figure 3 and §4, are `daily_mem`, `win15m_mem` and `win1h_mem`. Use the Spark names in Table 8, or add a note.

**D6. Two baseline rates are not reconciled. This was previous minor 9.** §8.1 says «базову (1,3 на добу)», while §6.3, Table 5 and the conclusions say 1,38. The 1,3 is the rate on 2018+ days that are not event days (`pz3_periods_vs_baseline.csv`); 1,38 is the rate on all 2018+ days. Add «у днях без подій» in §8.1.

### MINOR

- Conclusions and §6.5 quote lift 38,4 for «правило v2». 38,4 is the equal-budget variant (k = 8,87) from Table 4; the deployed k = 9 has 38,5 (Table 5, `pz3_lift.csv`). Say «38,4 за бюджету 0,10 %», or use 38,5.
- §8.2 says «ранг – місце серед 1 572 дат». The 2018+ rank column is out of 1 293 dates (`pz3_known_events_check.csv`). Add the second denominator.
- Figure 7 is about half Spark `[Stage …]` progress-bar noise. It does not show batches 7, 8 or 27, which the text names as the largest. Re-crop with `grep -v Stage` so it shows batches 7, 8, 18 and 27.
- Figure 5, lower panel: four legend entries but only one visible line, because the lines coincide. Say «лінії збігаються» in the caption. In the upper panel the watermark trails max event time by one microbatch because it is updated at the end of a batch. One clause in the text would explain this.
- Time split: lift is reported only for the 5 known days (42,5). The same file gives 11,3 on all 32 big-move days of 2020+ (`pz3_oos_summary.csv`), which is worth one clause for honesty.
- Layout: about 35 % of p. 9 and about 25 % of p. 12 are blank because a table or figure was pushed to the next page. This is acceptable, but tightening it would help if pages are re-flowed anyway.
- Table 7 cell text is 9 pt; FORMAT says 11, or 10,5 for wide tables.
- Prose uses `r_t`, `C_{t−1}` and `V_t` in underscore notation. This is acceptable inside formulas, but «r_t» in running text reads like code.

---

## Checks performed

**Guide tasks 1–8, in order, with evidence.**

| Task | Section | Evidence |
|---|---|---|
| 1 | §1 | Table 1, Figure 1 |
| 2 | §2 | Table 2, Figure 2, 10 ms delay |
| 3 | §3 | Figure 3 |
| 4 | §4 | Text only (D4) |
| 5 | §5 | Table 3, Figures 4–5, watermark |
| 6 | §6 | Tables 4–5, Figures 6–8 |
| 7 | §7 | Text only (D4) |
| 8 | §8 | Tables 6–8, Figure 9 |

- **Title page:** matches RULES.
- **Conclusions:** follow the guide's «Висновки» plan: Kafka, stream, aggregation, anomalies, plus one closing sentence of acquired skills.
- **Technical correctness:**
  - event-time windows, watermark 10 min;
  - OHLC open/close via `min/max(struct(event_time, x))`;
  - append semantics, with the unflushed last window explained;
  - update mode for memory and console sinks;
  - base window `shift(1)` with prewarm before `ts_first` (no look-ahead), confirmed by the perturbation test;
  - out-of-sample evaluation present;
  - all metrics from one run;
  - the daily query uses `window(…,"1 day")`;
  - idempotent producer;
  - `failOnDataLoss=true`.

  No errors found.

**Numbers spot-checked against results: 60+ values, all matching.**

| Group | Values checked | Source |
|---|---|---|
| Producer | 30 240, 322,093 s, 93,886/s | producer_stats |
| Broker | 10,1 s | timings_stages |
| Partitions | 10 080 / 20 160 / 0 | |
| Table 1 | all values | env_versions |
| Table 3 | all cells | window_verification |
| Window volume | 8 925 and 8 051 BTC | win1h_final |
| Watermark | disorder 0; dropped 0; gap 2,107 / 1,261 / 1,730·10⁸ s; 5,48 years; last mark 2022‑11‑12 23:49 | |
| Backlog test | 14 080 / 30 240; 939 windows; 1 083 dropped; 1 077 windows lost | backlog_test |
| Table 4 | all 9 rows | design_comparison |
| Thin market | 42,6 / 29,4 / 0,4 % | thin_market_stats |
| Volume | z_vol p99,99 3,067; 60,6 %; 5 437 vs 5 489; lift 23,2 | |
| Table 5 | all 7 rows | threshold_selection |
| Scored minutes | 6 453 867; 16,79 % | flat_minutes_handling |
| Microbatches | 8 (95), 7 (56), 27 (56), 18 (23) | batch_stats |
| Out-of-sample | LOO 30,7 (mean of 37,5 / 57,4 / 18,4 / 31,6 / 8,8); time split k 8,38, lift 42,5, 0,094 %, 118 days | oos_* |
| Table 6 | all cells; 30 016 minutes; Δ = 0 | periods_vs_baseline, lift, stream_vs_offline |
| Checks | 200 + 200 minutes; 40 of 40 | independent / lookahead |
| Table 7 | all cells | known_events_check, oos_loo |
| Patterns | 4 364; 0,113 / 0,052 / 2,17; 0,0358 / 0,1556; 30/20, 25/25; 71,1 % | patterns_summary, by_hour |
| Table 8 | all cells; loadavg 5,11–13,16, mean 8,31 | metrics_per_query, load_summary |
| Latency | 5,651 / 5,823 / 5,761 / 10,377 / 12,445 s; foreachBatch 0,716 s; offline 68,1 s | |
| Daily query | 21 days × 1 440 | daily_stats |

**Figures and tables:** 9 figures and 8 tables, each referenced before it appears and interpreted after it.

**Formatting:**

| Element | Measured | Status |
|---|---|---|
| Page | A4 | OK |
| Body text | TNR-metric 14 pt, 1,5 spacing, justified, indent 1,25 | OK |
| Page number | Arial 11, top right, none on title page | OK |
| Table captions | above, left, 14 pt | OK |
| Figure captions | below, centered, 13 pt | OK |
| Table cells | 11 pt; Table 7 is 9 pt | Table 7 below spec |
| Code in body | Courier 9 | OK |
| Appendix | starts on a new page; bold headings | OK |
| Body length | pp. 2–16 | OK |

**Style:**
- prose about 2 490 words, within budget;
- 0 em dashes;
- 0 banned phrases;
- no first person, no bold in paragraphs;
- decimal commas used;
- «lift» glossed on first use.

**Appendix А:** all 23 listings are identical to `code/` (D2 and D3 still apply).
