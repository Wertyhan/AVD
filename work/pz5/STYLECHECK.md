# STYLECHECK: PZ5 report.md

Source: /home/user/AVD/work/pz5/report.md (871 lines; line numbers below refer to this file). Rules: /home/user/AVD/work/RULES.md, section «Writing style». Checked by grep and Python scans; code blocks excluded from prose checks.

## Result

No violations of the banned-phrase list, em dash, bold, first person, straight quotes, or figure/table numbering. One optional change (conclusions list). Length and structure budgets are met.

## Checks

| Rule | Result | Evidence |
|---|---|---|
| Banned phrases («варто зазначити», «важливо підкреслити», «у сучасному світі», «відіграє ключову роль», «таким чином, можна зробити висновок», «комплексний аналіз», «глибоке занурення», «не просто … а …») | PASS | No matches in any inflection. The stem «важлив-» appears only in the neutral phrase «важливості» (line 222). |
| Em dash «—» | PASS | 0 occurrences in the whole file, including code comments. |
| Dash as separator | PASS | En dash «–» used in prose, captions and lists. No « - » separators in prose. |
| Hyphen inside words only | PASS | ISO dates in Table 4 (lines 115–118, «2018-01-01») are the only other hyphenated tokens. Acceptable. |
| Decimal comma in prose and tables | PASS | Only two digit.digit matches outside code, both dates («01.01.2018» line 64, «27.01.2012» line 47). Values use «,» (e.g. 0,7115). Code and versions keep their syntax. |
| Percent format | PASS | «NN %» with a space in prose (e.g. line 101 «перші 80 %»). Tables use bare numbers. |
| Bold inside paragraphs | PASS | No `**` or `__` outside code. |
| Bullet lists | NOTE | Conclusions (lines 256–270) are an 8-item numbered list rendered as dash bullets. The guide card asks for conclusions «за запропонованим планом», so the list follows the plan. Keep, or rewrite as prose if a stricter reading is wanted. |
| First person (я, ми, нам, наш, маємо, бачимо, …) | PASS | Token-level scan of prose: 0 hits. Impersonal forms used («обрано», «побудовано», «навчено», «зведено»). |
| Quotes «ялинки» | PASS | No straight or curly double quotes in prose. Guillemets used. |
| Figure numbering continuous, Рисунок 1–9 | PASS | Figures at lines 23, 78, 157, 161, 169, 210, 220, 228, 246. |
| Table numbering continuous, Таблиця 1–8 | PASS | Captions at lines 27, 49, 86, 111, 128, 179, 199, 232. |
| Each figure and table referenced before it appears | PASS | Рисунок 1 ref l.21; 2 l.76; 3 l.155; 4 l.159; 5 l.167; 6 l.197; 7 l.218; 8 l.226; 9 l.244. Таблиця 1 ref l.25; 2 l.47; 3 l.84; 4 l.109; 5 l.126; 6 l.177; 7 l.197; 8 l.230. |
| Interpretation after each figure and table | PASS | A paragraph with conclusions follows every object. |
| Image paths exist | PASS | 9 image links, all resolve: figures/fig01 to fig07 (7 files) and screenshots/term_06_pipeline.png, screenshots/spark_ui_jobs_top.png. |
| No placeholders | PASS | No `[СКРІНШОТ` or `[ЗАПОВНИТИ` left. |
| Theory section ≤ 350 words | PASS | 209 words (lines 5–15). |
| Main body prose 2200–2700 words | PASS | About 2264 words (excluding tables, captions, code, headings). |
| Figures 8–14 | PASS | 9 figures (2 are screenshots). |
| Tables ≤ 8 | PASS | 8 tables. |
| Short code in body ≤ 8 lines each | PASS | Body blocks: lines 70–74 (4), 103–107 (4), 146–153 (8 incl. comment line). Appendix blocks exempt. |
| Appendix code comments in Ukrainian | PASS | Sampled; comments are Ukrainian. |

## Notes (not violations)

1. Conclusions list (lines 256–270): optional rewrite to prose.
2. The heading «Мета роботи» (line 1) is followed by a paragraph starting «Мета роботи:» (line 3). Same pattern as the FORMAT example; intentional.
3. Negative values in Table 1 (line 31) use «−» (U+2212). Correct per FORMAT.md section 8.
4. Table 4 uses ISO dates with hyphens. Could be written as «01.01.2018» to match the rest of the report.
5. Numeric values were not checked against results CSVs here; that is the integrity check, a separate task.

## Verdict

PASS on style. Optional: convert the conclusions list (lines 256–270) to prose.
