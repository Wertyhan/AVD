# RULES — shared rules for all subagents (PZ1–PZ5, «Аналітика великих даних»)

## Context
- Course «Аналітика великих даних», ХНУРЕ, кафедра штучного інтелекту, магістратура, група ДМм-26-1.
- Student (sole author): Зелінський В.С. Instructor: Золотухін О.В. Year: 2026.
- Variant: Bitcoin Historical Data (kaggle.com/datasets/mczielinski/bitcoin-historical-data): 1-min BTC/USD candles since 2012 (Timestamp, Open, High, Low, Close, Volume).
- Requirements come ONLY from the guide card `work/cards/pzN.md` (raw text: `work/cards/raw/pzN.txt`). Invent no requirements. Do every task in the guide, in the guide's order and numbering.
- Dataset facts: ONLY from `work/data/README.md` / `work/data/facts.csv`. Environment facts: `work/ENV.md`. Formatting: `work/FORMAT.md`.

## Working conventions
- Each PZ lives in `work/pzN/`: `code/`, `results/` (csv with EVERY number used in the report), `figures/`, `screenshots/`, `report.md`, `defense.md`, `build/` (docx/pdf).
- Subagents exchange via files. Reply to the orchestrator in at most 10 lines: what was done, file paths, problems.
- Seed 42 everywhere. Record software versions, machine config (CPU/RAM) and runtimes in `results/env_versions.csv` and `results/timings.csv`.

## Integrity (non-negotiable)
1. Every number in a report comes from real program output and exists in `work/pzN/results/`. Nothing invented.
2. Screenshots are REAL: actual command output rendered to PNG as a terminal image (dark background, monospace, the command line shown with a `$` prompt followed by its actual output), or real web UIs (Spark UI :4040, NameNode :9870, YARN :8088) captured via headless Chromium/playwright. No imitations, no edited output.
2a. Screenshot legibility (printed at 16 cm width, text must be ≥ 8 pt): terminal PNGs wrap/trim lines to ≤ 90 characters and ≤ 35 lines, monospace font sized so that 90 columns span the full image width (e.g. 1600 px wide with ~30 px font, i.e. image width ≈ 90 × char width + padding); no empty right area. Web UI screenshots: viewport 1280×800, device_scale_factor 2, and capture only the relevant region (element/clip screenshot of the table/chart, not the whole page with empty space).
2b. Chart legibility: matplotlib figures are made at their print size — figsize=(6.3, 3.4) inches (16 × 8,5 cm) or (6.3, 4.7) for two-panel — with font sizes ≥ 9 pt (ticks 9, labels/legend 10, title 11), dpi 200; Ukrainian labels; decimal comma in tick labels and annotations (use a FuncFormatter).
3. Anything that could not be run here = placeholder `[СКРІНШОТ: опис]` or `[ЗАПОВНИТИ ПІСЛЯ ЗАПУСКУ]`. Never fill placeholders with guesses.
4. If something failed or did not beat a baseline, say so and explain why.

## Content quality (strong MIT grad-student level)
- Every decision (parameter, window, threshold, model, metric) is justified by the data in 1–2 sentences.
- Fair comparisons: baselines, identical data and conditions, measured wall-clock time (report the machine).
- 8–14 meaningful figures (incl. screenshots) and 5–10 tables per report; each referenced in the text BEFORE it appears («рисунок 3», «таблиця 2») with a conclusion drawn after it.
- Technical correctness: OHLC resampling = first/max/min/last/sum; timestamps are UTC; event-time windows with watermark in streaming; chronological split (no leakage) in ML; equal conditions in index comparisons.
- Mention data-specific surprises (gaps, zero-volume minutes, extreme events: 12–13.03.2020 COVID crash, 19.05.2021 crash, 08–09.11.2022 FTX collapse, etc. — only if confirmed by our results) and what was done about them.

## Writing style (reports in Ukrainian)
- Ukrainian academic register, impersonal forms («реалізовано», «отримано», «встановлено»). No first person.
- Specifics: numbers, dates, field names. Short interpretation after every table/figure.
- BANNED phrases (never use): «варто зазначити», «важливо підкреслити», «у сучасному світі», «відіграє ключову роль», «таким чином, можна зробити висновок», «комплексний аналіз», «глибоке занурення», construction «не просто …, а …». No introductions about the importance of big data.
- Dash: only en dash «–». Never em dash «—». Hyphen only inside words.
- Decimal comma in text and tables (3,14 not 3.14). Code/commands keep their own syntax.
- No bullet lists where a sentence suffices; no bold inside paragraphs.
- Theory section short (1–1,5 pages), own words, tied to this dataset, not a retelling of the guide.
- Length: 10–15 pages main body + appendix. At TNR 14 / 1,5 spacing one page holds ≈ 250 words, and figures/tables take ≈ 5–6 pages, so the main-body PROSE budget is 2200–2700 words (excluding tables, captions, code, appendix). Theory ≤ 350 words. Max 12 figures, max 8 tables; prefer combining related small charts into one multi-panel figure. Short code fragments in the body ≤ 8 lines each.
- Quotes: «ялинки».

## Report structure (report.md)
1. Title page (generated at assembly, see FORMAT.md): theme «…» verbatim from the guide.
2. «Мета роботи» — the guide's goal, adapted to the variant.
3. «Короткі теоретичні відомості».
4. «Результати виконання практичного заняття» — sections «1 …», «2 …» per guide task; subsections «2.1 …». Short command/code fragments next to screenshots.
5. «Висновки» — following the guide's plan/questions where it specifies one.
6. «Додаток А. Лістинг програми» — full commented code (comments in Ukrainian), subsections А.1, А.2 …

Markdown conventions in report.md (for the docx builder):
- Headings: `#` for top-level unnumbered sections (Мета роботи, Короткі теоретичні відомості, Результати…, Висновки, Додаток А…), `##` for «1 Назва», `###` for «1.1 Назва».
- Figure: on its own line `![Рисунок N – Назва](relative/path.png)`; placeholder figure: `[СКРІНШОТ: опис]` on its own line followed by the caption line `Рисунок N – Назва`.
- Table: caption line `Таблиця N – Назва` immediately before a pipe table.
- Code fragment: fenced ``` block.

## Formatting (details in work/FORMAT.md, which wins if they differ)
- A4; margins L 2,5 cm, R ~2,4 cm, T/B ~2 cm. Body Times New Roman 14, line spacing 1,5, justified, first-line indent 1,25 cm.
- Page number top right, Arial 11, none on the title page.
- Title page (centered): Міністерство освіти і науки України / Харківський національний університет радіоелектроніки / (gap) Кафедра штучного інтелекту / (gap) Дисципліна «Аналітика великих даних» / Практичне заняття №N / «Тема з методички». Then two columns: left «Виконав: / ст. гр. ДМм-26-1 / Зелінський В.С.», right «Прийняв: / Золотухін О.В.». Bottom: «Харків – 2026».
- Tables: caption above, left-aligned «Таблиця N – Назва»; table text Times New Roman 11; thin borders.
- Figures: centered; caption below, centered «Рисунок N – Назва»; continuous numbering.
- Listings in Appendix A: Courier New ~10 pt.
- Output file names: `ПЗN_АВД_Зелінський_ДМм-26-1.pdf` (and .docx).

## Defense notes (defense.md, Ukrainian, not part of the PDF)
- The work explained in very small steps, one at a time, plain language.
- 10–15 likely instructor questions with short answers.
- 5 questions testing understanding of the ready code, with file:line references.
- What to re-run before the defense.
