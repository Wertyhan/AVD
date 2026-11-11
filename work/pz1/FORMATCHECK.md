# FORMATCHECK: PZ1 PDF build/ПЗ1_АВД_Зелінський_ДМм-26-1.pdf

Method: 43 pages rendered with pymupdf at 70 dpi to build/pages/page-01.png … page-43.png. Pages 1–17 and the layout of 1–16 inspected visually. Fonts, sizes and line breaks from pymupdf span data. Screenshot legibility from pixel analysis of the source PNGs and the placed image widths in the PDF.

## Layout summary

- Page count 43: title p.1 (no number), body p.2–16 (15 pages, at the 10–15 limit), appendix p.17–43 (27 pages of listings).
- Page numbers: present from p.2, top right, none on p.1. PASS on position. FAIL on font, see F1.
- Appendix starts on a new page (p.17), bold centered title, bold left subheadings. PASS.
- Title page: ministry, university, department, discipline «Аналітика великих даних», Практичне заняття №1, topic in «», Виконав/Прийняв columns, Харків – 2026. PASS against FORMAT §3.
- Captions: table captions TNR 14 above, left (p.4, 5, 11, 13, 15); figure captions TNR 13 below, centered. PASS against FORMAT §5–6.
- Body text: TNR-equivalent 14 pt, justified, 1,5 spacing, 1,25 cm first-line indent. PASS visually.

## Findings

- F1. Page numbers are LiberationSerif 14 pt on every page (pymupdf span check, e.g. p.2, p.5, p.12). FORMAT §1 requires Arial 11 pt. Fix the default header: right-aligned page field in Arial 11.
- F2. Numbers break across lines in running prose, because thousands separators are ordinary spaces. Most table cells (Tables 2, 4, 5, 6) use NBSP and do not break; the Table 1 cell does not. Found:
  - p.3: «270 553 | 174 016 байт»
  - p.4, Table 1, row mapreduce cell: «2 487 | 697 408 байт»
  - p.8: «127 655 | 772 байти»
  - p.12: «5 024 | 184,243438596», «525 | 600», «(887 | 623,85 BTC)»
  - p.13: «Low 30 | 066,00»
  Fix: use U+00A0 for thousands separators in prose, as the table cells already do. Likely more instances than the scan caught; a regex on all «\d \d{3}» in prose would cover them.
- F3. Table 4 (Таблиця 4, Річні агрегати Hive) breaks across pages 11–12: rows 2012–2020 on p.11, 2021–2026 on p.12 (header repeated). FORMAT §9 states tables do not split. Fix by moving the caption and table to p.12 or by keep-together on table rows.
- F4. Large gaps at page bottoms, caused by figures moved to the next page:
  - p.6: about a quarter of the page empty; Figure 3 (screenshot) moved to p.7.
  - p.8: about a quarter empty; Figure 4 moved to p.9.
  - p.13: about a quarter empty; Figure 9 moved to p.14.
  - p.5: about a seventh empty, minor.
- F5. Screenshot legibility at print size (threshold 8 pt, RULES 2a). Source PNGs are 1600 px wide with a 37 px line pitch.
  - Рисунок 2 (fsck, p.5) is placed 10,05 cm wide. Scale 0,178 pt/px gives a line pitch of 6,6 pt, so the font is about 5,5 pt. FAIL.
  - Рисунок 6 (DESCRIBE FORMATTED, p.10) is placed 10,33 cm wide, about 5,7 pt. FAIL.
  - Рисунок 1 (jps), 3 (mr_yearly result) and 7 (hive select) are placed 16,1 cm wide, about 8,8 pt. PASS.
  Fix: place Рисунок 2 and 6 at 16 cm, or crop the PNG to the relevant lines and keep the 90-column rule.
- F6. Widow words in captions: «обсяг» alone on the second line of Рисунок 3 (p.7), «місяць)» alone on the second line of Рисунок 8 (p.12). Shorten the caption or allow the wrap elsewhere. Minor.
- F7. Font in the PDF is LiberationSerif (metric substitute), not Times New Roman. Code is LiberationMono 9,5 pt, FORMAT specifies Courier New 9,6 pt. The PDF was built without TNR installed. Note in the build or install TNR. Minor for the look, but FORMAT names TNR.
- F8. Appendix listings: long lines wrap, and the continuation lines are indented as a hanging indent (e.g. p.17, mapper_year.py `year_by_day` comment, and p.17–18 lines). FORMAT §7 says wrapped lines use the same 0,5 cm indent, no hanging indent. Shorten the lines to about 85 characters, or accept the wrap and set the continuation indent to the same value.
- F9. Body code fragments split across pages: the Python `reducer_ohlc` snippet (p.7 to p.8, `lo = min(...)` alone on p.8). Keep the block together. Minor.
- F10. Table 1 (p.4): the description column «Обґрунтування» is centered. FORMAT §5: first column left, numeric columns centered, text columns not specified. Left-align the text column for readability. Minor.
- F11. Justification gaps: p.10, «Тип EXTERNAL_TABLE і розташування…» stretches words across the line because of the long token. Rephrase or move the token to a new line. Minor.
- F12. Table 7 placeholders are yellow-highlighted (p.15). Intentional placeholders; the highlight is fine for a draft.

## Checked and passed

- Title page content and layout (FORMAT §3).
- Header page numbers from p.2, none on p.1.
- Caption sizes: table 14, figure 13 (FORMAT §5–6).
- Level-1 headings at 1,25 cm, level-2 at 2,5 cm (FORMAT §4 allows the inconsistency).
- Appendix on new page, bold title and subheadings (FORMAT §4).
- Figure images present and placed inline, centered.
- Screenshots at 16,1 cm, legible at print size (F5 excludes these).
