# Pre-delivery check: ПЗ5_АВД_Зелінський_ДМм-26-1.pdf (pages 1-17)

Renders: /home/user/AVD/work/pz5/build/pages_final/page_01.png ... page_17.png (110 dpi, pymupdf).

Verdict: NOT CLEAN. Two figure defects and one content inconsistency to fix before delivery.

## Defects

1. Page 9, Figure 4 (terminal screenshot): the line "LR: SQLTransformer -> ... -> Stan" and the RF/GBT lines run past the right edge of the image; "StandardScaler" is cut to "Stan" with the rest wrapped onto the next line mid-word. Fix: shorten the printed lines or widen the screenshot.
2. Page 10, Figure 5 (terminal screenshot): the header comment line "# фрагмент виводу: метрики на тесті: базові лінії та моделі (поріг 0,5 і поріг з валідації" runs to the right edge of the image and is truncated. Fix: shorten the comment in the source output.
3. Page 4, text below Table 1: "LR вгадала напрям у 7961 з 15 511 тестових годин". Every other place (pages 7, 8, Table 5, Table 6 and Figure 3) uses 15 534 test hours. Either explain why the direction task has 15 511 rows (for example, rows lacking a next-hour return) or correct the number.

## Checked and OK

- Page 1 title: ministry, ХНУРЕ, Кафедра штучного інтелекту, discipline, Практичне заняття №5, theme, author (ст. гр. ДМм-26-1, Зелінський В.С.), supervisor (Золотухін О.В.), Харків – 2026. All present.
- Page 2 starts with the work title.
- Tables 1 to 8 all fit within the text margins (x 108 to 805 px at 110 dpi). No columns are cut off and no numbers are split across lines.
- Table 2 (page 4) fits fully on one page. Its 9 pt font is the same as the other tables, but it is the smallest and most cramped table (first-column labels wrap to three lines). Readable, not a defect.
- Figures 1, 2, 3, 6, 7 and 8 are readable and match their captions. Figure 3 and Figure 5 are terminal captures; their numbers match the text.
- No placeholders (TODO, TBD, XXX, {{ }}, ???, etc.) found in the text layer of pages 1-17.
- No em dash (U+2014) found in pages 1-17. En dashes are used as the Ukrainian dash.
- No English prose sentences in the body. Only technical terms and code identifiers appear in Latin script.
- Appendix A (page 17) starts at the top of a new page; page 16 ends with the conclusions.

## Minor, cosmetic (not blocking)

- Captions break inside a number range or hyphenated word: Figure 1 caption "2018–/2026" (page 3), Figure 2 caption "30-/добових" (page 6), Table 3 caption "моделі)" orphan (page 6), Table 7 caption (page 12).
- Page 7 ends about 40% blank because Figure 3 is pushed to page 8.
- Page 10 justified lines have very wide word gaps ("Evaluation –", "write().save() у").
- Appendix A code (page 17): long comment lines wrap mid-line inside the listing.
