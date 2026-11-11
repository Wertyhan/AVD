# Formatting spec derived from the example lab report

Source: /home/user/AVD/work/format_example.pdf (12 pages, Ukrainian lab report, Харківський національний університет радіоелектроніки).
Evidence: pymupdf span geometry (/home/user/AVD/work/format/geom.txt), pdftotext (/home/user/AVD/work/format/all.txt), renders (/home/user/AVD/work/format/page-01.png … page-12.png), raw PDF text-matrix scale check.

Note on sizes: the PDF was generated with a 0.24 text scale, so pymupdf reports sizes like 13.92 and 10.56. These are 14 and 10.5 pt (the nominal values). Use the nominal values below.

---

## 1. Page setup (all pages)

| Item | Value | Evidence |
|---|---|---|
| Page size | A4, 21.0 × 29.7 cm | 595.44 × 841.68 pt |
| Margin left | 2.5 cm | body text x0 = 2.50 cm on every text page |
| Margin right | 2.4 cm | text right edge 18.59 cm, page 21.01 cm |
| Margin top | 2.0 cm | first body baseline 74.2 pt, consistent with 2.0 cm top (inferred, ±0.1) |
| Margin bottom | 2.0 cm (2.0–2.5 allowed) | table on p.9 reaches 27.19 cm, so bottom margin ≤ 2.5 cm |
| Header distance | 1.25 cm | page number glyph top at 1.28 cm |
| Footer | none | no footer text found |
| Page number | Arial 11 pt, top right, in header, right-aligned to right margin | "2" at x1 = 18.57 cm, y top 1.28 cm |
| Title page | no page number (different first page) | page 1 has no number |
| Numbering | title page = 1 (unnumbered), first printed number is "2" | page 2 shows "2" |

python-docx: `section.page_width = Cm(21.0)`, `section.page_height = Cm(29.7)`, `left_margin = Cm(2.5)`, `right_margin = Cm(2.4)`, `top_margin = Cm(2.0)`, `bottom_margin = Cm(2.0)`, `header_distance = Cm(1.25)`, `different_first_page_header_footer = True`. Page number field in the default header only, right-aligned, Arial 11.

---

## 2. Base text

- Font: Times New Roman 14 pt (all body text, captions to be specified below, headings).
- Line spacing: 1.5 lines (measured pitch 24.2 pt = 1.5 × 16.1 pt single for TNR 14).
- Alignment: justified (lines end flush at 18.59 cm).
- First-line indent: 1.25 cm for body paragraphs (first-line x = 3.75 cm, continuation x = 2.50 cm).
- Space before/after body paragraphs: 0 pt (baseline pitch between paragraph end and next heading equals 24.2 pt).
- Color: black.

python-docx: `style.font.name = "Times New Roman"`, set `rFonts` eastAsia too; `size = Pt(14)`; `paragraph_format.line_spacing = 1.5`; `first_line_indent = Cm(1.25)`; `alignment = JUSTIFY`; `space_before = Pt(0)`, `space_after = Pt(0)`.

---

## 3. Title page (page 1)

All lines: TNR 14 pt, centered, 1.5 spacing, no indent. Measured baselines (cm from top edge):

| # | Text (exact) | Baseline cm | Notes |
|---|---|---|---|
| 1 | Міністерство освіти і науки України | 2.21 | |
| 2 | Харківський національний університет радіоелектроніки | 3.06 | line pitch 24 pt (no extra space) |
| – | 4 empty 1.5-spaced lines | | gap 96.8 pt |
| 3 | Кафедра штучного інтелекту | 6.47 | |
| – | 3 empty lines | | gap 72.4 pt |
| 4 | Дисципліна «Обчислювальний інтелект» | 9.02 | |
| 5 | Лабораторна робота №1 | 9.87 | |
| 6 | «Згорткові нейронні мережі для обробки нестандартних зображень» | 10.73 | guillemets bold in PDF, text regular; use regular for all |
| – | 5 empty lines | | gap 144.2 pt |
| 7 | two-column block (see below) | 15.82 | |
| – | ~9 empty lines | | |
| 8 | Харків – 2026 | 27.07 | centered, en dash with spaces, bottom of title page |

Two-column block "Виконали / Прийняв" (borderless 2-column table, 0.19 cm cell padding):
- Left cell, x = 2.5 cm (text at 2.68): lines, 1.5 spacing:
  `Виконали:` / `ст. гр. ДМм-26-1` / `Зелінський В.С.` / `Лавриненко С.Р.` / `Спащенко Р.Т.`
- Right cell, column starts at ~10.8 cm (text at 10.98 cm):
  `Прийняв:` (same baseline as "Виконали:") / `Малєєв Д.В` (baseline of 2nd line, "ст. гр.")
- Left-aligned text in both cells. No borders.
- Note: the example writes "Малєєв Д.В" with no final period; keep as written.

---

## 4. Body structure and headings

Document order (exact names as in the example):

1. Title page (p.1), see section 3.
2. p.2: Work title (centered): `Реалізація згорткової нейронної мережі (CNN) для класифікації зображень з нестандартними розмірами`
3. p.2: `Мета роботи: Необхідно розробити згорткову нейронну мережу для класифікації зображень з нестандартними розмірами (наприклад, супутникові знімки або зображення природи). Для цього використайте датасет Kaggle Natural Scenes Dataset. Це завдання допоможе вам на практиці зрозуміти роботу CNN з обробкою зображень з різною роздільною здатністю.` (body paragraph, "Мета роботи:" NOT bold)
4. p.2: `Хід виконання роботи` (unnumbered, centered)
5. `1 Датасет та програмне середовище` (p.2)
6. `2 Розвідувальний аналіз даних (EDA)` (p.2)
7. `3 Якість даних та підготовка вибірок` (p.5). Subsections 3.1–3.3 do not appear; numbering jumps to 3.4.
8. `3.4 Приведення зображень до розміру 196×196` (p.5)
9. `3.5 Аугментація зображень` (p.6)
10. `3.6 Архітектура CNN` (p.7)
11. `3.7 Умови навчання` (p.7)
12. `3.8 Порівняння Stretch та Pad (базова модель: Adam + Dropout)` (p.7)
13. `3.9 Регуляризація та оптимізатор: L2 замість Dropout, SGD замість Adam` (p.9)
14. `3.10 Повтори з різними seed` (p.11)
15. `Висновки:` (lead-in in bold? NO, plain body paragraph, flush at 2.5 cm, p.11)
16. `Додаток А. Фрагменти програмного коду` (new page p.12, appendix title)
17. `А.1 Приведення зображень до розміру 196×196`, `А.2 Аугментація`, `А.3 Модель CNN`, `А.4 Навчання` (code listings, see section 7).

Heading rules:

| Element | Font | Size | Bold | Caps | Alignment | Left indent | Numbering text |
|---|---|---|---|---|---|---|---|
| Work title (p.2) | TNR | 14 | NO | no | centered | 1.25 cm (center measured 11.17 cm) | none |
| "Хід виконання роботи" | TNR | 14 | NO | no | centered | 1.25 cm (center 11.17) | none |
| Level 1 "1 Датасет…" | TNR | 14 | NO | no | left | 1.25 cm (x0 3.75), no first-line indent | "1" + space, no dot |
| Level 2 "3.4 …" | TNR | 14 | NO | no | left | INCONSISTENT: 0 cm in 3.4, 3.7, 3.8, 3.9, 3.10; 1.25 cm in 3.5, 3.6 | "3.4" + space |
| Appendix title "Додаток А. …" | TNR | 14 | YES | no | centered (full text width, center 10.48) | 0 | "Додаток А." |
| Appendix sub-heading "А.1 …" | TNR | 14 | YES | no | left | 0 (x0 2.50) | "А.1" + space |

Recommended settings: level-1 headings with `left_indent = Cm(1.25)` and `first_line_indent = 0`, non-bold, keep_with_next = True. Use the same indent for level 2 (1.25 cm) to match the majority of the example (5 of 7 subsections measured at 2.5 cm, 2 at 3.75 cm; treat 1.25 cm as the intent of level-1 style and report the inconsistency to the user if needed). Heading spacing: no extra space before or after (line pitch 24.2 pt).

Caption-to-heading: no extra space around headings.

---

## 5. Table captions and tables

Caption (above table):
- Format: `Таблиця N – Назва` (en dash U+2013 with one space on each side, N = arabic numeral).
- Font: TNR **14 pt** (NOT 11 as expected), regular, not bold.
- Alignment: LEFT, left indent 0 (x0 = 2.50 cm). Not centered.
- Line spacing: single (wrapped caption line pitch 16.1 pt, e.g. Table 8 caption wraps to 2 lines).
- Space before: ~6 pt extra (caption baseline is 30 pt below preceding body last line, vs 24.2 pt normal pitch).
- Keep with next: YES (caption stays with table).

Table body:
- Table: full grid, single black thin borders on all cells (tables 1–10 all gridded).
- Table width: Tables 2, 3, 4, 5, 6, 7, 8, 9, 10 span about 2.5 to 18.4 cm (full text width, ≈16 cm, centered). Table 1 is narrower: 4.47 to 16.48 cm (≈12 cm), centered.
- Cell text: TNR **11 pt**, single line spacing, space 0 before/after. Exception: Tables 7, 8, 9 use **10.5 pt** (both body and header).
- Header row: centered. Bold in Tables 2 and 8 only; regular in Tables 1, 3, 4, 5, 6, 7, 9, 10 (majority: regular). Recommended: regular header.
- Cell alignment: first column left-aligned; numeric columns centered (measured centers in cells).
- Cell padding: about 0.19 cm left/right (text starts 0.2 cm inside the grid line).
- Row pitch ≈ 15.1 pt (11 pt text).
- Table caption → table: no extra space between caption and table.

Table list in the example (number, page, caption):
- Таблиця 1 – Розподіл зображень за класами (p.2, above, 12 cm wide)
- Таблиця 2 – Описова статистика метаданих зображень (p.3)
- Таблиця 3 – Середні піксельні характеристики за класами (шкала 0–1) (p.4)
- Таблиця 4 – Розбиття очищеного датасету (p.5)
- Таблиця 5 – Архітектура моделі (базова конфігурація) (p.7)
- Таблиця 6 – Результати базової моделі (Adam + Dropout) (p.8)
- Таблиця 7 – Метрики за класами на тесті (Adam + Dropout) (p.8)
- Таблиця 8 – Порівняння конфігурацій та способів ресайзу на тесті (seed = 42) (p.9, wraps to 2 lines, second line flush left at 2.5 cm)
- Таблиця 9 – Парне порівняння Pad − Stretch усередині конфігурацій (p.10; minus sign U+2212)
- Таблиця 10 – Test accuracy базової моделі для різних seed (p.11)

---

## 6. Figures and figure captions

Figure caption (BELOW figure):
- Format: `Рисунок N – Назва` (en dash U+2013 with spaces).
- Font: TNR **13 pt** (NOT 11 as expected), regular.
- Alignment: CENTERED on full text width (center 10.48–10.5 cm).
- Line spacing: single (wrapped line pitch 15.4 pt, e.g. Рисунок 5 wraps to 2 lines).
- Space before: ~5 pt gap from image bottom to caption top.
- Space after: ~0–6 pt before the next body paragraph (baseline distance 25.7 pt to next paragraph line).

Images:
- Centered, inline (no floating). Widths: full text width (16 cm) or narrower (for example Figure 9 is 11 cm wide, Figure 10 is 12 cm). Image is centered (Figure 9 image center 10.48 cm).
- Caption number follows the image directly; caption is in the same page as the image.

Figure list:
- Рисунок 1 – Кількість зображень та розподіл за класами (p.3)
- Рисунок 2 – Розподіли ширини, висоти, співвідношення сторін та розміру файлу (p.3)
- Рисунок 3 – Випадкові приклади зображень кожного класу (p.4)
- Рисунок 4 – Середня інтенсивність каналів, яскравість та насиченість за класами (p.4)
- Рисунок 5 – Оригінал, Stretch та Pad 196×196 для зображень із різним співвідношенням сторін (p.6, 2 lines)
- Рисунок 6 – Приклади аугментації одного зображення (Stretch та Pad) (p.6)
- Рисунок 7 – Криві навчання базової моделі: accuracy та loss (пунктир – train, суцільна – val) (p.8)
- Рисунок 8 – Нормовані матриці помилок на тестовій вибірці (Stretch та Pad) (p.8)
- Рисунок 9 – Тест діапазону LR для SGD (momentum 0,9) (p.9)
- Рисунок 10 – Test accuracy конфігурацій із 95 % довірчими інтервалами Вілсона (p.10)
- Рисунок 11 – Криві навчання всіх конфігурацій (accuracy та крос-ентропія) (p.10)
- Рисунок 12 – Матриці помилок найкращої за середньою точністю конфігурації (Adam + L2) (p.11, 2 lines)

Figure numbering is continuous across the document. Table numbering is continuous.

---

## 7. Code listings (Appendix А)

- Font: Courier New **9.6 pt** (nominal, measured 9.6 exactly), regular.
- Line spacing: single (line pitch 10.8 pt).
- Alignment: left (not justified). No border, no shading, no background, no line numbers.
- Left indent: 0.5 cm (text x0 = 3.00 cm). Wrapped continuation lines use the same 0.5 cm indent (no hanging indent).
- Space before/after each block: about 1 line (the blank line between functions is kept as an empty line).
- Code is plain text as written (Python, 4-space indentation, no syntax colors).

Appendix headings: see section 4 (bold, 14 pt, left at 2.5 cm). "Додаток А. …" begins on a new page (p.12).

---

## 8. Punctuation and symbols

- En dash U+2013 "–" for captions (`Таблиця N – …`, `Рисунок N – …`), for "Харків – 2026", and for list bullets.
- List bullets: en dash "–" at x = 3.15 cm, text after the dash starts at 3.75 cm, wrap at 3.75 cm (left indent 1.25 cm, hanging ~0.6 cm). Example p.5 and p.9, items separated by ";" at end.
- Minus sign U+2212 "−" in Table 9 headers and in text formulas, e.g. "Pad − Stretch", "−0,0355".
- Decimal comma (Ukrainian style): "0,8099", "1,44". Thousands separator is a space (e.g. "1 426 213").
- Quotation marks: guillemets «…» (e.g. «Обчислювальний інтелект», «Згорткові …»).
- Multiplication sign "×" in sizes ("196×196", "320×240").
- Percent: "%" with a space before in body text ("13,99 %", "95 %"). Inside tables the value is written without %.

---

## 9. Page layout details

- Page breaks: title page is followed by a page break; the appendix starts on a new page (p.12). Other sections flow continuously (no forced breaks); tables and figures do not split across pages in the example.
- Keep-with-next: captions (table above, figure below) stay with their object.
- Widow/orphan control assumed ON (default).

---

## 10. Deviations from the expected spec

These differ from the expectations listed in the task:

1. Table caption size: 14 pt, not 11 pt. Caption is TNR 14 regular, left-aligned, above the table. Position (above, left) matches.
2. Figure caption size: 13 pt, not 11 pt. Caption is centered, below the figure. Position matches.
3. Table cell text: 11 pt in most tables, 10.5 pt in Tables 7–9.
4. Section headings are NOT bold, not uppercase, not centered; level 1 is indented 1.25 cm, level 2 is inconsistent (0 or 1.25 cm). Expected headings were not specified in the request beyond numbering. Subsections 3.1–3.3 are absent.
5. Appendix headings are bold 14 pt (appendix title centered, subheads left at 2.5 cm).
6. Right margin measured 2.42 cm (≈2.4). Top and bottom margins are estimates (top 2.0, bottom 2.0–2.5 cm).
7. Page number: Arial 11 pt, top right, present from page 2; title page has none. Confirmed.
8. Body: TNR 14, 1.5 spacing, justified, first-line indent 1.25 cm. Confirmed.
9. Table headers are bold in Tables 2 and 8 only (regular elsewhere).
10. Code listings use Courier New 9.6 pt with 0.5 cm left indent (not in the request).
11. Work title on p.2 is centered with left indent 1.25 cm (center offset to 11.17 cm).
12. Title page lines are separated by blank 1.5-spaced lines (see section 3 for counts). Work title is regular weight (guillemets bold, minor).
13. Dash is en dash U+2013 in captions and lists; minus U+2212 in Table 9.
14. Table 1 is about 12 cm wide and centered, all other tables are full text width.

---

## 11. python-docx checklist (summary)

1. A4 21.0 × 29.7; margins T 2.0, B 2.0, L 2.5, R 2.4; header distance 1.25 cm; different first page (title page has empty header).
2. Header (default): right-aligned page number field, Arial 11 pt.
3. Normal style: TNR 14 pt, 1.5 spacing, space before/after 0, justified, first-line indent 1.25 cm.
4. Title page: centered 14 pt lines, 1.5 spacing, blank-line gaps as in section 3; Харків – 2026 at bottom; borderless 2-column table for Виконали/Прийняв.
5. Work title (p.2): centered, 14 pt regular, left indent 1.25 cm. "Хід виконання роботи": centered, 14 pt regular, left indent 1.25 cm.
6. Level-1 headings: TNR 14 regular, left indent 1.25 cm, first-line 0, keep_with_next; level-2 headings: same settings (indent 1.25 cm recommended).
7. Table caption: TNR 14 regular, left, single spacing, space before 6 pt, keep_with_next. Table: full grid, TNR 11 (10.5 for Tables 7–9), single spacing, centered numeric cells, header regular, first column left.
8. Figure caption: TNR 13 regular, centered, single spacing, space before 5 pt; figure centered inline above it.
9. Code: Courier New 9.6 pt, single, left indent 0.5 cm, not justified, no border/shading.
10. Appendix: new page; title centered bold 14 pt; subheads bold 14 pt, left.
11. Dash: "–" (U+2013) in captions and bullets; minus "−" (U+2212) where it is a minus sign; decimal comma.
