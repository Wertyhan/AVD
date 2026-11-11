# FORMATCHECK PZ2 (PDF: build/ПЗ2_АВД_Зелінський_ДМм-26-1.pdf, 44 pages; main body = 29 pages (1-29), appendix А = pp.30-44)

Fixed: first build used theme with «» -> title page showed ««...»»; rebuilt with bare theme (builder adds guillemets). No builder/markdown changes needed.

Page -> issues
- 1: OK (text fits, «Харків – 2026» at bottom, no number). Block is "Виконав/Прийняв" (single author).
- 2-29: page numbers top right OK; body TNR 14 / 1.5 / justified / indent OK; headings, figure captions (below, centered), table captions (above, left) OK; tables within margins; no figure split from caption.
- 3, 7, 8, 16, 20, 21: ~20-25 % blank at page bottom (keep-with-next of heading/figure/table); acceptable.
- 12, 25, 26: figure/table pushed to next page leaves large gap on 25 (~35 %) and 21 (~30 %); not blank pages.
- 12: Spark UI screenshot has much internal whitespace (image content, not builder).
- 24, 26: placeholders [ЗАПОВНИТИ ПІСЛЯ ЗАПУСКУ] inside table cells and inline text (pp.24, 26, 28, 29) render as plain text; builder boxes only standalone-paragraph placeholders (none present).
- 8, 14, 15: table 5 headers wrap mid-word ("Асиметрі/я", "Мініму/м", "Максим/ум") on p.14 (narrow columns).
- 30-44: appendix starts on new page, А.1 heading present; code Courier readable, long lines wrap.
- Main body 29 pages vs target 10-15: exceeds target (text length, not a builder defect).
