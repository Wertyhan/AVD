# FORMATCHECK: PZ5 PDF

PDF: /home/user/AVD/work/pz5/build/ПЗ5_АВД_Зелінський_ДМм-26-1.pdf (29 pages, A4).
Renders (pymupdf, 70 dpi): /home/user/AVD/work/pz5/build/pages/page-01.png … page-29.png.
Spec: /home/user/AVD/work/FORMAT.md.
Method: visual review of every page; pymupdf geometry (text extents, table bounding boxes, image placement, font sizes); estimate of print legibility from raster pixel size (effective dpi) and text height inside each image.

## Summary

Structure and layout pass: title page, page numbers, caption positions, margins, appendix start, no blank pages, figure-caption grouping.

Fails: Table 6 (numbers and headers broken), Table 3 (split across pages), Table 5 (header breaks inside names), a large gap on p5, a broken code fragment on p9, and four screenshots and figures whose text is too small to read at print size.

## Per-item results

| Requirement | Result | Where / evidence |
|---|---|---|
| Title page on one page, «Харків – 2026» at bottom | PASS | p1. Single page, «Харків – 2026» near bottom (y ≈ 720 of 842 px at 70 dpi). |
| Title page without page number | PASS | p1 has no number. |
| Page numbers top right from p.2 | PASS | p2–p29, right-aligned at top, starting at «2». |
| Captions: figure below, centered | PASS | Рисунок 1–9 are below their images, centered (13 pt). |
| Captions: table above, left | PASS | Таблиця 1–8 above their tables, left at 2,5 cm (14 pt). |
| Tables within margins (2,5–18,6 cm) | PASS | Table bounding boxes x 2,5–18,6 cm (checked p4, p6, p11, p14). |
| No mid-word header wraps | FAIL | p8 Table 5: «CV ROC-/AUC», «CV PR-/AUC ± σ». p11 Table 6: «Персистент/ність», «Мажоритар/ний клас» (mid-word), «Навчання, /с». |
| Numbers not broken inside cells | FAIL | p11 Table 6, F1 and MCC columns: «0,00/00», «0,68/14», «0,67/27», «0,000/0», «0,62/72», «0,71/15» etc. (every row). p8 Table 5: «0,7456 ±/0,0338» and similar. |
| Tables not split across pages | FAIL | p6–p7 Table 3: only the last row («Календар») on p7, header not repeated. |
| Images not split from captions | PASS | All 9 figures stay on one page with their captions (p3, p6, p9, p10 ×2, p12, p13, p14, p15). |
| No blank or near-blank pages | PASS | No page below about 40 % of text. p1 is the intentional title page. |
| No huge gaps | FAIL | p5 ends at about y = 505 pt of 842; about 10 cm of blank space at the bottom because Рисунок 2 (p6) did not fit. |
| Appendix on a new page, А.1 … numbering | PASS | «Додаток А. Лістинг програми» starts p17 on a new page. Subsections А.1 (p17), А.2 (p19), А.3 (p28). |
| Code fragments readable | FAIL (p9) | Section 5 code (report lines 146–153): line 1 carries a long comment that wraps, leaving a stray «#» at the right margin and broken indentation across the block. |
| Appendix listing line wrap | MINOR | p17–p29: lines longer than about 90 characters wrap with uneven continuation indent (Courier 9,5 pt, as spec allows). Cosmetic. |
| Images readable at print size | FAIL (small text) | See table below. |
| Figure placement centered | MINOR | Images span x 2,82–18,92 cm, centered at 10,87 cm; text block centers at 10,55 cm. About 0,3 cm right offset on every figure (p3, p6, p9, p10, p12–p15). |
| Figure caption line breaks | MINOR | Рисунок 1 (p3) breaks at the en dash: «2018–» / «2026 роки». Рисунок 7 (p13) wraps to a second line («найбільших)»). Acceptable. |
| Body text justification | MINOR | p3 and p4: long inline code tokens (`pz2_hourly_features.parquet`, `rowsBetween(-(n−1), 0)`) cause wide word gaps on the justified line. p16: list items show the same gaps. |
| Page count | INFO | Main body p2–p16 (15 pages); appendix p17–p29 (13 pages). Upper limit of the RULES range (10–15 pages main body). |

## Image legibility (estimated from raster geometry)

Effective resolution of every image is 235–285 dpi (fine). The problem is the size of text inside each image at its placed width of 16,1 cm.

| Item | Page | Approx. text size at print | Verdict |
|---|---|---|---|
| Рисунок 3, terminal screenshot (ML Pipeline stages) | p9 | about 5 pt | FAIL. Too small. Enlarge the font in the source, or split into two lines of output. |
| Рисунок 4, Spark UI Jobs screenshot | p10 | about 4–5 pt | FAIL. Tables and labels unreadable. Crop to the Jobs table and use a larger browser zoom. |
| Рисунок 6, three-panel ROC-AUC / PR-AUC / F1 bars | p12 | about 6 pt (ticks, legend) | Borderline. Increase tick font or use two panels. |
| Рисунок 7, feature importance (RF, GBT, LR) | p13 | about 5–6 pt (feature names) | Borderline to FAIL. Feature names are the key content. Increase font or reduce to 12 bars with larger text. |
| Рисунок 8, confusion matrices | p14 | about 7–8 pt | PASS (legible). |
| Рисунок 9, error by year and hour | p15 | left panel about 7 pt, right panel about 5 pt | Borderline. |
| Рисунок 1, ACF plots | p3 | about 6–7 pt | Borderline, readable. |
| Рисунок 2, rv and threshold time series | p6 | about 6 pt (axis labels) | Borderline, readable. |
| Рисунок 5, ROC/PR curves | p10 | about 7 pt (legend) | PASS. |

## Recommended fixes (priority order)

1. Table 6 (p11): give the numeric columns enough width, reduce the first-column text size, or drop the «Навчання» column into a separate table, so no number wraps and no header breaks mid-word.
2. Table 3 (p6–p7): keep the table on one page (keep-with-next on rows), or move Рисунок 2 so the table is not split.
3. p5 gap: allow the text after Рисунок 2 reference to flow above the figure, or shrink Рисунок 2 so it fits p5 bottom.
4. Section 5 code (p9): shorten the comment on the first line and split long lines so no line wraps.
5. Table 5 (p8): replace «CV ROC-AUC» and «CV PR-AUC ± σ» with non-breaking hyphen (U+2011) or shorter headers; keep «0,7456 ± 0,0338» in one cell (no-break space around «±»).
6. Screenshots and figures (p9, p10, p13, p12, p15): raise the in-image font size, or crop to the relevant part.
7. Optional: align figures to the text-block center (remove the 0,3 cm offset), and reduce long inline code tokens in justified paragraphs (p3, p4) to avoid wide word gaps.

## Verdict

Layout and structure PASS. FORMAT not met on: Table 6 numeric and header wraps (p11), Table 3 page split (p6–p7), Table 5 header wraps (p8), large gap p5, broken code wrap (p9), and small text in Рисунок 3, 4, 7 (p9, p10, p13).
