# STYLECHECK: PZ1 report.md against RULES "Writing style"

Scope: report.md main body (lines 1–273). Appendix A code checked separately where noted. Method: grep and Unicode-aware Python scripts over report.md; image paths checked with a script.

## Summary

| Check | Result |
|---|---|
| Em dash «—» | PASS, 0 occurrences in report.md (body and appendix) |
| Banned phrases (any inflection) | PASS, 0 hits by stem: варто/зазнач, важлив/підкреслю/підкреслит, у сучасному світі, відіграє/відіграють/відіграв, таким чином, комплексн, глибок/занурен, не просто, ключов/роль |
| En dash only, no spaced hyphen as dash | PASS in body. Appendix code comments and docstrings use spaced hyphen (e.g. line 282 `mapper_year.py - Mapper`, 288, 327, 329, 371, 439, 630, 663, 1047). Code, so not a prose defect, but a reader may treat them as dashes |
| Decimal comma in prose and tables | PASS. Dotted tokens are version strings (3.3.6, 3.1.3, 1.8.0_472, 3.13.18, 3.0.5, 4.3.0, Hadoop 3.3) and dates (11.11.2026, 06.02.2018). Not decimals |
| Decimal comma in figure labels | DEFECT (minor). Figure 4 bar labels use a decimal point (5.0, 0.6, 4.7, 5.5 …) |
| Bold inside paragraphs | PASS, 0 occurrences in body |
| Bullet lists | DEFECT (minor). Висновки (lines 264–270) is a numbered list of four items, rendered as dash bullets in the PDF (pages 15–16). RULES: no bullet lists where a sentence suffices. The card (cards/pz1.md, item 3) asks for conclusions "за запропонованим планом" with three topics; the fourth item (Доцільність Hadoop) is outside the plan |
| First person | PASS. No я, ми, мені, наш, маємо, бачимо, перевірили and similar in the body (Unicode-aware check) |
| Impersonal forms | PASS. Forms like «реалізовано», «отримано», «встановлено» are used throughout |
| Quotes «» | PASS in body, no straight quotes |
| Minus sign | PASS. U+2212 used in «High − Low» |
| Numbering continuity: sections | PASS. 1–7, subsections 4.1–4.4 and 6.1–6.2 continuous |
| Numbering continuity: Tables | PASS. Таблиця 1–7 in order of appearance |
| Numbering continuity: Figures | PASS. Рисунок 1–9 in order of appearance |
| Numbering continuity: Appendix | PASS. А.1–А.17 continuous, Cyrillic А throughout |
| References before objects | PASS. Each table and figure is referenced in the text before its line (Table 1 at line 29, Table 2 at 61, Table 3 at 122, Table 4 at 164, Table 5 at 205, Table 6 at 230, Table 7 at 247; Figure 1 at 21 … Figure 9 at 222) |
| Image paths exist | PASS. 9 of 9 referenced paths exist (figures/ and screenshots/) |
| Body length | PASS. Prose 2 249 words (budget 2 200–2 700). Theory 244 words (≤350). Figures 9 (≤12). Tables 7 (≤8). Code fragments in body ≤7 lines (≤8) |
| Words that break a number | See FORMATCHECK.md F2 (layout, not wording) |

## Findings

- S1. Figure 4 (report.md line 140, file figures/fig_mr_yearly_volume.png). Bar labels use decimal points. RULES: decimal comma in text and tables. Fix in make_figures.py (format with a comma) and rebuild the figure.
- S2. Висновки (report.md lines 264–270). Numbered list with four items, rendered as bullets. Rewrite as prose paragraphs, or keep the list only if the instructor's plan requires it. The fourth item is not in the card plan.
- S3. Appendix code comments use spaced hyphens (lines 282, 288, 327, 329, 371, 439, 630, 663, 1047 and others). Acceptable in code; if RULES are meant to cover the whole file, replace with «–» in comments and docstrings. Not counted as a body defect.

## Not defects (checked and accepted)

- Table 7 and timing sentences contain «[ЗАПОВНИТИ ПІСЛЯ ЗАПУСКУ]» (intentional).
- The «Мета роботи» paragraph (line 3) opens with «Мета роботи:», repeating its heading. Content choice, not a RULES violation.
