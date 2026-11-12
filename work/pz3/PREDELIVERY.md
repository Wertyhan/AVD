# Pre-delivery check: ПЗ3_АВД_Зелінський_ДМм-26-1.pdf

Scope: pages 1-17 rendered at 110 dpi (pymupdf) into `build/pages_final/page_01.png` ... `page_17.png` and reviewed visually. Text grep over all 49 pages.

## Result: OK with 2 minor items for author decision

## Visual review (pages 1-17)
- p1 title page: all required lines present (Міністерство освіти і науки України, ХНУРЕ, Кафедра штучного інтелекту, дисципліна, Практичне заняття №3, тема, Виконав ст. гр. ДМм-26-1 Зелінський В.С., Прийняв Золотухін О.В., Харків – 2026). OK.
- p2 starts with the work title. OK.
- Figures 1-8 (p3, p4, p5, p6, p8, p12, p13, p15): all readable at 110 dpi; captions match content. Fig. 3 caption notes omitted columns. OK.
- Tables 1-8 (p3, p4, p7, p10, p11, p13, p14, p15): no broken layout, no overflow, all cells legible. OK.
- Figure and table numbers referenced in the text: Рисунок 1-8 and Таблиця 1-8 all referenced and all defined. OK.
- No blank pages, no text overflow, no placeholder text. OK.
- Appendix A («Додаток А. Лістинг програми») starts on a new page (p17). OK.

## Text grep (all 49 pages)
- `/home/user`: no hits.
- `coordinator`: no hits.
- `tools_report`: no hits.
- `Claude`: no hits.
- `агент`: no hits.
- `координатор`: 1 hit, p4: "завантаження координатора" (Kafka group coordinator, a legitimate technical term). Author to confirm it is intended.
- «ЗАПОВНИТИ», «СКРІНШОТ», «—» (em dash), TODO/TBD/Lorem: no hits.

## Minor items
1. p4: "Processed a total of 30240 messages з нульовим lag." is an English log quote in the body text. Translate or keep as a labelled quote.
2. p4: "координатора" hit (see above). Probably intended.

## Defects requiring a fix
None.
