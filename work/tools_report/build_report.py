#!/usr/bin/env python3
"""Markdown lab report -> .docx + .pdf per work/FORMAT.md.

python build_report.py --md work/pzN/report.md --pz N --theme "Тема" --out work/pzN/build/ПЗN_АВД_Зелінський_ДМм-26-1
"""
import argparse, os, re, subprocess, shutil, sys
from docx import Document
from docx.enum.table import WD_TABLE_ALIGNMENT, WD_CELL_VERTICAL_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH as AL, WD_LINE_SPACING
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Cm, Pt, Emu
from PIL import Image

TNR, ARIAL, COUR = "Times New Roman", "Arial", "Courier New"
TEXT_W = 21.0 - 2.5 - 2.4  # 16.1 cm
MAX_IMG_H = 15.5  # cm (~60 % of page height)
DASH = "–"


def set_font(run, name=TNR, size=None, bold=None, italic=None):
    run.font.name = name
    rpr = run._element.get_or_add_rPr()
    rf = rpr.find(qn("w:rFonts"))
    if rf is None:
        rf = OxmlElement("w:rFonts"); rpr.insert(0, rf)
    for a in ("w:ascii", "w:hAnsi", "w:eastAsia", "w:cs"):
        rf.set(qn(a), name)
    if size: run.font.size = Pt(size)
    if bold is not None: run.font.bold = bold
    if italic is not None: run.font.italic = italic


def pf(p, align=None, left=None, first=None, before=0, after=0, line=1.5, kwn=False, kt=False, pbb=False):
    f = p.paragraph_format
    if align is not None: f.alignment = align
    f.left_indent = Cm(left) if left is not None else None
    f.first_line_indent = Cm(first) if first is not None else None
    f.space_before, f.space_after = Pt(before), Pt(after)
    f.line_spacing = line
    f.keep_with_next = kwn
    f.keep_together = kt
    f.page_break_before = pbb
    return p


INLINE = re.compile(r"(`[^`]+`|\*\*[^*]+\*\*|(?<![\w*])\*[^*\s][^*]*\*(?![\w*]))")


def add_inline(p, text, size=14, base_bold=None):
    for part in INLINE.split(text):
        if not part: continue
        if part.startswith("`") and part.endswith("`") and len(part) > 2:
            set_font(p.add_run(part[1:-1]), COUR, size * 0.9 if size >= 12 else size, base_bold)
        elif part.startswith("**") and part.endswith("**") and len(part) > 4:
            set_font(p.add_run(part[2:-2]), TNR, size, True)
        elif part.startswith("*") and part.endswith("*") and len(part) > 2:
            set_font(p.add_run(part[1:-1]), TNR, size, base_bold, True)
        else:
            set_font(p.add_run(part), TNR, size, base_bold)


def setup(doc):
    s = doc.sections[0]
    s.page_width, s.page_height = Cm(21.0), Cm(29.7)
    s.left_margin, s.right_margin = Cm(2.5), Cm(2.4)
    s.top_margin, s.bottom_margin = Cm(2.0), Cm(2.0)
    s.header_distance, s.footer_distance = Cm(1.25), Cm(1.25)
    s.different_first_page_header_footer = True
    st = doc.styles["Normal"]
    st.font.name = TNR; st.font.size = Pt(14)
    rf = st.element.get_or_add_rPr().find(qn("w:rFonts"))
    for a in ("w:ascii", "w:hAnsi", "w:eastAsia", "w:cs"): rf.set(qn(a), TNR)
    f = st.paragraph_format
    f.space_before = f.space_after = Pt(0); f.line_spacing = 1.5
    f.alignment = AL.JUSTIFY; f.first_line_indent = Cm(1.25)
    # page number header (not on first page)
    hp = s.header.paragraphs[0]
    pf(hp, AL.RIGHT, left=0, first=0, line=1.0)
    r = hp.add_run(); set_font(r, ARIAL, 11)
    for t, txt in (("begin", None), (None, " PAGE "), ("end", None)):
        if t:
            e = OxmlElement("w:fldChar"); e.set(qn("w:fldCharType"), t)
        else:
            e = OxmlElement("w:instrText"); e.set(qn("xml:space"), "preserve"); e.text = txt
        r._element.append(e)
    s.first_page_header.is_linked_to_previous = False  # empty


def no_borders(tbl):
    tblPr = tbl._tbl.tblPr
    b = OxmlElement("w:tblBorders")
    for k in ("top", "left", "bottom", "right", "insideH", "insideV"):
        e = OxmlElement(f"w:{k}"); e.set(qn("w:val"), "nil"); b.append(e)
    tblPr.append(b)


def grid_borders(tbl, sz=4):
    b = OxmlElement("w:tblBorders")
    for k in ("top", "left", "bottom", "right", "insideH", "insideV"):
        e = OxmlElement(f"w:{k}")
        e.set(qn("w:val"), "single"); e.set(qn("w:sz"), str(sz)); e.set(qn("w:space"), "0"); e.set(qn("w:color"), "000000")
        b.append(e)
    tbl._tbl.tblPr.append(b)


def fix_layout(tbl, widths_cm, pad_cm=0.19):
    tblPr = tbl._tbl.tblPr
    lay = OxmlElement("w:tblLayout"); lay.set(qn("w:type"), "fixed"); tblPr.append(lay)
    w = OxmlElement("w:tblW"); w.set(qn("w:w"), str(int(sum(widths_cm) / 2.54 * 1440))); w.set(qn("w:type"), "dxa")
    old = tblPr.find(qn("w:tblW"))
    if old is not None: tblPr.remove(old)
    tblPr.append(w)
    mar = OxmlElement("w:tblCellMar")
    for k in ("left", "right"):
        e = OxmlElement(f"w:{k}"); e.set(qn("w:w"), str(int(pad_cm / 2.54 * 1440))); e.set(qn("w:type"), "dxa"); mar.append(e)
    tblPr.append(mar)
    grid = tbl._tbl.tblGrid
    for gc, wc in zip(grid.findall(qn("w:gridCol")), widths_cm):
        gc.set(qn("w:w"), str(int(wc / 2.54 * 1440)))
    for row in tbl.rows:
        for c, wc in zip(row.cells, widths_cm):
            c.width = Cm(wc)


def title_page(doc, pz, theme):
    def line(text="", **kw):
        p = doc.add_paragraph(); pf(p, AL.CENTER, left=0, first=0, **kw)
        if text: set_font(p.add_run(text), TNR, 14)
        return p
    def blanks(n):
        for _ in range(n): line()
    line("Міністерство освіти і науки України")
    line("Харківський національний університет радіоелектроніки")
    blanks(4)
    line("Кафедра штучного інтелекту")
    blanks(3)
    line("Дисципліна «Аналітика великих даних»")
    line(f"Практичне заняття №{pz}")
    line(f"«{theme}»")
    blanks(5)
    t = doc.add_table(rows=3, cols=2)
    no_borders(t); fix_layout(t, [8.3, 7.8])
    cells = [("Виконав:", "Прийняв:"), ("ст. гр. ДМм-26-1", "Золотухін О.В."), ("Зелінський В.С.", "")]
    for r, (a, b) in zip(t.rows, cells):
        for c, txt in zip(r.cells, (a, b)):
            p = c.paragraphs[0]; pf(p, AL.LEFT, left=0, first=0)
            if txt: set_font(p.add_run(txt), TNR, 14)
    blanks(7 if len(theme) > 62 else 8)
    line("Харків – 2026")


def heading(doc, text, level, appendix_sub=False, appendix_title=False, first_content=False):
    p = doc.add_paragraph()
    if appendix_title:
        pf(p, AL.CENTER, left=0, first=0, kwn=True, pbb=True)
        add_inline(p, text, 14, True)
    elif appendix_sub:
        pf(p, AL.LEFT, left=0, first=0, kwn=True)
        add_inline(p, text, 14, True)
    elif level == 1:
        pf(p, AL.CENTER, left=1.25, first=0, kwn=True, pbb=first_content)
        add_inline(p, text, 14)
    else:
        pf(p, AL.LEFT, left=1.25, first=0, kwn=True)
        add_inline(p, text, 14)
    return p


def body(doc, text, first_content=False):
    p = doc.add_paragraph(); pf(p, AL.JUSTIFY, left=0, first=1.25, pbb=first_content)
    add_inline(p, text, 14); return p


def bullet(doc, text):
    p = doc.add_paragraph(); pf(p, AL.JUSTIFY, left=1.25, first=-0.6)
    p.paragraph_format.tab_stops.add_tab_stop(Cm(1.25))
    add_inline(p, DASH + "\t" + text, 14); return p


def code_block(doc, lines):
    n = len(lines)
    for i, ln in enumerate(lines):
        p = doc.add_paragraph()
        pf(p, AL.LEFT, left=0.5, first=0, line=1.0, before=6 if i == 0 else 0, after=6 if i == n - 1 else 0)
        p.paragraph_format.widow_control = False
        if ln:
            r = p.add_run(ln.replace("\t", "    ")); set_font(r, COUR, 9.5)
        else:
            r = p.add_run(""); set_font(r, COUR, 9.5)
            p._p.get_or_add_pPr()
        # keep empty paragraph height at code size
        ppr = p._p.get_or_add_pPr(); rpr = OxmlElement("w:rPr")
        sz = OxmlElement("w:sz"); sz.set(qn("w:val"), "19"); rpr.append(sz); ppr.append(rpr)


def table_caption(doc, text):
    p = doc.add_paragraph(); pf(p, AL.LEFT, left=0, first=0, before=6, line=1.0, kwn=True)
    add_inline(p, text, 14); return p


def figure_caption(doc, text):
    p = doc.add_paragraph(); pf(p, AL.CENTER, left=0, first=0, before=5, after=6, line=1.0)
    add_inline(p, text, 13); return p


def figure(doc, path, mdir):
    full = path if os.path.isabs(path) else os.path.join(mdir, path)
    if not os.path.exists(full):
        print(f"WARNING: image not found: {full}", file=sys.stderr)
        return placeholder(doc, f"[ЗОБРАЖЕННЯ НЕ ЗНАЙДЕНО: {path}]")
    with Image.open(full) as im: w, h = im.size
    wc = TEXT_W; hc = wc * h / w
    if hc > MAX_IMG_H: hc = MAX_IMG_H; wc = hc * w / h
    p = doc.add_paragraph(); pf(p, AL.CENTER, left=0, first=0, line=1.0, kwn=True)
    p.add_run().add_picture(full, width=Cm(wc), height=Cm(hc))


def placeholder(doc, text):
    t = doc.add_table(rows=1, cols=1); t.alignment = WD_TABLE_ALIGNMENT.CENTER
    grid_borders(t, 6); wc = 12.0; fix_layout(t, [wc])
    row = t.rows[0]
    trPr = row._tr.get_or_add_trPr()
    h = OxmlElement("w:trHeight"); h.set(qn("w:val"), str(int(5 / 2.54 * 1440))); h.set(qn("w:hRule"), "atLeast"); trPr.append(h)
    c = row.cells[0]; c.vertical_alignment = WD_CELL_VERTICAL_ALIGNMENT.CENTER
    p = c.paragraphs[0]; pf(p, AL.CENTER, left=0, first=0, line=1.0, kwn=True)
    set_font(p.add_run(text), TNR, 14)


def split_row(line):
    s = line.strip()
    if s.startswith("|"): s = s[1:]
    if s.endswith("|"): s = s[:-1]
    return [c.strip().replace("\\|", "|") for c in re.split(r"(?<!\\)\|", s)]


def pipe_table(doc, rows):
    hdr = rows[0]; data = rows[2:]
    n = len(hdr)
    data = [r + [""] * (n - len(r)) if len(r) < n else r[:n] for r in data]
    allr = [hdr] + data
    size = 10.5 if n >= 8 else 11
    # widths by content length (longest word and total length)
    def need(j):
        mx = max(len(r[j].replace("`", "")) for r in allr)
        mw = max((len(w) for r in allr for w in r[j].replace("`", "").split()), default=1)
        return max(min(mx, 40), mw + 2, 4)
    ns = [need(j) for j in range(n)]
    tot = sum(ns); ws = [max(1.6, TEXT_W * x / tot) for x in ns]
    k = TEXT_W / sum(ws); ws = [w * k for w in ws]
    if n <= 3 and tot < 30:  # narrow table
        ws = [w * min(1.0, 12.0 / TEXT_W) for w in ws]
    t = doc.add_table(rows=len(allr), cols=n); t.alignment = WD_TABLE_ALIGNMENT.CENTER
    grid_borders(t); fix_layout(t, ws)
    for i, r in enumerate(allr):
        row = t.rows[i]
        trPr = row._tr.get_or_add_trPr()
        e = OxmlElement("w:cantSplit"); trPr.append(e)
        if i == 0:
            e = OxmlElement("w:tblHeader"); trPr.append(e)
        for j, txt in enumerate(r):
            c = row.cells[j]; c.vertical_alignment = WD_CELL_VERTICAL_ALIGNMENT.CENTER
            p = c.paragraphs[0]
            al = AL.CENTER if (i == 0 or j > 0) else AL.LEFT
            pf(p, al, left=0, first=0, line=1.0, kwn=(i < len(allr) - 1 and len(allr) <= 12))
            add_inline(p, txt, size)


def convert(md_text, mdir, doc, pz, theme):
    title_page(doc, pz, theme)
    lines = md_text.replace("\r\n", "\n").split("\n")
    i = 0; first = True; in_appendix = False; para = []
    def flush():
        nonlocal para, first
        if para:
            body(doc, " ".join(s.strip() for s in para), first); first = False; para = []
    while i < len(lines):
        ln = lines[i]; s = ln.strip()
        if s.startswith("```"):
            flush(); i += 1; buf = []
            while i < len(lines) and not lines[i].strip().startswith("```"):
                buf.append(lines[i].rstrip()); i += 1
            i += 1
            code_block(doc, buf); first = False; continue
        if not s:
            flush(); i += 1; continue
        m = re.match(r"^(#{1,3})\s+(.*)$", s)
        if m:
            flush(); lvl = len(m.group(1)); txt = m.group(2).strip()
            if lvl == 1 and txt.startswith("Додаток"):
                in_appendix = True; heading(doc, txt, 1, appendix_title=True)
            elif lvl > 1 and re.match(r"^[А-ЯA-Z]\.\d+\s", txt):
                heading(doc, txt, lvl, appendix_sub=True)
            else:
                heading(doc, txt, lvl, first_content=first)
            first = False; i += 1; continue
        m = re.match(r"^!\[(.*?)\]\((.*?)\)\s*$", s)
        if m:
            flush(); cap, path = m.groups()
            figure(doc, path, mdir)
            if cap.strip(): figure_caption(doc, cap.strip())
            first = False; i += 1; continue
        m = re.match(r"^\[(СКРІНШОТ[^\]]*|ЗАПОВНИТИ[^\]]*)\]$", s)
        if m:
            flush(); placeholder(doc, "[" + m.group(1) + "]"); first = False; i += 1; continue
        if re.match(r"^Рисунок \d+\s+[–\-—]\s", s) and not para:
            figure_caption(doc, s.replace(" - ", f" {DASH} ").replace(" — ", f" {DASH} ")); first = False; i += 1; continue
        if re.match(r"^Таблиця \d+\s+[–\-—]\s", s) and not para:
            table_caption(doc, s.replace(" - ", f" {DASH} ").replace(" — ", f" {DASH} ")); first = False; i += 1; continue
        if s.startswith("|"):
            flush(); rows = []
            while i < len(lines) and lines[i].strip().startswith("|"):
                rows.append(split_row(lines[i])); i += 1
            if len(rows) >= 2 and all(re.match(r"^:?-{2,}:?$", c.replace(" ", "")) for c in rows[1]):
                pipe_table(doc, rows)
                # spacer after table: small gap
                sp = doc.add_paragraph(); pf(sp, AL.LEFT, left=0, first=0, line=1.0)
                set_font(sp.add_run(""), TNR, 6)
                sp.paragraph_format.line_spacing = Pt(8); sp.paragraph_format.line_spacing_rule = WD_LINE_SPACING.EXACTLY
            else:
                for r in rows: body(doc, " | ".join(r))
            first = False; continue
        m = re.match(r"^[-*]\s+(.*)$", s) or re.match(r"^\d+[.)]\s+(.*)$", s)
        if m:
            flush(); bullet(doc, m.group(1)); first = False; i += 1; continue
        para.append(s); i += 1
    flush()


def to_pdf(docx_path):
    outdir = os.path.dirname(os.path.abspath(docx_path))
    exe = shutil.which("soffice") or shutil.which("libreoffice")
    subprocess.run([exe, "--headless", "--convert-to", "pdf", "--outdir", outdir, docx_path],
                   check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--md", required=True); ap.add_argument("--pz", required=True)
    ap.add_argument("--theme", required=True); ap.add_argument("--out", required=True, help="output path without extension")
    ap.add_argument("--no-pdf", action="store_true")
    a = ap.parse_args()
    out = re.sub(r"\.(docx|pdf)$", "", a.out)
    os.makedirs(os.path.dirname(os.path.abspath(out)), exist_ok=True)
    doc = Document(); setup(doc)
    # drop python-docx default empty first paragraph if any
    for p in list(doc.paragraphs):
        if not p.text and not p._p.xpath(".//w:drawing"): p._p.getparent().remove(p._p)
    with open(a.md, encoding="utf-8") as f: txt = f.read()
    convert(txt, os.path.dirname(os.path.abspath(a.md)), doc, a.pz, a.theme)
    doc.save(out + ".docx")
    if not a.no_pdf: to_pdf(out + ".docx")
    print(out + ".docx", out + ".pdf")


if __name__ == "__main__":
    main()
