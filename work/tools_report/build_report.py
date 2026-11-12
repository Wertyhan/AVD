#!/usr/bin/env python3
"""Markdown lab report -> .docx + .pdf per work/FORMAT.md.

python build_report.py --md work/pzN/report.md --pz N --theme "Тема" --out work/pzN/build/ПЗN_АВД_Зелінський_ДМм-26-1
"""
import argparse, os, re, subprocess, shutil, sys
from docx import Document
from docx.enum.table import WD_TABLE_ALIGNMENT, WD_CELL_VERTICAL_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH as AL, WD_LINE_SPACING, WD_COLOR_INDEX
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Cm, Pt, Emu
from PIL import Image

TNR, ARIAL, COUR = "Times New Roman", "Arial", "Courier New"
_FONT = next((p for p in ("/usr/share/fonts/truetype/liberation/LiberationSerif-Regular.ttf",
                          "/usr/share/fonts/truetype/msttcorefonts/Times_New_Roman.ttf") if os.path.exists(p)), "")
TEXT_W = 21.0 - 2.5 - 2.4  # 16.1 cm
IMG_W = 16.0
MAX_IMG_H = 10.0  # cm
MAX_IMG_H_TERM = 9.0  # cm, terminal screenshots
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
    if left == 0 or first == 0:
        f.right_indent = Cm(0)
    return p


INLINE = re.compile(r"(`[^`]+`|\*\*[^*]+\*\*|(?<![\w*])\*[^*\s][^*]*\*(?![\w*]))")


PH = re.compile(r"(\[ЗАПОВНИТИ[^\]]*\])")


def add_inline(p, text, size=14, base_bold=None):
    for seg in PH.split(text):
        if not seg: continue
        if PH.fullmatch(seg):
            r = p.add_run(seg); set_font(r, TNR, size, base_bold)
            r.font.highlight_color = WD_COLOR_INDEX.YELLOW
        else:
            _inline(p, seg, size, base_bold)


NB_NUM = re.compile(r"(?<=\d) (?=\d{3}\b)")
NB_UNIT = re.compile(r"(?<=\d) (?=(?:%|с|мс|МБ|ГБ)(?!\w))")


def nbsp(t):
    return NB_UNIT.sub("\u00a0", NB_NUM.sub("\u00a0", t))


def _inline(p, text, size, base_bold):
    for part in INLINE.split(text):
        if not part: continue
        if part.startswith("`") and part.endswith("`") and len(part) > 2:
            set_font(p.add_run(part[1:-1]), COUR, size * 0.9 if size >= 12 else size, base_bold)
        elif part.startswith("**") and part.endswith("**") and len(part) > 4:
            set_font(p.add_run(nbsp(part[2:-2])), TNR, size, True)
        elif part.startswith("*") and part.endswith("*") and len(part) > 2:
            set_font(p.add_run(nbsp(part[1:-1])), TNR, size, base_bold, True)
        else:
            set_font(p.add_run(nbsp(part)), TNR, size, base_bold)


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
    hs = hp.style
    hs.font.name = ARIAL; hs.font.size = Pt(11)
    hrf = hs.element.get_or_add_rPr().find(qn("w:rFonts"))
    for a in ("w:ascii", "w:hAnsi", "w:eastAsia", "w:cs"): hrf.set(qn(a), ARIAL)
    ppr = hp._p.get_or_add_pPr(); mrpr = OxmlElement("w:rPr")
    mrf = OxmlElement("w:rFonts")
    for a in ("w:ascii", "w:hAnsi", "w:eastAsia", "w:cs"): mrf.set(qn(a), ARIAL)
    mrpr.append(mrf)
    for tag in ("w:sz", "w:szCs"):
        e = OxmlElement(tag); e.set(qn("w:val"), "22"); mrpr.append(e)
    ppr.append(mrpr)
    for t, txt in (("begin", None), (None, " PAGE "), ("separate", None), ("res", "2"), ("end", None)):
        r = hp.add_run(); set_font(r, ARIAL, 11)
        if t == "res":
            r.text = txt; continue
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


def numbered(doc, num, text):
    p = doc.add_paragraph(); pf(p, AL.JUSTIFY, left=1.25, first=-0.6)
    p.paragraph_format.tab_stops.add_tab_stop(Cm(1.25))
    add_inline(p, f"{num}.\t" + text, 14); return p


def code_block(doc, lines):
    """Body code: 9.5 pt; if a line is wider than the text width at 9.5 pt, whole block drops to 9 pt.
    Lines still too long wrap with a hanging indent."""
    n = len(lines)
    lines = [ln.replace("\t", "    ") for ln in lines]
    mx = max((len(ln) for ln in lines), default=0)
    avail = TEXT_W - 1.0  # left indent 1.0 cm
    def fits(sz): return mx * sz * 0.6 / 72 * 2.54 <= avail  # Courier advance = 0.6 em
    size = 9.5 if fits(9.5) else 9.0
    for i, ln in enumerate(lines):
        p = doc.add_paragraph()
        pf(p, AL.LEFT, left=1.0, first=-0.5, line=1.0, before=6 if i == 0 else 0, after=6 if i == n - 1 else 0)
        p.paragraph_format.widow_control = False
        r = p.add_run(ln); set_font(r, COUR, size)
        ppr = p._p.get_or_add_pPr(); rpr = OxmlElement("w:rPr")
        sz = OxmlElement("w:sz"); sz.set(qn("w:val"), str(int(size * 2))); rpr.append(sz); ppr.append(rpr)


def table_caption(doc, text):
    p = doc.add_paragraph(); pf(p, AL.LEFT, left=0, first=0, before=6, line=1.0, kwn=True)
    add_inline(p, text, 14); return p


def figure_caption(doc, text):
    p = doc.add_paragraph(); pf(p, AL.CENTER, left=0, first=0, before=5, after=6, line=1.0, kt=True)
    add_inline(p, text, 13); return p


IMG_CACHE = None  # set in main(): build cache dir for cropped copies
MAX_IMG_H_CHART = 11.5  # cm
MIN_IMG_H_CHART = 6.0   # cm, floor for the pre-pass shrink
MIN_CHART_W = 14.0      # cm, floor width for charts in the pre-pass shrink
IMG_SCALE = {}          # basename -> shrink factor (pre-pass)
FIG_LOG = []            # (basename, kind) of figures in document order
MAX_IMG_H_SHOT = 12.0   # cm, terminal / web UI screenshots


def classify(bn):
    if bn.startswith("term_") or re.match(r"^s\d\d_", bn): return "term"
    if bn.startswith(("spark_ui", "namenode", "yarn", "hdfs", "web_", "ui_")) or "screenshot" in bn: return "web"
    return "chart"


def autocrop(full, kind, pad=14):
    """Crop uniform margins (and, for terminals, empty trailing columns). Returns (path, text_pt_estimate)."""
    import numpy as np
    with Image.open(full) as im0:
        im = im0.convert("RGB")
    a = np.asarray(im).astype(int)
    corners = [tuple(a[0, 0]), tuple(a[0, -1]), tuple(a[-1, 0]), tuple(a[-1, -1])]
    bg = max(set(corners), key=corners.count)
    diff = np.abs(a - np.array(bg)).sum(axis=2) > 40
    ys, xs = np.where(diff)
    if len(ys) == 0: return full, None
    x0, x1 = max(xs.min() - pad, 0), min(xs.max() + pad + 1, im.width)
    y0, y1 = max(ys.min() - pad, 0), min(ys.max() + pad + 1, im.height)
    cr = im.crop((x0, y0, x1, y1))
    os.makedirs(IMG_CACHE, exist_ok=True)
    out = os.path.join(IMG_CACHE, os.path.splitext(os.path.basename(full))[0] + "_crop.png")
    cr.save(out)
    est = None
    if kind == "term":  # estimate text height: median pitch of text rows
        rows = diff[y0:y1, x0:x1].any(axis=1); runs = []; st = None
        for i, v in enumerate(rows):
            if v and st is None: st = i
            if not v and st is not None: runs.append(st); st = None
        if len(runs) > 2:
            pitch = float(np.median(np.diff(runs)))
            est = pitch / 1.25 * (TEXT_W / 2.54 * 72) / cr.width  # font px -> pt at full text width
    return out, est


def figure(doc, path, mdir):
    full = path if os.path.isabs(path) else os.path.join(mdir, path)
    if not os.path.exists(full):
        print(f"WARNING: image not found: {full}", file=sys.stderr)
        return placeholder(doc, f"[ЗОБРАЖЕННЯ НЕ ЗНАЙДЕНО: {path}]")
    kind = classify(os.path.basename(full).lower())
    try:
        full, est = autocrop(full, kind)
    except Exception as e:
        print(f"WARNING: autocrop failed for {full}: {e}", file=sys.stderr); est = None
    with Image.open(full) as im: w, h = im.size
    mh = MAX_IMG_H_CHART if kind == "chart" else MAX_IMG_H_SHOT
    wc = IMG_W
    hc = wc * h / w
    if hc > mh: hc = mh; wc = hc * w / h
    FIG_LOG.append((os.path.basename(full), kind))
    if kind == "chart" and IMG_SCALE.get(os.path.basename(full), 1.0) < 1.0:
        k = IMG_SCALE[os.path.basename(full)]
        hn = max(hc * MIN_CHART_W / wc, hc * k)  # never narrower than MIN_CHART_W
        if hn < hc: wc *= hn / hc; hc = hn
    if est: print(f"  {os.path.basename(full)}: terminal text ~{est:.1f} pt at full width; placed {wc:.1f}x{hc:.1f} cm")
    p = doc.add_paragraph(); pf(p, AL.CENTER, left=0, first=0, line=1.0, kwn=True)
    shp = p.add_run().add_picture(full, width=Cm(wc), height=Cm(hc))
    for a in ("distT", "distB", "distL", "distR"):  # LibreOffice defaults to 0.32 cm side gaps otherwise
        shp._inline.set(a, "0")


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


NUMRE = re.compile(r"^[\s\[\]\d.,:;%±+\-−–—/()*×x<>=~\u00a0\u2011\u202f]*$")
_FONTCACHE = {}


def tw(word, sz, bold=False):
    from PIL import ImageFont
    try:
        f = _FONTCACHE.setdefault("f", ImageFont.truetype(_FONT, 100))
        return f.getlength(word) / 100 * sz / 72 * 2.54
    except Exception:
        return len(word) * sz * 0.5 / 72 * 2.54


def pipe_table(doc, rows):
    hdr = rows[0]; data = rows[2:]
    n = len(hdr)
    data = [r + [""] * (n - len(r)) if len(r) < n else r[:n] for r in data]
    allr = [hdr] + data
    clean = lambda t: t.replace("`", "").replace("**", "")
    NB, NBH = "\u00a0", "\u2011"
    # numeric column: every body cell is a number-like; such cells never wrap
    numeric = [all(NUMRE.match(clean(r[j])) for r in data if clean(r[j])) and any(clean(r[j]) for r in data)
               for j in range(n)]
    PAD = 2 * 0.19 + 0.10

    def tokens(txt, j, hyph_ok, is_hdr):
        t = clean(txt)
        if numeric[j] and not is_hdr: return [t.replace(" ", NB)]
        out = []
        for w in t.split():
            if hyph_ok and not (numeric[j] and not is_hdr): out += [x for x in re.split(r"(?<=-)", w) if x]
            else: out.append(w)
        return out or ["x"]

    def minw(sz, hyph_ok):
        return [max(tw(tk, sz) for r_i, r in enumerate(allr) for tk in tokens(r[j], j, hyph_ok, r_i == 0)) + PAD
                for j in range(n)]
    def full(sz):  # width needed to keep each column on one line
        return [max(tw(clean(r[j]).replace(" ", NB) if numeric[j] and i else clean(r[j]), sz)
                    for i, r in enumerate(allr)) + PAD for j in range(n)]

    size = 11; hyph_ok = False
    for size in (11, 10, 9):
        mins = minw(size, False)
        if sum(mins) <= TEXT_W: break
    else:
        size = 9; hyph_ok = True; mins = minw(size, True)
        if sum(mins) > TEXT_W:
            print(f"WARNING: table too wide even at 9 pt ({sum(mins):.1f} cm) {[round(m,1) for m in mins]} {hdr[:2]}", file=sys.stderr)
    if sum(mins) > TEXT_W:
        k = TEXT_W / sum(mins); mins = [m * k for m in mins]
    fw = full(size)
    cap = [min(f, max(m, 5.5)) if not numeric[j] and n > 2 else f for j, (f, m) in enumerate(zip(fw, mins))]
    cap = [max(c, m) for c, m in zip(cap, mins)]
    narrow = n <= 3 and sum(cap) < (8.0 if n == 2 else 12.0)
    target = min(TEXT_W, 12.0) if narrow else TEXT_W
    if sum(cap) <= target:
        ws = [c * target / sum(cap) for c in cap]
    else:  # distribute spare width above minima by wish
        spare = max(target - sum(mins), 0); wish = [c - m for c, m in zip(cap, mins)]
        tw_ = sum(wish) or 1
        ws = [m + spare * x / tw_ for m, x in zip(mins, wish)]
    # in sizes below 11 keep font; hyphen-protect in header/body unless hyph_ok
    t = doc.add_table(rows=len(allr), cols=n); t.alignment = WD_TABLE_ALIGNMENT.CENTER
    grid_borders(t); fix_layout(t, ws)
    N = len(allr)
    for i, r in enumerate(allr):
        row = t.rows[i]
        trPr = row._tr.get_or_add_trPr()
        trPr.append(OxmlElement("w:cantSplit"))
        if i == 0: trPr.append(OxmlElement("w:tblHeader"))
        if N <= 15: kwn = i < N - 1
        else: kwn = i < 2 or i >= N - 3 and i < N - 1
        for j, txt in enumerate(r):
            c = row.cells[j]; c.vertical_alignment = WD_CELL_VERTICAL_ALIGNMENT.CENTER
            p = c.paragraphs[0]
            al = AL.CENTER if (i == 0 or j > 0) else AL.LEFT
            pf(p, al, left=0, first=0, line=1.0, kwn=kwn)
            if not hyph_ok or (numeric[j] and i):
                if numeric[j] and i: txt = txt.replace(" ", NB)
                txt = re.sub(r"(?<=\w)-(?=\w)", NBH, txt) if not (hyph_ok) else txt
            add_inline(p, txt, size)


def work_title(doc, md_text, theme):
    """Page 2: work title (= theme), centered, indent 1.25, + empty line. Returns True if added."""
    norm = lambda t: re.sub(r"[\s«»\"#*]+", " ", t).strip().lower()
    head = next((l for l in md_text.split("\n") if l.strip()), "")
    if norm(head) == norm(theme): return False
    p = doc.add_paragraph(); pf(p, AL.CENTER, left=1.25, first=0, pbb=True, kwn=True)
    set_font(p.add_run(theme), TNR, 14)
    e = doc.add_paragraph(); pf(e, AL.CENTER, left=1.25, first=0); set_font(e.add_run(""), TNR, 14)
    return True


def convert(md_text, mdir, doc, pz, theme):
    title_page(doc, pz, theme)
    has_title = work_title(doc, md_text, theme)
    lines = md_text.replace("\r\n", "\n").split("\n")
    i = 0; first = not has_title; in_appendix = False; para = []
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
        m = re.match(r"^(\d+)[.)]\s+(.*)$", s)
        if m:
            flush(); numbered(doc, m.group(1), m.group(2)); first = False; i += 1; continue
        m = re.match(r"^[-*]\s+(.*)$", s)
        if m:
            flush(); bullet(doc, m.group(1)); first = False; i += 1; continue
        para.append(s); i += 1
    flush()


def to_pdf(docx_path):
    outdir = os.path.dirname(os.path.abspath(docx_path))
    exe = shutil.which("soffice") or shutil.which("libreoffice")
    subprocess.run([exe, "--headless", "--convert-to", "pdf", "--outdir", outdir, docx_path],
                   check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def shrink_pass(pdf, gap_cm=5.0, caption_cm=1.3):
    """Find pages with a big blank bottom followed by a page that starts with a chart; shrink that chart."""
    import pymupdf
    pt = 72 / 2.54; bottom = 29.7 * pt - 2.0 * pt; top = 2.0 * pt
    doc = pymupdf.open(pdf); changed = False
    infos = [(pg, sorted(pg.get_image_info(), key=lambda d: d["bbox"][1])) for pg in doc]
    counts = [len(x[1]) for x in infos]
    for n, (pg, imgs) in enumerate(infos):
        if n == 0 or not imgs: continue
        tops = [b[1] for b in pg.get_text("blocks") if b[1] > top - 1]
        d = imgs[0]
        if d["bbox"][1] > min(tops + [d["bbox"][1]]) + 2: continue
        fi = sum(counts[:n])
        if fi >= len(FIG_LOG): continue
        name, kind = FIG_LOG[fi]
        if kind != "chart": continue
        prev = infos[n - 1][0]
        ys = [b[3] for b in prev.get_text("blocks") if b[1] > top] + [x["bbox"][3] for x in prev.get_image_info()]
        blank = (bottom - max(ys, default=top)) / pt
        h = (d["bbox"][3] - d["bbox"][1]) / pt
        if blank <= gap_cm or h <= 0.5: continue
        k = max(MIN_CHART_W / IMG_W, min(1.0, (blank - caption_cm) / h))
        if blank - caption_cm < h * k: continue  # still would not fit
        IMG_SCALE[name] = IMG_SCALE.get(name, 1.0) * k; changed = True
    return changed


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--md", required=True); ap.add_argument("--pz", required=True)
    ap.add_argument("--theme", required=True); ap.add_argument("--out", required=True, help="output path without extension")
    ap.add_argument("--no-pdf", action="store_true")
    a = ap.parse_args()
    out = re.sub(r"\.(docx|pdf)$", "", a.out)
    os.makedirs(os.path.dirname(os.path.abspath(out)), exist_ok=True)
    global IMG_CACHE
    IMG_CACHE = os.path.join(os.path.dirname(os.path.abspath(out)), "imgcache")
    with open(a.md, encoding="utf-8") as f: txt = f.read()
    def build():
        FIG_LOG.clear()
        doc = Document(); setup(doc)
        for p in list(doc.paragraphs):
            if not p.text and not p._p.xpath(".//w:drawing"): p._p.getparent().remove(p._p)
        convert(txt, os.path.dirname(os.path.abspath(a.md)), doc, a.pz, a.theme)
        doc.save(out + ".docx")
        if not a.no_pdf: to_pdf(out + ".docx")
    IMG_SCALE.clear(); build()
    if not a.no_pdf:
        for it in range(2):
            if not shrink_pass(out + ".pdf"): break
            print(f"gap pre-pass iteration {it + 1}: {IMG_SCALE}")
            build()
    print(out + ".docx", out + ".pdf")


if __name__ == "__main__":
    main()
