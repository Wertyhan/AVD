# -*- coding: utf-8 -*-
"""compose_figs_pz1.py – збирання рисунків звіту ПЗ1 з готових реальних скріншотів (без зміни вмісту):
копії термінальних PNG під іменами term_*, вертикальне поєднання двох термінальних знімків,
обрізання веб-знімків до потрібних стовпців/панелей для читабельності при ширині 16 см."""
import shutil
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

P = Path(__file__).resolve().parent.parent
SH, SRC, FIG = P / 'screenshots', P / 'screenshots/ui_panel_src', P / 'figures'
BENCH_FIG = P.parent / 'bench/figures/fig_bench.png'


def stack(names, out, gap=16, bg=(255, 255, 255)):
    """Два термінальні знімки один під одним (ширина однакова, 1600 px)."""
    ims = [Image.open(SH / f'{n}.png').convert('RGB') for n in names]
    w = max(i.width for i in ims); h = sum(i.height for i in ims) + gap * (len(ims) - 1)
    cv = Image.new('RGB', (w, h), bg); y = 0
    for im in ims:
        cv.paste(im, (0, y)); y += im.height + gap
    cv.save(FIG / out)


def panel(items, out, width=1440):
    """Веб-знімки з підписами (а), (б) один під одним."""
    F = ImageFont.truetype('/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf', 30)
    ims = []
    for label, im in items:
        r = width / im.width
        if r < 1: im = im.resize((width, int(im.height * r)), Image.LANCZOS)
        ims.append((label, im))
    H = sum(im.height + 50 for _, im in ims) + 10
    cv = Image.new('RGB', (width, H), 'white'); d = ImageDraw.Draw(cv); y = 5
    for label, im in ims:
        d.text((10, y + 5), label, font=F, fill=(20, 20, 20)); y += 50
        cv.paste(im, (0, y)); y += im.height
    cv.save(FIG / out)


if __name__ == '__main__':
    for src, dst in [('01_jps_daemons', 'term_jps.png'), ('50_wordcount_cat_head', 'term_wordcount.png'),
                     ('16_mr_yearly_result', 'term_mr_yearly.png'), ('19_hive_create_table', 'term_hive_describe.png'),
                     ('20_hive_select', 'term_hive_select.png')]:
        shutil.copy(SH / f'{src}.png', FIG / dst)
    stack(['48_hdfs_check_ls_R', '49_hdfs_check_du_cat'], 'term_hdfs_check.png')
    stack(['51_hive_load_data', '52_hdfs_after_load'], 'term_hive_load.png')
    # NameNode: Summary + Browse Directory (таблицю Datanodes прибрано: у 16 см її шрифт < 8 pt)
    panel([('(а) NameNode: Summary', Image.open(SRC / 'nn_summary.png').convert('RGB')),
           ('(б) NameNode: Browse Directory /user/root/btc/raw', Image.open(SRC / 'nn_browse_raw.png').convert('RGB'))],
          'ui_hdfs_panel.png')
    # YARN Applications: стовпці Name … FinalStatus (стовпець ID, x < 366 px, обрізано)
    ya = Image.open(SRC / 'yarn_apps.png').convert('RGB')
    ya.crop((366, 0, ya.width, ya.height)).save(FIG / 'ui_yarn_apps.png')
    jc = Image.open(SRC / 'jh_counters.png').convert('RGB')   # без порожнього рядка File System Counters (y < 119 px)
    jc.crop((0, 119, jc.width, jc.height)).save(FIG / 'ui_jh_counters.png')
    shutil.copy(BENCH_FIG, FIG / 'fig_bench.png')
    print('ok')
