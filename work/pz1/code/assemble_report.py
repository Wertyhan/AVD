# -*- coding: utf-8 -*-
"""assemble_report.py – збирає report.md: текст звіту (report_body.md) + додаток А з лістингами файлів коду."""
import pathlib
S = pathlib.Path(__file__).resolve().parent.parent          # каталог pz1
C = S / 'code'
B = S.parent / 'bench' / 'code'                             # код порівняльного вимірювання часу
items = [(C, 'mapper_year.py', 'python', 'mapper річної задачі Hadoop Streaming'),
         (C, 'reducer_year.py', 'python', 'reducer і combiner річної задачі'),
         (C, 'mapper_ohlc.py', 'python', 'mapper добових OHLC з in-mapper combining'),
         (C, 'reducer_ohlc.py', 'python', 'reducer добових OHLC'),
         (C, 'hive_01_create_table.hql', 'sql', 'зовнішня таблиця candles і подання candles_v'),
         (C, 'hive_01b_describe.hql', 'sql', 'опис таблиці (DESCRIBE FORMATTED)'),
         (C, 'hive_02_select.hql', 'sql', 'вибірка та підрахунок рядків'),
         (C, 'hive_03a_yearly.hql', 'sql', 'групування за роками'),
         (C, 'hive_03b_monthly.hql', 'sql', 'групування за роком і місяцем'),
         (C, 'hive_04_daily_struct.hql', 'sql', 'добова таблиця OHLC (CTAS)'),
         (C, 'hive_05a_top_volume.hql', 'sql', 'топ-10 діб за обсягом'),
         (C, 'hive_05b_top_range.hql', 'sql', 'топ-10 діб за амплітудою High − Low'),
         (C, 'hive_06_daily_count.hql', 'sql', 'контроль добової таблиці'),
         (C, 'hive_07_load_data.hql', 'sql', 'керована таблиця і LOAD DATA INPATH'),
         (C, 'run_pz1.py', 'python', 'основний конвеєр: налаштування Hadoop і Hive, HDFS, MapReduce, Hive, Pandas, звірки'),
         (C, 'pandas_yearly.py', 'python', 'річні підсумки в Pandas'),
         (B, 'bench_all.py', 'python', 'порівняльне вимірювання часу Hadoop Streaming, Hive, Pandas і PySpark'),
         (B, 'bench_yearly.hql', 'sql', 'Hive-запит Т1 для вимірювання часу'),
         (B, 'bench_daily.hql', 'sql', 'Hive-запит Т2 для вимірювання часу')]
out = [(S / 'report_body.md').read_text().rstrip('\n'), '', '# Додаток А. Лістинг програми', '',
       'Допоміжні скрипти (рендер термінальних скріншотів, захоплення веб-інтерфейсів, побудова рисунків) зберігаються в репозиторії роботи і до лістингу не включені. Після виправлення DDL кроки Hive (А.5–А.14) повторно виконано окремим скриптом командою `hive -S -f`; він лише запускає ці файли і до лістингу не включений.', '']
for i, (d, f, lang, desc) in enumerate(items, 1):
    out += [f'## А.{i} Файл {f} – {desc}', '', f'```{lang}', (d / f).read_text().rstrip('\n'), '```', '']
(S / 'report.md').write_text('\n'.join(out))
