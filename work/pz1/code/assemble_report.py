import pathlib
C=pathlib.Path('/home/user/AVD/work/pz1/code')
S=pathlib.Path('/home/user/AVD/work/pz1')
items=[('mapper_year.py','python','mapper річної задачі Hadoop Streaming'),
('reducer_year.py','python','reducer і combiner річної задачі'),
('mapper_ohlc.py','python','mapper добових OHLC з in-mapper combining'),
('reducer_ohlc.py','python','reducer добових OHLC'),
('hive_01_create_table.hql','sql','зовнішня таблиця candles і подання candles_v'),
('hive_02_select.hql','sql','вибірка та підрахунок рядків'),
('hive_03a_yearly.hql','sql','групування за роками'),
('hive_03b_monthly.hql','sql','групування за роком і місяцем'),
('hive_04_daily_struct.hql','sql','добова таблиця OHLC (CTAS)'),
('hive_05a_top_volume.hql','sql','топ-10 діб за обсягом'),
('hive_05b_top_range.hql','sql','топ-10 діб за амплітудою High − Low'),
('hive_06_daily_count.hql','sql','контроль добової таблиці'),
('run_pz1.py','python','основний конвеєр: налаштування Hadoop і Hive, HDFS, MapReduce, Hive, Pandas, звірки'),
('bench_pz1.py','python','вимірювання часу Hadoop Streaming, Hive, Pandas і PySpark'),
('make_figures.py','python','побудова рисунків'),
('termshot.py','python','рендер виводу команд у зображення терміналу'),
('ui_shots_pz1.py','python','скріншоти веб-інтерфейсів NameNode, YARN і JobHistory')]
out=[(S/'report_body.md').read_text().rstrip('\n'),'','# Додаток А. Лістинг програми','']
for i,(f,lang,d) in enumerate(items,1):
    out+= [f'## А.{i} Файл {f} – {d}','',f'```{lang}',(C/f).read_text().rstrip('\n'),'```','']
pathlib.Path('/home/user/AVD/work/pz1/report.md').write_text('\n'.join(out))
