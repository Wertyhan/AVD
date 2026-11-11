import os
B="/home/user/AVD/work/pz4"
files=[("pz4_common.py","спільні утиліти: підключення, запис результатів, вимірювання до/після індексу"),
("pz4_load.py","створення колекцій та імпорт даних"),
("pz4_schema.py","кількість документів, BSON-типи, пропуски, розміри колекцій"),
("pz4_queries.py","запити find"),
("pz4_aggs.py","конвеєри агрегації"),
("pz4_indexes.py","індекси та порівняння до/після"),
("pz4_anomalies.py","імпорт аномалій ПЗ3, індекс і $lookup з ознаками Spark"),
("s18_explain_ts.js","план запиту E1 у mongosh"),
("s19_explain_vol.js","план запиту E3 у mongosh"),
("s20_explain_anom.js","план запиту ANOM1 у mongosh"),
("pz4_figures.py","побудова рисунків"),
("pz4_screens_txt.py","отримання реального виводу команд для скриншотів"),
("make_screens.py","рендер виводу терміналу в PNG")]
out=open(f"{B}/report_body.md").read().rstrip()+"\n\n# Додаток А. Лістинг програми\n\n"
for i,(f,d) in enumerate(files,1):
    code=open(f"{B}/code/{f}").read().rstrip("\n")
    lang="javascript" if f.endswith(".js") else "python"
    out+=f"## А.{i} Файл {f} – {d}\n\n```{lang}\n{code}\n```\n\n"
open(f"{B}/report.md","w").write(out)
