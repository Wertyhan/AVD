# -*- coding: utf-8 -*-
"""Завдання 4: запити find (пошук, фільтрація, сортування, limit, projection). Результати -> results/pz4_q*.csv"""
import sys, time, datetime as dt
sys.path.insert(0, __import__("os").path.dirname(__file__))
from pz4_common import *

# підключення до бази bigdata_db (параметри в pz4_common.py)
db = get_db()
# допоміжна функція: UTC-дата для фільтрів по полях типу BSON Date
U = lambda *a: dt.datetime(*a, tzinfo=dt.timezone.utc)
# зведення за всіма запитами (id, опис, колекція, кількість рядків, час) -> pz4_queries_summary.csv
summary = []


def run(qid, descr, coll, flt, proj=None, sort=None, limit=0, count_only=False):
    # run(): виконує один find-запит (або count) і вимірює повний wall-clock час разом із передачею результату
    t = time.perf_counter()
    if count_only:
        # лише кількість документів за фільтром (count_documents), без повернення самих документів
        res = [{"count": db[coll].count_documents(flt)}]
    else:
        # find(фільтр, проєкція, сортування, ліміт): проєкція обмежує поля, sort і limit виконує сервер
        res = list(db[coll].find(flt, proj, sort=sort, limit=limit))
    # час у мілісекундах
    dtm = (time.perf_counter() - t) * 1000
    # приводимо дати до рядка, прибираємо службове поле _id, щоб результат був придатний для CSV
    rows = [{k: (v.strftime("%Y-%m-%d %H:%M") if isinstance(v, dt.datetime) else v) for k, v in r.items() if k != "_id"} for r in res]
    save_csv(f"pz4_{qid}", rows)  # повний результат запиту -> results/pz4_<qid>.csv
    # запис у зведену таблицю
    summary.append(dict(query=qid, description=descr, collection=coll, returned=len(res), wall_ms=round(dtm, 1)))
    # короткий вивід у консоль: id, опис, кількість рядків, час і перші 3 рядки
    print(qid, descr, len(res), f"{dtm:.0f} мс"); print(rows[:3])


# Q1: фільтр за діапазоном дат ($gte/$lt) + умова close>60000 ($gt), проєкція трьох полів, сортування за датою за зростанням;
# запит по індексу date_1 колекції добових свічок
run("q1_close_gt_60000_2021", "дні 2021 із close>60000 (сортування за датою)", "candles_1d",
    {"date": {"$gte": U(2021, 1, 1), "$lt": U(2022, 1, 1)}, "close": {"$gt": 60000}}, {"_id": 0, "date": 1, "close": 1, "volume": 1}, [("date", 1)])
# Q2: сортування за спаданням обсягу + limit 10 -> топ-10 діб (використовує індекс year_1_volume_-1 / сортування за volume)
run("q2_top10_volume_days", "топ-10 діб за обсягом", "candles_1d", {}, {"_id": 0, "date": 1, "volume": 1, "close": 1}, [("volume", -1)], 10)
# Q3: вузький діапазон ts (одна година 12.03.2020) у хвилинній колекції (індекс ts_1), сортування за low зростаючи, limit 5
# -> 5 найнижчих мінімумів під час обвалу «чорного четверга»
run("q3_covid_hour", "12.03.2020 12:00-13:00 UTC, 5 найнижчих low", "candles_1m",
    {"ts": {"$gte": U(2020, 3, 12, 12), "$lt": U(2020, 3, 12, 13)}}, {"_id": 0, "ts": 1, "low": 1, "close": 1, "volume": 1}, [("low", 1)], 5)
# Q4: відсікаємо нульовий обсяг ($gt:0), сортуємо за volume спаданням, limit 5 -> хвилини з рекордним обсягом (індекс volume_-1)
run("q4_top5_volume_minutes", "5 хвилин з найбільшим обсягом", "candles_1m", {"volume": {"$gt": 0}},
    {"_id": 0, "ts": 1, "close": 1, "volume": 1}, [("volume", -1)], 5)
# Q5: запит до колекції з результатами Spark (ПЗ2): повні доби (is_complete=1) з падінням ret_1d<-10 %, від найгіршої; limit 10
run("q5_worst_days_spark", "повні доби з ret_1d<-0,10 (результати Spark)", "spark_daily_features",
    {"is_complete": 1, "ret_1d": {"$lt": -0.10}}, {"_id": 0, "date": 1, "close": 1, "ret_1d": 1, "rv": 1}, [("ret_1d", 1)], 10)
# Q6: пошук неповних діб (менше 1440 хвилин) із проєкцією лише дати та кількості хвилин; без sort і limit
run("q6_incomplete_days", "неповні доби (n_minutes<1440)", "candles_1d", {"n_minutes": {"$lt": 1440}},
    {"_id": 0, "date": 1, "n_minutes": 1})
# Q7: count_documents для хвилин з нульовим обсягом (повертається одне число, а не документи)
run("q7_zero_volume_count", "кількість хвилин з нульовим обсягом", "candles_1m", {"volume": 0}, count_only=True)
# Q8: комбінований логічний фільтр: $and із рівністю (year), $in (month 11 або 12) та flat_share=0; сортування за датою, limit 5
run("q8_in_or", "доби листопада-грудня 2022 з flat_share=0 ($in + $and)", "spark_daily_features",
    {"$and": [{"year": 2022}, {"month": {"$in": [11, 12]}}, {"flat_share": 0}]}, {"_id": 0, "date": 1, "flat_share": 1, "volume": 1}, [("date", 1)], 5)
# зберігаємо зведену таблицю часу виконання всіх запитів
save_csv("pz4_queries_summary", summary)
