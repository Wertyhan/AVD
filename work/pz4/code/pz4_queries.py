# -*- coding: utf-8 -*-
"""Завдання 4: запити find (пошук, фільтрація, сортування, limit, projection). Результати -> results/pz4_q*.csv"""
import sys, time, datetime as dt
sys.path.insert(0, __import__("os").path.dirname(__file__))
from pz4_common import *

db = get_db()
U = lambda *a: dt.datetime(*a, tzinfo=dt.timezone.utc)
summary = []


def run(qid, descr, coll, flt, proj=None, sort=None, limit=0, count_only=False):
    t = time.perf_counter()
    if count_only:
        res = [{"count": db[coll].count_documents(flt)}]
    else:
        res = list(db[coll].find(flt, proj, sort=sort, limit=limit))
    dtm = (time.perf_counter() - t) * 1000
    rows = [{k: (v.strftime("%Y-%m-%d %H:%M") if isinstance(v, dt.datetime) else v) for k, v in r.items() if k != "_id"} for r in res]
    save_csv(f"pz4_{qid}", rows)
    summary.append(dict(query=qid, description=descr, collection=coll, returned=len(res), wall_ms=round(dtm, 1)))
    print(qid, descr, len(res), f"{dtm:.0f} мс"); print(rows[:3])


run("q1_close_gt_60000_2021", "дні 2021 із close>60000 (сортування за датою)", "candles_1d",
    {"date": {"$gte": U(2021, 1, 1), "$lt": U(2022, 1, 1)}, "close": {"$gt": 60000}}, {"_id": 0, "date": 1, "close": 1, "volume": 1}, [("date", 1)])
run("q2_top10_volume_days", "топ-10 діб за обсягом", "candles_1d", {}, {"_id": 0, "date": 1, "volume": 1, "close": 1}, [("volume", -1)], 10)
run("q3_covid_hour", "12.03.2020 12:00-13:00 UTC, 5 найнижчих low", "candles_1m",
    {"ts": {"$gte": U(2020, 3, 12, 12), "$lt": U(2020, 3, 12, 13)}}, {"_id": 0, "ts": 1, "low": 1, "close": 1, "volume": 1}, [("low", 1)], 5)
run("q4_top5_volume_minutes", "5 хвилин з найбільшим обсягом", "candles_1m", {"volume": {"$gt": 0}},
    {"_id": 0, "ts": 1, "close": 1, "volume": 1}, [("volume", -1)], 5)
run("q5_worst_days_spark", "повні доби з ret_1d<-0,10 (результати Spark)", "spark_daily_features",
    {"is_complete": 1, "ret_1d": {"$lt": -0.10}}, {"_id": 0, "date": 1, "close": 1, "ret_1d": 1, "rv": 1}, [("ret_1d", 1)], 10)
run("q6_incomplete_days", "неповні доби (n_minutes<1440)", "candles_1d", {"n_minutes": {"$lt": 1440}},
    {"_id": 0, "date": 1, "n_minutes": 1})
run("q7_zero_volume_count", "кількість хвилин з нульовим обсягом", "candles_1m", {"volume": 0}, count_only=True)
run("q8_in_or", "доби листопада-грудня 2022 з flat_share=0 ($in + $and)", "spark_daily_features",
    {"$and": [{"year": 2022}, {"month": {"$in": [11, 12]}}, {"flat_share": 0}]}, {"_id": 0, "date": 1, "flat_share": 1, "volume": 1}, [("date", 1)], 5)
save_csv("pz4_queries_summary", summary)
