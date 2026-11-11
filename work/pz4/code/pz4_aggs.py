# -*- coding: utf-8 -*-
"""Завдання 5: конвеєри агрегації ($match/$sort/$group/$project/$bucket). Результати -> results/pz4_agg_*.csv"""
import sys, time, json
import pandas as pd
from bson import json_util
sys.path.insert(0, __import__("os").path.dirname(__file__))
from pz4_common import *

db = get_db()


def agg(name, coll, pipe, **kw):
    t = time.perf_counter()
    res = list(db[coll].aggregate(pipe, allowDiskUse=True, **kw))
    s = time.perf_counter() - t
    append_timing(f"agg_{name}", s, f"{coll}, {len(res)} рядків")
    print(name, len(res), f"{s:.1f} с", flush=True)
    return res, s


# A1. Місячні OHLC зі свічок 1 хв: $sort за ts + $first/$last
a1 = [{"$sort": {"ts": 1}},
      {"$group": {"_id": {"y": {"$year": "$ts"}, "m": {"$month": "$ts"}}, "open": {"$first": "$open"}, "high": {"$max": "$high"},
                  "low": {"$min": "$low"}, "close": {"$last": "$close"}, "volume": {"$sum": "$volume"}, "n": {"$sum": 1}}},
      {"$sort": {"_id": 1}},
      {"$project": {"_id": 0, "year": "$_id.y", "month": "$_id.m", "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1, "n": 1}}]
res, t_idx = agg("A1_monthly_ohlc_with_ts_index", "candles_1m", a1)
pd.DataFrame(res).to_csv(f"{RES}/pz4_agg_monthly_ohlc.csv", index=False)
ex = db.command({"explain": {"aggregate": "candles_1m", "pipeline": a1, "cursor": {}, "allowDiskUse": True}, "verbosity": "executionStats"})
json.dump(json.loads(json_util.dumps(ex)), open(f"{EXPL}/A1_monthly_ohlc_explain.json", "w"), indent=1)
# той самий конвеєр без індексу на ts (примусовий $natural): справедливе порівняння часу
res2, t_nat = agg("A1_monthly_ohlc_natural_hint", "candles_1m", a1, hint={"$natural": 1})
save_csv("pz4_agg_A1_timing", [dict(variant="з індексом ts_1 ($sort з індексу)", seconds=round(t_idx, 2)),
                               dict(variant="без індексу ($natural, зовнішнє сортування)", seconds=round(t_nat, 2))])

# перевірка коректності A1 проти pandas-ресемплінгу з btc_1d.parquet
d = pd.read_parquet(f"{W}/data/btc_1d.parquet").set_index("datetime_utc")
m = d.resample("MS").agg({"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"})
m["year"], m["month"] = m.index.year, m.index.month
a = pd.DataFrame(res).merge(m, on=["year", "month"])
chk = dict(months=len(a), max_abs_open=(a.open - a.Open).abs().max(), max_abs_close=(a.close - a.Close).abs().max(),
           max_abs_high=(a.high - a.High).abs().max(), max_abs_low=(a.low - a.Low).abs().max(),
           max_rel_volume=((a.volume - a.Volume).abs() / a.Volume.clip(lower=1e-9)).max())
save_csv("pz4_agg_A1_validation", [chk]); print(chk)

# A2. Обсяг за годиною доби (UTC), 2018+ (ринок після 2017 вже ліквідний)
a2 = [{"$match": {"ts": {"$gte": __import__("datetime").datetime(2018, 1, 1, tzinfo=__import__("datetime").timezone.utc)}}},
      {"$group": {"_id": {"$hour": "$ts"}, "total_volume": {"$sum": "$volume"}, "avg_volume_per_min": {"$avg": "$volume"}, "minutes": {"$sum": 1}}},
      {"$sort": {"_id": 1}}, {"$project": {"_id": 0, "hour": "$_id", "total_volume": 1, "avg_volume_per_min": 1, "minutes": 1}}]
res, _ = agg("A2_volume_by_hour", "candles_1m", a2)
pd.DataFrame(res).to_csv(f"{RES}/pz4_agg_volume_by_hour.csv", index=False)

# A3. Річна волатильність із результатів Spark (ПЗ2): лише повні доби
a3 = [{"$match": {"is_complete": 1}},
      {"$group": {"_id": "$year", "days": {"$sum": 1}, "avg_rv": {"$avg": "$rv"}, "avg_hl_rel": {"$avg": "$hl_rel"},
                  "min_ret_1d": {"$min": "$ret_1d"}, "max_ret_1d": {"$max": "$ret_1d"}, "total_volume": {"$sum": "$volume"},
                  "avg_flat_share": {"$avg": "$flat_share"}}},
      {"$sort": {"_id": 1}}, {"$project": {"_id": 0, "year": "$_id", "days": 1, "avg_rv": 1, "avg_hl_rel": 1, "min_ret_1d": 1,
                                           "max_ret_1d": 1, "total_volume": 1, "avg_flat_share": 1}}]
res, _ = agg("A3_yearly_volatility", "spark_daily_features", a3)
pd.DataFrame(res).to_csv(f"{RES}/pz4_agg_yearly_volatility.csv", index=False)

# A4. $bucket: розподіл діб за ціною закриття
a4 = [{"$bucket": {"groupBy": "$close", "boundaries": [0, 10, 100, 1000, 10000, 50000, 100000, 1000000], "default": "інше",
                   "output": {"days": {"$sum": 1}, "avg_volume": {"$avg": "$volume"}, "first_date": {"$min": "$date"}}}}]
res, _ = agg("A4_bucket_close", "candles_1d", a4)
rows = [dict(bucket_from=r["_id"], days=r["days"], avg_volume=r["avg_volume"], first_date=r["first_date"].strftime("%Y-%m-%d")) for r in res]
save_csv("pz4_agg_bucket_close", rows)

# A5. Річний підсумок з candles_1d (контроль A1): сума обсягу та діапазон цін за рік
a5 = [{"$group": {"_id": "$year", "volume": {"$sum": "$volume"}, "max_high": {"$max": "$high"}, "min_low": {"$min": "$low"}}},
      {"$sort": {"_id": 1}}]
res, _ = agg("A5_yearly_volume", "candles_1d", a5)
pd.DataFrame(res).rename(columns={"_id": "year"}).to_csv(f"{RES}/pz4_agg_yearly_volume.csv", index=False)
