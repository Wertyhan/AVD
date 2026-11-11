# -*- coding: utf-8 -*-
"""A1 (місячний OHLC по всіх 7,8 млн хвилин) з індексом ts_1 та без нього ($natural): 3 запуски кожного, медіана; explain обох;
usedDisk/spills зі стадії SORT; контроль: місячний OHLC з candles_1d (медіана з 5)."""
import os, sys, time, json, statistics
from bson import json_util
sys.path.insert(0, os.path.dirname(__file__))
from pz4_common import *
db = get_db()
a1 = [{"$sort": {"ts": 1}},
      {"$group": {"_id": {"y": {"$year": "$ts"}, "m": {"$month": "$ts"}}, "open": {"$first": "$open"}, "high": {"$max": "$high"},
                  "low": {"$min": "$low"}, "close": {"$last": "$close"}, "volume": {"$sum": "$volume"}, "n": {"$sum": 1}}},
      {"$sort": {"_id": 1}}]
assert "ts_1" in db.candles_1m.index_information()
def walk(o, key, out):
    if isinstance(o, dict):
        for k, v in o.items():
            if k == key: out.append(v)
            walk(v, key, out)
    elif isinstance(o, list):
        for v in o: walk(v, key, out)
    return out
rows = []
for label, hint in (("з індексом ts_1", None), ("без індексу ($natural)", {"$natural": 1})):
    kw = {"hint": hint} if hint else {}
    list(db.candles_1m.aggregate(a1[:1] + [{"$limit": 1000}], allowDiskUse=True, **kw))
    runs, la = [], []
    for _ in range(3):
        la.append(os.getloadavg()[0]); t = time.perf_counter(); r = list(db.candles_1m.aggregate(a1, allowDiskUse=True, **kw)); runs.append(time.perf_counter() - t)
        print(label, round(runs[-1], 1), flush=True)
    ex = db.command({"explain": {"aggregate": "candles_1m", "pipeline": a1, "cursor": {}, "allowDiskUse": True, **kw}, "verbosity": "executionStats"})
    tag = "A1_monthly_ohlc_explain" if not hint else "A1_monthly_ohlc_natural_explain"
    json.dump(json.loads(json_util.dumps(ex)), open(f"{EXPL}/{tag}.json", "w"), indent=1)
    j = json_util.dumps(ex)
    plan = ex.get("stages") or []
    docs_ex = max(walk(json.loads(j), "totalDocsExamined", [0]))
    used = walk(json.loads(j), "usedDisk", []); spills = walk(json.loads(j), "spills", [])
    rows.append(dict(variant=label, groups=len(r), seconds_median=round(statistics.median(runs), 2), seconds_runs=str([round(x, 2) for x in runs]),
                     totalDocsExamined=docs_ex, usedDisk_any=any(used), usedDisk_values=str(used), spills_values=str(spills),
                     winning_plan=str(ex.get("queryPlanner", {}).get("winningPlan", {}).get("queryPlan", ex.get("stages", [{}])[0].get("$cursor", {}).get("queryPlanner", {}).get("winningPlan", ""))).replace("\n", " ")[:300],
                     loadavg_1m=f"{min(la):.2f}-{max(la):.2f}", explain_file=f"explain/{tag}.json"))
    print(rows[-1], flush=True)
# контроль: місячний OHLC з добових свічок
pipe = [{"$sort": {"date": 1}}, {"$group": {"_id": {"y": "$year", "m": "$month"}, "open": {"$first": "$open"}, "high": {"$max": "$high"},
        "low": {"$min": "$low"}, "close": {"$last": "$close"}, "volume": {"$sum": "$volume"}}}, {"$sort": {"_id": 1}}]
list(db.candles_1d.aggregate(pipe)); ts_, la = [], []
for _ in range(5):
    la.append(os.getloadavg()[0]); t = time.perf_counter(); r = list(db.candles_1d.aggregate(pipe)); ts_.append((time.perf_counter() - t) * 1000)
rows.append(dict(variant="місячний OHLC з candles_1d (5 429 док.), мс", groups=len(r), seconds_median=round(statistics.median(ts_) / 1000, 4),
                 seconds_runs=str([round(x / 1000, 4) for x in ts_]), totalDocsExamined=5429, usedDisk_any="", usedDisk_values="", spills_values="",
                 winning_plan="", loadavg_1m=f"{min(la):.2f}-{max(la):.2f}", explain_file=""))
save_csv("pz4_agg_A1_timing", rows)
