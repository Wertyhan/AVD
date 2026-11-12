# -*- coding: utf-8 -*-
"""ПЗ4, запит Q7: explain("executionStats") для count_documents({"volume": 0}) у candles_1m.
count_documents у PyMongo виконується як aggregate [{$match}, {$group: {_id: 1, n: {$sum: 1}}}], тому explain знімається саме для цього конвеєра.
Результат -> results/explain/Q7_zero_volume_count_explain.json."""
import json
from bson import json_util
from pz4_common import get_db, EXPL

db = get_db()
cmd = {"aggregate": "candles_1m", "pipeline": [{"$match": {"volume": 0}}, {"$group": {"_id": 1, "n": {"$sum": 1}}}], "cursor": {}}
ex = db.command({"explain": cmd, "verbosity": "executionStats"})
with open(f"{EXPL}/Q7_zero_volume_count_explain.json", "w", encoding="utf-8") as f:
    json.dump(json.loads(json_util.dumps(ex, json_options=json_util.RELAXED_JSON_OPTIONS)), f, ensure_ascii=False, indent=1)
st = ex["executionStats"]
def find_stage(p, name):
    if isinstance(p, dict):
        if p.get("stage") == name: return True
        return any(find_stage(v, name) for v in p.values())
    return False
print("COUNT_SCAN:", find_stage(ex["queryPlanner"]["winningPlan"], "COUNT_SCAN"),
      "keys:", st["totalKeysExamined"], "docs:", st["totalDocsExamined"], "ms:", st["executionTimeMillis"])
