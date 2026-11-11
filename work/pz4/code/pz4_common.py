# -*- coding: utf-8 -*-
"""Спільні утиліти ПЗ4: підключення до MongoDB, шляхи, запис результатів."""
import os, csv, json, time, statistics, datetime
from bson import json_util
from pymongo import MongoClient
from bson.codec_options import CodecOptions

BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
W = os.path.abspath(os.path.join(BASE, ".."))
RES, FIG, EXPL = f"{BASE}/results", f"{BASE}/figures", f"{BASE}/results/explain"
for d in (RES, FIG, EXPL):
    os.makedirs(d, exist_ok=True)
DB_NAME = "bigdata_db"
URI = "mongodb://127.0.0.1:27017"


def get_db(tz_aware=True):
    """Повертає базу bigdata_db; дати читаються як UTC-aware."""
    cl = MongoClient(URI)
    return cl.get_database(DB_NAME, codec_options=CodecOptions(tz_aware=tz_aware, tzinfo=datetime.timezone.utc))


def save_csv(name, rows, header=None):
    """Зберігає список словників/кортежів у results/<name>.csv."""
    path = f"{RES}/{name}.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        if rows and isinstance(rows[0], dict):
            header = header or list(rows[0].keys())
            w = csv.DictWriter(f, fieldnames=header); w.writeheader(); w.writerows(rows)
        else:
            w = csv.writer(f)
            if header: w.writerow(header)
            w.writerows(rows)
    return path


def append_timing(stage, seconds, note=""):
    """Дописує вимір часу до results/timings.csv."""
    path = f"{RES}/timings.csv"
    new = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new: w.writerow(["stage", "seconds", "note"])
        w.writerow([stage, f"{seconds:.3f}", note])


def explain_find(db, coll, flt, sort=None, limit=0, proj=None, hint=None):
    """explain('executionStats') для find."""
    cmd = {"find": coll, "filter": flt}
    if sort: cmd["sort"] = dict(sort)
    if limit: cmd["limit"] = limit
    if proj: cmd["projection"] = proj
    if hint is not None: cmd["hint"] = hint
    return db.command({"explain": cmd, "verbosity": "executionStats"})


def stages(plan, out=None):
    """Збирає назви стадій виграшного плану (від кореня до листка)."""
    out = [] if out is None else out
    out.append(plan["stage"])
    if "inputStage" in plan: stages(plan["inputStage"], out)
    for s in plan.get("inputStages", []): stages(s, out)
    return out


def summarize(ex):
    """Витягує ключові метрики з виводу explain."""
    es = ex["executionStats"]
    return dict(executionTimeMillis=es["executionTimeMillis"], totalDocsExamined=es["totalDocsExamined"],
                totalKeysExamined=es["totalKeysExamined"], nReturned=es["nReturned"],
                winning_plan=" > ".join(stages(ex["queryPlanner"]["winningPlan"])))


# ---------- порівняння до/після індексу (спільне для pz4_indexes.py та pz4_anomalies.py) ----------
REPS = 5
_db = get_db()


def drop_secondary(coll):
    """Видаляє всі індекси колекції, крім _id_."""
    for ix in _db[coll].index_information():
        if ix != "_id_": _db[coll].drop_index(ix)


def measure(tag, coll, flt, sort, limit, hint=None):
    """warm-up + 5 вимірювань: executionTimeMillis (з explain) та wall-clock фактичного виконання."""
    run = lambda: list(_db[coll].find(flt, sort=sort, limit=limit))
    load_start = os.getloadavg()[0]
    explain_find(_db, coll, flt, sort, limit)                 # прогрів кешу
    ex_t, wall = [], []
    for _ in range(REPS):
        ex = explain_find(_db, coll, flt, sort, limit)
        ex_t.append(ex["executionStats"]["executionTimeMillis"])
        t = time.perf_counter(); run(); wall.append((time.perf_counter() - t) * 1000)
    s = summarize(ex)
    json.dump(json.loads(json_util.dumps(ex)), open(f"{EXPL}/{tag}.json", "w"), indent=1)
    s.update(loadavg_1m=f"{load_start:.2f}->{os.getloadavg()[0]:.2f}", explain_ms_runs=ex_t, wall_ms_runs=[round(x, 1) for x in wall],
             explain_ms_median=statistics.median(ex_t), wall_ms_median=round(statistics.median(wall), 1))
    return s


def experiment(name, coll, flt, sort, limit, spec, descr):
    drop_secondary(coll)
    before = measure(f"{name}_before", coll, flt, sort, limit)
    t = time.perf_counter(); iname = _db[coll].create_index(spec); build = time.perf_counter() - t
    after = measure(f"{name}_after", coll, flt, sort, limit)
    st = _db.command("collStats", coll)
    isize = st["indexSizes"][iname] / 2**20   # МіБ
    out = []
    for phase, m in (("до", before), ("після", after)):
        out.append(dict(experiment=name, description=descr, collection=coll, phase=phase, index=iname if phase == "після" else "-",
                        executionTimeMillis_explain_median=m["explain_ms_median"], wall_ms_median=m["wall_ms_median"],
                        totalDocsExamined=m["totalDocsExamined"], totalKeysExamined=m["totalKeysExamined"],
                        nReturned=m["nReturned"], winning_plan=m["winning_plan"], loadavg_1m=m["loadavg_1m"],
                        explain_ms_runs=m["explain_ms_runs"], wall_ms_runs=m["wall_ms_runs"],
                        index_size_MiB=round(isize, 2) if phase == "після" else 0, index_build_s=round(build, 2) if phase == "після" else 0))
    for o in out: print(o["experiment"], o["phase"], o["executionTimeMillis_explain_median"], o["wall_ms_median"],
                        o["totalDocsExamined"], o["totalKeysExamined"], o["nReturned"], o["winning_plan"], flush=True)
    return out


