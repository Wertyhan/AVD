# -*- coding: utf-8 -*-
"""Генерує реальні тексти виводу mongosh/python для скріншотів (screenshots/txt/*.txt).
Кожен файл: перший рядок "$ команда", далі фактичний вивід команди. Потім make_screens.py рендерить PNG."""
import subprocess, os, sys
BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
OUT = f"{BASE}/screenshots/txt"; os.makedirs(OUT, exist_ok=True)
MSH = "/home/user/AVD/work/tools/msh/node_modules/.bin/mongosh"
DB = "bigdata_db"


import shlex
ONLY = set(sys.argv[1:])   # якщо задано імена (s03 s04 ...), перегенеруються лише вони
os.environ["PATH"] = os.path.dirname(MSH) + ":" + os.environ["PATH"]


def trim(out, n=40):
    L = out.rstrip().split("\n")
    return out.rstrip() if len(L) <= n else "\n".join(L[:n - 1] + [f"… (ще {len(L) - n + 1} рядків)"])


def run(name, cmd, note=None):
    """Виконує РЕАЛЬНУ команду через bash (cwd = pz4/); заголовок "$" == ця сама команда, тіло == її вивід."""
    if ONLY and name.split("_")[0] not in ONLY: return
    r = subprocess.run(["bash", "-c", cmd], capture_output=True, text=True, timeout=900, cwd=BASE)
    out = trim(r.stdout + r.stderr)
    head = f"$ {cmd}" + (f"\n# {note}" if note else "")
    open(f"{OUT}/{name}.txt", "w", encoding="utf-8").write(f"{head}\n{out}\n")
    print("ok", name)


def sh(name, js, shown=None, db=DB):
    """mongosh --quiet --eval '<js>' db; заголовок == виконана команда (shown ігнорується)."""
    js = js.replace("'", '"')   # подвійні лапки в JS, щоб команда в оболонці була читабельною (без екранування '"'"')
    run(name, f"mongosh --quiet --eval {shlex.quote(js)} {db}")


def shf(name, jsfile, note=None, db=DB):
    """mongosh --quiet db --file code/<jsfile>.js"""
    run(name, f"mongosh --quiet {db} --file code/{jsfile}", note)




# --- 2. База і колекції ---
sh("s01_show_dbs", "show dbs", db="admin")
sh("s02_collections", "db.getCollectionNames().filter(n => !n.startsWith('system.'))")
sh("s03_counts", "db.getCollectionNames().filter(n=>!n.startsWith('system.')).forEach(n => print(n.padEnd(22), db[n].countDocuments({})))")
# --- 3. Структура, типи, пропуски ---
sh("s04_sample_1m", "printjson(db.candles_1m.findOne())")
sh("s05_sample_spark", "printjson(db.spark_daily_features.findOne({is_complete:1},{_id:0,date:1,close:1,volume:1,rv:1,hl_rel:1,ret_1d:1,year:1,dow:1}))")
sh("s06_types", "printjson(db.candles_1m.aggregate([{$limit:1},{$project:{_id:0,ts:{$type:'$ts'},unix:{$type:'$unix'},open:{$type:'$open'},volume:{$type:'$volume'}}}]).toArray()[0]); printjson(db.candles_1d.aggregate([{$limit:1},{$project:{_id:0,date:{$type:'$date'},year:{$type:'$year'},n_minutes:{$type:'$n_minutes'}}}]).toArray()[0])")
sh("s07_missing", "print('1m null/missing close:', db.candles_1m.countDocuments({$or:[{close:null},{close:{$exists:false}}]})); print('1m volume==0:', db.candles_1m.countDocuments({volume:0})); print('spark ret_1d null:', db.spark_daily_features.countDocuments({ret_1d:null})); print('spark vol_30d null:', db.spark_daily_features.countDocuments({vol_30d:null}))")
# --- 4. Запити ---
sh("s08_q1", "db.candles_1d.find({date:{$gte:ISODate('2021-01-01'),$lt:ISODate('2022-01-01')},close:{$gt:60000}},{_id:0,date:1,close:1,volume:1}).sort({date:1}).limit(8)",
   "db.candles_1d.find({date:{$gte:ISODate('2021-01-01'),$lt:ISODate('2022-01-01')},close:{$gt:60000}},{_id:0,date:1,close:1,volume:1}).sort({date:1}).limit(8)")
sh("s09_q2", "db.candles_1d.find({},{_id:0,date:1,volume:1,close:1}).sort({volume:-1}).limit(10)")
sh("s10_q3", "db.candles_1m.find({ts:{$gte:ISODate('2020-03-12T12:00:00Z'),$lt:ISODate('2020-03-12T13:00:00Z')}},{_id:0,ts:1,low:1,close:1,volume:1}).sort({low:1}).limit(5)")
sh("s11_q5", "db.spark_daily_features.find({is_complete:1,ret_1d:{$lt:-0.10}},{_id:0,date:1,close:1,ret_1d:1,rv:1}).sort({ret_1d:1}).limit(8)")
# --- 5. Агрегація ---
sh("s12_agg_ohlc", "db.candles_1m.aggregate([{$sort:{ts:1}},{$group:{_id:{y:{$year:'$ts'},m:{$month:'$ts'}},open:{$first:'$open'},high:{$max:'$high'},low:{$min:'$low'},close:{$last:'$close'},volume:{$sum:'$volume'}}},{$sort:{_id:1}},{$match:{'_id.y':2021,'_id.m':{$lte:4}}}],{allowDiskUse:true})",
   "db.candles_1m.aggregate([{$sort:{ts:1}},{$group:{_id:{y:{$year:'$ts'},m:{$month:'$ts'}},open:{$first:'$open'},high:{$max:'$high'},low:{$min:'$low'},close:{$last:'$close'},volume:{$sum:'$volume'}}},{$sort:{_id:1}},{$match:{'_id.y':2021,'_id.m':{\\$lte:4}}}],{allowDiskUse:true})")
sh("s13_agg_hour", "db.candles_1m.aggregate([{$match:{ts:{$gte:ISODate('2018-01-01')}}},{$group:{_id:{$hour:'$ts'},total:{$sum:'$volume'},avg_min:{$avg:'$volume'}}},{$sort:{_id:1}},{$limit:6}])")
sh("s14_agg_vol", "db.spark_daily_features.aggregate([{$match:{is_complete:1}},{$group:{_id:'$year',days:{$sum:1},avg_rv:{$avg:'$rv'},avg_hl:{$avg:'$hl_rel'}}},{$sort:{_id:1}},{$match:{_id:{$gte:2017}}}])")
sh("s15_agg_bucket", "db.candles_1d.aggregate([{$bucket:{groupBy:'$close',boundaries:[0,10,100,1000,10000,50000,100000,1000000],default:'other',output:{days:{$sum:1},avg_volume:{$avg:'$volume'}}}}])")
sh("s16_agg_lookup", "db.anomalies.aggregate([{$group:{_id:'$date',n_anom:{$sum:1},max_score:{$max:'$score'}}},{$lookup:{from:'spark_daily_features',localField:'_id',foreignField:'date',as:'f'}},{$unwind:'$f'},{$project:{_id:0,date:'$_id',n_anom:1,max_score:1,rv:'$f.rv',volume:'$f.volume'}},{$sort:{n_anom:-1}},{$limit:6}])")
# --- 6. Індекси ---
for _c in ["candles_1m", "candles_1h", "candles_1d", "candles_1d_buckets", "spark_daily_features", "anomalies"]:
    pass
sh("s17_getindexes", "for (const c of ['candles_1m','candles_1h','candles_1d','candles_1d_buckets','spark_daily_features','anomalies']) { print('// ' + c); console.log(db[c].getIndexes()) }")
EXPL = lambda coll, flt, extra: (
    "const f=(e)=>{const s=e.executionStats;let p=e.queryPlanner.winningPlan,st=[];while(p){st.push(p.stage);p=p.inputStage}; return {stage:st.reverse().join(' <- '),executionTimeMillis:s.executionTimeMillis,totalDocsExamined:s.totalDocsExamined,totalKeysExamined:s.totalKeysExamined,nReturned:s.nReturned}};"
    f"const q=()=>db.{coll}.find({flt}){extra};"
    "print('BEFORE (hint $natural):'); printjson(f(q().hint({$natural:1}).explain('executionStats')));"
    "print('AFTER (index):'); printjson(f(q().explain('executionStats')))")
shf("s18_explain_ts", "s18_explain_ts.js")
shf("s19_explain_vol", "s19_explain_vol.js")
shf("s20_explain_anom", "s20_explain_anom.js", "anomalies = v1 (залежить від перезапуску ПЗ3)")
sh("s21_ts_stats", "const s=db.candles_1m_ts.stats(); printjson({ns:s.ns,timeseries:{bucketCount:s.timeseries.bucketCount,avgBucketSize:s.timeseries.avgBucketSize},size:s.size,storageSize:s.storageSize}); const f=db.candles_1m.stats(); printjson({ns:f.ns,count:f.count,size:f.size,storageSize:f.storageSize,avgObjSize:f.avgObjSize})")
# --- shell-виводи ---
run("s22_load_log", "grep -E '^\\{|^DONE|7200000| 0s$' results/load.log | tail -c 4000",
    "журнал завантаження results/load.log (відфільтровані ключові рядки)")
run("s23_index_table", "python3 -c \"import pandas as pd; pd.set_option('display.width',200); t=pd.read_csv('results/pz4_index_comparison.csv'); print(t[['experiment','phase','executionTimeMillis_explain_median','totalDocsExamined','totalKeysExamined','nReturned','winning_plan']].to_string(index=False))\"")
