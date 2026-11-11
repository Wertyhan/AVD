# -*- coding: utf-8 -*-
"""Генерує реальні тексти виводу mongosh/python для скріншотів (screenshots/txt/*.txt).
Кожен файл: перший рядок "$ команда", далі фактичний вивід команди. Потім make_screens.py рендерить PNG."""
import subprocess, os, sys
BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
OUT = f"{BASE}/screenshots/txt"; os.makedirs(OUT, exist_ok=True)
MSH = "/home/user/AVD/work/tools/msh/node_modules/.bin/mongosh"
DB = "bigdata_db"


def sh(name, js, shown=None, db=DB):
    """Виконує js у mongosh; у скріншоті показується команда (shown або js)."""
    r = subprocess.run([MSH, "--quiet", db, "--eval", js], capture_output=True, text=True, timeout=900)
    out = (r.stdout + r.stderr).rstrip()
    cmd = shown or js
    open(f"{OUT}/{name}.txt", "w", encoding="utf-8").write(f"$ mongosh {db} --quiet --eval '{cmd}'\n{out}\n")
    print("ok", name)


def raw(name, cmd, text):
    open(f"{OUT}/{name}.txt", "w", encoding="utf-8").write(f"$ {cmd}\n{text.rstrip()}\n")


# --- 2. База і колекції ---
sh("s01_show_dbs", "show dbs", db="admin")
sh("s02_collections", "db.getCollectionNames().filter(n => !n.startsWith('system.'))")
sh("s03_counts", "db.getCollectionNames().filter(n=>!n.startsWith('system.')).forEach(n => print(n.padEnd(22), db[n].countDocuments({})))",
   "db.getCollectionNames()...forEach(n => print(n, db[n].countDocuments({})))")
# --- 3. Структура, типи, пропуски ---
sh("s04_sample_1m", "printjson(db.candles_1m.findOne())", "db.candles_1m.findOne()")
sh("s05_sample_spark", "printjson(db.spark_daily_features.findOne({is_complete:1},{_id:0,date:1,close:1,volume:1,rv:1,hl_rel:1,ret_1d:1,year:1,dow:1}))",
   "db.spark_daily_features.findOne({is_complete:1},{_id:0,date:1,close:1,volume:1,rv:1,hl_rel:1,ret_1d:1,year:1,dow:1})")
sh("s06_types", "printjson(db.candles_1m.aggregate([{$limit:1},{$project:{_id:0,ts:{$type:'$ts'},unix:{$type:'$unix'},open:{$type:'$open'},volume:{$type:'$volume'}}}]).toArray()[0]); printjson(db.candles_1d.aggregate([{$limit:1},{$project:{_id:0,date:{$type:'$date'},year:{$type:'$year'},n_minutes:{$type:'$n_minutes'}}}]).toArray()[0])",
   "db.candles_1m.aggregate([{$limit:1},{$project:{ts:{$type:'$ts'},unix:{$type:'$unix'},open:{$type:'$open'}}}]) ...")
sh("s07_missing", "print('1m null/missing close:', db.candles_1m.countDocuments({$or:[{close:null},{close:{$exists:false}}]})); print('1m volume==0:', db.candles_1m.countDocuments({volume:0})); print('spark ret_1d null:', db.spark_daily_features.countDocuments({ret_1d:null})); print('spark vol_30d null:', db.spark_daily_features.countDocuments({vol_30d:null}))",
   "db.candles_1m.countDocuments({close:null}); db.candles_1m.countDocuments({volume:0}); db.spark_daily_features.countDocuments({ret_1d:null}) ...")
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
sh("s17_getindexes", "db.candles_1m.getIndexes(); db.anomalies.getIndexes(); db.candles_1d.getIndexes()")
EXPL = lambda coll, flt, extra: (
    "const f=(e)=>{const s=e.executionStats;let p=e.queryPlanner.winningPlan,st=[];while(p){st.push(p.stage);p=p.inputStage}; return {stage:st.reverse().join(' <- '),executionTimeMillis:s.executionTimeMillis,totalDocsExamined:s.totalDocsExamined,totalKeysExamined:s.totalKeysExamined,nReturned:s.nReturned}};"
    f"const q=()=>db.{coll}.find({flt}){extra};"
    "print('BEFORE (hint $natural):'); printjson(f(q().hint({$natural:1}).explain('executionStats')));"
    "print('AFTER (index):'); printjson(f(q().explain('executionStats')))")
sh("s18_explain_ts", EXPL("candles_1m", "{ts:{$gte:ISODate('2021-05-19'),$lt:ISODate('2021-05-20')}}", ".sort({ts:1})"),
   "q = db.candles_1m.find({ts:{$gte:ISODate('2021-05-19'),$lt:ISODate('2021-05-20')}}).sort({ts:1}); q.hint({$natural:1}).explain('executionStats') vs q.explain('executionStats')")
sh("s19_explain_vol", EXPL("candles_1m", "{}", ".sort({volume:-1}).limit(10)"),
   "q = db.candles_1m.find().sort({volume:-1}).limit(10); q.hint({$natural:1}).explain('executionStats') vs q.explain('executionStats')")
sh("s20_explain_anom", EXPL("anomalies", "{date:{$gte:ISODate('2021-01-01'),$lt:ISODate('2022-01-01')}}", ".sort({score:-1}).limit(20)"),
   "q = db.anomalies.find({date:{...2021}}).sort({score:-1}).limit(20); q.hint({$natural:1}).explain('executionStats') vs q.explain('executionStats')")
sh("s21_ts_stats", "const s=db.candles_1m_ts.stats(); printjson({ns:s.ns,timeseries:{bucketCount:s.timeseries.bucketCount,avgBucketSize:s.timeseries.avgBucketSize},size:s.size,storageSize:s.storageSize}); const f=db.candles_1m.stats(); printjson({ns:f.ns,count:f.count,size:f.size,storageSize:f.storageSize,avgObjSize:f.avgObjSize})",
   "db.candles_1m_ts.stats(); db.candles_1m.stats()")
# --- python-виводи ---
rd = lambda p: open(f"{BASE}/{p}", encoding="utf-8").read()
raw("s22_load_log", "python3 code/pz4_load.py  (фрагмент виводу)", "\n".join(l for l in rd("results/load.log").splitlines() if l.startswith("{") or l.startswith("DONE") or "7200000" in l or l.endswith(" 0s"))[-4000:])
import pandas as pd
pd.set_option("display.width", 200); pd.set_option("display.max_columns", 20)
t = pd.read_csv(f"{BASE}/results/pz4_index_comparison.csv")
raw("s23_index_table", "python3 -c \"print(pd.read_csv('results/pz4_index_comparison.csv')[[...]])\"",
    t[["experiment", "phase", "executionTimeMillis_explain_median", "totalDocsExamined", "totalKeysExamined", "nReturned", "winning_plan"]].to_string(index=False))
