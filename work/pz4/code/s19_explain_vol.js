// Порівняння плану ДО (hint $natural = COLLSCAN) і ПІСЛЯ (за індексом) для одного й того ж запиту
const f = (e) => { const s = e.executionStats; let p = e.queryPlanner.winningPlan, st = [];
  while (p) { st.push(p.stage); p = p.inputStage }
  return { stage: st.reverse().join(' <- '), executionTimeMillis: s.executionTimeMillis,
           totalDocsExamined: s.totalDocsExamined, totalKeysExamined: s.totalKeysExamined, nReturned: s.nReturned } };
const q = () => db.candles_1m.find({}).sort({volume:-1}).limit(10);
print('BEFORE (hint $natural):'); printjson(f(q().hint({$natural: 1}).explain('executionStats')));
print('AFTER (index):'); printjson(f(q().explain('executionStats')));
