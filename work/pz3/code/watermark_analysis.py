# -*- coding: utf-8 -*-
"""Завдання 5 (watermark): вимірювання невпорядкованості подій при надходженні у Kafka-тему та водяних позначок цього прогону.
Результати: results/pz3_watermark_disorder.csv (запізнення подій відносно максимуму event time, побаченого раніше),
pz3_watermark_gap.csv (за запитами: watermark і розрив час обробки – watermark з lastProgress/progress),
pz3_watermark_lastprogress.csv (eventTime з lastProgress JSON)."""
import json, os, sys
import numpy as np, pandas as pd
from confluent_kafka import Consumer, TopicPartition
R = sys.argv[1]; TOPIC = sys.argv[2] if len(sys.argv) > 2 else "bigdata-topic"
c = Consumer({"bootstrap.servers": "localhost:9092", "group.id": "pz3-wm-analysis", "enable.auto.commit": False, "auto.offset.reset": "earliest"})
md = c.list_topics(TOPIC, timeout=10).topics[TOPIC]
tps = [TopicPartition(TOPIC, p, 0) for p in md.partitions]
hi = {p.partition: c.get_watermark_offsets(p, timeout=10)[1] for p in tps}
c.assign(tps)
rows = []
while len(rows) < sum(hi.values()):
    m = c.poll(5)
    if m is None or m.error():
        continue
    v = json.loads(m.value())
    rows.append((m.partition(), m.offset(), m.timestamp()[1] / 1000.0, m.key().decode(), v["ts"]))
c.close()
d = pd.DataFrame(rows, columns=["partition", "offset", "kafka_create_s", "key", "event_ts"])


def lateness(g):
    """запізнення = max(event_ts, побачений раніше) - event_ts, у секундах; порядок = порядок надходження"""
    mx = g["event_ts"].cummax()
    return (mx - g["event_ts"]).to_numpy()


out = []
def rec(scope, g):
    lt = lateness(g)
    out.append({"scope": scope, "n_messages": len(g), "n_out_of_order": int((lt > 0).sum()), "max_lateness_s": float(lt.max()),
                "p99_lateness_s": float(np.percentile(lt, 99)), "duplicates_event_ts": int(g["event_ts"].duplicated().sum()),
                "event_ts_min": pd.to_datetime(g["event_ts"].min(), unit="s", utc=True).isoformat(),
                "event_ts_max": pd.to_datetime(g["event_ts"].max(), unit="s", utc=True).isoformat()})
for p, g in d.groupby("partition"):
    rec(f"partition {p} (порядок зсувів)", g.sort_values("offset"))
for k, g in d.groupby("key"):
    rec(f"key {k} (порядок зсувів)", g.sort_values(["partition", "offset"]))
rec("усі партиції, порядок за Kafka CreateTime (реальний порядок надходження)", d.sort_values(["kafka_create_s", "partition", "offset"]))
# сценарій backlog: партиції читаються одним мікробатчем, партиція 0 (2022-11) повністю "випереджає" партицію 1 (2020, 2021)
per_p = {p: g.sort_values("offset") for p, g in d.groupby("partition")}
mixed = pd.concat([per_p[p].assign(r=np.arange(len(per_p[p])) / len(per_p[p])) for p in per_p]).sort_values(["r", "partition"])
rec("усі партиції, пропорційне змішування (backlog, maxOffsetsPerTrigger)", mixed)
pd.DataFrame(out).to_csv(os.path.join(R, "pz3_watermark_disorder.csv"), index=False)
print(pd.DataFrame(out).T.to_string())

# --- водяні позначки з прогресу цього прогону
pg = pd.read_csv(os.path.join(R, "stream", "progress.csv"), parse_dates=["timestamp", "watermark"])
pg = pg[pg["watermark"].notna() & (pg["watermark"] > "1971-01-01")]
g = pg.groupby("query").agg(batches_with_watermark=("batch_id", "size"), first_watermark=("watermark", "min"), last_watermark=("watermark", "max"),
                            gap_s_min=("watermark_gap_s", "min"), gap_s_median=("watermark_gap_s", "median"), gap_s_max=("watermark_gap_s", "max"),
                            rows_dropped_by_watermark=("dropped_by_watermark", "sum"))
last = pg.sort_values("batch_id").groupby("query").tail(1).set_index("query")
g["gap_s_last_batch"] = last["watermark_gap_s"]; g["last_batch_timestamp"] = last["timestamp"]
g["gap_years_median"] = (g["gap_s_median"] / 31_557_600).round(2)
g.to_csv(os.path.join(R, "pz3_watermark_gap.csv")); print(g.T.to_string())
lp = json.load(open(os.path.join(R, "stream", "lastProgress.json")))
rows = [{"query": q, "timestamp": p.get("timestamp"), "batchId": p.get("batchId"), "eventTime": json.dumps(p.get("eventTime")),
         "numRowsDroppedByWatermark": sum(o.get("numRowsDroppedByWatermark", 0) for o in p.get("stateOperators", []))} for q, p in lp.items()]
pd.DataFrame(rows).to_csv(os.path.join(R, "pz3_watermark_lastprogress.csv"), index=False)
print(pd.DataFrame(rows).to_string())
