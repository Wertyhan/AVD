-- T2: добові OHLC (open = open свічки з min ts, close = close свічки з max ts); результат пишеться в HDFS
USE btc;
INSERT OVERWRITE DIRECTORY '/user/root/btc/out/bench_hive_T2'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT CAST(trade_date AS STRING), min(struct(ts, open)).col2, max(high), min(low),
       max(struct(ts, close)).col2, sum(volume), count(*)
FROM candles_v
GROUP BY trade_date;
