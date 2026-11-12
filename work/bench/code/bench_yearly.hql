-- T1: свічки і сумарний обсяг за роками (без ORDER BY, як у Streaming); результат пишеться в HDFS
USE btc;
INSERT OVERWRITE DIRECTORY '/user/root/btc/out/bench_hive_T1'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT year(trade_date), COUNT(*), SUM(volume)
FROM candles_v
GROUP BY year(trade_date);
