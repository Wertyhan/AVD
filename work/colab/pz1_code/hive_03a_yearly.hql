-- hive_03a_yearly.hql
-- Завдання 6: групування та агрегація за роками.
USE btc;
SET hive.cli.print.header=true;
SET hive.resultset.use.unique.column.names=false;

SELECT year(trade_date)  AS yr,
       COUNT(*)          AS candles,
       SUM(volume)       AS total_volume,
       AVG(close)        AS avg_close,
       MAX(high)         AS max_high,
       MIN(low)          AS min_low
FROM candles_v
GROUP BY year(trade_date)
ORDER BY yr;
