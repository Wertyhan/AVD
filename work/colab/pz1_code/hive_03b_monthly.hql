-- hive_03b_monthly.hql
-- Завдання 6: групування за роком і місяцем.
USE btc;
SET hive.cli.print.header=true;
SET hive.resultset.use.unique.column.names=false;

SELECT year(trade_date)  AS yr,
       month(trade_date) AS mn,
       COUNT(*)          AS candles,
       SUM(volume)       AS total_volume,
       AVG(close)        AS avg_close
FROM candles_v
GROUP BY year(trade_date), month(trade_date)
ORDER BY yr, mn;
