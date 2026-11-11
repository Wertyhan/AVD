-- hive_05a_top_volume.hql
-- Топ-10 діб за сумарним обсягом торгів (з добової таблиці).
USE btc;
SET hive.cli.print.header=true;
SET hive.resultset.use.unique.column.names=false;

SELECT trade_date, open, high, low, close, volume, candles
FROM daily_ohlc
ORDER BY volume DESC
LIMIT 10;
