-- hive_05b_top_range.hql
-- Топ-10 діб за амплітудою High - Low (через подання daily_range_v).
USE btc;
SET hive.cli.print.header=true;
SET hive.resultset.use.unique.column.names=false;

CREATE VIEW IF NOT EXISTS daily_range_v AS
SELECT trade_date, open, high, low, close, volume, high - low AS price_range
FROM daily_ohlc;

SELECT trade_date, high, low, price_range, volume
FROM daily_range_v
ORDER BY price_range DESC
LIMIT 10;
