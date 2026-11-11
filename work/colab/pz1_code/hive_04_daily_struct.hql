-- hive_04_daily_struct.hql
-- Добова таблиця OHLCV зі свічок 1 хв (CTAS).
-- Open = Open свічки з мінімальним ts, Close = Close свічки з максимальним ts:
-- min/max над struct(ts, значення) порівнюють спочатку за ts (перше поле).
USE btc;
DROP VIEW  IF EXISTS daily_range_v;
DROP TABLE IF EXISTS daily_ohlc;

CREATE TABLE daily_ohlc STORED AS TEXTFILE AS
SELECT trade_date,
       min(struct(ts, open)).col2  AS open,
       max(high)                   AS high,
       min(low)                    AS low,
       max(struct(ts, close)).col2 AS close,
       sum(volume)                 AS volume,
       count(*)                    AS candles
FROM candles_v
GROUP BY trade_date;
