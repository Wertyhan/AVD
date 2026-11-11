-- hive_04_daily_window.hql
-- Запасний варіант добової таблиці через віконні функції row_number().
-- Використовується, якщо варіант зі struct не підтримується цією версією Hive.
USE btc;
DROP VIEW  IF EXISTS daily_range_v;
DROP TABLE IF EXISTS daily_ohlc;

CREATE TABLE daily_ohlc STORED AS TEXTFILE AS
SELECT trade_date,
       max(CASE WHEN rn_first = 1 THEN open  END) AS open,
       max(high)                                  AS high,
       min(low)                                   AS low,
       max(CASE WHEN rn_last  = 1 THEN close END) AS close,
       sum(volume)                                AS volume,
       count(*)                                   AS candles
FROM (
  SELECT trade_date, ts, open, high, low, close, volume,
         row_number() OVER (PARTITION BY trade_date ORDER BY ts ASC)  AS rn_first,
         row_number() OVER (PARTITION BY trade_date ORDER BY ts DESC) AS rn_last
  FROM candles_v
) t
GROUP BY trade_date;
