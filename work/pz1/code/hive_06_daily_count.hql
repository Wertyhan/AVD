-- hive_06_daily_count.hql
-- Контроль: кількість рядків добової таблиці та вибірка перших/останніх діб.
USE btc;
SET hive.cli.print.header=true;
SET hive.resultset.use.unique.column.names=false;

SELECT COUNT(*) AS daily_rows FROM daily_ohlc;
SELECT * FROM daily_ohlc ORDER BY trade_date LIMIT 5;
