-- hive_02_select.hql
-- Завдання 6: вибірка та простий підрахунок.
USE btc;
SET hive.cli.print.header=true;
SET hive.resultset.use.unique.column.names=false;

-- перші рядки таблиці
SELECT * FROM candles LIMIT 5;

-- вибірка з умовою: найбільші за обсягом хвилинні свічки
SELECT trade_date, ts, open, high, low, close, volume
FROM candles_v
WHERE volume > 1000
ORDER BY volume DESC
LIMIT 10;

-- контроль пропуску заголовка: рядків у самій таблиці (без подання)
SELECT COUNT(*) AS raw_rows FROM candles;

-- кількість свічок
SELECT COUNT(*) AS total_candles FROM candles_v;
