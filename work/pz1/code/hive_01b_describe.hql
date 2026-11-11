-- hive_01b_describe.hql
-- Метадані зовнішньої таблиці: Location, Table Type, Table Parameters (skip.header.line.count, totalSize).
USE btc;
DESCRIBE FORMATTED candles;
