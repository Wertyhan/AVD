-- hive_07_load_data.hql
-- Завдання 5, крок «Завантажити дані в таблицю»: керована (MANAGED) таблиця та LOAD DATA INPATH.
-- Джерело: копія CSV у /user/root/btc/load_src (файл зовнішньої таблиці в /user/root/btc/raw лишається).
-- LOAD DATA INPATH ПЕРЕМІЩУЄ файл у каталог сховища /user/hive/warehouse/btc.db/candles_managed.
USE btc;
DROP TABLE IF EXISTS candles_managed;

CREATE TABLE candles_managed (
  ts     DOUBLE,
  open   DOUBLE,
  high   DOUBLE,
  low    DOUBLE,
  close  DOUBLE,
  volume DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH '/user/root/btc/load_src/btcusd_1-min_data.csv' INTO TABLE candles_managed;

SET hive.cli.print.header=true;
SELECT COUNT(*) AS managed_rows FROM candles_managed;
SELECT * FROM candles_managed LIMIT 3;
