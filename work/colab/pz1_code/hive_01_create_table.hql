-- hive_01_create_table.hql
-- Завдання 5: зовнішня (EXTERNAL) таблиця Hive поверх CSV, що вже лежить у HDFS.
-- Дані не копіюються: Hive лише описує схему над каталогом /user/root/btc/raw.
CREATE DATABASE IF NOT EXISTS btc;
USE btc;

DROP VIEW IF EXISTS candles_v;
DROP TABLE IF EXISTS candles;

CREATE EXTERNAL TABLE candles (
  ts     DOUBLE,   -- Timestamp (unix-секунди, UTC)
  open   DOUBLE,
  high   DOUBLE,
  low    DOUBLE,
  close  DOUBLE,
  volume DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/root/btc/raw'
TBLPROPERTIES ('skip.header.line.number'='1');   -- пропустити рядок із назвами стовпців

-- Подання з датою UTC (без залежності від часового поясу сесії Hive)
-- та фільтром пропущених значень (аналог dropna у Pandas).
CREATE VIEW candles_v AS
SELECT date_add('1970-01-01', CAST(floor(ts / 86400) AS INT)) AS trade_date,
       ts, open, high, low, close, volume
FROM candles
WHERE ts IS NOT NULL AND open IS NOT NULL AND high IS NOT NULL
  AND low IS NOT NULL AND close IS NOT NULL AND volume IS NOT NULL;

DESCRIBE FORMATTED candles;
