# CHANGES: виправлення Hive (REVIEW C1, M4, M1, M2, C2-support)

## C1. TBLPROPERTIES пропуску заголовка
| | Було | Стало |
|---|---|---|
| `code/hive_01_create_table.hql` | `'skip.header.line.number'='1'` (Hive цієї властивості не знає, ігнорує) | `"skip.header.line.count"="1"` |
| `DESCRIBE FORMATTED candles` (Table Parameters) | `skip.header.line.number 1` | `skip.header.line.count 1` (скріншот 19; DESCRIBE перенесено в `hive_01b_describe.hql`, показано до Storage Information) |
| `SELECT * FROM candles LIMIT 5`, перший рядок | `NULL NULL NULL NULL NULL NULL` | `1.32537606E9 4.58 4.58 4.58 4.58 0.0` (рядка NULL немає) |
| `COUNT(*)` по таблиці `candles` | не виконувався (по поданню `candles_v`: 7 816 537) | `raw_rows` = 7 816 537 (додано в `hive_02_select.hql`); по `candles_v` теж 7 816 537 |
| Результати запитів (yearly, monthly, daily 5 429 діб, топ-10 за обсягом, топ-10 за High-Low) | `results/_old_before_hive_fix/*.csv` | побайтно ті самі csv (`cmp`), бо `candles_v` і раніше відкидав рядок із NULL |
| Звірка Hive vs MapReduce | yearly 15/15 True, daily 5429/5429 True | yearly 15/15 True, daily 5429/5429 True (`crosscheck_hive_vs_mr_*.csv` перераховано `code/rerun_hive_fix.py`) |
| `hive_count_check.csv` | hive_count, csv_rows, equal | hive_count_view, hive_count_table, csv_rows, equal_view, equal_table (усі 7 816 537, True) |
Висновок: у попередньому запуску заголовок CSV НЕ пропускався ніде (помилка в імені властивості); тепер пропускається самою таблицею, а `candles_v` додатково відкидає неповні рядки. Старі логи: `logs/_old_before_hive_fix/`. `crosscheck_mr_hive_vs_pandas.csv` не змінювався (Hive-результати ідентичні).
Перезнято скріншоти (імена збережено): 19, 20, 21, 22, 23, 24, 25. Усі кроки виконано реально командою `hive -S -f ...` (на скріншотах `hive -f ...`).

## M4. Завантаження даних у таблицю
`code/hive_07_load_data.hql`: керована таблиця `candles_managed` (TBLPROPERTIES skip.header.line.count=1) + `LOAD DATA INPATH '/user/root/btc/load_src/btcusd_1-min_data.csv'`; `COUNT(*)` = 7 816 537 (скріншот 51). Файл зовнішньої таблиці `/user/root/btc/raw` залишився (копія в `load_src` зроблена `hdfs dfs -cp`); після LOAD `load_src` порожній, файл у `/user/hive/warehouse/btc.db/candles_managed` (скріншот 52): LOAD переміщує файл, DROP керованої таблиці видалив би дані.

## M1. HDFS і WordCount
Нові скріншоти: 48 (`hdfs dfs -ls -R -h` raw, wc_in, out/wordcount), 49 (`-du -h` та `-cat | head -n 5`), 50 (`-cat .../wordcount/part-r-00000 | head -n 15`). WordCount не перезапускався (вивід у HDFS збережено).

## M2 / C2
`figures/ui_hdfs_panel.png` (NameNode Summary + Datanodes + Browse Directory з розміром 378.47 MB) і `figures/ui_yarn_panel.png` (YARN Applications + лічильники JobHistory mr_daily_ohlc з Shuffled Maps = 3). Джерела панелей: `screenshots/ui_panel_src/`; код `code/ui_panels_pz1.py`; значення в `results/ui_values.csv`. YARN RM після перезапуску демонів втратив список застосунків, тому в панелі показано 12 застосунків Hive поточного запуску; старі MR-задачі лишилися в JobHistory. Скріншоти 36/38/41/44 (старі обрізки) не оновлювалися. У NameNode Summary DFS Used 837,54 MB (більше, ніж на старих знімках: додалися таблиця `candles_managed`, копії bench і тимчасові файли); 378.47 MB (розмір CSV) показано в Browse Directory (панель c).

## Кількість застосунків Hive
`results/hive_yarn_applications.csv`, `results/hive_yarn_app_count.csv`: з REST `/ws/v1/cluster/apps` 12 застосунків (11 для hive_02..hive_06 + 1 для LOAD-кроку). У попередньому запуску було 10 (до додавання `COUNT(*) FROM candles`, який потребує окремого MR, і кроку M4; 10 застосунків у JobHistory job_1794418481996_0004..0013).
