-- q1_year_stats
SELECT year, COUNT(*) AS minutes, ROUND(AVG(Close),2) AS avg_close, MAX(High) AS max_high, MIN(Low) AS min_low, ROUND(SUM(Volume),0) AS volume, ROUND(100*AVG(is_flat),2) AS flat_pct FROM btc GROUP BY year ORDER BY year;

-- q2_top_days_range
SELECT date, open, high, low, ROUND((high-low)/open,4) AS hl_rel, ROUND(volume,0) AS volume, ROUND(flat_share,3) AS flat_share FROM btc_daily WHERE is_complete = 1 AND flat_share < 0.5 ORDER BY hl_rel DESC LIMIT 10;

-- q3_dow_hour_volume
SELECT dayofweek(datetime) AS dow, hour, ROUND(AVG(Volume),4) AS avg_volume_per_minute, ROUND(SUM(Volume),0) AS volume_total FROM btc WHERE year >= 2018 GROUP BY dow, hour ORDER BY dow, hour;

-- q4_month_seasonality
SELECT month, ROUND(AVG(rv),5) AS avg_daily_rv, ROUND(AVG(hl_rel),4) AS avg_hl_rel, ROUND(AVG(volume),1) AS avg_daily_volume, COUNT(*) AS days FROM btc_daily WHERE year >= 2018 AND is_complete = 1 GROUP BY month ORDER BY month;

-- q5_max_volume_day_per_year
SELECT year, date, ROUND(volume,0) AS volume, close FROM ( SELECT year, date, volume, close, ROW_NUMBER() OVER (PARTITION BY year ORDER BY volume DESC) AS rn FROM btc_daily) WHERE rn = 1 ORDER BY year;

-- q6_crash_days_vs_30d_median
SELECT * FROM ( SELECT date, ROUND(100*ret_1d,2) AS ret_pct, ROUND(rv,4) AS rv, ROUND(volume,0) AS volume, ROUND(volume / AVG(volume) OVER (ORDER BY date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING),2) AS volume_x_prev30_mean FROM btc_daily) WHERE date IN (DATE'2020-03-12', DATE'2020-03-13', DATE'2021-05-19', DATE'2022-06-13', DATE'2022-11-08', DATE'2022-11-09') ORDER BY date;

