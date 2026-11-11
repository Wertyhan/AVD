import sys, time, re
sys.path.insert(0, "/tmp")
from pyspark.sql import SparkSession
from playwright.sync_api import sync_playwright
src = open("/home/user/AVD/work/pz2/code/pz2_spark.py", encoding="utf-8").read()
RAW = "/home/user/AVD/work/data/raw/btcusd_1-min_data.csv"
spark = (SparkSession.builder.appName("PZ2_BTC_Spark").master("local[*]").config("spark.driver.memory","8g")
         .config("spark.sql.shuffle.partitions","16").config("spark.sql.session.timeZone","UTC").config("spark.ui.port","4040").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
from pyspark.sql import functions as F
d = spark.read.csv(RAW, header=True, inferSchema=True)
d = (d.withColumn("datetime", F.timestamp_seconds("Timestamp")).withColumn("year", F.year("datetime")).withColumn("month", F.month("datetime"))
      .withColumn("hour", F.hour("datetime")).withColumn("is_flat", (F.col("Open")==F.col("Close")).cast("int")))
d.createOrReplaceTempView("btc")
qs = re.findall(r"^-- q[123].*\n(.*);", open("/home/user/AVD/work/pz2/results/pz2_sql_queries.sql").read(), re.M)
for q in [
 "SELECT year, COUNT(*) AS minutes, ROUND(AVG(Close),2) AS avg_close, MAX(High) AS max_high, MIN(Low) AS min_low, ROUND(SUM(Volume),0) AS volume, ROUND(100*AVG(is_flat),2) AS flat_pct FROM btc GROUP BY year ORDER BY year",
 "SELECT dayofweek(datetime) AS dow, hour, ROUND(AVG(Volume),4) AS avg_volume_per_minute, ROUND(SUM(Volume),0) AS volume_total FROM btc WHERE year >= 2018 GROUP BY dow, hour ORDER BY dow, hour",
 "SELECT month, COUNT(*) AS minutes, ROUND(AVG(Volume),4) AS avg_vol FROM btc WHERE year >= 2018 GROUP BY month ORDER BY month"]:
    print(spark.sql(q).toPandas().shape, flush=True)
url = spark.sparkContext.uiWebUrl; print(url)
out = "/home/user/AVD/work/pz2/screenshots"
with sync_playwright() as p:
    b = p.chromium.launch(executable_path="/opt/pw-browsers/chromium-1243/chrome-linux64/chrome", args=["--no-sandbox"])
    pg = b.new_page(viewport={"width":1500,"height":900})
    pg.goto(url + "/SQL/", wait_until="load")
    pg.wait_for_selector("table tbody tr", timeout=30000); time.sleep(3)
    pg.screenshot(path=f"{out}/spark_ui_sql.png", full_page=True)
    links = pg.eval_on_selector_all("a[href*='SQL/execution/?id=']", "e=>e.map(a=>a.href)")
    print(links)
    pg.goto(links[0], wait_until="load")
    pg.wait_for_selector("svg", timeout=30000); time.sleep(3)
    pg.screenshot(path=f"{out}/spark_ui_sql_detail.png", full_page=True)
    b.close()
spark.stop()
