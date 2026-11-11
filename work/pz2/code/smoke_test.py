from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[2]").config("spark.ui.enabled","false").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
df = spark.range(1, 1001).selectExpr("id", "id % 7 AS k")
print("Spark", spark.version, "| count =", df.count())
df.groupBy("k").count().orderBy("k").show()
spark.stop()
