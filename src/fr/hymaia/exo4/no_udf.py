from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum, expr
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("No UDF Example") \
    .config("spark.jars", "./src/resources/exo4/udf.jar") \
    .getOrCreate()

df = spark.read.csv("./src/resources/exo4/sell.csv", header=True)


df = df.withColumn("category_name", when(col("category") < 6, "food").otherwise("furniture"))
"""
df = df.withColumn("total_price_per_category_per_day",
                   sum("price").over(Window.partitionBy("date", "category_name")))

window_spec = Window.partitionBy("category_name").orderBy("date").rowsBetween(-29, 0)
df = df.withColumn("total_price_per_category_per_day_last_30_days",
                   sum("price").over(window_spec))
"""
df.show(truncate=False)

spark.stop()