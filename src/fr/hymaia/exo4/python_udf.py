from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

spark = SparkSession.builder \
    .appName("UDF Example") \
    .getOrCreate()

sell_df = spark.read.csv("./src/resources/exo4/sell.csv", header=True)

def categorize(category):
    return "food" if int(category) < 6 else "furniture"

categorize_udf = udf(categorize, StringType())

df = sell_df.withColumn("category_name", categorize_udf(col("category")))

df.show(truncate=False)

spark.stop()