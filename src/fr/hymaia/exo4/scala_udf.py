from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.column import Column, _to_java_column, _to_seq

spark = SparkSession.builder \
    .appName("UDF Scala Example") \
    .config("spark.jars", "./src/resources/exo4/udf.jar") \
    .getOrCreate()

def addCategoryName(col):
    sc = spark.sparkContext
    add_category_name_udf = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()
    return Column(add_category_name_udf.apply(_to_seq(sc, [col], _to_java_column)))

data_path = "./src/resources/exo4/sell.csv"
df = spark.read.csv(data_path, header=True)

df_with_category_name = df.withColumn("category_name", addCategoryName(col("category")))

df_with_category_name.show(truncate=False)

spark.stop()