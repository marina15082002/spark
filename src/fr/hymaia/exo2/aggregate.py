from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, dense_rank
from pyspark.sql.window import Window
import os

def load_clean_data(spark):
    clean_df = spark.read.parquet("data/exo2/output")
    return clean_df

def calculate_population_by_department(clean_df):
    windowSpec = Window().partitionBy("department").orderBy(col("department"))

    aggregated_df = clean_df.groupBy("department").agg(count("name").alias("nb_people"))
    aggregated_df = aggregated_df.withColumn("rank", dense_rank().over(windowSpec))

    return aggregated_df.orderBy("rank", "department")

def write_aggregate_output(aggregated_df):
    output_dir = "data/exo2/aggregate"
    os.makedirs(output_dir, exist_ok=True)

    selected_df = aggregated_df.select("department", "nb_people")

    selected_df.coalesce(1).write.csv(output_dir, header=True, mode="overwrite")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("AggregateJob").getOrCreate()

    clean_df = load_clean_data(spark)
    aggregated_df = calculate_population_by_department(clean_df)

    spark.stop()
