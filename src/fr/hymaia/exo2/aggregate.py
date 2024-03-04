from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import os

def load_clean_data(spark):
    clean_df = spark.read.parquet("data/exo2/output")

    if clean_df.isEmpty():
        raise ValueError("Input DataFrame is empty")

    return clean_df

def calculate_population_by_department(clean_df):
    return clean_df.groupBy("department").agg(count("*").alias("nb_people")) \
        .orderBy(col("nb_people").desc(), col("department"))

def write_aggregate_output(aggregated_df):
    output_dir = "data/exo2/aggregate"
    os.makedirs(output_dir, exist_ok=True)

    selected_df = aggregated_df.select("department", "nb_people")

    selected_df.coalesce(1).write.csv(output_dir, header=True, mode="overwrite")

def execute_aggregate(spark) :
    clean_df = load_clean_data(spark)
    return calculate_population_by_department(clean_df)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("AggregateJob").getOrCreate()

    aggregated_df = execute_aggregate(spark)

    write_aggregate_output(aggregated_df)

    spark.stop()
