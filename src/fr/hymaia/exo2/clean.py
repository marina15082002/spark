from pyspark.sql.functions import when, col
from pyspark.sql import SparkSession


def load_data(spark):
    clients_df = spark.read.option("header", True).csv("src/resources/exo2/clients_bdd.csv")
    cities_df = spark.read.option("header", True).csv("src/resources/exo2/city_zipcode.csv")
    return clients_df, cities_df


def filter_clients(clients_df):
    return clients_df.filter(col("age") >= 18)

def join_data(clients_df, cities_df):
    return clients_df.join(cities_df, "zip", "left_outer")

def add_department_column(result_df):
    result_df = result_df.withColumn("department", 
    when(
        (col("zip").between("20000", "20190")), "2A"
    ).when(
        (col("zip").between("20190", "20999")), "2B"
    ).otherwise(
        result_df["zip"].substr(1, 2)
    ))
    return result_df

def write_output(result_df):
    result_df.write.parquet("data/exo2/output", mode="overwrite")

def execute_clean(clients_df, cities_df) :
    clients_df = filter_clients(clients_df)
    result_df = join_data(clients_df, cities_df)
    return add_department_column(result_df)

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("CleanJob") \
        .getOrCreate()

    clients_df, cities_df = load_data(spark)

    result_df = execute_clean(clients_df, cities_df)

    write_output(result_df)

    spark.stop()
