from pyspark.sql.functions import when, col
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def load_data(spark):
    clients_schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("zip", StringType(), True)
    ])

    city_schema = StructType([
        StructField("zip", StringType(), True),
        StructField("city", StringType(), True)
    ])

    clients_df = spark.read.csv("src/resources/exo2/clients_bdd.csv", schema=clients_schema, header=True)
    villes_df = spark.read.csv("src/resources/exo2/city_zipcode.csv", schema=city_schema, header=True)

    villes_df = villes_df.withColumnRenamed("zip", "city_zip")

    return clients_df, villes_df


def filter_clients(clients_df):
    result_df = clients_df.filter(col("age") >= 18)
    return result_df

def join_data(clients_df, villes_df):
    result_df = clients_df.join(villes_df, clients_df["zip"] == villes_df["city_zip"], "left")
    result_df = result_df.distinct()
    return result_df

def add_department_column(result_df):
    result_df = result_df.withColumn("department", when(
        (col("zip").between("20000", "20190")), "2A"
    ).when(
        (col("zip").between("20190", "20999")), "2B"
    ).otherwise(
        result_df["zip"].substr(1, 2)
    ))

    result_df = result_df.drop("city_zip")

    return result_df

def write_output(result_df):
    result_df.write.parquet("data/exo2/output", mode="overwrite")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("CleanJob").getOrCreate()

    clients_df, villes_df = load_data(spark)

    clients_df = filter_clients(clients_df)
    result_df = join_data(clients_df, villes_df)
    result_df = add_department_column(result_df)

    write_output(result_df)

    spark.stop()
