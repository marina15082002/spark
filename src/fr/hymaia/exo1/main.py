import pyspark.sql.functions as f
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("wordCount").master("local[*]").getOrCreate()

    df = spark.read.csv("src/resources/exo1/data.csv", header=True, inferSchema=True)

    df_result = wordcount(df, "text")

    df_result.show(truncate=False)

    df_result.write.parquet("data/exo1/output", mode="overwrite")

    spark.stop()



def wordcount(df, col_name):
    return df.withColumn('word', f.explode(f.split(f.col(col_name), ' '))) \
        .groupBy('word') \
        .count()


if __name__ == "__main__":
    main()

