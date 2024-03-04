import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src.fr.hymaia.exo2.clean import load_data, filter_clients, join_data, add_department_column, write_output, execute_clean

class TestClear(unittest.TestCase):
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    def test_load_data(self):
        clients_schema = StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("zip", StringType(), True)
        ])

        city_schema = StructType([
            StructField("city_zip", StringType(), True),
            StructField("city", StringType(), True)
        ])

        clients_df, villes_df = load_data(self.spark)

        self.assertEqual(clients_df.schema, clients_schema)
        self.assertEqual(villes_df.schema, city_schema)
        self.assertGreater(clients_df.count(), 0)
        self.assertGreater(villes_df.count(), 0)

    def test_filter_clients(self):
        clients_schema = StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("zip", StringType(), True)
        ])

        test_data = [("John", 25, "12345"), ("Alice", 17, "56789"), ("Bob", 30, "67890")]
        test_df = self.spark.createDataFrame(test_data, schema=clients_schema)

        result_df = filter_clients(test_df)

        expected_result = [("John", 25, "12345"), ("Bob", 30, "67890")]
        expected_df = self.spark.createDataFrame(expected_result, schema=clients_schema)

        self.assertEqual(result_df.collect(), expected_df.collect())

    def test_join_data(self):
        clients_schema = StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("zip", StringType(), True)
        ])

        villes_schema = StructType([
            StructField("city_zip", StringType(), True),
            StructField("city", StringType(), True)
        ])

        clients_data = [("John", 25, "12345"), ("Alice", 30, "67890")]
        clients_df = self.spark.createDataFrame(clients_data, schema=clients_schema)

        villes_data = [("12345", "Paris"), ("67890", "London")]
        villes_df = self.spark.createDataFrame(villes_data, schema=villes_schema)

        result_df = join_data(clients_df, villes_df)

        expected_columns = ["name", "age", "zip", "city_zip", "city"]
        self.assertEqual(result_df.columns, expected_columns)

        expected_data = [("John", 25, "12345", "12345", "Paris"), ("Alice", 30, "67890", "67890", "London")]
        expected_df = self.spark.createDataFrame(expected_data, schema=expected_columns)

        self.assertEqual(result_df.collect(), expected_df.collect())

        expected_data = [("John", 25, "12345", "", "Paris"), ("Alice", 30, "67890", "67890", "London")]
        expected_df = self.spark.createDataFrame(expected_data, schema=expected_columns)
        self.assertNotEqual(result_df.collect(), expected_df.collect())


    def test_add_department_column(self):
        result_schema = StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("zip", StringType(), True),
            StructField("department", StringType(), True)
        ])

        result_data = [("John", 25, "12345", ""), ("Alice", 30, "67890", "")]
        result_df = self.spark.createDataFrame(result_data, schema=result_schema)

        result_df = add_department_column(result_df)

        self.assertIn("department", result_df.columns)

        expected_data = [("John", 25, "12345", "12"), ("Alice", 30, "67890", "67")]
        expected_df = self.spark.createDataFrame(expected_data, schema=result_schema)

        self.assertEqual(result_df.collect(), expected_df.collect())

    def test_Corse(self):
        result_schema = StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("zip", StringType(), True),
            StructField("department", StringType(), True)
        ])

        result_data = [("John", 25, "20190", "")]
        result_df = self.spark.createDataFrame(result_data, schema=result_schema)

        result_df = add_department_column(result_df)

        expected_data = [("John", 25, "20190", "2A")]
        expected_df = self.spark.createDataFrame(expected_data, schema=result_schema)

        self.assertEqual(result_df.collect(), expected_df.collect())

    def test_integration(self):
        clients_schema = StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("zip", StringType(), True)
        ])

        villes_schema = StructType([
            StructField("city_zip", StringType(), True),
            StructField("city", StringType(), True)
        ])

        clients_data = [("John", 25, "12345"), ("Alice", 30, "67890")]
        clients_df = self.spark.createDataFrame(clients_data, schema=clients_schema)

        villes_data = [("12345", "Paris"), ("67890", "London")]
        villes_df = self.spark.createDataFrame(villes_data, schema=villes_schema)

        execute_clean(clients_df, villes_df)

if __name__ == "__main__":
    unittest.main()
