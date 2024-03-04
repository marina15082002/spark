import unittest
from pyspark.sql import SparkSession
from src.fr.hymaia.exo2.aggregate import load_clean_data, calculate_population_by_department, execute_aggregate
from pyspark.sql.utils import AnalysisException

class TestAggregate(unittest.TestCase):
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    def test_load_clean_data(self):
        clean_df = load_clean_data(self.spark)
        self.assertIsNotNone(clean_df)
        self.assertGreater(clean_df.count(), 0)

    def test_calculate_population_by_department(self):
        data = [("A", "John"), ("B", "Alice"), ("A", "Bob")]
        columns = ["department", "name"]
        clean_df = self.spark.createDataFrame(data, columns)

        aggregated_df = calculate_population_by_department(clean_df)

        expected_data = [("A", 2), ("B", 1)]
        expected_columns = ["department", "nb_people"]
        expected_df = self.spark.createDataFrame(expected_data, expected_columns)

        self.assertEqual(aggregated_df.select(expected_columns).collect(), expected_df.collect())

        expected_data = [("A", -2), ("B", 1)]
        expected_columns = ["department", "nb_people"]
        expected_df = self.spark.createDataFrame(expected_data, expected_columns)

        self.assertNotEqual(aggregated_df.select(expected_columns).collect(), expected_df.collect())

    def test_spark_error_handling(self):
        data = [("A", "John"), ("B", "Alice"), ("A", "Bob")]
        columns = ["dept", "name"]
        incorrect_df = self.spark.createDataFrame(data, columns)

        with self.assertRaises(AnalysisException):
            calculate_population_by_department(incorrect_df)

    def test_integration(self):
        execute_aggregate(self.spark)

if __name__ == "__main__":
    unittest.main()
