from clean import load_data, filter_clients, join_data, add_department_column, write_output
from aggregate import load_clean_data, calculate_population_by_department, write_aggregate_output
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("MainJob").getOrCreate()

    clients_df, villes_df = load_data(spark)

    clients_df = filter_clients(clients_df)
    result_df = join_data(clients_df, villes_df)
    result_df = add_department_column(result_df)

    write_output(result_df)
    
    clean_df = load_clean_data(spark)
    aggregated_df = calculate_population_by_department(clean_df)

    write_aggregate_output(aggregated_df)
    
    spark.stop()
    
if __name__ == "__main__":
    main()