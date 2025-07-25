from pyspark.sql import SparkSession, DataFrame
from .schema import raw_transactions_schema

def read_partitioned_data(spark: SparkSession, full_input_path: str) -> DataFrame:
    """
    Reads partitioned Parquet data from a fully specified GCS path.
    This function remains the same as it was already well-parameterized.
    """
    print(f"Reading data from GCS source: {full_input_path}")
    return spark.read.schema(raw_transactions_schema).parquet(full_input_path)

def read_dimension(spark: SparkSession, full_table_path: str) -> DataFrame:
    """
    Reads a dimension table from BigQuery using its fully-qualified path.

    Args:
        spark: The SparkSession object.
        full_table_path: The full BigQuery table name in the format
                         'project_id.dataset_id.table_name'.
    """
    print(f"Reading dimension from BigQuery: {full_table_path}")
    
    # CHANGED: The function now takes the complete path directly.
    # It no longer builds the path itself.
    return spark.read.format("bigquery") \
        .option("table", full_table_path) \
        .load()

def write_fact_table(df: DataFrame, full_table_path: str) -> None:
    """
    Writes the final DataFrame to a BigQuery table using its fully-qualified path.

    Args:
        df: The DataFrame to write.
        full_table_path: The full BigQuery table name in the format
                         'project_id.dataset_id.table_name'.
    """
    print(f"Writing fact table to BigQuery: {full_table_path}")
    
    # CHANGED: This function is now much simpler.
    # It takes the full path and no longer needs to know about the temp bucket,
    # as that is configured globally on the SparkSession in main.py.
    df.write.format("bigquery") \
        .option("table", full_table_path) \
        .mode("append") \
        .save()