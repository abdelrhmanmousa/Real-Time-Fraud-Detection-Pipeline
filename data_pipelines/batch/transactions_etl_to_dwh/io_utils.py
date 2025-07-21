from pyspark.sql import SparkSession, DataFrame
from config import Settings
from schema import raw_transactions_schema

def read_partitioned_data(spark: SparkSession, full_input_path: str) -> DataFrame:
    """Reads partitioned Parquet data from a fully specified GCS path."""
    print(f"Reading data from GCS source: {full_input_path}")
    return spark.read.schema(raw_transactions_schema).parquet(full_input_path)

def read_dimension(spark: SparkSession, table_name: str) -> DataFrame:
    """Reads a dimension table from BigQuery."""
    print(f"Reading dimension from BigQuery: {table_name}")
    return spark.read.format("bigquery") \
        .option("table", f"{Settings.BIGQUERY_DATASET}.{table_name}") \
        .load()

def write_fact_table(df: DataFrame, table_name: str) -> None:
    """Writes the final DataFrame to a BigQuery table."""
    print(f"Writing fact table to BigQuery: {table_name}")
    df.write.format("bigquery") \
        .option("table", f"{Settings.GCP_PROJECT_ID}.{Settings.BIGQUERY_DATASET}.{table_name}") \
        .option("temporaryGcsBucket", Settings.BIGQUERY_TEMP_GCS_BUCKET) \
        .mode("append") \
        .save()