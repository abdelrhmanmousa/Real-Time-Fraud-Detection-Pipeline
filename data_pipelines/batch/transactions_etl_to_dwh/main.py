import argparse
from dataclasses import dataclass
from pyspark.sql import SparkSession

import transactions_etl.io_utils as pipeline_io
import transactions_etl.transforms as transforms
from transactions_etl.schema import final_bq_schema



@dataclass
class PipelineSettings:
    """
    A container for all pipeline settings, populated from command-line arguments.
    This makes passing configuration around the application clean and explicit.
    """
    project_id: str
    input_gcs_path: str
    output_dataset: str
    bq_temp_bucket: str


def get_spark_session(settings: PipelineSettings) -> SparkSession:
    """
    Initializes and returns a SparkSession configured for BigQuery integration
    using the provided settings.
    """
    print("Initializing Spark session for Dataproc...")
    
    builder = SparkSession.builder.appName("Transactions_ETL_Pipeline")

    # Configure the BigQuery connector with the temp bucket from our settings object.
    # This is a global setting for all write operations in this session.
    builder.config("spark.sql.broadcastTimeout", "36000")
    builder.config("temporaryGcsBucket", settings.bq_temp_bucket)
    
    return builder.getOrCreate()


def main(settings: PipelineSettings, year: str, month: str, day: str, hour: str):
    """
    Main ETL logic, driven by the settings object and time partitions.
    """
    spark = get_spark_session(settings)

    # Construct the full GCS path for the specific source partition to process
    input_path = f"gs://{settings.input_gcs_path}/year={year}/month={month}/day={day}/hour={hour}/"
    print(f"Starting ETL job for source partition: {input_path}")

    # READ 
    transactions_df = pipeline_io.read_partitioned_data(spark, input_path)
    
    # Define the dimension table names (without prefixes/suffixes for cleanliness)
    DIMENSION_NAMES = ["Cards", "Users", "Merchants", "Devices", "Date", "Transaction_Flags"]
    
    # Read dimensions by dynamically constructing their full BigQuery table path.
    # The io_utils functions are now clean and take the full path directly.
    dimension_tables = {
        name.lower(): pipeline_io.read_dimension(
            spark, 
            f"{settings.project_id}.{settings.output_dataset}.Dim_{name}"
        )
        for name in DIMENSION_NAMES
    }
    
    # --- 2. TRANSFORM ---
    # The transformation functions are pure and only operate on DataFrames.
    cleaned_df = transforms.clean_transaction_data(transactions_df)
    enriched_df = transforms.enrich_with_surrogate_keys(cleaned_df, dimension_tables)
    fact_table_df = transforms.select_and_rename_for_fact(enriched_df)
    final_df = spark.createDataFrame(fact_table_df.rdd, schema=final_bq_schema)

    
    # WRITE 
    # Construct the fully-qualified name for the final fact table
    fact_table_path = f"{settings.project_id}.{settings.output_dataset}.Fact_Transactions"
    print(f"Writing final DataFrame to BigQuery table: {fact_table_path}")
    
    pipeline_io.write_fact_table(final_df, fact_table_path)

    print("ETL job completed successfully!")
    spark.stop()


if __name__ == "__main__":
    # The argparse setup is the single entrypoint for all runtime configuration.
    parser = argparse.ArgumentParser(description="Run Spark ETL job for Fraud Detection DWH.")
    
    # Define all runtime arguments required by the pipeline
    parser.add_argument("--project_id", required=True, help="The GCP Project ID.")
    parser.add_argument(
        "--input_gcs_path",
        required=True,
        help="The GCS path for source data (e.g., 'my-bucket/transactions/raw')."
    )
    parser.add_argument(
        "--output_dataset",
        required=True,
        help="The target BigQuery dataset to write the final tables to."
    )
    parser.add_argument(
        "--bq_temp_bucket",
        required=True,
        help="The GCS bucket for BigQuery connector temporary files."
    )
    parser.add_argument("--year", required=True, help="Partition year (YYYY)")
    parser.add_argument("--month", required=True, help="Partition month (MM)")
    parser.add_argument("--day", required=True, help="Partition day (DD)")
    parser.add_argument("--hour", required=True, help="Partition hour (HH)")

    args = parser.parse_args()

    pipeline_settings = PipelineSettings(
        project_id=args.project_id,
        input_gcs_path=args.input_gcs_path,
        output_dataset=args.output_dataset,
        bq_temp_bucket=args.bq_temp_bucket
    )
    
    print("--- Running ETL with the following configuration ---")
    print(f"Project ID: {pipeline_settings.project_id}")
    print(f"Input Path Root: {pipeline_settings.input_gcs_path}")
    print(f"Output Dataset: {pipeline_settings.output_dataset}")
    print(f"BigQuery Temp Bucket: {pipeline_settings.bq_temp_bucket}")
    print(f"Partition: {args.year}-{args.month}-{args.day} T{args.hour}:00")
    print("-------------------------------------------------")
    
    main(
        settings=pipeline_settings,
        year=args.year, 
        month=args.month, 
        day=args.day, 
        hour=args.hour
    )