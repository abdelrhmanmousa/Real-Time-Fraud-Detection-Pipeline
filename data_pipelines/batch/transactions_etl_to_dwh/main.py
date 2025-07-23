import os
import argparse
from pyspark.sql import SparkSession
from transactions_etl.config import Settings, PIPELINE_MODE
import transactions_etl.io_utils as pipeline_io
import transactions_etl.transforms as transforms


def get_spark_session():
    """
    Initializes and returns a SparkSession with configurations tailored
    to the execution environment (local vs. GCP Dataproc).
    """
    print(f"Initializing Spark in '{PIPELINE_MODE}' mode.")
    
    builder = SparkSession.builder.appName("Transactions_ETL_Pipeline")

    if PIPELINE_MODE == "local":
        # === LOCAL MODE CONFIGURATION ===
        
        # Correctly formatted Maven coordinates
        
        project_dir = os.path.abspath(os.path.dirname(__file__))
        gcs_jar_path = os.path.join(project_dir, "gcs-connector.jar")
        bq_jar_path = os.path.join(project_dir, "bq-connector.jar")

        # packages = [
        #     # gcs_jar_path,
        #     # "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.29.0",
        #     # bq_jar_path,
        #     "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.42.2"
            
        # ]

        # # Use .config() to set the packages property
        # builder.config("spark.jars.packages", ",".join(packages))
        


        builder.config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        builder.config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        
        # Configure to use Application Default Credentials (ADC)
        builder.config("spark.hadoop.google.cloud.auth.type", "APPLICATION_DEFAULT_CREDENTIALS")
        builder.master("local[4]") 
        builder.config("spark.driver.memory", "4g")
    
    return builder.getOrCreate()
    

def main(year, month, day, hour):
    """Main ETL logic, parameterized for scheduled runs."""
    spark = get_spark_session()

    # Always construct the GCS path dynamically from arguments
    if PIPELINE_MODE == "local":
        input_path = Settings.LOCAL_DATA
    else:
        input_path = (
            f"{Settings.GCS_INPUT_PATH_ROOT}/year={year}/month={month}/"
            f"day={day}/hour={hour}/"
        )
        print(f"Starting ETL job for GCS partition: {input_path}")

    # 1. READ
    transactions_df = pipeline_io.read_partitioned_data(spark, input_path)
    
    dimension_tables = {
        name: pipeline_io.read_dimension(spark, getattr(Settings, f"DIM_{name.upper()}_TABLE"))
        for name in ["cards", "users", "merchants", "devices", "date", "time", "flags"]
    }
    
    # 2. TRANSFORM (No changes needed here)
    cleaned_df = transforms.clean_transaction_data(transactions_df)
    enriched_df = transforms.enrich_with_dimensions(cleaned_df, dimension_tables)
    fact_table_df = transforms.select_and_rename_for_fact(enriched_df)

    # 3. WRITE
    pipeline_io.write_fact_table(fact_table_df, Settings.FACT_TABLE)

    print("ETL job completed successfully!")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Spark ETL job against GCP services.")
    parser.add_argument("--year", required=True, help="Partition year")
    parser.add_argument("--month", required=True, help="Partition month")
    parser.add_argument("--day", required=True, help="Partition day")
    parser.add_argument("--hour", required=True, help="Partition hour")
    
    args = parser.parse_args()
    main(args.year, args.month, args.day, args.hour)