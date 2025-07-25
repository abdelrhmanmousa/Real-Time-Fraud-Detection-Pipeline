from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col,
    when,
    trim,
    to_date,
    to_timestamp,
    date_format,
    broadcast,
    xxhash64,
    concat_ws,
)
from .schema import final_bq_schema

# Initial Cleaning 

def clean_transaction_data(df: DataFrame) -> DataFrame:
    """
    Performs basic cleaning on the pre-enriched transaction data.
    """
    print("Performing basic cleaning on pre-enriched data...")

    # Filter records with null critical identifiers needed for joins
    cleaned_df = df.filter(
        col("transaction_id").isNotNull()
        & col("user_id").isNotNull()
        & col("card_id").isNotNull()
        & col("merchant_id").isNotNull()
        & col("device_id").isNotNull()
    ).dropDuplicates(["transaction_id"])

    # Trim whitespace from identifiers to prevent join failures
    cleaned_df = (
        cleaned_df.withColumn("user_id", trim(col("user_id")))
        .withColumn("card_id", trim(col("card_id")))
        .withColumn("device_id", trim(col("device_id")))
        .withColumn("merchant_id", trim(col("merchant_id")))
    )

    # Simple validation on amount
    cleaned_df = cleaned_df.withColumn(
        "amount", when(col("amount") < 0, 0).otherwise(col("amount"))
    )

    return cleaned_df

# Dimensional Enrichment (Surrogate Key Lookup) 

def enrich_with_surrogate_keys(transactions_df: DataFrame, dims: dict) -> DataFrame:
    """
    Enriches the transaction data by joining with dimension tables to get their surrogate keys.
    """
    print("Enriching data with dimension surrogate keys...")

    # Create Join Keys 
    # The input data already contains all the boolean flags, so we can directly create the composite key for the flags dimension.

    df_with_join_keys = transactions_df.withColumn(
        "Date_Join_Key",
        date_format(to_date(col("timestamp")), "yyyyMMdd").cast("integer"),
    ).withColumn(
    "Flags_Join_Key",
    (
        when(col("is_weekend_transaction"), 32).otherwise(0) +
        when(col("is_night_transaction"), 16).otherwise(0) +
        when(col("is_geo_ip_mismatch"), 8).otherwise(0) +
        when(col("is_foreign_country_tx"), 4).otherwise(0) +
        when(col("was_3ds_successful"), 2).otherwise(0) +
        when(col("is_new_device"), 1).otherwise(0)
    )
)

    # The goal here is solely to append the surrogate key columns (User_SK, Card_SK, etc.).
    # We use broadcast for smaller dimension tables, which is a key optimization.
    
    enriched_df = (
        df_with_join_keys.join(broadcast(dims["users"]), "user_id", "left")
        .join(broadcast(dims["cards"]), "card_id", "left")
        .join(broadcast(dims["merchants"]), "merchant_id", "left")
        .join(broadcast(dims["devices"]), "device_id", "left")
        .join(
            broadcast(dims["date"]),
            col("Date_Join_Key") == dims["date"]["Date_SK"],
            "left",
        )
        .join(
            broadcast(dims["transaction_flags"]),
            col("Flags_Join_Key") == dims["transaction_flags"]["Transaction_Flags_SK"],
            "left",
        )
    )

    return enriched_df

# Final Selection and Renaming for DWH Schema 

def select_and_rename_for_fact(df: DataFrame) -> DataFrame:
    """
    Selects and renames columns from the enriched DataFrame to perfectly match
    the final BigQuery Fact_Transactions table schema.
    """
    print("Finalizing columns for the fact table...")
    
    fact_table_df =  df.select(
        # Primary & Foreign Keys 
        xxhash64(col("transaction_id")).alias("Transaction_SK"),
        col("User_SK").alias("User_SK_FK"),
        col("Card_SK").alias("Card_SK_FK"),
        col("Device_SK").alias("Device_SK_FK"),
        col("Merchant_SK").alias("Merchant_SK_FK"),
        col("Date_SK").alias("Date_SK_FK"),
        
        col("Transaction_Flags_SK").alias("Transaction_Flags_SK_FK"),
        
        # Core Transaction & Context Fields 
        col("transaction_id"),
        col("timestamp").alias("transaction_timestamp"),
        col("amount").cast("double"), # Using double for consistency with BQ FLOAT64
        col("currency"),
        col("ip_address"),
        col("ip_lat").cast("double"),
        col("ip_lon").cast("double"),
        col("user_account_age_days").alias("user_account_age"),
        col("account_balance_before_tx").alias("account_balance"),
        col("time_since_last_user_transaction_s").cast("integer"),
        col("distance_from_home_km").cast("double"),

        # Pre-Calculated Enriched Features
        col("user_avg_tx_amount_30d").cast("double"),
        col("user_max_distance_from_home_90d").cast("double"),
        col("user_num_distinct_countries_6m").cast("integer"),
        col("user_tx_count_24h").cast("integer"),
        col("user_failed_tx_count_1h").cast("integer"),
        col("user_num_distinct_mcc_24h").cast("integer"),
        col("tx_amount_vs_user_avg_ratio").cast("double"),

        # Prediction Result 
        col("is_fraud_prediction").cast("double"),
    )

    return fact_table_df