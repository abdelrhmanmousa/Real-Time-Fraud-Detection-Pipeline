from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, trim, to_date, date_format, broadcast, xxhash64

def clean_transaction_data(df: DataFrame) -> DataFrame:
    """
    Performs comprehensive cleaning of the raw transaction data.
    """
    print("Performing comprehensive data cleaning...")
    
    # 1. Handle Nulls & Duplicates
    # Drop records with null critical identifiers, as they cannot be joined.
    # Also, remove duplicate transactions.
    cleaned_df = df.filter(
        col("transaction_id").isNotNull() &
        col("user_id").isNotNull() &
        col("card_id").isNotNull() &
        col("merchant_id").isNotNull()
    ).dropDuplicates(['transaction_id'])

    # 2. String Cleaning
    # Trim whitespace from identifiers to prevent join failures.
    cleaned_df = cleaned_df.withColumn("user_id", trim(col("user_id"))) \
                           .withColumn("card_id", trim(col("card_id"))) \
                           .withColumn("device_id", trim(col("device_id"))) \
                           .withColumn("merchant_id", trim(col("merchant_id")))

    # 3. Value Validation & Correction
    # Ensure transaction amount is not negative.
    # Fill nulls for key numeric/boolean fields with sensible defaults.
    cleaned_df = cleaned_df.withColumn(
        "amount", when(col("amount") < 0, 0).otherwise(col("amount"))
    ).fillna({
        'amount': 0.0,
        'time_since_last_user_transaction_s': 0.0,
        'distance_from_home_km': 0.0,
        'account_balance_before_tx': 0.0,
        'is_weekend_transaction': False,
        'is_night_transaction': False,
        'is_geo_ip_mismatch': False,
        'is_foreign_country_tx': False,
        'is_new_device': False,
        'was_3ds_successful': False
    })
    
    return cleaned_df

def enrich_with_dimensions(transactions_df: DataFrame, dims: dict) -> DataFrame:
    """
    Enriches transaction data with primary dimension surrogate keys.
    """
    print("Enriching data with primary dimension keys...")
    
    # Prepare date and time keys for joining
    df_with_join_keys = transactions_df \
        .withColumn("Date_Join_Key", date_format(to_date(col("timestamp")), "yyyyMMdd").cast("integer")) \
        .withColumn("Time_Join_Key", date_format(col("timestamp"), "HHmmss").cast("integer"))

    # Join with dimension tables. Using broadcast for smaller tables is a key optimization.
    enriched_df = df_with_join_keys \
        .join(broadcast(dims['users']), "user_id", "left") \
        .join(broadcast(dims['cards']), "card_id", "left") \
        .join(broadcast(dims['merchants']), "merchant_id", "left") \
        .join(broadcast(dims['devices']), "device_id", "left") \
        .join(broadcast(dims['date']), col("Date_Join_Key") == dims['date']['Date_SK'], "left") \
        .join(broadcast(dims['time']), col("Time_Join_Key") == dims['time']['Time_SK'], "left") \
        .join(broadcast(dims['flags']), [
            transactions_df.is_weekend_transaction == dims['flags'].is_weekend,
            transactions_df.is_night_transaction == dims['flags'].is_night,
            transactions_df.is_geo_ip_mismatch == dims['flags'].is_geo_ip_mismatch,
            transactions_df.is_foreign_country_tx == dims['flags'].is_foreign_country,
            transactions_df.is_new_device == dims['flags'].is_new_device,
            transactions_df.was_3ds_successful == dims['flags'].was_3ds_successful
        ], "left")
        
    return enriched_df

def select_and_rename_for_fact(df: DataFrame) -> DataFrame:
    """Selects and renames columns to match the final fact table schema, incorporating the junk dimension."""
    print("Finalizing columns for the fact table...")
    return df.select(
        col("Card_sk").alias("Card_SK_FK"),
        col("User_SK").alias("User_SK_FK"),
        col("Merchant_sk").alias("Merchant_SK_FK"),
        col("Device_SK").alias("Device_SK_FK"),
        col("Date_SK").alias("Date_SK_FK"),
        col("Time_SK").alias("Time_SK_FK"),
        col("Transaction_Flags_SK").alias("Transaction_Flags_SK_FK"),
        "transaction_id",
        col("amount").cast("float"),
        "currency",
        "ip_address",
        col("ip_lat").cast("float"),
        col("ip_lon").cast("float"),
        col("user_account_age_days").alias("user_account_age").cast("integer"),
        col("account_balance_before_tx").alias("account_balance").cast("float"),
        col("time_since_last_user_transaction_s").cast("integer"),
        "distance_from_home_km",
        "user_avg_tx_amount_30d",
        "user_max_distance_from_home_90d",
        "user_num_distinct_countries_6m",
        "user_tx_count_24h",
        "user_failed_tx_count_1h",
        "user_num_distinct_mcc_24h",
        "tx_amount_vs_user_avg_ratio",
        "is_fraud_prediction"
    ).withColumn("Transaction_SK", xxhash64(col("transaction_id")))