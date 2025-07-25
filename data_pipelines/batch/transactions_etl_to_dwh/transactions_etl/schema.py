from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DoubleType, 
    IntegerType, BooleanType, LongType
)

# Schema for the raw transaction data from GCS Parquet files.
# Defining this schema makes your job more robust and efficient.
raw_transactions_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("entry_mode", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("merchant_lat", DoubleType(), True),
    StructField("merchant_lon", DoubleType(), True),
    StructField("user_id", StringType(), True),
    # StructField("user_home_lat", DoubleType(), True),
    # StructField("user_home_lon", DoubleType(), True),
    StructField("user_account_age_days", IntegerType(), True),
    StructField("card_id", StringType(), True),
    StructField("card_type", StringType(), True),
    StructField("card_country", StringType(), True),
    StructField("card_brand", StringType(), True),
    StructField("issuing_bank", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("ip_lat", DoubleType(), True),
    StructField("ip_lon", DoubleType(), True),
    StructField("device_id", StringType(), True),
    StructField("is_weekend_transaction", BooleanType(), True),
    StructField("is_night_transaction", BooleanType(), True),
    StructField("time_since_last_user_transaction_s", DoubleType(), True),
    StructField("distance_from_home_km", DoubleType(), True),
    StructField("is_geo_ip_mismatch", BooleanType(), True),
    StructField("is_foreign_country_tx", BooleanType(), True),
    StructField("user_avg_tx_amount_30d", DoubleType(), True),
    StructField("account_balance_before_tx", DoubleType(), True),
    StructField("tx_amount_to_balance_ratio", DoubleType(), True),
    StructField("user_max_distance_from_home_90d", DoubleType(), True),
    StructField("user_num_distinct_countries_6m", IntegerType(), True),
    StructField("user_tx_count_24h", IntegerType(), True),
    StructField("user_failed_tx_count_1h", IntegerType(), True),
    StructField("user_num_distinct_mcc_24h", IntegerType(), True),
    StructField("is_new_device", BooleanType(), True),
    StructField("was_3ds_successful", BooleanType(), True),
    StructField("tx_amount_vs_user_avg_ratio", DoubleType(), True),
    StructField("is_fraud_prediction", DoubleType(), True),
    StructField("processing_timestamp", StringType(), True),
    StructField("partition_key", StringType(), True)
])


final_bq_schema = StructType([
    # Keys 
    # Primary keys are never null.
    StructField("Transaction_SK", LongType(), False),
    # Foreign keys from LEFT joins can be null.
    StructField("User_SK_FK", LongType(), True),
    StructField("Card_SK_FK", LongType(), True),
    StructField("Device_SK_FK", LongType(), True),
    StructField("Merchant_SK_FK", LongType(), True),
    StructField("Date_SK_FK", IntegerType(), True),
    StructField("Transaction_Flags_SK_FK", LongType(), True),

    #  Core Fields 
    # The problematic field, now explicitly non-nullable.
    StructField("transaction_id", StringType(), True), # Assuming this can be nullable
    StructField("transaction_timestamp", TimestampType(), False),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("ip_lat", DoubleType(), True),
    StructField("ip_lon", DoubleType(), True),
    StructField("user_account_age", LongType(), True),
    StructField("account_balance", DoubleType(), True),
    StructField("time_since_last_user_transaction_s", IntegerType(), True),
    StructField("distance_from_home_km", DoubleType(), True),

    #features 
    StructField("user_avg_tx_amount_30d", DoubleType(), True),
    StructField("user_max_distance_from_home_90d", DoubleType(), True),
    StructField("user_num_distinct_countries_6m", IntegerType(), True),
    StructField("user_tx_count_24h", IntegerType(), True),
    StructField("user_failed_tx_count_1h", IntegerType(), True),
    StructField("user_num_distinct_mcc_24h", IntegerType(), True),
    StructField("tx_amount_vs_user_avg_ratio", DoubleType(), True),

    #prediction 
    StructField("is_fraud_prediction", DoubleType(), True),
])
