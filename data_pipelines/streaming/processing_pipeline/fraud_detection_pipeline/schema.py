import pyarrow as pa

"""
This schema is designed for Parquet format.
"""

# Define the fields for the schema, grouped for readability
transaction_schema = pa.schema([
    # Basic Transaction Details
    pa.field("transaction_id", pa.string(), nullable=False, metadata={"description": "A unique identifier for this specific transaction event."}),
    pa.field("timestamp", pa.timestamp('us', tz='UTC'), nullable=False, metadata={"description": "The UTC timestamp of the transaction."}),
    pa.field("amount", pa.float64(), nullable=False, metadata={"description": "The monetary value of the transaction."}),
    pa.field("currency", pa.string(), nullable=False, metadata={"description": "The currency code (e.g., 'EGP', 'USD', 'EUR')."}),
    pa.field("country_code", pa.string(), nullable=False, metadata={"description": "The ISO code of the country where the transaction occurred."}),
    pa.field("entry_mode", pa.string(), nullable=False, metadata={"description": "The method used to enter card details (e.g., 'Chip', 'Online')."}),
    
    # Merchant Details
    pa.field("merchant_id", pa.string(), nullable=False, metadata={"description": "The unique identifier for the merchant."}),
    pa.field("merchant_category", pa.string(), nullable=False, metadata={"description": "The Merchant Category Code (MCC)."}),
    pa.field("merchant_lat", pa.float64(), nullable=False, metadata={"description": "The latitude of the merchant's location."}),
    pa.field("merchant_lon", pa.float64(), nullable=False, metadata={"description": "The longitude of the merchant's location."}),
    
    # User Details
    pa.field("user_id", pa.string(), nullable=False, metadata={"description": "The unique identifier for the user."}),
    pa.field("user_account_age_days", pa.int32(), nullable=False, metadata={"description": "The age of the user's account in days."}),
    
    # Card Details
    pa.field("card_id", pa.string(), nullable=False, metadata={"description": "The unique identifier for the payment card."}),
    pa.field("card_type", pa.string(), nullable=False, metadata={"description": "The type of card (e.g., 'Credit', 'Debit')."}),
    pa.field("card_country", pa.string(), nullable=False, metadata={"description": "The country where the card was issued."}),
    pa.field("card_brand", pa.string(), nullable=False, metadata={"description": "The brand of the card (e.g., 'Visa', 'Mastercard')."}),
    pa.field("issuing_bank", pa.string(), nullable=False, metadata={"description": "The name of the issuing bank."}),
    
    # Geolocation and Device
    pa.field("ip_address", pa.string(), nullable=False, metadata={"description": "The IP address from which the transaction originates."}),
    pa.field("ip_lat", pa.float64(), nullable=False, metadata={"description": "The latitude associated with the IP address."}),
    pa.field("ip_lon", pa.float64(), nullable=False, metadata={"description": "The longitude associated with the IP address."}),
    pa.field("device_id", pa.string(), nullable=False, metadata={"description": "The identifier for the user's device."}),

    # Derived Transaction Features
    pa.field("is_weekend_transaction", pa.bool_(), nullable=False, metadata={"description": "True if the transaction occurred on a Saturday or Sunday."}),
    pa.field("is_night_transaction", pa.bool_(), nullable=False, metadata={"description": "True if the transaction occurred during local night hours (12 AM - 6 AM)."}),
    pa.field("time_since_last_user_transaction_s", pa.float64(), nullable=False, metadata={"description": "Time in seconds since this user's last transaction."}),
    pa.field("distance_from_home_km", pa.float64(), nullable=False, metadata={"description": "Distance between transaction and user's home (in km)."}),
    pa.field("is_geo_ip_mismatch", pa.bool_(), nullable=False, metadata={"description": "True if the IP country differs from the merchant country."}),
    pa.field("is_foreign_country_tx", pa.bool_(), nullable=False, metadata={"description": "True if transaction country is different from user's home country."}),
    
    # User Historical / Contextual Features
    pa.field("user_avg_tx_amount_30d", pa.float64(), nullable=False, metadata={"description": "User's average transaction amount over the last 30 days."}),
    pa.field("account_balance_before_tx", pa.float64(), nullable=False, metadata={"description": "User's account balance before the transaction."}),
    pa.field("tx_amount_to_balance_ratio", pa.float64(), nullable=False, metadata={"description": "Ratio of `amount / account_balance_before_tx`."}),
    pa.field("user_max_distance_from_home_90d", pa.float64(), nullable=False, metadata={"description": "Max distance from home the user has transacted from in 90 days."}),
    pa.field("user_num_distinct_countries_6m", pa.int32(), nullable=False, metadata={"description": "Number of distinct countries the user has transacted from in 6 months."}),
    pa.field("user_tx_count_24h", pa.int32(), nullable=False, metadata={"description": "Number of transactions this user has made in the last 24 hours."}),
    pa.field("user_failed_tx_count_1h", pa.int32(), nullable=False, metadata={"description": "Number of failed transactions by this user in the past hour."}),
    pa.field("user_num_distinct_mcc_24h", pa.int32(), nullable=False, metadata={"description": "Number of unique MCCs this user has used in 24 hours."}),
    #----
    # Authentication and Security Features
    pa.field("is_new_device", pa.bool_(), nullable=False, metadata={"description": "True if this is the first time the user has used this device."}),
    pa.field("was_3ds_successful", pa.bool_(), nullable=False, metadata={"description": "True if 3D Secure authentication was attempted and passed."}),
    
    # Final Ratio Feature
    pa.field("tx_amount_vs_user_avg_ratio", pa.float64(), nullable=False, metadata={"description": "Ratio of `amount / user_avg_tx_amount_30d`."}),

    # Fraud Score
    pa.field("is_fraud_prediction", pa.float64(), nullable=False, metadata={"description": "The model prediction score."}),
    pa.field('processing_timestamp', pa.string()),
    pa.field('partition_key', pa.string())
])
