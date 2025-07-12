from typing import List
from datetime import datetime
from ipaddress import IPv4Address
from pydantic import BaseModel, Field


class TransactionInput(BaseModel):
    """
    Defines the schema for a single transaction record, including raw data and
    pre-computed engineered features, ready for the model to score.
    """
    transaction_id: str = Field(..., description="A unique identifier for this specific transaction event.")
    timestamp: datetime = Field(..., description="The UTC timestamp of the transaction.")
    amount: float = Field(..., gt=0, description="The monetary value of the transaction.")
    currency: str = Field(..., description="The currency code (e.g., 'EGP', 'USD', 'EUR').")
    country_code: str = Field(..., description="The ISO code of the country where the transaction occurred.")
    entry_mode: str = Field(..., description="The method used to enter card details (e.g., 'Chip', 'Online').")
    
    merchant_id: str = Field(..., description="The unique identifier for the merchant.")
    merchant_category: str = Field(..., description="The Merchant Category Code (MCC).")
    merchant_lat: float = Field(..., ge=-90, le=90, description="The latitude of the merchant's location.")
    merchant_lon: float = Field(..., ge=-180, le=180, description="The longitude of the merchant's location.")
    
    user_id: str = Field(..., description="The unique identifier for the user.")
    user_account_age_days: int = Field(..., ge=0, description="The age of the user's account in days.")
    
    card_id: str = Field(..., description="The unique identifier for the payment card.")
    card_type: str = Field(..., description="The type of card (e.g., 'Credit', 'Debit').")
    card_country: str = Field(..., description="The country where the card was issued.")
    card_brand: str = Field(..., description="The brand of the card (e.g., 'Visa', 'Mastercard').")
    issuing_bank: str = Field(..., description="The name of the issuing bank.")
    
    ip_address: IPv4Address = Field(..., description="The IP address from which the transaction originates.")
    ip_lat: float = Field(..., ge=-90, le=90, description="The latitude associated with the IP address.")
    ip_lon: float = Field(..., ge=-180, le=180, description="The longitude associated with the IP address.")
    device_id: str = Field(..., description="The identifier for the user's device.")

    is_weekend_transaction: bool = Field(..., description="True if the transaction occurred on a Saturday or Sunday.")
    is_night_transaction: bool = Field(..., description="True if the transaction occurred during local night hours (12 AM - 6 AM).")
    time_since_last_user_transaction_s: float = Field(..., ge=0, description="Time in seconds since this user's last transaction.")
    
    distance_from_home_km: float = Field(..., ge=0, description="Distance between transaction and user's home (in km).")
    is_geo_ip_mismatch: bool = Field(..., description="True if the IP country differs from the merchant country.")
    is_foreign_country_tx: bool = Field(..., description="True if transaction country is different from user's home country.")
    
    user_avg_tx_amount_30d: float = Field(..., ge=0, description="User's average transaction amount over the last 30 days.")
    account_balance_before_tx: float = Field(..., ge=0, description="User's account balance before the transaction.")
    tx_amount_to_balance_ratio: float = Field(..., ge=0, description="Ratio of `amount / account_balance_before_tx`.")

    user_max_distance_from_home_90d: float = Field(..., ge=0, description="Max distance from home the user has transacted from in 90 days.")
    user_num_distinct_countries_6m: int = Field(..., ge=0, description="Number of distinct countries the user has transacted from in 6 months.")
    user_tx_count_24h: int = Field(..., ge=0, description="Number of transactions this user has made in the last 24 hours.")
    user_failed_tx_count_1h: int = Field(..., ge=0, description="Number of failed transactions by this user in the past hour.")
    user_num_distinct_mcc_24h: int = Field(..., ge=0, description="Number of unique MCCs this user has used in 24 hours.")
    
    is_new_device: bool = Field(..., description="True if this is the first time the user has used this device.")
    was_3ds_successful: bool = Field(..., description="True if 3D Secure authentication was attempted and passed.")
    
    tx_amount_vs_user_avg_ratio: float = Field(..., ge=0, description="Ratio of `amount / user_avg_tx_amount_30d`.")

    class Config:
        """Pydantic model configuration."""
        json_schema_extra = {
            "example": {
                "transaction_id": "txn_cf8387a3-832f-4835-b4fe-7756f75601c7",
                "timestamp": "2025-03-15T22:30:00Z",
                "amount": 2550.75,
                "currency": "EGP",
                "country_code": "DE",
                "entry_mode": "Online",
                "merchant_id": "merch_a1b2c3d4e5f6",
                "merchant_category": "5944",
                "merchant_lat": 52.5200,
                "merchant_lon": 13.4050,
                "user_id": "usr_c8a7f6b5",
                "user_account_age_days": 450,
                "card_id": "tok_1a2b3c4d5e6f",
                "card_type": "Credit",
                "card_country": "EG",
                "ip_address": "94.112.23.10",
                "ip_lat": 9.0765,
                "ip_lon": 7.3986,
                "device_id": "dev_new_fraud_device_123",
                "is_weekend_transaction": True,
                "is_night_transaction": True,
                "time_since_last_user_transaction_s": 2592000.0,
                "distance_from_home_km": 4471.12,
                "is_geo_ip_mismatch": True,
                "is_foreign_country_tx": True,
                "user_avg_tx_amount_30d": 520.50,
                "user_max_distance_from_home_90d": 15.2,
                "user_num_distinct_countries_6m": 1,
                "user_tx_count_24h": 5,
                "user_failed_tx_count_1h": 2,
                "user_num_distinct_mcc_24h": 4,
                "is_new_device": True,
                "was_3ds_successful": False,
                "tx_amount_vs_user_avg_ratio": 4.90
            }
        }


class PredictionInput(BaseModel):
    """
    Defines the schema for a batch of transactions sent to the model for prediction.
    """
    transactions: List[TransactionInput] = Field(..., description="A list of transaction records to be scored.")


# Output Schemas for Prediction

class PredictionOutput(BaseModel):
    """
    Defines the schema for the prediction result of a single transaction.
    """
    transaction_id: str = Field(..., description="The unique identifier of the original transaction.")
    fraud_score: float = Field(..., ge=0.0, le=1.0, description="The predicted fraud score, ranging from 0.0 (low risk) to 1.0 (high risk).")
    
    class Config:
        """Pydantic model configuration."""
        json_schema_extra = {
            "example": {
                "transaction_id": "txn_cf8387a3-832f-4835-b4fe-7756f75601c7",
                "fraud_score": 0.92
            }
        }

class BatchPredictionOutput(BaseModel):
    """
    Defines the schema for the batch prediction results.
    """
    predictions: List[PredictionOutput] = Field(..., description="A list of prediction results for each transaction in the input batch.")
