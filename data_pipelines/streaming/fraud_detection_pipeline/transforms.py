import os
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple

import apache_beam as beam
import requests
from geopy import distance
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .utils import FirestoreManager, UpdateOperation

main_output_tag = 'parsed_transactions'
failed_parse_tag = 'failed_parses'
failed_processing_tag = 'failed_batches'


class ParseAndTimestampDoFn(beam.DoFn):
    """
    Parses a JSON message, validates its structure, and assigns its event timestamp.
    
    Messages that fail parsing are routed to a dead-letter output.
    """

    def process(self, element: bytes):
        """
        Processes an incoming raw message.

        Yields:
            A TimestampedValue for successfully parsed messages to the main output.
            A raw bytes element for failed messages to the 'failed_parses' side output.
        """
        try:
            msg = json.loads(element.decode("utf-8"))
            
            ts_str = msg.get("timestamp")
            if not ts_str or not isinstance(ts_str, str):
                logging.warning(f"Message missing or has invalid timestamp: {element}")
                yield beam.pvalue.TaggedOutput(failed_parse_tag, element)
                return

            # The replace() call makes it compatible with Python's fromisoformat
            event_time = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            msg["timestamp"] = event_time
            
            yield beam.window.TimestampedValue(msg, event_time.timestamp())

        except (json.JSONDecodeError, TypeError, ValueError) as e:
            logging.error(f"Failed to parse or timestamp message: {element}, Error: {e}")
            yield beam.pvalue.TaggedOutput(failed_parse_tag, element)


class PredictFraudBatchDoFn(beam.DoFn):
    """
    Enriches a batch of transactions with user/merchant data from Firestore,
    sends them to a prediction model, and handles the results.
    """
    def __init__(
        self,
        endpoint_url: str,
        project_id: str,
        use_emulator: bool,
        emulator_host: str = None,
    ):
        self._endpoint_url = endpoint_url
        self._project_id = project_id
        self._use_emulator = use_emulator
        self._emulator_host = emulator_host
        self._session = None
        self.fs_manager = None

    def setup(self) -> None:
        """Initializes non-serializable resources: a session with retries and a Firestore client."""
        logging.info(f"[{id(self)}] SETUP: Initializing resources.")

        # Configure a requests Session with robust retry logic for HTTP calls
        self._session = requests.Session()
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],  # Retry on server errors and throttling
            backoff_factor=1
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)

        # Initialize FirestoreManager
        try:
            if self._use_emulator:
                logging.info(f"--- RUNNING IN EMULATOR MODE on {self._emulator_host} ---")
                os.environ["FIRESTORE_EMULATOR_HOST"] = self._emulator_host
                self.fs_manager = FirestoreManager(project_id=self._project_id, emulator_host=self._emulator_host)
            else:
                logging.info(f"--- RUNNING IN PRODUCTION MODE ---")
                self.fs_manager = FirestoreManager(project_id=self._project_id)

            self.fs_manager.connect()
            logging.info(f"[{id(self)}] SETUP: Firestore client connected successfully.")

        except Exception as e:
            logging.critical(f"[{id(self)}] SETUP: Failed to connect to Firestore. This is a fatal error.", exc_info=True)
            raise

    def process(self, batch_with_key: Tuple):
        """
        Processes a batch of transactions.
        
        Args:
            batch_with_key: A tuple where the second element is the list of transactions.
        """
        transaction_batch = batch_with_key[1]
        
        if not self.fs_manager:
            logging.error("FirestoreManager not initialized. Cannot process batch.")
            yield beam.pvalue.TaggedOutput(failed_processing_tag, transaction_batch)
            return

        if not transaction_batch:
            return

        # 1. Collect unique IDs for efficient batch reading from Firestore
        user_ids = list({tx.get("user_id") for tx in transaction_batch if tx.get("user_id")})
        merchant_ids = list({tx.get("merchant_id") for tx in transaction_batch if tx.get("merchant_id")})

        # 2. Batch read data from Firestore
        users_data = self.fs_manager.batch_read(collection_name="users", lookup_ids=user_ids)
        merchants_data = self.fs_manager.batch_read(collection_name="merchants", lookup_ids=merchant_ids)

        # 3. Enrich transactions, handling cases where enrichment data might be missing
        enriched_transactions = []
        for tx in transaction_batch:
            user_data = users_data.get(tx.get("user_id"))
            merchant_data = merchants_data.get(tx.get("merchant_id"))

            if not user_data or not merchant_data:
                logging.warning(f"Skipping transaction {tx.get('transaction_id')} due to missing user/merchant data.")
                continue  # Or yield to a separate "failed_enrichment" output

            # Pass a copy of user_data to prevent mutation issues within the batch
            enriched_tx = self._enrich_transaction(tx, user_data.copy(), merchant_data)
            enriched_transactions.append(enriched_tx)
        
        if not enriched_transactions:
            logging.warning("No transactions could be enriched in this batch.")
            return

        # 4. Prepare batch for API and make prediction request
        try:
            # Sanitize datetime objects to ISO strings for JSON serialization
            sanitized_batch_for_api = [self._sanitize_for_json(tx) for tx in enriched_transactions]
            
            payload = {"transactions": sanitized_batch_for_api} # Common API format
            
            response = self._session.post(self._endpoint_url, json=payload, timeout=30)
            response.raise_for_status() # Raises HTTPError for bad responses (4xx or 5xx)
            
            predictions = response.json().get("predictions", [])
            predictions_map = {pred.get("transaction_id"): pred for pred in predictions}
            logging.info(f"Successfully received {len(predictions)} predictions from model server.")

            # 5. Merge predictions and yield results
            for transaction in enriched_transactions:
                tx_id = transaction.get("transaction_id")
                pred_result = predictions_map.get(tx_id)
                
                if pred_result:
                    transaction["is_fraud_prediction"] = pred_result.get("fraud_score", -1.0)
                else:
                    transaction["is_fraud_prediction"] = -1.0 # Default value for missing prediction

                yield transaction
            
            # 6. After successful processing, update user timestamps in Firestore
            self._update_user_timestamps(enriched_transactions)

        except requests.exceptions.RequestException as e:
            logging.error(f"HTTP request to model server failed after retries: {e}")
            yield beam.pvalue.TaggedOutput(failed_processing_tag, transaction_batch)
        except Exception as e:
            logging.error(f"An unexpected error occurred in PredictFraudBatchDoFn: {e}", exc_info=True)
            yield beam.pvalue.TaggedOutput(failed_processing_tag, transaction_batch)

    def _enrich_transaction(self, tx: Dict, user_data: Dict, merchant_data: Dict) -> Dict:
        """Enriches a single transaction with derived features. This function is now safe from side effects."""
        timestamp = tx["timestamp"] # Assumes this is already a datetime object

        # --- Geo-Time Features ---
        geo_time_data = {}
        user_home_coords = (user_data.get("user_home_lat"), user_data.get("user_home_long"))
        merchant_coords = (merchant_data.get("merchant_lat"), merchant_data.get("merchant_lon"))

        if all(user_home_coords) and all(merchant_coords):
            geo_time_data["distance_from_home_km"] = distance.geodesic(user_home_coords, merchant_coords).kilometers
        
        last_tx_ts_raw = user_data.get("last_transaction_timestamp")
        last_tx_ts = datetime.fromisoformat(last_tx_ts_raw) if isinstance(last_tx_ts_raw, str) else last_tx_ts_raw
        
        if last_tx_ts:
            # CORRECT: Calculate delta between event timestamps using total_seconds()
            geo_time_data["time_since_last_user_transaction_s"] = (timestamp - last_tx_ts).total_seconds()

        geo_time_data["is_geo_ip_mismatch"] = tx.get("ip_country") != merchant_data.get("country_code")
        geo_time_data["is_foreign_country_tx"] = tx.get("ip_country") != user_data.get("user_home_country")
        geo_time_data["is_weekend_transaction"] = timestamp.weekday() in [5,4]  # Saturday (5), friday (4)
        geo_time_data["is_night_transaction"] = 0 <= timestamp.hour < 6

        # --- User/Account Features ---
        # SAFE: Check for division by zero
        balance = user_data.get("account_balance_before_tx", 0)
        user_avg = user_data.get("user_avg_tx_amount_30d", 0)
        user_data["tx_amount_to_balance_ratio"] = (tx["amount"] / balance) if balance > 0 else 0
        user_data["tx_amount_vs_user_avg_ratio"] = (tx["amount"] / user_avg) if user_avg > 0 else 0

        # --- Device and Card Features ---
        # SAFE: Use .get() to avoid mutating user_data with .pop()
        user_devices = [dev.get("device_id") for dev in user_data.get("devices", [])]
        is_new_device = tx.get("device_id") not in user_devices if tx.get("device_id") else False
        
        card_data = {}
        for card in user_data.get("cards", []):
            if card.get("card_id") == tx.get("card_id"):
                card_data = card
                break
        
        # Clean up data before merging to avoid redundant or sensitive fields
        user_data.pop("cards", None)
        user_data.pop("devices", None)

        return {**tx, **user_data, **card_data, **geo_time_data, **merchant_data, "is_new_device": is_new_device}

    def _update_user_timestamps(self, successful_transactions: List[Dict]):
        """
        Correctly updates the last transaction timestamp for each user based on the
        latest transaction within the processed batch.
        """
        latest_user_tx = {}
        for tx in successful_transactions:
            user_id = tx.get("user_id")
            if not user_id:
                continue
            
            # Find the newest timestamp for a given user_id in this batch
            current_ts = tx.get("timestamp")
            if user_id not in latest_user_tx or current_ts > latest_user_tx[user_id]:
                latest_user_tx[user_id] = current_ts
        
        # Create Firestore update operations. This assumes your utility can handle
        # a dictionary specifying the fields to update.
        # write_ops = []
        data = [{"user_id":user_id,"last_transaction_timestamp": ts} for user_id, ts in latest_user_tx.items()]
        # for user_id, ts in latest_user_tx.items():
        #     data
        #     op = UpdateOperation(doc_ref=user_id, data={"last_transaction_timestamp": ts})
        #     write_ops.append(op)

        write_ops = self.fs_manager.create_batch_update_operations_from_list(
            "users",
            "user_id",
            data
        )
        if write_ops:
            self.fs_manager.batch_write(operations=write_ops)
            logging.info(f"Updated 'last_transaction_timestamp' for {len(write_ops)} users.")

    def _sanitize_for_json(self, transaction: Dict) -> Dict:
        """Creates a sanitized copy of a dictionary, converting datetimes to ISO strings."""
        sanitized = transaction.copy()
        for key, value in sanitized.items():
            if isinstance(value, datetime):
                sanitized[key] = value.isoformat()
        return sanitized

    def teardown(self):
        """Closes external connections."""
        if self.fs_manager:
            logging.info(f"[{id(self)}] TEARDOWN: Closing Firestore client connection.")
            self.fs_manager.close()


class AttachEventTimestampDoFn(beam.DoFn):
    """
    Safely re-applies an event timestamp from a dictionary field after processing.
    This ensures the element's timestamp is maintained across steps that might lose it.
    """
    def process(self, element: Dict[str, Any]):
        event_time = element.get("timestamp")
        if isinstance(event_time, datetime):
            yield beam.window.TimestampedValue(element, event_time.timestamp())
        else:
            # If timestamp is missing or invalid, yield with current time to avoid crashing.
            # This could also be routed to another dead-letter queue.
            logging.warning(f"Element missing valid datetime object for timestamping: {element.get('transaction_id')}")
            yield element