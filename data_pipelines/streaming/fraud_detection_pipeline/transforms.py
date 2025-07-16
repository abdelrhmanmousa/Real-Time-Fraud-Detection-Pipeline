import os
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List

import apache_beam as beam
import requests
from geopy import distance
from .utils import FirestoreManager


class ParseAndTimestampDoFn(beam.DoFn):
    """Parses a JSON message and assigns its event timestamp."""

    def process(self, element: bytes):
        try:
            msg = json.loads(element.decode("utf-8"))
            ts_str = msg.get("timestamp")
            if not ts_str:
                return

            event_time = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            msg["timestamp"] = event_time
            
            yield beam.window.TimestampedValue(msg, event_time.timestamp())
        except (json.JSONDecodeError, TypeError, ValueError) as e:
            logging.error(
                f"Failed to parse or timestamp message: {element}, Error: {e}"
            )

class PredictFraudBatchDoFn(beam.DoFn):
    def __init__(
        self,
        endpoint_url: str,
        project_id: str,
        use_emulator: bool,
        emulator_host: str = None,
    ):
        self._endpoint_url = endpoint_url
        self._emulator_host = emulator_host
        self._project_id = project_id
        self._session = None
        self.fs_manager = None
        self._use_emulator = use_emulator

    def setup(self) -> None:
        logging.info(
            f"[{id(self)}] SETUP CALLED. Initializing requests.Session for instance."
        )

        self._session = requests.Session()
        logging.info(f"Initialized HTTP session for endpoint: {self._endpoint_url}")

        try:
            if self._use_emulator:
                logging.info(f"--- RUNNING IN EMULATOR MODE on {self._emulator_host} ---")
                os.environ["FIRESTORE_EMULATOR_HOST"] = self._emulator_host
                
                self.fs_manager = FirestoreManager(project_id=self._project_id, emulator_host=self._emulator_host)
            else:
                logging.info(f"--- RUNNING IN PRODUCTION MODE ---")
                self.fs_manager = FirestoreManager(project_id=self._project_id)

            logging.info(
                f"[{id(self)}] SETUP: Firestore client connected successfully."
            )
            self.fs_manager.connect()

        except Exception as e:
            logging.error(f"[{id(self)}] SETUP: Failed to connect to Firestore: {e}")
            raise

    def process(self, transaction_batch: tuple):
        logging.info(f"Processing a batch !!")
        transaction_batch = transaction_batch[1]
        if not self.fs_manager:
            logging.error("DatabaseManager not initialized. Cannot process batch.")
            return

        if not transaction_batch:
            return

        # 1. Collect all unique customer_ids from the batch.
        # Using a set is efficient for ensuring uniqueness.
        user_ids = list(
            {tx.get("user_id") for tx in transaction_batch if tx.get("user_id")}
        )
        merchants_ids = list(
            {tx.get("merchant_id") for tx in transaction_batch if tx.get("merchant_id")}
        )

        if not user_ids:
            # If no transactions have a user_id, just yield them back
            for tx in transaction_batch:
                yield tx
            return

        users_data = self.fs_manager.batch_read(
            collection_name="users",
            lookup_ids=user_ids,
        )
        merchants_data = self.fs_manager.batch_read(
            collection_name="merchants",
            lookup_ids=merchants_ids,
        )
        # print(user_ids)
        # print(users_data)
        enriched_transactions = []
        for tx in transaction_batch:
            enrichment_user_data = users_data[tx["user_id"]]
            enrichment_merchant_data = merchants_data[tx["merchant_id"]]
            enriched_transaction = self._enrich_transaction(
                tx, enrichment_user_data, enrichment_merchant_data
            )
            enriched_transactions.append(enriched_transaction)

        sanitized_batch_for_api = []

        for transaction in enriched_transactions:
            sanitized_transaction = transaction.copy()
            for key, value in sanitized_transaction.items():
                # Check if the value is a datetime object
                if isinstance(value, datetime):
                    # Convert it to a standard ISO 8601 string
                    sanitized_transaction[key] = value.isoformat()
            sanitized_batch_for_api.append(sanitized_transaction)

        try:
            payload = {"transactions": sanitized_batch_for_api}
            response = self._session.post(self._endpoint_url, json=payload, timeout=30)
            response.raise_for_status()
            predictions = response.json()["predictions"]
            predictions_map = {pred["transaction_id"]: pred for pred in predictions}
            logging.info(f">>> HTTP request to model server Succeeded !!!!")
            
            for transaction in enriched_transactions:
                transaction_id = transaction.get("transaction_id")
                prediction_result = predictions_map.get(transaction_id)
                if prediction_result:
                    transaction["is_fraud_prediction"] = prediction_result.get(
                        "fraud_score"
                    )
                else:
                    transaction["is_fraud_prediction"] = -1

                yield transaction

        except requests.exceptions.RequestException as e:
            logging.error(f"HTTP request to model server failed: {e}")
        except Exception as e:
            logging.error(f"An unexpected error occurred in PredictFraudBatchDoFn: {e}")

    def _enrich_transaction(self, tx, user_data, merchant_data):
        now = datetime.now(timezone.utc)
        geo_time_data = {}
        card_data = {}

        user_cards = user_data["cards"]
        for card in user_cards:
            if card["card_id"] == tx["card_id"]:
                card_data = card
        geo_time_data["distance_from_home_km"] = distance.geodesic(
            (user_data["user_home_lat"], user_data["user_home_long"]),
            (merchant_data["merchant_lat"], merchant_data["merchant_lon"]),
        ).kilometers

        if type(tx["timestamp"]) == str:
            timestamp = datetime.fromisoformat(tx["timestamp"])
        else:
            timestamp = tx["timestamp"]

        if type(user_data["last_transaction_timestamp"]) == str:
            last_tx_timestamp = datetime.fromisoformat(
                user_data["last_transaction_timestamp"]
            )
        else:
            last_tx_timestamp = user_data["last_transaction_timestamp"]

        geo_time_data["is_geo_ip_mismatch"] = (
            tx["ip_country"] == merchant_data["country_code"]
        )

        geo_time_data["is_foreign_country_tx"] = (
            tx["ip_country"] == user_data["user_home_country"]
        )

        geo_time_data["is_weekend_transaction"] = timestamp.weekday() in [
            4,
            5,
        ]  # Friday, satarday

        geo_time_data["is_night_transaction"] = 0 <= timestamp.hour < 6

        geo_time_data["time_since_last_user_transaction_s"] = (
            now - last_tx_timestamp
        ).seconds

        user_data["tx_amount_to_balance_ratio"] = (
            tx["amount"] / user_data["account_balance_before_tx"]
        )
        user_data["tx_amount_vs_user_avg_ratio"] = (
            tx["amount"] / user_data["user_avg_tx_amount_30d"]
        )

        user_devices = [dev["device_id"] for dev in user_data["devices"]]

        is_new_device = tx["device_id"] not in user_devices

        encriched_transaction = {
            **tx,
            **user_data,
            **card_data,
            **geo_time_data,
            **merchant_data,
            "is_new_device": is_new_device,
        }

        return encriched_transaction

    def teardown(self):
        """
        Closes the MongoDB client connection when the DoFn instance is torn down.
        """
        if self.fs_manager:
            logging.info(f"[{id(self)}] TEARDOWN: Closing Firestore client connection.")
            self.fs_manager.close()


class AttachEventTimestampDoFn(beam.DoFn):
    """Safely re-applies an event timestamp from a dictionary field."""

    def process(self, element: Dict[str, Any]):
        event_time = element.get("timestamp")
        if isinstance(event_time, datetime):
            yield beam.window.TimestampedValue(element, event_time.timestamp())
