import base64
import json
import logging
import os
from typing import List, Dict, Any

from fastapi import FastAPI, Request, Response
from pydantic import BaseModel, Field

import google.cloud.logging

logging_client = google.cloud.logging.Client()
logging_client.setup_logging()


# Configuration 
# You can load this from an environment variable or a config service in a real app
FRAUD_THRESHOLD = 0.75



# Define the structure of the inner message from Pub/Sub
class PubSubMessage(BaseModel):
    data: bytes
    attributes: Dict[str, Any] = Field(default_factory=dict)

# Define the overall structure of the push request from Pub/Sub
class PubSubEnvelope(BaseModel):
    message: PubSubMessage
    subscription: str



app = FastAPI()


@app.post("/")
async def process_pubsub_batch(envelope: PubSubEnvelope):
    """
    Receives and processes a batch of transactions from a Pub/Sub push subscription.
    FastAPI uses the PubSubEnvelope model to automatically validate the incoming request.
    """

    try:
        # The actual data is in `envelope.message.data`, which Pydantic has
        # already validated as bytes (from the base64 string).
        first_decode_bytes = envelope.message.data
        json_bytes = base64.b64decode(first_decode_bytes)
        
        # Decoding the bytes into a UTF-8 string (which is a JSON array).
        data_str_json = json_bytes.decode("utf-8")
        # Parse the JSON string into a Python list of transactions.
        transactions = json.loads(data_str_json)

    except Exception as e:
        logging.error(f"Error decoding message data: {e}", exc_info=True)
        # Return a 400 Bad Request if the payload is malformed.
        # Pub/Sub will not retry messages that receive this status.
        return Response(status_code=400, content="Bad Request: Could not decode message data")

    if not isinstance(transactions, list):
        logging.error(f"Decoded data is not a list as expected. Type: {type(transactions)}")
        return Response(status_code=400, content="Bad Request: Expected a list of transactions")

    logging.info(f"Received and decoded a batch of {len(transactions)} transactions.")

    # Process Each Transaction in the Batch 
    for transaction in transactions:
        try:
            # Use .get() for safety in case a field is missing in a record
            fraud_score = float(transaction.get("is_fraud_prediction", 0.0))
            tx_id = transaction.get("transaction_id", "N/A")
            user_id = transaction.get("user_id", "N/A")

            if fraud_score > FRAUD_THRESHOLD:
                # Construct the structured log payload for a high-risk transaction
                log_payload = {
                    "message": "High-risk transaction detected.",
                    "status": "TRANSACTION_FLAGGED",
                    "severity": "WARNING", # Explicitly set for clarity and filtering
                    "transaction_id": tx_id,
                    "user_id": user_id,
                    "fraud_score": fraud_score,
                    "merchant_id": transaction.get("merchant_id"),
                    "amount": transaction.get("amount"),
                    "currency": transaction.get("currency")
                }
                # logging.warning will automatically set the severity level
                logging.warning(log_payload)
            else:
                # Construct the structured log payload for a normal transaction
                log_payload = {
                    "message": "Transaction processed, score below threshold.",
                    "status": "TRANSACTION_PROCESSED_OK",
                    "severity": "INFO",
                    "transaction_id": tx_id,
                    "fraud_score": fraud_score
                }
                logging.info(log_payload)

        except (TypeError, ValueError) as e:
            # Log an error for the specific record but continue processing the rest of the batch
            logging.error(f"Could not process a record in the batch: {e}. Record: {transaction}", exc_info=True)
            continue
            
    # Return a 204 "No Content" to Pub/Sub to acknowledge successful receipt.
    # This prevents the message from being resent.
    return Response(status_code=204)