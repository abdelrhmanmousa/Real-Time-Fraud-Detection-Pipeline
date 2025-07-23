import os


PIPELINE_MODE = os.getenv("PIPELINE_MODE", "gcp")  # Defaults to "gcp"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

class Settings:
    # --- GCP Settings (Used when PIPELINE_MODE is "gcp") ---
    GCP_PROJECT_ID = "wired-effort-464808-u3"
    # NOTE: This is the ROOT path. The full path with year/month/day/hour will be built at runtime.
    GCS_INPUT_PATH_ROOT = "gs://fraud_detection_123/raw_processed_transactions/final"
    BIGQUERY_TEMP_GCS_BUCKET = "bigquery_staging123"
    BIGQUERY_DATASET = "DW_Fraud"

    LOCAL_DATA = os.path.join(BASE_DIR, "raw_processed_transactions/final")


    # --- Common Table Names ---
    FACT_TABLE = "Fact_Transactions"
    DIM_CARDS_TABLE = "dim_Cards"
    DIM_USERS_TABLE = "dim_Users"
    DIM_MERCHANTS_TABLE = "dim_Merchants"
    DIM_DEVICES_TABLE = "dim_device"
    DIM_DATE_TABLE = "Dim_Dates"
    DIM_TIME_TABLE = "Dim_Time"
    DIM_FLAGS_TABLE = "dim_Transaction_Flags"