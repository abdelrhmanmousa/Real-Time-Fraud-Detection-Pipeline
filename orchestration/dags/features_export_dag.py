from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.models.variable import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.operators.gcs import (
    GCSDeleteObjectsOperator,
)
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import (
    BigQueryToGCSOperator,
)
from airflow.utils.task_group import TaskGroup

from gcs_firestore.operators.gcs_to_firestore import GCSToFirestoreOperator

# Configuration 
GCP_PROJECT_ID = Variable.get("gcp_project_id", default_var="local-dev-project")
BQ_DATASET = Variable.get("bq_dataset", default_var="local_dev_dwh")
GCS_BUCKET_NAME = Variable.get("gcs_export_bucket_name", default_var="local-dev-bucket")

# Variables for the USERS export process 
USERS_FIRESTORE_COLLECTION = Variable.get("firestore_users_collection", "users")
USERS_BQ_SOURCE_TABLE = "For_Firestore_Users_Recalculated" # The table created by the SQL query
USERS_GCS_PREFIX = "exports/{{ ds_nodash }}/users"

# Variables for the MERCHANTS export process 
MERCHANTS_FIRESTORE_COLLECTION = Variable.get("firestore_merchants_collection", "merchants")
MERCHANTS_BQ_SOURCE_TABLE = "Dim_Merchants" # The direct source table
MERCHANTS_GCS_PREFIX = "exports/{{ ds_nodash }}/merchants"

# Schemas & Constants 
GCP_CONN_ID = "google_cloud_default"

FIRESTORE_USERS_DATA_SCHEMA = {
    "user_account_age_days": int,
    "user_home_lat": float,
    "user_home_long": float,
    "user_avg_tx_amount_30d": float,
    "user_max_distance_from_home_90d": float,
    "user_num_distinct_countries_6m": int,
    "user_tx_count_24h": int,
    "user_failed_tx_count_1h": int,
    "user_num_distinct_mcc_24h": int,
}

# Define a schema for merchants as well for type safety
FIRESTORE_MERCHANTS_DATA_SCHEMA = {
    "merchant_id": str,
    "merchant_category": str,
    "merchant_lat": float,
    "merchant_lon": float,
}

def task_failure_alert(context):
    """Callback function for task failures."""
    print(f"Task Failed! DAG: {context['dag'].dag_id}, Task: {context['task_instance'].task_id}")


default_args = {
    "owner": "Data Engineering Team",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": task_failure_alert,
}

with DAG(
    dag_id="dwh_export_to_firestore_v2",
    schedule="0 3 * * *", # Runs daily at 3:00 AM UTC
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    template_searchpath="/home/airflow/gcs/data/sql", # Path for get_users_historical_features.sql
    catchup=False,
    default_args=default_args,
    tags=["bigquery", "gcs", "firestore", "production"],
    doc_md="""
    ### DWH to Firestore Export
    This DAG recalculates user features and exports merchant dimensions from BigQuery to Firestore.
    - Runs daily at 3 AM UTC.
    - Exports data to a date-partitioned GCS path as JSON.
    - Loads data from GCS into corresponding Firestore collections.
    - **Best Practices**: Includes data quality checks, parallel execution, and automated cleanup.
    """,
) as dag:
    start = EmptyOperator(task_id="start")

    # TaskGroup for the USERS export process 
    with TaskGroup(group_id="process_users_to_firestore") as process_users:
        # Pre check: Ensure the source data for the calculation exists.
        # This is a placeholder; adjust the table and condition as needed.
        check_user_source_data = BigQueryCheckOperator(
            task_id="check_user_source_data",
            gcp_conn_id=GCP_CONN_ID,
            use_legacy_sql=False,
            sql=f"SELECT COUNT(*) FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.Fact_Transactions` WHERE DATE(transaction_timestamp) = '{{{{ ds }}}}'",
        )

        create_export_table = BigQueryInsertJobOperator(
            task_id="create_user_export_table",
            gcp_conn_id=GCP_CONN_ID,
            configuration={"query": {"query": "get_users_historical_features.sql", "useLegacySql": False}},
            params={"project_id": GCP_PROJECT_ID, "dataset": BQ_DATASET},
        )

        export_users_to_gcs = BigQueryToGCSOperator(
            task_id="export_users_to_gcs",
            gcp_conn_id=GCP_CONN_ID,
            source_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_DATASET}.{USERS_BQ_SOURCE_TABLE}",
            destination_cloud_storage_uris=[f"gs://{GCS_BUCKET_NAME}/{USERS_GCS_PREFIX}-*.json"],
            export_format="NEWLINE_DELIMITED_JSON",
        )

        # The GCS to Firestore operator now implicitly depends on the GCS export being successful.
        load_users_to_firestore = GCSToFirestoreOperator(
            task_id="load_users_to_firestore",
            gcs_bucket=GCS_BUCKET_NAME,
            gcs_object_prefix=USERS_GCS_PREFIX,
            firestore_collection=USERS_FIRESTORE_COLLECTION,
            firestore_document_id_field="user_id",
            gcp_conn_id=GCP_CONN_ID,
            schema=FIRESTORE_USERS_DATA_SCHEMA,
        )

        cleanup_user_files_in_gcs = GCSDeleteObjectsOperator(
            task_id="cleanup_user_files_in_gcs",
            gcp_conn_id=GCP_CONN_ID,
            bucket_name=GCS_BUCKET_NAME,
            prefix=USERS_GCS_PREFIX,
        )

        check_user_source_data >> create_export_table >> export_users_to_gcs >> load_users_to_firestore >> cleanup_user_files_in_gcs

    # TaskGroup for the MERCHANTS export process 
    with TaskGroup(group_id="process_merchants_to_firestore") as process_merchants:
        # Pre check: Ensure the merchant dimension table is not empty.
        check_merchant_source_data = BigQueryCheckOperator(
            task_id="check_merchant_source_data",
            gcp_conn_id=GCP_CONN_ID,
            use_legacy_sql=False,
            sql=f"SELECT COUNT(*) > 0 FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{MERCHANTS_BQ_SOURCE_TABLE}`",
        )

        export_merchants_to_gcs = BigQueryToGCSOperator(
            task_id="export_merchants_to_gcs",
            gcp_conn_id=GCP_CONN_ID,
            source_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_DATASET}.{MERCHANTS_BQ_SOURCE_TABLE}",
            destination_cloud_storage_uris=[f"gs://{GCS_BUCKET_NAME}/{MERCHANTS_GCS_PREFIX}-*.json"],
            export_format="NEWLINE_DELIMITED_JSON",
        )
        
        load_merchants_to_firestore = GCSToFirestoreOperator(
            task_id="load_merchants_to_firestore",
            gcp_conn_id=GCP_CONN_ID,
            gcs_bucket=GCS_BUCKET_NAME,
            gcs_object_prefix=MERCHANTS_GCS_PREFIX,
            firestore_collection=MERCHANTS_FIRESTORE_COLLECTION,
            firestore_document_id_field="merchant_id", # Adjust this field name as needed
            schema=FIRESTORE_MERCHANTS_DATA_SCHEMA,
        )

        cleanup_merchant_files_in_gcs = GCSDeleteObjectsOperator(
            task_id="cleanup_merchant_files_in_gcs",
            gcp_conn_id=GCP_CONN_ID,
            bucket_name=GCS_BUCKET_NAME,
            prefix=MERCHANTS_GCS_PREFIX,
        )

        check_merchant_source_data >> export_merchants_to_gcs >> load_merchants_to_firestore >> cleanup_merchant_files_in_gcs

    end = EmptyOperator(task_id="end")

    # Define the final DAG structure with parallel TaskGroups
    start >> [process_users, process_merchants] >> end