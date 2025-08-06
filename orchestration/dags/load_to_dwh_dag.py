from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.models.variable import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocInstantiateWorkflowTemplateOperator,
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.utils.task_group import TaskGroup

# Configuration Variables
GCP_PROJECT_ID = Variable.get("gcp_project_id", "your-gcp-project-id")
GCP_REGION = Variable.get("gcp_dataproc_region", "us-central1")
GCP_CONN_ID = "google_cloud_default"  # The name of your GCP connection in Airflow

DATAPROC_TEMPLATE_ID = Variable.get("dataproc_transactions_template_id", "spark-fraud-template")
GCS_BUCKET = Variable.get("transactions_input_gcs_bucket", "your-source-bucket-name") # Just the bucket name
INPUT_GCS_PREFIX = Variable.get("transactions_input_gcs_prefix", "transactions/raw") # Path inside bucket
OUTPUT_DATASET = Variable.get("transactions_output_dataset", "fraud_detection_dwh")
BQ_TEMP_BUCKET = Variable.get("transactions_bq_temp_bucket", "your-bq-temp-bucket")


def task_failure_alert(context):
    """
    A callback function to be executed when a task fails.
    This can be customized to send a Slack message, email, PagerDuty alert, etc.
    """
    task_instance = context.get('task_instance')
    log_url = task_instance.log_url
    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    
    # Example: Print a message. Replace with your preferred notification client.
    print(f"""
    --------------------------------------------------------------------
    Task Failed!
    DAG: {dag_id}
    Task: {task_id}
    Log URL: {log_url}
    --------------------------------------------------------------------
    """)

default_args = {
    "owner": "Data Engineering Team",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": task_failure_alert,
}

with DAG(
    dag_id="dataproc_hourly_transactions_etl",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule="@hourly",
    catchup=False,
    default_args=default_args,
    # sla=pendulum.duration(minutes=45), # Trigger an alert if the DAG run exceeds 45 mins
    tags=["dataproc", "etl", "transactions", "production"],
) as dag:
    start = EmptyOperator(task_id="start")

    with TaskGroup(group_id="etl_process") as etl_process:
        #  Use a Sensor to wait for the source data to exist.
        # This prevents the expensive Dataproc job from starting if data is missing (Fail fast).
        check_source_data_exists = GCSObjectsWithPrefixExistenceSensor(
            task_id="check_source_data_exists",
            bucket=GCS_BUCKET,
            # Dynamically build the prefix for the specific hour
            # prefix=f"{INPUT_GCS_PREFIX}/year={{{{ data_interval_start.strftime('%Y') }}}}/month={{% raw %}}{{{{ data_interval_start.strftime('%m') }}}}{{% endraw %}}/day={{% raw %}}{{{{ data_interval_start.strftime('%d') }}}}{{% endraw %}}/hour={{% raw %}}{{{{ data_interval_start.strftime('%H') }}}}{{% endraw %}}/",
            prefix=f"{INPUT_GCS_PREFIX}/year={{{{ data_interval_start.strftime('%Y') }}}}/month={{{{ data_interval_start.strftime('%m') }}}}/day={{{{ data_interval_start.strftime('%d') }}}}/hour={{{{ data_interval_start.strftime('%H') }}}}/",
            google_cloud_conn_id=GCP_CONN_ID,
            mode='poke',
            poke_interval=60, # Check every 60 seconds
            timeout=300,      # Timeout after 5 minutes
        )

        # Execut The main Dataproc job (the actaul task)
        run_spark_etl_job = DataprocInstantiateWorkflowTemplateOperator(
            task_id="run_dataproc_spark_job",
            project_id=GCP_PROJECT_ID,
            region=GCP_REGION,
            template_id=DATAPROC_TEMPLATE_ID,
            parameters={
                "PROJECT_ID": GCP_PROJECT_ID,
                "INPUT_GCS_PATH": f"{GCS_BUCKET}/{INPUT_GCS_PREFIX}",
                "OUTPUT_DATASET": OUTPUT_DATASET,
                "BQ_TEMP_BUCKET": BQ_TEMP_BUCKET,
                "YEAR": "{{ data_interval_start.strftime('%Y') }}",
                "MONTH": "{{ data_interval_start.strftime('%m') }}",
                "DAY": "{{ data_interval_start.strftime('%d') }}",
                "HOUR": "{{ data_interval_start.strftime('%H') }}",
            },
        )

        # After the job succeeds, run a quick check on the target table. This query verifies that at least one row was written for the processed hour.
        validate_fact_table_load = BigQueryCheckOperator(
            task_id="validate_fact_table_load",
            gcp_conn_id=GCP_CONN_ID,
            use_legacy_sql=False,
            sql=f"""
                SELECT COUNT(*)
                FROM `{GCP_PROJECT_ID}.{OUTPUT_DATASET}.Fact_Transactions`
                WHERE
                    TIMESTAMP_TRUNC(transaction_timestamp, HOUR) = TIMESTAMP_TRUNC(TIMESTAMP('{{{{ data_interval_start.isoformat() }}}}'), HOUR)
            """,
            # The operator will pass if the first cell of the first row is > 0
        )
        
        check_source_data_exists >> run_spark_etl_job >> validate_fact_table_load

    end = EmptyOperator(task_id="end")

    start >> etl_process >> end