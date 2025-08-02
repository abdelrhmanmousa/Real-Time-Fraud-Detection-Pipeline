# plugins/gcs_firestore/operators/gcs_to_firestore.py

import json
import logging
from typing import Sequence, Dict, Any, Callable, List

from airflow.models.baseoperator import BaseOperator
from airflow.models.variable import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from gcs_firestore.hooks.firestore import FirestoreHook


class GCSToFirestoreOperator(BaseOperator):
    """
    Loads one or more newline-delimited JSON files from GCS, enforces a schema,
    and writes the records to a Firestore collection.

    :param source_task_id: The task_id of the upstream GCSListObjectsOperator.
    :param firestore_collection: The target collection in Firestore.
    :param firestore_document_id_field: Field in the JSON to use as the document ID.
    :param schema: A dictionary mapping field names to their target Python types (e.g., int, float).
    :param gcp_conn_id: The Airflow connection ID.
    """
    template_fields: Sequence[str] = ("firestore_collection",)

    def __init__(
        self,
        *,
        source_task_id: str,
        firestore_collection: str,
        firestore_document_id_field: str,
        schema: Dict[str, Callable[[Any], Any]],
        gcp_conn_id: str = "google_cloud_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.source_task_id = source_task_id
        self.firestore_collection = firestore_collection
        self.firestore_document_id_field = firestore_document_id_field
        self.schema = schema
        self.gcp_conn_id = gcp_conn_id

    def _enforce_schema(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Applies type casting to a single record based on the provided schema.
        This function is robust against missing fields and casting errors.
        """
        typed_record = record.copy()
        for field, target_type in self.schema.items():
            if field in typed_record and typed_record[field] is not None:
                try:
                    # Attempt to cast the value to the target type
                    typed_record[field] = target_type(typed_record[field])
                except (ValueError, TypeError) as e:
                    self.log.warning(
                        f"Could not cast field '{field}' with value '{typed_record[field]}' "
                        f"to type {target_type.__name__}. Leaving as is. Error: {e}"
                    )
        return typed_record

    def execute(self, context):
        """Main execution logic for the operator."""
        ti = context["ti"]
        gcs_objects = ti.xcom_pull(task_ids=self.source_task_id, key="return_value")

        if not gcs_objects:
            self.log.warning(f"No GCS objects found in XCom from task '{self.source_task_id}'. Skipping.")
            return

        self.log.info(f"Found {len(gcs_objects)} file(s) to process: {gcs_objects}")

        gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id)
        firestore_hook = FirestoreHook(gcp_conn_id=self.gcp_conn_id)
        bucket_name = Variable.get("gcs_bucket_name")
        
        all_records = []
        for obj in gcs_objects:
            self.log.info(f"Processing gs://{bucket_name}/{obj}")
            file_content = gcs_hook.download(bucket_name=bucket_name, object_name=obj)
            records_from_file = [json.loads(line) for line in file_content.decode("utf-8").splitlines() if line]
            all_records.extend(records_from_file)

        if not all_records:
            self.log.warning("No records found in any files.")
            return

        # Apply type enforcement to all records
        self.log.info(f"Enforcing schema on {len(all_records)} records...")
        typed_records = [self._enforce_schema(rec) for rec in all_records]
        
        
        self.log.info(f"Total records to write to Firestore: {len(typed_records)}")
        
        # Use the new `typed_records` list for the write operations
        operations = firestore_hook.create_batch_set_operations_from_list(
            collection_name=self.firestore_collection,
            document_id_field=self.firestore_document_id_field,
            data_list=typed_records,
            merge=True,
        )
        
        batch_size = 500
        for i in range(0, len(operations), batch_size):
            batch = operations[i:i + batch_size]
            self.log.info(f"Writing batch of {len(batch)} records to Firestore...")
            firestore_hook.batch_write(batch)

        self.log.info("Firestore load process completed successfully.")