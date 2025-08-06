import json
from typing import Sequence, Dict, Any, Callable

from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from gcs_firestore.hooks.firestore import FirestoreHook


class GCSToFirestoreOperator(BaseOperator):
    """
    Lists files in a GCS path, loads each newline-delimited JSON file,
    enforces a schema, and writes the records to a Firestore collection.

    :param gcs_bucket: The GCS bucket where the source files are located.
    :param gcs_object_prefix: The prefix to match files in the GCS bucket.
    :param firestore_collection: The target collection in Firestore.
    :param firestore_document_id_field: Field in the JSON to use as the document ID.
    :param schema: A dictionary mapping field names to their target Python types (e.g., int, float).
    :param gcp_conn_id: The Airflow connection ID for Google Cloud.
    """
    template_fields: Sequence[str] = (
        "gcs_bucket",
        "gcs_object_prefix",
        "firestore_collection",
    )

    def __init__(
        self,
        *,
        gcs_bucket: str,
        gcs_object_prefix: str,
        firestore_collection: str,
        firestore_document_id_field: str,
        schema: Dict[str, Callable[[Any], Any]],
        gcp_conn_id: str = "google_cloud_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.gcs_bucket = gcs_bucket
        self.gcs_object_prefix = gcs_object_prefix
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
                    typed_record[field] = target_type(typed_record[field])
                except (ValueError, TypeError) as e:
                    self.log.warning(
                        f"Could not cast field '{field}' with value '{typed_record[field]}' "
                        f"to type {target_type.__name__}. Leaving as is. Error: {e}"
                    )
        return typed_record

    def execute(self, context):
        """Main execution logic for the operator."""
        gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id)
        firestore_hook = FirestoreHook(gcp_conn_id=self.gcp_conn_id)

        # List all files in GCS that match the given prefix
        gcs_objects = gcs_hook.list(
            bucket_name=self.gcs_bucket, prefix=self.gcs_object_prefix
        )

        if not gcs_objects:
            self.log.warning(
                f"No GCS objects found in gs://{self.gcs_bucket}/{self.gcs_object_prefix}. Skipping."
            )
            return

        self.log.info(f"Found {len(gcs_objects)} file(s) to process: {gcs_objects}")

        all_records = []
        for obj in gcs_objects:
            self.log.info(f"Processing gs://{self.gcs_bucket}/{obj}")
            file_content = gcs_hook.download(bucket_name=self.gcs_bucket, object_name=obj)
            # Filter out empty lines that might exist in the file
            records_from_file = [
                json.loads(line) for line in file_content.decode("utf-8").splitlines() if line
            ]
            all_records.extend(records_from_file)

        if not all_records:
            self.log.warning("No records found in any of the processed files.")
            return

        # Apply type enforcement to all records
        self.log.info(f"Enforcing schema on {len(all_records)} records...")
        typed_records = [self._enforce_schema(rec) for rec in all_records]

        self.log.info(f"Total records to write to Firestore: {len(typed_records)}")

        # Create batch write operations from the schema-enforced records
        operations = firestore_hook.create_batch_set_operations_from_list(
            collection_name=self.firestore_collection,
            document_id_field=self.firestore_document_id_field,
            data_list=typed_records,
            merge=True,  # Use merge=True to update existing documents
        )

        # Firestore allows a maximum of 500 operations per batch
        batch_size = 500
        for i in range(0, len(operations), batch_size):
            batch = operations[i : i + batch_size]
            self.log.info(f"Writing batch of {len(batch)} records to Firestore...")
            firestore_hook.batch_write(batch)

        self.log.info("Firestore load process completed successfully.")