import logging
from typing import NamedTuple, Dict, Any, List, Union

from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from google.api_core.exceptions import GoogleAPICallError
from google.cloud import firestore
from google.cloud.firestore_v1.base_client import BaseClient
from google.cloud.firestore_v1.document import DocumentReference


# These classes define the structure for different Firestore write operations.

class SetOperation(NamedTuple):
    """Represents a 'set' operation in Firestore (create or overwrite)."""
    doc_ref: DocumentReference
    data: Dict[str, Any]
    merge: bool = True

class UpdateOperation(NamedTuple):
    """Represents an 'update' operation in Firestore (modify existing)."""
    doc_ref: DocumentReference
    data: Dict[str, Any]

class DeleteOperation(NamedTuple):
    """Represents a 'delete' operation in Firestore."""
    doc_ref: DocumentReference

# A union type that can represent any of the supported write operations.
FirestoreWriteOperation = Union[SetOperation, UpdateOperation, DeleteOperation]


# The Firestore Hook

class FirestoreHook(GoogleBaseHook):
    """
    Airflow Hook for Google Cloud Firestore.

    This hook uses the GcpApiConnection (by default, `google_cloud_default`)
    to connect to Google Cloud and authenticate with Firestore.
    """
    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Union[str, List[str]] = None,
        **kwargs
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
            **kwargs
        )
        self._client = None

    def get_conn(self) -> BaseClient:
        """
        Retrieves the Firestore client, initializing it if it doesn't exist.
        This method leverages the parent class's authentication capabilities.
        """
        if not self._client:
            # self._get_credentials() and self._get_project_id() are powerful methods
            # from GoogleBaseHook that handle all authentication logic (ADC, keyfiles, etc.)
            # based on the provided gcp_conn_id.
            
            creds, project_id = self.get_credentials_and_project_id()

            logging.info(f"Creds: {creds}, Project ID: {project_id}")
            
            logging.info(f"Connecting to Firestore for project: {project_id}")
            try:
                self._client = firestore.Client(project=project_id, credentials=creds)
                # Perform a cheap, lightweight operation to confirm the connection is valid
                # and that the user has basic permissions. This helps fail early.
                self._client.collection('__test_connection').limit(1).get()
                logging.info("Firestore client established successfully.")
            except GoogleAPICallError as e:
                logging.error(f"Failed to connect to Firestore: {e}")
                self._client = None
                raise
        return self._client

    def batch_write(self, operations: List[FirestoreWriteOperation]) -> Dict[str, Any]:
        """
        Performs an efficient atomic batch write to Firestore. The entire batch
        will either succeed or fail together.

        :param operations: A list of Firestore write operations (SetOperation, UpdateOperation, etc.).
        :return: A dictionary summarizing the result of the batch operation.
        """
        client = self.get_conn()
        if not operations:
            logging.warning("batch_write called with an empty list of operations.")
            return {}

        try:
            batch = client.batch()
            for op in operations:
                if isinstance(op, SetOperation):
                    batch.set(op.doc_ref, op.data, merge=op.merge)
                elif isinstance(op, UpdateOperation):
                    batch.update(op.doc_ref, op.data)
                elif isinstance(op, DeleteOperation):
                    batch.delete(op.doc_ref)

            results = batch.commit()
            return {
                'commit_time': results[0].update_time,
                'write_results_count': len(results)
            }
        except GoogleAPICallError as e:
            logging.error(f"Firestore batch_write failed: {e}")
            raise

    # --- Helper methods for creating write operation objects ---

    def create_set_operation_from_data(
        self,
        collection_name: str,
        document_id_field: str,
        data: Dict[str, Any],
        merge: bool = True
    ) -> SetOperation:
        """Helper to create a single SetOperation from a data dictionary."""
        client = self.get_conn()
        doc_id = data.get(document_id_field)
        if not doc_id:
            raise ValueError(f"Document ID field '{document_id_field}' not found in data.")
        doc_ref = client.collection(collection_name).document(str(doc_id))
        return SetOperation(doc_ref=doc_ref, data=data, merge=merge)

    def create_batch_set_operations_from_list(
        self,
        collection_name: str,
        document_id_field: str,
        data_list: List[Dict[str, Any]],
        merge: bool = True
    ) -> List[SetOperation]:
        """Creates a list of SetOperation objects from a list of data dictionaries."""
        operations = []
        for data_item in data_list:
            try:
                op = self.create_set_operation_from_data(
                    collection_name=collection_name,
                    document_id_field=document_id_field,
                    data=data_item,
                    merge=merge
                )
                operations.append(op)
            except ValueError as e:
                logging.error(f"Skipping data item due to error: {e}. Bad item: {data_item}")
                continue
        return operations

    def create_update_operation_from_data(
        self,
        collection_name: str,
        document_id_field: str,
        data: Dict[str, Any]
    ) -> UpdateOperation:
        """Helper to create a single UpdateOperation from a data dictionary."""
        client = self.get_conn()
        doc_id = data.get(document_id_field)
        if not doc_id:
            raise ValueError(f"Document ID field '{document_id_field}' not found in data.")
        doc_ref = client.collection(collection_name).document(str(doc_id))
        return UpdateOperation(doc_ref=doc_ref, data=data)

    def create_batch_update_operations_from_list(
        self,
        collection_name: str,
        document_id_field: str,
        data_list: List[Dict[str, Any]]
    ) -> List[UpdateOperation]:
        """Creates a list of UpdateOperation objects from a list of data dictionaries."""
        operations = []
        for data_item in data_list:
            try:
                op = self.create_update_operation_from_data(
                    collection_name=collection_name,
                    document_id_field=document_id_field,
                    data=data_item,
                )
                operations.append(op)
            except ValueError as e:
                logging.error(f"Skipping update data item due to error: {e}. Bad item: {data_item}")
                continue
        return operations

    def create_delete_operation(
        self,
        collection_name: str,
        document_id: str
    ) -> DeleteOperation:
        """Helper to create a single DeleteOperation from a document ID."""
        client = self.get_conn()
        doc_ref = client.collection(collection_name).document(document_id)
        return DeleteOperation(doc_ref=doc_ref)

    def create_batch_delete_operations_from_list(
        self,
        collection_name: str,
        document_ids: List[str]
    ) -> List[DeleteOperation]:
        """Creates a list of DeleteOperation objects from a list of document IDs."""
        return [self.create_delete_operation(collection_name, doc_id) for doc_id in document_ids]