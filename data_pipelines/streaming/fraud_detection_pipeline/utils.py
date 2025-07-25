import os
import logging
from datetime import datetime
import pyarrow as pa
from typing import List, Dict, Any, NamedTuple

from google.cloud import firestore
from google.cloud.firestore_v1.base_client import BaseClient
from google.cloud.firestore_v1.document import DocumentReference
from google.auth.credentials import AnonymousCredentials
from google.api_core.exceptions import GoogleAPICallError


# class SetOperation(NamedTuple):
#     doc_ref: DocumentReference
#     data: Dict[str, Any]
#     merge: bool = True

# class UpdateOperation(NamedTuple):
#     doc_ref: DocumentReference
#     data: Dict[str, Any]

# class DeleteOperation(NamedTuple):
#     doc_ref: DocumentReference


# FirestoreWriteOperation = SetOperation | UpdateOperation | DeleteOperation


# class FirestoreManager:
#     """
#     A helper class to manage connections and batch operations with Google Cloud Firestore.
#     This class is not tied to Apache Beam and can be used independently.
#     """
#     def __init__(self, project_id: str, emulator_host: str | None = None):
#         """
#         Args:
#             project_id: Your Google Cloud project ID.
#             emulator_host: The host:port for a local Firestore emulator (e.g., 'localhost:8080').
#                            If None, connects to the live Firestore service.
#         """
#         self._project_id = project_id
#         self._emulator_host = emulator_host
#         self.client: BaseClient | None = None

#     def connect(self):
#         """Establishes the connection by creating the Firestore client object."""
#         if self.client:
#             return # Already connected

#         try:
#             logging.info("Attempting to establish new Firestore client.")
#             if self._emulator_host:
#                 # Connecting to local emulator (dev and testing)
#                 host_only = self._emulator_host.replace("http://", "")
#                 client_options = {"api_endpoint": host_only}
#                 credentials = AnonymousCredentials()
#                 self.client = firestore.Client(
#                     project=self._project_id,
#                     credentials=credentials,
#                     client_options=client_options
#                 )
#             else:
#                 # Connecting to live Firestore service (production)
#                 self.client = firestore.Client(project=self._project_id)

#             # A cheap check to confirm the client is valid and can connect.
#             # This will fail early if permissions or project ID are wrong.
#             self.client.collection('__test_connection').limit(1).get()
#             logging.info("Firestore client established successfully.")
#         except GoogleAPICallError as e:
#             logging.error(f"Failed to connect to Firestore: {e}")
#             self.client = None
#             raise

#     def close(self):
#         """Closes the Firestore client connection (by releasing the object)."""
#         if self.client:
#             logging.info("Closing Firestore client.")
#             # Firestore clients don't have an explicit close() method.
#             # Releasing the object allows its gRPC channels to be garbage collected.
#             self.client = None

#     def batch_read(
#         self,
#         collection_name: str,
#         lookup_ids: List[str]
#     ) -> Dict[str, Dict[str, Any]]:
#         """
#         Performs an efficient batch read using 'get_all'.

#         Args:
#             collection_name: The name of the collection.
#             lookup_ids: A list of DOCUMENT IDs to fetch.

#         Returns:
#             A dictionary mapping each document ID to its found document data.
#         """
#         if not self.client:
#             raise ConnectionError("Firestore client is not connected.")

#         try:
#             doc_refs = [self.client.collection(collection_name).document(doc_id) for doc_id in lookup_ids]
#             documents = self.client.get_all(doc_refs)
#             # Return a map for easy lookup, filtering out docs that don't exist
#             return {doc.id: doc.to_dict() for doc in documents if doc.exists}
#         except Exception as e:
#             logging.error(f"Firestore batch_read failed for collection {collection_name}: {e}")
#             return {}

#     def batch_write(
#         self,
#         operations: List[FirestoreWriteOperation]
#     ) -> Dict[str, Any]:
#         """
#         Performs an efficient atomic batch write.

#         Args:
#             operations: A list of Firestore write operations (SetOperation, UpdateOperation, etc.).

#         Returns:
#             A dictionary summarizing the result of the batch operation.
#         """
#         if not self.client:
#             raise ConnectionError("Firestore client is not connected.")

#         if not operations:
#             return {}

#         try:
#             batch = self.client.batch()
#             for op in operations:
#                 if isinstance(op, SetOperation):
#                     batch.set(op.doc_ref, op.data, merge=op.merge)
#                 elif isinstance(op, UpdateOperation):
#                     batch.update(op.doc_ref, op.data)
#                 elif isinstance(op, DeleteOperation):
#                     batch.delete(op.doc_ref)

#             results = batch.commit()
#             return {
#                 'commit_time': results[0].update_time, # Time of the first write result
#                 'write_results_count': len(results)
#             }
#         except GoogleAPICallError as e:
#             logging.error(f"Firestore batch_write failed: {e}")
#             return {'error': str(e)}


#     def create_set_operation_from_data(
#         self,
#         collection_name: str,
#         document_id_field: str,
#         data: Dict[str, Any],
#         merge: bool = True
#     ) -> SetOperation:
#         """
#         Creates a SetOperation by extracting the document ID from a data dictionary.

#         Args:
#             collection_name: The name of the collection to write to.
#             document_id_field: The key in the `data` dict that holds the unique document ID.
#             data: The dictionary of data to write.
#             merge: If True, merges the data with an existing doc. If False, overwrites it.

#         Returns:
#             A SetOperation tuple ready to be used with `batch_write`.

#         Raises:
#             ValueError: If the client is not connected or the document_id_field is missing.
#         """
#         if not self.client:
#             raise ConnectionError("Firestore client is not connected.")

#         # 1. Extract the "primary key" value from the data
#         doc_id = data.get(document_id_field)
#         if not doc_id:
#             raise ValueError(f"Document ID field '{document_id_field}' not found in data.")

#         # 2. Use the extracted ID to construct the DocumentReference (the "address")
#         doc_ref = self.client.collection(collection_name).document(str(doc_id))

#         # 3. Return the complete SetOperation object
#         return SetOperation(doc_ref=doc_ref, data=data, merge=merge)
    
#     def create_batch_set_operations_from_list(
#         self,
#         collection_name: str,
#         document_id_field: str,
#         data_list: List[Dict[str, Any]],
#         merge: bool = True
#     ) -> List[SetOperation]:
#         """
#         Creates a list of SetOperation objects from a list of data dictionaries.

#         This is a convenience wrapper around `create_set_operation_from_data`.

#         Args:
#             collection_name: The name of the collection to write to.
#             document_id_field: The key in each dict that holds the unique document ID.
#             data_list: A list of dictionaries to write.
#             merge: If True, merges the data with an existing doc. If False, overwrites it.

#         Returns:
#             A list of SetOperation tuples ready to be used with `batch_write`.
#         """
#         operations = []
#         for data_item in data_list:
#             try:
#                 # Reuse our single-item helper for each dictionary in the list
#                 op = self.create_set_operation_from_data(
#                     collection_name=collection_name,
#                     document_id_field=document_id_field,
#                     data=data_item,
#                     merge=merge
#                 )
#                 operations.append(op)
#             except ValueError as e:
#                 # Log an error for the bad item but continue processing the rest
#                 logging.error(f"Skipping data item due to error: {e}. Bad item: {data_item}")
#                 continue
#         return operations


import logging
from typing import NamedTuple, Dict, Any, List, Union

from google.api_core.exceptions import GoogleAPICallError
from google.auth.credentials import AnonymousCredentials
from google.cloud import firestore
from google.cloud.firestore_v1.base_client import BaseClient
from google.cloud.firestore_v1.document import DocumentReference

# --- Operation Type Definitions ---

class SetOperation(NamedTuple):
    """Represents a 'set' operation in Firestore."""
    doc_ref: DocumentReference
    data: Dict[str, Any]
    merge: bool = True

class UpdateOperation(NamedTuple):
    """Represents an 'update' operation in Firestore."""
    doc_ref: DocumentReference
    data: Dict[str, Any]

class DeleteOperation(NamedTuple):
    """Represents a 'delete' operation in Firestore."""
    doc_ref: DocumentReference

# Union type for any supported write operation
FirestoreWriteOperation = Union[SetOperation, UpdateOperation, DeleteOperation]


# --- FirestoreManager Class ---

class FirestoreManager:
    """
    A helper class to manage connections and batch operations with Google Cloud Firestore.
    This class is not tied to Apache Beam and can be used independently.
    """
    def __init__(self, project_id: str, emulator_host: str | None = None):
        """
        Args:
            project_id: Your Google Cloud project ID.
            emulator_host: The host:port for a local Firestore emulator (e.g., 'localhost:8080').
                           If None, connects to the live Firestore service.
        """
        self._project_id = project_id
        self._emulator_host = emulator_host
        self.client: BaseClient | None = None

    def connect(self):
        """Establishes the connection by creating the Firestore client object."""
        if self.client:
            return # Already connected

        try:
            logging.info("Attempting to establish new Firestore client.")
            if self._emulator_host:
                # Connecting to local emulator (dev and testing)
                host_only = self._emulator_host.replace("http://", "")
                client_options = {"api_endpoint": host_only}
                credentials = AnonymousCredentials()
                self.client = firestore.Client(
                    project=self._project_id,
                    credentials=credentials,
                    client_options=client_options
                )
            else:
                # Connecting to live Firestore service (production)
                self.client = firestore.Client(project=self._project_id)

            # A cheap check to confirm the client is valid and can connect.
            # This will fail early if permissions or project ID are wrong.
            self.client.collection('__test_connection').limit(1).get()
            logging.info("Firestore client established successfully.")
        except GoogleAPICallError as e:
            logging.error(f"Failed to connect to Firestore: {e}")
            self.client = None
            raise

    def close(self):
        """Closes the Firestore client connection (by releasing the object)."""
        if self.client:
            logging.info("Closing Firestore client.")
            # Firestore clients don't have an explicit close() method.
            # Releasing the object allows its gRPC channels to be garbage collected.
            self.client = None

    def batch_read(
        self,
        collection_name: str,
        lookup_ids: List[str]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Performs an efficient batch read using 'get_all'.

        Args:
            collection_name: The name of the collection.
            lookup_ids: A list of DOCUMENT IDs to fetch.

        Returns:
            A dictionary mapping each document ID to its found document data.
        """
        if not self.client:
            raise ConnectionError("Firestore client is not connected.")

        try:
            doc_refs = [self.client.collection(collection_name).document(doc_id) for doc_id in lookup_ids]
            documents = self.client.get_all(doc_refs)
            # Return a map for easy lookup, filtering out docs that don't exist
            return {doc.id: doc.to_dict() for doc in documents if doc.exists}
        except Exception as e:
            logging.error(f"Firestore batch_read failed for collection {collection_name}: {e}")
            return {}

    def batch_write(
        self,
        operations: List[FirestoreWriteOperation]
    ) -> Dict[str, Any]:
        """
        Performs an efficient atomic batch write.

        Args:
            operations: A list of Firestore write operations (SetOperation, UpdateOperation, etc.).

        Returns:
            A dictionary summarizing the result of the batch operation.
        """
        if not self.client:
            raise ConnectionError("Firestore client is not connected.")

        if not operations:
            return {}

        try:
            batch = self.client.batch()
            for op in operations:
                if isinstance(op, SetOperation):
                    batch.set(op.doc_ref, op.data, merge=op.merge)
                elif isinstance(op, UpdateOperation):
                    batch.update(op.doc_ref, op.data)
                elif isinstance(op, DeleteOperation):
                    batch.delete(op.doc_ref)

            results = batch.commit()
            return {
                'commit_time': results[0].update_time, # Time of the first write result
                'write_results_count': len(results)
            }
        except GoogleAPICallError as e:
            logging.error(f"Firestore batch_write failed: {e}")
            return {'error': str(e)}

    # --- SET Operation Helpers ---

    def create_set_operation_from_data(
        self,
        collection_name: str,
        document_id_field: str,
        data: Dict[str, Any],
        merge: bool = True
    ) -> SetOperation:
        """
        Creates a SetOperation by extracting the document ID from a data dictionary.

        Args:
            collection_name: The name of the collection to write to.
            document_id_field: The key in the `data` dict that holds the unique document ID.
            data: The dictionary of data to write.
            merge: If True, merges the data with an existing doc. If False, overwrites it.

        Returns:
            A SetOperation tuple ready to be used with `batch_write`.
        """
        if not self.client:
            raise ConnectionError("Firestore client is not connected.")

        doc_id = data.get(document_id_field)
        if not doc_id:
            raise ValueError(f"Document ID field '{document_id_field}' not found in data.")

        doc_ref = self.client.collection(collection_name).document(str(doc_id))
        return SetOperation(doc_ref=doc_ref, data=data, merge=merge)

    def create_batch_set_operations_from_list(
        self,
        collection_name: str,
        document_id_field: str,
        data_list: List[Dict[str, Any]],
        merge: bool = True
    ) -> List[SetOperation]:
        """
        Creates a list of SetOperation objects from a list of data dictionaries.
        """
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

    # --- UPDATE Operation Helpers (NEW) ---

    def create_update_operation_from_data(
        self,
        collection_name: str,
        document_id_field: str,
        data: Dict[str, Any]
    ) -> UpdateOperation:
        """
        Creates an UpdateOperation by extracting the document ID from a data dictionary.

        Args:
            collection_name: The name of the collection to update.
            document_id_field: The key in the `data` dict that holds the unique document ID.
            data: The dictionary of fields to update.

        Returns:
            An UpdateOperation tuple ready for `batch_write`.
        """
        if not self.client:
            raise ConnectionError("Firestore client is not connected.")

        doc_id = data.get(document_id_field)
        if not doc_id:
            raise ValueError(f"Document ID field '{document_id_field}' not found in data.")

        doc_ref = self.client.collection(collection_name).document(str(doc_id))
        return UpdateOperation(doc_ref=doc_ref, data=data)

    def create_batch_update_operations_from_list(
        self,
        collection_name: str,
        document_id_field: str,
        data_list: List[Dict[str, Any]]
    ) -> List[UpdateOperation]:
        """
        Creates a list of UpdateOperation objects from a list of data dictionaries.
        """
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

    # --- DELETE Operation Helpers (NEW) ---

    def create_delete_operation(
        self,
        collection_name: str,
        document_id: str
    ) -> DeleteOperation:
        """
        Creates a DeleteOperation from a document ID.

        Args:
            collection_name: The name of the collection where the document resides.
            document_id: The ID of the document to delete.

        Returns:
            A DeleteOperation tuple ready for `batch_write`.
        """
        if not self.client:
            raise ConnectionError("Firestore client is not connected.")

        doc_ref = self.client.collection(collection_name).document(document_id)
        return DeleteOperation(doc_ref=doc_ref)

    def create_batch_delete_operations_from_list(
        self,
        collection_name: str,
        document_ids: List[str]
    ) -> List[DeleteOperation]:
        """
        Creates a list of DeleteOperation objects from a list of document IDs.
        """
        if not self.client:
            raise ConnectionError("Firestore client is not connected.")
            
        return [
            self.create_delete_operation(collection_name, doc_id)
            for doc_id in document_ids
        ]

class TypeConverter:
    """Handles robust type conversion for a dictionary based on a PyArrow schema."""

    def __init__(self, schema: pa.Schema):
        self.schema = schema
        self.field_types = {field.name: field.type for field in schema}

    def convert(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Converts values in a dictionary to types defined in the schema."""
        converted = {}
        for field_name, value in record.items():
            if field_name not in self.field_types:
                continue  # Skip fields not in the schema

            field_type = self.field_types[field_name]
            try:
                converted[field_name] = self._convert_value(value, field_type)
            except Exception as e:
                logging.warning(f"Conversion failed for field '{field_name}': {e}")
                converted[field_name] = self._get_default_value(field_type)
        return converted

    def _convert_value(self, value: Any, field_type: pa.DataType) -> Any:
        """Converts a single value to the target PyArrow type."""
        if value is None:
            return self._get_default_value(field_type)

        if pa.types.is_timestamp(field_type):
            return self._to_datetime(value)
        if pa.types.is_boolean(field_type):
            return str(value).lower() in ["true", "1", "t", "yes"]
        if pa.types.is_integer(field_type):
            return int(value)
        if pa.types.is_floating(field_type):
            return float(value)
        if pa.types.is_string(field_type):
            return str(value)
        return value

    def _to_datetime(self, value: Any) -> datetime:
        """Robustly converts a value to a timezone-aware datetime object."""
        if isinstance(value, str):
            if "Z" in value:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            return datetime.fromisoformat(value)
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value, tz=datetime.timezone.utc)
        return datetime.now(datetime.timezone.utc)

    def _get_default_value(self, field_type: pa.DataType) -> Any:
        """Returns a sensible default for a given PyArrow type."""
        if pa.types.is_timestamp(field_type):
            return datetime.now(datetime.timezone.utc)
        if pa.types.is_boolean(field_type):
            return False
        if pa.types.is_integer(field_type):
            return 0
        if pa.types.is_floating(field_type):
            return 0.0
        if pa.types.is_string(field_type):
            return ""
        return None