import os
import logging
import uuid

import apache_beam as beam
import threading
import pyarrow as pa
import pyarrow.parquet as pq
from apache_beam.io.gcp import gcsio



class WriteWindowedBatchToGCS(beam.DoFn):
    """Writes a windowed batch of records to a partitioned Parquet file in GCS."""

    _gcs_client = None
    _client_lock = threading.Lock()

    def __init__(self, base_path: str, schema: pa.Schema, use_emulator:bool = False):
        self._base_path = base_path
        self._schema = schema
        self._use_emulator = use_emulator

    def setup(self):
        # Initialize client once per worker (not per bundle)
        if "STORAGE_EMULATOR_HOST" in os.environ:
            del os.environ["STORAGE_EMULATOR_HOST"]
        with WriteWindowedBatchToGCS._client_lock:
            if WriteWindowedBatchToGCS._gcs_client is None:
                if self._use_emulator:
                    # For emulator use
                    WriteWindowedBatchToGCS._gcs_client = gcsio.GcsIO(
                        client_options={"api_endpoint": "http://localhost:4443"}
                    )
                else:
                    # For production GCS
                    WriteWindowedBatchToGCS._gcs_client = gcsio.GcsIO()

    def process(
        self, element: tuple, window=beam.DoFn.WindowParam
    ):
        key, records_iterable = element
        records_list = list(records_iterable)

        if not records_list:
            return

        window_end = window.end.to_utc_datetime() # We use the end time stamp to make sure that no data is written to a directory at the same time it is being worked on by spark.
        partition_path = f"year={window_end.year}/month={window_end.month:02d}/day={window_end.day:02d}/hour={window_end.hour:02d}"
        full_path = f"{self._base_path.rstrip('/')}/{partition_path}/data-{uuid.uuid4()}.parquet"

        logging.info(
            f"Writing {len(records_list)} records for window {window_end} to {full_path}"
        )
        try:
            table = pa.Table.from_pylist(records_list, schema=self._schema)
            buf = pa.BufferOutputStream()
            pq.write_table(table, buf)
            parquet_data = buf.getvalue().to_pybytes()
            with WriteWindowedBatchToGCS._gcs_client.open(full_path, "wb") as f:
                f.write(parquet_data)

            logging.info(f"Successfully wrote to {full_path}")
        except Exception as e:
            logging.error(f"Failed to write Parquet file to {full_path}: {e}")