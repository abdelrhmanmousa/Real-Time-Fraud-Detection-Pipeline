import os
import json
from datetime import datetime
import apache_beam as beam
from apache_beam.io import ReadFromPubSub, WriteToText
from apache_beam.io.fileio import WriteToFiles, FileSink
import pyarrow as pa
import pyarrow.parquet as pq

from apache_beam.options.pipeline_options import (
    PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions
)
from apache_beam.transforms.window import FixedWindows

from . import config
from . import schema as pipeline_schema
from . import transforms
from . import pipeline_io


class FraudDetectionPipelineOptions(PipelineOptions):
    """
    Defines custom command-line arguments for the fraud detection pipeline.
    """
    @classmethod
    def _add_argparse_args(cls, parser):
        # Add only the arguments that are specific to your pipeline's logic
        parser.add_argument("--input_topic", required=True, help="Input Pub/Sub topic in format 'projects/PROJECT/topics/TOPIC'.")
        parser.add_argument("--output_path", required=True, help="Base GCS path for successful output.")
        
        # --- ADDED: Arguments for Dead-Letter Queue paths ---
        parser.add_argument("--parse_dlq_path", required=True, help="GCS path for parse-failure dead-letter files.")
        parser.add_argument("--processing_dlq_path", required=True, help="GCS path for processing-failure dead-letter files.")

        parser.add_argument("--model_endpoint_url", required=True, help="URL of the model service.")
        parser.add_argument("--window_duration_seconds", type=int, default=20, help="Window duration (s).")
        parser.add_argument("--allowed_lateness_seconds", type=int, default=60, help="Allowed lateness (s).")
        parser.add_argument("--ml_batch_size", type=int, default=50, help="Batch size for ML.")
        parser.add_argument("--allowed_wait_time_seconds", type=int, default=2, help="Allowed wait time for ML batching.")
        parser.add_argument("--ml_batch_max_size", type=int, default=100, help="Max batch size for ML.")
        parser.add_argument("--use_emulator", action='store_true', default=False, help="Use a local Firestore emulator.")
        parser.add_argument("--emulator_host", required=False, help="The host:port for a local Firestore emulator (e.g., 'localhost:8080').")



def build(options: FraudDetectionPipelineOptions):
    options.view_as(StandardOptions).streaming = True
    options.view_as(SetupOptions).save_main_session = True

    gcp_options = options.view_as(GoogleCloudOptions)
    p = beam.Pipeline(options=options)

    # Step 1 & 2: Read and Parse with Dead-Lettering
    raw_messages = p | "ReadFromPubSub" >> ReadFromPubSub(topic=options.input_topic)
    
    parsing_results = (
        raw_messages
        | "ParseAndTimestamp" >> beam.ParDo(transforms.ParseAndTimestampDoFn()).with_outputs(
                                      transforms.failed_parse_tag, 
                                      main='parsed_transactions'
                                  )
    )

    parsed_transactions = parsing_results.parsed_transactions
    failed_parses = parsing_results[transforms.failed_parse_tag]

    # Step 3: Dead-Letter Sink for Parsing Failures
    (
        failed_parses
        | "DecodeFailedParses" >> beam.Map(lambda b: b.decode('utf-8', 'ignore'))
        | "WindowFailedParses" >> beam.WindowInto(FixedWindows(options.window_duration_seconds))
        | "WriteParseDLQ" >> WriteToFiles(
            path=os.path.join(options.parse_dlq_path, str(datetime.now().isoformat(timespec="hours"))),
          )
    )


    # --- Step 4: ML Processing with Dead-Lettering ---
    prediction_results = (
        parsed_transactions
        | "KeyForML" >> beam.Map(lambda e: ("batch", e))
        | "BatchForML" >> beam.GroupIntoBatches(options.ml_batch_size, options.allowed_wait_time_seconds)
        | "RunMLInference" >> beam.ParDo(
            transforms.PredictFraudBatchDoFn(
                endpoint_url=options.model_endpoint_url,
                project_id=gcp_options.project,
                use_emulator=options.use_emulator,
                emulator_host=options.emulator_host,
            )
        ).with_outputs(
            transforms.failed_processing_tag, 
            main='successful_predictions'
        )
    )

    successful_predictions = prediction_results.successful_predictions
    failed_batches = prediction_results[transforms.failed_processing_tag]

    # --- Step 5: Dead-Letter Sink for Processing Failures (SIMPLIFIED & ENABLED) ---
    (
        failed_batches
        | "FailedBatchToString" >> beam.Map(lambda batch: json.dumps(batch, default=str))
        | "WindowFailedProcessing" >> beam.WindowInto(FixedWindows(options.window_duration_seconds))
        | "WriteProcessingDLQ" >> WriteToFiles(
            path=os.path.join(options.processing_dlq_path, str(datetime.now().isoformat(timespec="hours"))),
          )
    )

    # Step 6: The "happy path" analytics write 
    (
        successful_predictions
        | "Window" >> beam.WindowInto(
            FixedWindows(options.window_duration_seconds),
            allowed_lateness=options.allowed_lateness_seconds,
            accumulation_mode=config.ACCUMULATION_MODE,
        )
       
        | "KeyForGCS" >> beam.Map(lambda e: (config.CONSTANT_BATCH_KEY, e))
        
        | "GroupForGCS" >> beam.GroupByKey()
        
        | "WriteWindowedBatch" >> beam.ParDo(
            pipeline_io.WriteWindowedBatchToGCS(
                base_path=os.path.join(options.output_path, "final"),
                schema=pipeline_schema.transaction_schema,
                use_emulator=options.use_emulator,
            )
        )
    )
    return p