import os
import json
from datetime import datetime
import apache_beam as beam
from apache_beam.io import ReadFromPubSub, WriteToText, WriteToPubSub
from apache_beam.io.fileio import WriteToFiles, FileSink
import pyarrow as pa
import pyarrow.parquet as pq

from apache_beam.options.pipeline_options import (
    PipelineOptions,
    GoogleCloudOptions,
    StandardOptions,
    SetupOptions,
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
        parser.add_argument(
            "--input_topic",
            required=True,
            help="Input Pub/Sub topic in format 'projects/PROJECT/topics/TOPIC'.",
        )
        parser.add_argument(
            "--output_path", required=True, help="Base GCS path for successful output."
        )

        parser.add_argument(
            "--output_topic",
            required=True,
            help="Output Pub/Sub topic for scored transactions in format 'projects/PROJECT/topics/TOPIC'.",
        )

        # --- ADDED: Arguments for Dead-Letter Queue paths ---
        parser.add_argument(
            "--parse_dlq_path",
            required=True,
            help="GCS path for parse-failure dead-letter files.",
        )
        parser.add_argument(
            "--processing_dlq_path",
            required=True,
            help="GCS path for processing-failure dead-letter files.",
        )

        parser.add_argument(
            "--model_endpoint_url", required=True, help="URL of the model service."
        )
        parser.add_argument(
            "--window_duration_seconds",
            type=int,
            default=120,
            help="Window duration (s).",
        )
        parser.add_argument(
            "--allowed_lateness_seconds",
            type=int,
            default=60,
            help="Allowed lateness (s).",
        )
        parser.add_argument(
            "--ml_batch_size", type=int, default=50, help="Batch size for ML."
        )
        parser.add_argument(
            "--allowed_wait_time_seconds",
            type=int,
            default=2,
            help="Allowed wait time for ML batching.",
        )
        parser.add_argument(
            "--ml_batch_max_size", type=int, default=100, help="Max batch size for ML."
        )
        parser.add_argument(
            "--use_emulator",
            action="store_true",
            default=False,
            help="Use a local Firestore emulator.",
        )
        parser.add_argument(
            "--emulator_host",
            required=False,
            help="The host:port for a local Firestore emulator (e.g., 'localhost:8080').",
        )


def build(options: FraudDetectionPipelineOptions):
    options.view_as(StandardOptions).streaming = True
    options.view_as(SetupOptions).save_main_session = True

    gcp_options = options.view_as(GoogleCloudOptions)
    p = beam.Pipeline(options=options)

    # Step 1 & 2: Read and Parse with Dead-Lettering
    raw_messages = p | "ReadFromPubSub" >> ReadFromPubSub(topic=options.input_topic)

    parsing_results = raw_messages | "ParseAndTimestamp" >> beam.ParDo(
        transforms.ParseAndTimestampDoFn()
    ).with_outputs(transforms.failed_parse_tag, main="parsed_transactions")

    parsed_transactions = parsing_results.parsed_transactions
    failed_parses = parsing_results[transforms.failed_parse_tag]

    # Step 3: Dead-Letter Sink for Parsing Failures
    (
        failed_parses
        | "DecodeFailedParses" >> beam.Map(lambda b: b.decode("utf-8", "ignore"))
        | "WindowFailedParses"
        >> beam.WindowInto(FixedWindows(options.window_duration_seconds))
        | "WriteParseDLQ"
        >> WriteToFiles(
            path=os.path.join(
                options.parse_dlq_path, str(datetime.now().isoformat(timespec="hours"))
            ),
        )
    )

    # Step 4: ML Processing with Dead-Lettering ---
    prediction_results = (
        parsed_transactions
        | "AddMLFanoutKey" >> beam.ParDo(transforms.AddRandomKey(2))
        | "BatchForML"
        >> beam.GroupIntoBatches(
            options.ml_batch_size, options.allowed_wait_time_seconds
        )
        | "RunMLInference"
        >> beam.ParDo(
            transforms.PredictFraudBatchDoFn(
                endpoint_url=options.model_endpoint_url,
                project_id=gcp_options.project,
                use_emulator=options.use_emulator,
                emulator_host=options.emulator_host,
            )
        ).with_outputs(transforms.failed_processing_tag, main="successful_predictions")
    )

    successful_predictions = prediction_results.successful_predictions
    failed_batches = prediction_results[transforms.failed_processing_tag]

    # Step 5: Dead-Letter Sink for Processing Failures 
    (
        failed_batches
        | "FailedBatchToString"
        >> beam.Map(lambda batch: json.dumps(batch, default=str))
        | "WindowFailedProcessing"
        >> beam.WindowInto(FixedWindows(options.window_duration_seconds))
        | "WriteProcessingDLQ"
        >> WriteToFiles(
            path=os.path.join(
                options.processing_dlq_path,
                str(datetime.now().isoformat(timespec="hours")),
            ),
        )
    )

    formatting_results = successful_predictions | "Format for Output" >> beam.ParDo(
        transforms.FormatForPubSubDoFn()
    ).with_outputs(transforms.failed_format_tag, main="formatted_transactions")

    formatted_transactions = formatting_results.formatted_transactions
    failed_formats = formatting_results[transforms.failed_format_tag]

    # Handle the formatting dead-letter queue (optional but good practice)
    (
        failed_formats
        | "Window Failed Formats"
        >> beam.WindowInto(FixedWindows(options.window_duration_seconds))
        | "WriteFormatDLQ"
        >> WriteToFiles(
            path=os.path.join(
                options.processing_dlq_path,
                "format_failures",
                str(datetime.now().isoformat(timespec="hours")),
            )
        )
    )

    # Continue the main pipeline path
    # (
    #     formatted_transactions
    #     | "Serialize to JSON" >> beam.Map(lambda elem: json.dumps(elem))
    #     | "Encode to Bytes" >> beam.Map(lambda s: s.encode("utf-8"))
    #     | "Publish Scored Transaction" >> WriteToPubSub(topic=options.output_topic)
    # )

    (
        formatted_transactions
        # Step 1: Add a key to each element to allow for batching.
        # Using a constant key (e.g., 1) groups all elements together, but for
        # parallelism, a random key might be better if the volume is very high.
        # For simplicity, we'll use a constant key.
        | "AddKeyForPubSubBatching" >> beam.ParDo(transforms.AddRandomKey(2))

        # Step 2: Group the keyed elements into batches.
        # This creates batches of up to 100 elements or waits a maximum of 5 seconds.
        # You should tune these values for your specific needs.
        | "GroupIntoPubSubBatches" >> beam.GroupIntoBatches(options.ml_batch_size, options.allowed_wait_time_seconds)

        # Step 3: At this point, we have a PCollection of (key, [list_of_elements]).
        # We only want the list of elements for our payload.
        | "ExtractBatchList" >> beam.Map(lambda key_value_tuple: key_value_tuple[1])

        # Step 4: Serialize the entire list (the batch) into a single JSON string.
        # The output will be a string like: '[{"tx_1"}, {"tx_2"}, ...]'
        | "Serialize Batch to JSON" >> beam.Map(lambda batch_list: json.dumps(batch_list))

        # Step 5: Encode the single JSON string to bytes for Pub/Sub.
        | "Encode Batch to Bytes" >> beam.Map(lambda json_string: json_string.encode("utf-8"))

        # Step 6: Publish the byte string. Each byte string now represents an entire
        # batch and will be published as a single Pub/Sub message.
        | "Publish Scored Batch" >> WriteToPubSub(topic=options.output_topic)
    )
    
    # Step 6: The "happy path" analytics write
    grouped_data = (
        successful_predictions
        | "WindowForGCS"
        >> beam.WindowInto(
            FixedWindows(options.window_duration_seconds),
            allowed_lateness=options.allowed_lateness_seconds,
            accumulation_mode=config.ACCUMULATION_MODE,
        )
        | "AddGCSFanoutKey" >> beam.ParDo(transforms.AddRandomKey(2))
        | "GroupForGCS" >> beam.GroupByKey()
    )

    gcs_write_results = (
        grouped_data  # The output of "GroupForGCS"
        | "WriteWindowedBatch"
        >> beam.ParDo(
            pipeline_io.WriteWindowedBatchToGCS(
                base_path=os.path.join(options.output_path, "final"),
                schema=pipeline_schema.transaction_schema,
                use_emulator=options.use_emulator,
            )
        ).with_outputs(
            pipeline_io.failed_gcs_write_tag, main="main_output"
        )  # Specify the tag
    )

    return p
