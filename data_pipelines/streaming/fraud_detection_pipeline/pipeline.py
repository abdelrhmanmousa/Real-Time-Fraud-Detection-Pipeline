# import os
# import apache_beam as beam
# from apache_beam.io import ReadFromPubSub
# from apache_beam.options.pipeline_options import (
#     PipelineOptions,
#     GoogleCloudOptions,
#     StandardOptions,
#     SetupOptions
# )
# from apache_beam.transforms.window import FixedWindows

# from . import config
# from . import pipeline_io
# from . import schema as pipeline_schema
# from . import transforms

# class FraudDetectionPipelineOptions(PipelineOptions):
#     """
#     Defines custom command-line arguments for the fraud detection pipeline.
#     Standard arguments like --project, --runner, --region are handled automatically.
#     """
#     @classmethod
#     def _add_argparse_args(cls, parser):
#         # Add only the arguments that are specific to your pipeline's logic
#         parser.add_argument("--input_topic", required=True, help="Input Pub/Sub topic in format 'projects/PROJECT/topics/TOPIC'.")
#         parser.add_argument("--output_path", required=True, help="Base GCS path for output.")
#         parser.add_argument("--model_endpoint_url", required=True, help="URL of the model service.")
#         parser.add_argument("--window_duration_seconds", type=int, default=20, help="Window duration (s).")
#         parser.add_argument("--allowed_lateness_seconds", type=int, default=60, help="Allowed lateness (s).")
#         parser.add_argument("--ml_batch_size", type=int, default=50, help="Batch size for ML.")
#         parser.add_argument("--ml_batch_max_size", type=int, default=100, help="Max batch size for ML.")
#         parser.add_argument("--use_emulator", action='store_true', default=False, help="Use a local Firestore emulator.")
#         parser.add_argument("--emulator_host", required=False, help="The host:port for a local Firestore emulator (e.g., 'localhost:8080').")



# def build(options: FraudDetectionPipelineOptions):
#     """
#     Defines and returns a runnable Beam pipeline using the parsed options.
#     """
#     # Set necessary options that are not passed as flags
#     options.view_as(StandardOptions).streaming = True
#     options.view_as(SetupOptions).save_main_session = True

#     # The 'options' object now correctly contains all arguments.
#     # We can access standard options using .view_as()
#     gcp_options = options.view_as(GoogleCloudOptions)
#     standard_options = options.view_as(StandardOptions)

#     print(f"The current runner is >>> {standard_options.runner} <<<")
#     print(f"The pipeline will run in project >>> {gcp_options.project} <<<")

#     p = beam.Pipeline(options=options)

#     # --- All pipeline logic below is UNCHANGED, it just accesses arguments correctly ---

#     ml_batch_size = options.ml_batch_size
#     window_size_secs = options.window_duration_seconds
#     allowed_lateness = options.allowed_lateness_seconds

#     raw = (
#         p
#         | "ReadFromPubSub" >> ReadFromPubSub(topic=options.input_topic)
#         | "ParseAndTimestamp" >> beam.ParDo(transforms.ParseAndTimestampDoFn())
#     )

#     # Path A: ML inference in micro-batches
#     ml_results = (
#         raw
#         | "KeyForML" >> beam.Map(lambda e: ("batch", e))
#         | "BatchForML" >> beam.GroupIntoBatches.WithShardedKey(ml_batch_size, 3)
#         | "RunMLInference"
#         >> beam.ParDo(
#             transforms.PredictFraudBatchDoFn(
#                 endpoint_url=options.model_endpoint_url,
#                 project_id=gcp_options.project, # Access project ID correctly
#                 use_emulator=options.use_emulator,
#                 emulator_host=options.emulator_host,
#             )
#         )
#     )
    
#     # Path B: analytics / windowed writes
#     (
#         ml_results
#         | "AttachEventTs" >> beam.ParDo(transforms.AttachEventTimestampDoFn())
#         | "Window"
#         >> beam.WindowInto(
#             FixedWindows(window_size_secs),
#             allowed_lateness=allowed_lateness,
#             accumulation_mode=config.ACCUMULATION_MODE,
#         )
#         | "KeyForGCS" >> beam.Map(lambda e: (config.CONSTANT_BATCH_KEY, e))
#         | "GroupForGCS" >> beam.GroupByKey()
#         | "WriteWindowedBatch"
#         >> beam.ParDo(
#             pipeline_io.WriteWindowedBatchToGCS(
#                 base_path=os.path.join(options.output_path, "final"),
#                 schema=pipeline_schema.transaction_schema,
#                 use_emulator=options.use_emulator,
#             )
#         )
#     )
    
#     return p


import os
import apache_beam as beam
from apache_beam.io import ReadFromPubSub
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    GoogleCloudOptions,
    StandardOptions,
    SetupOptions
)
from apache_beam.transforms.window import FixedWindows

from . import config
from . import pipeline_io
from . import schema as pipeline_schema
from . import transforms

failed_parse_tag = 'failed_parses'
failed_processing_tag = 'failed_batches'

# In your pipeline definition file
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
    """
    Defines and returns a runnable Beam pipeline with dead-lettering.
    """
    options.view_as(StandardOptions).streaming = True
    options.view_as(SetupOptions).save_main_session = True

    gcp_options = options.view_as(GoogleCloudOptions)
    
    p = beam.Pipeline(options=options)

    # --- Step 1: Read from Pub/Sub and apply the first transformation ---
    raw_messages = p | "ReadFromPubSub" >> ReadFromPubSub(topic=options.input_topic)

    # --- Step 2: Use .with_outputs to split the stream from the parsing DoFn ---
    # The result of this ParDo is a special tuple-like object containing all outputs.
    parsing_results = (
        raw_messages
        | "ParseAndTimestamp" >> beam.ParDo(transforms.ParseAndTimestampDoFn()).with_outputs(
                                      transforms.failed_parse_tag, 
                                      main='parsed_transactions'
                                  )
    )

    # --- Step 3: Extract the PCollections from the results using the tags ---
    # Good data continues down the main path
    parsed_transactions = parsing_results.parsed_transactions
    
    # Bad data is diverted to a dead-letter PCollection
    failed_parses = parsing_results[transforms.failed_parse_tag]

    # --- Step 4: Define the Dead-Letter Sink for parsing failures ---
    # This branch of the pipeline takes the failed messages and writes them to GCS.
    # (
    #     failed_parses
    #     | "WindowFailedParses" >> beam.WindowInto(
    #         FixedWindows(300),
    #         accumulation_mode=config.ACCUMULATION_MODE,
    #         ) # e.g., 5-minute windows
    #     # ======================================================================
    #     | "AddKey" >> beam.Map(lambda x: ("FailedToParse", x))
    #     | "Group" >> beam.GroupByKey()
    #     # Now WriteToText is safe because it receives finite, windowed PCollections.
    #     | "WriteParseDLQToGCS" >> beam.io.WriteToText(
    #         os.path.join(options.parse_dlq_path, "parse_errors"),
    #         file_name_suffix=".jsonl",
    #       )
    # )


    # --- Step 5: Process the "happy path" data from the parsing stage ---
    # We use the 'parsed_transactions' PCollection as the input here.
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

    # --- Step 6: Again, extract successful PCollection and dead-letter PCollection ---
    successful_predictions = prediction_results.successful_predictions
    failed_batches = prediction_results[transforms.failed_processing_tag]

    # --- Step 7: Define the Dead-Letter Sink for processing failures ---
    # (
    #     failed_batches
    #     # | "FailedBatchToString" >> beam.Map(lambda batch: f"Failed to process batch: {str(batch)}")
    #     | "WindowFailedProcessing" >> beam.WindowInto(
    #         FixedWindows(300),
    #         accumulation_mode=config.ACCUMULATION_MODE,

    #         )
    #     | "KeyForGCS" >> beam.Map(lambda e: ("FailedToProcess", e))
    #     | "GroupForGCS" >> beam.GroupByKey()
    #     | "WriteProcessingDLQToGCS" >> beam.io.WriteToText(
    #           os.path.join(options.processing_dlq_path, "processing_errors"),
    #           file_name_suffix=".jsonl",
    #         #   max_records_per_shard=1000
    #       )
    # )

    # --- Step 8: The final "happy path" analytics write ---
    # This part of the pipeline now only receives data that has been successfully
    # parsed and processed.
    (
        successful_predictions
        | "AttachEventTs" >> beam.ParDo(transforms.AttachEventTimestampDoFn())
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