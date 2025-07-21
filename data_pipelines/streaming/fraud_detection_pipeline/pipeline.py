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

class FraudDetectionPipelineOptions(PipelineOptions):
    """
    Defines custom command-line arguments for the fraud detection pipeline.
    Standard arguments like --project, --runner, --region are handled automatically.
    """
    @classmethod
    def _add_argparse_args(cls, parser):
        # Add only the arguments that are specific to your pipeline's logic
        parser.add_argument("--input_topic", required=True, help="Input Pub/Sub topic in format 'projects/PROJECT/topics/TOPIC'.")
        parser.add_argument("--output_path", required=True, help="Base GCS path for output.")
        parser.add_argument("--model_endpoint_url", required=True, help="URL of the model service.")
        parser.add_argument("--window_duration_seconds", type=int, default=20, help="Window duration (s).")
        parser.add_argument("--allowed_lateness_seconds", type=int, default=60, help="Allowed lateness (s).")
        parser.add_argument("--ml_batch_size", type=int, default=50, help="Batch size for ML.")
        parser.add_argument("--ml_batch_max_size", type=int, default=100, help="Max batch size for ML.")
        parser.add_argument("--use_emulator", action='store_true', default=False, help="Use a local Firestore emulator.")
        parser.add_argument("--emulator_host", required=False, help="The host:port for a local Firestore emulator (e.g., 'localhost:8080').")



def build(options: FraudDetectionPipelineOptions):
    """
    Defines and returns a runnable Beam pipeline using the parsed options.
    """
    # Set necessary options that are not passed as flags
    options.view_as(StandardOptions).streaming = True
    options.view_as(SetupOptions).save_main_session = True

    # The 'options' object now correctly contains all arguments.
    # We can access standard options using .view_as()
    gcp_options = options.view_as(GoogleCloudOptions)
    standard_options = options.view_as(StandardOptions)

    print(f"The current runner is >>> {standard_options.runner} <<<")
    print(f"The pipeline will run in project >>> {gcp_options.project} <<<")

    p = beam.Pipeline(options=options)

    # --- All pipeline logic below is UNCHANGED, it just accesses arguments correctly ---

    ml_batch_size = options.ml_batch_size
    window_size_secs = options.window_duration_seconds
    allowed_lateness = options.allowed_lateness_seconds

    raw = (
        p
        | "ReadFromPubSub" >> ReadFromPubSub(topic=options.input_topic)
        | "ParseAndTimestamp" >> beam.ParDo(transforms.ParseAndTimestampDoFn())
    )

    # Path A: ML inference in micro-batches
    ml_results = (
        raw
        | "KeyForML" >> beam.Map(lambda e: ("batch", e))
        | "BatchForML" >> beam.GroupIntoBatches.WithShardedKey(ml_batch_size, 3)
        | "RunMLInference"
        >> beam.ParDo(
            transforms.PredictFraudBatchDoFn(
                endpoint_url=options.model_endpoint_url,
                project_id=gcp_options.project, # Access project ID correctly
                use_emulator=options.use_emulator,
                emulator_host=options.emulator_host,
            )
        )
    )
    
    # Path B: analytics / windowed writes
    (
        ml_results
        | "AttachEventTs" >> beam.ParDo(transforms.AttachEventTimestampDoFn())
        | "Window"
        >> beam.WindowInto(
            FixedWindows(window_size_secs),
            allowed_lateness=allowed_lateness,
            accumulation_mode=config.ACCUMULATION_MODE,
        )
        | "KeyForGCS" >> beam.Map(lambda e: (config.CONSTANT_BATCH_KEY, e))
        | "GroupForGCS" >> beam.GroupByKey()
        | "WriteWindowedBatch"
        >> beam.ParDo(
            pipeline_io.WriteWindowedBatchToGCS(
                base_path=os.path.join(options.output_path, "final"),
                schema=pipeline_schema.transaction_schema,
                use_emulator=options.use_emulator,
            )
        )
    )
    
    return p