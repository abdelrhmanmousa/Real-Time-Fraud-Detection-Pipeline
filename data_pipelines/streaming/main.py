import logging
from fraud_detection_pipeline.pipeline import build, FraudDetectionPipelineOptions # Import the custom options and build function
from apache_beam.options.pipeline_options import StandardOptions


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    pipeline_options = FraudDetectionPipelineOptions()

    logging.info("Starting pipeline with arguments: %s", pipeline_options.get_all_options())

    # Build the pipeline with the unified options object
    pipeline = build(pipeline_options)
    result = pipeline.run()

    is_streaming = pipeline_options.view_as(StandardOptions).streaming
    if not is_streaming:
        logging.info("Running in batch mode, waiting for pipeline to complete.")
        result.wait_until_finish()
        logging.info("Pipeline finished.")
    else:
        logging.info("Pipeline is now running in streaming mode on the selected runner.")