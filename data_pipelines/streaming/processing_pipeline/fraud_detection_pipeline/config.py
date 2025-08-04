from apache_beam.transforms.trigger import AccumulationMode


ACCUMULATION_MODE = AccumulationMode.DISCARDING

# That is just used for convenience
CONSTANT_BATCH_KEY = "hourly_batch"