gcloud dataproc batches submit pyspark main.py \
  --region=us-central1 \
  --deps-bucket=gs://dataproc-temp123 \
  --py-files=deps.zip \
  --jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.26.0.jar
