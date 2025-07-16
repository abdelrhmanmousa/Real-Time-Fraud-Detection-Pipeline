$(gcloud beta emulators pubsub env-init)
export STORAGE_EMULATOR_HOST=http://localhost:4443
# Start the subscriber (Beam)
python main.py --input_topic projects/local-test-project/topics/raw_transactions \
 --output_path gs://fraud-detection-ml-artifacts/raw_transactions \
 --model_endpoint_url https://ml-fraud-detection-service-310091317660.us-central1.run.app/predict \
 --emulator_host localhost:8090 \
 --project_id wired-effort-464808-u3 \
 --streaming
#  --use_emulator \
 


#  --mongo_uri mongodb://root:password@localhost:27017/ \
#  --mongo_db_name transaction_database \
