set -e
set -u
set -o pipefail

# Default Values
REGION="us-central1"
IS_UPDATE="false"

# help
usage() {
  echo "Usage: $0 --project-id <ID> --gcs-bucket <BUCKET> --topic-id <TOPIC> --model-url <URL> --job-source-dir <DIR> [options]"
  echo
  echo "Required arguments:"
  echo "  --project-id           Google Cloud project ID."
  echo "  --gcs-output-bucket      GCS bucket for the output data (raw transactions)."
  echo "  --gcs-dlq-bucket      GCS bucket for the DLQ data."
  echo "  --input-topic-id               Pub/Sub topic ID to read from."
  echo "  --output-topic-id              Pub/Sub topic ID to write to."
  echo "  --model-url              URL of the deployed ML model."
  echo "  --job-source-dir         Relative path to the Dataflow job's source code directory."
  echo
  echo "Optional arguments:"
  echo "  --region                 GCP region (default: us-central1)."
  echo "  --sa-name                Service account name (e.g., dataflow-sa)."
  echo "  --update                 Set this flag to update an existing job."
  echo "  --job-name-to-update     The exact name of the job to update (Required if --update is set)."
  exit 1
}

# Parsing args
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --project-id) GCP_PROJECT_ID="$2"; shift ;;
        --gcs-output-bucket) GCS_OUTPUT_BUCKET="$2"; shift ;;
        --gcs-dlq-bucket) GCS_DLQ_BUCKET="$2"; shift ;;
        --input-topic-id) INPUT_TOPIC_ID="$2"; shift ;;
        --output-topic-id) OUTPUT_TOPIC_ID="$2"; shift ;;
        --model-url) MODEL_ENDPOINT_URL="$2"; shift ;;
        --job-source-dir) JOB_SOURCE_DIR="$2"; shift ;; # <<< MODIFIED: New argument
        --region) REGION="$2"; shift ;;
        --sa-name) SERVICE_ACCOUNT_NAME="$2"; shift ;;
        --update) IS_UPDATE="true" ;;
        --job-name-to-update) EXISTING_JOB_NAME_TO_UPDATE="$2"; shift ;;
        *) echo "Unknown parameter passed: $1"; usage ;;
    esac
    shift
done

# aguments validation
if [[ -z "${GCP_PROJECT_ID-}" || -z "${GCS_OUTPUT_BUCKET-}" || -z "${INPUT_TOPIC_ID-}" || -z "${OUTPUT_TOPIC_ID-}" || -z "${MODEL_ENDPOINT_URL-}" || -z "${JOB_SOURCE_DIR-}" || -z "${SERVICE_ACCOUNT_NAME-}" ]]; then
  echo " ERROR: Missing one or more required arguments." && usage
fi


# constructing variables
JOB_NAME_PREFIX="fraud-detection-streaming"
SERVICE_ACCOUNT_EMAIL="${SERVICE_ACCOUNT_NAME}@${GCP_PROJECT_ID}.iam.gserviceaccount.com"
SETUP_FILE_PATH="${JOB_SOURCE_DIR}/setup.py"
MAIN_SCRIPT_PATH="${JOB_SOURCE_DIR}/main.py"
INPUT_TOPIC_PATH="projects/${GCP_PROJECT_ID}/topics/${INPUT_TOPIC_ID}"
OUTPUT_TOPIC_PATH="projects/${GCP_PROJECT_ID}/topics/${OUTPUT_TOPIC_ID}"
STAGING_LOCATION="gs://${GCS_OUTPUT_BUCKET}/${JOB_NAME_PREFIX}/staging/"
TEMP_LOCATION="gs://${GCS_OUTPUT_BUCKET}/${JOB_NAME_PREFIX}/temp/"
OUTPUT_PATH="gs://${GCS_OUTPUT_BUCKET}/raw_processed_transactions/"
DLQ_PATH="gs://${GCS_DLQ_BUCKET}/"



if [[ "${IS_UPDATE}" == "true" ]]; then
  if [[ -z "${EXISTING_JOB_NAME_TO_UPDATE-}" ]]; then
    echo " ERROR: When using --update, you must provide a job name with --job-name-to-update." && exit 1
  fi
  JOB_NAME="${EXISTING_JOB_NAME_TO_UPDATE}"
  UPDATE_FLAG="--update"
else
  JOB_NAME="${JOB_NAME_PREFIX}-$(date +%Y%m%d-%H%M%S)"
  UPDATE_FLAG=""
fi

echo ">>> Starting deployment with the following configuration:"
echo "    Job Source Directory: ${JOB_SOURCE_DIR}"
echo "    Project ID: ${GCP_PROJECT_ID}"
echo "    Service Account: ${SERVICE_ACCOUNT_EMAIL}"
echo "    Job Name: ${JOB_NAME}"
echo "-------------------------------------------------------------------"

echo ">>> Deploying Dataflow job '${JOB_NAME}'..."

python3 "${MAIN_SCRIPT_PATH}" \
  --runner DataflowRunner \
  --project "${GCP_PROJECT_ID}" \
  --region "${REGION}" \
  --job_name "${JOB_NAME}" \
  ${UPDATE_FLAG} \
  --service_account_email "${SERVICE_ACCOUNT_EMAIL}" \
  --staging_location "${STAGING_LOCATION}" \
  --temp_location "${TEMP_LOCATION}" \
  --subnetwork "regions/${REGION}/subnetworks/default" \
  --ml_batch_size 50 \
  --window_duration_seconds 300 \
  --setup_file "${SETUP_FILE_PATH}" \
  --input_topic "${INPUT_TOPIC_PATH}" \
  --output_path "${OUTPUT_PATH}" \
  --output_topic "${OUTPUT_TOPIC_PATH}" \
  --parse_dlq_path "${DLQ_PATH}/parsing_dlq/" \
  --processing_dlq_path "${DLQ_PATH}/processing_dlq/" \
  --model_endpoint_url "${MODEL_ENDPOINT_URL}"\
  --worker_machine_type "n1-standard-2" \
  --max_num_workers "10" \
  --streaming &

echo ""
echo ">>>  Deployment command sent successfully!"