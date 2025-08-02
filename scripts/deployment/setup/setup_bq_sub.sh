set -e  # Exit immediately if a command exits with a non-zero status.
set -o pipefail # Fail a pipe if any sub-command fails.

# --- Function to display usage information ---
usage() {
  echo "Usage: $0 -p <PROJECT_ID> -t <TOPIC_NAME> -s <SUBSCRIPTION_NAME> -b <BIGQUERY_TABLE_ID> -d <DEAD_LETTER_TOPIC_NAME>"
  echo ""
  echo "  -p: Google Cloud Project ID."
  echo "  -t: The name of the main Pub/Sub topic to create for incoming data."
  echo "  -s: The name of the BigQuery subscription to create."
  echo "  -b: The full ID of the destination BigQuery table (format: project.dataset.table)."
  echo "  -d: The name of the topic to create for the Dead-Letter Queue (DLQ)."
  echo ""
  exit 1
}

# --- Parse Command-Line Arguments ---
while getopts ":p:t:s:b:d:" opt; do
  case ${opt} in
    p ) PROJECT_ID=$OPTARG;;
    t ) TOPIC_NAME=$OPTARG;;
    s ) SUBSCRIPTION_NAME=$OPTARG;;
    b ) BIGQUERY_TABLE_ID=$OPTARG;;
    d ) DEAD_LETTER_TOPIC_NAME=$OPTARG;;
    \? ) usage;;
  esac
done

# --- Validate that all required arguments were provided ---
if [ -z "${PROJECT_ID}" ] || [ -z "${TOPIC_NAME}" ] || [ -z "${SUBSCRIPTION_NAME}" ] || [ -z "${BIGQUERY_TABLE_ID}" ] || [ -z "${DEAD_LETTER_TOPIC_NAME}" ]; then
    echo "Error: Missing one or more required arguments."
    usage
fi

# --- Check for gcloud CLI ---
if ! command -v gcloud &> /dev/null; then
    echo "Error: gcloud command-line tool not found. Please install the Google Cloud SDK."
    exit 1
fi

echo "✓ All arguments provided and gcloud found. Starting setup..."
echo "--------------------------------------------------------"

# --- 1. Set the active project ---
echo "--- [Step 1/4] Setting active project to ${PROJECT_ID} ---"
gcloud config set project "${PROJECT_ID}"
echo "✓ Project set."
echo ""

# --- 2. Grant Pub/Sub Service Account permissions to write to BigQuery ---
echo "--- [Step 2/4] Configuring IAM permissions for the Pub/Sub service account ---"
PROJECT_NUMBER=$(gcloud projects describe "${PROJECT_ID}" --format="value(projectNumber)")
SERVICE_ACCOUNT="service-${PROJECT_NUMBER}@gcp-sa-pubsub.iam.gserviceaccount.com"

# Check if the binding already exists to make the script idempotent
if gcloud projects get-iam-policy "${PROJECT_ID}" --format="json" | grep -q "\"serviceAccount:${SERVICE_ACCOUNT}\"" && \
   gcloud projects get-iam-policy "${PROJECT_ID}" --format="json" | grep "roles/bigquery.dataEditor" | grep -q "\"serviceAccount:${SERVICE_ACCOUNT}\""; then
  echo "✓ IAM binding for Pub/Sub service account to BigQuery Data Editor already exists. Skipping."
else
  echo "  Adding IAM policy binding: Granting BigQuery Data Editor role to ${SERVICE_ACCOUNT}..."
  gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
      --member="serviceAccount:${SERVICE_ACCOUNT}" \
      --role="roles/bigquery.dataEditor" \
      --condition=None > /dev/null
  echo "✓ IAM binding created."
fi
echo ""

# --- 3. Create Pub/Sub Topics (Main and Dead-Letter) ---
echo "--- [Step 3/4] Creating Pub/Sub topics ---"
# Loop to create both topics to avoid code repetition
for topic in "${TOPIC_NAME}" "${DEAD_LETTER_TOPIC_NAME}"; do
  if gcloud pubsub topics describe "$topic" &> /dev/null; then
    echo "✓ Topic '${topic}' already exists. Skipping creation."
  else
    echo "  Creating topic '${topic}'..."
    if [ "$topic" == "$TOPIC_NAME" ]; then
      # Main topic gets the 3-day retention policy
      gcloud pubsub topics create "$topic" --message-retention-duration="3d"
    else
      # DLQ topic uses default retention
      gcloud pubsub topics create "$topic"
    fi
    echo "✓ Topic '${topic}' created."
  fi
done
echo ""

# --- 4. Create the BigQuery Subscription ---
echo "--- [Step 4/4] Creating the Pub/Sub subscription for BigQuery ---"
if gcloud pubsub subscriptions describe "${SUBSCRIPTION_NAME}" &> /dev/null; then
  echo "✓ BigQuery subscription '${SUBSCRIPTION_NAME}' already exists. Skipping creation."
else
  echo "  Creating BigQuery subscription '${SUBSCRIPTION_NAME}'..."
  echo "  - Source Topic: ${TOPIC_NAME}"
  echo "  - Destination BigQuery Table: ${BIGQUERY_TABLE_ID}"
  echo "  - Dead-Letter Topic: ${DEAD_LETTER_TOPIC_NAME}"

  gcloud pubsub subscriptions create "${SUBSCRIPTION_NAME}" \
      --topic="${TOPIC_NAME}" \
      --bigquery-table="${BIGQUERY_TABLE_ID}" \
      --use-table-schema \
      --dead-letter-topic="${DEAD_LETTER_TOPIC_NAME}" \
      --max-delivery-attempts=5

  echo "✓ BigQuery subscription '${SUBSCRIPTION_NAME}' created."
fi
echo ""

echo "--------------------------------------------------------"
echo "✅ Setup complete!"
echo "Your pipeline is now configured to stream data from Pub/Sub topic '${TOPIC_NAME}' to BigQuery table '${BIGQUERY_TABLE_ID}'."