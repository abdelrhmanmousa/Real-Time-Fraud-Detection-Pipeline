set -e
set -o pipefail

# --- Usage and Help Function ---
usage() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Deploys a fully configurable service to Cloud Run."
    echo ""
    echo "Parameter Precedence: Command-line arguments > Environment variables > config.sh"
    echo ""
    echo "Options:"
    echo "  -p <project_id>      GCP Project ID. (Env: GCP_PROJECT_ID_ENV)"
    echo "  -r <region>          GCP Region. (Env: GCP_REGION_ENV)"
    echo "  -s <service_name>    Cloud Run Service Name. (Env: SERVICE_NAME_ENV)"
    echo "  -t <topic_id>        Input Pub/Sub Topic ID. (Env: INPUT_TOPIC_ID_ENV)"
    echo "  -d <source_dir>      (Optional) Source code directory. (Env: SOURCE_DIR_ENV)"
    echo ""
    echo "  Performance Options:"
    echo "  -c <cpu>             (Optional) Number of vCPUs. Default: ${CPU}."
    echo "  -e <memory>          (Optional) Memory (e.g., 512Mi, 1Gi). Default: ${MEMORY}."
    echo "  -o <concurrency>     (Optional) Max concurrent requests. Default: ${CONCURRENCY}."
    echo "  -x <timeout>         (Optional) Request timeout in seconds. Default: ${TIMEOUT}."
    echo ""
    echo "  Scaling Options:"
    echo "  -m <min_instances>   (Optional) Min instances. Default: 0."
    echo "  -n <max_instances>   (Optional) Max instances. Default: 2."
    echo ""
    echo "  -h                   Display this help message and exit."
    exit 1
}

# --- Initialize Variables ---
# Required
GCP_PROJECT_ID=""
GCP_REGION=""
SERVICE_NAME=""
INPUT_TOPIC_ID=""
# Optional with defaults
SOURCE_DIR="."
MIN_INSTANCES=0
MAX_INSTANCES=2
CPU="1"
MEMORY="512Mi"
CONCURRENCY="80"
TIMEOUT="300"

# --- Parameter Loading and Precedence ---
# 1. Load from config.sh (Lowest Priority)
if [ -f "./config.sh" ]; then
    source ./config.sh
fi
# 2. Override with Environment Variables (Medium Priority)
if [ -n "${GCP_PROJECT_ID_ENV-}" ]; then GCP_PROJECT_ID="$GCP_PROJECT_ID_ENV"; fi
if [ -n "${GCP_REGION_ENV-}" ]; then GCP_REGION="$GCP_REGION_ENV"; fi
if [ -n "${SERVICE_NAME_ENV-}" ]; then SERVICE_NAME="$SERVICE_NAME_ENV"; fi
if [ -n "${INPUT_TOPIC_ID_ENV-}" ]; then INPUT_TOPIC_ID="$INPUT_TOPIC_ID_ENV"; fi
if [ -n "${SOURCE_DIR_ENV-}" ]; then SOURCE_DIR="$SOURCE_DIR_ENV"; fi
if [ -n "${CPU_ENV-}" ]; then CPU="$CPU_ENV"; fi
if [ -n "${MEMORY_ENV-}" ]; then MEMORY="$MEMORY_ENV"; fi
if [ -n "${CONCURRENCY_ENV-}" ]; then CONCURRENCY="$CONCURRENCY_ENV"; fi
if [ -n "${TIMEOUT_ENV-}" ]; then TIMEOUT="$TIMEOUT_ENV"; fi
# 3. Override with Command-Line Arguments (Highest Priority)
while getopts "p:r:s:t:d:m:n:c:e:o:x:h" opt; do
  case ${opt} in
    p) GCP_PROJECT_ID=$OPTARG ;; r) GCP_REGION=$OPTARG ;; s) SERVICE_NAME=$OPTARG ;;
    t) INPUT_TOPIC_ID=$OPTARG ;; d) SOURCE_DIR=$OPTARG ;; m) MIN_INSTANCES=$OPTARG ;;
    n) MAX_INSTANCES=$OPTARG ;; c) CPU=$OPTARG ;; e) MEMORY=$OPTARG ;;
    o) CONCURRENCY=$OPTARG ;; x) TIMEOUT=$OPTARG ;; h) usage ;; \?) usage ;;
  esac
done

# --- Validate Parameters ---
if [ -z "$GCP_PROJECT_ID" ] || [ -z "$GCP_REGION" ] || [ -z "$SERVICE_NAME" ] || [ -z "$INPUT_TOPIC_ID" ]; then echo "ERROR: Required parameter missing." && usage; fi
if [ ! -d "$SOURCE_DIR" ]; then echo "ERROR: Source directory not found at '${SOURCE_DIR}'." && exit 1; fi

EVENTARC_TRIGGER_NAME="${SERVICE_NAME}-pubsub-trigger"

# --- Confirm Final Parameters with User ---
echo "--- Deployment Configuration ---"
echo "  Source Directory:      ${SOURCE_DIR}"
echo "  GCP Project ID:        ${GCP_PROJECT_ID}"
echo "  GCP Region:            ${GCP_REGION}"
echo "  Cloud Run Service:     ${SERVICE_NAME}"
echo "  Input Pub/Sub Topic:   ${INPUT_TOPIC_ID}"
echo "--- Performance & Scaling ---"
echo "  vCPUs:                 ${CPU}"
echo "  Memory:                ${MEMORY}"
echo "  Concurrency:           ${CONCURRENCY}"
echo "  Timeout:               ${TIMEOUT}s"
echo "  Min/Max Instances:     ${MIN_INSTANCES} / ${MAX_INSTANCES}"
echo "--------------------------------"
read -p "Do you want to continue? (y/n) " -n 1 -r; echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then echo "Deployment cancelled." && exit 1; fi

# --- Get Default Service Account ---
# For simplicity, this script uses the default Compute Engine service account.
# Eventarc needs the email address of the identity that will be invoking the trigger.
echo "INFO: Fetching default compute service account..."
PROJECT_NUMBER=$(gcloud projects describe "${GCP_PROJECT_ID}" --format='value(projectNumber)')
DEFAULT_COMPUTE_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

gcloud run services add-iam-policy-binding "${SERVICE_NAME}" \
    --project="${GCP_PROJECT_ID}" \
    --region="${GCP_REGION}" \
    --platform=managed \
    --member="serviceAccount:${DEFAULT_COMPUTE_SA}" \
    --role="roles/run.invoker" > /dev/null


# --- Step 1: Enable APIs ---
echo "INFO: Enabling required services..."
gcloud services enable run.googleapis.com cloudbuild.googleapis.com artifactregistry.googleapis.com iam.googleapis.com pubsub.googleapis.com eventarc.googleapis.com --project="${GCP_PROJECT_ID}"

# --- Step 2: Deploy Cloud Run Service ---
echo "INFO: Deploying service '${SERVICE_NAME}'..."
gcloud run deploy "${SERVICE_NAME}" \
  --source "${SOURCE_DIR}" \
  --platform managed \
  --project="${GCP_PROJECT_ID}" \
  --region="${GCP_REGION}" \
  --no-allow-unauthenticated \
  --min-instances="${MIN_INSTANCES}" \
  --max-instances="${MAX_INSTANCES}" \
  --cpu="${CPU}" \
  --memory="${MEMORY}" \
  --concurrency="${CONCURRENCY}" \
  --timeout="${TIMEOUT}"


echo "Deleting existing Eventarc trigger '${EVENTARC_TRIGGER_NAME}' to ensure a clean slate..."

# The "|| true" part ensures the script doesn't fail if the trigger doesn't exist
gcloud eventarc triggers delete "${EVENTARC_TRIGGER_NAME}" \
  --project="${GCP_PROJECT_ID}" \
  --location="${GCP_REGION}" \
  || true
# --- Step 3: Create Eventarc Trigger ---
echo "INFO: Creating Eventarc trigger '${EVENTARC_TRIGGER_NAME}'..."

gcloud eventarc triggers create "${EVENTARC_TRIGGER_NAME}" \
  --project="${GCP_PROJECT_ID}" \
  --location="${GCP_REGION}" \
  --destination-run-service="${SERVICE_NAME}" \
  --destination-run-region="${GCP_REGION}" \
  --transport-topic="projects/${GCP_PROJECT_ID}/topics/${INPUT_TOPIC_ID}" \
  --event-filters="type=google.cloud.pubsub.topic.v1.messagePublished" \
  --service-account="${DEFAULT_COMPUTE_SA}"

# --- Final Confirmation ---
echo "✅ Deployment successful!"
echo "Service '${SERVICE_NAME}' is deployed and will be triggered by messages from topic '${INPUT_TOPIC_ID}'."