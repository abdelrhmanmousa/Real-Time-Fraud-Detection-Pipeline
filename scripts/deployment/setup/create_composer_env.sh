set -e
set -u
set -o pipefail

# Usage function
usage() {
  echo "Usage: $0 --project-id <ID> --environment-name <NAME> --location <LOCATION> [options]"
  echo "Options:"
  echo "  --image-version <VERSION>        Image version (default: composer-3-airflow-2.10.5-build.10)"
  echo "  --service-account-name <NAME>    Service account name (e.g., composer-airflow-sa)"
  echo "  --scheduler-count <COUNT>        Scheduler count (default: 1)"
  echo "  --scheduler-cpu <CPU>            Scheduler CPU (default: 0.5)"
  echo "  --scheduler-memory <MEMORY>      Scheduler memory, e.g., 1GB (default: 1GB)"
  echo "  --scheduler-storage <STORAGE>    Scheduler storage, e.g., 0GB (default: 0GB)"
  echo "  --triggerer-count <COUNT>        Triggerer count (default: 0)"
  echo "  --triggerer-cpu <CPU>            Triggerer CPU (default: 0.5)"
  echo "  --triggerer-memory <MEMORY>      Triggerer memory, e.g., 1GB (default: 1GB)"
  echo "  --dag-processor-count <COUNT>    DAG processor count (default: 1)"
  echo "  --dag-processor-cpu <CPU>        DAG processor CPU (default: 0.5)"
  echo "  --dag-processor-memory <MEMORY>  DAG processor memory, e.g., 1GB (default: 1GB)"
  echo "  --dag-processor-storage <STORAGE> DAG processor storage, e.g., 0GB (default: 0GB)"
  echo "  --web-server-cpu <CPU>           Web server CPU (default: 1)"
  echo "  --web-server-memory <MEMORY>     Web server memory, e.g., 2GB (default: 2GB)"
  echo "  --web-server-storage <STORAGE>   Web server storage, e.g., 0GB (default: 0GB)"
  echo "  --worker-cpu <CPU>               Worker CPU (default: 0.5)"
  echo "  --worker-memory <MEMORY>         Worker memory, e.g., 1GB (default: 1GB)"
  echo "  --worker-storage <STORAGE>       Worker storage, e.g., 0GB (default: 0GB)"
  echo "  --min-workers <COUNT>            Minimum workers (default: 1)"
  echo "  --max-workers <COUNT>            Maximum workers (default: 1)"
  exit 1
}

# Default values
IMAGE_VERSION="composer-3-airflow-2.10.5-build.10"
SERVICE_ACCOUNT_NAME="composer-airflow-sa"
SCHEDULER_COUNT=1
SCHEDULER_CPU=0.5
SCHEDULER_MEMORY="1GB"
SCHEDULER_STORAGE="2GB"
TRIGGERER_COUNT=0
TRIGGERER_CPU=0.5
TRIGGERER_MEMORY="1GB"
DAG_PROCESSOR_COUNT=1
DAG_PROCESSOR_CPU=0.5
DAG_PROCESSOR_MEMORY="1GB"
DAG_PROCESSOR_STORAGE="0GB"
WEB_SERVER_CPU=1
WEB_SERVER_MEMORY="2GB"
WEB_SERVER_STORAGE="0GB"
WORKER_CPU=0.5
WORKER_MEMORY="1GB"
WORKER_STORAGE="0GB"
MIN_WORKERS=1
MAX_WORKERS=1

# Parse arguments
while [[ "$#" -gt 0 ]]; do
  case $1 in
    --project-id) PROJECT_ID="$2"; shift ;;
    --environment-name) ENVIRONMENT_NAME="$2"; shift ;;
    --location) LOCATION="$2"; shift ;;
    --image-version) IMAGE_VERSION="$2"; shift ;;
    --service-account-name) SERVICE_ACCOUNT_NAME="$2"; shift ;;
    --scheduler-count) SCHEDULER_COUNT="$2"; shift ;;
    --scheduler-cpu) SCHEDULER_CPU="$2"; shift ;;
    --scheduler-memory) SCHEDULER_MEMORY="$2"; shift ;;
    --scheduler-storage) SCHEDULER_STORAGE="$2"; shift ;;
    --triggerer-count) TRIGGERER_COUNT="$2"; shift ;;
    --triggerer-cpu) TRIGGERER_CPU="$2"; shift ;;
    --triggerer-memory) TRIGGERER_MEMORY="$2"; shift ;;
    --dag-processor-count) DAG_PROCESSOR_COUNT="$2"; shift ;;
    --dag-processor-cpu) DAG_PROCESSOR_CPU="$2"; shift ;;
    --dag-processor-memory) DAG_PROCESSOR_MEMORY="$2"; shift ;;
    --dag-processor-storage) DAG_PROCESSOR_STORAGE="$2"; shift ;;
    --web-server-cpu) WEB_SERVER_CPU="$2"; shift ;;
    --web-server-memory) WEB_SERVER_MEMORY="$2"; shift ;;
    --web-server-storage) WEB_SERVER_STORAGE="$2"; shift ;;
    --worker-cpu) WORKER_CPU="$2"; shift ;;
    --worker-memory) WORKER_MEMORY="$2"; shift ;;
    --worker-storage) WORKER_STORAGE="$2"; shift ;;
    --min-workers) MIN_WORKERS="$2"; shift ;;
    --max-workers) MAX_WORKERS="$2"; shift ;;
    *) echo "Unknown parameter passed: $1"; usage ;;
  esac
  shift
done

# Check required parameters
if [ -z "$PROJECT_ID" ] || [ -z "$ENVIRONMENT_NAME" ] || [ -z "$LOCATION" ]; then
  echo "ERROR: Missing one or more required arguments: --project-id, --environment-name, --location"
  usage
fi

# Construct command
COMMAND=("gcloud" "composer" "environments" "create" "$ENVIRONMENT_NAME" "--project" "$PROJECT_ID" "--location" "$LOCATION" "--image-version" "$IMAGE_VERSION")
if [ -n "$SERVICE_ACCOUNT_NAME" ]; then
  SERVICE_ACCOUNT_EMAIL="${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
  COMMAND+=("--service-account" "$SERVICE_ACCOUNT_EMAIL")
fi
COMMAND+=("--scheduler-count" "$SCHEDULER_COUNT")
COMMAND+=("--scheduler-cpu" "$SCHEDULER_CPU")
COMMAND+=("--scheduler-memory" "$SCHEDULER_MEMORY")
COMMAND+=("--scheduler-storage" "$SCHEDULER_STORAGE")
COMMAND+=("--triggerer-count" "$TRIGGERER_COUNT")
if [ $TRIGGERER_COUNT -gt 0 ]; then
  COMMAND+=("--triggerer-cpu" "$TRIGGERER_CPU")
  COMMAND+=("--triggerer-memory" "$TRIGGERER_MEMORY")
fi
COMMAND+=("--dag-processor-count" "$DAG_PROCESSOR_COUNT")
COMMAND+=("--dag-processor-cpu" "$DAG_PROCESSOR_CPU")
COMMAND+=("--dag-processor-memory" "$DAG_PROCESSOR_MEMORY")
COMMAND+=("--dag-processor-storage" "$DAG_PROCESSOR_STORAGE")
COMMAND+=("--web-server-cpu" "$WEB_SERVER_CPU")
COMMAND+=("--web-server-memory" "$WEB_SERVER_MEMORY")
COMMAND+=("--web-server-storage" "$WEB_SERVER_STORAGE")
COMMAND+=("--worker-cpu" "$WORKER_CPU")
COMMAND+=("--worker-memory" "$WORKER_MEMORY")
COMMAND+=("--worker-storage" "$WORKER_STORAGE")
COMMAND+=("--min-workers" "$MIN_WORKERS")
COMMAND+=("--max-workers" "$MAX_WORKERS")

# Execute command
echo "Creating Cloud Composer environment with the following command:"
echo "${COMMAND[@]}"
"${COMMAND[@]}"
echo "Environment creation initiated. This may take up to 15-20 minutes to complete."