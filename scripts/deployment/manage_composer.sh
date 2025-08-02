# Default values for optional arguments 
readonly DEFAULT_LOCATION="us-central1"
readonly DEFAULT_IMAGE_VERSION="composer-3-airflow-2.10.5-build.10"

#  Default Creation & Start Parameters 
readonly DEFAULT_ENVIRONMENT_SIZE="small"
# Scheduler
readonly DEFAULT_SCHEDULER_COUNT=1
readonly DEFAULT_SCHEDULER_CPU=0.5
readonly DEFAULT_SCHEDULER_MEMORY="1GB"
readonly DEFAULT_SCHEDULER_STORAGE="1GB"
# Web Server
readonly DEFAULT_WEB_SERVER_CPU=0.5
readonly DEFAULT_WEB_SERVER_MEMORY="1.875GB"
readonly DEFAULT_WEB_SERVER_STORAGE="1GB"
# Worker (also used for 'start' command)
readonly DEFAULT_WORKER_MIN=1
readonly DEFAULT_WORKER_MAX=2
readonly DEFAULT_WORKER_CPU=0.5
readonly DEFAULT_WORKER_MEMORY="1.875GB"
readonly DEFAULT_WORKER_STORAGE="1GB"
# Triggerer
readonly DEFAULT_TRIGGERER_COUNT=0 # Disabled by default to save cost
readonly DEFAULT_TRIGGERER_CPU=0.5
readonly DEFAULT_TRIGGERER_MEMORY="0.5GB"
# DAG Processor
readonly DEFAULT_DAG_PROCESSOR_COUNT=1
readonly DEFAULT_DAG_PROCESSOR_CPU=0.5
readonly DEFAULT_DAG_PROCESSOR_MEMORY="2GB"
readonly DEFAULT_DAG_PROCESSOR_STORAGE="1GB"


#help Function
usage() {
  cat << EOF
Usage: $0 <command> [options]

A tool to manage Cloud Composer 3 environments with fine-grained control.

Commands:
  create    Creates a new Composer environment.
  delete    Permanently deletes an existing environment.
  start     Starts a stopped environment by scaling its resources.
  stop      Stops an environment by scaling its resources to zero.
  deploy    Deploys artifacts (DAGs, plugins, data) to an environment.

--------------------------------------------------------------------------------
Required options for all commands:
  --project-id <ID>         Google Cloud project ID.
  --env-name <NAME>         Name of the Composer environment.

--------------------------------------------------------------------------------
Options for 'deploy':
  --source-dir <PATH>         (Required) Path to the source directory. The script will
                            deploy from the 'dags', 'plugins', and 'data'
                            subdirectories if they exist within this path.
  --location <REGION>         (Optional) GCP region (default: ${DEFAULT_LOCATION}).

--------------------------------------------------------------------------------
Options for 'create':
  (Run with '--help' for a full list of detailed creation flags)

--------------------------------------------------------------------------------
Options for 'delete', 'start', 'stop':
  --location <REGION>         (Optional) GCP region (default: ${DEFAULT_LOCATION}).

EOF
  exit 1
}

# Helper Functions 

# Sets the gcloud project configuration.
set_project() {
  echo ">> Targeting project: '${GCP_PROJECT_ID}' in location: '${LOCATION}'"
  gcloud config set project "${GCP_PROJECT_ID}"
}

# Core Functions 

# Creates a new Cloud Composer environment.
do_create() {
  echo ">> Starting Cloud Composer environment creation for '${ENV_NAME}'..."
  set_project

  echo "Enabling the Cloud Composer API (if not already enabled)..."
  gcloud services enable composer.googleapis.com --project="${GCP_PROJECT_ID}" --quiet

  local gcloud_cmd="gcloud composer environments create \"${ENV_NAME}\" \
      --location \"${LOCATION}\" \
      --image-version \"${IMAGE_VERSION}\" \
      --environment-size \"${ENVIRONMENT_SIZE}\" \
      --scheduler-count ${SCHEDULER_COUNT} --scheduler-cpu ${SCHEDULER_CPU} --scheduler-memory ${SCHEDULER_MEMORY} --scheduler-storage ${SCHEDULER_STORAGE} \
      --web-server-cpu ${WEB_SERVER_CPU} --web-server-memory ${WEB_SERVER_MEMORY} --web-server-storage ${WEB_SERVER_STORAGE} \
      --worker-cpu ${WORKER_CPU} --worker-memory ${WORKER_MEMORY} --worker-storage ${WORKER_STORAGE} \
      --min-workers ${MIN_WORKERS} --max-workers ${MAX_WORKERS} \
      --triggerer-count ${TRIGGERER_COUNT} \
      --dag-processor-count ${DAG_PROCESSOR_COUNT} --dag-processor-cpu ${DAG_PROCESSOR_CPU} --dag-processor-memory ${DAG_PROCESSOR_MEMORY} --dag-processor-storage ${DAG_PROCESSOR_STORAGE} \
      --async"

  if [[ ${TRIGGERER_COUNT} -gt 0 ]]; then
    gcloud_cmd+=" --triggerer-cpu ${TRIGGERER_CPU} --triggerer-memory ${TRIGGERER_MEMORY}"
  fi

  echo "Submitting creation request. This may take 20-30 minutes."
  eval "${gcloud_cmd}"

  if [[ $? -eq 0 ]]; then
      echo ">> Successfully submitted creation request for '${ENV_NAME}'."
  else
      echo "X | Error submitting creation request." >&2
      exit 1
  fi
}

# Deletes a Cloud Composer environment.
do_delete() {
  echo "--------------------------------------------------"
  echo "!! WARNING: You are about to PERMANENTLY DELETE the following environment:"
  echo "   Project:     ${GCP_PROJECT_ID}"
  echo "   Environment: ${ENV_NAME}"
  echo "   Location:    ${LOCATION}"
  echo "This action is irreversible."
  echo "--------------------------------------------------"

  read -p "Are you sure you want to continue? (Type 'yes' to confirm): " CONFIRMATION
  if [[ "${CONFIRMATION}" != "yes" ]]; then
      echo "Deletion cancelled."
      exit 0
  fi

  echo ">> Submitting deletion request for '${ENV_NAME}'..."
  set_project
  gcloud composer environments delete "${ENV_NAME}" --location "${LOCATION}" --quiet

  if [[ $? -eq 0 ]]; then
      echo ">> Successfully submitted deletion request for '${ENV_NAME}'."
  else
      echo "X | Error submitting deletion request." >&2
      exit 1
  fi
}

# Starts a Cloud Composer environment.
do_start() {
  echo ">> 'Starting' environment '${ENV_NAME}' by scaling workers to min=${MIN_WORKERS}, max=${MAX_WORKERS}..."
  set_project

  gcloud composer environments update "${ENV_NAME}" \
      --location "${LOCATION}" \
      --update-workloads-config "worker-min-count=${MIN_WORKERS},worker-max-count=${MAX_WORKERS}"

  if [[ $? -eq 0 ]]; then
      echo ">> Successfully submitted start request for '${ENV_NAME}'."
  else
      echo "X | Error submitting start request." >&2
      exit 1
  fi
}

# Stops a Cloud Composer environment.
do_stop() {
  echo ">> 'Stopping' environment '${ENV_NAME}' by scaling workers to 0..."
  echo "(Note: This minimizes costs but does not stop all charges.)"
  set_project

  gcloud composer environments update "${ENV_NAME}" \
      --location "${LOCATION}" \
      --update-workloads-config "worker-min-count=0,worker-max-count=0"

  if [[ $? -eq 0 ]]; then
      echo ">> Successfully submitted stop request for '${ENV_NAME}'."
  else
      echo "X | Error submitting stop request." >&2
      exit 1
  fi
}

# Deploys code to a Cloud Composer environment.
do_deploy() {
  if [[ ! -d "${SOURCE_DIR}" ]]; then
    echo "X | Error: Source directory not found at '${SOURCE_DIR}'." >&2
    exit 1
  fi
  echo ">> Starting deployment from '${SOURCE_DIR}' to environment '${ENV_NAME}'..."
  set_project

  local deployment_failed=false

  # Deploy DAGs if the directory exists
  if [[ -d "${SOURCE_DIR}/dags" ]]; then
    echo "--> Found 'dags' directory. Deploying..."
    gcloud composer environments storage dags import --environment "${ENV_NAME}" --location "${LOCATION}" --source "${SOURCE_DIR}/dags"
    if [[ $? -ne 0 ]]; then echo "X | Error deploying 'dags'." >&2; deployment_failed=true; fi
  else
    echo "--> No 'dags' directory found in source. Skipping."
  fi

  # Deploy Plugins if the directory exists
  if [[ -d "${SOURCE_DIR}/plugins" ]]; then
    echo "--> Found 'plugins' directory. Deploying..."
    gcloud composer environments storage plugins import --environment "${ENV_NAME}" --location "${LOCATION}" --source "${SOURCE_DIR}/plugins"
    if [[ $? -ne 0 ]]; then echo "X | Error deploying 'plugins'." >&2; deployment_failed=true; fi
  else
    echo "--> No 'plugins' directory found in source. Skipping."
  fi

  # Deploy Data (for includes, etc.) if the directory exists
  if [[ -d "${SOURCE_DIR}/data" ]]; then
    echo "--> Found 'data' directory. Deploying..."
    gcloud composer environments storage data import --environment "${ENV_NAME}" --location "${LOCATION}" --source "${SOURCE_DIR}/data"
    if [[ $? -ne 0 ]]; then echo "X | Error deploying 'data'." >&2; deployment_failed=true; fi
  else
    echo "--> No 'data' directory found in source. Skipping."
  fi

  if [[ "${deployment_failed}" == "true" ]]; then
      echo "X | One or more deployment steps failed." >&2
      exit 1
  else
      echo ">> Deployment process completed successfully for '${ENV_NAME}'."
  fi
}


# --- Script Logic ---

if [[ -z "$1" ]] || [[ "$1" == "--help" ]]; then
    usage
fi

MAIN_COMMAND="$1"
shift

# Argument Parsing
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --project-id) GCP_PROJECT_ID="$2"; shift ;;
        --env-name) ENV_NAME="$2"; shift ;;
        --location) LOCATION="$2"; shift ;;
        --image-version) IMAGE_VERSION="$2"; shift ;;
        --source-dir) SOURCE_DIR="$2"; shift ;;
        --environment-size) ENVIRONMENT_SIZE="$2"; shift ;;
        --scheduler-count) SCHEDULER_COUNT="$2"; shift ;;
        --scheduler-cpu) SCHEDULER_CPU="$2"; shift ;;
        --scheduler-memory) SCHEDULER_MEMORY="$2"; shift ;;
        --scheduler-storage) SCHEDULER_STORAGE="$2"; shift ;;
        --web-server-cpu) WEB_SERVER_CPU="$2"; shift ;;
        --web-server-memory) WEB_SERVER_MEMORY="$2"; shift ;;
        --web-server-storage) WEB_SERVER_STORAGE="$2"; shift ;;
        --worker-cpu) WORKER_CPU="$2"; shift ;;
        --worker-memory) WORKER_MEMORY="$2"; shift ;;
        --worker-storage) WORKER_STORAGE="$2"; shift ;;
        --min-workers) MIN_WORKERS="$2"; shift ;;
        --max-workers) MAX_WORKERS="$2"; shift ;;
        --triggerer-count) TRIGGERER_COUNT="$2"; shift ;;
        --triggerer-cpu) TRIGGERER_CPU="$2"; shift ;;
        --triggerer-memory) TRIGGERER_MEMORY="$2"; shift ;;
        --dag-processor-count) DAG_PROCESSOR_COUNT="$2"; shift ;;
        --dag-processor-cpu) DAG_PROCESSOR_CPU="$2"; shift ;;
        --dag-processor-memory) DAG_PROCESSOR_MEMORY="$2"; shift ;;
        --dag-processor-storage) DAG_PROCESSOR_STORAGE="$2"; shift ;;
        *) echo "Unknown parameter passed: $1"; usage ;;
    esac
    shift
done

# --- Set Default Values ---
LOCATION="${LOCATION:-${DEFAULT_LOCATION}}"
IMAGE_VERSION="${IMAGE_VERSION:-${DEFAULT_IMAGE_VERSION}}"
ENVIRONMENT_SIZE="${ENVIRONMENT_SIZE:-${DEFAULT_ENVIRONMENT_SIZE}}"
SCHEDULER_COUNT="${SCHEDULER_COUNT:-${DEFAULT_SCHEDULER_COUNT}}"
SCHEDULER_CPU="${SCHEDULER_CPU:-${DEFAULT_SCHEDULER_CPU}}"
SCHEDULER_MEMORY="${SCHEDULER_MEMORY:-${DEFAULT_SCHEDULER_MEMORY}}"
SCHEDULER_STORAGE="${SCHEDULER_STORAGE:-${DEFAULT_SCHEDULER_STORAGE}}"
WEB_SERVER_CPU="${WEB_SERVER_CPU:-${DEFAULT_WEB_SERVER_CPU}}"
WEB_SERVER_MEMORY="${WEB_SERVER_MEMORY:-${DEFAULT_WEB_SERVER_MEMORY}}"
WEB_SERVER_STORAGE="${WEB_SERVER_STORAGE:-${DEFAULT_WEB_SERVER_STORAGE}}"
WORKER_CPU="${WORKER_CPU:-${DEFAULT_WORKER_CPU}}"
WORKER_MEMORY="${WORKER_MEMORY:-${DEFAULT_WORKER_MEMORY}}"
WORKER_STORAGE="${WORKER_STORAGE:-${DEFAULT_WORKER_STORAGE}}"
MIN_WORKERS="${MIN_WORKERS:-${DEFAULT_WORKER_MIN}}"
MAX_WORKERS="${MAX_WORKERS:-${DEFAULT_WORKER_MAX}}"
TRIGGERER_COUNT="${TRIGGERER_COUNT:-${DEFAULT_TRIGGERER_COUNT}}"
TRIGGERER_CPU="${TRIGGERER_CPU:-${DEFAULT_TRIGGERER_CPU}}"
TRIGGERER_MEMORY="${TRIGGERER_MEMORY:-${DEFAULT_TRIGGERER_MEMORY}}"
DAG_PROCESSOR_COUNT="${DAG_PROCESSOR_COUNT:-${DEFAULT_DAG_PROCESSOR_COUNT}}"
DAG_PROCESSOR_CPU="${DAG_PROCESSOR_CPU:-${DEFAULT_DAG_PROCESSOR_CPU}}"
DAG_PROCESSOR_MEMORY="${DAG_PROCESSOR_MEMORY:-${DEFAULT_DAG_PROCESSOR_MEMORY}}"
DAG_PROCESSOR_STORAGE="${DAG_PROCESSOR_STORAGE:-${DEFAULT_DAG_PROCESSOR_STORAGE}}"

# --- Command Dispatcher ---
if [[ -z "${GCP_PROJECT_ID}" ]] || [[ -z "${ENV_NAME}" ]]; then
    echo "Error: --project-id and --env-name are required for all commands." >&2
    usage
fi

case "${MAIN_COMMAND}" in
    create)
        do_create
        ;;
    delete)
        do_delete
        ;;
    start)
        do_start
        ;;
    stop)
        do_stop
        ;;
    deploy)
        if [[ -z "${SOURCE_DIR}" ]]; then
            echo "Error: --source-dir is required for the 'deploy' command." >&2
            usage
        fi
        do_deploy
        ;;
    *)
        echo "Error: Unknown command '${MAIN_COMMAND}'" >&2
        usage
        ;;
esac