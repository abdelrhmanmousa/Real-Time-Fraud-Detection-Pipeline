set -e
set -u

usage() {
  echo "Usage: $0 --project-id <ID> --service <dataproc|dataflow>"
  echo "  Deploys permissions for a well-defined service by enforcing a naming convention."
  exit 1
}

# Arg Parsing
PROJECT_ID=""
SERVICE_TYPE=""
while [[ "$#" -gt 0 ]]; do
  case $1 in
    --project-id) PROJECT_ID="$2"; shift ;;
    --service) SERVICE_TYPE="$2"; shift ;;
    *) echo "Unknown parameter passed: $1"; usage ;;
  esac
  shift
done
if [ -z "$PROJECT_ID" ] || [ -z "$SERVICE_TYPE" ]; then usage; fi


# The script, not the caller, defines the names and roles.
if [[ "${SERVICE_TYPE}" == "dataproc" ]]; then
  SA_NAME="dataproc-etl-runner"
  SA_DISPLAY_NAME="Dataproc ETL Runner"
  ROLES_TO_GRANT=(
    "roles/dataproc.worker" "roles/storage.objectAdmin"
    "roles/bigquery.admin"
  )
elif [[ "${SERVICE_TYPE}" == "dataflow" ]]; then
  SA_NAME="dataflow-sa"
  SA_DISPLAY_NAME="Dataflow Pipeline Worker"
  ROLES_TO_GRANT=(
    "roles/dataflow.admin" "roles/dataflow.worker" "roles/storage.objectAdmin"
    "roles/pubsub.admin" "roles/datastore.user" "roles/run.invoker"
  )
else
  echo "ERROR: Invalid service type '${SERVICE_TYPE}'. Must be 'dataproc' or 'dataflow'."
  exit 1
fi

echo ">>> Configuring Permissions for Service: ${SERVICE_TYPE}"

# Logic for SA Creation and Role Granting
FULL_SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

echo "Ensuring Service Account '${SA_NAME}' exists..."
if ! gcloud iam service-accounts describe ${FULL_SA_EMAIL} --project=${PROJECT_ID} > /dev/null 2>&1; then
  gcloud iam service-accounts create "${SA_NAME}" --project=${PROJECT_ID} --display-name="${SA_DISPLAY_NAME}"
else
  echo "Service Account already exists."
fi

echo "Applying project-level IAM roles..."
for role in "${ROLES_TO_GRANT[@]}"; do
  echo "  - Applying ${role}"
  gcloud projects add-iam-policy-binding "${PROJECT_ID}" --member="serviceAccount:${FULL_SA_EMAIL}" --role="${role}" --condition=None > /dev/null
done

echo "Permissions deployment complete for ${SERVICE_TYPE}."