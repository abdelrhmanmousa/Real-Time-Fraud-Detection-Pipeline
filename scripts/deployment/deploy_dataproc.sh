set -e 
set -u 

# help func
usage() {
  echo "Usage: $0 --project-id <PROJECT_ID> --region <REGION> --gcs-bucket <GCS_BUCKET> --template-id <TEMPLATE_ID> --template-yaml-path <YAML_PATH> --job-source-dir <SOURCE_DIR>"
  echo "  Deploys a PySpark job and its corresponding Dataproc Workflow Template."
  exit 1
}

# Initialize variables to avoid unbound variable errors
PROJECT_ID="${GCP_PROJECT_ID:-}"
REGION="${GCP_REGION:-}"
GCS_BUCKET="${DATAPROC_GCS_BUCKET:-}"
TEMPLATE_ID="${DATAPROC_WORKFLOW_TEMPLATE_ID:-}"
TEMPLATE_YAML_PATH="${DATAPROC_WORKFLOW_TEMPLATE_YAML_PATH:-}"
JOB_SOURCE_DIR_LOCAL="${DATAPROC_JOB_SOURCE_DIR:-}"
SA_NAME="dataproc-etl-runner"

if [ "$#" -eq 0 ]; then
    usage
fi

while [[ "$#" -gt 0 ]]; do
  case $1 in
    --project-id) PROJECT_ID="$2"; shift ;;
    --region) REGION="$2"; shift ;;
    --gcs-bucket) GCS_BUCKET="$2"; shift ;;
    --template-id) TEMPLATE_ID="$2"; shift ;;
    --template-yaml-path) TEMPLATE_YAML_PATH="$2"; shift ;;
    --job-source-dir) JOB_SOURCE_DIR_LOCAL="$2"; shift ;;
    *) echo "Unknown parameter passed: $1"; usage ;;
  esac
  shift
done

# making sure that all required arguments were provided
if [ -z "$PROJECT_ID" ] || [ -z "$REGION" ] || [ -z "$GCS_BUCKET" ] || [ -z "$TEMPLATE_ID" ] || [ -z "$TEMPLATE_YAML_PATH" ] || [ -z "$JOB_SOURCE_DIR_LOCAL" ]; then
    echo "Error: Missing one or more required arguments."
    usage
fi

# main logic
# Construct the destination GCS path from the template ID for consistency
JOB_SOURCE_DIR_GCS="gs://${GCS_BUCKET}/pyspark_jobs/${TEMPLATE_ID}"

echo ">> Step 1 of 2: Packaging PySpark source code and copying it to GCS ..."


echo "Packaging the following files:"
ls -R ${JOB_SOURCE_DIR_LOCAL}
cd ${JOB_SOURCE_DIR_LOCAL}

echo "Creating package etl_job.zip from contents..."
zip -r "../../../etl_job.zip" .

cd - > /dev/null

trap 'rm -f etl_job.zip' EXIT

echo "Package created successfully: etl_job.zip"

echo "Uploading  to ${JOB_SOURCE_DIR_GCS}..."

gcloud storage cp -q etl_job.zip "${JOB_SOURCE_DIR_GCS}/etl_job.zip"

gcloud storage cp "${JOB_SOURCE_DIR_LOCAL}/main.py" "${JOB_SOURCE_DIR_GCS}/main.py"


echo -e "\n>> Step 2 of 2: Deploying Dataproc Workflow Template ..."
echo "Template ID: '${TEMPLATE_ID}' in project '${PROJECT_ID}' and region '${REGION}'"
echo "Source File: '${TEMPLATE_YAML_PATH}'"

export GCP_PROJECT_ID="${PROJECT_ID}"
export DATAPROC_SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
export GCS_BUCKET="${GCS_BUCKET}"
export TEMPLATE_ID="${TEMPLATE_ID}"

# Define a temporary file for the final, substituted YAML
FINAL_YAML_PATH="/tmp/${TEMPLATE_ID}.yaml"

echo "Substituting variables into template: '${TEMPLATE_YAML_PATH}'"

envsubst < "${TEMPLATE_YAML_PATH}" > "${FINAL_YAML_PATH}"
echo "Generated final config at: '${FINAL_YAML_PATH}'"


echo "Importing template '${TEMPLATE_ID}' to Dataproc..."
gcloud dataproc workflow-templates import ${TEMPLATE_ID} \
    --project=${PROJECT_ID} \
    --region=${REGION} \
    --source="${FINAL_YAML_PATH}"


rm "${FINAL_YAML_PATH}"

echo "Workflow template deployment complete."