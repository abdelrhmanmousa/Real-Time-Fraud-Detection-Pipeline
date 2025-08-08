# set -e
# set -u
# set -o pipefail

# # Usage function
# usage() {
#   echo "Usage: $0 --project-id <ID> --environment-name <NAME> --location <LOCATION> --local-airflow-dir <DIR> "
#   exit 1
# }

# # Parse arguments
# while [[ "$#" -gt 0 ]]; do
#     case $1 in
#         --project-id) PROJECT_ID="$2"; shift ;;
#         --environment-name) ENVIRONMENT_NAME="$2"; shift ;;
#         --location) LOCATION="$2"; shift ;;
#         --local-airflow-dir) LOCAL_AIRFLOW_DIR="$2"; shift ;;
#         *) echo "Unknown parameter passed: $1"; usage ;;
#     esac
#     shift
# done

# # Check required arguments
# if [[ -z "${PROJECT_ID-}" || -z "${ENVIRONMENT_NAME-}" || -z "${LOCATION-}" || -z "${LOCAL_AIRFLOW_DIR-}" ]]; then
#   echo "ERROR: Missing one or more required arguments."
#   usage
# fi

# # Check if local directories exist
# if [ ! -d "$LOCAL_AIRFLOW_DIR" ]; then
#   echo "Error: Directory $LOCAL_AIRFLOW_DIR does not exist."
#   exit 1
# fi

# # Get DAG GCS prefix
# DAG_GCS_PREFIX=$(gcloud composer environments describe $ENVIRONMENT_NAME --location $LOCATION --project $PROJECT_ID --format="value(config.dagGcsPrefix)")

# if [ -z "$DAG_GCS_PREFIX" ]; then
#   echo "Error: Could not retrieve DAG GCS prefix for environment $ENVIRONMENT_NAME in location $LOCATION."
#   exit 1
# fi

# # Set plugins GCS prefix if local plugins dir is provided
# if [ -n "${LOCAL_PLUGINS_DIR-}" ]; then
#   PLUGINS_GCS_PREFIX="${DAG_GCS_PREFIX/dags/plugins}"
# fi

# # Deploy DAGs
# echo "Deploying DAGs from $LOCAL_AIRFLOW_DIR to $DAG_GCS_PREFIX"
# gsutil -m rsync -d -r $LOCAL_AIRFLOW_DIR $DAG_GCS_PREFIX

# if [ $? -ne 0 ]; then
#   echo "Error: Failed to deploy DAGs."
#   exit 1
# fi

# # Deploy plugins if specified
# if [ -n "${LOCAL_PLUGINS_DIR-}" ]; then
#   echo "Deploying plugins from $LOCAL_PLUGINS_DIR to $PLUGINS_GCS_PREFIX"
#   gsutil -m rsync -d -r $LOCAL_PLUGINS_DIR $PLUGINS_GCS_PREFIX

#   if [ $? -ne 0 ]; then
#     echo "Error: Failed to deploy plugins."
#     exit 1
#   fi
# fi

# echo "Deployment completed successfully."


#!/bin/bash
set -e
set -u
set -o pipefail

# Usage function
usage() {
  echo "Usage: $0 --project-id <ID> --environment-name <NAME> --location <LOCATION> --local-airflow-dir <DIR> "
  exit 1
}

# Parse arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --project-id) PROJECT_ID="$2"; shift ;;
        --environment-name) ENVIRONMENT_NAME="$2"; shift ;;
        --location) LOCATION="$2"; shift ;;
        --local-airflow-dir) LOCAL_AIRFLOW_DIR="$2"; shift ;;
        *) echo "Unknown parameter passed: $1"; usage ;;
    esac
    shift
done

#Check required arguments
if [ -z "${PROJECT_ID-}" ] || [ -z "${ENVIRONMENT_NAME-}" ] || [ -z "${LOCATION-}" ] || [ -z "${LOCAL_AIRFLOW_DIR-}" ]; then
  echo "ERROR: Missing one or more required arguments."
  usage
fi

LOCAL_DAGS_DIR="${LOCAL_AIRFLOW_DIR}/dags"
LOCAL_PLUGINS_DIR="${LOCAL_AIRFLOW_DIR}/plugins"
LOCAL_DATA_DIR="${LOCAL_AIRFLOW_DIR}/data"


# Check if directories exist
if [ ! -d "$LOCAL_DAGS_DIR" ]; then
  echo "Error: Directory $LOCAL_DAGS_DIR does not exist."
  exit 1
fi
if [ -n "${LOCAL_PLUGINS_DIR-}" ] && [ ! -d "$LOCAL_PLUGINS_DIR" ]; then
  echo "Error: Directory $LOCAL_PLUGINS_DIR does not exist."
  exit 1
fi
if [ -n "${LOCAL_DATA_DIR-}" ] && [ ! -d "$LOCAL_DATA_DIR" ]; then
  echo "Error: Directory $LOCAL_DATA_DIR does not exist."
  exit 1
fi

# Get DAG GCS prefix
DAG_GCS_PREFIX=$(gcloud composer environments describe "$ENVIRONMENT_NAME" --location "$LOCATION" --project "$PROJECT_ID" --format="value(config.dagGcsPrefix)")

if [ -z "$DAG_GCS_PREFIX" ]; then
  echo "Error: Could not retrieve DAG GCS prefix for environment $ENVIRONMENT_NAME in location $LOCATION."
  exit 1
fi

# Construct plugins and data GCS prefixes
BUCKET=${DAG_GCS_PREFIX%/dags}
PLUGINS_GCS_PREFIX="$BUCKET/plugins"
DATA_GCS_PREFIX="$BUCKET/data"

# Deploy DAGs
echo "Deploying DAGs from $LOCAL_DAGS_DIR to $DAG_GCS_PREFIX"
gsutil -m rsync -d -r "$LOCAL_DAGS_DIR" "$DAG_GCS_PREFIX"
if [ $? -ne 0 ]; then
  echo "Error: Failed to deploy DAGs."
  exit 1
fi

# Deploy plugins if specified
if [ -n "${LOCAL_PLUGINS_DIR-}" ]; then
  echo "Deploying plugins from $LOCAL_PLUGINS_DIR to $PLUGINS_GCS_PREFIX"
  gsutil -m rsync -d -r "$LOCAL_PLUGINS_DIR" "$PLUGINS_GCS_PREFIX"
  if [ $? -ne 0 ]; then
    echo "Error: Failed to deploy plugins."
    exit 1
  fi
fi

# Deploy data files if specified
if [ -n "${LOCAL_DATA_DIR-}" ]; then
  echo "Deploying data files from $LOCAL_DATA_DIR to $DATA_GCS_PREFIX"
  gsutil -m rsync -d -r "$LOCAL_DATA_DIR" "$DATA_GCS_PREFIX"
  if [ $? -ne 0 ]; then
    echo "Error: Failed to deploy data files."
    exit 1
  fi
fi

echo "Deployment completed successfully."