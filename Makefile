# Makefile for deploying services
#
# Environment variables are loaded from config.yaml
# see README.md for instructions

.PHONY: all permissions deploy-dataflow deploy-dataproc deploy-ml-service test

all: permissions deploy-dataproc deploy-ml-service deploy-dataflow

permissions:
	@echo ">>> Configuring permissions..."
	@scripts/deployment/configure_service_permissions.sh --project-id $(GCP_PROJECT_ID) --service dataflow
	@scripts/deployment/configure_service_permissions.sh --project-id $(GCP_PROJECT_ID) --service dataproc
	@scripts/deployment/configure_service_permissions.sh --project-id $(GCP_PROJECT_ID) --service ml-service

deploy-dataflow:
	@echo ">>> Deploying Dataflow job..."
	@GCP_PROJECT_ID=$(GCP_PROJECT_ID) \
	REGION=$(GCP_REGION) \
	GCS_OUTPUT_BUCKET=$(DATAFLOW_GCS_OUTPUT_BUCKET) \
	GCS_DLQ_BUCKET=$(DATAFLOW_GCS_DLQ_BUCKET) \
	INPUT_TOPIC_ID=$(DATAFLOW_INPUT_TOPIC_ID) \
	MODEL_ENDPOINT_URL=$(DATAFLOW_ML_MODEL_ENDPOINT_URL) \
	JOB_SOURCE_DIR=$(DATAFLOW_JOB_SOURCE_DIR) \
	scripts/deployment/deploy_dataflow.sh

deploy-dataproc:
	@echo ">>> Deploying Dataproc workflow template..."
	@PROJECT_ID=$(GCP_PROJECT_ID) \
	REGION=$(GCP_REGION) \
	GCS_BUCKET=$(DATAPROC_GCS_BUCKET) \
	TEMPLATE_ID=$(DATAPROC_WORKFLOW_TEMPLATE_ID) \
	TEMPLATE_YAML_PATH=$(DATAPROC_WORKFLOW_TEMPLATE_YAML_PATH) \
	JOB_SOURCE_DIR_LOCAL=$(DATAPROC_JOB_SOURCE_DIR) \
	scripts/deployment/deploy_dataproc.sh

deploy-ml-service:
	@echo ">>> Deploying ML service..."
	@GCP_PROJECT_ID=$(GCP_PROJECT_ID) \
	REGION=$(GCP_REGION) \
	SERVICE_NAME=$(ML_SERVICE_NAME) \
	MODEL_PATH_IN_GCS=$(ML_SERVICE_MODEL_PATH_IN_GCS) \
	scripts/deployment/deploy_ml_service.sh

test:
	@echo "GCP_PROJECT_ID: $(GCP_PROJECT_ID)"
	@echo "GCP_REGION: $(GCP_REGION)"
	@echo "DATAFLOW_GCS_OUTPUT_BUCKET: $(DATAFLOW_GCS_OUTPUT_BUCKET)"
	@echo "DATAFLOW_GCS_DLQ_BUCKET: $(DATAFLOW_GCS_DLQ_BUCKET)"
	@echo "DATAFLOW_INPUT_TOPIC_ID: $(DATAFLOW_INPUT_TOPIC_ID)"
	@echo "DATAFLOW_ML_MODEL_ENDPOINT_URL: $(DATAFLOW_ML_MODEL_ENDPOINT_URL)"
	@echo "DATAFLOW_JOB_SOURCE_DIR: $(DATAFLOW_JOB_SOURCE_DIR)"
	@echo "DATAPROC_GCS_BUCKET: $(DATAPROC_GCS_BUCKET)"
	@echo "DATAPROC_WORKFLOW_TEMPLATE_ID: $(DATAPROC_WORKFLOW_TEMPLATE_ID)"
	@echo "DATAPROC_WORKFLOW_TEMPLATE_YAML_PATH: $(DATAPROC_WORKFLOW_TEMPLATE_YAML_PATH)"
	@echo "DATAPROC_JOB_SOURCE_DIR: $(DATAPROC_JOB_SOURCE_DIR)"
	@echo "ML_SERVICE_NAME: $(ML_SERVICE_NAME)"
	@echo "ML_SERVICE_MODEL_PATH_IN_GCS: $(ML_SERVICE_MODEL_PATH_IN_GCS)"
