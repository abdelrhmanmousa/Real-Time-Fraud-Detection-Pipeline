# Monorepo for Data Pipelines and ML Services

This repository contains data pipelines, machine learning models, and deployment scripts.

## Deployment

Deployment is orchestrated using a `Makefile`. To deploy the services, you need to set the following environment variables:

### Required Environment Variables

All configuration is managed in the `config.yaml` file. The following variables must be set in that file before deployment:

*   `GCP_PROJECT_ID`: Your Google Cloud project ID.
*   `GCP_REGION`: The GCP region for your services (e.g., `us-central1`).
*   `DATAFLOW_GCS_OUTPUT_BUCKET`: The GCS bucket for Dataflow output data.
*   `DATAFLOW_GCS_DLQ_BUCKET`: The GCS bucket for Dataflow Dead Letter Queue data.
*   `DATAFLOW_INPUT_TOPIC_ID`: The Pub/Sub topic ID for the Dataflow job to read from.
*   `DATAFLOW_ML_MODEL_ENDPOINT_URL`: The URL of the deployed ML model.
*   `DATAPROC_GCS_BUCKET`: The GCS bucket for Dataproc job artifacts.
*   `DATAPROC_WORKFLOW_TEMPLATE_ID`: The ID for the Dataproc workflow template.
*   `ML_SERVICE_NAME`: The name for the Cloud Run ML service.
*   `ML_SERVICE_MODEL_PATH_IN_GCS`: The GCS path to the ML model artifact.

### Makefile Targets

*   `make all`: Deploys all services in the correct order.
*   `make permissions`: Configures service account permissions for `dataflow`, `dataproc`, and `ml-service`.
*   `make deploy-dataproc`: Deploys the Dataproc workflow template.
*   `make deploy-ml-service`: Deploys the ML inference service.
*   `make deploy-dataflow`: Deploys the Dataflow streaming job.
*   `make test`: Prints the values of the environment variables to verify they are set correctly.

### Configuration

All non-sensitive configuration parameters are defined in `config.example.yaml`. Before running the deployment, you need to create your own `config.yaml` file and populate it with your specific values. The `config.yaml` file is ignored by git.

### Example Usage

1.  **Create your `config.yaml` file:**
    ```bash
    cp config.example.yaml config.yaml
    ```

2.  **Update `config.yaml` with your specific values.**

3.  **Install yq:**
    Follow the installation instructions for `yq` from the official documentation: [https://github.com/mikefarah/yq#install](https://github.com/mikefarah/yq#install)

4.  **Export the configuration:**
    ```bash
    export $(yq e 'to_entries | .[] | .key + "=" + .value' config.yaml | xargs)
    ```

5.  **Run the deployment:**
    ```bash
    make all
    ```
