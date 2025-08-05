# Real-Time Fraud Detection Pipeline

## Introduction

This repository contains the complete source code and documentation for a scalable, real-time fraud detection system built on the Google Cloud Platform (GCP). The project is designed to ingest raw transaction events, enrich them with historical data in real-time, score them using a machine learning model, and take appropriate action, all within seconds. The architecture leverages a modern data stack, combining streaming and batch processing to create a robust and resilient pipeline that feeds a central Data Warehouse (DWH) for analytics and continuous model improvement.

## Project Objective

The primary objective of this project is to design and implement an end-to-end data pipeline that can accurately detect and flag fraudulent financial transactions in real-time. By doing so, the system aims to minimize financial losses for the business and protect user accounts from unauthorized activity, providing a secure transaction environment.

## Pipeline Architecture

The system is architected as a hybrid streaming and batch processing pipeline, leveraging managed services on Google Cloud to ensure scalability, reliability, and low operational overhead. The architecture is designed around a central data flow where events are processed immediately upon arrival, while batch jobs handle periodic data transformations and updates.

The core of the real-time component is a **Dataflow** streaming pipeline that consumes transactions from **Pub/Sub**, enriches them against a low-latency **Firestore** cache, and uses a **Cloud Run**-hosted ML model to generate fraud scores. The batch component uses **Dataproc** for hourly ETL from a GCS data lake into the **BigQuery** DWH and **Cloud Composer (Airflow)** to orchestrate daily updates from the DWH back to the Firestore cache.

## Data Flow

The journey of a single transaction from initiation to final analysis is a continuous loop. Here is a step-by-step breakdown of how data flows through the system:

### 1. Raw Transaction Ingestion

The process begins when a raw transaction event is sent from a payment gateway (emulated by a GCP virtual machine) to a Google Cloud Pub/Sub topic. This initial event is lightweight, containing only the essential details of the transaction.

*__Example of a raw transaction event:__*
```json
{
    "transaction_id": "txn_91b145c2-0f71-4053-a09b-ba0a85c406c4",
    "timestamp": "2025-01-01T00:06:04Z",
    "amount": 453,
    "currency": "EGP",
    "entry_mode": "Online",
    "merchant_id": "merch_193d1b1f-096",
    "user_id": "usr_46cf5be8",
    "ip_address": "101.20.16.204",
    "device_id": "dev_c98a8040",
    "card_id": "tok_4a1e5316-18e",
    "card_type": "Credit",
    "card_country": "EG",
    "was_3ds_successful": true
}
```
This raw transaction cannot be directly used by the machine learning model, as the model requires a richer set of features. To address this, the transaction must be "enriched" with historical data. For performance, this enrichment data is pre-calculated by a daily batch job that extracts features from the Data Warehouse (BigQuery) and loads them into a NoSQL database (Firestore), which serves as a real-time cache.

### 2. Real-time Enrichment and Prediction

A **Dataflow** streaming job subscribes to the Pub/Sub topic and consumes the raw transactions. For each transaction, it performs the following real-time operations:

*   **Enrichment:** It uses identifiers like `user_id`, `merchant_id`, and `card_id` to fetch corresponding historical features from the Firestore cache. This process adds valuable context, such as the user's average transaction amount, account age, and more.
*   **Preprocessing:** It performs any final data transformations needed to prepare the enriched data for the ML model.
*   **Prediction:** The Dataflow job sends the fully enriched data as a request to a deployed ML inference service. For simplicity and low latency, the ML model is embedded within the Dataflow job itself, which computes and appends the fraud score.

*__Example of a transaction after enrichment and ML inference:__*
```json
{
    "transaction_id": "txn_3f3216b7-c596-4e93-b9ec-6f0614a3d47a",
    "timestamp": "2025-01-01T14:35:40Z",
    "amount": 368,
    "currency": "EGP",
    "country_code": "EG",
    "merchant_id": "merch_b6801c5a-f51",
    "user_id": "usr_ff34c0b3",
    "user_account_age_days": 1096,
    "card_id": "tok_a878bbbf-bc5",
    "is_weekend_transaction": false,
    "is_night_transaction": false,
    "time_since_last_user_transaction_s": 86313600,
    "distance_from_home_km": 177.58,
    "user_avg_tx_amount_30d": 0.0,
    "fraud_score": 0.05
}
```

### 3. Action and Data Archiving

Once the fraud score is obtained, the Dataflow job executes two tasks in parallel:

*   **Alerting/Action:** Based on the fraud score, it can trigger predefined actions, such as sending a high-priority alert to an action service or publishing a message to a separate topic for review.
*   **Data Archiving:** It archives the complete, enriched transaction data (including the fraud score) to a designated raw data lake bucket in Google Cloud Storage (GCS).

### 4. Hourly Data Transformation to DWH

A **Spark job**, running on a **Dataproc** cluster, is scheduled to execute every hour. This job processes the hourly batches of enriched data that have accumulated in the GCS data lake, performs necessary transformations, and loads the structured data into the **BigQuery** Data Warehouse.

### 5. DWH for BI and Feature Updates

The data in the BigQuery DWH is the central source of truth and serves two critical functions:

*   **Business Intelligence (BI) and Dashboarding:** It is used to build reports, dashboards, and perform analytical queries to gain insights into transaction patterns, fraud trends, and system performance.
*   **Enrichment Data Source:** As mentioned in the first step, the DWH provides the data for the daily batch job (orchestrated by Airflow) that updates the Firestore cache. This ensures the real-time enrichment process always has access to fresh, relevant historical features, completing the data loop.

## Pipeline Components

The project is organized into several key modules, each responsible for a specific part of the pipeline:

*   **`data_pipelines/`**: Contains the core data processing logic.
    *   **`streaming/`**: Holds the Apache Beam code for the real-time Dataflow pipeline (`processing_pipeline`) and the alerting service (`real_time_action`).
    *   **`batch/`**: Contains the PySpark code for the hourly ETL job (`transactions_etl_to_dwh`) that runs on Dataproc.
*   **`machine_learning/`**: Includes all code related to the ML model.
    *   **`training_pipeline/`**: A containerized pipeline to train the XGBoost fraud detection model. It includes scripts for data preparation, hyperparameter optimization with Optuna, and experiment tracking with Weights & Biases.
    *   **`inference_service/`**: A lightweight FastAPI application designed to be deployed on Cloud Run. It serves the trained model and provides a real-time prediction endpoint.
*   **`orchestration/`**: Manages the scheduling and automation of batch jobs.
    *   **`dags/`**: Contains the Apache Airflow DAGs for orchestrating the batch pipelines, including the daily features export to Firestore and the hourly load to BigQuery.
    *   **`plugins/`**: Includes a custom Airflow operator (`gcs_to_firestore`) for efficiently loading data from GCS to Firestore.
*   **`scripts/`**: A collection of shell scripts for automating deployment and setup.
    *   **`deployment/`**: Scripts for deploying all cloud resources, including Dataflow jobs, Dataproc templates, Cloud Run services, and Cloud Composer environments.
    *   **`setup/`**: Scripts for initial configuration, such as setting up IAM permissions and BigQuery subscriptions.

## Tech Stack, Services & Frameworks

This project utilizes a modern stack of technologies and frameworks to build a robust and scalable system.

*   **Cloud Provider**: **Google Cloud Platform (GCP)**
*   **GCP Services**:
    *   **Data Ingestion & Streaming**: Google Cloud Pub/Sub
    *   **Stream Processing**: Google Cloud Dataflow
    *   **Batch Processing**: Google Cloud Dataproc
    *   **Data Warehouse**: Google BigQuery
    *   **Real-time Data Cache**: Google Cloud Firestore (in Datastore Mode)
    *   **Data Lake & Staging**: Google Cloud Storage (GCS)
    *   **ML Model Serving**: Google Cloud Run
    *   **ML Model & Container Registry**: Google Artifact Registry
    *   **CI/CD & Automation**: Google Cloud Build
    *   **Event-Driven Actions**: Google Cloud Eventarc
    *   **Workflow Orchestration**: Google Cloud Composer
*   **Programming Language**: **Python**
*   **Data Processing Frameworks**:
    *   **Apache Beam**: For defining and executing the real-time streaming pipeline on Dataflow.
    *   **Apache Spark**: For the hourly batch ETL job on Dataproc.
    *   **Pandas**: For in-memory data manipulation during model training.
*   **Machine Learning**:
    *   **Scikit-learn**: For creating preprocessing and model pipelines.
    *   **XGBoost**: The core algorithm for the fraud detection model.
    *   **Optuna**: For advanced hyperparameter optimization.
    *   **Joblib**: For model serialization.
*   **MLOps**:
    *   **Weights & Biases (W&B)**: For experiment tracking, model versioning, and results visualization.
*   **API & Web Frameworks**:
    *   **FastAPI**: For building the high-performance ML inference service.
    *   **Uvicorn & Gunicorn**: For serving the FastAPI application.
*   **Orchestration**:
    *   **Apache Airflow**: For defining, scheduling, and monitoring the batch data pipelines.
*   **Containerization**:
    *   **Docker & Docker Compose**: For containerizing applications and local development.
*   **Data Formats**: **JSON** (for Pub/Sub and APIs), **Parquet** (for GCS data lake storage).

## Automation

The entire batch processing workflow is fully automated and orchestrated using **Apache Airflow**, managed via **Google Cloud Composer**. This ensures that data is consistently processed and made available for both analytics and real-time enrichment without manual intervention.

Two primary DAGs manage the automation:

1.  **`features_export_dag.py`**: This DAG runs daily. Its responsibility is to query the BigQuery DWH to calculate the latest historical features for all users and merchants. It then exports this data to GCS and uses a custom operator to load it into the Firestore cache, ensuring the real-time pipeline has access to up-to-date enrichment data.
2.  **`load_to_dwh_dag.py`**: This DAG runs at the end of every hour. It triggers a Dataproc job that runs a Spark ETL pipeline. The pipeline reads the enriched, scored transaction data from the GCS data lake, transforms it into a dimensional model, and loads it into the final fact and dimension tables in BigQuery.

These automated workflows form a closed loop, where the system continuously learns from new data and improves its feature store.

## License

This project is licensed under the MIT License.
