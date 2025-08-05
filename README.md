![Status](https://img.shields.io/badge/status-work_in_progress-yellow) ![Python](https://img.shields.io/badge/python-3.12-blue.svg) ![Framework](https://img.shields.io/badge/FastAPI-purple.svg) ![License](https://img.shields.io/badge/License-MIT-red) ![cloud](https://img.shields.io/badge/cloud-GCP-blue) ![airflow](https://img.shields.io/badge/Apache-Airflow-blue) ![spark](https://img.shields.io/badge/Apache-Spark-blue)
![Beam](https://img.shields.io/badge/Apache-Beam-blue) ![docker](https://img.shields.io/badge/Docker-blue) ![Bash](https://img.shields.io/badge/Bash-red) ![XGBoost](https://img.shields.io/badge/XGBoost-blue) ![Scikit-learn](https://img.shields.io/badge/Scikit-Learn-orange) ![Optuna](https://img.shields.io/badge/Optuna-red)

# Real-Time Fraud Detection Pipeline

This repository contains the complete source code and documentation for a scalable, real-time fraud detection system built on the Google Cloud Platform (GCP). The project is designed to ingest raw transaction events, enrich them with historical data in real-time, score them using a machine learning model, and take appropriate action, all within seconds. The architecture leverages a modern data stack, combining streaming and batch processing to create a robust and resilient pipeline that feeds a central Data Warehouse (DWH) for analytics and continuous model improvement.

### **Table of Contents**

1.  **Introduction**
2.  **Project Objectives**
3.  **System Architecture**
4.  **Core Design Concepts**
5.  **Tech Stack, Services & Frameworks**
6.  **Codebase & Pipeline Components**
7.  **The Hot Path: Real-Time Transaction Scoring**
8.  **The Cold Path: Analytics and Feature Engineering**
9.  **Automation: Infrastructure and Deployment**
10. **Future Work**
11. **License**


##  📖 Introduction

### High-Level Project Overview

This repository provides the complete source code, infrastructure scripts, and documentation for a production-grade, **end-to-end fraud detection system**. It is engineered to identify and act on fraudulent financial transactions in real-time by leveraging a powerful, event-driven architecture built entirely on the **Google Cloud Platform (GCP)**.

The core of the project is a sophisticated data pipeline that showcases a modern, hybrid approach to data processing. It combines a low-latency **streaming "hot path"** for immediate transaction scoring with a robust **batch-processing "cold path"** for data warehousing, analytics, and feature engineering. This dual-path design ensures that potentially fraudulent events are flagged within seconds, while also building a rich historical data foundation for business intelligence and continuous model improvement.

### Core Purpose: Real-Time Fraud Scoring

The primary mission of this system is to ingest raw transaction events from a payment gateway, enrich them with critical historical context, and score them using a machine learning model—all in a single, seamless, and near-instantaneous flow.

The process is designed to be highly resilient and scalable, featuring:
*   **Real-Time Enrichment:** Augmenting raw transaction data with pre-computed features from a low-latency NoSQL cache (Firestore).
*   **Decoupled ML Inference:** Calling a dedicated, auto-scaling ML service (hosted on Cloud Run) to get fraud predictions without tightly coupling the model to the data pipeline.
*   **Automated Feedback Loop:** Using an orchestrated workflow (Cloud Composer / Airflow) to periodically refresh the real-time feature cache from a central Data Warehouse (BigQuery).

Ultimately, this repository serves as a comprehensive blueprint for building and deploying complex, event-driven machine learning systems on the cloud, complete with modular code, deployment automation scripts, and detailed architectural documentation.

***

## 🚀 Project Objectives

The project is driven by a clear set of business needs and is designed to meet specific technical requirements for building a modern, scalable, and maintainable data system.

### Primary Business Goals

The ultimate aim of the pipeline is to deliver direct value to the business by mitigating the risks associated with fraudulent transactions.

*   **Minimize Financial Losses:** The foremost objective is to detect and prevent fraudulent transactions before they are fully processed, thereby directly reducing financial losses from chargebacks and unauthorized fund transfers.
*   **Enhance Customer Trust and Security:** By providing a secure transaction environment and proactively protecting user accounts from fraudulent activity, the system aims to increase customer confidence and loyalty.
*   **Enable Data-Driven Decision-Making:** By creating a centralized Data Warehouse in BigQuery, the project provides a single source of truth for transaction and fraud data. This enables business analysts and data scientists to generate reports, build dashboards, and uncover deeper insights into fraud trends and user behavior.
*   **Improve Operational Efficiency:** Automating the detection process reduces the need for manual review of transactions, allowing fraud analysis teams to focus their efforts on investigating the most critical and complex cases.

### Technical Goals

To achieve the business objectives, the project was engineered with the following technical goals in mind:

*   **Achieve Low-Latency Processing:** The entire "hot path" pipeline—from ingestion to enrichment and scoring—is designed to execute within a few seconds to ensure that actions can be taken on suspicious transactions before they are finalized.
*   **Build a Scalable and Elastic System:** The architecture exclusively uses serverless and managed GCP services (Dataflow, Cloud Run, Pub/Sub) that can automatically scale their resources up or down based on the incoming transaction volume, ensuring both performance during peak loads and cost-efficiency during quiet periods.
*   **Ensure High Resilience and Fault Tolerance:** The system is designed to be robust against failures. This is achieved through the use of dead-letter queues (DLQs) to isolate problematic data, automated retries in batch workflows, and the decoupling of services to prevent a failure in one component from cascading to the entire system.
*   **Implement a Modular and Maintainable Codebase:** The project follows a clear separation of concerns, with distinct modules for the streaming pipeline, batch ETL jobs, ML model training, ML inference, and orchestration. This modularity makes the system easier to understand, maintain, and extend over time.
*   **Establish an Automated MLOps Feedback Loop:** A key technical goal is to create an automated, end-to-end process where the machine learning model is not static. The system continuously collects new data, which is then used by orchestrated Airflow jobs to retrain and improve the feature store, ensuring the model's accuracy and relevance are maintained over time.

Of course. Here is the "System Architecture" section, complete with a placeholder for the diagram and a detailed overview that ties all the components together.

***

## 🏗️ System Architecture

### High-Level Diagram

The following diagram provides a visual representation of the end-to-end fraud detection pipeline, illustrating the flow of data through both the real-time "hot path" and the analytical "cold path."

![System Architecture Diagram](./assets/system_architecture.png)
*(Note: This is a placeholder for the architecture diagram image.)*

### Architectural Overview

The system is designed as a highly scalable, hybrid data architecture that leverages the strengths of both streaming and batch processing. It is composed of two primary data flows—the **Hot Path** for immediate, real-time scoring and the **Cold Path** for analytics, reporting, and feature engineering.

**The Hot Path (Real-Time Processing):**

The hot path is optimized for ultra-low latency to ensure that transactions can be analyzed and acted upon within seconds.

1.  **Ingestion:** A raw transaction event from a payment gateway is pushed into a **Google Cloud Pub/Sub** topic, which serves as the main entry point into the system. Pub/Sub provides a scalable and durable buffer for incoming events.
2.  **Streaming Pipeline:** A **Google Cloud Dataflow** job, built with Apache Beam, acts as a streaming subscriber to the Pub/Sub topic. It immediately consumes each transaction message as it arrives.
3.  **Real-Time Enrichment:** To add necessary context for the ML model, the Dataflow pipeline queries a **Google Cloud Firestore** database. Firestore acts as a low-latency, NoSQL cache that stores pre-computed historical features for every user and merchant.
4.  **ML Inference:** The enriched transaction data is sent via an HTTP request to a dedicated ML model-serving endpoint. This endpoint is a containerized **FastAPI** application hosted on **Google Cloud Run**, which provides auto-scaling and serverless compute. This decoupling ensures that the ML model can be updated independently of the data pipeline.
5.  **Downstream Action:** The Dataflow pipeline receives the fraud score from the ML service and publishes the fully enriched and scored transaction to a second Pub/Sub topic. From there, another lightweight Cloud Run service (`real_time_action`) can consume these messages to trigger alerts, block users, or initiate other business processes.

**The Cold Path (Batch Analytics and Feedback Loop):**

The cold path is designed for analytical workloads and for feeding insights back into the real-time system.

1.  **Data Lake Archiving:** Concurrently with its real-time actions, the Dataflow pipeline writes the enriched and scored transaction data into hourly-partitioned files in a **Google Cloud Storage (GCS)** bucket. This GCS bucket serves as the raw data lake.
2.  **ETL to Data Warehouse:** An hourly **Apache Spark** job, running on a managed **Google Cloud Dataproc** cluster, reads the newly arrived data from the GCS data lake. It performs schema enforcement, transformations, and loads the cleaned data into a structured dimensional model within the **Google BigQuery** Data Warehouse.
3.  **Orchestration and Feature Refresh:** All batch jobs are scheduled and monitored by **Google Cloud Composer (Apache Airflow)**. The most critical orchestrated workflow is the daily feature store refresh. This Airflow DAG queries BigQuery to re-calculate the historical features for all users and merchants, exports the results to GCS, and then loads this fresh data into the Firestore cache, completing the feedback loop and ensuring the hot path always has up-to-date enrichment data.

***

## ➡️ Data Flow

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

### Real-time Enrichment and Prediction

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
    "entry_mode": "Online",
    "merchant_id": "merch_b6801c5a-f51",
    "merchant_category": 5541,
    "merchant_lat": 30.0839,
    "merchant_lon": 31.2464,
    "user_id": "usr_ff34c0b3",
    "user_account_age_days": 1096,
    "card_id": "tok_a878bbbf-bc5",
    "card_type": "Debit",
    "card_brand": "Mastercard",
    "issuing_bank": "National Bank of Egypt",
    "card_country": "EG",
    "ip_address": "184.172.44.80",
    "ip_lat": 30.088,
    "ip_lon": 31.2526,
    "device_id": "dev_2c60717d",
    "account_balance_before_tx": 32353.37,
    "is_weekend_transaction": false,
    "is_night_transaction": false,
    "time_since_last_user_transaction_s": 86313600,
    "distance_from_home_km": 177.58,
    "is_geo_ip_mismatch": false,
    "is_foreign_country_tx": false,
    "user_avg_tx_amount_30d": 0.0,
    "user_max_distance_from_home_90d": 0.0,
    "user_num_distinct_countries_6m": 1,
    "user_tx_count_24h": 0,
    "user_failed_tx_count_1h": 0,
    "user_num_distinct_mcc_24h": 0,
    "is_new_device": false,
    "was_3ds_successful": false,
    "tx_amount_vs_user_avg_ratio": 999.0,
    "tx_amount_to_balance_ratio": 0.011,
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

***

## 🧠 Core Design Concepts

The architecture of this fraud detection system is not just a collection of services, but a carefully designed ecosystem built on several core principles. These concepts are fundamental to its performance, scalability, and reliability in a production environment.

### 4.1 Hot Path vs. Cold Path: A Hybrid Architecture

To meet the dual requirements of immediate action and long-term analysis, the system is intentionally split into two distinct but interconnected data flows:

*   **The Hot Path (Real-Time Stream Processing):**
    *   **Purpose:** To answer the question, "Is this transaction fraudulent *right now*?"
    *   **Characteristics:** This path is optimized for ultra-low latency. Its sole responsibility is to ingest, enrich, score, and act on an individual transaction in the shortest time possible—ideally, within seconds. It is the system's reactive nervous system.
    *   **Technologies:** Governed by **Pub/Sub**, **Dataflow**, **Firestore**, and **Cloud Run**.

*   **The Cold Path (Analytical Batch Processing):**
    *   **Purpose:** To answer questions like, "What were the fraud trends last week?" or "What are the latest spending patterns for this user?"
    *   **Characteristics:** This path is optimized for throughput and cost-effective processing of large volumes of historical data. It handles data warehousing, business intelligence, complex analytics, and, most importantly, feeds insights back into the hot path. It is the system's long-term memory and learning center.
    *   **Technologies:** Governed by **Google Cloud Storage (GCS)**, **Dataproc (Spark)**, **BigQuery**, and **Cloud Composer (Airflow)**.

The true power of the architecture lies in the synergy between these two paths. The cold path periodically analyzes historical data in BigQuery to generate fresh, aggregated features, which it then loads into the Firestore cache. This act directly "warms up" the hot path, ensuring its real-time decisions are always based on the most up-to-date context.

### 4.2 Decoupling for Scalability and Maintainability

A key architectural principle is the strict **decoupling of services**. Instead of a single, monolithic application, the system is composed of independent, specialized components that communicate over well-defined interfaces (like REST APIs and Pub/Sub messages). The primary decoupled services are the Dataflow pipeline, the ML inference service, and the real-time action service.

This separation of concerns provides several significant benefits:

*   **Independent Scaling:** The Dataflow pipeline may need to scale based on the number of incoming transactions, while the ML inference service on Cloud Run might need to scale based on the computational complexity of its predictions (e.g., requiring more CPU). Decoupling allows each component to scale independently, optimizing resource usage and cost.
*   **Independent Development and Deployment:** The data science team can iterate on and deploy new versions of the ML model to Cloud Run without ever touching or redeploying the data engineering pipeline. Conversely, the data engineering team can optimize the Dataflow job without any knowledge of the model's internal workings. This autonomy accelerates development cycles and reduces cross-team friction.
*   **Technological Flexibility:** Each service can be built with the best technology for its specific job. The ML service uses Python and FastAPI, but it could be rewritten in Go or Rust if needed, as long as it adheres to the API contract. This prevents technology lock-in and allows for future evolution.
*   **Enhanced Fault Isolation:** If the ML inference service experiences an error or becomes unavailable, the Dataflow pipeline will not crash. Instead, it is designed to catch the failure, log the error, and route the problematic transaction to a dead-letter queue. This contains the failure to a single component and ensures the rest of the system remains operational.

### 4.3 Performance Optimization: Strategic Timed Batching

While the system is "real-time," processing each transaction individually from end-to-end can be inefficient. Making a separate network request to the ML service and a separate write to Pub/Sub for every single event would introduce significant overhead and underutilize service resources.

To solve this, the pipeline employs a **strategic micro-batching** technique using Apache Beam's `GroupIntoBatches` transform.

*   **How It Works:** Instead of processing one transaction at a time, the Dataflow pipeline groups transactions into small batches. This grouping is controlled by two parameters: a maximum batch size (e.g., 50 elements) and a maximum waiting time (e.g., 2 seconds). The pipeline emits a batch as soon as either of these thresholds is met.
*   **Benefits:**
    1.  **Reduced Network Overhead:** It makes one larger, more efficient HTTP request to the ML service with a batch of 50 transactions instead of 50 small, individual requests.
    2.  **Increased Throughput:** The ML service can often process a batch of data more efficiently than single items due to vectorized operations in libraries like NumPy and XGBoost.
    3.  **Latency Control:** The time-based trigger ensures that no transaction waits longer than the specified threshold, thus preserving the low-latency, real-time nature of the pipeline.

This same batching principle is applied when the scored transactions are written to the destination Pub/Sub topic, optimizing the overall throughput of the system.

### 4.4 Resilience by Design: Error Handling and Dead-Lettering

The system is designed with the assumption that failures will happen. Rather than allowing errors to crash the pipeline, it uses a robust error-handling strategy centered around **Dead-Letter Queues (DLQs)** to isolate and archive problematic data for later analysis.

This strategy is applied at critical failure points in both the hot and cold paths:

*   **In the Hot Path (Dataflow):**
    *   **Parsing Failures:** If an incoming message from Pub/Sub is not valid JSON or is missing a critical field like a timestamp, the initial parsing step will fail. A `try-except` block catches this error, and the raw, malformed message is immediately routed to a dedicated **parsing DLQ** in GCS.
    *   **Processing Failures:** If an error occurs during the main processing logic—for example, if a call to the ML service times out or returns an error, or if an expected document is not found in Firestore—the entire problematic batch of transactions is captured and sent to a **processing DLQ** in GCS.
    In both cases, the failed data is isolated, and the main pipeline continues to process valid transactions without interruption.

*   **In the Cold Path (Airflow & Dataproc):**
    *   **Proactive Checks:** The Airflow DAGs use `Sensors` to ensure that source data exists in GCS *before* triggering an expensive Dataproc job. This "fail-fast" approach prevents wasted resources.
    *   **Task Retries:** Airflow tasks are configured with automatic retries, which can handle transient issues like temporary network flakes or API unavailability.
    *   **Failure Notifications:** If a task fails after all retries, Airflow's `on_failure_callback` mechanism is used to trigger alerts (e.g., via email or Slack), immediately notifying the on-call team of a persistent issue that requires manual intervention.

***

## 💻 Tech Stack, Services & Frameworks

This project integrates a comprehensive suite of modern, cloud-native technologies to build a robust and scalable system. The stack was chosen to optimize for performance, maintainability, and operational efficiency on the Google Cloud Platform.

### Cloud Provider

*   **Google Cloud Platform (GCP):** The entire infrastructure is hosted on GCP, leveraging its managed services to reduce operational overhead.

### GCP Services

*   **Data Ingestion & Streaming:**
    *   **Google Cloud Pub/Sub:** Serves as the high-throughput, real-time messaging bus for ingesting raw transaction events and for distributing scored transactions to downstream consumers.
*   **Data Processing:**
    *   **Google Cloud Dataflow:** The managed Apache Beam runner for executing the serverless, auto-scaling real-time streaming pipeline.
    *   **Google Cloud Dataproc:** The managed Apache Spark service used to run the hourly batch ETL jobs that process data from the data lake into the data warehouse.
*   **Data Storage & Analytics:**
    *   **Google BigQuery:** The core serverless Data Warehouse for storing structured historical data, performing large-scale analytics, and serving as the source for BI and feature engineering.
    *   **Google Cloud Firestore:** A highly scalable NoSQL database used in Datastore mode as the low-latency cache for real-time feature enrichment.
    *   **Google Cloud Storage (GCS):** The central object store used as the system's data lake (for raw and processed data), as a staging area for batch jobs, and for storing model artifacts.
*   **Machine Learning & Application Hosting:**
    *   **Google Cloud Run:** The serverless container hosting platform used to deploy the stateless ML inference service and the real-time action service, providing auto-scaling from zero.
    *   **Google Artifact Registry:** A managed registry for storing and managing Docker container images.
    *   **Google Cloud Build:** Used for automating the process of building and publishing container images.
*   **Orchestration & Eventing:**
    *   **Google Cloud Composer:** The managed Apache Airflow service used to schedule, orchestrate, and monitor the complex batch workflows of the cold path.
    *   **Google Cloud Eventarc:** Manages event-driven triggers, connecting Pub/Sub topics to Cloud Run services for asynchronous processing.

### Programming Language

*   **Python:** The exclusive language used for all data processing, machine learning, API development, and scripting.

### Data Processing & Scientific Computing Frameworks

*   **Apache Beam:** Used to define the complex, multi-stage real-time data processing pipeline that runs on Dataflow.
*   **Apache Spark:** The core framework for the large-scale, batch ETL job that transforms data from GCS and loads it into BigQuery.
*   **Pandas & NumPy:** Essential libraries for data manipulation and numerical computation, used primarily within the ML model training pipeline and inference service.

### Machine Learning

*   **Scikit-learn:** Used to build the end-to-end model training `Pipeline`, which encapsulates preprocessing (imputation, scaling, encoding) and the final estimator.
*   **XGBoost:** The core gradient boosting library used to train the high-performance fraud detection classification model.
*   **Optuna:** A modern hyperparameter optimization framework used to efficiently search for the best model parameters during training.
*   **Joblib:** For serializing and deserializing the trained Scikit-learn pipeline object.

### MLOps

*   **Weights & Biases (W&B):** Integrated into the training pipeline for comprehensive experiment tracking, model versioning, metric visualization, and storing model artifacts.

### API & Web Frameworks

*   **FastAPI:** A high-performance Python web framework used to build the REST API for the ML inference service.
*   **Uvicorn & Gunicorn:** An ASGI server and process manager, respectively, used to serve the FastAPI application in the production Cloud Run environment.

### Orchestration

*   **Apache Airflow:** The core workflow management platform used to define, schedule, and monitor all batch processes, including the hourly ETL and the daily feature store refresh.

### Containerization

*   **Docker & Docker Compose:** Used to containerize all applications (ML training, inference, etc.) and to orchestrate a local development environment that mirrors the cloud setup.

### Data Formats

*   **JSON (Newline Delimited):** The standard format for messages in Pub/Sub, API requests/responses, and for intermediate data exported to GCS from BigQuery.
*   **Parquet:** The primary storage format for the data lake in GCS. It is a highly efficient, compressed, columnar format ideal for Spark and BigQuery.

***

## 📂 Codebase & Pipeline Components

The project is organized into a modular structure that reflects the separation of concerns between data processing, machine learning, and orchestration. This design makes the system easier to navigate, maintain, and extend.

### Directory Structure Breakdown

```
real-time-fraud-detection-pipeline/
├── data_pipelines/
│   ├── batch/
│   │   └── transactions_etl_to_dwh/  # Dataproc Spark ETL Job
│   └── streaming/
│       ├── processing_pipeline/      # Dataflow Streaming Pipeline
│       └── real_time_action/         # Cloud Run Action Service
├── machine_learning/
│   ├── inference_service/          # Cloud Run ML Inference Service
│   └── training_pipeline/          # Dockerized ML Training Pipeline
├── orchestration/
│   ├── dags/                       # Airflow DAGs
│   ├── plugins/                    # Custom Airflow Plugins
│   ├── data/
│   └── docker-compose.yaml         # For local Airflow development
├── scripts/
│   ├── deployment/                 # Deployment scripts for all GCP services
│   └── local/                      # Scripts for local execution
└── templates/
    └── transactions_etl_workflow.yaml # Dataproc Workflow Template
```

### Key Modules Explained

#### `data_pipelines/`
This directory contains the core data transformation logic for both the hot and cold paths.

*   **`data_pipelines/streaming/`**: Houses all real-time processing components.
    *   `processing_pipeline/`: This is the main **Apache Beam** pipeline designed to run on **Google Cloud Dataflow**. Its `main.py` and `pipeline.py` define the multi-stage streaming logic: consuming from Pub/Sub, enriching with Firestore, calling the ML service, and fanning out results to a downstream Pub/Sub topic and the GCS data lake.
    *   `real_time_action/`: A lightweight **FastAPI** service intended for deployment on **Cloud Run**. It subscribes to the final "scored transactions" Pub/Sub topic and is responsible for executing business logic based on the fraud score (e.g., logging high-risk alerts).

*   **`data_pipelines/batch/`**: Contains the code for the cold path's batch ETL job.
    *   `transactions_etl_to_dwh/`: An **Apache Spark** job designed to be executed on **Google Cloud Dataproc**. Its `main.py` reads the hourly data from the GCS data lake, performs transformations to create a star schema, and loads the data into **BigQuery**.

#### `machine_learning/`
This module encapsulates the entire machine learning lifecycle, from training to serving. It is intentionally decoupled from the data pipelines.

*   **`machine_learning/training_pipeline/`**: A containerized Python application that orchestrates the model training process. It handles data preparation, hyperparameter optimization using **Optuna**, and experiment tracking with **Weights & Biases**. It produces a serialized model file (`model.joblib`) as its final artifact.
*   **`machine_learning/inference_service/`**: A high-performance **FastAPI** application designed to be deployed on **Cloud Run**. It loads the trained model artifact and exposes a `/predict` endpoint. This service receives a batch of enriched transaction data and returns fraud scores in real-time.

#### `orchestration/`
This directory contains all the components needed to schedule and automate the system's batch workflows using **Apache Airflow**.

*   `orchestration/dags/`: This is where the Airflow DAGs (Directed Acyclic Graphs) are defined.
    *   `load_to_dwh_dag.py`: The hourly DAG that triggers the Dataproc Spark job.
    *   `features_export_dag.py`: The daily DAG that orchestrates the refresh of the Firestore feature cache from BigQuery.
*   `orchestration/plugins/`: Contains custom extensions for Airflow.
    *   `gcs_firestore/`: A custom plugin that includes a `GCSToFirestoreOperator`, making it simple to load data from files in GCS directly into a Firestore collection within a DAG.
*   `orchestration/docker-compose.yaml`: A configuration file for running a complete Airflow environment (scheduler, webserver, worker) locally using Docker, which is invaluable for development and testing of DAGs.

#### `scripts/`
This module holds all the automation scripts for infrastructure setup and application deployment.

*   `scripts/deployment/`: A collection of shell scripts (`.sh`) that automate the deployment of every component to GCP. There are dedicated scripts for deploying the Dataflow job, the Dataproc workflow template, the Cloud Run services, and for managing the Cloud Composer environment.
*   `scripts/local/`: Contains scripts for running parts of the pipeline on a local machine, which is useful for quick testing and development cycles.

#### `templates/`
This directory contains declarative configuration templates.

*   `transactions_etl_workflow.yaml`: A **Dataproc Workflow Template**. This YAML file defines the Spark job, its dependencies (like the BigQuery connector), and the cluster configuration. This allows the job to be submitted to Dataproc as a pre-defined, reusable workflow, which is then triggered by Airflow.

***

## ⚡ The Hot Path: Real-Time Transaction Scoring

The Hot Path is the core of the fraud detection system, engineered for speed and reactivity. Its singular purpose is to process and score each incoming transaction in near real-time, typically within a few seconds. This entire workflow is orchestrated by a serverless, auto-scaling **Google Cloud Dataflow** pipeline.

### Step 1: Ingestion from Payment Gateway to Pub/Sub

The journey begins the moment a financial transaction is initiated.

*   **Event Source:** A payment gateway (emulated by a VM in this project) generates a transaction event.
*   **Ingestion Point:** This event is immediately published as a lightweight JSON message to a **Google Cloud Pub/Sub** topic named `raw_transactions`.

Pub/Sub acts as the highly scalable and durable front door to the entire system. It decouples the payment gateway from the processing pipeline, allowing each to operate and scale independently. It can absorb massive spikes in transaction volume without overwhelming the downstream services.

An example of the raw, unenriched message looks like this:

```json
{
    "transaction_id": "txn_91b145c2-0f71-4053-a09b-ba0a85c406c4",
    "timestamp": "2025-01-01T00:06:04Z",
    "amount": 453,
    "currency": "EGP",
    "user_id": "usr_46cf5be8",
    "merchant_id": "merch_193d1b1f-096",
    "card_id": "tok_4a1e5316-18e",
    ...
}
```

### Step 2: Streaming Enrichment & Prediction (Dataflow)

The Dataflow pipeline, defined in the `data_pipelines/streaming/processing_pipeline/` module, immediately consumes messages from the `raw_transactions` topic and performs a series of stateful operations in-flight.

1.  **Consume & Parse:** The pipeline reads the raw JSON message. The first step inside the `ParseAndTimestampDoFn` transform is to validate the message format and extract the event's `timestamp`, which is crucial for event-time processing and windowing.

2.  **Real-Time Enrichment via Firestore Cache:** The raw transaction data is insufficient for our ML model, which requires historical context. To get this context without querying the slow, analytical Data Warehouse (BigQuery), the pipeline performs a lookup against a **Google Cloud Firestore** database.
    *   Firestore acts as a **low-latency feature store cache**. It contains pre-computed features for every user and merchant (e.g., `user_avg_tx_amount_30d`, `user_account_age_days`).
    *   Using the `user_id` and `merchant_id` from the transaction, the `PredictFraudBatchDoFn` transform fetches the relevant documents from Firestore and merges their data into the transaction object.

3.  **ML Inference via Decoupled Cloud Run Service:** With the transaction now fully enriched, it is ready for scoring.
    *   **Strategic Batching:** To optimize performance and reduce network overhead, the Dataflow pipeline uses a `GroupIntoBatches` transform. It collects transactions into small micro-batches (e.g., 50 transactions or whatever arrives within a 2-second window).
    *   **API Call:** A single HTTP POST request containing this batch of enriched transactions is sent to the ML inference service—a **FastAPI** application deployed on **Google Cloud Run**.
    *   **Scoring:** The Cloud Run service processes the batch, generates a fraud score for each transaction, and returns a list of predictions. The Dataflow pipeline then merges these scores back into their corresponding transaction objects.

### Step 3: Action & Downstream Notification

After a batch of transactions is successfully scored, the enriched data (now including the `fraud_score`) is published to a final Pub/Sub topic, `scored_transactions`.

This "fan-out" approach is a key design pattern. Publishing to a final topic allows multiple, independent services to subscribe and react to the scored events. In this project, the **`real_time_action` service** (a simple Cloud Run service) acts as a subscriber. It is responsible for containing the business logic that runs after a score is received, such as:
*   Logging a high-severity alert if `fraud_score` > `0.75`.
*   Triggering a notification to a fraud review team.
*   In a more advanced scenario, initiating an action to block a user's account.

### Step 4: Error Handling in the Hot Path

The hot path is designed for resilience using a multi-stage **Dead-Letter Queue (DLQ)** strategy. This ensures that a small number of bad messages or transient errors do not halt the entire pipeline.

*   **Dead-Letter Queue for Parsing Failures:**
    *   **Trigger:** An incoming message from Pub/Sub is malformed (e.g., not valid JSON, missing a required field).
    *   **Action:** The `ParseAndTimestampDoFn` catches the exception. The raw, unprocessed byte string of the message is immediately diverted to a dedicated folder in **Google Cloud Storage** (`gs://<bucket>/dlq/parsing_dlq/`).
    *   **Result:** The malformed message is isolated for offline debugging, and the pipeline continues processing valid messages without interruption.

*   **Dead-Letter Queue for Processing & Enrichment Failures:**
    *   **Trigger:** An error occurs during the main enrichment or prediction stage. This could be a network timeout when calling the ML service, an unexpected `500` error from the API, or a failure to retrieve a document from Firestore.
    *   **Action:** The `PredictFraudBatchDoFn` catches the exception. The *entire batch of transactions* that was being processed is serialized to a JSON string and written to a separate folder in **GCS** (`gs://<bucket>/dlq/processing_dlq/`).
    *   **Result:** This prevents data loss and allows engineers to analyze the exact batch that caused the failure. The pipeline continues to process subsequent batches.
 
***

## 🧊 The Cold Path: Analytics and Feature Engineering

The Cold Path is the system's analytical backbone and its long-term memory. Unlike the hot path, which is optimized for immediate, low-latency reactions, the cold path is designed for high-throughput, scheduled batch processing of large data volumes. Its primary goals are to build a historical source of truth in a data warehouse and to power the real-time system through an automated feature engineering feedback loop.

### Step 1: Archiving to the Data Lake (Dataflow to GCS)

The entry point to the cold path originates within the Dataflow streaming pipeline itself. Simultaneously with its real-time duties, the pipeline archives the enriched and scored transaction data for batch processing.

*   **Windowing and Batching:** The Dataflow pipeline uses a `FixedWindows` transform to group the continuous stream of scored transactions into hourly batches. This ensures that data is collected and organized based on its event time.
*   **Writing to Google Cloud Storage:** At the end of each window, the pipeline writes the batch of data as one or more **Parquet** files to a **Google Cloud Storage (GCS)** bucket. This GCS bucket acts as the system's **data lake**. The data is written to a partitioned path structure to allow for efficient querying by batch jobs:
    `gs://<your-bucket>/raw_processed_transactions/year=YYYY/month=MM/day=DD/hour=HH/`

This process effectively creates an immutable, hour-by-hour log of all scored transactions, ready for large-scale analysis.

### Step 2: Orchestrated Batch Workflows (Cloud Composer & Airflow)

All batch workflows are orchestrated by **Google Cloud Composer (Apache Airflow)**. Airflow is responsible for scheduling, triggering, and monitoring the two critical jobs of the cold path.

#### Hourly DWH Load

*   **Trigger:** An Airflow DAG (`load_to_dwh_dag.py`) runs on an hourly schedule (`@hourly`).
*   **Action:** The DAG's primary task is to trigger a pre-defined **Dataproc Workflow Template**. This template instantiates an ephemeral Spark cluster on **Google Cloud Dataproc**.
*   **The Spark Job:** The Spark job (`data_pipelines/batch/transactions_etl_to_dwh/`) executes the following logic:
    1.  **Reads** the specific hourly partition of Parquet files from the GCS data lake corresponding to the DAG's execution hour.
    2.  **Transforms** the flat data structure into a relational **star schema**, separating out dimensions (like Users, Merchants, Cards) from the central transaction fact table.
    3.  **Loads** the transformed data into the final fact and dimension tables in the **Google BigQuery** Data Warehouse.
*   **Outcome:** The BigQuery DWH is kept consistently up-to-date, with a data lag of no more than one hour.

#### Daily Feature Store Refresh

This is the critical feedback loop that powers the entire system's intelligence.

*   **Trigger:** A separate Airflow DAG (`features_export_dag.py`) runs on a daily schedule (`0 3 * * *` - every day at 3 AM UTC).
*   **Action:** This DAG orchestrates a multi-step process to refresh the features in the real-time Firestore cache:
    1.  **Calculate Features:** It executes a parameterized SQL query (`get_users_historical_features.sql`) directly against the **BigQuery** DWH. This query calculates fresh historical features (e.g., `user_avg_tx_amount_30d`, `user_tx_count_24h`) for all users and merchants.
    2.  **Export to GCS:** The results of the query are exported from BigQuery as newline-delimited JSON files to a temporary location in GCS.
    3.  **Load to Firestore:** The DAG then uses a custom **`GCSToFirestoreOperator`** to read these JSON files and perform a batch write to the **Firestore** cache, overwriting the existing documents with the newly calculated features.
*   **Outcome:** The Firestore feature store is refreshed daily, ensuring the hot path's real-time enrichment process is always working with recent, relevant historical data.

### Step 3: Error Handling in the Cold Path

The cold path is designed with multiple layers of resilience to ensure the reliability of its batch workflows.

*   **Proactive Data Sensing:** Before launching an expensive Dataproc job, the hourly Airflow DAG uses a `GCSObjectsWithPrefixExistenceSensor`. This task checks if the data for that specific hour has actually arrived in the GCS data lake. If the data is missing (e.g., due to an upstream delay), the sensor will wait, preventing the system from wasting resources on a job that is guaranteed to fail. This is a crucial "fail-fast" mechanism.
*   **Automated Retries and Alerts:** All tasks within the Airflow DAGs are configured with a retry policy. This allows them to automatically recover from transient issues, such as a temporary network blip or a brief API outage. If a task fails after all retries are exhausted, Airflow's `on_failure_callback` system is configured to trigger an alert, notifying the engineering team that manual intervention is required.
*   **Centralized Logging and Monitoring:** The logs for all Airflow tasks and the Dataproc Spark jobs they trigger are centralized in **Google Cloud Logging**. The Airflow UI provides direct links to these logs, making it easy to diagnose the root cause of any failures in the batch processing system.

***

## 🤖 Automation: Infrastructure and Deployment

To ensure consistency, repeatability, and to minimize manual error, the entire process of setting up the cloud infrastructure and deploying the application code is automated using a collection of shell scripts. These scripts act as a practical form of "Infrastructure as Code," allowing a new environment to be provisioned and configured with a series of simple commands.

All automation scripts are located in the `scripts/` directory.

### Overview of the `scripts/` Directory

*   **`scripts/deployment/`**: This directory contains the primary scripts for deploying each component of the application to the Google Cloud Platform.
*   **`scripts/deployment/setup/`**: These are one-time setup scripts used to configure foundational GCP resources and permissions that the deployment scripts depend on.
*   **`scripts/local/`**: This directory holds helper scripts for running parts of the pipeline on a local developer machine, useful for testing and debugging.

### Automated Setup and Deployment Workflow

A typical workflow to stand up the entire system from scratch using these scripts would involve the following steps:

#### 1. Initial Service and Permission Configuration
Before deploying the applications, the foundational GCP services and IAM (Identity and Access Management) permissions must be in place. This is handled by the scripts in `scripts/deployment/setup/`:

*   **`configure_service_permissions.sh`**: This crucial script creates dedicated IAM service accounts for the Dataflow and Dataproc services. It then grants them the specific, fine-grained permissions they need to interact with other GCP services like GCS, BigQuery, Pub/Sub, and Firestore. This follows the principle of least privilege.
*   **`setup_bq_sub.sh`**: This script configures the Pub/Sub topic and a BigQuery subscription. This is used for creating a direct ingestion path from a Pub/Sub topic to a BigQuery table, which can be useful for certain raw data streams.

#### 2. Deploying the Cloud Services and Applications
Once the foundational setup is complete, the main deployment scripts in `scripts/deployment/` are used to deploy each component:

*   **`deploy_ml_service.sh`**: This script handles the end-to-end deployment of the machine learning inference service. It performs several actions:
    1.  Builds the FastAPI application into a Docker container using **Google Cloud Build**.
    2.  Pushes the container image to **Google Artifact Registry**.
    3.  Deploys the container to **Google Cloud Run**, configuring environment variables (like the path to the model artifact in GCS), service account identity, and scaling parameters.

*   **`deploy_dataflow.sh`**: This script launches the real-time streaming pipeline. It packages the Apache Beam code and submits it as a new job to the **Google Cloud Dataflow** service. It accepts parameters to configure the job, such as the input and output Pub/Sub topics, the path to the ML service, and the service account to run as. It also supports an `--update` flag to perform in-place updates of an already running pipeline.

*   **`deploy_dataproc.sh`**: This script deploys the batch ETL workflow. It first packages the PySpark application and uploads it to GCS. It then uses the `gcloud` CLI to import the `transactions_etl_workflow.yaml` file, creating a reusable **Dataproc Workflow Template**. This template defines the Spark job and the configuration of the ephemeral cluster it runs on.

*   **`deploy_real_time_action.sh`**: Similar to the ML service deployment, this script deploys the simple alerting service. It builds the container and deploys it to **Cloud Run**. It also uses **Google Cloud Eventarc** to create a trigger that automatically invokes this service whenever a message is published to the `scored_transactions` Pub/Sub topic.

*   **`manage_composer.sh`**: A comprehensive script for managing the lifecycle of the **Google Cloud Composer** environment. It supports commands to `create`, `delete`, `stop`, `start`, and `deploy` artifacts (DAGs and plugins) to the environment, providing full programmatic control over the orchestration infrastructure.

***

## ✨ Future Work

This project provides a solid and functional foundation for a real-time fraud detection system. However, to further enhance its robustness, maintainability, and development velocity in a production setting, the following improvements are recommended as next steps.

### 10.1 Infrastructure as Code (IaC) with Terraform

Currently, the cloud infrastructure is provisioned using a series of imperative shell scripts located in the `scripts/deployment/` directory. While effective for initial setup, this approach has limitations in managing the long-term state and evolution of the infrastructure.

**The next logical step is to migrate the entire infrastructure provisioning process to a declarative Infrastructure as Code (IaC) framework like HashiCorp Terraform.**

**Benefits of Migrating to Terraform:**

*   **Declarative and Idempotent:** Instead of defining *how* to create resources (the steps), you declare the desired *state* of the infrastructure. Terraform then figures out the most efficient way to achieve that state. Rerunning the same configuration will not cause errors or duplicate resources; it will simply ensure the cloud environment matches the code.
*   **State Management:** Terraform creates a state file that keeps track of all managed resources. This provides a single source of truth and allows for planning changes before applying them (`terraform plan`), giving visibility into what will be created, modified, or destroyed.
*   **Dependency Graphing:** Terraform automatically understands the dependencies between resources. For example, it knows it must create a Pub/Sub topic before creating a subscription for it, and it will provision them in the correct order without needing to be explicitly told.
*   **Modularity and Reusability:** The entire GCP setup could be encapsulated into reusable Terraform modules, making it easier to spin up new environments (e.g., for staging, development, or disaster recovery) with consistency and confidence.
*   **Improved Auditing and Version Control:** With the entire infrastructure defined as code in a Git repository, every change is versioned, reviewable through pull requests, and auditable, significantly improving governance.

### 10.2 Continuous Integration & Deployment (CI/CD)

The current workflow relies on developers manually running deployment scripts from their local machines. To accelerate development cycles and improve code quality, **implementing a formal CI/CD pipeline using a tool like GitHub Actions is highly recommended.**

A robust CI/CD pipeline would automate the following workflows:

**Continuous Integration (On Pull Request):**
1.  **Code Linting & Formatting:** Automatically check all submitted Python code for style consistency and potential errors.
2.  **Unit & Integration Testing:** Execute the test suites for the Airflow DAGs, data pipeline components, and the ML inference service to catch bugs before they are merged.
3.  **Build Validation:** For services with Dockerfiles (like the ML inference service), the CI pipeline would run a `docker build` to ensure the container can be successfully built.

**Continuous Deployment (On Merge to `main` branch):**
1.  **Build and Push Artifacts:**
    *   For containerized applications, the pipeline would automatically build the Docker image, tag it with the commit SHA, and push it to **Google Artifact Registry**.
    *   For the Dataflow and Dataproc jobs, it would package the code and upload it to GCS.
2.  **Deploy to Staging:** Automatically deploy the new version of the application or data pipeline to a dedicated staging environment in GCP.
3.  **Run End-to-End Tests:** Execute a suite of automated tests against the staging environment to ensure all components work together correctly.
4.  **Promote to Production:** Upon successful testing in staging (and potentially a manual approval step), the pipeline would automatically deploy the new version to the production environment, ensuring zero-downtime updates for services like Cloud Run and Dataflow.

***

## 📜 License

This project is licensed under the MIT License. See the [LICENSE](LICENSE.md) file for details.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
