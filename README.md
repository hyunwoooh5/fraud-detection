# End-to-End Fraud Detection Data Pipeline



## Overview

This project implements an end-to-end data engineering pipeline designed to process, transform, and visualize financial transaction data for fraud detection. The architecture leverages local computation for Spark-based ETL and Airflow orchestration, targeting Google Cloud Platform (GCS, BigQuery) for storage, and dbt for in-warehouse data transformations.

-----

## Problem Statement

Financial fraud results in significant monetary losses and operational inefficiencies. Identifying fraudulent transactions within highly imbalanced datasets requires robust feature engineering and reliable data pipelines. A scalable infrastructure is necessary to consistently deliver processed transaction data to analytical dashboards and downstream machine learning models.

-----

## Objective

1. **Develop a robust ETL pipeline** to process and augment raw transaction data using PySpark.
2. **Implement workflow orchestration** using Apache Airflow to automate data generation, cloud storage uploads, data warehouse loading, and data transformations.
3. **Execute data modeling** using dbt to aggregate daily fraud metrics and create analytical data marts.
4. **Visualizing fraud patterns** and anomalies via an interactive dashboard in Google Looker Studio.


-----

## Architecture

![Diagram](images/diagram.svg)


-----

## Dataset

* **Source:** [Kaggle Dataset - Synthetic Financial Datasets For Fraud Detection](https://www.kaggle.com/datasets/ealaxi/paysim1)
* **Processing:** The raw dataset is augmented randomly via a PySpark generator to scale the volume and simulate continuous time-series data for temporal analysis.


----

## Setup


### Environment Variables (`.envrc`)

Configure your local environment variables to ensure Airflow and GCP SDK operate correctly without macOS multiprocessing conflicts.

```bash
export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/gcp-pipeline-key.json"
export AIRFLOW_HOME="$(pwd)"
export no_proxy="*"
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
```

Apply the variables using direnv:

```bash
direnv allow
```

### dbt Configuration (`~/.dbt/profiles.yml`)

Configure the dbt profile to connect to your BigQuery dataset using the generated service account key.

```yaml
fraud_detection:
  outputs:
    dev:
      dataset: fraud_detection
      job_execution_timeout_seconds: 300
      job_retries: 1
      keyfile: /path/to/your/service-account-key.json
      location: us-east1
      method: service-account
      priority: interactive
      project: project_id
      threads: 4
      type: bigquery
  target: dev
```


### Google Cloud Authentication

Authenticate your local environment with Google Cloud.

```bash
gcloud auth application-default login
```


-----

## Steps

**1. Data Preparation**

Download the Kaggle dataset and place the CSV file into the local `data/` directory.



**2. Infrastructure as Code (Terraform)**

Provision the necessary GCP resources (GCS Bucket, BigQuery Dataset, Service Account, and IAM roles) and extract the authentication key.

```bash
cd infrastructure
terraform init
terraform plan
terraform apply -auto-approve
terraform output -raw service_account_key | base64 --decode > ../gcp-pipeline-key.json
```

*Note: `variables.tf` should be updated with your project_id before.*

Set the credentials for the current session:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/gcp-pipeline-key.json"
```

**3. Airflow Initialization**

Initialize the Airflow SQLite database and create an admin user for the Web UI.

```bash
# Set current project directory as AIRFLOW_HOME
export AIRFLOW_HOME="$(pwd)"

# Initialize the Airflow SQLite database
airflow db migrate

# Create an admin user for the Web UI
airflow users create \
    --username admin \
    --firstname Data \
    --lastname Engineer \
    --role Admin \
    --email admin@example.com \
    --password admin
```


**4. Airflow GCP Connection Setup**

Add the GCP connection to Airflow's internal metadata database using the CLI to allow operators to interact with GCS and BigQuery.

```bash
airflow connections add 'google_cloud_default' \
    --conn-type 'google_cloud_platform' \
    --conn-extra '{"key_path": "/path/to/project/fraud-detection/gcp-pipeline-key.json", "project": "your-project-name"}'
```

**5. Start Airflow**
Launch the Airflow scheduler and webserver in standalone mode.

```bash
airflow standalone
```

*Note: Log in to `http://localhost:8080` using the credentials found in `simple_auth_manager_passwords.json` or the ones you manually configured.*



**6. Execute the Pipeline**

In the Airflow Web UI, unpause and trigger the `fraud_detection_pipeline` DAG. Monitor the Graph View to ensure all 5 tasks (Spark Generation → Spark ETL → GCS Upload → BigQuery Load → dbt Build) complete successfully.

**7. Dashboard Visualization**

Connect Google Looker Studio to the `fraud_monitor_daily` table in BigQuery.

-----


## Results

**Dashboard by Google Looker Studio**

The dashboard visualizes the correlation between daily fraud attempts and transaction error balances, providing an objective overview of systematic anomalies generated by the data pipeline.


![Dashboard](images/dashboard.png)


------


## License

This project is licensed under the MIT License.