import os
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from pathlib import Path
import hcl2

CURRENT_DIR = Path(__file__).resolve().parent

with open(CURRENT_DIR.parent / "terraform" / "variables.tf", "r") as f:
    dict_data = hcl2.load(f)

# Project variable
PROJECT_ROOT = CURRENT_DIR.parent
PROJECT_ID = dict_data["variable"][0]["project_id"]["default"]
BUCKET_NAME = f"fraud-detection-de-{PROJECT_ID}"
DATASET_NAME = "fraud_detection"

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
}

with DAG(
    dag_id='fraud_detection_pipeline',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['fraud_detection', 'pyspark', 'dbt'],
) as dag:

    # Task 1: Spark data generation
    generate_data = BashOperator(
        task_id='generate_augmented_data',
        bash_command='uv run spark_jobs/data_generator.py',
        cwd=PROJECT_ROOT,
    )

    # Task 2: Spark ETL
    run_etl = BashOperator(
        task_id='run_spark_etl',
        bash_command='uv run spark_jobs/etl_job.py',
        cwd=PROJECT_ROOT,
    )

    # Task 3: Parquet to GCS
    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_parquet_to_gcs',
        src='data/transactions_augmented_parquet/*.parquet',
        dst='processed/transactions/',
        bucket=BUCKET_NAME,
        gcp_conn_id='google_cloud_default', # need to be set up
    )

    # Task 4: GCS to BigQuery
    load_to_bq = GCSToBigQueryOperator(
        task_id='gcs_to_bigquery',
        bucket=BUCKET_NAME,
        source_objects=['processed/transactions/*.parquet'],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_NAME}.raw_transactions",
        source_format='PARQUET',
        write_disposition='WRITE_TRUNCATE',
        gcp_conn_id='google_cloud_default',
    )

    # Task 5: dbt build
    run_dbt = BashOperator(
        task_id='run_dbt_build',
        bash_command='cd dbt && dbt build',
        cwd=PROJECT_ROOT,
    )

    # dependency
    generate_data >> run_etl >> upload_to_gcs >> load_to_bq >> run_dbt