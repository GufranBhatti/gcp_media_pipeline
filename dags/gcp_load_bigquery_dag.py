from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# --- CONFIGURATION ---
# Replace with your actual Project ID from GCP!
PROJECT_ID = "streaming-media-platform"
DATASET_NAME = "streaming_raw"
STD_BUCKET = "gufran-streaming-standardized"

# --- DAG DEFINITION ---
with DAG(
    'gcp_load_to_bigquery',
    start_date=datetime(2026, 5, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    # 1. Load Billing Data
    # write_disposition='WRITE_TRUNCATE' makes this Idempotent (ACID-like). 
    # If the DAG runs twice, it overwrites the table instead of duplicating data.
    load_billing = GCSToBigQueryOperator(
        task_id='load_billing_to_bq',
        bucket=STD_BUCKET,
        source_objects=['billing/*.parquet'],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_NAME}.users",
        source_format='PARQUET',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True # Automatically figures out columns/datatypes from Parquet!
    )

    # 2. Load API Movie Metadata
    load_movies = GCSToBigQueryOperator(
        task_id='load_movies_to_bq',
        bucket=STD_BUCKET,
        source_objects=['api/*.parquet'],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_NAME}.movies",
        source_format='PARQUET',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True
    )

    # 3. Load High-Volume Watch Logs
    load_logs = GCSToBigQueryOperator(
        task_id='load_logs_to_bq',
        bucket=STD_BUCKET,
        source_objects=['logs/*'], 
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_NAME}.watch_events",
        source_format='PARQUET',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True
    )

    # Run in parallel
    [load_billing, load_movies, load_logs]