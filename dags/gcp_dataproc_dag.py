from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)

# --- CONFIGURATION ---
PROJECT_ID = "streaming-media-platform"  # <--- CHANGE THIS TO YOUR GCP PROJECT ID
REGION = "us-central1"
CLUSTER_NAME = "ephemeral-spark-cluster"
# The path where you uploaded the script in Step 2
PYSPARK_URI = "gs://gufran-streaming-raw/scripts/process_media_data.py"

# Define the hardware and security for the temporary cluster
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},
    },
    "gce_cluster_config": {
        "service_account": "airflow-orchestrator@streaming-media-platform.iam.gserviceaccount.com",
        "service_account_scopes": ["https://www.googleapis.com/auth/cloud-platform"]
    }
}

# Define the Spark Job payload
SPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
}

# --- DAG DEFINITION ---
with DAG(
    'gcp_spark_standardization',
    start_date=datetime(2026, 5, 1),
    schedule_interval=None, # Running manually for now
    catchup=False
) as dag:

    # Task 1: Boot up the cloud servers
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    # Task 2: Send the PySpark code to the servers
    submit_spark_job = DataprocSubmitJobOperator(
        task_id="submit_spark_job",
        job=SPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID,
    )

    # Task 3: Destroy the servers (Trigger rule ensures it deletes even if the Spark job fails)
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule="all_done", 
    )

    # The Pipeline Flow
    create_cluster >> submit_spark_job >> delete_cluster