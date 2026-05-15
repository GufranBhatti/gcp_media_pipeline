import json
import random
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage

# --- CONFIGURATION ---
# Replace this with your exact bucket name!
RAW_BUCKET = "gufran-streaming-raw"

# --- THE MAGIC UPLOAD FUNCTION ---
def upload_to_gcs(bucket_name, destination_blob_name, data_string):
    """Takes data from memory and streams it directly to Google Cloud Storage."""
    # The storage.Client() automatically looks for the GOOGLE_APPLICATION_CREDENTIALS env variable!
    client = storage.Client() 
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    
    # Upload the string directly to the cloud
    blob.upload_from_string(data_string, content_type='application/json')
    print(f"Successfully uploaded {destination_blob_name} to {bucket_name}.")

# --- EXTRACTION TASKS ---
def extract_billing_db():
    """Simulates pulling paid user subscriptions from a Cloud SQL Postgres DB."""
    users = [{"user_id": i, "plan": random.choice(["Basic", "Premium"]), "country": random.choice(["PK", "US", "UK"])} for i in range(1, 500)]
    file_name = f"billing/users_{datetime.now().strftime('%Y%m%d')}.json"
    upload_to_gcs(RAW_BUCKET, file_name, json.dumps(users))

def extract_movie_api():
    """Simulates pulling content metadata from the TMDB REST API."""
    movies = [
        {"movie_id": 101, "title": "Inception", "genre": "Sci-Fi", "rating": 8.8},
        {"movie_id": 102, "title": "The Godfather", "genre": "Crime", "rating": 9.2},
        {"movie_id": 103, "title": "The Dark Knight", "genre": "Action", "rating": 9.0}
    ]
    file_name = f"api/movies_{datetime.now().strftime('%Y%m%d')}.json"
    upload_to_gcs(RAW_BUCKET, file_name, json.dumps(movies))

def extract_watch_logs():
    """Simulates raw, high-volume clickstream logs from the media player."""
    logs = [{"user_id": random.randint(1, 500), "movie_id": random.choice([101, 102, 103]), "watch_duration_mins": random.randint(5, 120)} for _ in range(2000)]
    
    # Format as JSON-Lines (JSONL) for Big Data processing
    jsonl_data = "\n".join([json.dumps(log) for log in logs])
    file_name = f"logs/watch_events_{datetime.now().strftime('%Y%m%d')}.jsonl"
    upload_to_gcs(RAW_BUCKET, file_name, jsonl_data)

# --- DAG DEFINITION ---
with DAG(
    'gcp_media_extractor',
    start_date=datetime(2026, 5, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    # 1. Define the Operators
    task_db = PythonOperator(task_id='extract_billing_db', python_callable=extract_billing_db)
    task_api = PythonOperator(task_id='extract_movie_api', python_callable=extract_movie_api)
    task_logs = PythonOperator(task_id='extract_watch_logs', python_callable=extract_watch_logs)

    # 2. Set Parallel Execution
    # All three sources extract at the exact same time!
    [task_db, task_api, task_logs]