from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col

# 1. Initialize Cloud Spark Session
spark = SparkSession.builder \
    .appName("MediaDataStandardization") \
    .getOrCreate()

# Replace with your exact bucket names
RAW_BUCKET = "gs://gufran-streaming-raw"
STD_BUCKET = "gs://gufran-streaming-standardized"

# 2. Process Billing Data
print("Processing Billing Data...")
df_billing = spark.read.json(f"{RAW_BUCKET}/billing/*.json")
df_billing_clean = df_billing.withColumn("loaded_at", current_timestamp())
df_billing_clean.write.mode("overwrite").parquet(f"{STD_BUCKET}/billing/")

# 3. Process API Metadata
print("Processing Movie API Data...")
df_movies = spark.read.json(f"{RAW_BUCKET}/api/*.json")
df_movies_clean = df_movies.withColumn("loaded_at", current_timestamp())
df_movies_clean.write.mode("overwrite").parquet(f"{STD_BUCKET}/api/")

# 4. Process High-Volume Watch Logs
print("Processing Watch Logs...")
df_logs = spark.read.json(f"{RAW_BUCKET}/logs/*.jsonl")
# Data Quality Check: Filter out any corrupted records with 0 or negative watch times
df_logs_clean = df_logs.filter(col("watch_duration_mins") > 0).withColumn("loaded_at", current_timestamp())
# Partitioning the large logs dataset by movie_id to optimize future queries
df_logs_clean.write.mode("overwrite").parquet(f"{STD_BUCKET}/logs/")

print("Standardization Complete!")
spark.stop()