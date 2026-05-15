# Start with the official Airflow image
FROM apache/airflow:2.9.0

# Temporarily switch to root to install system-level dependencies (Java for Spark)
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to the airflow user to install Python libraries
USER airflow

# Install Google Cloud libraries and requests
RUN pip install --no-cache-dir \
    google-cloud-storage \
    apache-airflow-providers-google \
    requests \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.0/constraints-3.12.txt"

# Step 2: Install dbt-bigquery SEPARATELY without the constraints
# This prevents the pip resolver from getting stuck in an infinite loop!
RUN pip install --no-cache-dir dbt-bigquery