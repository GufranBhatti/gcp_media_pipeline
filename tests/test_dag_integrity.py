import os
import pytest
from airflow.models import DagBag

# Point pytest to your local DAGs folder
DAG_PATH = os.path.join(os.path.dirname(__file__), '..', 'dags')

def test_no_import_errors():
    """Test to ensure all DAGs load without any Python syntax or import errors."""
    dag_bag = DagBag(dag_folder=DAG_PATH, include_examples=False)
    
    # If this list is not empty, a DAG is broken!
    assert len(dag_bag.import_errors) == 0, f"DAG import failures: {dag_bag.import_errors}"

def test_dag_count():
    """Ensure our 3 core DAGs are present."""
    dag_bag = DagBag(dag_folder=DAG_PATH, include_examples=False)
    expected_dags = ['gcp_media_extractor', 'gcp_spark_standardization', 'gcp_load_to_bigquery']
    
    for dag_id in expected_dags:
        assert dag_id in dag_bag.dags, f"Missing DAG: {dag_id}"