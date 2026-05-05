from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Inside Astro Docker container, project root is mounted at /usr/local/airflow
sys.path.insert(0, "/usr/local/airflow")

default_args = {
    "owner": "data-engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

def run_ingest(**context):
    from scripts.ingest import ingest_data
    ingest_data(run_date=context["ds"])

def run_transform(**context):
    from scripts.transform import transform_data
    transform_data(run_date=context["ds"])

def run_validate(**context):
    from scripts.validate import validate_data
    validate_data()

def run_load(**context):
    from scripts.load import load_data
    load_data(run_date=context["ds"])

with DAG(
    dag_id="batch_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["data-pipeline", "pyspark"]
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest",
        python_callable=run_ingest
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=run_transform
    )

    validate_task = PythonOperator(
        task_id="validate",
        python_callable=run_validate
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=run_load
    )

    ingest_task >> transform_task >> validate_task >> load_task
