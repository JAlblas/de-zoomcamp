import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from upload_gcs import upload_to_gcs


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DL_URL = URL_PREFIX + \
    '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + \
    '/output_yellow_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
# TABLE_NAME_TEMPLATE = 'yellow_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="ingestion_yellow_taxi_v03",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2021, 1, 1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['de-taxi'],
) as dag:

    fetch_data = BashOperator(
        task_id="fetch_data_yellow_taxi",
        bash_command=f'curl -sSLf {DL_URL} > {OUTPUT_FILE_TEMPLATE}'
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": "raw/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet",
            "local_file": OUTPUT_FILE_TEMPLATE
        },
    )

    fetch_data >> local_to_gcs_task