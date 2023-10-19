import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from upload_gcs import upload_to_gcs


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
# BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')
# PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DL_URL = URL_PREFIX + \
    '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + \
    '/output_fhv_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
# TABLE_NAME_TEMPLATE = 'yellow_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="ingestion_fhv_v01",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2021, 1, 1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['de-taxi'],
) as dag:

    fetch_data = BashOperator(
        task_id="fetch_data_fhv",
        bash_command=f'curl -sSLf {DL_URL} > {OUTPUT_FILE_TEMPLATE}'
    )

    local_to_gcs = PythonOperator(
        task_id="fhv_to_gcs",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": "raw/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet",
            "local_file": OUTPUT_FILE_TEMPLATE
        },
    )

    fetch_data >> local_to_gcs
