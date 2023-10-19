import os
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from google.cloud import storage
import pyarrow.csv as pv
import pyarrow.parquet as pq

from upload_gcs import upload_to_gcs

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
ZONE_URL = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output_zones.csv'
BUCKET = os.environ.get("GCP_GCS_BUCKET")


def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error(
            "Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="ingestion_zones",
    start_date=days_ago(1),
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    fetch_data_zones = BashOperator(
        task_id="fetch_data_zones",
        bash_command=f"curl -sSL {ZONE_URL} > {OUTPUT_FILE_TEMPLATE}"
    )

    format_to_parquet = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{OUTPUT_FILE_TEMPLATE}",
        },
    )

    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    local_to_gcs = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/zones",
            "local_file": f"{OUTPUT_FILE_TEMPLATE.replace('.csv', '.parquet')}",
        },
    )
    '''
    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{parquet_file}"],
            },
        },
    )
    '''
    fetch_data_zones >> format_to_parquet >> local_to_gcs
