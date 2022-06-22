import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_gcs_dag_load_taxi_fhv",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    for item in ("yellow", "green", "fhv"):

        move_files_gcs_task = GCSToGCSOperator(
            task_id=f'move_{item}_files_task',
            source_bucket=BUCKET,
            source_object=f'raw/{item}_tripdata*.parquet',
            destination_bucket=BUCKET,
            destination_object=f'{item}/{item}_tripdata',
            move_object=True
        )

        bigquery_external_table_task = BigQueryCreateExternalTableOperator(
            task_id=f"bigquery_{item}_external_table_task",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": f"{item}_tripdata_external_table",
                },
                "externalDataConfiguration": {
                    "sourceFormat": "PARQUET",
                    "sourceUris": [f"gs://{BUCKET}/{item}/*"],
                },
            },
        )


        move_files_gcs_task >> bigquery_external_table_task