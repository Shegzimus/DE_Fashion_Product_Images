import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import BigQueryToBigQueryOperator
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.load import upload_folder_to_gcs, process_gcs_paths, create_bigquery_table_callable


# Retrieve variables from Airflow environment
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
STAGING = os.environ.get("BQ_DATASET_STAGING")
WAREHOUSE = os.environ.get("BQ_DATASET_WAREHOUSE")

# Define the DAG and default args
default_args = {
    'owner': 'Oluwasegun',
    'depends_on_past': False,
    'retries': 1,
}

dag = DAG(
    'upload_folders_to_gcs',
    default_args=default_args,
    description='Upload multiple folders after extraction to GCS',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

"""
Task 0: Define the folder paths and their GCS target prefix

"""

bucket_name = BUCKET
folder_paths = [
    {"local_folder": "opt/airflow/data/input/fashion-dataset/images", "gcs_prefix": "fashion-dataset/images"},
    {"local_folder": "opt/airflow/data/input/fashion-dataset/styles", "gcs_prefix": "fashion-dataset/styles"},
    {"local_folder": "opt/airflow/data/output/greyscale", "gcs_prefix": "output/greyscale"},
    {"local_folder": "opt/airflow/data/output/metadata", "gcs_prefix": "output/metadata"},
    {"local_folder": "opt/airflow/data/output/parquet", "gcs_prefix": "output/parquet"}
]



"""
Task 1: Upload the specified folders to GCS Bucket

"""
upload_to_gcs_tasks = []
for folder in folder_paths:
    task = PythonOperator(
        task_id=f"upload_{os.path.basename(folder['local_folder'])}_to_gcs",
        python_callable=upload_folder_to_gcs,
        op_kwargs={
            "bucket_name": bucket_name,
            "local_folder": folder["local_folder"],
            "target_folder_prefix": folder["gcs_prefix"]
        },
        provide_context=True,
        dag=dag,
    )
    upload_to_gcs_tasks.append(task)



"""
Task 2: Collect the XCom output of the file paths generated by the upload task

"""
collect_gcs_paths = PythonOperator(
    task_id="process_gcs_paths",
    python_callable=process_gcs_paths,
    provide_context=True,
    dag=dag,
)


"""
Task 2: Create BigQuery Tables

"""
create_bq_tables_task = create_bigquery_table_callable(
    dataset_id='',
    table_id= '',
    parquet_file_path= '',
)


"""
Task 2: Upload the parquet files to BigQuery

"""
transfer_images_to_bigquery = GCSToBigQueryOperator(
    task_id="transfer_images_to_bigquery",
    bucket=BUCKET,
    source_objects='',
    destination_project_dataset_table=f"{PROJECT_ID}.{STAGING}.fashion_dataset.images",
    write_disposition="WRITE_APPEND",
    create_disposition="CREATE_IF_NEEDED",
    dag=dag,
)


transfer_styles_pq_to_bigquery = GCSToBigQueryOperator(
    task_id="transfer_styles_pq_to_bigquery",
    bucket=BUCKET,
    source_objects='',
    destination_project_dataset_table=f"{PROJECT_ID}.{STAGING}.fashion_dataset.styles",
    write_disposition="WRITE_APPEND",
    create_disposition="CREATE_IF_NEEDED",
    dag=dag,
)

"""
Task 3: Send image files to BigQuery

"""


begin = DummyOperator(task_id="begin", dag=dag)

files_uploaded = DummyOperator(task_id="files_uploaded", dag=dag)

end = DummyOperator(task_id="end", dag=dag)

# Set dependencies
begin >>
for task in upload_to_gcs_tasks:
    task >> collect_gcs_paths



[collect_gcs_paths] >> files_uploaded