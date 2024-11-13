import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.load import upload_folder_to_gcs, create_bigquery_table_callable





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
        dag=dag,
    )
    upload_to_gcs_tasks.append(task)


"""
Task 2: Create BigQuery Tables

"""
create_images_table_task = create_bigquery_table_callable(
    dataset_id='',
    table_id= '',
    parquet_file_path= '',
)




"""
Task 2: Upload the parquet files to BigQuery

"""


"""
Task 3: Send image files to BigQuery

"""


begin = DummyOperator(task_id="begin", dag=dag)

files_uploaded = DummyOperator(task_id="files_uploaded", dag=dag)

end = DummyOperator(task_id="end", dag=dag)

# Set dependencies
begin >> upload_to_gcs_tasks >> files_uploaded


files_uploaded