
from airflow import DAG
from datetime import datetime
import os
import sys
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator


# Add the parent directory to the system path for importing the constants file
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.constants import (
    kaggle_dataset_download_ref, kaggle_dataset_name, kaggle_dataset_user, 
    path_to_local_home, original_image_folder, greyscale_output_folder, PROJECT_ID, BUCKET)

from pipelines.extract import (
  download_and_unzip_kaggle_dataset, 
  convert_to_greyscale, 
  extract_and_save_metadata
  )



default_args = {
    'owner': 'Oluwasegun',
    'start_date': datetime(2024, 11, 2, 0, 0)
}

dag = DAG(
    dag_id='etl_kaggle_fashion_images_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['kaggle', 'etl', 'fashion_images']
)


"""
Task 1: Download the Kaggle dataset

"""

download_task = PythonOperator(
    task_id='download_kaggle_dataset',
    python_callable=extract.download_and_unzip_kaggle_dataset,
    op_kwargs={
        'kaggle_dataset_download_ref': kaggle_dataset_download_ref,
        'kaggle_dataset_name': kaggle_dataset_name,
        'kaggle_dataset_user': kaggle_dataset_user,
        'path_to_local_home': path_to_local_home
    },
    dag=dag
)






begin = DummyOperator(task_id="begin", dag=dag)
end = DummyOperator(task_id="end", dag=dag)