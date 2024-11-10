
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
    path_to_local_home)

from pipelines import extract, transform, load

default_args = {
    'owner': 'Oluwasegun',
    'start_date': datetime(2024, 11, 2, 0, 0)
}

dag = DAG(
    dag_id='etl_reddit_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['kaggle', 'etl', 'fashion_images']
)

begin = DummyOperator(task_id="begin", dag=dag)
end = DummyOperator(task_id="end", dag=dag)