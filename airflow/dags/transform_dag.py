from airflow import DAG
from datetime import datetime
import os
import sys
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator


# Add the parent directory to the system path for importing the constants file
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))





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