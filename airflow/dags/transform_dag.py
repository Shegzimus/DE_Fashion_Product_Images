from airflow import DAG
from datetime import datetime
import os
import sys
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator


# Add the parent directory to the system path for importing the constants file
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from pipelines.transform import (parse_and_convert_styles_fields, 
                             parse_and_convert_images_fields, 
                             save_styles_to_parquet, 
                             save_images_to_parquet
)

from utils.constants import csv_directory, output_directory



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
Task 1: Convert the styles table fields

"""
convert_styles_task = PythonOperator(
    task_id='convert_styles_fields',
    python_callable=parse_and_convert_styles_fields,
    op_kwargs={
        'local_directory': csv_directory,
        'file_name': 'styles.csv'
    },
    dag=dag
)

"""
Task 2: Convert the images table fields

"""

convert_images_task = PythonOperator(
    task_id='convert_images_fields',
    python_callable=parse_and_convert_images_fields,
    op_kwargs={
        'local_directory': csv_directory,
        'file_name': 'images.csv'
    },
    dag=dag
)

"""
Task 3: Transform fields in the styles table and save to Parquet format

"""

save_styles_task = PythonOperator(
    task_id='save_styles_to_parquet',
    python_callable=save_styles_to_parquet,
    op_kwargs={
        'df_json': "{{ ti.xcom_pull(task_ids='convert_styles_fields') }}",  # Pull convert_styles_task result from XCom
        'output_directory': output_directory,
        'file_name': 'styles'
    },
    dag=dag)


"""
Task 4: Transform fields in the images table and save to Parquet format

"""


save_images_task = PythonOperator(
    task_id='save_images_to_parquet',
    python_callable=save_images_to_parquet,
    op_kwargs={
        'df_json': "{{ ti.xcom_pull(task_ids='convert_images_fields') }}",  # Pull convert_images_task result from XCom
        'output_directory': output_directory,
        'file_name': 'images'
    },
    dag=dag
)


begin = DummyOperator(task_id="begin", dag=dag)
end = DummyOperator(task_id="end", dag=dag)



"""
# Define the dependency between tasks
"""

begin >> [convert_images_task >> convert_styles_task]

convert_images_task >> save_images_task
convert_styles_task >> save_styles_task

[save_images_task >> save_styles_task] >> end