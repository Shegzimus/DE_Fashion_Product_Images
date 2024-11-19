import os

# ENVIRONMENT VARS
PROJECT_ID = os.environ.get("GCP_PROJECT_ID") # Retrieve the project ID from the environment variable
BUCKET = os.environ.get("GCP_GCS_BUCKET")  # Retrieve the bucket name from the environment variable


# KAGGLE
kaggle_dataset_user = "paramaggarwal"
kaggle_dataset_name = "fashion-product-images-dataset"
kaggle_dataset_download_ref = f"{kaggle_dataset_user}/{kaggle_dataset_name}"

# LOCAL PATHS
path_to_local_home = os.environ.get("AIRFLOW_HOME", "opt/airflow")
original_image_folder = 'opt/airflow/data/input/fashion-dataset/fashion-dataset/images'
csv_directory = 'opt/airflow/data/input/fashion-dataset'
styles_csv_directory = 'opt/airflow/data/input/styles.csv'
output_directory = 'opt/airflow/data/output'
greyscale_output_folder = 'opt/airflow/data/output/greyscale'

# GCS PATHS
styles_parquet_path = 'gs://de_data_lake_fashionimages-441305//raw/output/parquet/styles.parquet'
images_parquet_path = 'gs://de_data_lake_fashionimages-441305//raw/output/parquet/images.parquet'
greyscale_images_path = 'de_data_lake_fashionimages-441305//raw/output/greyscale'
original_image_path = 'de_data_lake_fashionimages-441305//raw/input/fashion-dataset/images'
