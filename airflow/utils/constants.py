import os

# Environment variables
PROJECT_ID = os.environ.get("GCP_PROJECT_ID") # Retrieve the project ID from the environment variable
BUCKET = os.environ.get("GCP_GCS_BUCKET")  # Retrieve the bucket name from the environment variable



kaggle_dataset_user = "paramaggarwal"
kaggle_dataset_name = "fashion-product-images-dataset"
kaggle_dataset_download_ref = f"{kaggle_dataset_user}/{kaggle_dataset_name}"

path_to_local_home = os.environ.get("AIRFLOW_HOME", "opt/airflow")

original_image_folder = 'opt/airflow/data/input/fashion-dataset/fashion-dataset/images'

csv_directory = 'opt/airflow/data/input/fashion-dataset'

styles_csv_directory = 'opt/airflow/data/input/styles.csv'

output_directory = 'opt/airflow/data/output'

greyscale_output_folder = 'opt/airflow/data/output/greyscale'