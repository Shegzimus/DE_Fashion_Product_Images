import subprocess
import zipfile
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from utils.constants import (
    kaggle_dataset_download_ref, kaggle_dataset_name, kaggle_dataset_user, 
    path_to_local_home)

def download_and_unzip_kaggle_dataset(kaggle_dataset_download_ref, kaggle_dataset_name, path_to_local_home):
    """
    Downloads and unzips a Kaggle dataset specified by the given reference and name.

    Parameters:
    kaggle_dataset_download_ref (str): The reference to the Kaggle dataset.
    kaggle_dataset_name (str): The name of the Kaggle dataset.
    path_to_local_home (str): The local path where the dataset will be downloaded and unzipped.

    Returns:
    None
    """
    # Define the paths
    download_path = os.path.join(path_to_local_home, 'data\input')
    zip_file_path = os.path.join(download_path, f"{kaggle_dataset_name}.zip")

    # Download the Kaggle dataset
    download_command = [
        "kaggle", "datasets", "download", kaggle_dataset_download_ref,
        "-p", download_path
    ]

    try:
        subprocess.run(download_command, check=True)
        print("Dataset downloaded successfully.")
    except subprocess.CalledProcessError as e:
        print("Error downloading dataset:", e)
        return

    # Unzip the dataset
    try:
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(download_path)
            print(f"Dataset unzipped successfully to {download_path}.")
    except zipfile.BadZipFile as e:
        print("Error unzipping file:", e)
    finally:
        # Remove the zip file after extraction
        if os.path.exists(zip_file_path):
            os.remove(zip_file_path)


 
# download_and_unzip_kaggle_dataset(kaggle_dataset_download_ref, kaggle_dataset_name, path_to_local_home)
