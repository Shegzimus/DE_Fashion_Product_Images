from PIL import Image, ExifTags
import pandas as pd
import os
import sys


sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.constants import (
    kaggle_dataset_download_ref, kaggle_dataset_name, kaggle_dataset_user, 
    path_to_local_home, original_image_folder)




def download_and_unzip_kaggle_dataset(kaggle_dataset_download_ref: str, kaggle_dataset_name: str, path_to_local_home: str) -> None:
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
        # Optionally remove the zip file after extraction
        if os.path.exists(zip_file_path):
            os.remove(zip_file_path)


def convert_to_greyscale(image_folder: str , output_folder: str) -> Image:
    """
    Convert all images in a folder to greyscale.
    
    :param image_folder: Path to the folder containing images.
    :param output_folder: Path to the folder to save greyscale images.
    """
    # Ensure the output folder exists, create if not
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    # Iterate over files in the folder
    for filename in os.listdir(image_folder):
        file_path = os.path.join(image_folder, filename)
        
        # Check if the file is an image (you can modify this check as needed)
        if filename.lower().endswith(('.jpg')):
            try:
                # Open the image
                with Image.open(file_path) as img:
                    # Convert the image to greyscale
                    greyscale_img = img.convert('L')
                    
                    # Save the greyscale image to the output folder
                    output_path = os.path.join(output_folder, filename)
                    greyscale_img.save(output_path)
                    print(f"Converted {filename} to greyscale.")
            except Exception as e:
                print(f"Error processing {filename}: {e}")



# Function to extract metadata from a single image
def extract_and_save_metadata_to_csv(image_directory: list) -> None:
    """
    Extracts metadata from images in a given directory and saves it to a CSV file.

    Parameters:
    image_directory (list): A list containing the path(s) to the directory(ies) containing images.

    Returns:
    None

    The function iterates through each image in the specified directory(ies), extracts metadata using the EXIF data,
    maps the EXIF tags to readable names, and appends the metadata to a list. The list is then converted to a pandas DataFrame,
    and the DataFrame is saved to a CSV file located at 'opt/airflow/data/output/metadata/image_metadata.csv'.
    If the output directory does not exist, it is created.
    """
    metadata_list = []

    # Loop through each image in the directory
    for filename in os.listdir(image_directory):
        if filename.lower().endswith(('.jpg', '.jpeg', '.png')):
            image_path = os.path.join(image_directory, filename)

            try:
                # Open image and extract EXIF data
                image = Image.open(image_path)
                exif_data = image._getexif()

                # Map EXIF data to readable tags
                metadata = {'filename': filename}
                if exif_data:
                    for tag, value in exif_data.items():
                        tag_name = ExifTags.TAGS.get(tag, tag)
                        metadata[tag_name] = value

                # Append metadata dictionary to list
                metadata_list.append(metadata)

            except Exception as e:
                print(f"Error processing {filename}: {e}")

    # Convert list of metadata dictionaries to a DataFrame
    df_metadata = pd.DataFrame(metadata_list)

    # Save DataFrame to CSV
    output_csv_path = 'opt/airflow/data/output/metadata/image_metadata.csv'
    os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)
    df_metadata.to_csv(output_csv_path, index=False)
    print(f"Metadata saved to {output_csv_path}")

