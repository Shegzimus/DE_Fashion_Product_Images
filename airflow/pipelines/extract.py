from PIL import Image, ExifTags
import pandas as pd
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
        # Optionally remove the zip file after extraction
        if os.path.exists(zip_file_path):
            os.remove(zip_file_path)











# Function to extract metadata from a single image
def extract_metadata(image_path):
    image = Image.open(image_path)
    exif_data = image._getexif()  # Extract EXIF data
    
    # Map EXIF data to readable tags
    metadata = {}
    if exif_data:
        for tag, value in exif_data.items():
            tag_name = ExifTags.TAGS.get(tag, tag)
            metadata[tag_name] = value
    metadata['filename'] = os.path.basename(image_path)  # Add filename for reference
    return metadata

# Directory containing images
image_directory = 'pipelines'
image_files = [os.path.join(image_directory, f) for f in os.listdir(image_directory) if f.lower().endswith('.jpg')]

# Extract metadata for each image and store in a list
metadata_list = [extract_metadata(image_file) for image_file in image_files]

# Convert the list of metadata dictionaries into a DataFrame
df_metadata = pd.DataFrame(metadata_list)

# Save the DataFrame to a CSV file
output_csv_path = 'image_metadata.csv'
df_metadata.to_csv(output_csv_path, index=False)

print(f"Metadata saved to {output_csv_path}")
