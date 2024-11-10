import subprocess
import zipfile
import os
import sys
from PIL import Image
from PIL import Image, ExifTags
import pandas as pd
from tqdm import tqdm


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


def convert_to_greyscale(image_folder, output_folder):
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
        if filename.lower().endswith(('.jpg', '.jpeg', '.png')):
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
def extract_and_save_metadata(image_directory: str, output_csv_path: str) -> None:
    """
    Extracts metadata from all images in a directory and saves it to a CSV file.

    Parameters:
    image_directory (str): The directory containing the image files.
    output_csv_path (str): The path where the metadata CSV file will be saved.

    Returns:
    None
    """
    metadata_list = []

    # Get list of image files in the directory
    image_files = [
        os.path.join(image_directory, f) for f in os.listdir(image_directory)
        if f.lower().endswith(('.jpg'))
    ]

    # Extract metadata for each image (with a progress bar)
    for image_file in tqdm(image_files, desc="Extracting metadata", unit="image" ):
        image = Image.open(image_file)
        exif_data = image._getexif()  # Extract EXIF data

        # Map EXIF data to readable tags
        metadata = {}
        if exif_data:
            for tag, value in exif_data.items():
                tag_name = ExifTags.TAGS.get(tag, tag)
                metadata[tag_name] = value
        metadata['filename'] = os.path.basename(image_file)  # Add filename for reference
        metadata_list.append(metadata)

    # Convert the list of metadata dictionaries into a DataFrame
    df_metadata = pd.DataFrame(metadata_list)

    # Save the DataFrame to a CSV file
    df_metadata.to_csv(output_csv_path, index=False)
    print(f"Metadata saved to {output_csv_path}")






# Example usage
# image_directory = 'airflow/data/input/fashion-dataset/fashion-dataset/images'
# output_csv_path = 'airflow/data/output/metadata/image_metadata.csv'
# extract_and_save_metadata(image_directory, output_csv_path)






# image_path = 'airflow/data/input/fashion-dataset/fashion-dataset/images'
# output_folder = 'airflow/data/output/greyscale'
# convert_to_greyscale(image_folder, output_folder)

# download_and_unzip_kaggle_dataset(kaggle_dataset_download_ref, kaggle_dataset_name, path_to_local_home)



