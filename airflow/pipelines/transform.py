import subprocess
import zipfile
import os
import sys
from PIL import Image
from PIL import Image, ExifTags
import pandas as pd
from tqdm import tqdm
import csv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.constants import (
    csv_directory
)

def csv_to_parquet(csv_file_path: str, parquet_file_path: str) -> None:
    """
    Convert a CSV file to Parquet format.
    
    :param csv_file_path: Path to the input CSV file.
    :param parquet_file_path: Path to save the output Parquet file.
    """
    # Read the CSV file into a Pandas DataFrame
    df = pd.read_csv(csv_file_path)
    
    # Write the DataFrame to a Parquet file
    df.to_parquet(parquet_file_path, engine='pyarrow')
    print(f"File converted to {parquet_file_path}")


def parse_and_trim_csv_file(local_directory: str, file_name: str) -> pd.DataFrame:
    """
    Read a specified CSV file in the directory, trim rows with extra columns, and return a DataFrame.
    
    :param local_directory: Path to the directory containing the CSV file.
    :param file_name: Name of the CSV file to read.
    :return: DataFrame containing the CSV data with trimmed rows if needed.
    """
    # Construct the full path to the CSV file
    csv_file_path = os.path.join(local_directory, file_name)
    
    # Check if the file exists
    if not os.path.exists(csv_file_path):
        print(f"File not found: {csv_file_path}")
        return None
    
    # Read the header to get the expected number of columns
    with open(csv_file_path, 'r') as file:
        reader = csv.reader(file)
        header = next(reader)  # Read the header row
        expected_columns = len(header)

    # Process each row and trim to the expected number of columns
    trimmed_rows = [header]  # Start with the header as the first row
    with open(csv_file_path, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header row since it's already added
        for row in reader:
            # Trim the row to the expected number of columns
            trimmed_row = row[:expected_columns]
            trimmed_rows.append(trimmed_row)
    
    # Create a DataFrame
    df = pd.DataFrame(trimmed_rows[1:], columns=trimmed_rows[0])  # [1:] skips the header row in data
    
    # Display DataFrame info
    # print(df.info())
    
    return df


def parse_and_convert_styles_fields(local_directory: str, file_name: str) -> pd.DataFrame:
    """
    Read a specified CSV file in the directory, trim rows with extra columns, convert specified columns to integer and string types, and return a DataFrame.

    Parameters:
    local_directory (str): Path to the directory containing the CSV file.
    file_name (str): Name of the CSV file to read.

    Returns:
    pd.DataFrame: DataFrame containing the CSV data with trimmed rows and converted columns.
    """
    df = parse_and_trim_csv_file(local_directory, file_name)

    # Define columns to convert
    int_columns = ['id', 'year']  # Columns that should be integers
    str_columns = ['gender', 'masterCategory', 'subCategory', 'articleType', 
                   'baseColour', 'season', 'usage', 'productDisplayName']  # Columns that should be strings

    # Convert specified columns to integer
    for col in int_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')  # Use 'Int64' to allow NaN if any values fail conversion

    # Convert specified columns to string
    for col in str_columns:
        if col in df.columns:
            df[col] = df[col].astype(pd.StringDtype())  # Use 'StringDtype' to allow NaN if any values fail conversion

    # Display DataFrame info to confirm conversions
    print(df.info())

    return df


def parse_and_convert_images_fields(local_directory: str, file_name: str) -> pd.DataFrame:
    """
    This function reads a specified CSV file containing image metadata, trims rows with extra columns, 
    extracts 'id' and 'format' from 'filename', converts 'id' to integer and 'format' and 'link' to string, 
    and returns a DataFrame.

    Parameters:
    local_directory (str): The path to the directory containing the CSV file.
    file_name (str): The name of the CSV file to read.

    Returns:
    pd.DataFrame: A DataFrame containing the image metadata with trimmed rows and converted columns.
    """
    df = parse_and_trim_csv_file(local_directory, file_name)

    # Extract 'id' and 'format' from 'filename'
    df[['id', 'format']] = df['filename'].str.extract(r'(\d+)\.(\w+)', expand=True)

    # Convert 'id' to integer
    df['id'] = pd.to_numeric(df['id'], errors='coerce').astype('Int64')  # Use 'Int64' to allow NaN if any values fail conversion

    # Ensure 'format' and 'link' are strings
    df['format'] = df['format'].astype(pd.StringDtype())
    df['link'] = df['link'].astype(pd.StringDtype())

    # Display DataFrame info to confirm conversions
    print(df.info())

    return df


def save_styles_to_parquet(output_directory: str, file_name: str) -> None:
    """
    Save a DataFrame containing fashion styles data to a Parquet file.

    Parameters:
    output_directory (str): The directory where the Parquet file will be saved.
    file_name (str): The name of the Parquet file (without extension).

    The function performs the following steps:
    1. Ensures the output directory exists.
    2. Constructs the full path to the output Parquet file.
    3. Parses and converts the 'styles.csv' file to a DataFrame using the 'parse_and_convert_styles_fields' function.
    4. Saves the DataFrame to the Parquet file.
    5. Prints a success message indicating the location of the saved Parquet file.

    Returns:
    None
    """
    # Ensure the output directory exists
    os.makedirs(output_directory, exist_ok=True)

    # Construct the full path to the output Parquet file
    parquet_file_path = os.path.join(output_directory, f"{file_name}.parquet")

    # Parse and convert the styles CSV file to a DataFrame
    df = parse_and_convert_styles_fields(local_directory= 'airflow/data/input/fashion-dataset', file_name= 'styles.csv')
    # Save the DataFrame to Parquet format
    df.to_parquet(parquet_file_path, index=False)

    print(f"DataFrame saved to {parquet_file_path}")


def save_images_to_parquet(output_directory: str, file_name: str) -> None:
    """
    Save a DataFrame containing image metadata to a Parquet file.

    Parameters:
    output_directory (str): The directory where the Parquet file will be saved.
    file_name (str): The name of the Parquet file (without extension).

    Returns:
    None
    """
    # Ensure the output directory exists
    os.makedirs(output_directory, exist_ok=True)

    # Construct the full path to the output Parquet file
    parquet_file_path = os.path.join(output_directory, f"{file_name}.parquet")

    # Parse and convert the images CSV file to a DataFrame
    df = parse_and_convert_images_fields(local_directory= 'airflow/data/input/fashion-dataset', file_name= 'images.csv')
    # Save the DataFrame to Parquet format
    df.to_parquet(parquet_file_path, index=False)

    print(f"DataFrame saved to {parquet_file_path}")

