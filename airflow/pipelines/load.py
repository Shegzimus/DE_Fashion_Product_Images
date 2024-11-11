import sys
import os
from google.cloud import storage

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))



def upload_file_to_gcs(bucket, local_folder, target_folder_prefix=""):
    """
    This function uploads a local file to a specified Google Cloud Storage (GCS) bucket.

    The function uses the google-cloud-storage library to interact with GCS. It also includes a workaround
    for a known issue where uploading files larger than 6 MB may result in a timeout error.

    Parameters:
    bucket (str): The name of the GCS bucket to upload the file to.
    local_file (str): The path and filename of the local file to be uploaded.
    target_file (str): The path and filename in the GCS bucket where the file will be stored.

    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python

    Returns:
    None
    """

    # Adjust GCS upload settings for large files
    # Ref: https://github.com/googleapis/python-storage/issues/74
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    # Walk through the local folder
    for root, _, files in os.walk(local_folder):
        for file in files:
            # Full local path
            local_file_path = os.path.join(root, file)
            # Generate GCS target path by preserving folder structure
            relative_path = os.path.relpath(local_file_path, local_folder)
            target_file_path = os.path.join(target_folder_prefix, relative_path).replace( "/")

    # Create a client object and get the specified bucket
    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(target_file_path)
    blob.upload_from_filename(local_file_path)


