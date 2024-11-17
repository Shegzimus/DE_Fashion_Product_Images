import sys
import os
from google.cloud import storage, bigquery
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
import pyarrow.parquet as pq
import gcsfs

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def configure_gcs_upload_settings():
    """
    Configures the settings for uploading files to Google Cloud Storage (GCS).

    This function adjusts the maximum multipart size and default chunk size for GCS uploads.
    By increasing these settings, the function allows for efficient uploading of large files.

    Parameters:
    None

    Returns:
    None
    """
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

def initialize_gcs_bucket(bucket_name: str) -> storage.bucket.Bucket:
    """
    Initializes a Google Cloud Storage (GCS) bucket using the provided bucket name.

    This function creates a client object using the Google Cloud Storage library and then
    retrieves the specified bucket from the client. If the bucket does not exist, this function
    will raise a NotFound exception.

    Parameters:
    bucket_name (str): The name of the GCS bucket to initialize.

    Returns:
    storage.bucket.Bucket: The initialized GCS bucket object.
    """
    client = storage.Client()
    return client.bucket(bucket_name)


def generate_gcs_target_path(local_file_path: str, local_folder: str, target_folder_prefix: str) -> str:
    """
    Generates a target path in Google Cloud Storage (GCS) for a local file, preserving the folder structure.

    This function takes the local file path, the local folder path, and a target folder prefix as input.
    It calculates the relative path of the local file from the local folder, then joins it with the target folder prefix.
    The resulting path is formatted to replace backslashes with forward slashes, ensuring compatibility with GCS.

    Parameters:
    local_file_path (str): The full path of the local file.
    local_folder (str): The path of the local folder containing the file.
    target_folder_prefix (str): The prefix path in GCS where the file will be stored.

    Returns:
    str: The target path in GCS, preserving the folder structure.
    """
    relative_path = os.path.relpath(local_file_path, local_folder)
    return os.path.join(target_folder_prefix, relative_path).replace("\\", "/")


def upload_file_to_gcs(bucket, local_file_path: str, target_file_path: str):
    """
    Uploads a local file to a specified Google Cloud Storage (GCS) bucket.

    This function takes a Google Cloud Storage (GCS) bucket object, a local file path, and a target file path.
    It creates a blob object in the specified bucket using the target file path, then uploads the local file
    to the GCS bucket using the blob's `upload_from_filename` method.

    Parameters:
    bucket (storage.bucket.Bucket): The Google Cloud Storage (GCS) bucket object to upload the file to.
    local_file_path (str): The full path of the local file to be uploaded.
    target_file_path (str): The path in the GCS bucket where the file will be stored.

    Returns:
    None: The function does not return any value. It uploads the file to the GCS bucket.
    """
    blob = bucket.blob(target_file_path)
    blob.upload_from_filename(local_file_path)


def is_parquet_file(filename: str) -> bool:
    """
    Checks if a given filename has a .parquet extension.

    Parameters:
    filename (str): The name of the file to check.

    Returns:
    bool: True if the filename ends with ".parquet", False otherwise.
    """
    return filename.endswith(".parquet")


def upload_folder_to_gcs(bucket_name: str, local_folder: str, target_folder_prefix="") -> list:
    """
    Uploads all files in a local folder to a specified Google Cloud Storage (GCS) bucket, preserving
    the folder structure in GCS. Returns the list of GCS paths for any .parquet files uploaded.

    Parameters:
    bucket_name (str): The name of the GCS bucket to upload to.
    local_folder (str): The path of the local folder to upload.
    target_folder_prefix (str, optional): The prefix path in GCS where the files will be stored.
                                         Defaults to an empty string.

    Returns:
    list: List of GCS paths where .parquet files were uploaded.
    """
    configure_gcs_upload_settings()
    bucket = initialize_gcs_bucket(bucket_name)
    parquet_gcs_paths = []

    for root, _, files in os.walk(local_folder):
        for file in files:
            local_file_path = os.path.join(root, file)
            target_file_path = generate_gcs_target_path(local_file_path, local_folder, target_folder_prefix)
            upload_file_to_gcs(bucket, local_file_path, target_file_path)

            if is_parquet_file(file):
                parquet_gcs_paths.append(f"gs://{bucket_name}/{target_file_path}")
                print(f"Uploaded {local_file_path} to gs://{bucket_name}/{target_file_path}")

    return parquet_gcs_paths


def extract_schema_from_parquet(file_path: str) -> list[bigquery.SchemaField]: 
    # Create a GCS filesystem object
    fs = gcsfs.GCSFileSystem()

    # Open the file using GCSFS
    with fs.open(file_path, 'rb') as f:
        parquet_file = pq.ParquetFile(f)
        schema = parquet_file.schema_arrow

    # Convert PyArrow schema to BigQuery schema fields
    bq_schema = []
    for field in schema:
        field_type = field.type
        # Map PyArrow types to BigQuery types
        if field_type == "int64":
            bq_type = "INTEGER"
        elif field_type == "float64":
            bq_type = "FLOAT"
        elif field_type == "string":
            bq_type = "STRING"
        elif field_type == "bool":
            bq_type = "BOOLEAN"
        elif field_type == "timestamp":
            bq_type = "TIMESTAMP"
        else:
            bq_type = "STRING"  # Default to STRING for any unrecognized type

        bq_schema.append(bigquery.SchemaField(name=field.name, field_type=bq_type))
    return bq_schema

def create_bigquery_table_callable(dataset_id: str, table_id: str, parquet_file_path: str) -> None:
    """
    Creates a BigQuery table with a schema extracted from a Parquet file.

    Parameters:
    dataset_id (str): BigQuery dataset ID.
    table_id (str): BigQuery table ID.
    parquet_file_path (str): Path to the Parquet file.

    Returns:
    BigQueryCreateEmptyTableOperator: An Airflow operator to create the BigQuery table.
    """
    # Extract schema from Parquet file
    schema_fields = extract_schema_from_parquet(parquet_file_path)

    return BigQueryCreateEmptyTableOperator(
        task_id="create_bigquery_table",
        dataset_id=dataset_id,
        table_id=table_id,
        schema_fields=schema_fields,
    )


def process_gcs_paths(**kwargs):
    """
    This function processes the output of multiple GCS upload tasks and collects all the GCS paths.
    It pulls the XCom values from the specified tasks using the provided kwargs, and then prints
    all the collected GCS paths.

    Parameters:
    kwargs (dict): Keyword arguments passed to the function. It should contain a 'ti' key, which is an instance of
                    the TaskInstance class from the Airflow library. The 'ti' instance is used to pull XCom values.

    Returns:
    None: The function does not return any value. It only prints the collected GCS paths.
    """
    gcs_paths = []
    for task in upload_to_gcs_tasks:
        task_gcs_paths = kwargs['ti'].xcom_pull(task_ids=task.task_id)
        if task_gcs_paths:  # Check if there's any output to avoid adding None
            gcs_paths.extend(task_gcs_paths)
    print("All GCS paths:", gcs_paths)



#######################################
def initialize_bq_client(): 
    """
    This function initializes a BigQuery client.
    """
    client = bigquery.Client()
    return client

def read_file_from_gcs(file_path:str)-> None:
    """
    This function reads a file from a Google Cloud Storage (GCS) bucket and returns its content as bytes.

    Parameters:
    file_path (str): The path to the file in the GCS bucket, including the bucket name and file path.

    Returns:
    bytes: The content of the file as bytes.
    """
    fs = gcsfs.GCSFileSystem()
    with fs.open(file_path, "rb") as source_file:
        return source_file.read()


def create_table(dataset_id, file_path, client):
    """
    This function creates a table in a specified BigQuery dataset using a Parquet file from a Google Cloud Storage (GCS) bucket.

    Parameters:
    dataset_id (str): The ID of the BigQuery dataset to create the table in.
    file_path (str): The path to the Parquet file in the GCS bucket, including the bucket name and file path.
    client (bigquery.Client): The BigQuery client to use for creating the table.

    Returns:
    tuple: A tuple containing the ID of the created table and its reference. If an error occurs, returns (None, None).

    Raises:
    Exception: If an error occurs while creating the table.
    """
    try:
        # Extract schema from Parquet file
        schema_fields = extract_schema_from_parquet(file_path)

        # Derive table ID from file path
        table_id = file_path.split("/")[-1].split(".")[0]

        # Reference dataset and table
        dataset_ref = client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)

        # Create table with schema
        table = bigquery.Table(table_ref)
        table.schema = schema_fields
        table = client.create_table(table)
        print(f"Created table {table_id} in dataset {dataset_id}")


        # Extract table ID from table reference
        return table_id, table_ref

    except Exception as e:
        print(f"Error creating table {table_id}: {e}")
        return None, None
 

def bucket_to_bq(dataset_id: str, file_path: str)-> None:
    """
    This function uploads a Parquet file from a Google Cloud Storage (GCS) bucket to a specified BigQuery dataset.

    Parameters:
    dataset_id (str): The ID of the BigQuery dataset to upload the file to.
    file_path (str): The path to the Parquet file in the GCS bucket, including the bucket name and file path.

    Returns:
    None
    """
    client = bigquery.Client()

    file_data = read_file_from_gcs(file_path)

    # Create table using extracted pq schema
    table_id, table_ref = create_table(dataset_id, file_path, client)

    # Upload Parquet file to BigQuery
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.PARQUET
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED

    # with open(file_path, "rb") as source_file:
    #     job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
    # job.result()

    print(f"Uploaded {file_path} to BigQuery table {table_id}")


