import sys
import os
from google.cloud import storage, bigquery
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))



def upload_folder_to_gcs(bucket: str, local_folder: str, target_folder_prefix="") -> None:
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

    # Create a client object and get the specified bucket
    client = storage.Client()
    bucket = client.bucket(bucket)

    # Prepare a list to store the GCS paths of uploaded files
    gcs_paths = []

    # Walk through the local folder
    for root, _, files in os.walk(local_folder):
        for file in files:
            # Full local path
            local_file_path = os.path.join(root, file)
            # Generate GCS target path by preserving folder structure
            relative_path = os.path.relpath(local_file_path, local_folder)
            target_file_path = os.path.join(target_folder_prefix, relative_path).replace( "/")

            # Upload file to GCS
            blob = bucket.blob(target_file_path)
            blob.upload_from_filename(local_file_path)

            # Log or add to list the GCS path of uploaded file
            gcs_paths.append(f"gs://{bucket}/{target_file_path}")
            print(f"Uploaded {local_file_path} to gs://{bucket}/{target_file_path}")

    return gcs_paths


def extract_schema_from_parquet(file_path: str) -> list[bigquery.SchemaField]:
    """
    Extracts the schema from a Parquet file and converts it to a format compatible with Google BigQuery.

    This function reads the Parquet file schema using the pyarrow library, then converts the schema
    from PyArrow format to Google BigQuery format. It maps PyArrow types to BigQuery types, and defaults
    to STRING for any unrecognized types.

    Parameters:
    file_path (str): The path to the Parquet file.

    Returns:
    List[bigquery.SchemaField]: A list of BigQuery SchemaField objects representing the table schema.
    """
    # Read the Parquet file schema using pyarrow
    parquet_file = pq.ParquetFile(file_path)
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



    


