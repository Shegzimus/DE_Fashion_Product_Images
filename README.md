# ETLt Data Pipeline with Kaggle, Airflow, Celery, Postgres, GCS, BigQuery, Terraform, Redis and Docker


## Table of Contents
- [ETLt Data Pipeline with Kaggle, Airflow, Celery, Postgres, GCS, BigQuery, Terraform, Redis and Docker](#etlt-data-pipeline-with-kaggle-airflow-celery-postgres-gcs-bigquery-terraform-redis-and-docker)
  - [Table of Contents](#table-of-contents)
  - [Motivation](#motivation)
  - [Overview](#overview)
  - [Architecture](#architecture)
  - [Prerequisites](#prerequisites)
  - [System Configuration](#system-configuration)

## Motivation
I just wanted to build a pipeline that handles large amounts of images and their metadata seemlessly. I imagined supporting a team of machine learning engineers who would need a pipeline with standardized images to feed a developing model. The focus of this project was to sharpen my image parsing skills with python and writing refactorable code for similar purposes in the future.

## Overview
This pipeline is designed to:
1. Extract data from Kaggle using its API
2. Clean and transform the raw data by generating metadata, standardizing images, creating greyscale copies and changing column types
3. Upload the data to GCS using folder oriented and flexible functions.
4. Load the transformed data into BigQuery
5. Perform aggregations on the image data and it's metadata.


## Architecture


1. **Kaggle API**: Source of the data.
2. **Apache Airflow & Celery**: Orchestrates the ETL process and manages task distribution.
3. **PostgreSQL**: Temporary storage and metadata management.
4. **Google Cloud Storage(GCS)**: Raw data storage.
5. **Google BigQuery(BQ)**: Data warehousing, analytics and SQL-based data transformation.
   
## Prerequisites
1. Google console account with appropriate permissions for GCS and BigQuery
2. Kaggle API Credentials
3. Terraform installation
4. Docker Installation
5. Python 3.9 or higher

## System Configuration
1. Clone the repository
    ```bash
    git clone https://github.com/Shegzimus/DE_Fashion_Product_Images.git
    ```

2. Create a virtual environment in your local machine
    ```bash
    python3 -m venv venv
    ```


3. Activate the virtual environment
    ```bash
    source venv/bin/activate
    ```

4. Install dependencies
   ```bash
   pip install -r airflow/requirements.txt
   ```

5. Create directories to store your google and kaggle credentials
   ```bash
   cd airflow && mkdir -p .google .kaggle

   ```

6. Generate and place your google service account credentials in the .google file the terraform file

7. Generate and place your kaggle credentials in the .kaggle file

8. Adjust the DockerFile and docker-compose configuration to reflect these changes

9.  Build the Docker Image
    ```bash
    docker build -d --
    ```

10. Start the Docker containers
    ```bash
    docker-compose up -d
    ```

11. Launch the Airflow web UI
    ```bash
    open http://localhost:8080
    ```