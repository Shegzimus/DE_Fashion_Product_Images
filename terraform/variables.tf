locals {
  data_lake_bucket = "de_data_lake"
}

variable "project" {
  description = "Your GCP Project ID"
  default = "fashionimages-441305"
  type = string
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "europe-west3"
  type = string
}

variable "credentials" {
  description = "Path to the credential json file"
  default = "fashionimages-441305-8e0ac87a473e.json"
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}



variable "bq_dataset_staging" {
  description = "BigQuery Dataset that serves as staging layer where data tables are injested into"
  type = string
  default = "de_dataset_staging"
}

variable "bq_dataset_warehouse" {
  description = "BigQuery Dataset that serves as final layer where aggregates are injested into"
  type = string
  default = "de_dataset_warehouse"
}

 # Output section to expose variable names to DAGs via the Airflow environment
output "BQ_DATASET_STAGING" {
  value = var.bq_dataset_staging
}

output "BQ_DATASET_WAREHOUSE" {
  value = var.bq_dataset_warehouse
}
