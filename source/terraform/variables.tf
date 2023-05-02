variable "project" {
    description = "Your GCP Project ID"
    type        = string
}

variable "bucket" {
    description = "The name of your bucket. This should be unique across GCP"
    type        = string
}

variable "region" {
    description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
    default     = "us-west1"
    type        = string
}

variable "storage_class" {
    description = "Storage class type for your bucket. Check official docs for more info"
    default = "STANDARD"
    type        = string
}

variable "stg_bq_dataset" {
    description = "BigQuery dataset for raw data"
    default     = "staging"
    type        = string
}

variable "prod_bq_dataset" {
    description = "BigQuery dataset for upstream data visualization"
    default     = "prod"
    type        = string
}