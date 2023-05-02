terraform {
    required_version = ">= 1.0"
    backend "local" {}
    required_providers {
        google = {
            source  = "hashicorp/google"
        }
    }
}

provider "google" {
    project = var.project
    region = var.region
}

# Data lake
resource "google_storage_bucket" "bucket" {
    name          = var.bucket
    location      = var.region
    force_destroy = true

    uniform_bucket_level_access = true

    lifecycle_rule {
        action {
            type = "Delete"
        }
        condition {
            age = 30  # days
        }
    }
}

# DWH - staging
resource "google_bigquery_dataset" "stg_dataset" {
    dataset_id                 = var.stg_bq_dataset
    project                    = var.project
    location                   = var.region
    delete_contents_on_destroy = true
}

# DWH - prod
resource "google_bigquery_dataset" "prod_dataset" {
    dataset_id                 = var.prod_bq_dataset
    project                    = var.project
    location                   = var.region
    delete_contents_on_destroy = true
}
