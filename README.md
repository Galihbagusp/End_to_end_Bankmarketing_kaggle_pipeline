# End_to_end_Bankmarketing_kaggle_pipeline

## Introduction

This is lambda data processing (batch and stream) docker-containerized ELT data pipeline, the goals is to get data from Kaggle and send it daily to BigQuery data warehouse

## Dataset

The data comes from Kaggle dataset (https://www.kaggle.com/datasets/sahistapatel96/bankadditionalfullcsv). It is related to direct marketing campaign of a Portuguese banking institution. The marketing campaigns were based on phone calls. Often, more than one contact to the same client was required, in order to access if the product (bank term deposit) would be ('yes') or not ('no') subscribed.

## Tools & Technologies

- Cloud - [**Google Cloud Platform**](https://cloud.google.com)
- Infrastructure as Code - [**Terraform**](https://www.terraform.io)
- Containerization - [**Docker**](https://www.docker.com), [**Docker Compose**](https://docs.docker.com/compose/)
- Batch Processing and Orchestration - [**Airflow**](https://airflow.apache.org)
- Stream Processing - [**Apache Kafka**](https://kafka.apache.org)
- Transformation - [**dbt**](https://www.getdbt.com)
- Data Lake - [**Google Cloud Storage**](https://cloud.google.com/storage)
- Data Warehouse - [**BigQuery**](https://cloud.google.com/bigquery)
- Language - [**Python**](https://www.python.org)

## Architecture

![FLOW](./document/Data%20Architecture.png)

## Setup

**WARNING**: You will be charged for all the infrastructure setup. You can avail 300$ in credit by creating a new account on Google Cloud Platform (GCP).

### Pre-requisites

- Google Cloud Platform Account
    - You have a GCP project with the project ID `<your-gcp-project-id>`
    - You have environment variable `GOOGLE_APPLICATION_CREDENTIALS` set, which points to your Google Service Account JSON file. The service account should have **Storage Admin**, **Storage Object Admin**, and **BigQuery Admin** permissions.
    - `gcloud` sdk installed. To authenticate, run
        ```
        gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS

        ```
- Terraform
- Docker, Docker Compose

#### Initiate terraform and download the required dependencies
```
terraform init

```

#### View the Terraform plan

You will be asked to enter two values. For the GCP Project ID, enter `<your-gcp-project-id>`. For the GCS bucket, enter any name that is unqiue across GCS. A good idea is to concatenate desired name with your GCS Project ID to make it unique. We will refer to the entered bucket name as `<your-gcs-bucket-name>`.

```
terraform plan

```

Terraform plan should show the plan for creating following services:

1. `google_storage_bucket.bucket` - This is the data lake bucket, used to store raw parquet files in monthly batches
2. `google_bigquery_dataset.stg_dataset` - This is the `staging` dataset for raw data
3. `google_bigquery_dataset.prod_dataset` - This is the `prod` dataset for BI reporting

#### Apply the infrastructure
```
terraform apply

```

You should now see the above bucket and datasets in your GCS project.

Once done, set the environment variables `GCP_PROJECT_ID`, `GCP_GCS_BUCKET`, `GOOGLE_APPLICATION_CREDENTIALS`,  `AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT` respectively, and get `KAGGLE_USERNAME`, and `KAGGLE_KEY` from your Kaggle account.

### Get Going with Airflow

### Set environment inside airflow
```
echo -e "AIRFLOW_UID=$(id -u)" > .env

```

### Run the container for batch and streaming
```
docker compose up
```
#### Stop and remove containers
```
docker compose down

```

#### Destroy Infrastructure
```
go to terraform directory and run `terraform destroy`

```

## Future work

- Add Visualization
- Include CI/CD