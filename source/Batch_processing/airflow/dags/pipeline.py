import os
import logging
import pyarrow.csv as pv
import pyarrow.parquet as pq
from google.cloud import storage
from kaggle.api.kaggle_api_extended import KaggleApi

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.operators.bash import BashOperator

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET = os.getenv("GCP_GCS_BUCKET")
os.environ["GOOGLE_CLOUD_PROJECT"] = PROJECT_ID
os.environ['KAGGLE_USERNAME'] = os.getenv('KAGGLE_USERNAME')
os.environ['KAGGLE_KEY'] = os.getenv('KAGGLE_KEY')

dataset_file = "bank-additional-full.csv"
# dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}"
path_to_local_home = os.getenv("AIRFLOW_HOME")
parquet_file = dataset_file.replace('.csv', '.parquet')
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'staging')
TABLE = 'raw_data'

# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

    # for ifile in glob.glob(local_glob):
    #     filename = os.path.basename(ifile)
    #     blob = bucket.blob(f"{object_dir}/{filename}")
    #     blob.upload_from_filename(ifile)


def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error(
            "Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(
        src_file,  parse_options=pv.ParseOptions(delimiter=";"))
    table = table.rename_columns([x.replace('.', '_')
                                 for x in table.column_names])
    pq.write_table(table, src_file.replace('.csv', '.parquet'))


def download_file(path):
    api = KaggleApi()
    api.authenticate()
    # api.dataset_list_files('sahistapatel96/bankadditionalfullcsv').files
    api.dataset_download_files(
        'sahistapatel96/bankadditionalfullcsv', path=path, unzip=True)


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="kaggle_gcs_bq",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    download_file_task = PythonOperator(
        task_id="download_file_task",
        python_callable=download_file,
        op_kwargs={
            "path": f"{path_to_local_home}/data_raw/",
        },
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/data_raw/{dataset_file}",
        },
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"data_lake/{parquet_file}",
            "local_file": f"{path_to_local_home}/data_raw/{parquet_file}",
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/data_lake/{parquet_file}"],
            },
        },
    )

    CREATE_BQ_TBL_QUERY = (
        f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{TABLE} \
            AS \
            SELECT * FROM {BIGQUERY_DATASET}.external_table;"
    )
    bq_create_partitioned_table_job = BigQueryInsertJobOperator(
        task_id="bq_create_table_task",
        configuration={
            "query": {
                "query": CREATE_BQ_TBL_QUERY,
                "useLegacySql": False,
            }
        }
    )

    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"cd {path_to_local_home}/dbt && dbt debug --profiles-dir ."
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {path_to_local_home}/dbt && dbt run --profiles-dir ."
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {path_to_local_home}/dbt && dbt test --profiles-dir ."
    )

    download_file_task >> format_to_parquet_task >> local_to_gcs_task >> bigquery_external_table_task >> bq_create_partitioned_table_job >> dbt_debug >> dbt_run >> dbt_test
