# https://medium.com/@sant1/using-minio-with-docker-and-python-cbbad397cb5d
import boto3
import os
from airflow.sdk import DAG
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()
# import kaggle ONLY after loading environment variables
import kaggle as kg

MINIO_BUCKET_NAME = 'berka-raw-data-bucket'
DATASETS_FOLDER = 'datasets'

kg.api.authenticate()

# TODO HERE:
# Check if datasets folder exists
# Check if data files already exist
# If not, download

#kaggle datasets download -d vijayuv/onlineretail # Copied API command, take the dataset name from this command

kg.api.dataset_download_files(dataset = "marceloventura/the-berka-dataset", path=DATASETS_FOLDER, unzip=True)

dag = DAG(
    "berka_elt",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "depends_on_past": True,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function, # or list of functions
        # 'on_success_callback': some_other_function, # or list of functions
        # 'on_retry_callback': another_function, # or list of functions
        # 'sla_miss_callback': yet_another_function, # or list of functions
        # 'on_skipped_callback': another_function, #or list of functions
        'trigger_rule': 'all_success'
    },
    description="A dag which extracts, loads and transforms data from Berka financial dataset with DBT and Clickhouse",
    schedule=timedelta(days=1),
    start_date=datetime(2026, 7, 15),
    catchup=False,
    tags=["personal-project", "berka"],)

with dag:
    pass