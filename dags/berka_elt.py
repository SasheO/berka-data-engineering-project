# https://medium.com/@sant1/using-minio-with-docker-and-python-cbbad397cb5d
import boto3
import logging
import os
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task_group, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import clickhouse_connect
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()
import kaggle as kg # import kaggle ONLY after loading environment variables

logger = logging.getLogger(__name__)

MINIO_BUCKET_NAME = 'berka-raw-data-bucket'
CLICKHOUSE_CONN_ID = "clickhouse_conn"
MINIO_CONN_ID = "minio_conn"
DAGS_DIR = Path(__file__).resolve().parent
SQL_SCRIPTS_PATH =  "/opt/airflow/include/sql"
SQL_DDL_SCRIPTS_PATH = f'{SQL_SCRIPTS_PATH}/create_tables'
DATASETS_PATH =  "/opt/airflow/datasets"
EMAIL_ON_FAILURE = os.getenv("MY_EMAIL")
SOURCE_NAME_TO_INGESTION_SCRIPT_MAPPING = {
        "account": "src_accounts",
        "card": "src_cards",
        "client": "src_clients",
        "disp": "sr_disposition",
        "district": "src_demographic_districts",
        "loan": "src_loans",
        "order": "src_permanent_orders",
        "trans": "src_transactions",
    }

# TODO: add multiline comments to all functions
def list_all_files_within_path(path: str, with_path_in_name: bool = False):
    if not os.path.exists(path):
        raise FileNotFoundError(f"The directory {path} does not exist.")
    if with_path_in_name:
        return [os.path.join(path, f) for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
    else:
        return [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]

def extract_source_data_from_kaggle():
    kg.api.authenticate()
    kg.api.dataset_download_files(dataset = "marceloventura/the-berka-dataset", path=DATASETS_PATH, unzip=True)
    logger.info(f"Successfully retrieved marceloventura/the-berka-dataset into {DATASETS_PATH}")
    

def ingest_staged_data_into_source_tables():
    
    pass

dag = DAG(
    "berka_elt",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "depends_on_past": True,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        'email': [EMAIL_ON_FAILURE],
        'email_on_failure': True,
        'email_on_retry': False,
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
    tags=["personal-project", "berka"],
    template_searchpath=[DAGS_DIR,
                        SQL_DDL_SCRIPTS_PATH,
                        DATASETS_PATH
                         ],
)

with dag:
    create_source_tables = SQLExecuteQueryOperator(
    task_id="create_source_tables",
    conn_id=CLICKHOUSE_CONN_ID,
    sql=list_all_files_within_path(SQL_DDL_SCRIPTS_PATH)
    )

    @task_group
    def stage_source_data_in_minio_bucket():
        # TODO: clean up staged data from bucket at the end of DAG
        # Create bucket if not exists
        create_bucket = S3CreateBucketOperator(
            task_id="create_minio_bucket",
            bucket_name=MINIO_BUCKET_NAME,
            region_name="us-east-1",
            aws_conn_id=MINIO_CONN_ID, 
        )

        @task
        def upload_files_to_bucket():
            s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
            
            if not os.path.exists(DATASETS_PATH):
                raise FileNotFoundError(f"The directory {DATASETS_PATH} does not exist.")
                
            files = [f for f in os.listdir(DATASETS_PATH) if os.path.isfile(os.path.join(DATASETS_PATH, f))]
            
            if not files:
                logger.warning(f"No files found in {DATASETS_PATH} to upload.")
                return

            logger.info(f"Found {len(files)} files to upload to s3://{MINIO_BUCKET_NAME}/")

            for file_name in files:
                local_file_path = os.path.join(DATASETS_PATH, file_name)
                
                print(f"Uploading {file_name}...")
                s3_hook.load_file(
                    filename=local_file_path,
                    key=file_name,
                    bucket_name=MINIO_BUCKET_NAME,
                    replace=True  # Overwrites the file if it already exists in S3
                )
            logger.info("All files successfully uploaded.")
            return files

        upload_files = upload_files_to_bucket()
        create_bucket >> upload_files

    
    extract = PythonOperator(
        task_id="extract_source_data_from_kaggle",
        python_callable=extract_source_data_from_kaggle,
        # op_kwargs={"key": "value"}  # Passes keyword arguments to the function
    )

    stage = stage_source_data_in_minio_bucket()

    ingest_clickhouse = PythonOperator(
        task_id="ingest_staged_data_into_clickhouse_source_tables",
        python_callable=ingest_staged_data_into_source_tables,
    )

    create_source_tables >> \
    extract >> stage >> ingest_clickhouse