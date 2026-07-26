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

CLICKHOUSE_CONN_ID = "clickhouse_conn"
MINIO_BUCKET_NAME = 'berka-raw-data-bucket'
MINIO_CONN_ID = "minio_conn"
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
DAGS_DIR = Path(__file__).resolve().parent
SQL_SCRIPTS_PATH =  "/opt/airflow/include/sql"
SQL_DDL_SCRIPTS_PATH_PREFIX = 'create_tables'
DATASETS_PATH =  "/opt/airflow/datasets"
EMAIL_ON_FAILURE_LIST = [os.getenv("MY_EMAIL")]
SOURCE_NAME_TO_INGESTION_SCRIPT_MAPPING = {
    # each record is file_name: (table_name, ingestion_script_name)
        "account": ("src_accounts", "ingest_csv_with_names"),
        "card": ("src_cards", "ingest_csv_with_names"),
        "client": ("src_clients", "ingest_csv_with_names"),
        "disp": ("src_disposition", "ingest_csv_with_names"),
        "district": ("src_demographic_districts", "src_demographic_districts"),
        "loan": ("src_loans", "ingest_csv_with_names"),
        "order": ("src_permanent_orders", "ingest_csv_with_names"),
        "trans": ("src_transactions", "ingest_csv_with_names"),
    }

CLICKHOUSE_SCHEMA_NAME=os.getenv("CLICKHOUSE_SCHEMA_NAME", "berka_analytics")

# TODO: add multiline comments to all functions
def list_all_files_within_path(path: str, with_path_prefix: str = ''):
    if not os.path.exists(path):
        raise FileNotFoundError(f"The directory {path} does not exist.")
    if with_path_prefix:
        return [with_path_prefix + "/" + f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
    else:
        return [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]

def extract_source_data_from_kaggle():
    kg.api.authenticate()
    kg.api.dataset_download_files(dataset = "marceloventura/the-berka-dataset", path=DATASETS_PATH, unzip=True)
    logger.info(f"Successfully retrieved marceloventura/the-berka-dataset into {DATASETS_PATH}")
    



dag = DAG(
    "berka_elt",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "depends_on_past": True,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        'email': EMAIL_ON_FAILURE_LIST,
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
        'trigger_rule': 'all_success',
        'params': {
            'db_schema': CLICKHOUSE_SCHEMA_NAME,
            },
    },
    description="A dag which extracts, loads and transforms data from Berka financial dataset with DBT and Clickhouse",
    schedule=timedelta(days=1),
    start_date=datetime(2026, 7, 15),
    catchup=False,
    tags=["personal-project", "berka"],
    template_searchpath=[DAGS_DIR,
                        # SQL_DDL_SCRIPTS_PATH,
                        SQL_SCRIPTS_PATH,
                        DATASETS_PATH
                         ],
)

with dag:
    create_schema_tables = SQLExecuteQueryOperator(
    task_id="create_schema",
    conn_id=CLICKHOUSE_CONN_ID,
    sql=f'CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_SCHEMA_NAME};'
    )

    create_source_tables = SQLExecuteQueryOperator(
    task_id="create_source_tables",
    conn_id=CLICKHOUSE_CONN_ID,
    sql=list_all_files_within_path(SQL_SCRIPTS_PATH+"/"+SQL_DDL_SCRIPTS_PATH_PREFIX, SQL_DDL_SCRIPTS_PATH_PREFIX)
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
    )


    @task_group
    def ingest_staged_data_into_source_tables():
        for file_name, tup in SOURCE_NAME_TO_INGESTION_SCRIPT_MAPPING.items():
            table_name, ingestion_script_name = tup
            SQLExecuteQueryOperator(
                task_id=f"ingest_into_{table_name}",
                conn_id=CLICKHOUSE_CONN_ID,
                sql="ingestion/"+ingestion_script_name,
                params={'db_schema': CLICKHOUSE_SCHEMA_NAME,
                        "table_name": table_name,
                        "minio_endpoint": MINIO_ENDPOINT,
                        "minio_username": MINIO_ROOT_USER,
                        "minio_password": MINIO_ROOT_PASSWORD,
                        "file_name": file_name,
                        "minio_bucket_name": MINIO_BUCKET_NAME,
                        }
            )

    stage = stage_source_data_in_minio_bucket()
    ingest_clickhouse = ingest_staged_data_into_source_tables()

    create_schema_tables >> create_source_tables >> \
    extract >> stage >> ingest_clickhouse