import logging
import os
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task_group, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import io
import zipfile
from datetime import datetime, timedelta
import clickhouse_connect
from pathlib import Path
from dotenv import load_dotenv
from helpers import list_all_files_within_path
import requests

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
KAGGLE_KEY=os.getenv('KAGGLE_KEY')
KAG_USER=os.getenv("KAGGLE_USERNAME")
SOURCE_NAME_TO_INGESTION_SCRIPT_MAPPING = {
    # each record is file_name: (table_name, ingestion_script_name)
        "account": ("src_accounts", "ingest_csv_with_names"),
        "card": ("src_cards", "ingest_csv_with_names"),
        "client": ("src_clients", "ingest_csv_with_names"),
        "disp": ("src_disposition", "ingest_csv_with_names"),
        "district": ("src_demographic_district", "src_demographic_districts"),
        "loan": ("src_loans", "ingest_csv_with_names"),
        "order": ("src_permanent_orders", "ingest_csv_with_names"),
        "trans": ("src_transactions", "ingest_csv_with_names"),
    }
CLICKHOUSE_SCHEMA_NAME=os.getenv("CLICKHOUSE_SCHEMA_NAME", "berka_analytics")

@task()
def stream_and_stage_source_data_from_kaggle():
    url = "https://kaggle.com/marceloventura/the-berka-dataset"
    
    response = requests.get(url,  auth=(KAG_USER, KAGGLE_KEY), stream=True)
    if response.status_code != 200:
        raise ValueError(f"Kaggle API Error {response.status_code}: {response.text}")

    content_type = response.headers.get('Content-Type')
    logger.info(f"--- DIAGNOSTIC: Content-Type from server is: {content_type} ---")

    logger.info(f"Successfully retrieved marceloventura/the-berka-dataset")
    
    # Convert the raw stream into a file-like object in memory
    zip_buffer = zipfile.ZipFile(io.BytesIO(response.content))
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)

    kaggle_file_names = []

    # Extract and upload each file individually without writing to disk
    with zipfile.ZipFile(zip_buffer) as z:
        for file_info in z.infolist():
            # Skip directory markers inside the zip archive
            if file_info.is_dir():
                continue
                
            logger.info(f"Extracting and uploading: {file_info.filename}")
            
            # Open the specific file inside the zip as a stream
            with z.open(file_info.filename) as extracted_file:
                # Construct the target object path in MinIO
                object_name = file_info.filename[-4:] # sliced to remove ".csv"
                
                # Stream the file directly into MinIO
                logger.info(f"Uploading {object_name}...")
                s3_hook.load_file(
                    bytes_data=extracted_file,
                    key=object_name, # sliced to remove ".csv"
                    bucket_name=MINIO_BUCKET_NAME,
                    replace=True  # Overwrites the file if it already exists in S3
                )

                kaggle_file_names.append(object_name)

    logger.info("All files unzipped and transferred successfully!")
    return kaggle_file_names # put this in context for deleting files later in pipeline

@task_group()
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
                key=file_name[:-4], # sliced to remove ".csv"
                bucket_name=MINIO_BUCKET_NAME,
                replace=True  # Overwrites the file if it already exists in S3
            )
        logger.info("All files successfully uploaded.")
        return files

    upload_files = upload_files_to_bucket()
    create_bucket >> upload_files

@task_group()
def ingest_staged_data_into_source_tables():
    for file_name, tup in SOURCE_NAME_TO_INGESTION_SCRIPT_MAPPING.items():
        table_name, ingestion_script_name = tup
        SQLExecuteQueryOperator(
            task_id=f"ingest_into_{table_name}",
            conn_id=CLICKHOUSE_CONN_ID,
            sql="ingestion/"+ingestion_script_name+".sql",
            params={'db_schema': CLICKHOUSE_SCHEMA_NAME,
                    "table_name": table_name,
                    "minio_endpoint": MINIO_ENDPOINT,
                    "minio_username": MINIO_ROOT_USER,
                    "minio_password": MINIO_ROOT_PASSWORD,
                    "file_name": file_name,
                    "minio_bucket_name": MINIO_BUCKET_NAME,
                    }
        )
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

    
    extract_and_stage = stream_and_stage_source_data_from_kaggle()

    ingest_clickhouse = ingest_staged_data_into_source_tables()

    create_schema_tables >> create_source_tables >> \
    extract_and_stage >> ingest_clickhouse