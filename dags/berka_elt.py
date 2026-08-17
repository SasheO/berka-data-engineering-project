import logging
import os
from airflow.models import DAG
from airflow.decorators import task_group, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import io
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
from helpers import list_all_files_within_path
import requests
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.constants import TestBehavior
from cosmos.operators import DbtDocsS3Operator
from cosmos.profiles import ClickhouseUserPasswordProfileMapping

load_dotenv()
import kaggle as kg # import kaggle ONLY after loading environment variables

logger = logging.getLogger(__name__)

CLICKHOUSE_CONN_ID = "clickhouse_conn"
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")
MINIO_CONN_ID = "minio_conn"
DAGS_DIR = Path(__file__).resolve().parent
SQL_SCRIPTS_PATH =  "/opt/airflow/include/sql"
SQL_DDL_SCRIPTS_PATH_PREFIX = 'create_tables'
EMAIL_ON_FAILURE_LIST = [os.getenv("MY_EMAIL")]
KAGGLE_KEY=os.getenv('KAGGLE_KEY')
KAG_USER=os.getenv("KAGGLE_USERNAME")
BERKA_DBT_PROJECT_PATH="/opt/airflow/berka_dbt_project"
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
CLICKHOUSE_SCHEMA_NAME=os.getenv("CLICKHOUSE_SCHEMA_NAME", "berka_raw")

profile_config = ProfileConfig(
    profile_name="berka_dbt_profile",
    target_name="dev",
    profile_mapping=ClickhouseUserPasswordProfileMapping( 
         conn_id=CLICKHOUSE_CONN_ID, 
         profile_args={
            "schema": "berka_analytics",
            "port": 9000,
            "driver": "native" 
        },
     ), 
)

project_config = ProjectConfig(
    dbt_project_path=BERKA_DBT_PROJECT_PATH
    )

render_config=RenderConfig(
        test_behavior=TestBehavior.AFTER_EACH,
        should_detach_multiple_parents_tests=True,
    )

@task()
def stream_and_stage_source_data_from_kaggle():
    dataset_name = "marceloventura/the-berka-dataset"
    url = f"https://www.kaggle.com/api/v1/datasets/download/{dataset_name}"
    
    response = requests.get(url,  auth=(KAG_USER, KAGGLE_KEY), stream=True)
    if response.status_code != 200:
        raise ValueError(f"Kaggle API Error {response.status_code}: {response.text}")

    content_type = response.headers.get('Content-Type')
    logger.info(f"Successfully retrieved {dataset_name}, file type: {content_type}")
    
    # Convert the raw stream into a file-like object in memory
    zip_buffer = io.BytesIO(response.content)
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)

    kaggle_file_names = []

    # Extract and upload each file individually without writing to disk
    with zipfile.ZipFile(zip_buffer) as z:

        # for each file zipfile object
        for file_info in z.infolist():
            # Skip directory markers inside the zip archive
            if file_info.is_dir():
                continue
                
            logger.info(f"Extracting and uploading: {file_info.filename}")
            
            # Open the specific file inside the zip as a file object in memory
            with z.open(file_info.filename) as extracted_file:

                object_name = file_info.filename[:-4] # sliced to remove ".csv"

                # Upload file object to minio
                logger.info(f"Uploading {object_name}...")
                s3_hook.load_file_obj(
                    file_obj=extracted_file,
                    key="raw_data/"+object_name,
                    bucket_name=MINIO_BUCKET_NAME,
                    replace=True  # Overwrites the file if it already exists in S3
                )

                kaggle_file_names.append(object_name)

    logger.info("All files unzipped and transferred successfully")
    return kaggle_file_names # put this in context for deleting files later in pipeline

@task_group()
def ingest_staged_data_into_source_tables():
    for file_name, tup in SOURCE_NAME_TO_INGESTION_SCRIPT_MAPPING.items():
        table_name, ingestion_script_name = tup
        ingest = SQLExecuteQueryOperator(
            task_id=f"ingest_into_{table_name}",
            conn_id=CLICKHOUSE_CONN_ID,
            sql="ingestion/"+ingestion_script_name+".sql",
            params={'db_schema': CLICKHOUSE_SCHEMA_NAME,
                    "table_name": table_name,
                    "file_name": file_name,
                    "minio_bucket_name": MINIO_BUCKET_NAME,
                    }
        )
        deduplicate = SQLExecuteQueryOperator(
            task_id=f"optimize_{table_name}",
            conn_id=CLICKHOUSE_CONN_ID,
            sql="ingestion/optimise_table_deduplicate_clickhouse.sql",
            params={'db_schema': CLICKHOUSE_SCHEMA_NAME,
                    "table_name": table_name,
                    }
        )
        ingest >> deduplicate

@task()
def create_bucket_if_not_exists():
    hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    # Check if the bucket already exists
    if not hook.check_for_bucket(MINIO_BUCKET_NAME):
        hook.create_bucket(bucket_name=MINIO_BUCKET_NAME)
        logger.info(f"Bucket {MINIO_BUCKET_NAME} created successfully.")
    else:
        logger.info(f"Bucket {MINIO_BUCKET_NAME} already exists. Skipping creation.")

@task_group()
def post_ingestion():
    SQLExecuteQueryOperator(
    task_id="enrich_transactions_with_ordering",
    conn_id=CLICKHOUSE_CONN_ID,
    sql=f"ingestion/src_transactions_enriched.sql",
    params={'db_schema': CLICKHOUSE_SCHEMA_NAME}
    )

    SQLExecuteQueryOperator(
    task_id="add_invalid_pointer_ref_to_districts",
    conn_id=CLICKHOUSE_CONN_ID,
    sql=f"ingestion/src_demographic_district_invalid_pointer.sql",
    params={'db_schema': CLICKHOUSE_SCHEMA_NAME}
    )

dag = DAG(
    dag_id="berka_elt",
    max_active_runs=1,
    max_active_tasks=5,
    description="A dag which extracts, loads and transforms data from Berka financial dataset with DBT and Clickhouse",
    schedule=timedelta(days=1),
    start_date=datetime(2026, 7, 15),
    catchup=False,
    tags=["personal-project", "berka"],
    default_args={
        "depends_on_past": True, # so if a relationship data test fails for example (they are not fail-fast but run after all berka tasks), data quality issues don't compound. The DE gets a notification and can resolve the issue before future runs.
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        'email': EMAIL_ON_FAILURE_LIST,
        'email_on_failure': True,
        'email_on_retry': False,
        'execution_timeout': timedelta(minutes=10),
        'trigger_rule': 'all_success',
        'params': {
            'db_schema': CLICKHOUSE_SCHEMA_NAME,
            },
    },
    # where DAG looks for files
    template_searchpath=[DAGS_DIR,
                        SQL_SCRIPTS_PATH
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


    dbt_models = DbtTaskGroup(
        group_id = "dbt_models",
        project_config = project_config,
        profile_config = profile_config,
        render_config = render_config,
    )

    generate_dbt_docs_to_minio_bucket = DbtDocsS3Operator(
        task_id="generate_dbt_docs_to_minio_bucket",
        project_dir=BERKA_DBT_PROJECT_PATH,
        profile_config=profile_config,
        connection_id=MINIO_CONN_ID,
        bucket_name=MINIO_BUCKET_NAME,
        folder_dir="/dbt_docs"
    )

    create_minio_bucket = create_bucket_if_not_exists()

    extract_and_stage = stream_and_stage_source_data_from_kaggle()

    ingest_clickhouse = ingest_staged_data_into_source_tables()

    post_ingestion_tasks =  post_ingestion()

    create_schema_tables >> create_source_tables >> create_minio_bucket >> \
    extract_and_stage >> ingest_clickhouse >> post_ingestion_tasks >> \
    dbt_models >> generate_dbt_docs_to_minio_bucket