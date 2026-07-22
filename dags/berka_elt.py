# https://medium.com/@sant1/using-minio-with-docker-and-python-cbbad397cb5d
import boto3
import os
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

MINIO_BUCKET_NAME = 'berka-raw-data-bucket'
DATASETS_FOLDER = 'datasets'
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
def extract_source_data_from_kaggle():
    import kaggle as kg # import kaggle ONLY after loading environment variables
    kg.api.authenticate()
    kg.api.dataset_download_files(dataset = "marceloventura/the-berka-dataset", path=DATASETS_FOLDER, unzip=True)
    
def stage_source_data_in_minio_bucket():
    # TODO: clean up staged data from bucket at the end of DAG
    s3_client = boto3.client('s3_client',
                    endpoint_url='http://localhost:9000',
                    aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
                    aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"))
    # Create bucket if not exists
    try:
        s3_client.head_bucket(Bucket=MINIO_BUCKET_NAME) # will throw error if bucket doesn't exist
    except:
        s3_client.create_bucket(Bucket=MINIO_BUCKET_NAME)

    path = DATASETS_FOLDER
    files = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
    for file in files:
        # Upload a file to the bucket: https://docs.aws.amazon.com/boto3/latest/reference/services/s3_client/client/upload_file.html
        s3_client.upload_file(os.path.join(path, file), MINIO_BUCKET_NAME, file)
    
    return files # push file names to xcom (?) -> this can be used for deleting them in cleanup tasks at the end

def create_source_table():
    pass

def ingest_staged_data_into_clickhouse_source_tables():
    
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
    tags=["personal-project", "berka"],)

with dag:
    extract = PythonOperator(
        task_id="extract_source_data_from_kaggle",
        python_callable=extract_source_data_from_kaggle,
        # op_kwargs={"key": "value"}  # Passes keyword arguments to the function
    )

    stage = PythonOperator(
        task_id="stage_source_data_in_minio_bucket",
        python_callable=stage_source_data_in_minio_bucket,
    )

    ingest_clickhouse = PythonOperator(
        task_id="ingest_staged_data_into_clickhouse_source_tables",
        python_callable=ingest_staged_data_into_clickhouse_source_tables,
    )

    extract >> stage >> ingest_clickhouse