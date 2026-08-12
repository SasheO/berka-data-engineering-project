# berka-data-engineering-project

## Overview of the project and the business problem

## Pipeline Architecture
![Pipeline architecture diagram](images/pipeline_architecture.png)

## Setup Instructions 
Clone this repo onto your device using by running this command in your terminal:
```
git clone https://github.com/SasheO/berka-data-engineering-project.git
```

Copy `.env_example` into a file named .env and fill in your environment variables.
Set MINIO_USERNAME to `admin` and pick your preferred MinIO password.
Set CLICKHOUSE_USERNAME to `default` and pick your preferred ClickHouse password.

### Configure User Permissions
* If you’re using Linux, set a user ID to prevent permission issues when Docker writes files locally:
echo -e "AIRFLOW_UID=$(id -u)". Copy that into AIRFLOW_UID in .env
* If you’re using macOS or Windows, leave the value as is: `AIRFLOW_UID=50000`

Create Kaggle login credentials using api key, put credentials in .env file as `KAGGLE_USERNAME` and `KAGGLE_KEY`

### Set Up Connections and Variables in Airflow
In your terminal, navigate to the folder where this repository is stored and run `docker compose up`.

Open up the Airflow webserver in a browser using at [http://localhost:8080/](http://localhost:8080/). 

Go to `admin` > `connections` and click the `+` sign to create a new connection. The connections to add are:

**Clickhouse:**
* name: clickhouse_conn
* connection type: clickhouse
* host name: clickhouse-server
* login and password: use your credentials in .env for login and password
* port 8123


**Minio:**
* name: minio_conn
* connection type: AWS connection type
* login and password: use your credentials in .env for login and password
* extras: replace MINIO_USERNAME and MINIO_PASSWORD with what is in your .env file.
```
{
        "aws_access_key_id": MINIO_USERNAME, 
    "aws_secret_access_key": MINIO_PASSWORD,
    "endpoint_url": "http://minio:9000",
    "region_name": "us-east-1"
    }
```
If you test the MinIO connections, it may fail. This is completely normal for MinIO because Airflow's test engine inherently tries to reach the live AWS Security Token Service (STS) endpoint to validate credentials, which local MinIO services do not support. Save the connection anyway. It will still work perfectly in your DAGs.

**Variables**
Go to `admin` > `Variable` and click the `+` sign to create a new variable. The variables to add are:
set up variabes in airflow:
* name: minio_endpoint
    value: http://minio:9000
* name: minio_password
    value: with what is in your .env file.
* name: minio_username
    value: with what is in your .env file.


## Steps to Run the DAG and Inspect Results by Querying Clickhouse Tables


## Dimensional Model
Here is a link to the dimensional model with description on each table and many fields: [Dimensonal Model](https://dbdiagram.io/d/berka-dataset-v2-6a4e96184ac62e474c5dd29c). DBT Docs on airflow also shows this same information but on a physical implementation rather than logical level.

![Dimensional model diagram](images/erd.png)

### Reasons for Various Design choices

## Tests and Validation

## Known limitations and ideas for extending the project
