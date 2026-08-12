# berka-data-engineering-project

## Overview of the project and the business problem

## Pipeline Architecture
![Pipeline architecture diagram](images/pipeline_architecture.png)

## Setup Instructions 
Clone this repo onto your device using by running this command in your terminal:
```
git clone https://github.com/SasheO/berka-data-engineering-project.git
```

Copy `.env_example` into a file named `.env` and fill in your environment variables.
The environment variables you should change start with `fill_in_`.
Set `MINIO_ROOT_USER` to `admin` and pick your preferred MinIO password.
Set `CLICKHOUSE_USER` to `default` and pick your preferred ClickHouse password.
Create Kaggle login credentials using api key, put credentials in .env file as `KAGGLE_USERNAME` and `KAGGLE_KEY`

To configure user permissions:
* If you’re using Linux, set a user ID to prevent permission issues when Docker writes files locally:
echo -e "AIRFLOW_UID=$(id -u)". Copy that into AIRFLOW_UID in .env
* If you’re using macOS or Windows, leave the value as is: `AIRFLOW_UID=50000`


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
* extras: Copy what is below into the text box and replace MINIO_ROOT_USER and MINIO_PASSWORD with what is in your .env file.
```
{
        "aws_access_key_id": MINIO_ROOT_USER, 
    "aws_secret_access_key": MINIO_PASSWORD,
    "endpoint_url": "http://minio:9000",
    "region_name": "us-east-1"
    }
```
If you test the MinIO connections, it may fail. This is completely normal because Airflow's test engine inherently tries to reach the live AWS Security Token Service (STS) endpoint to validate credentials, which local MinIO services do not support. Save the connection anyway. It will still work perfectly in your DAGs.

**Variables**
Go to `admin` > `Variable` and click the `+` sign to create a new variable. The variables to add are:
set up variabes in airflow:
* name: minio_endpoint
    value: http://minio:9000
* name: minio_password
    value: with what is in your .env file.
* name: minio_username
    value: with what is in your .env file.


## Steps to Run the DAG and Inspect Results by Querying ClickHouse Tables
From the [Airflow webserver](http://localhost:8080/), type in  `berka_elt` in the search bar to find the DAG. Click on the berka DAG.

Toggle the button at the top-left of the screen to unpause the DAG. If the pipeline does not get automatically triggerred after unpausing it, you can trigger it by clicking the play button at the top-left of the page.

The DAG will run and populate various staging, dimension, snapshot, and fact tables in `berka_analytics` schema. The source data that was ingested raw will be in `berka_raw` schema.

To query ClickHouse tables, open up the [ClickHouse Web UI](http://localhost:8123/). Put in your default username and password in the dialogue boxes for credentials at the top left of the screen. Run your queries in the query box. An example of a query that shows which districts have the highest loan default rates:

```sql
SELECT -- TODO: insert
```

The DAG will also generate DBT docs with descriptions of tables, columns, relationships, dependencies, data tests, and more which are viewable within the Airflow webserver if you click `browse` > `DBT Docs`.

## Dimensional Model
Here is a link to the dimensional model with description on each table and many fields: [Dimensonal Model](https://dbdiagram.io/d/berka-dataset-v2-6a4e96184ac62e474c5dd29c). DBT Docs in the [Airflow webserver](http://localhost:8080/) also shows this same information but on a physical implementation rather than logical level.

![Dimensional model diagram](images/erd.png)

### Reasons for Various Design Choices

## Tests and Validation


## Known limitations and ideas for extending the project
