FROM apache/airflow:2.11.2-python3.12
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# FROM apache/airflow:2.11.2

# # update system dependencies, if any
# USER root
# RUN apt-get update && apt-get clean

# USER airflow
# COPY requirements.txt /requirements.txt
# RUN pip install --no-cache-dir -r /requirements.txt
