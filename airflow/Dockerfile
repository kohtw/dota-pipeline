FROM apache/airflow:3.3.2

# /data is the shared dota-data volume; a new named volume copies this ownership
USER root
RUN mkdir -p /data/replays /data/output && chown -R airflow:0 /data
USER airflow

RUN pip install --no-cache-dir "apache-airflow==3.3.2" apache-airflow-providers-docker requests zstandard boto3 psycopg2-binary dbt-core dbt-postgres
