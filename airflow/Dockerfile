FROM apache/airflow:3.3.2

# /data is the shared dota-data volume; a new named volume copies this ownership
USER root
RUN mkdir -p /data/replays /data/output && chown -R airflow:0 /data
USER airflow

RUN pip install --no-cache-dir "apache-airflow==3.3.2" apache-airflow-providers-docker==4.5.9 requests==2.34.2 zstandard==0.25.0 boto3==1.43.75 psycopg2-binary==2.9.13 dbt-core==1.12.6 dbt-postgres==1.11.0
