FROM apache/airflow:2.9.3-python3.11
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt" \
    dbt-core==1.8.* dbt-duckdb==1.8.* duckdb psycopg2-binary astronomer-cosmos==1.*
