"""Airflow Daily Batch Pipeline for E-Commerce Lakehouse.

Flow:
1. Synthetic Data Generation & Ingestion (MinIO S3 Raw)
2. PySpark Cleansing, Broadcast Join & Silver Lakehouse Storage (Parquet & Postgres Staging)
3. dbt Model Transformations (Staging -> Core SCD Type 2 -> Analytics Marts)
4. dbt Data Quality & Integrity Testing
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="ecommerce_daily_lakehouse_pipeline",
    default_args=default_args,
    description="End-to-end daily batch pipeline with Spark, MinIO S3, Postgres, and dbt",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "lakehouse", "spark", "dbt", "minio"],
) as dag:

    # Task 1: Generate Raw Data & Land into MinIO S3 Raw Bucket
    task_generate_data = BashOperator(
        task_id="generate_and_ingest_raw_data",
        bash_command=(
            "python3 /opt/airflow/scripts/data_generator.py "
            "--date {{ ds }} "
            "--output auto"
        ),
    )

    # Task 2: PySpark Bronze to Silver Processing (Deduplication, Broadcast Join, Parquet & Staging DB)
    task_spark_transform = BashOperator(
        task_id="spark_bronze_to_silver_transform",
        bash_command=(
            "python3 /opt/airflow/spark/jobs/process_bronze_to_silver.py "
            "--date {{ ds }} "
            "--storage-type minio"
        ),
    )

    # Task 3: Run dbt Transformations (Staging -> Core Dimensions/Facts -> Analytics Marts)
    task_dbt_run = BashOperator(
        task_id="dbt_transform_models",
        bash_command=(
            "dbt run "
            "--project-dir /opt/airflow/dbt_project "
            "--profiles-dir /opt/airflow/dbt_project"
        ),
    )

    # Task 4: Run dbt Data Quality & Referential Integrity Tests
    task_dbt_test = BashOperator(
        task_id="dbt_test_data_quality",
        bash_command=(
            "dbt test "
            "--project-dir /opt/airflow/dbt_project "
            "--profiles-dir /opt/airflow/dbt_project"
        ),
    )

    # Define linear execution topology
    task_generate_data >> task_spark_transform >> task_dbt_run >> task_dbt_test
