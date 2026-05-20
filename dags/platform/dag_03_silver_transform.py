"""
DAG 03 - Silver Layer Transformation
Aggregates bronze Parquet data into silver Iceberg tables via Spark.
"""

from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from lakehouse_utils import SPARK_PACKAGES, spark_conf

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="dag_03_silver_transform",
    description="Aggregate bronze data into silver Iceberg tables",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["silver", "iceberg", "spark"],
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    spark_silver = SparkSubmitOperator(
        task_id="spark_bronze_to_silver",
        application="/opt/airflow/dags/platform/spark_jobs/silver_transform.py",
        conn_id="spark_default",
        conf=spark_conf(),
        application_args=["--date", "{{ ds }}"],
        packages=SPARK_PACKAGES,
        executor_memory="1g",
        driver_memory="1g",
        name="silver-transform-{{ ds }}",
    )

    end = EmptyOperator(task_id="end")

    start >> spark_silver >> end
