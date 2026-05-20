"""
DAG 05 - Spark Processing Pipeline
Full Spark job: reads bronze, writes gold Iceberg table.
"""

from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from lakehouse_utils import spark_conf, SPARK_PACKAGES

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


def notify_completion(**context) -> None:
    print(f"Pipeline completed for {context['ds']} - Gold Iceberg table updated.")


with DAG(
    dag_id="dag_05_spark_processing",
    description="Spark job: bronze -> gold Iceberg aggregated table",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["gold", "spark", "iceberg"],
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    spark_gold = SparkSubmitOperator(
        task_id="spark_to_gold_iceberg",
        application="/opt/airflow/dags/platform/spark_jobs/gold_processing.py",
        conn_id="spark_default",
        conf=spark_conf(),
        application_args=["--date", "{{ ds }}"],
        packages=SPARK_PACKAGES,
        executor_memory="1g",
        driver_memory="1g",
        name="gold-processing-{{ ds }}",
    )

    notify = PythonOperator(
        task_id="notify_completion",
        python_callable=notify_completion,
    )

    end = EmptyOperator(task_id="end")

    start >> spark_gold >> notify >> end
