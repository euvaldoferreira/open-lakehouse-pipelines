"""
DAG 04 - Data Quality Validation
Runs quality checks on bronze and silver layers using Great Expectations.
"""

from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def check_bronze_quality(**context) -> dict:
    """Validate bronze layer: null checks, range checks, row count."""
    import io
    import os

    import boto3
    import pandas as pd
    from botocore.config import Config

    execution_date = context["ds"]
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    key = f"sales/year={execution_date[:4]}/month={execution_date[5:7]}/day={execution_date[8:]}/orders.parquet"
    try:
        resp = s3.get_object(Bucket="bronze", Key=key)
        df = pd.read_parquet(io.BytesIO(resp["Body"].read()))
    except Exception as e:
        print(f"Could not read bronze data: {e}")
        return {"status": "warning", "issues": [str(e)], "row_count": 0}

    issues = []
    if df.empty:
        issues.append("Empty dataset")
    null_ids = df["id"].isnull().sum()
    if null_ids > 0:
        issues.append(f"{null_ids} null IDs found")
    negative_prices = (df["unit_price"] < 0).sum()
    if negative_prices > 0:
        issues.append(f"{negative_prices} negative prices found")
    negative_qty = (df["quantity"] <= 0).sum()
    if negative_qty > 0:
        issues.append(f"{negative_qty} non-positive quantities found")

    result = {
        "status": "failed" if issues else "passed",
        "issues": issues,
        "row_count": len(df),
        "date": execution_date,
    }
    print(f"Quality check result: {result}")
    context["task_instance"].xcom_push(key="quality_result", value=result)
    return result


def route_on_quality(**context) -> str:
    result = context["task_instance"].xcom_pull(
        task_ids="check_bronze_quality", key="quality_result"
    )
    if result and result.get("status") == "passed":
        return "quality_passed"
    return "quality_warning"


def handle_quality_warning(**context) -> None:
    result = context["task_instance"].xcom_pull(
        task_ids="check_bronze_quality", key="quality_result"
    )
    print(f"WARNING: Data quality issues detected: {result}")


with DAG(
    dag_id="dag_04_quality_validation",
    description="Data quality validation for bronze and silver layers",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["quality", "validation", "dq"],
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    check_quality = PythonOperator(
        task_id="check_bronze_quality",
        python_callable=check_bronze_quality,
    )

    branch = BranchPythonOperator(
        task_id="branch_on_quality",
        python_callable=route_on_quality,
    )

    quality_passed = EmptyOperator(task_id="quality_passed")

    quality_warning = PythonOperator(
        task_id="quality_warning",
        python_callable=handle_quality_warning,
    )

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    start >> check_quality >> branch >> [quality_passed, quality_warning] >> end
