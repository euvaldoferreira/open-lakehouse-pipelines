"""
DAG 02 - Bronze Layer Transformation
Reads raw JSON data, applies minimal cleaning and writes to bronze as Parquet.
"""

from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def transform_raw_to_bronze(**context) -> None:
    """Read raw JSON from MinIO and write cleaned Parquet to bronze bucket."""
    import io
    import json
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

    raw_key = f"sales/year={execution_date[:4]}/month={execution_date[5:7]}/day={execution_date[8:]}/orders.json"
    response = s3.get_object(Bucket="raw", Key=raw_key)
    records = json.loads(response["Body"].read())

    df = pd.DataFrame(records)
    df["ingested_at"] = pd.Timestamp.now()
    df["total_price"] = (df["quantity"] * df["unit_price"]).round(2)
    df = df.dropna(subset=["id", "customer_id", "product_id"])

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    bronze_key = f"sales/year={execution_date[:4]}/month={execution_date[5:7]}/day={execution_date[8:]}/orders.parquet"
    s3.put_object(Bucket="bronze", Key=bronze_key, Body=buffer.getvalue())

    print(f"Bronze: wrote {len(df)} rows to s3://bronze/{bronze_key}")
    context["task_instance"].xcom_push(key="row_count", value=len(df))


def validate_bronze(**context) -> None:
    """Basic quality gate: ensure row count > 0."""
    row_count = context["task_instance"].xcom_pull(
        task_ids="transform_to_bronze", key="row_count"
    )
    assert row_count is not None and row_count > 0, f"Bronze validation failed: row_count={row_count}"
    print(f"Bronze validation passed: {row_count} rows")


with DAG(
    dag_id="dag_02_bronze_transform",
    description="Transform raw JSON to bronze Parquet layer",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["bronze", "transform", "sales"],
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    transform = PythonOperator(
        task_id="transform_to_bronze",
        python_callable=transform_raw_to_bronze,
    )

    validate = PythonOperator(
        task_id="validate_bronze",
        python_callable=validate_bronze,
    )

    end = EmptyOperator(task_id="end")

    start >> transform >> validate >> end
