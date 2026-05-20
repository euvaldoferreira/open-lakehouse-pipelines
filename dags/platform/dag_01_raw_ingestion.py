"""
DAG 01 - Raw Data Ingestion
Simulates ingesting raw data into the MinIO raw bucket.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def generate_sample_data(**context) -> dict:
    """Generate synthetic sales data."""
    import random

    execution_date = context["ds"]
    records = []
    for i in range(100):
        records.append({
            "id": f"ORD-{execution_date}-{i:04d}",
            "customer_id": f"CUST-{random.randint(1, 500):04d}",
            "product_id": f"PROD-{random.randint(1, 200):04d}",
            "quantity": random.randint(1, 20),
            "unit_price": round(random.uniform(10.0, 999.99), 2),
            "event_ts": datetime.now().isoformat(),
            "partition_date": execution_date,
        })
    return {"records": records, "count": len(records), "date": execution_date}


def upload_to_minio(**context) -> None:
    """Upload generated data to MinIO raw bucket."""
    import boto3
    from botocore.config import Config

    data = context["task_instance"].xcom_pull(task_ids="generate_data")
    execution_date = context["ds"]

    s3_client = boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    key = f"sales/year={execution_date[:4]}/month={execution_date[5:7]}/day={execution_date[8:]}/orders.json"
    payload = json.dumps(data["records"], indent=2)

    s3_client.put_object(
        Bucket="raw",
        Key=key,
        Body=payload.encode("utf-8"),
        ContentType="application/json",
    )

    print(f"Uploaded {data['count']} records to s3://raw/{key}")


with DAG(
    dag_id="dag_01_raw_ingestion",
    description="Ingest raw sales data into MinIO raw bucket",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["ingestion", "raw", "sales"],
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    generate_data = PythonOperator(
        task_id="generate_data",
        python_callable=generate_sample_data,
    )

    upload_raw = PythonOperator(
        task_id="upload_to_raw",
        python_callable=upload_to_minio,
    )

    end = EmptyOperator(task_id="end")

    start >> generate_data >> upload_raw >> end
