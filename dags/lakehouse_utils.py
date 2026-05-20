"""
Shared utilities for Lakehouse DAGs.

Centralises connection factories, Spark configuration and audit helpers so
every domain (ONS, sales, ANEEL, IBGE …) imports from a single source.
"""

from __future__ import annotations

import json
import os
from datetime import date, timedelta
from typing import Any

# ---------------------------------------------------------------------------
# Default DAG args — common baseline, override per DAG as needed
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ---------------------------------------------------------------------------
# Spark — base configuration shared by all Spark DAGs
# Credentials are read from env vars (never hardcoded).
# ---------------------------------------------------------------------------
SPARK_CONF_BASE: dict[str, str] = {
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    # silver catalog → s3://silver/
    "spark.sql.catalog.silver": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.silver.type": "hadoop",
    "spark.sql.catalog.silver.warehouse": "s3a://silver/",
    # gold catalog → s3://gold/
    "spark.sql.catalog.gold": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.gold.type": "hadoop",
    "spark.sql.catalog.gold.warehouse": "s3a://gold/",
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.sql.shuffle.partitions": "4",
}

SPARK_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)


def spark_conf(extra: dict[str, str] | None = None) -> dict[str, str]:
    import os
    conf = dict(SPARK_CONF_BASE)
    conf["spark.hadoop.fs.s3a.access.key"] = os.environ["MINIO_ROOT_USER"]
    conf["spark.hadoop.fs.s3a.secret.key"] = os.environ["MINIO_ROOT_PASSWORD"]
    if extra:
        conf.update(extra)
    return conf


# ---------------------------------------------------------------------------
# Connection factories
# ---------------------------------------------------------------------------

def get_s3_client():
    """Return a boto3 S3 client pre-configured for MinIO."""
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def get_pg_conn():
    """Return a psycopg2 connection to the platform PostgreSQL."""
    import psycopg2

    return psycopg2.connect(
        os.environ["AIRFLOW__CORE__SQL_ALCHEMY_CONN"].replace(
            "postgresql+psycopg2://", "postgresql://"
        )
    )


# ---------------------------------------------------------------------------
# Audit helpers — write to shared operational tables
# ---------------------------------------------------------------------------

def get_checkpoint_date(dataset: str, dag_id: str, partition_key: str) -> "date | None":
    """Return the last processed date for *(dataset, dag_id, partition_key)*, or None.

    partition_key is domain-defined: "2024" for annual, "2024-01" for monthly,
    "2024-01-15" for daily data.
    """
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT last_date FROM lakehouse.pipeline_checkpoint "
                "WHERE dataset = %s AND dag_id = %s AND partition_key = %s",
                (dataset, dag_id, partition_key),
            )
            row = cur.fetchone()
    finally:
        conn.close()
    return row[0] if row else None


def set_checkpoint_date(
    dataset: str, dag_id: str, partition_key: str, target_date, row_count: int | None = None
) -> None:
    """Upsert the checkpoint for *(dataset, dag_id, partition_key)*."""
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO lakehouse.pipeline_checkpoint
                    (dataset, dag_id, partition_key, last_date, row_count, updated_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (dataset, dag_id, partition_key) DO UPDATE
                    SET last_date  = EXCLUDED.last_date,
                        row_count  = EXCLUDED.row_count,
                        updated_at = NOW()
                """,
                (dataset, dag_id, partition_key, target_date, row_count),
            )
        conn.commit()
    finally:
        conn.close()


def get_partitions_to_process(
    dataset: str, source_dag_id: str, target_dag_id: str | None = None
) -> list[str]:
    """Return partition_keys where source is newer than target (or target has no entry).

    If target_dag_id is None, returns all partition_keys present in source.
    Results are ordered lexicographically (works for "2024", "2024-01", "2024-01-15").
    """
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            if target_dag_id is None:
                cur.execute(
                    "SELECT partition_key FROM lakehouse.pipeline_checkpoint "
                    "WHERE dataset = %s AND dag_id = %s ORDER BY partition_key",
                    (dataset, source_dag_id),
                )
            else:
                cur.execute(
                    """
                    SELECT s.partition_key
                    FROM lakehouse.pipeline_checkpoint s
                    LEFT JOIN lakehouse.pipeline_checkpoint t
                        ON t.dataset = s.dataset
                       AND t.dag_id = %s
                       AND t.partition_key = s.partition_key
                    WHERE s.dataset = %s AND s.dag_id = %s
                      AND (t.last_date IS NULL OR s.last_date > t.last_date)
                    ORDER BY s.partition_key
                    """,
                    (target_dag_id, dataset, source_dag_id),
                )
            return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


def write_pipeline_audit(
    dag_id: str,
    run_id: str,
    layer: str,
    status: str,
    row_count: int | None = None,
    notes: str | None = None,
) -> None:
    """Insert one row into lakehouse.pipeline_audit."""
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO lakehouse.pipeline_audit
                    (dag_id, run_id, layer, status, row_count, started_at, finished_at, notes)
                VALUES (%s, %s, %s, %s, %s, NOW(), NOW(), %s)
                """,
                (dag_id, run_id, layer, status, row_count, notes),
            )
        conn.commit()
    finally:
        conn.close()


def write_dq_result(
    dag_id: str,
    run_date: str,
    layer: str,
    check_name: str,
    status: str,
    issue_count: int = 0,
    details: dict[str, Any] | None = None,
) -> None:
    """Insert one row into lakehouse.dq_results."""
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO lakehouse.dq_results
                    (dag_id, run_date, layer, check_name, status, issue_count, details)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (dag_id, run_date, layer, check_name, status, issue_count,
                 json.dumps(details or {})),
            )
        conn.commit()
    finally:
        conn.close()
