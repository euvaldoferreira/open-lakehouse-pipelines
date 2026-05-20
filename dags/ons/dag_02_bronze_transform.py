"""
ONS DAG 02 - Bronze Layer Transformation (Multi-year)

Reads every year where raw checkpoint is newer than bronze checkpoint,
enriches the Parquet, and writes to the bronze bucket.
"""

from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

DATASET = "balanco_energia_subsistema_ho"
REQUIRED_COLS = {
    "id_subsistema", "nom_subsistema", "din_instante",
    "val_carga", "val_gerhidraulica", "val_gertermica",
    "val_gereolica", "val_gersolar", "val_intercambio",
}

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

DAG_ID = "ons_dag_02_bronze_transform"
RAW_DAG_ID = "ons_dag_01_raw_ingestion"

from lakehouse_utils import (
    get_s3_client as _get_s3_client,
    write_pipeline_audit as _write_audit,
    get_checkpoint_date as _get_checkpoint_date,
    set_checkpoint_date as _set_checkpoint_date,
    get_partitions_to_process as _get_partitions_to_process,
)


def transform_to_bronze(**context) -> None:
    import io
    import pandas as pd

    years = _get_partitions_to_process(DATASET, RAW_DAG_ID, DAG_ID)
    if not years:
        print("No years pending for bronze transform")
        context["task_instance"].xcom_push(key="processed_years", value=[])
        return

    s3 = _get_s3_client()
    total_rows = 0
    processed = []

    for year in years:
        raw_key = f"ons/{DATASET}/year={year}/BALANCO_ENERGIA_SUBSISTEMA_{year}.parquet"
        response = s3.get_object(Bucket="raw", Key=raw_key)
        df = pd.read_parquet(io.BytesIO(response["Body"].read()))

        missing = REQUIRED_COLS - set(df.columns)
        if missing:
            raise ValueError(f"Year {year}: missing columns {missing}")

        df["data_referencia"] = df["din_instante"].dt.date.astype(str)
        df["ingested_at"] = pd.Timestamp.now()

        # Spark rejects TIMESTAMP(NANOS) — coerce all datetime columns to microseconds
        for col in df.select_dtypes(include=["datetime64"]).columns:
            df[col] = df[col].astype("datetime64[us]")

        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow",
                      coerce_timestamps="us", allow_truncated_timestamps=True)
        buffer.seek(0)

        bronze_key = f"ons/{DATASET}/year={year}/data.parquet"
        s3.put_object(Bucket="bronze", Key=bronze_key, Body=buffer.getvalue())

        raw_cp = _get_checkpoint_date(DATASET, RAW_DAG_ID, year)
        _set_checkpoint_date(DATASET, DAG_ID, year, raw_cp, len(df))
        total_rows += len(df)
        processed.append(year)
        print(f"Bronze year {year}: {len(df):,} rows")

    context["task_instance"].xcom_push(key="processed_years", value=processed)
    context["task_instance"].xcom_push(key="total_rows", value=total_rows)
    print(f"Bronze complete. Years: {processed}, total rows: {total_rows:,}")


def _gate_trigger(**context) -> bool:
    if not context["dag_run"].conf.get("trigger_next", True):
        return False
    processed = context["task_instance"].xcom_pull(task_ids="transform_to_bronze", key="processed_years")
    return bool(processed)


def write_pipeline_audit(**context) -> None:
    processed = context["task_instance"].xcom_pull(task_ids="transform_to_bronze", key="processed_years") or []
    total_rows = context["task_instance"].xcom_pull(task_ids="transform_to_bronze", key="total_rows") or 0
    _write_audit(
        dag_id=context["dag"].dag_id,
        run_id=context["run_id"],
        layer="bronze",
        status="success" if processed else "skipped",
        row_count=total_rows,
        notes=f"dataset={DATASET} years={processed}",
    )


with DAG(
    dag_id=DAG_ID,
    description="[ONS] Multi-year bronze enrichment",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["ons", "bronze", "transform", "energia"],
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    transform = PythonOperator(
        task_id="transform_to_bronze",
        python_callable=transform_to_bronze,
    )

    audit = PythonOperator(
        task_id="write_pipeline_audit",
        python_callable=write_pipeline_audit,
        trigger_rule="all_done",
    )

    gate = ShortCircuitOperator(
        task_id="gate_trigger",
        python_callable=_gate_trigger,
    )

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_transform",
        trigger_dag_id="ons_dag_03_silver_transform",
        wait_for_completion=False,
    )

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    start >> transform >> [audit, gate]
    gate >> trigger_silver
    [audit, trigger_silver] >> end
