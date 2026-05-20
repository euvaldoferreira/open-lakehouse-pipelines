"""
ONS DAG 01 - Raw Ingestion (Multi-year)

Downloads all pending annual Parquet files from the ONS public S3 bucket.

Strategy:
  - First run: downloads all years from START_YEAR to current year.
  - Subsequent runs: skips years already processed (checkpoint exists and max_date unchanged),
    always refreshes the current year (updated daily by ONS).
  - Checkpoint key: (dataset, dag_id, year) — one slot per year.
"""

from __future__ import annotations

from datetime import date, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

DATASET = "balanco_energia_subsistema_ho"
ONS_BASE_URL = "https://ons-aws-prod-opendata.s3-us-west-2.amazonaws.com/dataset"
START_YEAR = 2000

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

DAG_ID = "ons_dag_01_raw_ingestion"

from lakehouse_utils import (
    get_s3_client as _get_s3_client,
    write_pipeline_audit as _write_audit,
    get_checkpoint_date as _get_checkpoint_date,
    set_checkpoint_date as _set_checkpoint_date,
)


def download_all_pending(**context) -> None:
    """Download every year that has new data or has never been ingested."""
    import io
    import requests
    import pandas as pd

    current_year = date.today().year
    s3 = _get_s3_client()
    processed = []

    for year in range(START_YEAR, current_year + 1):
        filename = f"BALANCO_ENERGIA_SUBSISTEMA_{year}.parquet"
        url = f"{ONS_BASE_URL}/{DATASET}/{filename}"

        try:
            resp = requests.get(url, timeout=120, allow_redirects=True)
            resp.raise_for_status()
            parquet_bytes = resp.content

            if parquet_bytes[:4] != b"PAR1":
                print(f"Year {year}: non-Parquet response, skipping")
                continue

            df = pd.read_parquet(io.BytesIO(parquet_bytes), columns=["din_instante"])
            max_date = str(df["din_instante"].max().date())
            row_count = len(df)

            cp = _get_checkpoint_date(DATASET, DAG_ID, year)
            if cp is not None and str(cp) == max_date:
                print(f"Year {year}: no new data (max_date={max_date}), skipping")
                continue

            raw_key = f"ons/{DATASET}/year={year}/{filename}"
            s3.put_object(
                Bucket="raw", Key=raw_key, Body=parquet_bytes,
                ContentType="application/octet-stream",
                Metadata={"source": "ons-opendata", "dataset": DATASET, "year": str(year)},
            )
            _set_checkpoint_date(DATASET, DAG_ID, year, max_date, row_count)
            processed.append(year)
            print(f"Year {year}: {row_count:,} rows, max_date={max_date} → uploaded")

        except Exception as exc:
            if year >= current_year - 1:
                raise
            print(f"Year {year}: failed ({exc}), skipping historical year")

    context["task_instance"].xcom_push(key="processed_years", value=processed)
    print(f"Ingestion complete. Updated years: {processed}")


def _gate_trigger(**context) -> bool:
    if not context["dag_run"].conf.get("trigger_next", True):
        return False
    processed = context["task_instance"].xcom_pull(task_ids="download_all_pending", key="processed_years")
    return bool(processed)


def write_pipeline_audit(**context) -> None:
    processed = context["task_instance"].xcom_pull(task_ids="download_all_pending", key="processed_years") or []
    _write_audit(
        dag_id=context["dag"].dag_id,
        run_id=context["run_id"],
        layer="raw",
        status="success" if processed else "skipped",
        notes=f"dataset={DATASET} years={processed}",
    )


with DAG(
    dag_id=DAG_ID,
    description="[ONS] Multi-year incremental ingestion of balanco_energia_subsistema_ho",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["ons", "ingestion", "raw", "energia", "incremental"],
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    download = PythonOperator(
        task_id="download_all_pending",
        python_callable=download_all_pending,
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

    trigger_bronze = TriggerDagRunOperator(
        task_id="trigger_bronze_transform",
        trigger_dag_id="ons_dag_02_bronze_transform",
        wait_for_completion=False,
    )

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    start >> download >> [audit, gate]
    gate >> trigger_bronze
    [audit, trigger_bronze] >> end
