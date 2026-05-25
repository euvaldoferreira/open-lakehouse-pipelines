"""
ONS DAG 01 - Raw Ingestion (Multi-month)

Downloads all pending monthly Parquet files from the ONS public S3 bucket.

Strategy:
  - First run: downloads all months from 2020-05 to current month.
  - Subsequent runs: skips months already processed (checkpoint exists and max_date unchanged),
    always refreshes the current month (updated every 15 minutes by ONS).
  - Checkpoint key: (dataset, dag_id, yearmonth) — one slot per month ("2024-01").
"""

from __future__ import annotations

from datetime import date, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

DATASET = "dados_hidrologicos_ho"
ONS_BASE_URL = "https://ons-aws-prod-opendata.s3-us-west-2.amazonaws.com/dataset"
START_YEAR = 2020
START_MONTH = 5

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

DAG_ID = "ons_hidrologicos_dag_01_raw_ingestion"

from lakehouse_utils import (
    get_checkpoint_date as _get_checkpoint_date,
)
from lakehouse_utils import (
    get_s3_client as _get_s3_client,
)
from lakehouse_utils import (
    set_checkpoint_date as _set_checkpoint_date,
)
from lakehouse_utils import (
    write_pipeline_audit as _write_audit,
)


def _iter_yearmonths(start_year: int, start_month: int) -> list[tuple[int, int]]:
    today = date.today()
    end_year, end_month = today.year, today.month
    result = []
    year, month = start_year, start_month
    while (year, month) <= (end_year, end_month):
        result.append((year, month))
        month += 1
        if month > 12:
            month = 1
            year += 1
    return result


def download_all_pending(**context) -> None:
    """Download every month that has new data or has never been ingested."""
    import io

    import pandas as pd
    import requests

    today = date.today()
    current_ym = f"{today.year}-{today.month:02d}"
    s3 = _get_s3_client()
    processed = []

    for year, month in _iter_yearmonths(START_YEAR, START_MONTH):
        yearmonth = f"{year}-{month:02d}"
        filename = f"DADOS_HIDROLOGICOS_HO_{year}_{month:02d}.parquet"
        url = f"{ONS_BASE_URL}/{DATASET}/{filename}"

        try:
            resp = requests.get(url, timeout=120, allow_redirects=True)
            resp.raise_for_status()
            parquet_bytes = resp.content

            if parquet_bytes[:4] != b"PAR1":
                print(f"Yearmonth {yearmonth}: non-Parquet response, skipping")
                continue

            df = pd.read_parquet(io.BytesIO(parquet_bytes), columns=["din_instante"])
            max_date = str(df["din_instante"].max().date())
            row_count = len(df)

            cp = _get_checkpoint_date(DATASET, DAG_ID, yearmonth)
            if cp is not None and str(cp) == max_date and yearmonth != current_ym:
                print(f"Yearmonth {yearmonth}: no new data (max_date={max_date}), skipping")
                continue

            raw_key = f"ons/{DATASET}/yearmonth={yearmonth}/{filename}"
            s3.put_object(
                Bucket="raw", Key=raw_key, Body=parquet_bytes,
                ContentType="application/octet-stream",
                Metadata={"source": "ons-opendata", "dataset": DATASET, "yearmonth": yearmonth},
            )
            _set_checkpoint_date(DATASET, DAG_ID, yearmonth, max_date, row_count)
            processed.append(yearmonth)
            print(f"Yearmonth {yearmonth}: {row_count:,} rows, max_date={max_date} → uploaded")

        except Exception as exc:
            ym_tuple = (year, month)
            cutoff = (today.year - 1, today.month)
            if ym_tuple >= cutoff:
                raise
            print(f"Yearmonth {yearmonth}: failed ({exc}), skipping historical month")

    context["task_instance"].xcom_push(key="processed_yearmonths", value=processed)
    print(f"Ingestion complete. Updated yearmonths: {processed}")


def _gate_trigger(**context) -> bool:
    if not context["dag_run"].conf.get("trigger_next", True):
        return False
    processed = context["task_instance"].xcom_pull(task_ids="download_all_pending", key="processed_yearmonths")
    return bool(processed)


def write_pipeline_audit(**context) -> None:
    processed = context["task_instance"].xcom_pull(task_ids="download_all_pending", key="processed_yearmonths") or []
    _write_audit(
        dag_id=context["dag"].dag_id,
        run_id=context["run_id"],
        layer="raw",
        status="success" if processed else "skipped",
        notes=f"dataset={DATASET} yearmonths={processed}",
    )


with DAG(
    dag_id=DAG_ID,
    description="[ONS] Multi-month incremental ingestion of dados_hidrologicos_ho",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["ons", "ingestion", "raw", "hidrologico", "reservatorio", "incremental"],
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
        trigger_dag_id="ons_hidrologicos_dag_02_bronze_transform",
        wait_for_completion=False,
    )

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    start >> download >> [audit, gate]
    gate >> trigger_bronze
    [audit, trigger_bronze] >> end
