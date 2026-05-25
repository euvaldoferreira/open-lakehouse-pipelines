"""
ONS DAG 04 - Data Quality Validation (Multi-month)

Runs quality checks on every month present in the silver checkpoint.

Checks performed (per month):
  1. completeness    — at least 50 000 rows (~100 reservoirs × 30 days × 24 h)
  2. null_check      — no nulls in id_reservatorio or din_instante
  3. volume_range    — val_volumeutil between 0 and 105 (% — slight overflow allowed)
  4. flow_range      — val_vazaoafluente >= 0 (non-negative flow)
  5. temporal_order  — no future timestamps
  6. duplicate_check — no duplicate (id_reservatorio, din_instante) pairs
"""

from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

DATASET = "dados_hidrologicos_ho"
MIN_ROWS = 50_000
MAX_VOLUME_PCT = 105.0

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

DAG_ID = "ons_hidrologicos_dag_04_quality_validation"
SILVER_DAG_ID = "ons_hidrologicos_dag_03_silver_transform"

from lakehouse_utils import (
    get_checkpoint_date as _get_checkpoint_date,
)
from lakehouse_utils import (
    get_partitions_to_process as _get_partitions_to_process,
)
from lakehouse_utils import (
    get_s3_client as _get_s3_client,
)
from lakehouse_utils import (
    set_checkpoint_date as _set_checkpoint_date,
)
from lakehouse_utils import (
    write_dq_result as _write_dq_result,
)


def run_quality_checks(**context) -> dict:
    import io

    import pandas as pd

    yearmonths = _get_partitions_to_process(DATASET, SILVER_DAG_ID)
    if not yearmonths:
        context["task_instance"].xcom_push(key="overall_status", value="skipped")
        return {"overall_status": "skipped"}

    dag_id = context["dag"].dag_id
    any_issues = False

    for yearmonth in yearmonths:
        bronze_key = f"ons/{DATASET}/yearmonth={yearmonth}/data.parquet"
        s3 = _get_s3_client()
        run_date = str(_get_checkpoint_date(DATASET, SILVER_DAG_ID, yearmonth))

        try:
            resp = s3.get_object(Bucket="bronze", Key=bronze_key)
            df = pd.read_parquet(io.BytesIO(resp["Body"].read()))
        except Exception as exc:
            _write_dq_result(dag_id, run_date, "bronze", "file_exists", "failed", 1, {"error": str(exc)})
            any_issues = True
            continue

        # 1. Completeness
        if len(df) < MIN_ROWS:
            _write_dq_result(dag_id, run_date, "bronze", "completeness", "failed", 1,
                             {"row_count": len(df), "min_expected": MIN_ROWS, "yearmonth": yearmonth})
            any_issues = True
        else:
            _write_dq_result(dag_id, run_date, "bronze", "completeness", "passed", 0,
                             {"row_count": len(df), "yearmonth": yearmonth})

        # 2. Null check
        null_id = int(df["id_reservatorio"].isnull().sum())
        null_ts = int(df["din_instante"].isnull().sum())
        if null_id > 0 or null_ts > 0:
            _write_dq_result(dag_id, run_date, "bronze", "null_check", "failed",
                             null_id + null_ts,
                             {"null_id_reservatorio": null_id, "null_din_instante": null_ts,
                              "yearmonth": yearmonth})
            any_issues = True
        else:
            _write_dq_result(dag_id, run_date, "bronze", "null_check", "passed", 0,
                             {"yearmonth": yearmonth})

        # 3. Volume range (0–105 %)
        df["val_volumeutil"] = pd.to_numeric(df["val_volumeutil"], errors="coerce")
        vol_series = df["val_volumeutil"].dropna()
        out_of_range = int(((vol_series < 0) | (vol_series > MAX_VOLUME_PCT)).sum())
        if out_of_range > 0:
            _write_dq_result(dag_id, run_date, "bronze", "volume_range", "warning", out_of_range,
                             {"out_of_range_rows": out_of_range, "yearmonth": yearmonth})
            any_issues = True
        else:
            _write_dq_result(dag_id, run_date, "bronze", "volume_range", "passed", 0,
                             {"min_volume": float(vol_series.min()),
                              "max_volume": float(vol_series.max()), "yearmonth": yearmonth})

        # 4. Flow range (non-negative)
        df["val_vazaoafluente"] = pd.to_numeric(df["val_vazaoafluente"], errors="coerce")
        neg_flow = int((df["val_vazaoafluente"] < 0).sum())
        if neg_flow > 0:
            _write_dq_result(dag_id, run_date, "bronze", "flow_range", "warning", neg_flow,
                             {"negative_flow_rows": neg_flow, "yearmonth": yearmonth})
            any_issues = True
        else:
            _write_dq_result(dag_id, run_date, "bronze", "flow_range", "passed", 0,
                             {"yearmonth": yearmonth})

        # 5. Temporal order
        future_rows = int((df["din_instante"] > pd.Timestamp.now()).sum())
        if future_rows > 0:
            _write_dq_result(dag_id, run_date, "bronze", "temporal_order", "warning", future_rows,
                             {"future_timestamps": future_rows, "yearmonth": yearmonth})
            any_issues = True
        else:
            _write_dq_result(dag_id, run_date, "bronze", "temporal_order", "passed", 0,
                             {"yearmonth": yearmonth})

        # 6. Duplicate check
        dup_count = int(df.duplicated(subset=["id_reservatorio", "din_instante"]).sum())
        if dup_count > 0:
            _write_dq_result(dag_id, run_date, "bronze", "duplicate_check", "warning", dup_count,
                             {"duplicate_rows": dup_count, "yearmonth": yearmonth})
            any_issues = True
        else:
            _write_dq_result(dag_id, run_date, "bronze", "duplicate_check", "passed", 0,
                             {"yearmonth": yearmonth})

        silver_cp = _get_checkpoint_date(DATASET, SILVER_DAG_ID, yearmonth)
        _set_checkpoint_date(DATASET, DAG_ID, yearmonth, silver_cp)
        print(f"Quality checks yearmonth {yearmonth}: {'issues found' if any_issues else 'passed'}")

    overall = "warning" if any_issues else "passed"
    context["task_instance"].xcom_push(key="overall_status", value=overall)
    context["task_instance"].xcom_push(key="checked_yearmonths", value=yearmonths)
    print(f"Quality validation: {overall} for yearmonths {yearmonths}")
    return {"overall_status": overall}


def route_on_quality(**context) -> str:
    status = context["task_instance"].xcom_pull(task_ids="run_quality_checks", key="overall_status")
    return "quality_passed" if status in ("passed", "skipped") else "quality_warning"


def handle_quality_warning(**context) -> None:
    yearmonths = context["task_instance"].xcom_pull(task_ids="run_quality_checks", key="checked_yearmonths")
    print(f"WARNING: Data quality issues found for ONS {DATASET} yearmonths {yearmonths}. "
          f"Check lakehouse.dq_results for details.")


def _gate_trigger(**context) -> bool:
    status = context["task_instance"].xcom_pull(task_ids="run_quality_checks", key="overall_status")
    if not context["dag_run"].conf.get("trigger_next", True):
        return False
    return status in ("passed", "warning")


with DAG(
    dag_id=DAG_ID,
    description="[ONS] Multi-month data quality validation (dados_hidrologicos_ho)",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["ons", "quality", "dq", "hidrologico", "reservatorio"],
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    check = PythonOperator(
        task_id="run_quality_checks",
        python_callable=run_quality_checks,
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

    gate = ShortCircuitOperator(
        task_id="gate_trigger",
        python_callable=_gate_trigger,
    )

    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold_processing",
        trigger_dag_id="ons_hidrologicos_dag_05_gold_processing",
        wait_for_completion=False,
    )

    final = EmptyOperator(task_id="final", trigger_rule="none_failed_min_one_success")

    start >> check >> branch >> [quality_passed, quality_warning] >> end >> gate >> trigger_gold >> final
