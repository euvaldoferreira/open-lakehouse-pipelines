"""
ONS DAG 04 - Data Quality Validation (Multi-year)

Runs quality checks on every year present in the silver checkpoint.

Checks performed (per year):
  1. completeness   — at least 3 600 rows (30 days × 5 subsystems × 24 h)
  2. null_check     — no nulls in id_subsistema or din_instante
  3. subsystem_set  — expected subsystems: SE, S, NE, N, SIN
  4. range_check    — val_carga between 0 and 200 000 MWmed
  5. temporal_order — no future timestamps
  6. duplicate_check — no duplicate (id_subsistema, din_instante) pairs
"""

from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

DATASET = "balanco_energia_subsistema_ho"
EXPECTED_SUBSYSTEMS = {"SE", "S", "NE", "N", "SIN"}
MIN_ROWS = 3_600
MAX_LOAD_MW = 200_000

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

DAG_ID = "ons_dag_04_quality_validation"
SILVER_DAG_ID = "ons_dag_03_silver_transform"

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

    # Run checks for every year that has been silver-processed
    years = _get_partitions_to_process(DATASET, SILVER_DAG_ID)
    if not years:
        context["task_instance"].xcom_push(key="overall_status", value="skipped")
        return {"overall_status": "skipped"}

    dag_id = context["dag"].dag_id
    any_issues = False

    for year in years:
        bronze_key = f"ons/{DATASET}/year={year}/data.parquet"
        s3 = _get_s3_client()
        run_date = str(_get_checkpoint_date(DATASET, SILVER_DAG_ID, year))

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
                             {"row_count": len(df), "min_expected": MIN_ROWS, "year": year})
            any_issues = True
        else:
            _write_dq_result(dag_id, run_date, "bronze", "completeness", "passed", 0,
                             {"row_count": len(df), "year": year})

        # 2. Null check
        null_id = int(df["id_subsistema"].isnull().sum())
        null_ts = int(df["din_instante"].isnull().sum())
        if null_id > 0 or null_ts > 0:
            _write_dq_result(dag_id, run_date, "bronze", "null_check", "failed",
                             null_id + null_ts,
                             {"null_id_subsistema": null_id, "null_din_instante": null_ts, "year": year})
            any_issues = True
        else:
            _write_dq_result(dag_id, run_date, "bronze", "null_check", "passed", 0, {"year": year})

        # 3. Subsystem set
        actual = set(df["id_subsistema"].dropna().str.upper().unique())
        unexpected = actual - EXPECTED_SUBSYSTEMS
        missing_subs = EXPECTED_SUBSYSTEMS - actual
        if unexpected or missing_subs:
            _write_dq_result(dag_id, run_date, "bronze", "subsystem_set", "warning",
                             len(unexpected) + len(missing_subs),
                             {"unexpected": list(unexpected), "missing": list(missing_subs), "year": year})
            any_issues = True
        else:
            _write_dq_result(dag_id, run_date, "bronze", "subsystem_set", "passed", 0,
                             {"subsystems": list(actual), "year": year})

        # 4. Range check
        out_of_range = int(((df["val_carga"] < 0) | (df["val_carga"] > MAX_LOAD_MW)).sum())
        if out_of_range > 0:
            _write_dq_result(dag_id, run_date, "bronze", "range_check", "warning", out_of_range,
                             {"out_of_range_rows": out_of_range, "year": year})
            any_issues = True
        else:
            _write_dq_result(dag_id, run_date, "bronze", "range_check", "passed", 0,
                             {"min_carga": float(df["val_carga"].min()),
                              "max_carga": float(df["val_carga"].max()), "year": year})

        # 5. Temporal order
        future_rows = int((df["din_instante"] > pd.Timestamp.now()).sum())
        if future_rows > 0:
            _write_dq_result(dag_id, run_date, "bronze", "temporal_order", "warning", future_rows,
                             {"future_timestamps": future_rows, "year": year})
            any_issues = True
        else:
            _write_dq_result(dag_id, run_date, "bronze", "temporal_order", "passed", 0, {"year": year})

        # 6. Duplicate check
        dup_count = int(df.duplicated(subset=["id_subsistema", "din_instante"]).sum())
        if dup_count > 0:
            _write_dq_result(dag_id, run_date, "bronze", "duplicate_check", "warning", dup_count,
                             {"duplicate_rows": dup_count, "year": year})
            any_issues = True
        else:
            _write_dq_result(dag_id, run_date, "bronze", "duplicate_check", "passed", 0, {"year": year})

        silver_cp = _get_checkpoint_date(DATASET, SILVER_DAG_ID, year)
        _set_checkpoint_date(DATASET, DAG_ID, year, silver_cp)
        print(f"Quality checks year {year}: {'issues found' if any_issues else 'passed'}")

    overall = "warning" if any_issues else "passed"
    context["task_instance"].xcom_push(key="overall_status", value=overall)
    context["task_instance"].xcom_push(key="checked_years", value=years)
    print(f"Quality validation: {overall} for years {years}")
    return {"overall_status": overall}


def route_on_quality(**context) -> str:
    status = context["task_instance"].xcom_pull(task_ids="run_quality_checks", key="overall_status")
    return "quality_passed" if status in ("passed", "skipped") else "quality_warning"


def handle_quality_warning(**context) -> None:
    years = context["task_instance"].xcom_pull(task_ids="run_quality_checks", key="checked_years")
    print(f"WARNING: Data quality issues found for ONS {DATASET} years {years}. "
          f"Check lakehouse.dq_results for details.")


def _gate_trigger(**context) -> bool:
    status = context["task_instance"].xcom_pull(task_ids="run_quality_checks", key="overall_status")
    if not context["dag_run"].conf.get("trigger_next", True):
        return False
    return status in ("passed", "warning")


with DAG(
    dag_id=DAG_ID,
    description="[ONS] Multi-year data quality validation",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["ons", "quality", "dq", "energia"],
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
        trigger_dag_id="ons_dag_05_gold_processing",
        wait_for_completion=False,
    )

    final = EmptyOperator(task_id="final", trigger_rule="none_failed_min_one_success")

    start >> check >> branch >> [quality_passed, quality_warning] >> end >> gate >> trigger_gold >> final
