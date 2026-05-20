"""
ONS DAG 03 - Silver Layer Transformation (Multi-year)

Processes all years where bronze checkpoint is newer than silver checkpoint.
Uses SparkSubmitHook to run one Spark job per pending year.
"""

from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

DATASET = "balanco_energia_subsistema_ho"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

DAG_ID = "ons_dag_03_silver_transform"
BRONZE_DAG_ID = "ons_dag_02_bronze_transform"

from lakehouse_utils import (
    SPARK_PACKAGES,
    spark_conf,
)
from lakehouse_utils import (
    get_checkpoint_date as _get_checkpoint_date,
)
from lakehouse_utils import (
    get_partitions_to_process as _get_partitions_to_process,
)
from lakehouse_utils import (
    set_checkpoint_date as _set_checkpoint_date,
)
from lakehouse_utils import (
    write_pipeline_audit as _write_audit,
)


def run_silver_transform(**context) -> None:
    from airflow.providers.apache.spark.hooks.spark_submit import SparkSubmitHook

    years = _get_partitions_to_process(DATASET, BRONZE_DAG_ID, DAG_ID)
    if not years:
        print("No years pending for silver transform")
        context["task_instance"].xcom_push(key="processed_years", value=[])
        return

    ds = context["ds"]
    for year in years:
        print(f"Running silver Spark job for year {year}")
        hook = SparkSubmitHook(
            conn_id="spark_default",
            conf=spark_conf(),
            packages=SPARK_PACKAGES,
            executor_memory="1g",
            driver_memory="1g",
            name=f"ons-silver-{year}-{ds}",
            application_args=["--year", str(year)],
        )
        hook.submit("/opt/airflow/dags/ons/spark_jobs/ons_silver_transform.py")

        bronze_cp = _get_checkpoint_date(DATASET, BRONZE_DAG_ID, year)
        _set_checkpoint_date(DATASET, DAG_ID, year, bronze_cp)
        print(f"Silver year {year}: checkpoint → {bronze_cp}")

    context["task_instance"].xcom_push(key="processed_years", value=years)
    print(f"Silver complete. Years: {years}")


def _gate_trigger(**context) -> bool:
    if not context["dag_run"].conf.get("trigger_next", True):
        return False
    processed = context["task_instance"].xcom_pull(task_ids="run_silver_transform", key="processed_years")
    return bool(processed)


def write_pipeline_audit(**context) -> None:
    processed = context["task_instance"].xcom_pull(task_ids="run_silver_transform", key="processed_years") or []
    _write_audit(
        dag_id=context["dag"].dag_id,
        run_id=context["run_id"],
        layer="silver",
        status="success" if processed else "skipped",
        notes=f"dataset={DATASET} years={processed}",
    )


with DAG(
    dag_id=DAG_ID,
    description="[ONS] Multi-year Bronze → Silver Iceberg via Spark",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["ons", "silver", "iceberg", "spark", "energia"],
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    silver = PythonOperator(
        task_id="run_silver_transform",
        python_callable=run_silver_transform,
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

    trigger_quality = TriggerDagRunOperator(
        task_id="trigger_quality_validation",
        trigger_dag_id="ons_dag_04_quality_validation",
        wait_for_completion=False,
    )

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    start >> silver >> [audit, gate]
    gate >> trigger_quality
    [audit, trigger_quality] >> end
