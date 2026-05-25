"""
ONS DAG 05 - Gold Layer Processing (Multi-month)

Processes all months where silver checkpoint is newer than gold checkpoint.
Uses SparkSubmitHook to run one Spark job per pending month.
"""

from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

DATASET = "dados_hidrologicos_ho"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

DAG_ID = "ons_hidrologicos_dag_05_gold_processing"
SILVER_DAG_ID = "ons_hidrologicos_dag_03_silver_transform"

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


def run_gold_processing(**context) -> None:
    from airflow.providers.apache.spark.hooks.spark_submit import SparkSubmitHook

    yearmonths = _get_partitions_to_process(DATASET, SILVER_DAG_ID, DAG_ID)
    if not yearmonths:
        print("No yearmonths pending for gold processing")
        context["task_instance"].xcom_push(key="processed_yearmonths", value=[])
        return

    ds = context["ds"]
    for yearmonth in yearmonths:
        print(f"Running gold Spark job for yearmonth {yearmonth}")
        hook = SparkSubmitHook(
            conn_id="spark_default",
            conf=spark_conf(),
            packages=SPARK_PACKAGES,
            executor_memory="1g",
            driver_memory="1g",
            name=f"ons-hidrologicos-gold-{yearmonth}-{ds}",
            application_args=["--yearmonth", yearmonth],
        )
        hook.submit("/opt/airflow/dags/ons/dados_hidrologicos_ho/spark_jobs/ons_hidrologicos_gold_processing.py")

        silver_cp = _get_checkpoint_date(DATASET, SILVER_DAG_ID, yearmonth)
        _set_checkpoint_date(DATASET, DAG_ID, yearmonth, silver_cp)
        print(f"Gold yearmonth {yearmonth}: checkpoint → {silver_cp}")

    context["task_instance"].xcom_push(key="processed_yearmonths", value=yearmonths)
    print(f"Gold complete. Yearmonths: {yearmonths}")


def write_pipeline_audit(**context) -> None:
    processed = context["task_instance"].xcom_pull(task_ids="run_gold_processing", key="processed_yearmonths") or []
    _write_audit(
        dag_id=context["dag"].dag_id,
        run_id=context["run_id"],
        layer="gold",
        status="success" if processed else "skipped",
        notes=f"dataset={DATASET} yearmonths={processed}",
    )


with DAG(
    dag_id=DAG_ID,
    description="[ONS] Multi-month Silver → Gold daily aggregations via Spark (dados_hidrologicos_ho)",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["ons", "gold", "spark", "iceberg", "hidrologico", "reservatorio"],
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    gold = PythonOperator(
        task_id="run_gold_processing",
        python_callable=run_gold_processing,
    )

    audit = PythonOperator(
        task_id="write_pipeline_audit",
        python_callable=write_pipeline_audit,
        trigger_rule="all_done",
    )

    end = EmptyOperator(task_id="end")

    start >> gold >> audit >> end
