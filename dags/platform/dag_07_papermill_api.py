"""
DAG 07 - Papermill via REST API (Option 2)
Airflow calls the notebook-runner HTTP API to execute a parametrized notebook.
Execution environment: notebook-runner container (dedicated, isolated).
Airflow role: orchestration only.

Setup required (Airflow UI → Admin → Connections):
  Connection ID : notebook_runner
  Conn Type     : HTTP
  Host          : http://notebook-runner
  Port          : 8000

No SSH or shared filesystem needed — fully decoupled via HTTP.
"""

from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Import custom operator from plugins
from operators.papermill_api_operator import PapermillApiOperator

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def log_result(**context) -> None:
    output_path = context["task_instance"].xcom_pull(task_ids="run_notebook_via_api")
    print(f"Notebook executed successfully. Output saved at: {output_path}")


with DAG(
    dag_id="dag_07_papermill_api",
    description="[Option 2] Execute notebook in notebook-runner container via HTTP API",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["papermill", "api", "notebook", "option-2"],
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    run_notebook = PapermillApiOperator(
        task_id="run_notebook_via_api",
        notebook="02_bronze_analysis_parametrized.ipynb",
        parameters={
            "execution_date": "{{ ds }}",
            "bucket": "bronze",
        },
        runner_url="http://notebook-runner:8000",
        poll_interval=15,
        timeout=3600,
    )

    log = PythonOperator(
        task_id="log_result",
        python_callable=log_result,
    )

    end = EmptyOperator(task_id="end")

    start >> run_notebook >> log >> end
