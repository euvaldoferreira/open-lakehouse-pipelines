"""
PapermillApiOperator
Submits a parametrized notebook to the notebook-runner HTTP API,
then polls until execution completes or fails.
"""

from __future__ import annotations

import time
from typing import Any

import requests
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator


class PapermillApiOperator(BaseOperator):
    """
    Execute a Jupyter notebook via the notebook-runner REST API.

    :param notebook: Relative path of the notebook inside the runner's input dir.
                     Example: ``"02_bronze_analysis_parametrized.ipynb"``
    :param parameters: Dict of parameters to inject. String values are templated.
    :param runner_url: Base URL of the notebook-runner service.
    :param poll_interval: Seconds between status checks.
    :param timeout: Max seconds to wait before raising a timeout error.
    """

    template_fields = ("parameters",)

    def __init__(
        self,
        *,
        notebook: str,
        parameters: dict[str, Any] | None = None,
        runner_url: str = "http://notebook-runner:8000",
        poll_interval: int = 15,
        timeout: int = 3600,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.notebook = notebook
        self.parameters = parameters or {}
        self.runner_url = runner_url.rstrip("/")
        self.poll_interval = poll_interval
        self.timeout = timeout

    def execute(self, context: dict) -> str:
        payload = {
            "notebook": self.notebook,
            "parameters": self.parameters,
        }

        self.log.info("Submitting notebook '%s' with parameters: %s", self.notebook, self.parameters)

        resp = requests.post(f"{self.runner_url}/run", json=payload, timeout=30)
        resp.raise_for_status()
        job = resp.json()
        job_id = job["job_id"]
        self.log.info("Job submitted: %s", job_id)

        elapsed = 0
        while elapsed < self.timeout:
            time.sleep(self.poll_interval)
            elapsed += self.poll_interval

            status_resp = requests.get(f"{self.runner_url}/status/{job_id}", timeout=10)
            status_resp.raise_for_status()
            job = status_resp.json()
            self.log.info("[%ds] Job %s status: %s", elapsed, job_id, job["status"])

            if job["status"] == "success":
                self.log.info("Notebook executed successfully. Output: %s", job.get("output"))
                return job.get("output", "")

            if job["status"] == "failed":
                raise AirflowException(
                    f"Notebook execution failed.\nNotebook: {self.notebook}\nError: {job.get('error')}"
                )

        raise AirflowException(f"Notebook job {job_id} timed out after {self.timeout}s")
