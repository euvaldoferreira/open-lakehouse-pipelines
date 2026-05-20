import pytest
from airflow.exceptions import AirflowException

from operators.papermill_api_operator import PapermillApiOperator


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_op(**kwargs) -> PapermillApiOperator:
    defaults = dict(
        task_id="test_task",
        notebook="test.ipynb",
        runner_url="http://runner:8000",
        poll_interval=1,
        timeout=3,
    )
    defaults.update(kwargs)
    return PapermillApiOperator(**defaults)


JOB_RUNNING = {
    "job_id": "abc-123",
    "status": "running",
    "notebook": "test.ipynb",
    "parameters": {},
    "output": None,
    "error": None,
    "started_at": "2024-01-01T00:00:00",
    "finished_at": None,
}

JOB_SUCCESS = {**JOB_RUNNING, "status": "success", "output": "/output/test.ipynb"}
JOB_FAILED  = {**JOB_RUNNING, "status": "failed",  "error": "Kernel died"}


# ---------------------------------------------------------------------------
# Inicialização
# ---------------------------------------------------------------------------

def test_init_attributes():
    op = make_op(parameters={"date": "2024-01-01"})
    assert op.notebook == "test.ipynb"
    assert op.parameters == {"date": "2024-01-01"}
    assert op.poll_interval == 1
    assert op.timeout == 3


def test_init_default_parameters():
    op = make_op()
    assert op.parameters == {}


def test_init_strips_trailing_slash():
    op = make_op(runner_url="http://runner:8000/")
    assert op.runner_url == "http://runner:8000"


# ---------------------------------------------------------------------------
# Execução com sucesso
# ---------------------------------------------------------------------------

def test_execute_success(requests_mock):
    op = make_op()
    requests_mock.post("http://runner:8000/run", json={**JOB_RUNNING, "job_id": "abc-123"})
    requests_mock.get("http://runner:8000/status/abc-123", json=JOB_SUCCESS)

    result = op.execute({})

    assert result == "/output/test.ipynb"
    assert requests_mock.call_count == 2


def test_execute_passes_parameters(requests_mock):
    op = make_op(parameters={"date": "2024-06-01", "bucket": "bronze"})
    requests_mock.post("http://runner:8000/run", json={**JOB_RUNNING, "job_id": "abc-123"})
    requests_mock.get("http://runner:8000/status/abc-123", json=JOB_SUCCESS)

    op.execute({})

    post_body = requests_mock.request_history[0].json()
    assert post_body["parameters"] == {"date": "2024-06-01", "bucket": "bronze"}


# ---------------------------------------------------------------------------
# Falha na execução do notebook
# ---------------------------------------------------------------------------

def test_execute_failure_raises(requests_mock):
    op = make_op()
    requests_mock.post("http://runner:8000/run", json={**JOB_RUNNING, "job_id": "abc-456"})
    requests_mock.get("http://runner:8000/status/abc-456", json={**JOB_FAILED, "job_id": "abc-456"})

    with pytest.raises(AirflowException, match="Notebook execution failed"):
        op.execute({})


def test_execute_failure_includes_error_message(requests_mock):
    op = make_op()
    requests_mock.post("http://runner:8000/run", json={**JOB_RUNNING, "job_id": "abc-456"})
    requests_mock.get("http://runner:8000/status/abc-456", json={**JOB_FAILED, "job_id": "abc-456"})

    with pytest.raises(AirflowException, match="Kernel died"):
        op.execute({})


# ---------------------------------------------------------------------------
# Timeout
# ---------------------------------------------------------------------------

def test_execute_timeout_raises(requests_mock):
    op = make_op(timeout=2, poll_interval=1)
    requests_mock.post("http://runner:8000/run", json={**JOB_RUNNING, "job_id": "abc-789"})
    requests_mock.get("http://runner:8000/status/abc-789", json={**JOB_RUNNING, "job_id": "abc-789"})

    with pytest.raises(AirflowException, match="timed out"):
        op.execute({})


# ---------------------------------------------------------------------------
# Erros HTTP do runner
# ---------------------------------------------------------------------------

def test_execute_http_error_on_submit(requests_mock):
    op = make_op()
    requests_mock.post("http://runner:8000/run", status_code=404)

    with pytest.raises(Exception):
        op.execute({})


def test_execute_http_error_on_poll(requests_mock):
    op = make_op()
    requests_mock.post("http://runner:8000/run", json={**JOB_RUNNING, "job_id": "abc-999"})
    requests_mock.get("http://runner:8000/status/abc-999", status_code=500)

    with pytest.raises(Exception):
        op.execute({})
