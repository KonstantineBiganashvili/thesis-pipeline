import sys
import os
import pytest
from airflow.models import DagBag

sys.path.insert(0, os.path.join(os.getcwd(), "dags"))


def test_no_import_errors():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    assert len(dag_bag.import_errors) == 0, (
        f"Import errors found in DAGs: {dag_bag.import_errors}"
    )

def test_dag_loaded():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    assert "clickstream_etl" in dag_bag.dags, "clickstream_etl DAG not found"

def test_task_count():
    dag = DagBag(dag_folder="dags", include_examples=False).get_dag("clickstream_etl")
    expected = {"gcs_to_bq", "transform"}
    actual = {task.task_id for task in dag.tasks}
    missing = expected - actual
    assert not missing, f"Missing tasks in clickstream_etl DAG: {missing}"

def test_default_args():
    import clickstream_etl
    dag = clickstream_etl.dag
    defaults = dag.default_args
    assert defaults.get("retries", 0) >= 1, "Default 'retries' should be at least 1"
    assert "retry_delay" in defaults, "Default 'retry_delay' is missing"

def test_schedule_interval():
    import clickstream_etl
    dag = clickstream_etl.dag
    assert dag.schedule_interval == "@hourly", (
        f"Expected schedule_interval '@hourly', got {dag.schedule_interval}"
    )