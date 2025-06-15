import pytest
from airflow.models import DagBag

def test_no_import_errors():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    assert len(dag_bag.import_errors) == 0, \
        f"Import errors found: {dag_bag.import_errors}"

def test_dag_loaded():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    assert "clickstream_etl" in dag_bag.dags
    dag = dag_bag.get_dag("clickstream_etl")
    assert dag is not None

def test_task_count():
    dag = DagBag(dag_folder="dags", include_examples=False).get_dag("clickstream_etl")
    expected_tasks = {"gcs_to_bq", "transform"}
    actual = set(task.task_id for task in dag.tasks)
    assert expected_tasks.issubset(actual), \
        f"Missing tasks: {expected_tasks - actual}"

def test_default_args():
    from clickstream_etl import dag as clickstream_dag
    defaults = clickstream_dag.default_args
    assert defaults.get("retries", 0) >= 1
    assert "retry_delay" in defaults
