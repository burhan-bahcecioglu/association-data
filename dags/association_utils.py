"""Association Orchestration Utilities"""
from datetime import datetime, timedelta
from typing import Optional, List

from airflow import DAG
from airflow.decorators import task
from airflow.providers.databricks.operators.databricks import (
    DatabricksSubmitRunOperator
)


DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "depends_on_past": False,
    "catchup": False,
    "default_view": "graph"
}

RUN_DATE_XCOM = "{{ ti.xcom_pull(task_ids='calculate_run_date') }}"
RUN_DATE_PARAM = [f"--run-date={RUN_DATE_XCOM}"]


@task
def calculate_run_date(add_hours: int = 0, **kwargs) -> str:
    """
    Get or calculate run date, get it from dag_run configuration if passed,
    otherwise calculate it.
    """
    run_date = kwargs.get("dag_run").conf.get("run_date")
    if not run_date:
        run_date_dt = datetime.now() + timedelta(hours=add_hours)
        run_date = run_date_dt.strftime("%Y-%m-%d")
    return run_date


def create_databricks_task(
    task_id: str, module: str, dag: DAG,
    params: Optional[List[str]] = None, **kwargs
):
    """Creates an association specific databricks task."""
    if params:
        params = params + RUN_DATE_PARAM
    else:
        params = RUN_DATE_PARAM

    spark_python_task = {
        "python_file": "dbfs:/FileStore/burhan/src/module_runner.py",
        "parameters": [module] + params,
    }
    dbx_task = DatabricksSubmitRunOperator(
        task_id=task_id,
        existing_cluster_id="0320-221349-8pmg3tcy",
        spark_python_task=spark_python_task,
        run_name=f"{dag.dag_id} - {task_id}",
        databricks_conn_id="databricks_fozzy",
        **kwargs,
        dag=dag
    )
    return dbx_task
