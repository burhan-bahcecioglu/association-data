"""
Association Bootstrap DAG
"""
from airflow import DAG
from airflow.operators.empty import EmptyOperator

from association_utils import (
    DEFAULT_ARGS, calculate_run_date, create_databricks_task
)

dag = DAG(
    dag_id="association_bootstrap",
    default_args=DEFAULT_ARGS,
    schedule=None,
    tags=["fozzy", "association"]
)
start = EmptyOperator(task_id="association_start", dag=dag)
end = EmptyOperator(task_id="association_end", dag=dag)
etl_product = create_databricks_task(
    task_id="etl_product",
    module="association.etl.product",
    dag=dag
)
etl_product_translation = create_databricks_task(
    task_id="etl_bootstrap_product_translation",
    module="association.etl.bootstrap.product_translation",
    dag=dag
)

(
    start >> calculate_run_date.override(dag=dag)(add_hours=-21)
    >> etl_product >> etl_product_translation >> end
)
