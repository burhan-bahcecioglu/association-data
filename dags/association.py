"""
Weekly Association DAG
"""
from airflow import DAG
from airflow.operators.empty import EmptyOperator

from association_utils import (
    DEFAULT_ARGS, calculate_run_date, create_databricks_task
)

dag = DAG(
    dag_id="association",
    default_args=DEFAULT_ARGS,
    schedule="0 6 * * *",
    tags=["fozzy", "association"]
)
start = EmptyOperator(task_id="association_start", dag=dag)
end = EmptyOperator(task_id="association_end", dag=dag)
etl_start = EmptyOperator(task_id="etl_start", dag=dag)
etl_end = EmptyOperator(task_id="etl_end", dag=dag)
model_start = EmptyOperator(task_id="model_start", dag=dag)
model_end = EmptyOperator(task_id="model_end", dag=dag)
result_delivery_start = EmptyOperator(task_id="result_delivery_start", dag=dag)
result_delivery_end = EmptyOperator(task_id="result_delivery_end", dag=dag)
etl_product_translation = create_databricks_task(
    task_id="etl_product_translation",
    module="association.etl.product_translation",
    dag=dag
)
etl_store = create_databricks_task(
    task_id="etl_store",
    module="association.etl.store",
    dag=dag
)
etl_transactional_sales = create_databricks_task(
    task_id="etl_transactional_sales",
    module="association.etl.transactional_sales",
    dag=dag
)
etl_product = create_databricks_task(
    task_id="etl_product",
    module="association.etl.product",
    dag=dag
)
etl_store_translation = create_databricks_task(
    task_id="etl_store_translation",
    module="association.etl.store_translation",
    dag=dag
)
model_fp_growth = create_databricks_task(
    task_id="model_fp_growth",
    module="association.modelling.fp_growth",
    dag=dag
)

result_delivery_database = create_databricks_task(
    task_id="result_delivery_database",
    module="association.result_delivery.database",
    dag=dag
)
result_delivery_object_storage = create_databricks_task(
    task_id="result_delivery_object_storage",
    module="association.result_delivery.object_storage",
    dag=dag
)

(
    start >> calculate_run_date.override(dag=dag)(add_hours=-21)
    >> etl_start >> [
        etl_product_translation,
        etl_store,
        etl_transactional_sales,
    ],
    etl_product_translation >> etl_product,
    etl_store >> etl_store_translation,
    [
        etl_transactional_sales,
        etl_store_translation,
        etl_product
    ] >> etl_end >> model_start >> model_fp_growth >> model_end >>
    result_delivery_start >> [
        result_delivery_database,
        result_delivery_object_storage,
    ] >> result_delivery_end
    >> end
)
