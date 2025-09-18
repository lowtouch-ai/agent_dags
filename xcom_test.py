from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from typing import cast
from airflow.models.xcom_arg import XComArg

def return_date(**kwargs):
    return "20250916"

def return_with_xcomarg(**kwargs):
    #  Returns dict containing XComArg, not JSON serializable
    get_date_task = kwargs["dag"].get_task("get_date_str")
    return {
        "file_name": cast(str, XComArg(get_date_task, key="return_value")),
        "folder_path": "rimes/idx",
    }

with DAG(
    dag_id="xcomarg_error_dag_repro",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    get_date_str_task = PythonOperator(
        task_id="get_date_str",
        python_callable=return_date,
    )

    bad_task = PythonOperator(
        task_id="bad_task",
        python_callable=return_with_xcomarg,
        provide_context=True,
    )

    get_date_str_task >> bad_task
