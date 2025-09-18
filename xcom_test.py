from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models.xcom_arg import XComArg
from typing import cast

def return_date_str():
    return "20250916"

def use_bad_xcomarg(**context):
    # ❌ Returning a dict with an XComArg (not serializable)
    return {
        "file_name": cast(str, XComArg(context["dag"].get_task("get_date_str"), key="return_value")),
        "folder_path": "rimes/idx",
    }

with DAG(
    dag_id="xcomarg_json_error_repro",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    get_date_str_task = PythonOperator(
        task_id="get_date_str",
        python_callable=return_date_str,
    )

    bad_task = PythonOperator(
        task_id="bad_task",
        python_callable=use_bad_xcomarg,
        provide_context=True,
    )

    get_date_str_task >> bad_task
