from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models.xcom_arg import XComArg
from typing import cast

def return_date_str():
    return "20250916"

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

    # ✅ This runs fine but produces invalid XCom JSON in UI
    bad_task = EmptyOperator(
        task_id="bad_task",
        params={
            "file_name": cast(str, XComArg(get_date_str_task, key="return_value")),
            "folder_path": "rimes/idx",
        },
    )

    get_date_str_task >> bad_task
