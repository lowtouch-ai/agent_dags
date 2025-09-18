from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from typing import cast
from airflow.models.xcom_arg import XComArg

def return_date(**kwargs):
    return "20250916"

with DAG(
    dag_id="xcomarg_error_dag",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # Task that returns a date string
    get_date_str_task = PythonOperator(
        task_id="get_date_str",
        python_callable=return_date,
    )

    #  Example 1: f-string with cast + XComArg
    echo_mv_task = BashOperator(
        task_id="echo_mv",
        bash_command=f"echo IndexReturns_MV_{cast(str, XComArg(get_date_str_task, key='return_value'))}",
    )

    #  Example 2: dict param with cast + XComArg
    echo_param_task = BashOperator(
        task_id="echo_param",
        bash_command=(
            "echo file_name={{ params.file_name }} && "
            "echo folder_path={{ params.folder_path }}"
        ),
        params={
            "file_name": cast(str, XComArg(get_date_str_task, key="return_value")),
            "folder_path": "rimes/idx",
        },
    )

    get_date_str_task >> [echo_mv_task, echo_param_task]
