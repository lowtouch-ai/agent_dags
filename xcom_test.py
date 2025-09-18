from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.models.xcom_arg import XComArg

def push_date(**kwargs):
    # Simulate your get_last_tcw_business_day_for_vendor_file
    return "20250916"

with DAG(
    dag_id="xcom_json_issue_demo",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # Task 1: Push value into XCom
    t1 = PythonOperator(
        task_id="get_date",
        python_callable=push_date,
    )

    # ❌ This is the problematic task (works in 2.7, breaks in 2.10)
    # Because it tries to JSON-serialize XComArg directly
    t2 = BashOperator(
        task_id="use_date_bad",
        bash_command=f"echo {XComArg(t1)}"
    )

    # ✅ This is the fixed version (works in 2.10)
    # Using Jinja templating to resolve XCom at runtime
    t3 = BashOperator(
        task_id="use_date_fixed",
        bash_command="echo {{ ti.xcom_pull(task_ids='get_date') }}"
    )

    t1 >> [t2, t3]
