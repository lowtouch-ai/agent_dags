from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'lowtouch.ai_developers',
    'start_date': datetime(2025, 4, 4),
    'retries': 0,  # Ensure no retries mask the failure
}

with DAG(
    dag_id='test_slack_alert',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    def fail_task():
        raise ValueError("Intentional failure for testing!")

    task1 = PythonOperator(
        task_id='fail_task',
        python_callable=fail_task,
    )
