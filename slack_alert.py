from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define the DAG
with DAG(
    dag_id='slack_alert',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    def fail_task():
        raise ValueError("This task is designed to fail!")

    # Create a failing task
    failing_task = PythonOperator(
        task_id='fail_task',
        python_callable=fail_task,
    )
