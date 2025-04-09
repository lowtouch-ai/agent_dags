from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define the DAG
with DAG(
    dag_id='test_slack_alert',
    start_date=datetime(2025, 4, 1),
    schedule_interval=None,  # Manual trigger for testing
    catchup=False,
) as dag:
    # Task that will fail
    fail_task = BashOperator(
        task_id='fail_task',
        bash_command='exit 1',  # This will cause the task (and thus the DAG) to fail
    )
