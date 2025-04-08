from airflow import DAG
from default_dag_args import default_args
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    'slack_alert',
    default_args=default_args,
    description='A simple DAG with Slack alerts',
    schedule_interval='@daily',
    start_date=datetime(2025, 4, 1),
    catchup=False,
) as dag:
    task = BashOperator(
        task_id='fail_task',
        bash_command='exit 1',  # This will fail and trigger the alert
    )
