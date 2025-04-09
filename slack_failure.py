from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from agent_dags.slack_utils import slack_failure_alert
import logging

logger = logging.getLogger(__name__)

default_args = {
    'on_failure_callback': slack_failure_alert,  # Place it here
}

# Define the DAG
with DAG(
    dag_id='slack_alert',
    start_date=datetime(2025, 4, 1),
    schedule_interval=None,  # Manual trigger for testing
    catchup=False,
    default_args=default_args,
) as dag:
    # Task that will fail
    fail_task = BashOperator(
        task_id='fail_task',
        bash_command='exit 1',  # This will cause the task (and thus the DAG) to fail
    )
