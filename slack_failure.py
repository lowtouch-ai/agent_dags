from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
import logging

logger = logging.getLogger(__name__)

def slack_failure_alert(context):
    """Send a Slack alert when a DAG fails, including details of the failed task."""
    dag_id = context['dag'].dag_id
    task_id = context.get('task_instance').task_id if context.get('task_instance') else "N/A"
    execution_date = context.get('execution_date')
    log_url = context.get('task_instance').log_url if context.get('task_instance') else "No log available"
    exception = str(context.get('exception', 'No exception details provided'))

    slack_msg = (
        f":red_circle: *DAG Failure Alert*\n"
        f"*DAG*: {dag_id}\n"
        f"*Task*: {task_id}\n"
        f"*Execution Date*: {execution_date}\n"
        f"*Exception*: {exception}\n"
        f"*Log URL*: {log_url}"
    )

    try:
        slack_hook = SlackWebhookHook(slack_webhook_conn_id='slack_webhook')
        slack_hook.send(text=slack_msg)
        logger.info("Slack alert sent successfully for DAG %s, Task %s", dag_id, task_id)
    except Exception as e:
        logger.error("Failed to send Slack alert: %s", str(e))

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
