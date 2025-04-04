from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

def slack_failure_alert(context):
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

    slack_hook = SlackWebhookHook(slack_webhook_conn_id='slack_webhook')
    slack_hook.send(text=slack_msg)

default_args = {
    'owner': 'lowtouch.ai_developers',
    'start_date': datetime(2025, 4, 4),
    'on_failure_callback': slack_failure_alert,
}

with DAG(
    dag_id='test_slack_alert',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    def fail_task():
        raise ValueError("This task is designed to fail for testing!")

    task1 = PythonOperator(
        task_id='fail_task',
        python_callable=fail_task,
    )
