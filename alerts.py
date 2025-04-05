# alerts.py
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
