from slack_alerts import slack_failure_alert

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'on_failure_callback': slack_failure_callback,  # Note: Typo fixed to slack_failure_alert
}
