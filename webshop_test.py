from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pendulum
import requests
import os

# Set IST timezone
ist = pendulum.timezone("Asia/Kolkata")

# Fetch Airflow variables
api_token = Variable.get("API_TOKEN")
api_url = Variable.get("API_URL")
slack_webhook = Variable.get("SLACK_WEBHOOK_URL")  # add webhook as Airflow Variable

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 12, tzinfo=ist),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def slack_alert(**context):
    """Send Slack alert if mvn test failed"""
    if os.path.exists("/tmp/mvn_failed.flag"):
        msg = f":x: Maven tests failed in DAG *{context['dag'].dag_id}*, Task *{context['task'].task_id}*"
        requests.post(slack_webhook, json={"text": msg})
    else:
        print("No Maven failures detected, skipping Slack alert.")

with DAG(
    dag_id='webshop_run_chatapi_automation',
    default_args=default_args,
    schedule_interval='30 14 * * *',  # 14:30 IST
    catchup=False,
    tags=['maven', 'automation', 'test'],
) as dag:

    run_mvn_test = BashOperator(
        task_id='run_mvn_test',
        bash_command=(
            f'cd /appz/home/airflow/dags/agent_dags/WebshopChatAPIAutomation && '
            f'API_TOKEN="{api_token}" API_URL="{api_url}" mvn test '
            f'|| (echo "Maven tests failed" && touch /tmp/mvn_failed.flag); exit 0'
        )
    )

    slack_notify = PythonOperator(
        task_id="slack_notify",
        python_callable=slack_alert,
        provide_context=True,
    )

    run_mvn_test >> slack_notify
