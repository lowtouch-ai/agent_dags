from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pendulum
import requests
import os
import re

# Set IST timezone
ist = pendulum.timezone("Asia/Kolkata")

# Airflow Variables (set in UI or via CLI)
login_email = Variable.get("LOGIN_EMAIL")
login_password = Variable.get("LOGIN_PASSWORD")
base_url = Variable.get("BASE_URL")
from_email = Variable.get("FROM_EMAIL")
app_password = Variable.get("APP_PASSWORD")
invoflux_agent_email = Variable.get("INVOFLUX_AGENT_EMAIL")

slack_webhook = Variable.get("SLACK_WEBHOOK_URL")
server_name = Variable.get("SERVER")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 3, tzinfo=ist),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def slack_alert(**context):
    """Send Slack alert if Invoflux tests failed"""
    if os.path.exists("/tmp/invoflux_failed.flag"):
        failed_tests = []
        reports_dir = "/appz/home/airflow/dags/agent_dags/Invoflux-ui-tests/target/surefire-reports"

        try:
            # Look inside all surefire reports for "FAILURES:"
            for f in os.listdir(reports_dir):
                if f.endswith(".txt") or f.endswith(".xml"):
                    with open(os.path.join(reports_dir, f), "r", encoding="utf-8", errors="ignore") as fh:
                        content = fh.read()
                        matches = re.findall(r"InvofluxTests\.(\w+)", content)  # capture test names
                        if matches:
                            failed_tests.extend(matches)
        except Exception as e:
            failed_tests = [f"Could not parse test report ({e})"]

        if failed_tests:
            failed_text = "\n• " + "\n• ".join(set(failed_tests))
        else:
            failed_text = "Unknown"

        msg = (
            f":x: Invoflux UI tests failed in DAG *{context['dag'].dag_id}* on SERVER {server_name}\n"
            f"*Failed Tests:*{failed_text}"
        )
        requests.post(slack_webhook, json={"text": msg})
    else:
        print("No Invoflux failures detected, skipping Slack alert.")

with DAG(
    dag_id='invoflux_run_ui_tests',
    default_args=default_args,
    schedule_interval='0 15 * * *',  # 15:00 IST daily
    catchup=False,
    tags=['maven', 'automation', 'invoflux'],
) as dag:

    run_invoflux_tests = BashOperator(
        task_id='run_invoflux_tests',
        bash_command=(
            'rm -f /tmp/invoflux_failed.flag && '
            'cd /appz/home/airflow/dags/agent_dags/Invoflux-ui-tests && '
            f'LOGIN_EMAIL="{login_email}" '
            f'LOGIN_PASSWORD="{login_password}" '
            f'BASE_URL="{base_url}" '
            f'FROM_EMAIL="{from_email}" '
            f'APP_PASSWORD="{app_password}" '
            f'INVOFLUX_AGENT_EMAIL="{invoflux_agent_email}" '
            'mvn test || (echo "Invoflux tests failed" && touch /tmp/invoflux_failed.flag); exit 0'
        )
    )

    slack_notify = PythonOperator(
        task_id="slack_notify",
        python_callable=slack_alert,
        provide_context=True,
    )

    run_invoflux_tests >> slack_notify
