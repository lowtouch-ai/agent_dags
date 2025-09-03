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
app_password = Variable.get("GMAIL_TOKEN")
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
    if not os.path.exists("/tmp/invoflux_failed.flag"):
        print("No Invoflux failures detected, skipping Slack alert.")
        return

    failed_reasons = {}
    reports_dir = "/appz/home/airflow/dags/agent_dags/Invoflux-ui-tests/target/surefire-reports"

    try:
        # Read Surefire report files
        for f in os.listdir(reports_dir):
            if not f.endswith(".txt"):
                continue
            with open(os.path.join(reports_dir, f), "r", encoding="utf-8", errors="ignore") as fh:
                content = fh.read()
                lines = content.splitlines()
                i = 0
                while i < len(lines):
                    line = lines[i]
                    # Look for lines indicating a test failure
                    if "Time elapsed:" in line and "<<< FAILURE!" in line:
                        parts = line.split("  Time elapsed:")
                        if len(parts) > 1:
                            test_full = parts[0].strip()
                            # Extract test name after the last dot
                            test_name = test_full.split('.')[-1]
                            # Look for the error message in the next line
                            if i + 1 < len(lines):
                                error_line = lines[i + 1].strip()
                                # Extract the error message (remove stack trace details)
                                if error_line.startswith("java.lang") or error_line.startswith("javax.mail"):
                                    # Extract only the meaningful error message
                                    error_parts = error_line.split(':', 1)
                                    msg = error_parts[1].strip() if len(error_parts) > 1 else error_line
                                    # Clean up the message
                                    msg = re.sub(r"\s*expected \[true\] but found \[false\]$", "", msg).strip()
                                    failed_reasons[test_name] = msg
                                else:
                                    # Handle cases where the error message is directly in the line
                                    msg = re.sub(r"\s*expected \[true\] but found \[false\]$", "", error_line).strip()
                                    failed_reasons[test_name] = msg
                    i += 1
    except Exception as e:
        failed_reasons = {"Error": f"Could not parse test report ({e})"}

    if failed_reasons:
        # Format the failed tests with their reasons
        failed_text = "\n".join([f"• {test}\n  {reason}" for test, reason in failed_reasons.items()])
    else:
        failed_text = "Unknown"

    msg = (
        f":x: Invoflux UI tests failed in DAG *{context['dag'].dag_id}* on SERVER {server_name}\n"
        f"*Failed Tests:*\n{failed_text}"
    )
    requests.post(slack_webhook, json={"text": msg})

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