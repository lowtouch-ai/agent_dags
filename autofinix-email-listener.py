from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import os
import json
import time
import logging
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

# Default DAG arguments
default_args = {
    "owner": "lowtouch.ai_developers",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 24),
    "retries": 1,
    "retry_delay": timedelta(seconds=15),
}

# Configuration variables
AUTOFINIX_FROM_ADDRESS = Variable.get("AUTOFINIX_FROM_ADDRESS")
GMAIL_CREDENTIALS = Variable.get("AUTOFINIX_GMAIL_CREDENTIALS")

LAST_PROCESSED_EMAIL_FILE = "/appz/cache/last_processed_email.json"

def authenticate_gmail():
    try:
        creds = Credentials.from_authorized_user_info(json.loads(GMAIL_CREDENTIALS))
        service = build("gmail", "v1", credentials=creds)
        profile = service.users().getProfile(userId="me").execute()
        logged_in_email = profile.get("emailAddress", "")
        if logged_in_email.lower() != AUTOFINIX_FROM_ADDRESS.lower():
            raise ValueError(f"Wrong Gmail account! Expected {AUTOFINIX_FROM_ADDRESS}, but got {logged_in_email}")
        logging.info(f"Authenticated Gmail account: {logged_in_email}")
        return service
    except Exception as e:
        logging.error(f"Failed to authenticate Gmail: {str(e)}")
        return None

def get_last_checked_timestamp():
    if os.path.exists(LAST_PROCESSED_EMAIL_FILE):
        with open(LAST_PROCESSED_EMAIL_FILE, "r") as f:
            last_checked = json.load(f).get("last_processed", None)
            if last_checked:
                return last_checked
    current_timestamp_ms = int(time.time() * 1000)
    update_last_checked_timestamp(current_timestamp_ms)
    return current_timestamp_ms

def update_last_checked_timestamp(timestamp):
    os.makedirs(os.path.dirname(LAST_PROCESSED_EMAIL_FILE), exist_ok=True)
    with open(LAST_PROCESSED_EMAIL_FILE, "w") as f:
        json.dump({"last_processed": timestamp}, f)

def fetch_unread_emails(**kwargs):
    service = authenticate_gmail()
    last_checked_timestamp = get_last_checked_timestamp()
    query = f"is:unread after:{last_checked_timestamp // 1000}"
    results = service.users().messages().list(userId="me", labelIds=["INBOX"], q=query).execute()
    messages = results.get("messages", [])
    unread_emails = []
    max_timestamp = last_checked_timestamp
    for msg in messages:
        msg_data = service.users().messages().get(userId="me", id=msg["id"]).execute()
        headers = {h["name"]: h["value"] for h in msg_data["payload"]["headers"]}
        sender = headers.get("From", "").lower()
        timestamp = int(msg_data["internalDate"])
        if "no-reply" in sender or timestamp <= last_checked_timestamp:
            continue
        email_object = {
            "id": msg["id"],
            "threadId": msg_data.get("threadId"),
            "headers": headers,
            "content": msg_data.get("snippet", ""),
            "timestamp": timestamp
        }
        unread_emails.append(email_object)
        if timestamp > max_timestamp:
            max_timestamp = timestamp
    if unread_emails:
        update_last_checked_timestamp(max_timestamp)
    kwargs['ti'].xcom_push(key="unread_emails", value=unread_emails)
    return unread_emails

def branch_function(**kwargs):
    ti = kwargs['ti']
    unread_emails = ti.xcom_pull(task_ids="fetch_unread_emails", key="unread_emails")
    if unread_emails and len(unread_emails) > 0:
        logging.info("Unread emails found, proceeding to trigger response tasks.")
        return "trigger_email_response_task"
    else:
        logging.info("No unread emails found, proceeding to no_email_found_task.")
        return "no_email_found_task"

def trigger_response_tasks(**kwargs):
    ti = kwargs['ti']
    unread_emails = ti.xcom_pull(task_ids="fetch_unread_emails", key="unread_emails")
    if not unread_emails:
        return
    for email in unread_emails:
        task_id = f"trigger_response_{email['id'].replace('-', '_')}"
        trigger_task = TriggerDagRunOperator(
            task_id=task_id,
            trigger_dag_id="autofinix_send_message_email",
            conf={"email_data": email},
        )
        trigger_task.execute(context=kwargs)
    ti.xcom_push(key="unread_emails", value=[])

def no_email_found(**kwargs):
    logging.info("No new emails found to process.")

readme_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mailbox_monitor.md')
with open(readme_path, 'r') as file:
    readme_content = file.read()

with DAG("autofinix_monitor_mailbox",
         default_args=default_args,
         schedule=timedelta(minutes=1),
         catchup=False,
         doc_md=readme_content,
         tags=["mailbox", "autofinix", "monitor"]) as dag:

    fetch_emails_task = PythonOperator(
        task_id="fetch_unread_emails",
        python_callable=fetch_unread_emails,
        provide_context=True
    )

    branch_task = BranchPythonOperator(
        task_id="branch_task",
        python_callable=branch_function,
        provide_context=True
    )

    trigger_email_response_task = PythonOperator(
        task_id="trigger_email_response_task",
        python_callable=trigger_response_tasks,
        provide_context=True
    )

    no_email_found_task = PythonOperator(
        task_id="no_email_found_task",
        python_callable=no_email_found,
        provide_context=True
    )

    # Task dependencies
    fetch_emails_task >> branch_task
    branch_task >> [trigger_email_response_task, no_email_found_task]
