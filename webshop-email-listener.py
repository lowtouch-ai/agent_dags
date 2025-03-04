from airflow import DAG
from airflow.operators.python import PythonOperator
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
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 24),
    "retries": 1,
    "retry_delay": timedelta(seconds=15),
}

# Configuration variables
WEBSHOP_FROM_ADDRESS = Variable.get("WEBSHOP_FROM_ADDRESS")  
GMAIL_CREDENTIALS = Variable.get("WEBSHOP_GMAIL_CREDENTIALS", deserialize_json=True)  
LAST_PROCESSED_EMAIL_FILE = "/appz/cache/last_processed_email.json"

def authenticate_gmail():
    """Authenticate Gmail API and verify the correct email account is used."""
    creds = Credentials.from_authorized_user_info(GMAIL_CREDENTIALS)
    service = build("gmail", "v1", credentials=creds)

    profile = service.users().getProfile(userId="me").execute()
    logged_in_email = profile.get("emailAddress", "")

    if logged_in_email.lower() != WEBSHOP_FROM_ADDRESS.lower():
        raise ValueError(f"Wrong Gmail account! Expected {WEBSHOP_FROM_ADDRESS}, but got {logged_in_email}")

    logging.info(f"Authenticated Gmail Account: {logged_in_email}")
    return service

def get_last_checked_timestamp():
    """Retrieve the last processed timestamp, ensuring it's in milliseconds."""
    if os.path.exists(LAST_PROCESSED_EMAIL_FILE):
        with open(LAST_PROCESSED_EMAIL_FILE, "r") as f:
            last_checked = json.load(f).get("last_processed", None)
            if last_checked:
                logging.info(f"Retrieved last processed email timestamp (milliseconds): {last_checked}")
                return last_checked

    current_timestamp_ms = int(time.time() * 1000)
    logging.info(f"No previous timestamp, initializing to {current_timestamp_ms}")
    update_last_checked_timestamp(current_timestamp_ms)
    return current_timestamp_ms

def update_last_checked_timestamp(timestamp):
    """Ensure the timestamp is stored in milliseconds."""
    os.makedirs(os.path.dirname(LAST_PROCESSED_EMAIL_FILE), exist_ok=True)
    with open(LAST_PROCESSED_EMAIL_FILE, "w") as f:
        json.dump({"last_processed": timestamp}, f)
    logging.info(f"Updated last processed email timestamp (milliseconds): {timestamp}")

def fetch_unread_emails(**kwargs):
    """Fetch unread emails received after the last processed email timestamp."""
    service = authenticate_gmail()
    
    last_checked_timestamp = get_last_checked_timestamp()

    query = f"is:unread after:{last_checked_timestamp // 1000}"
    logging.info(f"Fetching emails with query: {query}")

    results = service.users().messages().list(userId="me", labelIds=["INBOX"], q=query).execute()
    messages = results.get("messages", [])

    logging.info(f"Found {len(messages)} unread emails.")

    unread_emails = []
    max_timestamp = last_checked_timestamp

    for msg in messages:
        msg_data = service.users().messages().get(userId="me", id=msg["id"]).execute()
        
        headers = {header["name"]: header["value"] for header in msg_data["payload"]["headers"]}
        sender = headers.get("From", "").lower()
        timestamp = int(msg_data["internalDate"])

        if "no-reply" in sender or timestamp <= last_checked_timestamp:
            logging.info(f"Skipping email from {sender} (timestamp: {timestamp})")
            continue

        body = msg_data.get("snippet", "")
        email_object = {
            "id": msg["id"],
            "threadId": msg_data.get("threadId"),  # Add threadId for threading support
            "headers": headers,  
            "content": body,
            "timestamp": timestamp
        }

        logging.info(f"Adding unread email: {email_object}")
        unread_emails.append(email_object)

        if timestamp > max_timestamp:
            max_timestamp = timestamp

    if unread_emails:
        update_last_checked_timestamp(max_timestamp)

    kwargs['ti'].xcom_push(key="unread_emails", value=unread_emails)

# Define DAG
with DAG("webshop-email-listener",
         default_args=default_args,
         schedule_interval=timedelta(minutes=1),
         catchup=False) as dag:

    fetch_emails_task = PythonOperator(
        task_id="fetch_unread_emails",
        python_callable=fetch_unread_emails,
        provide_context=True
    )

    def trigger_response_tasks(**kwargs):
        """Trigger 'webshop-email-respond' for each unread email."""
        ti = kwargs['ti']
        unread_emails = ti.xcom_pull(task_ids="fetch_unread_emails", key="unread_emails")

        logging.info(f"Retrieved {len(unread_emails) if unread_emails else 0} unread emails from XCom.")

        if not unread_emails:
            logging.info("No unread emails found in XCom, skipping trigger.")
            return

        for email in unread_emails:
            task_id = f"trigger_response_{email['id'].replace('-', '_')}"  

            logging.info(f"Triggering Response DAG with email data: {email}")  

            trigger_task = TriggerDagRunOperator(
                task_id=task_id,
                trigger_dag_id="webshop-email-respond",
                conf={"email_data": email},
            )

            trigger_task.execute(context=kwargs)  

        ti.xcom_push(key="unread_emails", value=[])

    trigger_email_response_task = PythonOperator(
        task_id="trigger-email-response-dag",
        python_callable=trigger_response_tasks,
        provide_context=True
    )

    fetch_emails_task >> trigger_email_response_task
