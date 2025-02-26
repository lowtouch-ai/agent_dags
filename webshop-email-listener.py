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
    "retry_delay": timedelta(minutes=5),
}

# Configuration variables
EMAIL_ACCOUNT = Variable.get("EMAIL_ID")  
GMAIL_CREDENTIALS = Variable.get("GMAIL_CREDENTIALS", deserialize_json=True)  
LAST_PROCESSED_EMAIL_FILE = "/appz/cache/last_processed_email.json"  # Track last responded email timestamp

def authenticate_gmail():
    """Authenticate Gmail API and verify the correct email account is used."""
    creds = Credentials.from_authorized_user_info(GMAIL_CREDENTIALS)
    service = build("gmail", "v1", credentials=creds)

    profile = service.users().getProfile(userId="me").execute()
    logged_in_email = profile.get("emailAddress", "")

    if logged_in_email.lower() != EMAIL_ACCOUNT.lower():
        raise ValueError(f" Wrong Gmail account! Expected {EMAIL_ACCOUNT}, but got {logged_in_email}")

    logging.info(f" Authenticated Gmail Account: {logged_in_email}")
    return service

def get_last_checked_timestamp():
    """Retrieve the last processed email timestamp or fetch all unread emails if no responses exist."""
    if os.path.exists(LAST_PROCESSED_EMAIL_FILE):
        with open(LAST_PROCESSED_EMAIL_FILE, "r") as f:
            last_checked = json.load(f).get("last_processed", None)
            if last_checked:
                logging.info(f" Retrieved last processed email timestamp: {last_checked}")
                return last_checked

    # If no response emails have been sent, fetch all unread emails
    logging.info(" No responses sent yet, fetching all unread emails.")
    return None

def fetch_unread_emails(**kwargs):
    """Fetch unread emails received after the last processed email timestamp, or all unread emails if no responses exist."""
    service = authenticate_gmail()
    
    last_checked_timestamp = get_last_checked_timestamp()
    if last_checked_timestamp:
        query = f"is:unread after:{last_checked_timestamp}"
        logging.info(f" Checking for unread emails after timestamp: {last_checked_timestamp}")
    else:
        query = "is:unread"  # Fetch all unread emails
        logging.info(" Fetching all unread emails (no timestamp filter).")

    results = service.users().messages().list(userId="me", labelIds=["INBOX"], q=query).execute()
    messages = results.get("messages", [])
    logging.info(f" Found {len(messages)} unread emails in query results.")

    unread_emails = []
    max_timestamp = last_checked_timestamp if last_checked_timestamp else int(time.time())

    for msg in messages:
        msg_data = service.users().messages().get(userId="me", id=msg["id"]).execute()

        headers = {header["name"]: header["value"] for header in msg_data["payload"]["headers"]}
        sender = headers.get("From", "").lower()
        timestamp = int(msg_data["internalDate"])  

        if "no-reply" in sender:
            logging.info(f" Skipping email from {sender} (timestamp: {timestamp})")
            continue

        body = msg_data.get("snippet", "")
        email_object = {
            "id": msg["id"],
            "headers": headers,  
            "content": body,
            "timestamp": timestamp
        }

        logging.info(f"Adding unread email: {email_object}")
        unread_emails.append(email_object)

        if timestamp > max_timestamp:
            max_timestamp = timestamp

    kwargs['ti'].xcom_push(key="unread_emails", value=unread_emails)

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
        ti = kwargs['ti']
        unread_emails = ti.xcom_pull(task_ids="fetch_unread_emails", key="unread_emails")

        logging.info(f" Retrieved {len(unread_emails) if unread_emails else 0} unread emails from XCom.")

        if not unread_emails:
            logging.info(" No unread emails found in XCom, skipping trigger.")
            return

        for email in unread_emails:
            task_id = f"trigger_response_{email['id'].replace('-', '_')}"  

            logging.info(f" Triggering Response DAG with email data: {email}")  

            trigger_task = TriggerDagRunOperator(
                task_id=task_id,
                trigger_dag_id="webshop-email-respond",
                conf={"email_data": email},
            )

            trigger_task.execute(context=kwargs)  

    trigger_email_response_task = PythonOperator(
        task_id="trigger-email-response-dag",
        python_callable=trigger_response_tasks,
        provide_context=True
    )

    fetch_emails_task >> trigger_email_response_task
