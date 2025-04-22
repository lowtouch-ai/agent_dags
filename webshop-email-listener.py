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

# Configure logging
logging.basicConfig(level=logging.INFO)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 24),
    "retries": 1,
    "retry_delay": timedelta(seconds=15),
}

WEBSHOP_FROM_ADDRESS = Variable.get("WEBSHOP_FROM_ADDRESS")
GMAIL_CREDENTIALS = Variable.get("WEBSHOP_GMAIL_CREDENTIALS", deserialize_json=True)
LAST_PROCESSED_EMAIL_FILE = "/appz/cache/last_processed_email.json"

def authenticate_gmail():
    try:
        creds = Credentials.from_authorized_user_info(GMAIL_CREDENTIALS)
        service = build("gmail", "v1", credentials=creds)
        profile = service.users().getProfile(userId="me").execute()
        logged_in_email = profile.get("emailAddress", "")
        if logged_in_email.lower() != WEBSHOP_FROM_ADDRESS.lower():
            raise ValueError(f"Wrong Gmail account! Expected {WEBSHOP_FROM_ADDRESS}, but got {logged_in_email}")
        logging.info(f"Authenticated Gmail account: {logged_in_email}")
        return service
    except Exception as e:
        logging.error(f"Failed to authenticate Gmail: {str(e)}")
        return None

def get_last_checked_timestamp():
    try:
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
    except Exception as e:
        logging.error(f"Error retrieving last checked timestamp: {str(e)}")
        return int(time.time() * 1000)

def update_last_checked_timestamp(timestamp):
    try:
        os.makedirs(os.path.dirname(LAST_PROCESSED_EMAIL_FILE), exist_ok=True)
        with open(LAST_PROCESSED_EMAIL_FILE, "w") as f:
            json.dump({"last_processed": timestamp}, f)
        logging.info(f"Updated last processed email timestamp (milliseconds): {timestamp}")
    except Exception as e:
        logging.error(f"Error updating last checked timestamp: {str(e)}")

def fetch_unread_emails(**kwargs):
    try:
        service = authenticate_gmail()
        if not service:
            logging.error("Gmail authentication failed, aborting email fetch.")
            return

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
                "threadId": msg_data.get("threadId"),
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
    except Exception as e:
        logging.error(f"Unexpected error in fetch_unread_emails: {str(e)}")

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
        try:
            ti = kwargs['ti']
            unread_emails = ti.xcom_pull(task_ids="fetch_unread_emails", key="unread_emails") or []
            logging.info(f"Retrieved {len(unread_emails)} unread emails from XCom.")

            if not unread_emails:
                logging.info("No unread emails found in XCom, skipping trigger.")
                return

            # Store trigger tasks to set dependencies
            trigger_tasks = []
            for email in unread_emails:
                task_id = f"trigger_response_{email['id'].replace('-', '_')}"
                logging.info(f"Creating TriggerDagRunOperator for email: {email['id']}")

                trigger_task = TriggerDagRunOperator(
                    task_id=task_id,
                    trigger_dag_id="webshop-email-respond",
                    conf={"email_data": email},
                    dag=dag
                )
                trigger_tasks.append(trigger_task)

            # Set dependencies: fetch_emails_task -> trigger_tasks
            for trigger_task in trigger_tasks:
                fetch_emails_task >> trigger_task

            # Clear unread_emails in XCom
            ti.xcom_push(key="unread_emails", value=[])
        except Exception as e:
            logging.error(f"Unexpected error in trigger_response_tasks: {str(e)}")
            raise

    trigger_email_response_task = PythonOperator(
        task_id="trigger-email-response-dag",
        python_callable=trigger_response_tasks,
        provide_context=True
    )

    fetch_emails_task >> trigger_email_response_task
