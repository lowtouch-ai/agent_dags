from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import base64
import os
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 18),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

CREDENTIALS_PATH = "/appz/home/airflow/dags/credentials.json"
EMAIL_ACCOUNT = Variable.get("EMAIL_ACCOUNT")  # Fetch from Airflow Variables

def authenticate_gmail():
    """Authenticate Gmail API and verify the correct email account is used."""
    creds = None
    if os.path.exists(CREDENTIALS_PATH):
        creds = Credentials.from_authorized_user_file(CREDENTIALS_PATH)
    service = build("gmail", "v1", credentials=creds)

    # Fetch authenticated email
    profile = service.users().getProfile(userId="me").execute()
    logged_in_email = profile.get("emailAddress", "")

    if logged_in_email.lower() != EMAIL_ACCOUNT.lower():
        raise ValueError(f"Wrong Gmail account! Expected {EMAIL_ACCOUNT}, but got {logged_in_email}")

    print(f"Authenticated Gmail Account: {logged_in_email}")
    return service

def send_response(**kwargs):
    """Send auto-response without modifying email read status."""
    email_data = kwargs['dag_run'].conf  # Get email details from trigger
    service = authenticate_gmail()
    
    recipient = email_data["headers"].get("From", "")

    # Construct email response
    message_body = f"Hi,\n\nThanks for reaching out. Our support team will get back to you soon.\n\nBest Regards,\nWebshop Support"
    email_msg = f"From: me\nTo: {recipient}\nSubject: Re: {email_data['headers'].get('Subject', 'No Subject')}\n\n{message_body}"
    encoded_message = base64.urlsafe_b64encode(email_msg.encode("utf-8")).decode("utf-8")

    # Send email response
    service.users().messages().send(
        userId="me",
        body={"raw": encoded_message}
    ).execute()

    print(f"Response sent to {recipient}")

# Define DAG
with DAG("webshop-email-respond",
         default_args=default_args,
         schedule_interval=None,  # This DAG is triggered dynamically
         catchup=False) as dag:

    send_response_task = PythonOperator(
        task_id="send-response",
        python_callable=send_response,
        provide_context=True
    )

    send_response_task
