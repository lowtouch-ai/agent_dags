from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import base64
import json
import logging
import re
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from ollama import Client
from ollama._types import ResponseError
from email import message_from_bytes
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(level=logging.INFO)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 18),
    "retries": 0,
    "retry_delay": timedelta(seconds=15),
}

WEBSHOP_FROM_ADDRESS = Variable.get("WEBSHOP_FROM_ADDRESS")
GMAIL_CREDENTIALS = Variable.get("WEBSHOP_GMAIL_CREDENTIALS", deserialize_json=True)
OLLAMA_HOST = Variable.get("WEBSHOP_OLLAMA_HOST")

def authenticate_gmail():
    creds = Credentials.from_authorized_user_info(GMAIL_CREDENTIALS)
    service = build("gmail", "v1", credentials=creds)
    profile = service.users().getProfile(userId="me").execute()
    logged_in_email = profile.get("emailAddress", "")
    if logged_in_email.lower() != WEBSHOP_FROM_ADDRESS.lower():
        raise ValueError(f"Wrong Gmail account! Expected {WEBSHOP_FROM_ADDRESS}, but got {logged_in_email}")
    logging.info(f"Authenticated Gmail account: {logged_in_email}")
    return service

def decode_email_payload(payload):
    if "data" in payload.get("body", {}):
        return base64.urlsafe_b64decode(payload["body"]["data"]).decode("utf-8")
    return ""

def get_email_thread(service, email_data):
    thread_id = email_data.get("threadId")
    message_id = email_data["headers"].get("Message-ID", "")
    
    if not thread_id:
        query_result = service.users().messages().list(userId="me", q=f"rfc822msgid:{message_id}").execute()
        messages = query_result.get("messages", [])
        if messages:
            message = service.users().messages().get(userId="me", id=messages[0]["id"]).execute()
            thread_id = message.get("threadId")
    
    if not thread_id:
        logging.warning(f"No thread ID found for message ID {message_id}. Treating as a single email.")
        return [{"headers": email_data["headers"], "content": email_data["content"]}]

    thread = service.users().threads().get(userId="me", id=thread_id).execute()
    return [
        {
            "headers": {header["name"]: header["value"] for header in msg.get("payload", {}).get("headers", [])},
            "content": decode_email_payload(msg.get("payload", {})).strip(),
        }
        for msg in thread.get("messages", [])
    ]

def get_ai_response(user_query):
    try:
        client = Client(host=OLLAMA_HOST, headers={'x-ltai-client': 'webshop-email-respond'})
        response = client.chat(
            model='webshop-email:0.5',
            messages=[{"role": "user", "content": user_query}],
            stream=False
        )
        return response.get('message', {}).get('content', "We are currently experiencing technical difficulties. Please check back later.")
    except ResponseError as e:
        logging.error(f"Ollama API error: {str(e)} (status: {getattr(e, 'status_code', 'unknown')})")
        return "We are currently experiencing technical difficulties. Please check back later."

def send_email(service, recipient, subject, body, in_reply_to, references):
    msg = MIMEMultipart()
    msg["From"] = f"WebShop via lowtouch.ai <{WEBSHOP_FROM_ADDRESS}>"
    msg["To"] = recipient
    msg["Subject"] = subject
    msg["In-Reply-To"] = in_reply_to
    msg["References"] = references
    msg.attach(MIMEText(body, "html"))
    raw_msg = base64.urlsafe_b64encode(msg.as_string().encode("utf-8")).decode("utf-8")
    return service.users().messages().send(userId="me", body={"raw": raw_msg}).execute()

def send_response(**kwargs):
    email_data = kwargs['dag_run'].conf.get("email_data", {})
    logging.info(f"Received email data: {email_data}")
    if not email_data:
        logging.warning("No email data received! This DAG was likely triggered manually.")
        return
    
    service = authenticate_gmail()
    sender_email = email_data["headers"].get("From", "")
    subject = f"Re: {email_data['headers'].get('Subject', 'No Subject')}"
    user_query = email_data["content"]
    ai_response_html = get_ai_response(user_query)
    send_email(
        service, sender_email, subject, ai_response_html,
        email_data["headers"].get("Message-ID", ""),
        email_data["headers"].get("References", "")
    )

with DAG("webshop-email-respond", default_args=default_args, schedule_interval=None, catchup=False) as dag:
    send_response_task = PythonOperator(
        task_id="send-response",
        python_callable=send_response,
        provide_context=True
    )
