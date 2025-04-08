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
import os

# Configure logging
logging.basicConfig(level=logging.INFO)

default_args = {
    "owner": "lowtouch.ai_developers",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 18),
    "retries": 0,
    "retry_delay": timedelta(seconds=15),
}

WEBSHOP_FROM_ADDRESS = Variable.get("WEBSHOP_FROM_ADDRESS")
GMAIL_CREDENTIALS = Variable.get("WEBSHOP_GMAIL_CREDENTIALS", deserialize_json=True)
OLLAMA_HOST = Variable.get("WEBSHOP_OLLAMA_HOST")

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

def decode_email_payload(payload):
    try:
        if "data" in payload.get("body", {}):
            return base64.urlsafe_b64decode(payload["body"]["data"]).decode("utf-8")
        return ""
    except Exception as e:
        logging.error(f"Error decoding email payload: {str(e)}")
        return ""

def get_email_thread(service, email_data):
    try:
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
        email_thread = [{
                "headers": {header["name"]: header["value"] for header in msg.get("payload", {}).get("headers", [])},
                "content": decode_email_payload(msg.get("payload", {})).strip(),
            }
            for msg in thread.get("messages", [])]
        return email_thread
    except Exception as e:
        logging.error(f"Error retrieving email thread: {str(e)}")
        return []

def get_ai_response(user_query):
    try:
        client = Client(host=OLLAMA_HOST, headers={'x-ltai-client': 'webshop-email-respond'})
        response = client.chat(
            model='webshop-invoice:0.5',
            messages=[{"role": "user", "content": user_query}],
            stream=False
        )
        ai_content = response.get('message', {}).get('content', '')
        # Ensure the response is properly formatted as HTML
        if not ai_content.strip().startswith('<'):
            ai_content = f"<html><body>{ai_content}</body></html>"
        return ai_content
    except ResponseError as e:
        logging.error(f"Ollama API error: {str(e)} (status: {getattr(e, 'status_code', 'unknown')})")
        return "<html><body>We are currently experiencing technical difficulties. Please check back later.</body></html>"
    except Exception as e:
        logging.error(f"Unexpected error in AI response generation: {str(e)}")
        return "<html><body>We are currently experiencing technical difficulties. Please check back later.</body></html>"

def send_email(service, recipient, subject, body, in_reply_to, references):
    try:
        msg = MIMEMultipart()
        msg["From"] = f"WebShop via lowtouch.ai <{WEBSHOP_FROM_ADDRESS}>"
        msg["To"] = recipient
        msg["Subject"] = subject
        msg["In-Reply-To"] = in_reply_to
        msg["References"] = references
        msg.attach(MIMEText(body, "html"))
        raw_msg = base64.urlsafe_b64encode(msg.as_string().encode("utf-8")).decode("utf-8")
        return service.users().messages().send(userId="me", body={"raw": raw_msg}).execute()
    except Exception as e:
        logging.error(f"Failed to send email: {str(e)}")
        return None

def send_response(**kwargs):
    try:
        email_data = kwargs['dag_run'].conf.get("email_data", {})
        logging.info(f"Received email data: {email_data}")
        if not email_data:
            logging.warning("No email data received! This DAG was likely triggered manually.")
            return
        
        service = authenticate_gmail()
        if not service:
            logging.error("Gmail authentication failed, aborting email response.")
            return
        
        sender_email = email_data["headers"].get("From", "")
        subject = f"Re: {email_data['headers'].get('Subject', 'No Subject')}"
        user_query = email_data.get("content", "").strip()
        ai_response_html = get_ai_response(user_query) if user_query else "<html><body>No content provided in the email.</body></html>"

        send_email(
            service, sender_email, subject, ai_response_html,
            email_data["headers"].get("Message-ID", ""),
            email_data["headers"].get("References", "")
        )
    except Exception as e:
        logging.error(f"Unexpected error in send_response: {str(e)}")

readme_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'email_responder.md')
with open(readme_path, 'r') as file:
    readme_content = file.read()

with DAG("shared_send_message_email", default_args=default_args, schedule_interval=None, catchup=False, doc_md=readme_content, tags=["email", "shared", "send", "message"]) as dag:
    send_response_task = PythonOperator(
        task_id="send-response",
        python_callable=send_response,
        provide_context=True
    )