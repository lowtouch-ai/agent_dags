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
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from bs4 import BeautifulSoup

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 18),
    "retries": 0,  # No retries on failure for FAILED_TO_RESPOND
    "retry_delay": timedelta(seconds=15),
}

WEBSHOP_FROM_ADDRESS = Variable.get("WEBSHOP_FROM_ADDRESS")  
GMAIL_CREDENTIALS = Variable.get("GMAIL_CREDENTIALS", deserialize_json=True)  

def authenticate_gmail():
    creds = Credentials.from_authorized_user_info(GMAIL_CREDENTIALS)
    service = build("gmail", "v1", credentials=creds)

    profile = service.users().getProfile(userId="me").execute()
    logged_in_email = profile.get("emailAddress", "")

    if logged_in_email.lower() != WEBSHOP_FROM_ADDRESS.lower():
        raise ValueError(f"Wrong Gmail account! Expected {WEBSHOP_FROM_ADDRESS}, but got {logged_in_email}")

    return service

def get_ai_response(user_query):
    client = Client(
        host='http://agentomatic:8000',
        headers={'x-ltai-client': 'webshop-email-respond'}
    )
    response = client.chat(
        model='webshop-email:0.5',
        messages=[{"role": "user", "content": user_query}],
        stream=False
    )
    agent_response = response['message']['content']
    logging.info(f"Agent Response: {agent_response}")
    return agent_response

def get_email_thread(service, email_data):
    """Fetch the full thread of emails based on the threadId."""
    thread_id = email_data.get("threadId")
    if not thread_id:
        logging.warning("No threadId found, treating as a single email.")
        return [email_data]

    thread = service.users().threads().get(userId="me", id=thread_id).execute()
    messages = thread.get("messages", [])
    
    thread_content = []
    for msg in messages:
        headers = {h["name"]: h["value"] for h in msg["payload"]["headers"]}
        body = msg.get("snippet", "")
        thread_content.append({
            "id": msg["id"],
            "headers": headers,
            "content": body,
            "timestamp": int(msg["internalDate"])
        })
    return thread_content

def clean_subject(subject):
    """Remove duplicate 'Re:' prefixes and normalize the subject."""
    return re.sub(r"^(Re:\s*)+", "Re: ", subject, flags=re.IGNORECASE).strip()

def send_response(**kwargs):
    email_data = kwargs['dag_run'].conf.get("email_data", {})  

    logging.info(f"Received email data: {email_data}")  

    if not email_data:
        logging.warning("No email data received! This DAG was likely triggered manually.")
        return  

    service = authenticate_gmail()

    # Fetch thread history
    thread_emails = get_email_thread(service, email_data)
    thread_context = "\n\n".join([f"From: {e['headers'].get('From')}\nContent: {e['content']}" for e in thread_emails])
    
    sender_email = email_data["headers"].get("From", "")
    subject = clean_subject(email_data["headers"].get("Subject", "No Subject"))
    user_query = email_data["content"]
    
    # Include thread context in the AI query
    full_query = f"Previous thread:\n{thread_context}\n\nCurrent email:\n{user_query}"
    ai_response_html = get_ai_response(full_query)
    ai_response_html = re.sub(r"^```(?:html)?\n?|```$", "", ai_response_html.strip(), flags=re.MULTILINE)

    # Handle FAILED_TO_RESPOND
    if "FAILED_TO_RESPOND" in ai_response_html:
        error_message = ai_response_html
        friendly_query = f"The response was '{error_message}'. Provide a friendly email response to ask the user for more details."
        friendly_response = get_ai_response(friendly_query)
        friendly_response = re.sub(r"^```(?:html)?\n?|```$", "", friendly_response.strip(), flags=re.MULTILINE)

        # Send friendly response
        msg = MIMEMultipart()
        msg["From"] = f"WebShop via lowtouch.ai <{WEBSHOP_FROM_ADDRESS}>"
        msg["To"] = sender_email
        msg["Subject"] = subject
        msg["In-Reply-To"] = email_data["headers"].get("Message-ID", "")
        msg["References"] = email_data["headers"].get("References", "") + " " + email_data["headers"].get("Message-ID", "")
        msg.attach(MIMEText(friendly_response, "html"))
        service.users().messages().send(userId="me", body={"raw": base64.urlsafe_b64encode(msg.as_string().encode("utf-8")).decode("utf-8")}).execute()

        # Fail the task
        raise ValueError(error_message)

    # Send regular response
    msg = MIMEMultipart()
    msg["From"] = f"WebShop via lowtouch.ai <{WEBSHOP_FROM_ADDRESS}>"
    msg["To"] = sender_email
    msg["Subject"] = subject
    msg["In-Reply-To"] = email_data["headers"].get("Message-ID", "")
    msg["References"] = email_data["headers"].get("References", "") + " " + email_data["headers"].get("Message-ID", "")
    msg.attach(MIMEText(ai_response_html, "html"))

    service.users().messages().send(userId="me", body={"raw": base64.urlsafe_b64encode(msg.as_string().encode("utf-8")).decode("utf-8")}).execute()

with DAG("webshop-email-respond", default_args=default_args, schedule_interval=None, catchup=False) as dag:
    send_response_task = PythonOperator(
        task_id="send-response",
        python_callable=send_response,
        provide_context=True
    )
