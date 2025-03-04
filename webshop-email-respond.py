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
GMAIL_CREDENTIALS = Variable.get("GMAIL_CREDENTIALS", deserialize_json=True)
OLLAMA_HOST = Variable.get("OLLAMA_HOST")

def authenticate_gmail():
    creds = Credentials.from_authorized_user_info(GMAIL_CREDENTIALS)
    service = build("gmail", "v1", credentials=creds)
    profile = service.users().getProfile(userId="me").execute()
    logged_in_email = profile.get("emailAddress", "")
    if logged_in_email.lower() != WEBSHOP_FROM_ADDRESS.lower():
        raise ValueError(f"Wrong Gmail account! Expected {WEBSHOP_FROM_ADDRESS}, but got {logged_in_email}")
    logging.info(f"Authenticated Gmail account: {logged_in_email}")
    return service

def get_ai_response(user_query):
    try:
        logging.info(f"Attempting to connect to Ollama at: {OLLAMA_HOST}")
        client = Client(
            host=OLLAMA_HOST,
            headers={'x-ltai-client': 'webshop-email-respond'}
        )
        logging.info(f"Sending query to Ollama: {user_query[:100]}...")
        response = client.chat(
            model='webshop-email:0.5',
            messages=[{"role": "user", "content": user_query}],
            stream=False
        )
        agent_response = response['message']['content']
        logging.info(f"Agent Response: {agent_response[:100]}...")
        return agent_response
    except ResponseError as e:
        error_detail = str(e)
        logging.error(f"API call failed with ResponseError: {error_detail} (status code: {getattr(e, 'status_code', 'unknown')})")
        return "AI service authentication failed due to an invalid API key."

def clean_subject(subject):
    return re.sub(r"^(Re:\s*)+", "Re: ", subject, flags=re.IGNORECASE).strip()

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
    thread_emails = []
    
    for msg in thread.get("messages", []):
        headers = {header["name"]: header["value"] for header in msg.get("payload", {}).get("headers", [])}
        payload = msg.get("payload", {})
        content = ""
        if "parts" in payload:
            for part in payload["parts"]:
                if part["mimeType"] in ["text/plain", "text/html"]:
                    content = base64.urlsafe_b64decode(part["body"].get("data", "").encode("ASCII")).decode("utf-8")
                    break
        elif "body" in payload and "data" in payload["body"]:
            content = base64.urlsafe_b64decode(payload["body"]["data"].encode("ASCII")).decode("utf-8")
        
        if "text/html" in payload.get("mimeType", ""):
            soup = BeautifulSoup(content, "html.parser")
            content = soup.get_text()
        
        thread_emails.append({"headers": headers, "content": content.strip()})
    
    logging.info(f"Retrieved {len(thread_emails)} emails in thread {thread_id}")
    return thread_emails

def send_response(**kwargs):
    email_data = kwargs['dag_run'].conf.get("email_data", {})
    logging.info(f"Received email data: {email_data}")

    if not email_data:
        logging.warning("No email data received! This DAG was likely triggered manually.")
        return

    service = authenticate_gmail()
    thread_emails = get_email_thread(service, email_data)
    thread_context = "\n\n".join([f"From: {e['headers'].get('From')}\nContent: {e['content']}" for e in thread_emails])
    
    sender_email = email_data["headers"].get("From", "")
    subject = clean_subject(email_data["headers"].get("Subject", "No Subject"))
    user_query = email_data["content"]
    full_query = f"Previous thread:\n{thread_context}\n\nCurrent email:\n{user_query}"

    try:
        ai_response_html = get_ai_response(full_query)
        ai_response_html = re.sub(r"^```(?:html)?\n?|```$", "", ai_response_html.strip(), flags=re.MULTILINE)

        if "FAILED_TO_RESPOND" in ai_response_html:
            error_message = ai_response_html
            logging.info(f"Detected FAILED_TO_RESPOND: {error_message}")
            friendly_query = f"The response was '{error_message}'. Provide a friendly email response to ask the user for more details."
            friendly_response = get_ai_response(friendly_query)
            friendly_response = re.sub(r"^```(?:html)?\n?|```$", "", friendly_response.strip(), flags=re.MULTILINE)
            msg = MIMEMultipart()
            msg["From"] = f"WebShop via lowtouch.ai <{WEBSHOP_FROM_ADDRESS}>"
            msg["To"] = sender_email
            msg["Subject"] = subject
            msg["In-Reply-To"] = email_data["headers"].get("Message-ID", "")
            msg["References"] = email_data["headers"].get("References", "") + " " + email_data["headers"].get("Message-ID", "")
            msg.attach(MIMEText(friendly_response, "html"))
            service.users().messages().send(userId="me", body={"raw": base64.urlsafe_b64encode(msg.as_string().encode("utf-8")).decode("utf-8")}).execute()
            raise ValueError(error_message)

    except ValueError as e:
        logging.error(f"Caught ValueError: {str(e)}")
        friendly_message = """
        <p>Hello,</p>
        <p>Thank you for reaching out! We're experiencing a temporary issue processing your request. Could you please provide more details or confirm your request? We'll get back to you as soon as possible.</p>
        <p>Best regards,<br>WebShop Support Team</p>
        """
        msg = MIMEMultipart()
        msg["From"] = f"WebShop via lowtouch.ai <{WEBSHOP_FROM_ADDRESS}>"
        msg["To"] = sender_email
        msg["Subject"] = subject
        msg["In-Reply-To"] = email_data["headers"].get("Message-ID", "")
        msg["References"] = email_data["headers"].get("References", "") + " " + email_data["headers"].get("Message-ID", "")
        msg.attach(MIMEText(friendly_message, "html"))
        service.users().messages().send(userId="me", body={"raw": base64.urlsafe_b64encode(msg.as_string().encode("utf-8")).decode("utf-8")}).execute()
        raise

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
