from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import os
import json
import time
import logging
import base64
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import re
from langchain_community.document_loaders import PyPDFLoader
from PIL import Image
import io

# Default DAG arguments
default_args = {
    "owner": "lowtouch.ai_developers",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 24),
    "retries": 1,
    "retry_delay": timedelta(seconds=15),
}

HELPDESK_FROM_ADDRESS = Variable.get("HELPDESK_FROM_ADDRESS")
HELPDESK_GMAIL_CREDENTIALS = Variable.get("HELPDESK_GMAIL_CREDENTIALS")
LAST_PROCESSED_EMAIL_FILE = "/appz/cache/last_processed_email.json"
ATTACHMENT_DIR = "/appz/data/attachments/"

def authenticate_gmail():
    """Authenticate Gmail API and verify the correct email account is used."""
    creds = Credentials.from_authorized_user_info(json.loads(HELPDESK_GMAIL_CREDENTIALS))
    service = build("gmail", "v1", credentials=creds)
    profile = service.users().getProfile(userId="me").execute()
    logged_in_email = profile.get("emailAddress", "")
    if logged_in_email.lower() != HELPDESK_FROM_ADDRESS.lower():
        raise ValueError(f"Wrong Gmail account! Expected {HELPDESK_FROM_ADDRESS}, but got {logged_in_email}")
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

def sanitize_text(text: str) -> str:
    """Sanitize text by removing or replacing problematic characters."""
    text = re.sub(r'[^\x20-\x7E]+', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def pdf_to_markdown(pdf_path: str) -> dict:
    """Extracts text from a PDF and returns serializable data using PyPDFLoader."""
    try:
        if not os.path.exists(pdf_path):
            logging.error(f"PDF file not found: {pdf_path}")
            return {"content": "", "metadata": {}}
        loader = PyPDFLoader(pdf_path)
        documents = loader.load()
        if not documents:
            logging.warning(f"No content extracted from PDF: {pdf_path}")
            return {"content": "", "metadata": {}}
        extracted_content = "\n".join(doc.page_content for doc in documents)
        metadata = [doc.metadata for doc in documents]
        if not extracted_content.strip():
            logging.info(f"Extracted content is empty for PDF: {pdf_path}")
            return {"content": "", "metadata": metadata}
        logging.info(f"Extracted content from PDF {pdf_path}: {extracted_content[:100]}...")
        sanitized_content = sanitize_text(extracted_content)
        return {
            "content": f"```\n{sanitized_content}\n```",
            "metadata": metadata
        }
    except Exception as e:
        logging.error(f"Error extracting PDF {pdf_path}: {str(e)}", exc_info=True)
        return {"content": str(e), "metadata": {}}

def image_to_base64(image_path: str) -> str:
    """Convert an image to a base64 string."""
    try:
        with Image.open(image_path) as img:
            img_byte_arr = io.BytesIO()
            img.save(img_byte_arr, format=img.format)
            img_data = img_byte_arr.getvalue()
            base64_string = base64.b64encode(img_data).decode("utf-8")
            logging.info(f"Converted image {image_path} to base64 string (length: {len(base64_string)})")
            return base64_string
    except Exception as e:
        logging.error(f"Error converting image {image_path} to base64: {str(e)}", exc_info=True)
        return ""

def fetch_unread_emails(**kwargs):
    """Fetch unread emails and their attachments after the last processed email timestamp."""
    service = authenticate_gmail()
    last_checked_timestamp = get_last_checked_timestamp()
    query = f"is:unread after:{last_checked_timestamp // 1000}"
    logging.info(f"Fetching emails with query: {query}")
    results = service.users().messages().list(userId="me", labelIds=["INBOX"], q=query).execute()
    messages = results.get("messages", [])
    logging.info(f"Found {len(messages)} unread emails since last checked timestamp: {last_checked_timestamp}")
    
    unread_emails = []
    max_timestamp = last_checked_timestamp
    os.makedirs(ATTACHMENT_DIR, exist_ok=True)

    for msg in messages:
        msg_data = service.users().messages().get(userId="me", id=msg["id"], format="full").execute()
        headers = {header["name"]: header["value"] for header in msg_data["payload"]["headers"]}
        sender = headers.get("From", "").lower()
        timestamp = int(msg_data["internalDate"])
        if "no-reply" in sender or timestamp <= last_checked_timestamp:
            logging.info(f"Skipping email from {sender} (timestamp: {timestamp})")
            continue
        body = msg_data.get("snippet", "")
        attachments = []
        if "parts" in msg_data["payload"]:
            for part in msg_data["payload"].get("parts", []):
                if part.get("filename") and part.get("body", {}).get("attachmentId"):
                    attachment_id = part["body"]["attachmentId"]
                    attachment_data = service.users().messages().attachments().get(
                        userId="me", messageId=msg["id"], id=attachment_id
                    ).execute()
                    file_data = base64.urlsafe_b64decode(attachment_data["data"].encode("UTF-8"))
                    filename = part["filename"]
                    mime_type = part.get("mimeType", "application/octet-stream")
                    attachment_path = os.path.join(ATTACHMENT_DIR, f"{msg['id']}_{filename}")
                    with open(attachment_path, "wb") as f:
                        f.write(file_data)
                    extracted_content = {}
                    base64_content = ""
                    if mime_type == "application/pdf":
                        extracted_content = pdf_to_markdown(attachment_path)
                        logging.info(f"Extracted content from PDF {filename}: {extracted_content['content'][:100]}...")
                    elif mime_type in ["image/png", "image/jpeg", "image/jpg"]:
                        base64_content = image_to_base64(attachment_path)
                        logging.info(f"Converted image {filename} to base64")
                    attachments.append({
                        "filename": filename,
                        "mime_type": mime_type,
                        "path": attachment_path,
                        "extracted_content": extracted_content,
                        "base64_content": base64_content
                    })
                    logging.info(f"Saved attachment: {filename} at {attachment_path}")
        email_object = {
            "id": msg["id"],
            "threadId": msg_data.get("threadId"),
            "headers": headers,
            "content": body,
            "timestamp": timestamp,
            "attachments": attachments
        }
        logging.info(f"Adding unread email with {len(attachments)} attachments: {email_object}")
        unread_emails.append(email_object)
        if timestamp > max_timestamp:
            max_timestamp = timestamp
    if unread_emails:
        update_last_checked_timestamp(max_timestamp)
    kwargs['ti'].xcom_push(key="unread_emails", value=unread_emails)
    return unread_emails

def branch_function(**kwargs):
    """Decide which task to execute based on the presence of unread emails."""
    ti = kwargs['ti']
    unread_emails = ti.xcom_pull(task_ids="fetch_unread_emails", key="unread_emails")
    if unread_emails and len(unread_emails) > 0:
        logging.info("Unread emails found, proceeding to trigger response tasks.")
        return "trigger_email_response_task"
    else:
        logging.info("No unread emails found, proceeding to no_email_found_task.")
        return "no_email_found_task"

def trigger_response_tasks(**kwargs):
    """Trigger 'helpdesk_send_message_email' for each unread email with attachments."""
    ti = kwargs['ti']
    unread_emails = ti.xcom_pull(task_ids="fetch_unread_emails", key="unread_emails")
    logging.info(f"Retrieved {len(unread_emails) if unread_emails else 0} unread emails from XCom.")
    if not unread_emails:
        logging.info("No unread emails found in XCom, skipping trigger.")
        return
    for email in unread_emails:
        task_id = f"trigger_response_{email['id'].replace('-', '_')}"
        logging.info(f"Triggering Response DAG with email data and {len(email['attachments'])} attachments: {email}")
        trigger_task = TriggerDagRunOperator(
            task_id=task_id,
            trigger_dag_id="helpdesk_send_message_email",
            conf={"email_data": email},
        )
        trigger_task.execute(context=kwargs)
    ti.xcom_push(key="unread_emails", value=[])

def no_email_found(**kwargs):
    """Log when no emails are found."""
    logging.info("No new emails found to process.")

readme_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mailbox_monitor.md')
with open(readme_path, 'r') as file:
    readme_content = file.read()

# Define DAG
with DAG("helpdesk_monitor_mailbox",
         default_args=default_args,
         schedule=timedelta(minutes=1),
         catchup=False,
         doc_md=readme_content,
         tags=["mailbox", "helpdesk", "monitor"]) as dag:

    fetch_emails_task = PythonOperator(
        task_id="fetch_unread_emails",
        python_callable=fetch_unread_emails,
    )

    branch_task = BranchPythonOperator(
        task_id="branch_task",
        python_callable=branch_function,
    )

    trigger_email_response_task = PythonOperator(
        task_id="trigger_email_response_task",
        python_callable=trigger_response_tasks,
    )

    no_email_found_task = PythonOperator(
        task_id="no_email_found_task",
        python_callable=no_email_found,
    )

    # Set task dependencies
    fetch_emails_task >> branch_task
    branch_task >> [trigger_email_response_task, no_email_found_task]
