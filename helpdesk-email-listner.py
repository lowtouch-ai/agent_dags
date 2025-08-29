from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
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

def extract_email_body(payload):
    """Extract the body content from email payload."""
    body = ""
    
    if "parts" in payload:
        for part in payload["parts"]:
            if part["mimeType"] == "text/plain":
                if "data" in part["body"]:
                    body = base64.urlsafe_b64decode(part["body"]["data"]).decode("utf-8")
                    break
            elif part["mimeType"] == "text/html" and not body:
                if "data" in part["body"]:
                    body = base64.urlsafe_b64decode(part["body"]["data"]).decode("utf-8")
    else:
        # Single part message
        if payload["mimeType"] in ["text/plain", "text/html"]:
            if "data" in payload["body"]:
                body = base64.urlsafe_b64decode(payload["body"]["data"]).decode("utf-8")
    
    return body if body else ""

def process_attachments(service, message_id, payload):
    """Process and extract attachments from email message."""
    attachments = []
    
    def process_parts(parts):
        for part in parts:
            if part.get("filename") and part.get("body", {}).get("attachmentId"):
                attachment_id = part["body"]["attachmentId"]
                try:
                    attachment_data = service.users().messages().attachments().get(
                        userId="me", messageId=message_id, id=attachment_id
                    ).execute()
                    file_data = base64.urlsafe_b64decode(attachment_data["data"].encode("UTF-8"))
                    filename = part["filename"]
                    mime_type = part.get("mimeType", "application/octet-stream")
                    attachment_path = os.path.join(ATTACHMENT_DIR, f"{message_id}_{filename}")
                    
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
                except Exception as e:
                    logging.error(f"Error processing attachment {part.get('filename', 'unknown')}: {str(e)}")
            
            # Recursively process nested parts
            if "parts" in part:
                process_parts(part["parts"])
    
    if "parts" in payload:
        process_parts(payload["parts"])
    
    return attachments

def get_full_thread(service, thread_id):
    """Fetch the complete email thread with all messages."""
    try:
        thread = service.users().threads().get(userId="me", id=thread_id, format="full").execute()
        thread_messages = []
        
        for message in thread.get("messages", []):
            headers = {header["name"]: header["value"] for header in message["payload"]["headers"]}
            body = extract_email_body(message["payload"])
            attachments = process_attachments(service, message["id"], message["payload"])
            
            message_data = {
                "id": message["id"],
                "threadId": message.get("threadId"),
                "headers": headers,
                "content": body,
                "snippet": message.get("snippet", ""),
                "timestamp": int(message["internalDate"]),
                "attachments": attachments,
                "labelIds": message.get("labelIds", [])
            }
            thread_messages.append(message_data)
        
        # Sort messages by timestamp to maintain chronological order
        thread_messages.sort(key=lambda x: x["timestamp"])
        
        logging.info(f"Retrieved complete thread {thread_id} with {len(thread_messages)} messages")
        return {
            "thread_id": thread_id,
            "messages": thread_messages,
            "total_messages": len(thread_messages)
        }
        
    except Exception as e:
        logging.error(f"Error fetching thread {thread_id}: {str(e)}")
        return None

def fetch_unread_emails(**kwargs):
    """Fetch unread emails and their complete threads after the last processed email timestamp."""
    service = authenticate_gmail()
    last_checked_timestamp = get_last_checked_timestamp()
    query = f"is:unread after:{last_checked_timestamp // 1000}"
    logging.info(f"Fetching emails with query: {query}")
    
    results = service.users().messages().list(userId="me", labelIds=["INBOX"], q=query).execute()
    messages = results.get("messages", [])
    logging.info(f"Found {len(messages)} unread emails since last checked timestamp: {last_checked_timestamp}")
    
    processed_threads = set()  # Track processed thread IDs to avoid duplicates
    email_threads = []
    max_timestamp = last_checked_timestamp
    os.makedirs(ATTACHMENT_DIR, exist_ok=True)

    for msg in messages:
        try:
            msg_data = service.users().messages().get(userId="me", id=msg["id"], format="full").execute()
            headers = {header["name"]: header["value"] for header in msg_data["payload"]["headers"]}
            sender = headers.get("From", "").lower()
            timestamp = int(msg_data["internalDate"])
            thread_id = msg_data.get("threadId")
            
            # Skip no-reply emails and emails older than last processed
            if "no-reply" in sender or timestamp <= last_checked_timestamp:
                logging.info(f"Skipping email from {sender} (timestamp: {timestamp})")
                continue
            
            # Skip if we've already processed this thread
            if thread_id in processed_threads:
                logging.info(f"Thread {thread_id} already processed, skipping")
                continue
            
            # Get the complete thread
            full_thread = get_full_thread(service, thread_id)
            if full_thread:
                # Find the latest message in the thread to use for timestamp tracking
                latest_timestamp = max(message["timestamp"] for message in full_thread["messages"])
                
                email_threads.append({
                    "trigger_message_id": msg["id"],  # The specific message that triggered this
                    "thread_data": full_thread,
                    "latest_timestamp": latest_timestamp
                })
                
                processed_threads.add(thread_id)
                
                if latest_timestamp > max_timestamp:
                    max_timestamp = latest_timestamp
                
                logging.info(f"Added complete thread {thread_id} with {full_thread['total_messages']} messages")
            
        except Exception as e:
            logging.error(f"Error processing message {msg['id']}: {str(e)}")
            continue
    
    if email_threads:
        update_last_checked_timestamp(max_timestamp)
        logging.info(f"Processed {len(email_threads)} email threads")
    
    kwargs['ti'].xcom_push(key="email_threads", value=email_threads)
    return email_threads

def branch_function(**kwargs):
    """Decide which task to execute based on the presence of unread email threads."""
    ti = kwargs['ti']
    email_threads = ti.xcom_pull(task_ids="fetch_unread_emails", key="email_threads")
    if email_threads and len(email_threads) > 0:
        logging.info(f"Found {len(email_threads)} email threads, proceeding to trigger response tasks.")
        return "trigger_email_response_task"
    else:
        logging.info("No email threads found, proceeding to no_email_found_task.")
        return "no_email_found_task"

def trigger_response_tasks(**kwargs):
    """Trigger 'helpdesk_send_message_email' for each email thread."""
    ti = kwargs['ti']
    email_threads = ti.xcom_pull(task_ids="fetch_unread_emails", key="email_threads")
    logging.info(f"Retrieved {len(email_threads) if email_threads else 0} email threads from XCom.")
    
    if not email_threads:
        logging.info("No email threads found in XCom, skipping trigger.")
        return
    
    for thread_data in email_threads:
        # Create a safe task ID from the thread ID
        safe_thread_id = thread_data["thread_data"]["thread_id"].replace('-', '_').replace('+', '_')
        task_id = f"trigger_response_{safe_thread_id}"
        
        # Count total attachments across all messages in the thread
        total_attachments = sum(len(msg["attachments"]) for msg in thread_data["thread_data"]["messages"])
        
        logging.info(f"Triggering Response DAG with complete thread data:")
        logging.info(f"  Thread ID: {thread_data['thread_data']['thread_id']}")
        logging.info(f"  Total Messages: {thread_data['thread_data']['total_messages']}")
        logging.info(f"  Total Attachments: {total_attachments}")
        logging.info(f"  Trigger Message ID: {thread_data['trigger_message_id']}")
        
        trigger_task = TriggerDagRunOperator(
            task_id=task_id,
            trigger_dag_id="helpdesk_send_message_email",
            conf={
                "thread_data": thread_data["thread_data"],  # Complete thread with all messages
                "trigger_message_id": thread_data["trigger_message_id"],  # The specific message that triggered this
                "latest_timestamp": thread_data["latest_timestamp"]
            },
        )
        trigger_task.execute(context=kwargs)
    
    # Clear the processed threads
    ti.xcom_push(key="email_threads", value=[])

def no_email_found(**kwargs):
    """Log when no emails are found."""
    logging.info("No new emails found to process.")

readme_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mailbox_monitor.md')
with open(readme_path, 'r') as file:
    readme_content = file.read()

# Define DAG
with DAG("helpdesk_monitor_mailbox",
         default_args=default_args,
         schedule_interval=timedelta(minutes=1),
         catchup=False,
         doc_md=readme_content,
         tags=["mailbox", "odoo", "monitor"]) as dag:

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

    # Set task dependencies
    fetch_emails_task >> branch_task
    branch_task >> [trigger_email_response_task, no_email_found_task]