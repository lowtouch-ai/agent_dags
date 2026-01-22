from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
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

GMAIL_FROM_ADDRESS = Variable.get("ltai.api.test.from_address", default_var="")  
GMAIL_CREDENTIALS = Variable.get("ltai.api.test.gmail_credentials", default_var="")  
LAST_PROCESSED_EMAIL_FILE = "/appz/cache/api_testing_last_processed_email.json"
ATTACHMENT_DIR = "/appz/data/attachments/"

def authenticate_gmail():
    """Authenticate Gmail API and verify the correct email account is used."""
    creds = Credentials.from_authorized_user_info(json.loads(GMAIL_CREDENTIALS))
    service = build("gmail", "v1", credentials=creds)
    profile = service.users().getProfile(userId="me").execute()
    logged_in_email = profile.get("emailAddress", "")
    if logged_in_email.lower() != GMAIL_FROM_ADDRESS.lower():
        raise ValueError(f"Wrong Gmail account! Expected {GMAIL_FROM_ADDRESS}, but got {logged_in_email}")
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
            "content": f"```Invoice Content\n{sanitized_content}\n```",
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
    """Fetch unread emails and process JSON, PDF, and YAML attachments."""
    service = authenticate_gmail()
    last_checked_timestamp = get_last_checked_timestamp()
    query = f"is:unread after:{last_checked_timestamp // 1000}"
    logging.info(f"Fetching emails with query: {query}")

    results = service.users().messages().list(
        userId="me", labelIds=["INBOX"], q=query
    ).execute()
    messages = results.get("messages", [])
    logging.info(f"Found {len(messages)} unread emails")

    processed_emails = []
    max_timestamp = last_checked_timestamp
    os.makedirs(ATTACHMENT_DIR, exist_ok=True)

    for msg in messages:
        msg_data = service.users().messages().get(
            userId="me", id=msg["id"], format="full"
        ).execute()

        headers = {h["name"]: h["value"] for h in msg_data["payload"]["headers"]}
        sender = headers.get("From", "").lower()
        timestamp = int(msg_data["internalDate"])
        thread_id = msg_data.get("threadId", "")

        if "no-reply" in sender or timestamp <= last_checked_timestamp:
            continue

        json_attachments = []
        pdf_attachments = []
        config_attachment = None

        if "parts" in msg_data["payload"]:
            for part in msg_data["payload"].get("parts", []):
                filename = part.get("filename", "")
                
                # Check file type
                is_json = filename.lower().endswith(".json")
                is_pdf = filename.lower().endswith(".pdf")
                is_yaml = filename.lower().endswith((".yaml", ".yml"))
                
                # Skip if not a supported file type
                if not (is_json or is_pdf or is_yaml):
                    continue

                if not part.get("body", {}).get("attachmentId"):
                    continue

                att_id = part["body"]["attachmentId"]
                att = service.users().messages().attachments().get(
                    userId="me", messageId=msg["id"], id=att_id
                ).execute()

                file_data = base64.urlsafe_b64decode(att["data"])
                
                # Handle config files
                if is_yaml and filename.lower() in ["config.yaml", "config.yml"]:
                    config_dir = Variable.get("ltai.test.base_dir", default_var="/appz/pyunit_testing/") + f"{thread_id}"
                    os.makedirs(config_dir, exist_ok=True)
                    config_path = os.path.join(config_dir, "config.yaml")
                    
                    with open(config_path, "wb") as f:
                        f.write(file_data)
                    
                    logging.info(f"Saved config file to: {config_path}")
                    
                    config_attachment = {
                        "filename": "config.yaml",
                        "path": config_path,
                        "mime_type": part.get("mimeType", "application/x-yaml"),
                    }
                
                # Handle JSON files (Postman collections)
                elif is_json:
                    safe_filename = f"{msg['id']}_{filename}"
                    attachment_path = os.path.join(ATTACHMENT_DIR, safe_filename)

                    with open(attachment_path, "wb") as f:
                        f.write(file_data)

                    # Validate JSON
                    try:
                        with open(attachment_path, "r", encoding="utf-8") as f:
                            json_content = json.loads(f.read())
                        logging.info(f"Valid JSON attachment: {filename}")
                        
                        json_attachments.append({
                            "filename": filename,
                            "path": attachment_path,
                            "mime_type": part.get("mimeType", "application/json"),
                            "size": len(file_data)
                        })
                    except Exception as e:
                        logging.warning(f"Invalid JSON in {filename}: {e}")
                        continue
                
                # Handle PDF files
                elif is_pdf:
                    safe_filename = f"{msg['id']}_{filename}"
                    attachment_path = os.path.join(ATTACHMENT_DIR, safe_filename)

                    with open(attachment_path, "wb") as f:
                        f.write(file_data)
                    
                    logging.info(f"Saved PDF attachment: {filename}")
                    
                    # Extract PDF content
                    pdf_content = pdf_to_markdown(attachment_path)
                    
                    pdf_attachments.append({
                        "filename": filename,
                        "path": attachment_path,
                        "mime_type": part.get("mimeType", "application/pdf"),
                        "size": len(file_data),
                        "extracted_content": pdf_content.get("content", ""),
                        "metadata": pdf_content.get("metadata", {})
                    })

        # Only process emails that have at least one JSON file (Postman collection required)
        if json_attachments:
            email_object = {
                "id": msg["id"],
                "threadId": thread_id,
                "headers": headers,
                "content": msg_data.get("snippet", ""),
                "timestamp": timestamp,
                "json_attachments": json_attachments,
                "pdf_attachments": pdf_attachments,
                "config": config_attachment,
                "has_pdf": len(pdf_attachments) > 0
            }
            processed_emails.append(email_object)
            if timestamp > max_timestamp:
                max_timestamp = timestamp
            
            status_parts = [
                f"{len(json_attachments)} JSON file(s)",
                f"{len(pdf_attachments)} PDF file(s)" if pdf_attachments else "no PDFs",
                "with config" if config_attachment else "without config"
            ]
            logging.info(f"Processed email with {', '.join(status_parts)}")

    if processed_emails:
        update_last_checked_timestamp(max_timestamp)

    kwargs['ti'].xcom_push(key="emails_with_attachments", value=processed_emails)
    return len(processed_emails)

def branch_function(**kwargs):
    """Branch based on whether emails with JSON attachments were found."""
    ti = kwargs['ti']
    emails = ti.xcom_pull(task_ids="fetch_unread_emails", key="emails_with_attachments")
    
    if emails and len(emails) > 0:
        logging.info(f"Found {len(emails)} email(s) with JSON attachments → triggering response")
        return "trigger_test_runner_task"
    else:
        logging.info("No emails with JSON attachments found")
        return "no_email_found_task"

def trigger_response_tasks(**kwargs):
    """Trigger the test case runner DAG for each email with attachments."""
    ti = kwargs['ti']
    emails = ti.xcom_pull(task_ids="fetch_unread_emails", key="emails_with_attachments")
    
    if not emails:
        logging.info("No emails with attachments in XCom → nothing to trigger")
        return

    for email in emails:
        task_id = f"trigger_test_runner_{email['id'].replace('-','_')}"
        
        # Prepare configuration for the test runner
        conf_data = {
            "email_id": email['id'],
            "thread_id": email['threadId'],
            "json_files": email['json_attachments'],
            "pdf_files": email['pdf_attachments'],
            "config_file": email.get('config'),
            "has_pdf": email.get('has_pdf', False),
            "email_headers": email['headers'],
            "email_content": email['content']
        }
        
        logging.info(
            f"Triggering api_test_case_executor for email {email['id']} with "
            f"{len(email['json_attachments'])} JSON file(s) and "
            f"{len(email['pdf_attachments'])} PDF file(s)"
        )

        TriggerDagRunOperator(
            task_id=task_id,
            trigger_dag_id="api_test_case_executor",
            conf=conf_data,
            wait_for_completion=False,
        ).execute(context=kwargs)

def no_email_found(**kwargs):
    """Log when no emails are found."""
    logging.info("No new emails with JSON attachments found to process.")

readme_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mailbox_monitor.md')
with open(readme_path, 'r') as file:
    readme_content = file.read()

# Define DAG
with DAG("api_testing_monitor_mailbox",
         default_args=default_args,
         schedule=timedelta(minutes=1),
         catchup=False,
         doc_md=readme_content,
         tags=["mailbox", "api", "testing", "monitor"]) as dag:

    fetch_emails_task = PythonOperator(
        task_id="fetch_unread_emails",
        python_callable=fetch_unread_emails,
    )

    branch_task = BranchPythonOperator(
        task_id="branch_task",
        python_callable=branch_function,
    )

    trigger_test_runner = PythonOperator(
        task_id="trigger_test_runner",
        python_callable=trigger_response_tasks,
    )

    no_email_found_task = PythonOperator(
        task_id="no_email_found_task",
        python_callable=no_email_found,
    )

    # Set task dependencies
    fetch_emails_task >> branch_task
    branch_task >> [trigger_test_runner, no_email_found_task]