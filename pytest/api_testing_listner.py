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
from agent_dags.utils.email_utils import send_email, mark_email_as_read, extract_all_recipients
from agent_dags.utils.agent_utils import get_ai_response, extract_json_from_text

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
MODEL_NAME = Variable.get("ltai.api.test.model.name", default_var="APITestAgent:5.0")
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


def _extract_email_body(payload):
    """Extract the full email body text from a Gmail message payload.

    Walks the MIME parts tree, preferring text/plain over text/html.
    Returns decoded body string or empty string if none found.
    """
    def _get_body_from_parts(parts):
        plain_text = ""
        html_text = ""
        for part in parts:
            mime_type = part.get("mimeType", "")
            body_data = part.get("body", {}).get("data")
            if mime_type == "text/plain" and body_data:
                plain_text += base64.urlsafe_b64decode(body_data).decode("utf-8", errors="replace")
            elif mime_type == "text/html" and body_data:
                html_text += base64.urlsafe_b64decode(body_data).decode("utf-8", errors="replace")
            elif "parts" in part:
                nested_plain, nested_html = _get_body_from_parts(part["parts"])
                plain_text += nested_plain
                html_text += nested_html
        return plain_text, html_text

    # Simple message with body directly in payload
    body_data = payload.get("body", {}).get("data")
    if body_data:
        return base64.urlsafe_b64decode(body_data).decode("utf-8", errors="replace")

    # Multipart message — walk the parts tree
    if "parts" in payload:
        plain, html = _get_body_from_parts(payload["parts"])
        if plain:
            return plain
        if html:
            return re.sub(r'<[^>]+>', '', html)

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

        # Extract full email body for AI classification
        email_body = _extract_email_body(msg_data["payload"])

        # Process emails that have attachments OR non-trivial body content
        has_attachments = json_attachments or pdf_attachments
        has_body = len(email_body.strip()) > 10
        if has_attachments or has_body:
            email_object = {
                "id": msg["id"],
                "threadId": thread_id,
                "headers": headers,
                "content": email_body or msg_data.get("snippet", ""),
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
                "with config" if config_attachment else "without config",
                "with body" if has_body else "no body"
            ]
            logging.info(f"Processed email with {', '.join(status_parts)}")

    if processed_emails:
        update_last_checked_timestamp(max_timestamp)

    kwargs['ti'].xcom_push(key="emails_with_attachments", value=processed_emails)
    return len(processed_emails)

def classify_and_route(**kwargs):
    """Classify each email using AI and route to the appropriate downstream task.

    Returns one or more task IDs for the BranchPythonOperator:
      - "trigger_test_dag"       for emails requesting API testing
      - "reply_general_inquiry"  for general questions
      - "no_email_found_task"    when there are no emails to process
    """
    ti = kwargs['ti']
    emails = ti.xcom_pull(task_ids="fetch_unread_emails", key="emails_with_attachments")

    if not emails:
        logging.info("No emails to classify")
        return "no_email_found_task"

    CLASSIFICATION_SYSTEM_PROMPT = (
        "You are an email routing assistant for an automated API testing service. "
        "Classify the email intent into exactly one of four categories:\n"
        '- "scenario_testing": The sender describes a business flow, journey, or end-to-end '
        "scenario to test (e.g. 'test the buying flow: create product, stock it, buy it, check order'). "
        "Keywords: workflow, flow, journey, scenario, sequence, end-to-end, step-by-step, pipeline.\n"
        '- "api_testing": The sender wants standard API tests generated or executed. They may have '
        "attached a Postman collection (JSON), API documentation (PDF), or described APIs to test.\n"
        '- "postman_export": The sender wants a Postman collection generated from a previous test session. '
        "They are asking for test results to be exported or converted into a Postman collection. "
        "Keywords: postman, collection, export, send me the collection, convert to postman, download collection.\n"
        '- "general_inquiry": The sender is asking a general question (what can you do, help, '
        "status, how does this work, etc.) that does not require running the test pipeline.\n\n"
        "When in doubt between scenario_testing and api_testing, prefer api_testing.\n\n"
        'Return ONLY strict JSON: {"intent": "scenario_testing" or "api_testing" or "postman_export" or "general_inquiry", "reason": "..."}'
    )

    emails_to_test = []
    emails_to_reply = []
    emails_to_export = []

    for email in emails:
        subject = email["headers"].get("Subject", "")
        body_preview = (email.get("content") or "")[:2000]
        attachment_names = [a["filename"] for a in email.get("json_attachments", [])] + \
                           [a["filename"] for a in email.get("pdf_attachments", [])]

        user_prompt = (
            f"Subject: {subject}\n"
            f"Body:\n{body_preview}\n"
            f"Attachments: {', '.join(attachment_names) if attachment_names else 'none'}"
        )

        try:
            ai_response = get_ai_response(
                prompt=user_prompt,
                system_message=CLASSIFICATION_SYSTEM_PROMPT,
                model=MODEL_NAME,
                stream=False,
            )
            classification = extract_json_from_text(ai_response)
            intent = (classification or {}).get("intent", "general_inquiry")
            reason = (classification or {}).get("reason", "")
            logging.info(
                f"Email {email['id']} classified as '{intent}': {reason}"
            )
        except Exception as e:
            logging.error(f"AI classification failed for email {email['id']}: {e}")
            # Fallback: if there are any attachments (JSON or PDF), assume api_testing
            has_test_attachments = email.get("json_attachments") or email.get("pdf_attachments")
            intent = "api_testing" if has_test_attachments else "general_inquiry"
            logging.info(f"Fallback classification for email {email['id']}: {intent}")

        if intent == "scenario_testing":
            email["testing_type"] = "scenario"
            emails_to_test.append(email)
        elif intent == "api_testing":
            email["testing_type"] = "api_only"
            emails_to_test.append(email)
        elif intent == "postman_export":
            emails_to_export.append(email)
        else:
            emails_to_reply.append(email)

    ti.xcom_push(key="emails_to_test", value=emails_to_test)
    ti.xcom_push(key="emails_to_reply", value=emails_to_reply)
    ti.xcom_push(key="emails_to_export", value=emails_to_export)

    # Determine which downstream tasks to activate
    branches = []
    if emails_to_test:
        branches.append("trigger_test_dag")
    if emails_to_reply:
        branches.append("reply_general_inquiry")
    if emails_to_export:
        branches.append("trigger_postman_export")

    if not branches:
        return "no_email_found_task"

    logging.info(
        f"Routing: {len(emails_to_test)} email(s) to testing, "
        f"{len(emails_to_reply)} email(s) to general reply, "
        f"{len(emails_to_export)} email(s) to postman export"
    )
    return branches

def trigger_response_tasks(**kwargs):
    """Trigger the test case runner DAG for each email classified as api_testing."""
    ti = kwargs['ti']
    emails = ti.xcom_pull(task_ids="classify_and_route", key="emails_to_test")

    if not emails:
        logging.info("No emails classified for API testing → nothing to trigger")
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
            "email_content": email['content'],
            "testing_type": email.get('testing_type', 'api_only')
        }
        
        logging.info(
            f"Triggering api_test_case_eapi_test_executor_scenario_based executor for email {email['id']} with "
            f"{len(email['json_attachments'])} JSON file(s) and "
            f"{len(email['pdf_attachments'])} PDF file(s)"
        )

        TriggerDagRunOperator(
            task_id=task_id,
            trigger_dag_id="api_test_executor_scenario_based",
            conf=conf_data,
            wait_for_completion=False,
        ).execute(context=kwargs)

def reply_general_inquiry(**kwargs):
    """Generate an AI response and reply to emails classified as general inquiries."""
    ti = kwargs['ti']
    emails = ti.xcom_pull(task_ids="classify_and_route", key="emails_to_reply")

    if not emails:
        logging.info("No general inquiry emails to reply to")
        return

    service = authenticate_gmail()

    REPLY_SYSTEM_PROMPT = (
        "You are the API Testing Assistant powered by lowtouch.ai. "
        "A user has sent an email with a general question about the service. "
        "Write a helpful, concise, and professional HTML email reply.\n\n"
        "The service works as follows:\n"
        "- Users email a Postman collection (JSON file) and optionally API documentation (PDF) "
        "and a config.yaml with auth credentials.\n"
        "- The system automatically generates pytest-based API test cases using AI.\n"
        "- Tests are executed and a detailed HTML report is emailed back.\n"
        "- Supported auth types: header, basic, bearer, api_key, custom.\n"
        "- DELETE endpoints are never tested.\n"
        "- Each test has a single assertion for clarity.\n\n"
        "Reply to the user's question helpfully. Use simple HTML formatting "
        "(paragraphs, bullet lists). Do NOT include a subject line — just the body content."
    )

    for email in emails:
        subject = email["headers"].get("Subject", "")
        sender = email["headers"].get("From", "")
        message_id = email["headers"].get("Message-ID", "")
        references = email["headers"].get("References", "")
        body_preview = (email.get("content") or "")[:2000]

        user_prompt = (
            f"The user sent this email:\n"
            f"Subject: {subject}\n"
            f"Body:\n{body_preview}\n\n"
            f"Write a helpful reply."
        )

        try:
            reply_html = get_ai_response(
                prompt=user_prompt,
                system_message=REPLY_SYSTEM_PROMPT,
                model=MODEL_NAME,
                stream=False,
            )

            # Wrap in basic HTML if the AI didn't produce a full tag
            if "<p>" not in reply_html and "<div>" not in reply_html:
                reply_html = f"<p>{reply_html}</p>"

            # Determine recipients (reply to sender, CC others)
            recipients_info = extract_all_recipients(email)
            reply_to = sender
            cc_list = [
                addr for addr in recipients_info.get("cc", [])
                if addr.lower() != GMAIL_FROM_ADDRESS.lower()
            ]

            reply_subject = subject if subject.lower().startswith("re:") else f"Re: {subject}"

            result = send_email(
                service=service,
                recipient=reply_to,
                subject=reply_subject,
                body=reply_html,
                in_reply_to=message_id,
                references=references,
                from_address=GMAIL_FROM_ADDRESS,
                cc=cc_list if cc_list else None,
                thread_id=email["threadId"],
                agent_name="API Test Agent",
            )
            logging.info(
                f"Sent general inquiry reply to {reply_to} "
                f"(message_id={result.get('id', 'unknown')})"
            )

            mark_email_as_read(service, email["id"])

        except Exception as e:
            logging.error(
                f"Failed to reply to general inquiry email {email['id']}: {e}",
                exc_info=True,
            )


def trigger_postman_export_tasks(**kwargs):
    """Trigger the postman collection exporter DAG for each email requesting an export."""
    ti = kwargs['ti']
    emails = ti.xcom_pull(task_ids="classify_and_route", key="emails_to_export")

    if not emails:
        logging.info("No emails classified for Postman export → nothing to trigger")
        return

    for email in emails:
        task_id = f"trigger_postman_export_{email['id'].replace('-','_')}"

        conf_data = {
            "email_id": email['id'],
            "thread_id": email['threadId'],
            "email_headers": email['headers'],
            "email_content": email.get('content', ''),
        }

        logging.info(
            f"Triggering postman_collection_exporter for email {email['id']} "
            f"(thread: {email['threadId']})"
        )

        TriggerDagRunOperator(
            task_id=task_id,
            trigger_dag_id="postman_collection_exporter",
            conf=conf_data,
            wait_for_completion=False,
        ).execute(context=kwargs)


def no_email_found(**kwargs):
    """Log when no emails are found."""
    logging.info("No new emails found to process.")

readme_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mailbox_monitor.md')
try:
    with open(readme_path, 'r') as file:
        readme_content = file.read()
except FileNotFoundError:
    readme_content = None

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

    classify_route_task = BranchPythonOperator(
        task_id="classify_and_route",
        python_callable=classify_and_route,
    )

    trigger_test_runner = PythonOperator(
        task_id="trigger_test_dag",
        python_callable=trigger_response_tasks,
    )

    reply_inquiry_task = PythonOperator(
        task_id="reply_general_inquiry",
        python_callable=reply_general_inquiry,
    )

    trigger_postman_export_task = PythonOperator(
        task_id="trigger_postman_export",
        python_callable=trigger_postman_export_tasks,
    )

    no_email_found_task = PythonOperator(
        task_id="no_email_found_task",
        python_callable=no_email_found,
    )

    # Set task dependencies
    fetch_emails_task >> classify_route_task
    classify_route_task >> [trigger_test_runner, reply_inquiry_task, trigger_postman_export_task, no_email_found_task]