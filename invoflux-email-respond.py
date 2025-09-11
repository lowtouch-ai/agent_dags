from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import base64
import json
import logging
import re
from email.message import EmailMessage  # Add this import
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from ollama import Client
from email import message_from_bytes
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from bs4 import BeautifulSoup
import os
from email.utils import parsedate_to_datetime

# Configure detailed logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

default_args = {
    "owner": "lowtouch.ai_developers",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 18),
    "retries": 0,
    "retry_delay": timedelta(seconds=15),
}

ODOO_FROM_ADDRESS = Variable.get("INVOFLUX_FROM_ADDRESS")
GMAIL_CREDENTIALS = Variable.get("INVOFLUX_GMAIL_CREDENTIALS")
LAST_PROCESSED_EMAIL_FILE = "/appz/cache/last_processed_email.json"
ATTACHMENT_DIR = "/appz/data/attachments/"
OLLAMA_HOST = "http://agentomatic:8000/"

def authenticate_gmail():
    try:
        creds = Credentials.from_authorized_user_info(json.loads(GMAIL_CREDENTIALS))
        service = build("gmail", "v1", credentials=creds)
        profile = service.users().getProfile(userId="me").execute()
        logged_in_email = profile.get("emailAddress", "")
        if logged_in_email.lower() != ODOO_FROM_ADDRESS.lower():
            raise ValueError(f"Wrong Gmail account! Expected {ODOO_FROM_ADDRESS}, but got {logged_in_email}")
        logging.info(f"Authenticated Gmail account: {logged_in_email}")
        return service
    except Exception as e:
        logging.error(f"Failed to authenticate Gmail: {str(e)}")
        return None

def decode_email_payload(msg):
    try:
        if msg.is_multipart():
            for part in msg.walk():
                content_type = part.get_content_type()
                if content_type in ["text/plain", "text/html"]:
                    try:
                        body = part.get_payload(decode=True).decode()
                        return body
                    except UnicodeDecodeError:
                        body = part.get_payload(decode=True).decode('latin-1')
                        return body
        else:
            try:
                body = msg.get_payload(decode=True).decode()
                return body
            except UnicodeDecodeError:
                body = part.get_payload(decode=True).decode('latin-1')
                return body
        return ""
    except Exception as e:
        logging.error(f"Error decoding email payload: {str(e)}")
        return ""

def get_email_thread(service, email_data, from_address):
    """Retrieve and format the email thread as a conversation list with user and response roles."""
    try:
        thread_id = email_data.get("threadId")
        message_id = email_data["headers"].get("Message-ID", "")
        logging.info(f"Processing email thread for message ID: {message_id}")

        if not thread_id:
            query_result = service.users().messages().list(userId="me", q=f"rfc822msgid:{message_id}").execute()
            messages = query_result.get("messages", [])
            if messages:
                message = service.users().messages().get(userId="me", id=messages[0]["id"]).execute()
                thread_id = message.get("threadId")

        if not thread_id:
            logging.warning(f"No thread ID found for message ID {message_id}. Treating as a single email.")
            raw_message = service.users().messages().get(userId="me", id=email_data["id"], format="raw").execute()
            msg = message_from_bytes(base64.urlsafe_b64decode(raw_message["raw"]))
            content = decode_email_payload(msg)
            headers = {header["name"]: header["value"] for header in email_data.get("payload", {}).get("headers", [])}
            sender = headers.get("From", "").lower()
            role = "user" if sender != from_address.lower() else "assistant"
            return [{"role": role, "content": content.strip()}] if content.strip() else []

        thread = service.users().threads().get(userId="me", id=thread_id).execute()
        conversation = []
        logging.info(f"Retrieved thread with ID: {thread_id} containing {len(thread.get('messages', []))} messages")
        messages_with_dates = []
        for msg in thread.get("messages", []):
            raw_msg = base64.urlsafe_b64decode(msg["raw"]) if "raw" in msg else None
            if not raw_msg:
                raw_message = service.users().messages().get(userId="me", id=msg["id"], format="raw").execute()
                raw_msg = base64.urlsafe_b64decode(raw_message["raw"])
            
            email_msg = message_from_bytes(raw_msg)
            headers = {header["name"]: header["value"] for header in msg.get("payload", {}).get("headers", [])}
            content = decode_email_payload(email_msg)
            
            if not content.strip():
                continue
                
            sender = headers.get("From", "").lower()
            role = "user" if sender != from_address.lower() else "assistant"
            date_str = headers.get("Date", "")
            
            messages_with_dates.append({
                "role": role,
                "content": content.strip(),
                "date": date_str,
                "message_id": headers.get("Message-ID", "")
            })

        try:
            messages_with_dates.sort(key=lambda x: parsedate_to_datetime(x["date"]) if x["date"] else datetime.min)
        except Exception as e:
            logging.warning(f"Error sorting by date, using original order: {str(e)}")
        
        for msg in messages_with_dates:
            conversation.append({
                "role": msg["role"],
                "content": msg["content"]
            })

        logging.info(f"Retrieved complete thread with {len(conversation)} messages")
        logging.info(f"Full conversation {conversation}")
        return conversation
        
    except Exception as e:
        logging.error(f"Error retrieving email thread: {str(e)}")
        return []

def get_ai_response(prompt, images=None, conversation_history=None):
    try:
        logging.debug(f"Query received: {prompt}")
        
        if not prompt or not isinstance(prompt, str):
            return "<html><body>Invalid input provided. Please enter a valid query.</body></html>"

        client = Client(host=OLLAMA_HOST, headers={'x-ltai-client': 'Invoflux-email'})
        logging.debug(f"Connecting to Ollama at {OLLAMA_HOST} with model 'InvoFlux:0.3'")

        messages = []
        if conversation_history:
            for history_item in conversation_history:
                # Handle email thread history format
                messages.append({
                    "role": history_item["role"],
                    "content": history_item["content"]
                })
        
        user_message = {"role": "user", "content": prompt}
        if images:
            logging.info(f"Images provided: {len(images)}")
            user_message["images"] = images
        messages.append(user_message)

        response = client.chat(
            model='invoflux-email:0.3',
            messages=messages,
            stream=False
        )
        logging.info(f"Raw response from agent: {str(response)[:500]}...")

        if not (hasattr(response, 'message') and hasattr(response.message, 'content')):
            logging.error("Response lacks expected 'message.content' structure")
            return "<html><body>Invalid response format from AI. Please try again later.</body></html>"
        
        ai_content = response.message.content
        logging.info(f"Full message content from agent: {ai_content[:500]}...")

        if "technical difficulties" in ai_content.lower() or "error" in ai_content.lower():
            logging.warning("AI response contains potential error message")
            return "<html><body>Unexpected response received. Please contact support.</body></html>"

        ai_content = re.sub(r'```html\n|```', '', ai_content).strip()
        if not ai_content.strip():
            logging.warning("AI returned empty content")
            return "<html><body>No response generated. Please try again later.</body></html>"

        if not ai_content.strip().startswith('<!DOCTYPE') and not ai_content.strip().startswith('<html'):
            logging.warning("Response doesn't appear to be proper HTML, wrapping it")
            ai_content = f"<html><body>{ai_content}</body></html>"

        return ai_content.strip()

    except Exception as e:
        logging.error(f"Error in get_ai_response: {str(e)}")
        return f"<html><body>An error occurred while processing your request: {str(e)}</body></html>"

def send_email(service, recipient, subject, body, in_reply_to, references):
    try:
        logging.debug(f"Preparing email to {recipient} with subject: {subject}")
        msg = MIMEMultipart()
        msg["From"] = f"InvoFlux via lowtouch.ai <{ODOO_FROM_ADDRESS}>"
        msg["To"] = recipient
        msg["Subject"] = subject
        msg["In-Reply-To"] = in_reply_to
        msg["References"] = references
        msg.attach(MIMEText(body, "html"))
        raw_msg = base64.urlsafe_b64encode(msg.as_string().encode("utf-8")).decode("utf-8")
        result = service.users().messages().send(userId="me", body={"raw": raw_msg}).execute()
        logging.info(f"Email sent successfully: {result}")
        return result
    except Exception as e:
        logging.error(f"Failed to send email: {str(e)}")
        return None

def step_1_process_email(ti, **context):
    """Step 1: Process message from email with image attachment"""
    email_data = context['dag_run'].conf.get("email_data", {})
    
    service = authenticate_gmail()
    if not service:
        logging.error("Gmail authentication failed, aborting.")
        return "Gmail authentication failed"
    
    complete_email_thread = get_email_thread(service, email_data,ODOO_FROM_ADDRESS)
    image_attachments = []
    
    from_header = email_data["headers"].get("From", "Unknown <unknown@example.com>")
    sender_name = "Unknown"
    sender_email = "unknown@example.com"
    name_email_match = re.match(r'^(.*?)\s*<(.*?@.*?)>$', from_header)
    if name_email_match:
        sender_name = name_email_match.group(1).strip() or "Unknown"
        sender_email = name_email_match.group(2).strip()
    elif re.match(r'^.*?@.*?$', from_header):
        sender_email = from_header.strip()
        sender_name = sender_email.split('@')[0]

    # Collect image attachments
    if email_data.get("attachments"):
        logging.info(f"Number of attachments: {len(email_data['attachments'])}")
        for attachment in email_data["attachments"]:
            if "base64_content" in attachment and attachment["base64_content"]:
                image_attachments.append(attachment["base64_content"])
                logging.info(f"Found base64 image attachment: {attachment['filename']}")
    
    # Extract attachment content (e.g., from PDFs)
    attachment_content = ""
    if email_data.get("attachments"):
        for attachment in email_data["attachments"]:
            if "extracted_content" in attachment and "content" in attachment["extracted_content"]:
                attachment_content += f"\nAttachment ({attachment['filename']}):\n{attachment['extracted_content']['content']}\n"
    
    # ADDED: Split thread into history and current content
    if len(complete_email_thread) > 1:
        conversation_history = complete_email_thread[:-1]  # All previous messages
        current_content = complete_email_thread[-1]["content"]  # Current message
    else:
        conversation_history = []  # No previous conversation
        current_content = complete_email_thread[0]["content"] if complete_email_thread else email_data.get("content", "").strip()
    
    # ADDED: Log conversation history
    logging.info(f"Complete conversation history contains {len(conversation_history)} messages")
    logging.info(f"Current content: {current_content}...")
    for i, msg in enumerate(conversation_history[:3]):  # Log first 3 for brevity
        logging.info(f"History message {i+1}: Role={msg['role']}, Content={msg['content'][:100]}...")
    
    # ADDED: Clean current content
    if current_content:
        soup = BeautifulSoup(current_content, "html.parser")
        current_content = soup.get_text(separator=" ", strip=True)
    
    # ADDED: Append attachment content to current content
    if attachment_content:
        current_content += f"\n{attachment_content}"

    logging.info(f"final current content: {current_content}")

    prompt = f"Extract the invoice and prepare for system entry:\n\n{current_content}\n\nNote: Extract the invoice details from the image is enough and ask for confirmation"

    logging.info(f"Final prompt to AI: {prompt}...")
    
    response = get_ai_response(prompt, images=image_attachments if image_attachments else None, conversation_history=conversation_history)

    ti.xcom_push(key="step_1_prompt", value=prompt)
    ti.xcom_push(key="step_1_response", value=response)
    ti.xcom_push(key="conversation_history", value=conversation_history + [{"role": "user", "content": prompt}, {"role": "assistant", "content": response}])
    
    logging.info(f"Step 1 completed: {response[:200]}...")
    return response

def step_2_create_vendor_bill(ti, **context):
    """Step 2: Extract and prepare for system entry"""
    step_1_response = ti.xcom_pull(key="step_1_response")
    
    prompt = "Create the Vendor bill"
    
    # Pass only the previous step's response as history
    history = [{"role": "assistant", "content": step_1_response}] if step_1_response else []
    response = get_ai_response(prompt, conversation_history=history)
    
    # Append to history for consistency with downstream tasks
    full_history = ti.xcom_pull(key="conversation_history") or []
    full_history.append({"role": "user", "content": prompt})
    full_history.append({"role": "assistant", "content": response})
    ti.xcom_push(key="step_2_prompt", value=prompt)
    ti.xcom_push(key="step_2_response", value=response)
    ti.xcom_push(key="conversation_history", value=full_history)
    
    logging.info(f"Step 2 completed: {response[:200]}...")
    return response

def step_3_compose_email(ti, **context):
    """Step 3: Create the vendor bill"""
    step_2_response = ti.xcom_pull(key="step_2_response")
    sender_name = context['dag_run'].conf.get("email_data", {}).get("headers", {}).get("From", "Valued Customer")
    name_email_match = re.match(r'^(.*?)\s*<(.*?@.*?)>$', sender_name)
    if name_email_match:
        sender_name = name_email_match.group(1).strip() or "Valued Customer"
    else:
        sender_name = "Valued Customer"
    logging.info(f"Sender name extracted: {sender_name}")
    
    prompt = f"""
        Compose a professional and human-like business email in American English, written in the tone of a senior Customer Success Manager, to notify a vendor about the outcome of an invoice submission (Posted, Draft, or Failed).
        Address the email to the vendor using their name: '{sender_name}'.
        The email must include:
        - A clear introductory line acknowledging the invoice receipt with the invoice number and vendor name (if available), followed by a short sentence stating the current status of the invoice (Posted, Draft, or Failed).
        - A natural explanation of the status outcome (including Invoice Number and, for Draft or Failed, specific issues like price mismatch, missing PO, unreadable file, duplicate invoice, or unrecognized product in a bulleted list).
        - A concise summary of invoice details (Invoice Number, Invoice Date, Due Date, Internal Invoice ID, Status, Vendor Name if available, Purchase Order if available, Currency, Subtotal, Tax with rate if available, Total Amount) in paragraph or bullet format.
        - A product line item table **with borders** (including Item Description, Quantity, Unit Price, Tax, Total Price), placed immediately after the invoice summary.
        - If the invoice status is Draft or Failed, include a short, natural-language paragraph below the product table briefly summarizing the **validation issues**.
        - A naturally worded sentence or short paragraph explaining the next steps based on the invoice status:
            - For **Posted**, confirm the invoice has been successfully entered into the payment cycle.
            - For **Draft** or **Failed**, kindly request corrections and resubmission to invoflux-agent-8013@lowtouch.ai.
        - A polite closing paragraph offering further assistance, mentioning the contact email invoflux-agent-8013@lowtouch.ai, and signed with 'Invoice Processing Team, InvoFlux'.
        Use only clean, valid HTML for the email body without any section headers (e.g., no 'Status-Based Message', 'Invoice Summary', or 'Validation Issues Identified'). Avoid technical or template-style formatting and placeholders (e.g., '[Invoice Number]'). The email should read as if it was personally written.
        Return only the HTML body, and nothing else.
    """
    # Pass only the previous step's response as history
    history = [{"role": "assistant", "content": step_2_response}] if step_2_response else []
    response = get_ai_response(prompt, conversation_history=history)
    # Clean the HTML response
    cleaned_response = re.sub(r'```html\n|```', '', response).strip()
    
    if not cleaned_response.strip().startswith('<!DOCTYPE') and not cleaned_response.strip().startswith('<html'):
        if not cleaned_response.strip().startswith('<'):
            cleaned_response = f"<html><body>{cleaned_response}</body></html>"
    
    # Append to history for consistency with downstream tasks
    full_history = ti.xcom_pull(key="conversation_history") or []
    full_history.append({"role": "user", "content": prompt})
    full_history.append({"role": "assistant", "content": response})
    ti.xcom_push(key="step_3_prompt", value=prompt)
    ti.xcom_push(key="step_3_response", value=response)
    ti.xcom_push(key="conversation_history", value=full_history)
    ti.xcom_push(key="final_html_content", value=cleaned_response)
    
    logging.info(f"Step 3 completed: {response[:200]}...")
    return response


def step_4_send_email(ti, **context):
    """Step 5: Send the final email"""
    try:
        email_data = context['dag_run'].conf.get("email_data", {})
        if not email_data:
            logging.warning("No email data received! This DAG was likely triggered manually.")
            return "No email data available"
        
        final_html_content = ti.xcom_pull(key="final_html_content")
        if not final_html_content:
            logging.error("No final HTML content found from previous steps")
            return "Error: No content to send"
        
        service = authenticate_gmail()
        if not service:
            logging.error("Gmail authentication failed, aborting email response.")
            return "Gmail authentication failed"
        
        sender_email = email_data["headers"].get("From", "")
        subject = f"Re: {email_data['headers'].get('Subject', 'Invoice Processing')}"
        in_reply_to = email_data["headers"].get("Message-ID", "")
        references = email_data["headers"].get("References", "")
        
        result = send_email(
            service, sender_email, subject, final_html_content,
            in_reply_to, references
        )
        
        if result:
            logging.info(f"Email sent successfully to {sender_email}")
            return f"Email sent successfully to {sender_email}"
        else:
            logging.error("Failed to send email")
            return "Failed to send email"
            
    except Exception as e:
        logging.error(f"Error in step_5_send_email: {str(e)}")
        return f"Error sending email: {str(e)}"

readme_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'email_responder.md')
readme_content = ""
try:
    with open(readme_path, 'r') as file:
        readme_content = file.read()
except FileNotFoundError:
    readme_content = "Multi-step invoice processing DAG"

with DAG(
    "invoflux_send_message_email",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    doc_md=readme_content,
    tags=["email", "shared", "send", "message", "invoice"]
) as dag:
    
    task_1 = PythonOperator(
        task_id="step_1_process_email",
        python_callable=step_1_process_email,
        provide_context=True
    )
    
    task_2 = PythonOperator(
        task_id="step_2_create_vendor_bill",
        python_callable=step_2_create_vendor_bill,
        provide_context=True
    )
    
    task_3 = PythonOperator(
        task_id="step_3_compose_email",
        python_callable=step_3_compose_email,
        provide_context=True
    )
    
    
    task_4 = PythonOperator(
        task_id="step_4_send_email",
        python_callable=step_4_send_email,
        provide_context=True
    )
    
    task_1 >> task_2 >> task_3 >> task_4 
