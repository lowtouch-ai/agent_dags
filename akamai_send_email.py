from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from datetime import datetime, timedelta
import base64
import json
import logging
import re
import os
from bs4 import BeautifulSoup
from email.utils import parsedate_to_datetime

from email.message import EmailMessage
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

CLOUD_ASSESS_FROM_ADDRESS = Variable.get("AKAMAI_CLOUD_ASSESS_FROM_ADDRESS")
GMAIL_CREDENTIALS = Variable.get("AKAMAI_CLOUD_ASSESS_GMAIL_CREDENTIALS")
LAST_PROCESSED_EMAIL_FILE = "/appz/cache/last_processed_email.json"
ATTACHMENT_DIR = "/appz/data/attachments/"
OLLAMA_HOST = "http://agentomatic:8000/"

def authenticate_gmail():
    try:
        creds = Credentials.from_authorized_user_info(json.loads(GMAIL_CREDENTIALS))
        service = build("gmail", "v1", credentials=creds)
        profile = service.users().getProfile(userId="me").execute()
        logged_in_email = profile.get("emailAddress", "")
        if logged_in_email.lower() != CLOUD_ASSESS_FROM_ADDRESS.lower():
            raise ValueError(f"Wrong Gmail account! Expected {CLOUD_ASSESS_FROM_ADDRESS}, but got {logged_in_email}")
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

def remove_quoted_text(text):
    """
    Remove quoted email thread history from the email body, keeping only the current message.
    Handles common email client quote patterns (e.g., 'On ... wrote:', '>', '---').
    """
    try:
        logging.info("Removing quoted text from email body")
        # Common patterns for quoted text
        patterns = [
            r'On\s.*?\swrote:',  # Standard 'On ... wrote:'
            r'-{2,}\s*Original Message\s*-{2,}',  # Outlook-style
            r'_{2,}\s*',  # Some clients use underscores
            r'From:\s*.*?\n',  # Quoted 'From:' headers
            r'>.*?\n'  # Lines starting with '>' (quoted replies)
        ]
        
        # Split text by any of the patterns
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
            if match:
                text = text[:match.start()].strip()
        
        # Remove any remaining lines starting with '>'
        lines = text.split('\n')
        cleaned_lines = [line for line in lines if not line.strip().startswith('>')]
        text = '\n'.join(cleaned_lines).strip()
        logging.info(f"Text after removing quoted text: {text[:100] if text else ''}...")
        return text if text else "No content after removing quoted text"
    except Exception as e:
        logging.error(f"Error in remove_quoted_text: {str(e)}")
        return text.strip()

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

        client = Client(host=OLLAMA_HOST, headers={'x-ltai-client': 'CloudAssess'})
        logging.debug(f"Connecting to Ollama at {OLLAMA_HOST} with model 'llama3'")

        messages = []
        if conversation_history:
            for history_item in conversation_history:
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
            model='akamai-presales:0.3',  # Adapted model for general use
            messages=messages,
            stream=False
        )
        logging.info(f"Raw response from agent: {str(response)[:500]}...")

        if not (hasattr(response, 'message') and hasattr(response.message, 'content')):
            logging.error("Response lacks expected 'message.content' structure")
            return "<html><body>Invalid response format from AI. Please try again later.</body></html>"
        
        ai_content = response.message.content
        logging.info(f"Full message content from agent: {ai_content[:500]}...")


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
        msg["From"] = f"Akamai-presales via lowtouch.ai <{CLOUD_ASSESS_FROM_ADDRESS}>"
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
    """Step 1: Process message from email, handle attachments or errors."""
    email_data = context['dag_run'].conf.get("email_data", {})
    
    service = authenticate_gmail()
    if not service:
        logging.error("Gmail authentication failed, aborting.")
        return "Gmail authentication failed"

    # detect attachment type
    att_type = "none"
    for att in email_data.get("attachments", []):
        mime = att.get("mime_type", "")
        if "pdf" in mime:
            att_type = "pdf"
            break
        elif "excel" in mime or "spreadsheet" in mime:
            att_type = "excel"
        elif "image" in mime:
            att_type = "image"

    ti.xcom_push(key="attachment_type", value=att_type)
    logging.info(f"Attachment type: {att_type}")

    # get thread + content
    thread = get_email_thread(service, email_data, CLOUD_ASSESS_FROM_ADDRESS)
    history = thread[:-1] if thread else []
    current = thread[-1]["content"] if thread else email_data.get("content", "")

    soup = BeautifulSoup(current, "html.parser")
    current = remove_quoted_text(soup.get_text(separator=" ", strip=True))

    att_content = ""
    has_valid_attachment = False
    for att in email_data.get("attachments", []):
        extracted = att.get("extracted_content", {})
        content = extracted.get("content")

        if content and isinstance(content, str) and content.strip():
        # Only valid excel/pdf extraction will be appended
            att_content += f"\n{content}\n"
            has_valid_attachment = True
    
    # Handle no attachment edge case
    if not has_valid_attachment:
        att_content = "No valid Excel or PDF attachment found."  

    current += f"\n{att_content}"

    prompt = f"Assess the cloud details:\n{current}"

    response = get_ai_response(prompt, conversation_history=history)
    
    logging.info(f"Step 1 completed: {response[:200]}...")
    return response

# ----------------------------------------------------------
# BRANCH SELECTOR
# ----------------------------------------------------------
def route_based_on_attachment(ti, **kwargs):
    att = ti.xcom_pull(key="attachment_type")
    if att == "pdf":
        return "step_pdf_process"
    if att == "excel":
        return "step_excel_process"
    if att == "image":
        return "step_image_process"
    return "step_excel_process"

# ----------------------------------------------------------
# PDF Processor
# ----------------------------------------------------------
def step_pdf_process(ti, **context):
    content = ti.xcom_pull(key="email_content")
    hist = ti.xcom_pull(key="conversation_history") or []

    prompt = f"""
        Client submitted a PDF. Produce a detailed **Akamai Cloud Comparison Report**.
        Input:{content}
        Return clean HTML (without <html> wrapper).
    """
    response = get_ai_response(prompt, conversation_history=hist)
    ti.xcom_push(key="markdown_email_content", value=response)
    return response

# ----------------------------------------------------------
# Excel Processor
# ----------------------------------------------------------
def step_excel_process(ti, **context):
    sender = ti.xcom_pull(key="sender_name") or "Client"
    step1 = ti.xcom_pull(key="step_1_response")
    hist = ti.xcom_pull(key="conversation_history")

    prompt = f"""
        Generate a professional Cloud Assessment email addressed to {sender}.
        Assessment content:
        {step1}

        Return ONLY HTML body, no <html> tag.
    """

    response = get_ai_response(prompt, conversation_history=hist)
    ti.xcom_push(key="markdown_email_content", value=response)
    return response

# ----------------------------------------------------------
# Image Processor
# ----------------------------------------------------------
def step_image_process(ti, **context):
    hist = ti.xcom_pull(key="conversation_history")
    email_data = context['dag_run'].conf.get("email_data")

    images = []
    for att in email_data.get("attachments", []):
        if "image" in att.get("mime_type", ""):
            images.append(att["base64_content"])

    prompt = """
        Extract cost and cloud info from images, compare with Akamai, return HTML only.
    """

    response = get_ai_response(prompt, images=images, conversation_history=hist)
    ti.xcom_push(key="markdown_email_content", value=response)
    return response

def step_3_convert_to_html(ti, **context):
    """Step 4: Convert Markdown email body to standards-compliant HTML for email rendering."""
    try:
        markdown_email = ti.xcom_pull(key="markdown_email_content") or "No email content available."
        prompt = f"""
            Convert the following Markdown-formatted email body into a **well-structured and standards-compliant HTML document** for email rendering.

            ### Requirements:
            1. Output must be **pure valid HTML** (no Markdown traces like `**`, `#`, or backticks).
            2. Begin with:
            ```html
            <html><head><title>Cloud Assessment Report</title></head><body>
            ```
            and end with:
            ```html
            </body></html>
            ```
            3. Preserve **all structure and content exactly** as provided — do not modify or shorten any section.
            4. Use **semantic HTML tags**:
               - Markdown headings → `<h1>`, `<h2>`, `<h3>`, `<h4>`
               - Paragraphs → `<p>`
               - Bullet/numbered lists → `<ul>` / `<ol>` with `<li>`
               - Code blocks → `<pre><code>` (preserve indentation and SQL syntax)
               - Bold text → `<strong>`
               - Italic text → `<em>`
               - Tables → `<table border="1" style="border-collapse: collapse; width:100%; border: 1px solid #333;">`
                 Include `<tr>`, `<th>`, `<td>` with visible borders.
               - Links → `<a href="...">` (preserve mailto links like `mailto:cloud-assess@lowtouch.ai`)
            5. Maintain **consistent indentation and spacing** for readability.
            6. Ensure **Markdown characters (like `**`, `_`, `#`, ```) are fully removed** and replaced with proper HTML formatting.

            ### Example formatting rules:
            - `**Assessment Report**` → `<h2>Assessment Report</h2>`
            - `### Conclustion` → `<h3>Conclustion</h3>`

            Now, convert the following Markdown email body to clean, valid, and visually structured HTML:

            {markdown_email}

            Output **only** the final HTML code (no Markdown, no commentary).
            """
        html_response = get_ai_response(prompt)
        if not html_response.startswith('<html>'):
            html_response = f"<html><head><title>Cloud Assessment Report</title></head><body>{html_response}</body></html>"
        ti.xcom_push(key="final_html_content", value=html_response)
        logging.info(f"HTML email content generated: {html_response[:200]}...")
        return html_response
    except Exception as e:
        logging.error(f"Error in convert_to_html: {str(e)}")
        raise

def step_4_send_email(ti, **context):
    """Step 3: Send the final email."""
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
        subject = f"Re: {email_data['headers'].get('Subject', 'Cloud Assessment')}"
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
        logging.error(f"Error in step_3_send_email: {str(e)}")
        return f"Error sending email: {str(e)}"

readme_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'email_responder.md')
readme_content = ""
try:
    with open(readme_path, 'r') as file:
        readme_content = file.read()
except FileNotFoundError:
    readme_content = "Cloud assessment response DAG"

with DAG(
    "akamai_presales_send_response_email",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    doc_md=readme_content,
    tags=["email", "cloud_assess", "send", "response"]
) as dag:
    
    task_1 = PythonOperator(
        task_id="step_1_process_email",
        python_callable=step_1_process_email,
        provide_context=True
    )

    branch = BranchPythonOperator(
        task_id="branch_on_attachment",
        python_callable=route_based_on_attachment,
        provide_context=True
    )

    pdf_task = PythonOperator(
        task_id="step_pdf_process",
        python_callable=step_pdf_process,
        provide_context=True
    )

    excel_task = PythonOperator(
        task_id="step_excel_process",
        python_callable=step_excel_process,
        provide_context=True
    )

    image_task = PythonOperator(
        task_id="step_image_process",
        python_callable=step_image_process,
        provide_context=True
    )

    join = PythonOperator(
        task_id="join_after_branch",
        python_callable=lambda: logging.info("Branch complete"),
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    html_task = PythonOperator(
        task_id="step_3_convert_to_html",
        python_callable=step_3_convert_to_html,
        provide_context=True
    )

    task_4 = PythonOperator(
        task_id="step_4_send_email",
        python_callable=step_4_send_email,
        provide_context=True
    )

    # DAG FLOW
    task_1 >> branch >> [pdf_task, excel_task, image_task] >> join >> html_task >> task_4