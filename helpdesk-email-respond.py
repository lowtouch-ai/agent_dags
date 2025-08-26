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
from email import message_from_bytes
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from bs4 import BeautifulSoup
import os

# Configure detailed logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

default_args = {
    "owner": "lowtouch.ai_developers",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 18),
    "retries": 0,
    "retry_delay": timedelta(seconds=15),
}

HELPDESK_FROM_ADDRESS = Variable.get("HELPDESK_FROM_ADDRESS")
HELPDESK_GMAIL_CREDENTIALS = Variable.get("HELPDESK_GMAIL_CREDENTIALS")
LAST_PROCESSED_EMAIL_FILE = "/appz/cache/last_processed_email.json"
ATTACHMENT_DIR = "/appz/data/attachments/"
OLLAMA_HOST = "http://agentomatic:8000/"

def authenticate_gmail():
    try:
        creds = Credentials.from_authorized_user_info(json.loads(HELPDESK_GMAIL_CREDENTIALS))
        service = build("gmail", "v1", credentials=creds)
        profile = service.users().getProfile(userId="me").execute()
        logged_in_email = profile.get("emailAddress", "")
        if logged_in_email.lower() != HELPDESK_FROM_ADDRESS.lower():
            raise ValueError(f"Wrong Gmail account! Expected {HELPDESK_FROM_ADDRESS}, but got {logged_in_email}")
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

def get_email_thread(service, email_data):
    try:
        thread_id = email_data.get("threadId")
        message_id = email_data["headers"].get("Message-ID", "")
        logging.info(f"Email data body is: {json.dumps(email_data, indent=2)}")
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
            return [{
                "headers": {header["name"]: header["value"] for header in email_data.get("payload", {}).get("headers", [])},
                "content": decode_email_payload(msg)
            }]

        thread = service.users().threads().get(userId="me", id=thread_id).execute()
        email_thread = []
        for msg in thread.get("messages", []):
            raw_msg = base64.urlsafe_b64decode(msg["raw"]) if "raw" in msg else None
            if not raw_msg:
                raw_message = service.users().messages().get(userId="me", id=msg["id"], format="raw").execute()
                raw_msg = base64.urlsafe_b64decode(raw_message["raw"])
            email_msg = message_from_bytes(raw_msg)
            headers = {header["name"]: header["value"] for header in msg.get("payload", {}).get("headers", [])}
            content = decode_email_payload(email_msg)
            email_thread.append({
                "headers": headers,
                "content": content.strip()
            })
        email_thread.sort(key=lambda x: x["headers"].get("Date", ""), reverse=False)
        logging.debug(f"Retrieved thread with {len(email_thread)} messages")
        return email_thread
    except Exception as e:
        logging.error(f"Error retrieving email thread: {str(e)}")
        return []

def get_ai_response(prompt, images=None, conversation_history=None):
    try:
        logging.debug(f"Query received: {prompt}")
        
        if not prompt or not isinstance(prompt, str):
            return "<html><body>Invalid input provided. Please enter a valid query.</body></html>"

        client = Client(host=OLLAMA_HOST, headers={'x-ltai-client': 'help-desk-agent'})
        logging.debug(f"Connecting to Ollama at {OLLAMA_HOST} with model 'help-desk-agent:0.3'")

        messages = []
        if conversation_history:
            for history_item in conversation_history:
                messages.append({"role": "user", "content": history_item["prompt"]})
                messages.append({"role": "assistant", "content": history_item["response"]})
        
        user_message = {"role": "user", "content": prompt}
        if images:
            logging.info(f"Images provided: {len(images)}")
            user_message["images"] = images
        messages.append(user_message)

        response = client.chat(
            model='help-desk-agent:0.3',
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
        msg["From"] = f"HelpDesk via lowtouch.ai <{HELPDESK_FROM_ADDRESS}>"
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
    
    email_thread = get_email_thread(service, email_data)
    image_attachments = []
    
    if email_data.get("attachments"):
        logging.info(f"Number of attachments: {len(email_data['attachments'])}")
        for attachment in email_data["attachments"]:
            if "base64_content" in attachment and attachment["base64_content"]:
                image_attachments.append(attachment["base64_content"])
                logging.info(f"Found base64 image attachment: {attachment['filename']}")
    
    thread_history = ""
    for idx, email in enumerate(email_thread, 1):
        email_content = email.get("content", "").strip()
        email_from = email["headers"].get("From", "Unknown")
        email_date = email["headers"].get("Date", "Unknown date")
        if email_content:
            soup = BeautifulSoup(email_content, "html.parser")
            email_content = soup.get_text(separator=" ", strip=True)
        thread_history += f"Email {idx} (From: {email_from}, Date: {email_date}):\n{email_content}\n\n"
    logging.info(f"Email thread history: {thread_history}...")
    current_content = email_data.get("content", "").strip()
    if current_content:
        soup = BeautifulSoup(current_content, "html.parser")
        current_content = soup.get_text(separator=" ", strip=True)
    thread_history += f"Current Email (From: {email_data['headers'].get('From', 'Unknown')}):\n{current_content}\n"
    
    if email_data.get("attachments"):
        attachment_content = ""
        for attachment in email_data["attachments"]:
            if "extracted_content" in attachment and "content" in attachment["extracted_content"]:
                attachment_content += f"\nAttachment ({attachment['filename']}):\n{attachment['extracted_content']['content']}\n"
        thread_history += f"\n{attachment_content}" if attachment_content else ""
    
    prompt = f"""extract the content from the image and review the content and suggest fixes. Content: \n{thread_history}
    # outptu format:
    # Analysis of the Problem
    ..
    # Possible Root Causes
    ..
    # Suggested Solution Steps
    ...
    """
    
    response = get_ai_response(prompt, images=image_attachments if image_attachments else None)
    
    ti.xcom_push(key="step_1_prompt", value=prompt)
    ti.xcom_push(key="step_1_response", value=response)
    ti.xcom_push(key="conversation_history", value=[{"prompt": prompt, "response": response}])
    
    logging.info(f"Step 1 completed: {response[:200]}...")
    return response



def step_2_compose_email(ti, **context):
    """Step 3: Compose email based on the AI response"""
    history = ti.xcom_pull(key="conversation_history")
    
    prompt = """
     
        Compose a professional and human-like business email in American English, written in the tone of a L1 support agent, with the above content with analysis of the problem, possible root causes, and suggested solution steps.
        - replace sender_name with the actual sender's name from the email thread.
        - The email should be concise, clear, and easy to understand for a non-technical audience
        - The email should be having a polite closing paragraph offering further assistance, mentioning the contact email helpdeskagent-9228@lowtouch.ai.
        - Use only clean, valid HTML for the email body without any section headers. Avoid technical or template-style formatting and placeholders. The email should read as if it was personally written.
        - Return only the HTML body, and nothing else.
        - strictluy use the following outptut format, do not deviate from this
        **outptut format**
        ```html
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Problem Analysis Email</title>
            <style>
                body {
                    font-family: Arial, sans-serif;
                    line-height: 1.6;
                    color: #333;
                    max-width: 600px;
                    margin: 0 auto;
                    padding: 20px;
                }
                h1, h2 {
                    color: #005555;
                }
                .container {
                    background-color: #f9f9f9;
                    padding: 20px;
                    border-radius: 5px;
                }
                .footer {
                    margin-top: 20px;
                    font-size: 0.9em;
                    color: #777;
                }
            </style>
        </head>
        <body>
            <div class="container">
                <h1>Problem Analysis Report</h1>
                <p>Dear sender_name,</p>
                <p>Please find below the detailed analysis of the issue, including potential root causes and suggested steps for resolution.</p>

                <h2>Analysis of the Problem</h2>
                <p>...</p>

                <h2>Possible Root Causes</h2>
                <p>...</p>

                <h2>Suggested Solution Steps</h2>
                <p>...</p>

                <p>Thank you for your attention to this matter. Please let me know if you need further details or assistance in implementing the suggested solutions.</p>

                <div class="footer">
                    <p>Best regards,</p>
                    <p>Help desk suport</p>
                </div>
            </div>
        </body>
        </html>
        ```
        """
    
    response = get_ai_response(prompt, conversation_history=history)
    # Clean the HTML response
    cleaned_response = re.sub(r'```html\n|```', '', response).strip()
    
    if not cleaned_response.strip().startswith('<!DOCTYPE') and not cleaned_response.strip().startswith('<html'):
        if not cleaned_response.strip().startswith('<'):
            cleaned_response = f"<html><body>{cleaned_response}</body></html>"
    
    history.append({"prompt": prompt, "response": response})
    ti.xcom_push(key="step_3_prompt", value=prompt)
    ti.xcom_push(key="step_3_response", value=response)
    ti.xcom_push(key="conversation_history", value=history)
    ti.xcom_push(key="final_html_content", value=cleaned_response)
    
    logging.info(f"Step 3 completed: {response[:200]}...")
    return response


def step_3_send_email(ti, **context):
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
        subject = f"Re: {email_data['headers'].get('Subject', 'HelpDesk Processing')}"
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

readme_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'helpdesk_email_responder.md')
readme_content = ""
try:
    with open(readme_path, 'r') as file:
        readme_content = file.read()
except FileNotFoundError:
    readme_content = "Multi-step Helpdesk DAG"

with DAG(
    "helpdesk_send_message_email",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    doc_md=readme_content,
    tags=["email", "shared", "send", "message", "helpdesk"]
) as dag:
    
    task_1 = PythonOperator(
        task_id="step_1_process_email",
        python_callable=step_1_process_email,
        provide_context=True
    )

    
    task_2 = PythonOperator(
        task_id="step_2_compose_email",
        python_callable=step_2_compose_email,
        provide_context=True
    )
    
    
    task_3 = PythonOperator(
        task_id="step_3_send_email",
        python_callable=step_3_send_email,
        provide_context=True
    )
    
    task_1 >> task_2 >> task_3 