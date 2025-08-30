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

def decode_email_payload(email_msg: EmailMessage) -> str:
    """Decode the email payload to extract the text content."""
    try:
        if email_msg.is_multipart():
            for part in email_msg.walk():
                content_type = part.get_content_type()
                if content_type == "text/plain" or content_type == "text/html":
                    try:
                        content = part.get_payload(decode=True).decode("utf-8", errors="ignore")
                        if content_type == "text/html":
                            soup = BeautifulSoup(content, "html.parser")
                            content = soup.get_text(separator=" ", strip=True)
                        return content.strip()
                    except Exception as e:
                        logging.warning(f"Error decoding part: {str(e)}")
                        continue
        else:
            content = email_msg.get_payload(decode=True).decode("utf-8", errors="ignore")
            if email_msg.get_content_type() == "text/html":
                soup = BeautifulSoup(content, "html.parser")
                content = soup.get_text(separator=" ", strip=True)
            return content.strip()
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
            role = "user" if sender != from_address.lower() else "response"
            return [{"role": role, "content": content.strip()}] if content.strip() else []

        thread = service.users().threads().get(userId="me", id=thread_id).execute()
        conversation = []
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
            role = "user" if sender != from_address.lower() else "response"
            conversation.append({
                "role": role,
                "content": content.strip()
            })

        # Sort by date to ensure chronological order
        conversation.sort(key=lambda x: headers.get("Date", ""), reverse=False)
        logging.debug(f"Retrieved thread with {len(conversation)} messages")
        return conversation
    except Exception as e:
        logging.error(f"Error retrieving email thread: {str(e)}")
        return []

def get_ai_response(prompt, conversation_history=None, stream=True,images=None):
    """Get AI response with conversation history context"""
    try:
        logging.debug(f"Query received: {prompt}")
        
        # Validate input
        if not prompt or not isinstance(prompt, str):
            return "Invalid input provided. Please enter a valid query."

        client = Client(host=OLLAMA_HOST, headers={'x-ltai-client': 'help-desk-agent:0.3'})
        logging.debug(f"Connecting to Ollama at {OLLAMA_HOST} with model 'help-desk-agent:0.3'")

        # Build messages array with conversation history
        messages = []
        if conversation_history:
            for history_item in conversation_history:
                messages.append({"role": "user", "content": history_item["prompt"]})
                messages.append({"role": "assistant", "content": history_item["response"]})
        user_message = {"role": "user", "content": prompt}
        if images:
            logging.info(f"Images provided: {len(images)}")
            user_message["images"] = images
        # Add current prompt
        messages.append(user_message)

        response = client.chat(
            model='help-desk-agent:0.3',
            messages=messages,
            stream=stream
        )
        logging.info(f"Raw response from agent: {str(response)[:500]}...")

        # Handle response based on streaming mode
        if stream:
            # For streaming, iterate over chunks
            ai_content = ""
            for chunk in response:
                if hasattr(chunk, 'message') and hasattr(chunk, 'message') and hasattr(chunk.message, 'content'):
                    ai_content += chunk.message.content
                else:
                    logging.error("Chunk lacks expected 'message.content' structure")
                    return "Invalid response format from AI stream. Please try again later."
        else:
            # For non-streaming, access response directly
            if not (hasattr(response, 'message') and hasattr(response.message, 'content')):
                logging.error("Response lacks expected 'message.content' structure")
                return "Invalid response format from AI. Please try again later."
            ai_content = response.message.content

        logging.info(f"Full message content from agent: {ai_content[:500]}...")
        return ai_content.strip()

    except Exception as e:
        logging.error(f"Error in get_ai_response: {str(e)}")
        return f"An error occurred while processing your request: {str(e)}"

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
    """Step 1: Process message from email with image attachment and send conversation history."""
    email_data = context['dag_run'].conf.get("email_data", {})
    
    service = authenticate_gmail()
    if not service:
        logging.error("Gmail authentication failed, aborting.")
        return "Gmail authentication failed"
    
    # Retrieve the email thread with user/response roles
    email_thread = get_email_thread(service, email_data, HELPDESK_FROM_ADDRESS)
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
        sender_name = sender_email.split('@')[0]  # Fallback: use email prefix as name


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
    
    # Prepare conversation history (previous emails) and current content
    conversation_history = email_thread[:-1] if email_thread else []  # All but the last message
    current_content = email_thread[-1]["content"] if email_thread else email_data.get("content", "").strip()
    logging.info(f"Current content is {current_content}")
    logging.info(f"Conversation history is {conversation_history}")
    # Clean current content if it exists
    if current_content:
        soup = BeautifulSoup(current_content, "html.parser")
        current_content = soup.get_text(separator=" ", strip=True)
    
    # Append attachment content to the current content (prompt)
    if attachment_content:
        current_content += f"\n{attachment_content}"
    intent_prompt = f"""
    Get the user intent for the following content:\n{current_content} if the user intnet is to get help with an issue, return the json data with the following format:\n{{\"intent\": \"get_help\"}}\n if the inetent is to escalate the issue to L2 support, return the json data with the following format:\n{{\"intent\": \"escalate_to_l2\"}}\n if the intent is to get more information about the issue, return the json data with the following format:\n{{\"intent\": \"get_more_info\"}}\n if the intent is to close the issue, return the json data with the following format:\n{{\"intent\": \"close_issue\"}}"""
    intent_response = get_ai_response(intent_prompt, conversation_history=conversation_history, images=image_attachments if image_attachments else None)
    intent="get_help"
    try:
        match = re.search(r'\{.*\}', intent_response, re.DOTALL)
        status = None
        if match:
            result_json = json.loads(match.group())
            intent = result_json.get("intent", "")
        if intent and status.lower() == "failure":
            logging.error(f"Step 6a failed: {intent_response}")
            # raise Exception(f"step_1a_validate_feature_file failed: {response}")
    except Exception as e:
        logging.error(f"Error parsing validation response: {str(e)}")

    prompt= f"User query: \n{current_content}"
    if intent.lower() == "escalate_to_l2":
        prompt = f"""
        The user has requested to escalate the issue to L2 support. So 
        - User query: \n{current_content}
        - Sender Name: {sender_name}
        - Sender Email: {sender_email}
        escalate the issue to L2 support without asking for conformation. Extract the relavent information from the email and if more information is needed, ask the user for more information. If the user has provided enough information, escalate the issue to L2 support. 
        """
    # Construct the prompt for AI
    if intent.lower() == "get_help":
        prompt = f"""
        # Your task
        - Extract the content from the provided conversation and attachments, review the content, and provide a report in the output format below.
        - User query: \n{current_content}
        - Compose a professional and human-like business email in American English, written in the tone of an L1 support agent, including analysis of the problem, possible root causes, and suggested solution steps.
        - **DO NOT ESCALATE THE ISSUE TO L2 SUPPORT.** Provide a thorough analysis and actionable steps to resolve the issue at the L1 level.
        - The email should be concise, clear, and easy to understand for a non-technical audience.
        - The email should include a polite closing paragraph offering further assistance, mentioning the contact email helpdeskagent-9228@lowtouch.ai.
        - Use only clean, valid HTML for the email body without any section headers in the content (except as specified in the format). Avoid technical or template-style formatting and placeholders. The email should read as if it was personally written.
        - Return only the HTML body, and nothing else.
        - Strictly use the following output format, do not deviate from this.

        # **output format**
        ```html
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <title>Problem Analysis Email</title>
        </head>
        <body>
            <div class="container">
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
                    <p>Help Desk Support</p>
                    <p>Contact: <a href="mailto:helpdeskagent-9228@lowtouch.ai">helpdeskagent-9228@lowtouch.ai</a></p>
                </div>
            </div>
        </body>
        </html>
        ```
        """
    else:
        prompt = f"""
        - Extract the content from the provided conversation and attachments, review the content, and provide a report in the output format below.
        - User query: \n{current_content}
        - Compose a professional and human-like business email in American English, written in the tone of an L1 support agent
        """
    # Get AI response with conversation history
    response = get_ai_response(prompt, conversation_history=conversation_history, images=image_attachments if image_attachments else None)
    
    # Clean the HTML response
    cleaned_response = re.sub(r'```html\n|```', '', response).strip()
    
    if not cleaned_response.strip().startswith('<!DOCTYPE') and not cleaned_response.strip().startswith('<html'):
        if not cleaned_response.strip().startswith('<'):
            cleaned_response = f"<html><body>{cleaned_response}</body></html>"
    
    # Push the cleaned response to XCom
    ti.xcom_push(key="final_html_content", value=cleaned_response)
    
    logging.info(f"Step 1 completed: {cleaned_response[:200]}...")
    return cleaned_response



def step_2_send_email(ti, **context):
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
        task_id="step_2_send_email",
        python_callable=step_2_send_email,
        provide_context=True
    )
    
    
    
    
    task_1 >> task_2 