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

        client = Client(host=OLLAMA_HOST, headers={'x-ltai-client': 'InvoFlux'})
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
            model='InvoFlux:0.3',
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
                attachment_content += f"\nAttachment :\n{attachment['extracted_content']['content']}\n"
    
    # ADDED: Split thread into history and current content
    if complete_email_thread :
        conversation_history = [] # All previous messages
        current_content = complete_email_thread[-1]["content"] if len(complete_email_thread) > 0 else email_data.get("content", "").strip()  # Current message
    else:
        conversation_history = []  # No previous conversation
        current_content = email_data.get("content", "").strip()
    
    # ADDED: Log conversation history
    logging.info(f"Complete conversation history contains {len(conversation_history)} messages")
    logging.info(f"Current content: {current_content}...")
    for i, msg in enumerate(conversation_history[:3]):  # Log first 3 for brevity
        logging.info(f"History message {i+1}: Role={msg['role']}, Content={msg['content'][:100]}...")
    
    # ADDED: Clean current content
    if current_content:
        soup = BeautifulSoup(current_content, "html.parser")
        current_content = soup.get_text(separator=" ", strip=True)
        current_content = remove_quoted_text(current_content)
    
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
    ti.xcom_push(key="email content", value=current_content)
    ti.xcom_push(key="complete_email_thread", value=complete_email_thread)
    
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
    step_1_response = ti.xcom_pull(key="step_1_response")
    step_2_response = ti.xcom_pull(key="step_2_response")
    if step_1_response:
        # Remove the specific confirmation sentence, case-insensitive
        confirmation_pattern = r'Please confirm the extracted details\. Would you like to create the vendor bill by performing three-way matching\?'
        step_1_response = re.sub(confirmation_pattern, '', step_1_response, flags=re.IGNORECASE).strip()
        # Also remove any trailing whitespace or newlines
        step_1_response = re.sub(r'\n\s*\n\s*$', '', step_1_response)
    content_appended = step_1_response + "\n\n" + step_2_response if step_1_response and step_2_response else step_1_response or step_2_response or ""
    sender_name = context['dag_run'].conf.get("email_data", {}).get("headers", {}).get("From", "Valued Customer")
    name_email_match = re.match(r'^(.*?)\s*<(.*?@.*?)>$', sender_name)
    if name_email_match:
        sender_name = name_email_match.group(1).strip() or "Valued Customer"
    else:
        sender_name = "Valued Customer"
    logging.info(f"Sender name extracted: {sender_name}")
    
    prompt=f"""
        Generate a professional, human-like business email in American English, written in the tone of a senior Customer Success Manager, to notify a vendor, addressed by name '{sender_name}', about the status of their invoice submission (Posted or Draft), including any duplicate invoice detections.

        Content: {content_appended}

        Follow this exact structure for the email body, using clean, valid HTML without any additional wrappers like <html> or <body>. Do not omit any sections or elements listed below. Use natural, professional wording but adhere strictly to the format. Extract all details from the provided Content; use 'N/A' if a value is not available. For the due date, if not mentioned in the Content, specify '30 days from the invoice date'.

        1. Greeting: <p>Dear {sender_name},</p>

        2. Opening paragraph: If the Content does not indicate a duplicate (e.g., no 'Duplicate invoice number found' in validation issues): 
           - If Status is Posted, use: We acknowledge receipt of your invoice [Invoice Number] dated [Invoice Date] from [Vendor Name if available, else omit 'from [Vendor Name]']. It has been created in the Posted status.
           - If Status is Draft, use: We acknowledge receipt of your invoice [Invoice Number] dated [Invoice Date] from [Vendor Name if available, else omit 'from [Vendor Name]']. The invoice has been recorded in our system; however, it currently remains in Draft status due to the following validation issues.

        If the Content indicates a duplicate (e.g., 'Duplicate invoice number found' in validation issues), use: 'We acknowledge receipt of your invoice [Invoice Number] dated [Invoice Date] from [Vendor Name]. However, this invoice is a duplicate. The existing invoice is in [Posted/Draft] status, and no new invoice was created' [due to the following validation issues: only if the existing status is Draft and there are other non-duplicate validation issues, else omit this part]. Use the existing status from Content if available, else infer from context (e.g., Draft if validation failed).
        
        3. Issues list (only if Draft or if duplicate with other validation issues): If there are validation issues, include only non-duplicate issues, do not include duplicate issues:
        *  followed by
        * [each specific issue].
        
        4. Invoice Summary section: <p><strong>Invoice Summary</strong></p> followed by a list in paragraph form (each on new line with <br>): 
           Invoice Number: [value]<br>
           Invoice Date: [value]<br>
           Due Date: [value if available, else '30 days from the invoice date']<br>
           Internal Invoice ID: [value]<br>
           Status: [value]<br>
           Vendor Name: [value]<br>
           Purchase Order: [value]<br>
           Currency: [value]<br>
           Subtotal: [value]<br>
           Tax ([rate if available]%): [amount if available]<br>
           Total Amount: [value]<br>

        For duplicates, label this as 'Existing Invoice Summary' or 'Existing Draft Invoice Summary' based on the existing status, and populate with the existing invoice's details.

        5. Product Details section: <p><strong>Product Details</strong></p> followed immediately by a table: <table border="1" cellspacing="0" cellpadding="5"><tr><th>Item Description</th><th>Quantity</th><th>Unit Price</th><th>Tax</th><th>Total Price</th></tr> [rows with <tr><td>[desc]</td><td>[qty]</td><td>[price]</td><td>[tax]</td><td>[total]</td></tr> for each item] </table>

        6. Closing statement paragraph: <p>[A concise natural statement based on the invoice status:
           - Posted: The invoice is now in our payment cycle and will be processed accordingly.
           - Draft: Please review the issues above, make the necessary corrections, and resubmit the updated invoice to <a href="mailto:invoflux-agent-8013@lowtouch.ai">invoflux-agent-8013@lowtouch.ai</a>.
           - Duplicate (Posted): Please review the existing invoice details above. If this was submitted in error, no further action is needed; otherwise, contact us for support.
           - Duplicate (Draft): Please review the existing draft details above, correct any issues, and resubmit to <a href="mailto:invoflux-agent-8013@lowtouch.ai">invoflux-agent-8013@lowtouch.ai</a>.
           ]</p>

        7. Assistance paragraph: <p>Please let us know if you need any assistance. You can reach us at <a href="mailto:invoflux-agent-8013@lowtouch.ai">invoflux-agent-8013@lowtouch.ai</a>.</p>

        8. Signature: <p>Best regards,<br>Invoice Processing Team,<br>InvoFlux</p>

        Ensure the email is natural, professional, and concise. Avoid rigid or formulaic language to maintain a human-like tone. Do not use placeholders; replace with actual extracted values. Return only the HTML content as specified, without <!DOCTYPE>, <html>, or <body> tags.

        Return only the HTML body of the email.
        """
    # Pass only the previous step's response as history
    history = [{"role": "assistant", "content": content_appended}] if content_appended else []
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
    ti.xcom_push(key="email content", value=content_appended)
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
    schedule=None,
    catchup=False,
    doc_md=readme_content,
    tags=["email", "shared", "send", "message", "invoice"]
) as dag:
    
    task_1 = PythonOperator(
        task_id="step_1_process_email",
        python_callable=step_1_process_email,
    )
    
    task_2 = PythonOperator(
        task_id="step_2_create_vendor_bill",
        python_callable=step_2_create_vendor_bill,
    )
    
    task_3 = PythonOperator(
        task_id="step_3_compose_email",
        python_callable=step_3_compose_email,
    )
    
    
    task_4 = PythonOperator(
        task_id="step_4_send_email",
        python_callable=step_4_send_email,
    )
    
    task_1 >> task_2 >> task_3 >> task_4 
