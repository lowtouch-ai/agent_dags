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
from email import message_from_bytes
from email.header import decode_header
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

WEBSHOP_FROM_ADDRESS = Variable.get("WEBSHOP_FROM_ADDRESS")
GMAIL_CREDENTIALS = Variable.get("WEBSHOP_GMAIL_CREDENTIALS", deserialize_json=True)
OLLAMA_HOST = Variable.get("WEBSHOP_OLLAMA_HOST")

def authenticate_gmail():
    try:
        creds = Credentials.from_authorized_user_info(GMAIL_CREDENTIALS)
        service = build("gmail", "v1", credentials=creds)
        profile = service.users().getProfile(userId="me").execute()
        logged_in_email = profile.get("emailAddress", "")
        if logged_in_email.lower() != WEBSHOP_FROM_ADDRESS.lower():
            raise ValueError(f"Wrong Gmail account! Expected {WEBSHOP_FROM_ADDRESS}, but got {logged_in_email}")
        logging.info(f"Authenticated Gmail account: {logged_in_email}")
        return service
    except Exception as e:
        logging.error(f"Failed to authenticate Gmail: {str(e)}")
        return None

def decode_email_payload(msg):
    try:
        # Handle multipart emails
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
        # Handle single-part emails
        else:
            try:
                body = msg.get_payload(decode=True).decode()
                return body
            except UnicodeDecodeError:
                body = msg.get_payload(decode=True).decode('latin-1')
                return body
        return ""
    except Exception as e:
        logging.error(f"Error decoding email payload: {str(e)}")
        return ""

def get_email_thread(service, email_data):
    try:
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
        # Sort by date to ensure chronological order
        email_thread.sort(key=lambda x: x["headers"].get("Date", ""), reverse=False)
        logging.debug(f"Retrieved thread with {len(email_thread)} messages")
        return email_thread
    except Exception as e:
        logging.error(f"Error retrieving email thread: {str(e)}")
        return []

def get_ai_response(prompt, conversation_history=None):
    """Get AI response with conversation history context"""
    try:
        logging.debug(f"Query received: {prompt}")
        
        # Validate input
        if not prompt or not isinstance(prompt, str):
            return "Invalid input provided. Please enter a valid query."

        client = Client(host=OLLAMA_HOST, headers={'x-ltai-client': 'webshop-email-respond'})
        logging.debug(f"Connecting to Ollama at {OLLAMA_HOST} with model 'webshop:0.5'")

        # Build messages array with conversation history
        messages = []
        if conversation_history:
            for history_item in conversation_history:
                messages.append({"role": "user", "content": history_item["prompt"]})
                messages.append({"role": "assistant", "content": history_item["response"]})
        
        # Add current prompt
        messages.append({"role": "user", "content": prompt})

        response = client.chat(
            model='webshop-email:0.5',
            messages=messages,
            stream=False
        )
        logging.info(f"Raw response from agent: {str(response)[:500]}...")

        # Extract content
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
        msg["From"] = f"WebShop via lowtouch.ai <{WEBSHOP_FROM_ADDRESS}>"
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

# Multi-step prompt processing functions
def step_1_initial_quote_request(ti, **context):
    """Step 1: Process initial quote request"""
    email_data = context['dag_run'].conf.get("email_data", {})
    
    # Get email content
    current_content = email_data.get("content", "").strip()
    if current_content:
        soup = BeautifulSoup(current_content, "html.parser")
        current_content = soup.get_text(separator=" ", strip=True)
    
    prompt = f'Here is an email from a customer: {current_content}'
    
    response = get_ai_response(prompt)
    
    # Store in XCom
    ti.xcom_push(key="step_1_prompt", value=prompt)
    ti.xcom_push(key="step_1_response", value=response)
    ti.xcom_push(key="conversation_history", value=[{"prompt": prompt, "response": response}])
    
    logging.info(f"Step 1 completed: {response[:200]}...")
    return response

def step_2_customer_lookup(ti, **context):
    """Step 2: Look up customer information"""
    # Get previous conversation
    history = ti.xcom_pull(key="conversation_history")
    
    prompt = "look up the customer; get customer id and email"
    
    response = get_ai_response(prompt, history)
    
    # Update conversation history
    history.append({"prompt": prompt, "response": response})
    ti.xcom_push(key="step_2_prompt", value=prompt)
    ti.xcom_push(key="step_2_response", value=response)
    ti.xcom_push(key="conversation_history", value=history)
    
    logging.info(f"Step 2 completed: {response[:200]}...")
    return response

def step_3_apply_discounts(ti, **context):
    """Step 3: Apply discounts to each article"""
    # Get previous conversation
    history = ti.xcom_pull(key="conversation_history")
    
    prompt = "apply discounts to each article"
    
    response = get_ai_response(prompt, history)
    
    # Update conversation history
    history.append({"prompt": prompt, "response": response})
    ti.xcom_push(key="step_3_prompt", value=prompt)
    ti.xcom_push(key="step_3_response", value=response)
    ti.xcom_push(key="conversation_history", value=history)
    
    logging.info(f"Step 3 completed: {response[:200]}...")
    return response

def step_4_include_summary(ti, **context):
    """Step 4: Include subtotal, discount, and shipping"""
    # Get previous conversation
    history = ti.xcom_pull(key="conversation_history")
    
    prompt = "Include subtotal, total discount ($ and %) and shipping to the summary"
    
    response = get_ai_response(prompt, history)
    
    # Update conversation history
    history.append({"prompt": prompt, "response": response})
    ti.xcom_push(key="step_4_prompt", value=prompt)
    ti.xcom_push(key="step_4_response", value=response)
    ti.xcom_push(key="conversation_history", value=history)
    
    logging.info(f"Step 4 completed: {response[:200]}...")
    return response

def step_5_calculate_discount(ti, **context):
    """Step 5: Calculate total discount percentage"""
    # Get previous conversation
    history = ti.xcom_pull(key="conversation_history")
    
    prompt = "calculate the total discount = (total cost of items before discount - total cost of items after discount)/(total cost of items before discount) * 100"
    
    response = get_ai_response(prompt, history)
    
    # Update conversation history
    history.append({"prompt": prompt, "response": response})
    ti.xcom_push(key="step_5_prompt", value=prompt)
    ti.xcom_push(key="step_5_response", value=response)
    ti.xcom_push(key="conversation_history", value=history)
    
    logging.info(f"Step 5 completed: {response[:200]}...")
    return response

def step_6_quality_check(ti, **context):
    """Step 6: Conduct quality check"""
    # Get previous conversation
    history = ti.xcom_pull(key="conversation_history")
    
    prompt = "conduct a quality check to ensure the total, subtotal, total discount are all accurate as per original request from the customer"
    
    response = get_ai_response(prompt, history)
    
    # Update conversation history
    history.append({"prompt": prompt, "response": response})
    ti.xcom_push(key="step_6_prompt", value=prompt)
    ti.xcom_push(key="step_6_response", value=response)
    ti.xcom_push(key="conversation_history", value=history)
    
    logging.info(f"Step 6 completed: {response[:200]}...")
    return response

def step_7_compose_final_email(ti, **context):
    """Step 7: Compose final quote email"""
    # Get previous conversation
    history = ti.xcom_pull(key="conversation_history")
    
    prompt = "compose the final quote in the form of an email addressed to the customer using american business tone in html; include all details of the articles in tabular format; Include subtotal, total discount ($ and %) and shipping to the summary."
    
    response = get_ai_response(prompt, history)
    
    # Clean up markdown markers if present
    cleaned_response = re.sub(r'```html\n|```', '', response).strip()
    
    # Ensure proper HTML structure
    if not cleaned_response.strip().startswith('<!DOCTYPE') and not cleaned_response.strip().startswith('<html'):
        if not cleaned_response.strip().startswith('<'):
            cleaned_response = f"<html><body>{cleaned_response}</body></html>"
    
    # Update conversation history
    history.append({"prompt": prompt, "response": cleaned_response})
    ti.xcom_push(key="step_7_prompt", value=prompt)
    ti.xcom_push(key="step_7_response", value=cleaned_response)
    ti.xcom_push(key="conversation_history", value=history)
    ti.xcom_push(key="final_html_content", value=cleaned_response)
    
    logging.info(f"Step 7 completed: {cleaned_response[:200]}...")
    return cleaned_response

def step_8_send_quote_email(ti, **context):
    """Step 8: Send the final quote email"""
    try:
        email_data = context['dag_run'].conf.get("email_data", {})
        if not email_data:
            logging.warning("No email data received! This DAG was likely triggered manually.")
            return "No email data available"
        
        # Get the final HTML content from previous step
        final_html_content = ti.xcom_pull(key="final_html_content")
        if not final_html_content:
            logging.error("No final HTML content found from previous steps")
            return "Error: No content to send"
        
        # Authenticate Gmail
        service = authenticate_gmail()
        if not service:
            logging.error("Gmail authentication failed, aborting email response.")
            return "Gmail authentication failed"
        
        # Prepare email details
        sender_email = email_data["headers"].get("From", "")
        subject = f"Re: {email_data['headers'].get('Subject', 'Quote Request')}"
        in_reply_to = email_data["headers"].get("Message-ID", "")
        references = email_data["headers"].get("References", "")
        
        # Send the email
        result = send_email(
            service, sender_email, subject, final_html_content,
            in_reply_to, references
        )
        
        if result:
            logging.info(f"Quote email sent successfully to {sender_email}")
            return f"Email sent successfully to {sender_email}"
        else:
            logging.error("Failed to send quote email")
            return "Failed to send email"
            
    except Exception as e:
        logging.error(f"Error in step_8_send_quote_email: {str(e)}")
        return f"Error sending email: {str(e)}"

# Read README if available
readme_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'email_responder.md')
readme_content = ""
try:
    with open(readme_path, 'r') as file:
        readme_content = file.read()
except FileNotFoundError:
    readme_content = "Multi-step email quote processing DAG"

with DAG(
    "webshop_quote_email_v2", 
    default_args=default_args, 
    schedule_interval=None, 
    catchup=False, 
    doc_md=readme_content, 
    tags=["email", "webshop", "quote", "multi-step"]
) as dag:
    
    # Define all tasks in sequence
    task_1 = PythonOperator(
        task_id="step_1_initial_quote_request",
        python_callable=step_1_initial_quote_request,
        provide_context=True
    )
    
    task_2 = PythonOperator(
        task_id="step_2_customer_lookup",
        python_callable=step_2_customer_lookup,
        provide_context=True
    )
    
    task_3 = PythonOperator(
        task_id="step_3_apply_discounts",
        python_callable=step_3_apply_discounts,
        provide_context=True
    )
    
    task_4 = PythonOperator(
        task_id="step_4_include_summary",
        python_callable=step_4_include_summary,
        provide_context=True
    )
    
    task_5 = PythonOperator(
        task_id="step_5_calculate_discount",
        python_callable=step_5_calculate_discount,
        provide_context=True
    )
    
    task_6 = PythonOperator(
        task_id="step_6_quality_check",
        python_callable=step_6_quality_check,
        provide_context=True
    )
    
    task_7 = PythonOperator(
        task_id="step_7_compose_final_email",
        python_callable=step_7_compose_final_email,
        provide_context=True
    )
    
    task_8 = PythonOperator(
        task_id="step_8_send_quote_email",
        python_callable=step_8_send_quote_email,
        provide_context=True
    )
    
    # Set up task dependencies (sequential execution)
    task_1 >> task_2 >> task_3 >> task_4 >> task_5 >> task_6 >> task_7 >> task_8
