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
    "owner": "autofinix_developers",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 18),
    "retries": 0,
    "retry_delay": timedelta(seconds=15),
}

AUTOFINIX_FROM_ADDRESS = Variable.get("AUTOFINIX_FROM_ADDRESS")
GMAIL_CREDENTIALS = Variable.get("AUTOFINIX_GMAIL_CREDENTIALS")

OLLAMA_HOST = "http://agentomatic:8000/"

def authenticate_gmail():
    try:
        creds = Credentials.from_authorized_user_info(json.loads(GMAIL_CREDENTIALS))
        service = build("gmail", "v1", credentials=creds)
        profile = service.users().getProfile(userId="me").execute()
        logged_in_email = profile.get("emailAddress", "")
        if logged_in_email.lower() != AUTOFINIX_FROM_ADDRESS.lower():
            raise ValueError(f"Wrong Gmail account! Expected {AUTOFINIX_FROM_ADDRESS}, but got {logged_in_email}")
        logging.info(f"Authenticated Gmail account: {logged_in_email}")
        return service
    except Exception as e:
        logging.error(f"Failed to authenticate Gmail: {str(e)}")
        raise ValueError(f"Gmail authentication failed: {str(e)}")

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

        client = Client(host=OLLAMA_HOST, headers={'x-ltai-client': 'Autofinix'})
        logging.debug(f"Connecting to Ollama at {OLLAMA_HOST} with model 'AutoFinix:0.3'")

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
            model='autofinix:0.3',
            messages=messages,
            stream=False
        )
        logging.info(f"Raw response from agent: {response}")

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
        logging.debug(f"Email body: {body[:200]}...")
        msg = MIMEMultipart()
        msg["From"] = f"AutoFinix via lowtouch.ai <{AUTOFINIX_FROM_ADDRESS}>"
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

def step_1_execute_thread_history(ti, **context):
    """Step 1: Execute email thread history to extract loan-related information."""
    email_data = context['dag_run'].conf.get("email_data", {})
    
    service = authenticate_gmail()
    if not service:
        logging.error("Gmail authentication failed, aborting.")
        raise ValueError("Gmail authentication failed")
    
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
    
    prompt = f"""Analyze the following email thread and extract the loan-related information requested (e.g., EMI schedule or loan statement). Provide the extracted information in plain text, including the request type (e.g., 'EMI schedule', 'loan statement') and specific details like loan ID, amount, etc., if available.
    Thread history:
    {thread_history}
    Parse the correct loan id from users query.
    Pass the extracted loan id to the API tool."""
    
    response = get_ai_response(prompt, images=image_attachments if image_attachments else None)
    
    ti.xcom_push(key="step_1_prompt", value=prompt)
    ti.xcom_push(key="step_1_response", value=response)
    ti.xcom_push(key="conversation_history", value=[{"prompt": prompt, "response": response}])
    ti.xcom_push(key="sender_email", value=email_data["headers"].get("From", ""))
    
    logging.info(f"Step 1 completed: Extracted thread history - {response[:200]}...")
    return response

def step_2_validate_and_provide_info(ti, **context):
    """Step 2: Validate customer details and provide information only if they match."""
    history = ti.xcom_pull(key="conversation_history")
    extracted_info = ti.xcom_pull(key="step_1_response")
    sender_email = ti.xcom_pull(key="sender_email")
    
    # Improved validation prompt with clearer instructions
    prompt = f"""You are a banking validation system. Your task is to validate customer access to loan information.

Customer requesting information: {sender_email}
Requested loan information from Step 1: {extracted_info}

VALIDATION STEPS:
1. Use the provided tools to retrieve customer information for email: {sender_email}
2. Extract the customer name and loan details from the retrieved information
3. Compare the retrieved customer information with the loan request details
4. IMPORTANT: Respond with EXACTLY one of these two options:

Option A - If validation PASSES (customer is authorized):
Respond with: VALIDATION_PASSED
Then provide the requested loan information in a clean format.

Option B - If validation FAILS (customer is NOT authorized):
Respond with: VALIDATION_FAILED

Do not include any other text, explanations, or HTML formatting in your response."""
    
    response = get_ai_response(prompt, conversation_history=history)
    
    # Improved validation logic
    response_text = response.strip().lower()
    # Remove HTML tags if present
    soup = BeautifulSoup(response, "html.parser")
    clean_response = soup.get_text(separator=" ", strip=True).lower()
    
    logging.debug(f"Validation response: {response}")
    logging.debug(f"Clean response: {clean_response}")
    
    # Check for validation status
    is_valid = "validation_passed" in clean_response
    validation_failed = "validation_failed" in clean_response
    
    if validation_failed:
        is_valid = False
        logging.warning("Validation explicitly failed")
    elif not is_valid and "false" in clean_response:
        is_valid = False
        logging.warning("Validation failed - found 'false' in response")
    elif not is_valid:
        # If no clear validation status, assume failed for security
        is_valid = False
        logging.warning("No clear validation status - defaulting to failed for security")
    
    history.append({"prompt": prompt, "response": response})
    ti.xcom_push(key="step_2_prompt", value=prompt)
    ti.xcom_push(key="step_2_response", value=response)
    ti.xcom_push(key="conversation_history", value=history)
    ti.xcom_push(key="is_valid", value=is_valid)
    ti.xcom_push(key="final_response", value=response)
    
    if not is_valid:
        logging.warning("Customer validation failed. Access denied response generated.")
    else:
        logging.info(f"Step 2 completed: Validated and provided info - {response[:200]}...")
    
    return response

def step_3_compose_email(ti, **context):
    """Step 3: Compose a professional banking email response."""
    history = ti.xcom_pull(key="conversation_history")
    is_valid = ti.xcom_pull(key="is_valid")
    step_1_response = ti.xcom_pull(key="step_1_response")
    step_2_response = ti.xcom_pull(key="step_2_response")

    logging.debug(f"Step 3: is_valid={is_valid}, step_1_response={step_1_response[:200]}..., step_2_response={step_2_response[:200]}...")

    if not is_valid:
        # Access denied email
        cleaned_response = """<html><body>
        <p>Dear Valued Customer,</p>
        <p>Thank you for contacting AutoFinix regarding your loan information request.</p>
        <p>We regret to inform you that we cannot provide the requested information at this time. This could be due to:</p>
        <ul>
            <li>Insufficient verification of your identity</li>
            <li>The loan information requested may not be associated with your account</li>
            <li>Additional security verification may be required</li>
        </ul>
        <p>For your security and privacy, please contact our support team directly at <a href="mailto:autofinix-agent@lowtouch.ai">autofinix-agent@lowtouch.ai</a> or visit our nearest branch office for assistance.</p>
        <p>We apologize for any inconvenience and appreciate your understanding.</p>
        <p>Best regards,<br>
        Customer Support Team<br>
        AutoFinix</p>
        </body></html>"""
        
        history.append({"prompt": "Validation failed: access denied", "response": cleaned_response})
        ti.xcom_push(key="step_3_prompt", value="Validation failed: access denied")
        ti.xcom_push(key="step_3_response", value=cleaned_response)
        ti.xcom_push(key="conversation_history", value=history)
        ti.xcom_push(key="final_html_content", value=cleaned_response)
        logging.info("Step 3 completed: Access denied email prepared.")
        return cleaned_response

    # If valid, compose using the information from step 2
    prompt = f"""Compose a professional business email in American English, written in the tone of a senior Customer Success Manager, to provide the customer with the requested loan information.

Use the validated information from Step 2: {step_2_response}

The email must include:
- A professional greeting addressing the customer
- An acknowledgment of their specific request (EMI schedule, loan statement, etc.)
- The requested information presented clearly:
  * If loan statement requested: Display loan repayment and disbursement details in clean HTML table format
  * If EMI schedule requested: Display EMI details in clean HTML table format  
  * If repayment details requested: Display only repayment information in clean HTML table format
- Use proper HTML table formatting with borders and headers
- A closing paragraph offering further assistance
- Professional signature: "Customer Support Team, AutoFinix"
- Contact information: autofinix-agent@lowtouch.ai

IMPORTANT: 
- Provide ONLY the HTML email content
- Do NOT include any introductory text outside the HTML
- Ensure tables are properly formatted with borders and headers
- Keep the tone professional but friendly"""
    
    response = get_ai_response(prompt, conversation_history=history)
    
    # Clean up the response
    cleaned_response = re.sub(r'```html\n|```', '', response).strip()
    
    # Ensure proper HTML structure
    if not cleaned_response.strip().startswith('<!DOCTYPE') and not cleaned_response.strip().startswith('<html'):
        if not cleaned_response.strip().startswith('<'):
            cleaned_response = f"<html><body>{cleaned_response}</body></html>"

    history.append({"prompt": prompt, "response": response})
    ti.xcom_push(key="step_3_prompt", value=prompt)
    ti.xcom_push(key="step_3_response", value=response)
    ti.xcom_push(key="conversation_history", value=history)
    ti.xcom_push(key="final_html_content", value=cleaned_response)

    logging.info(f"Step 3 completed: Composed email - {cleaned_response[:200]}...")
    return cleaned_response

def step_4_send_email(ti, **context):
    """Step 4: Send the final email."""
    try:
        email_data = context['dag_run'].conf.get("email_data", {})
        if not email_data:
            logging.warning("No email data received! This DAG was likely triggered manually.")
            return "No email data available"
        
        final_html_content = ti.xcom_pull(key="final_html_content")
        is_valid = ti.xcom_pull(key="is_valid")
        
        logging.debug(f"Step 4: final_html_content={final_html_content[:200]}..., is_valid={is_valid}")
        
        if not final_html_content:
            logging.error("No final HTML content found from previous steps")
            return "Error: No content to send"
        
        service = authenticate_gmail()
        if not service:
            logging.error("Gmail authentication failed, aborting email response.")
            return "Gmail authentication failed"
        
        sender_email = email_data["headers"].get("From", "")
        subject = f"Re: {email_data['headers'].get('Subject', 'Loan Request')}"
        in_reply_to = email_data["headers"].get("Message-ID", "")
        references = email_data["headers"].get("References", "")
        
        result = send_email(
            service, sender_email, subject, final_html_content,
            in_reply_to, references
        )
        
        if result:
            status_msg = "access denied" if not is_valid else "loan information"
            logging.info(f"Email sent successfully to {sender_email} - {status_msg}")
            return f"Email sent successfully to {sender_email} - {status_msg}"
        else:
            logging.error("Failed to send email")
            return "Failed to send email"
            
    except Exception as e:
        logging.error(f"Error in step_4_send_email: {str(e)}")
        return f"Error sending email: {str(e)}"

readme_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'email_responder.md')
readme_content = ""
try:
    with open(readme_path, 'r') as file:
        readme_content = file.read()
except FileNotFoundError:
    readme_content = "Multi-step loan request processing DAG for AutoFinix"

with DAG(
    "autofinix_send_message_email",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    doc_md=readme_content,
    tags=["email", "shared", "send", "message", "loan", "autofinix"]
) as dag:
    
    task_1 = PythonOperator(
        task_id="step_1_execute_thread_history",
        python_callable=step_1_execute_thread_history,
        provide_context=True
    )
    
    task_2 = PythonOperator(
        task_id="step_2_validate_and_provide_info",
        python_callable=step_2_validate_and_provide_info,
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
