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

def step_1_extract_loan_request(ti, **context):
    """Step 1: Extract and retrieve loan-related information from email thread."""
    logging.info("=== STEP 1: EXTRACTING LOAN REQUEST ===")
    
    email_data = context['dag_run'].conf.get("email_data", {})
    
    service = authenticate_gmail()
    if not service:
        logging.error("Gmail authentication failed, aborting.")
        raise ValueError("Gmail authentication failed")
    
    # Get email thread for context
    email_thread = get_email_thread(service, email_data)
    image_attachments = []
    
    # Process attachments
    if email_data.get("attachments"):
        logging.info(f"Processing {len(email_data['attachments'])} attachments")
        for attachment in email_data["attachments"]:
            if "base64_content" in attachment and attachment["base64_content"]:
                image_attachments.append(attachment["base64_content"])
                logging.info(f"Added image attachment: {attachment['filename']}")
    
    # Build complete thread history
    thread_history = ""
    for idx, email in enumerate(email_thread, 1):
        email_content = email.get("content", "").strip()
        email_from = email["headers"].get("From", "Unknown")
        email_date = email["headers"].get("Date", "Unknown date")
        
        # Clean HTML content
        if email_content:
            soup = BeautifulSoup(email_content, "html.parser")
            email_content = soup.get_text(separator=" ", strip=True)
        
        thread_history += f"Email {idx} (From: {email_from}, Date: {email_date}):\n{email_content}\n\n"
    
    # Add current email content
    current_content = email_data.get("content", "").strip()
    if current_content:
        soup = BeautifulSoup(current_content, "html.parser")
        current_content = soup.get_text(separator=" ", strip=True)
    thread_history += f"Current Email (From: {email_data['headers'].get('From', 'Unknown')}):\n{current_content}\n"
    
    # Add attachment content if available
    if email_data.get("attachments"):
        attachment_content = ""
        for attachment in email_data["attachments"]:
            if "extracted_content" in attachment and "content" in attachment["extracted_content"]:
                attachment_content += f"\nAttachment ({attachment['filename']}):\n{attachment['extracted_content']['content']}\n"
        if attachment_content:
            thread_history += f"\n{attachment_content}"
    
    # Create AI prompt for loan data extraction
    prompt = f"""You are a loan data extraction system. Analyze the following email thread and extract loan information using available API tools.

THREAD HISTORY:
{thread_history}

INSTRUCTIONS:
1. Identify the specific loan ID mentioned in the customer's request
2. Determine what type of information is being requested (EMI schedule, statement, balance, etc.)
3. Use the provided API tools to retrieve the actual loan data for that loan ID
4. Return ONLY the data that comes from the API tools - do not create or hallucinate any information
5. If the API returns "No records found" or similar, return exactly that message
6. Format the response as plain text with clear structure

REQUIRED OUTPUT FORMAT:
- Request Type: [e.g., EMI Schedule, Loan Statement, Repayment statement]
- If user asks for loan statement provide it in below output :
    Disbursal Statement Table

    | LOAN ID | DISBURSAL DATE | AMOUNT DISBURSED | PAYMENT MODE |

    Repayment Statement Table

    | LOAN ID | REPAYMENT DATE | AMOUNT PAID | PAYMENT MODE | STATUS |
    - if no records found:
        Disbursal Statement Table
        LOAN ID	DISBURSAL DATE	AMOUNT DISBURSED	PAYMENT MODE
        No records found			
        Repayment Statement Table
        LOAN ID	REPAYMENT DATE	AMOUNT PAID	PAYMENT MODE	STATUS
        No records found
- If user asks for EMI schedule provide it in below output :
    EMI Schedule Table 
    | EMI NO. | DUE DATE | EMI AMOUNT | OUTSTANDING AMOUNT | PAYMENT STATUS |
    - if no records found:
        No records found
- Loan ID: [exact loan ID from request]
- API Response: [exact data returned from API tools, or "No records found"]

Do not compose emails or provide additional formatting - only extract and return API data."""
    
    # Get AI response
    response = get_ai_response(prompt, images=image_attachments if image_attachments else None)
    
    # Store results
    ti.xcom_push(key="step_1_prompt", value=prompt)
    ti.xcom_push(key="step_1_loan_data", value=response)
    ti.xcom_push(key="conversation_history", value=[{"prompt": prompt, "response": response}])
    ti.xcom_push(key="sender_email", value=email_data["headers"].get("From", ""))
    
    logging.info(f"Step 1 completed: Loan data extracted - {len(response)} characters")
    return response


def step_2_validate_customer_access(ti, **context):
    """Step 2: Validate customer authorization to access the requested loan information."""
    logging.info("=== STEP 2: VALIDATING CUSTOMER ACCESS ===")
    
    history = ti.xcom_pull(key="conversation_history")
    loan_request_info = ti.xcom_pull(key="step_1_loan_data")
    sender_email = ti.xcom_pull(key="sender_email")
    
    # Create validation prompt
    prompt = f"""You are a banking security validation system. Validate customer access to loan information.

CUSTOMER EMAIL: {sender_email}
REQUESTED LOAN INFORMATION: {loan_request_info}

VALIDATION PROCESS:
1. Use available API tools to verify if the email address {sender_email} is registered as a customer in the system.
2. If not registered, immediately fail validation.
3. Use available API tools to retrieve ONLY the email address associated with the requested loan (do not retrieve any other loan details).
4. If no loan-associated email is found or the loan doesn't exist, fail validation.
5. Check if the retrieved loan-associated email EXACTLY matches (case-insensitive) the customer email {sender_email}.
6. Verify the customer's identity and access permissions based on the above checks.

IMPORTANT: 
- Retrieve ONLY the necessary information for validation: customer registration status and loan-associated email.
- Do NOT retrieve, generate, create, or provide any other loan data or information.
- Use only the provided API tools for validation.
- If ANY check fails (e.g., not registered, no loan email, mismatch), the validation MUST fail.
- Be strict: only pass if ALL checks confirm exact match and authorization.
- Do not add explanations or extra text in your response.

OUTPUT FORMAT:
Return EXACTLY one of these responses with NO additional text, descriptions, tables, or data:
VALIDATION_PASSED
VALIDATION_FAILED"""
    
    # Get validation response
    response = get_ai_response(prompt, conversation_history=history)
    
    # Process validation result
    soup = BeautifulSoup(response, "html.parser")
    clean_response = soup.get_text(separator=" ", strip=True).strip().upper()
    
    logging.debug(f"Raw validation response: {response}")
    logging.debug(f"Cleaned validation response: {clean_response}")
    
    # Extract status (handle potential extra text or colon)
    validation_status = clean_response.split(":")[0].strip() if ":" in clean_response else clean_response
    
    # Determine validation status
    if validation_status == "VALIDATION_PASSED":
        is_valid = True
        logging.info(f"Customer validation PASSED for {sender_email}")
    elif validation_status == "VALIDATION_FAILED":
        is_valid = False
        logging.warning(f"Customer validation FAILED for {sender_email}")
    else:
        is_valid = False
        logging.warning(f"Unclear or invalid validation response format - defaulting to FAILED for security: {clean_response}")
    
    # Update conversation history
    history.append({"prompt": prompt, "response": response})
    
    # Store results
    ti.xcom_push(key="step_2_prompt", value=prompt)
    ti.xcom_push(key="step_2_validation_response", value=response)
    ti.xcom_push(key="conversation_history", value=history)
    ti.xcom_push(key="is_customer_valid", value=is_valid)
    
    logging.info(f"Step 2 completed: Customer validation = {'PASSED' if is_valid else 'FAILED'}")
    return response


def step_3_compose_email_response(ti, **context):
    """Step 3: Compose professional email response based on validation result."""
    logging.info("=== STEP 3: COMPOSING EMAIL RESPONSE ===")
    
    history = ti.xcom_pull(key="conversation_history")
    is_customer_valid = ti.xcom_pull(key="is_customer_valid")
    loan_data_from_step1 = ti.xcom_pull(key="step_1_loan_data")
    sender_email = ti.xcom_pull(key="sender_email")

    logging.info(f"Customer validation status: {'VALID' if is_customer_valid else 'INVALID'}")

    if not is_customer_valid:
        # Compose access denied email
        logging.info("Composing access denied email")
        access_denied_html = """<html><body>
        <p>Dear Valued Customer,</p>
        
        <p>Thank you for contacting AutoFinix regarding your loan information request.</p>
        
        <p>We regret to inform you that we cannot provide the requested information at this time. This could be due to:</p>
        <ul>
            <li>Your email address may not be registered with our system</li>
            <li>The loan information requested may not be associated with your account</li>
            <li>Additional security verification may be required</li>
        </ul>
        
        <p>For your security and privacy protection, please contact our customer support team directly at 
        <a href="mailto:autofinix-agent@lowtouch.ai">autofinix-agent@lowtouch.ai</a> or visit our nearest branch office for assistance with your loan inquiries.</p>
        
        <p>We apologize for any inconvenience and appreciate your understanding of these security measures.</p>
        
        <p>Best regards,<br>
        Customer Support Team<br>
        AutoFinix Financial Services</p>
        </body></html>"""
        
        history.append({"prompt": "Customer validation failed - composing access denied email", "response": access_denied_html})
        ti.xcom_push(key="step_3_prompt", value="Validation failed: Access denied email composed")
        ti.xcom_push(key="step_3_email_content", value=access_denied_html)
        ti.xcom_push(key="conversation_history", value=history)
        
        logging.info("Step 3 completed: Access denied email prepared")
        return access_denied_html

    # Customer is validated - compose email with loan data
    logging.info("Customer validated - composing email with loan information")
    
    prompt = f"""You are composing a professional business email response for AutoFinix Financial Services. 

CUSTOMER EMAIL: {sender_email} (VALIDATED ✓)
LOAN DATA FROM STEP 1: {loan_data_from_step1}

INSTRUCTIONS:
1. Create a professional email in American English
2. Use a tone appropriate for a senior Customer Success Manager
3. Address the customer courteously
4. Provide the exact loan information retrieved in Step 1
5. If Step 1 data shows "No records found" - inform customer appropriately
6. If Step 1 contains actual loan data - present it in a clear, professional format with proper HTML tables
7. Include a helpful closing and contact information

EMAIL REQUIREMENTS:
- Professional greeting addressing the customer
- Acknowledge their specific loan information request  
- Present the information from Step 1 clearly:
  * If no data found: Professionally explain that the requested information is not available
  * If data found: Format in clean HTML tables with proper headers and borders
- Closing paragraph offering further assistance
- Professional signature: "Customer Support Team, AutoFinix"
- Contact: autofinix-agent@lowtouch.ai

IMPORTANT:
- Use ONLY the actual data from Step 1 - do not create or hallucinate any loan information
- Ensure proper HTML formatting for tables if data exists
- Maintain professional but friendly tone
- Provide only the HTML draft of the email, with no introductory or concluding sentences outside the HTML content."""
    
    # Get AI response for email composition
    response = get_ai_response(prompt, conversation_history=history)
    
    # Clean up the response
    email_content = re.sub(r'```html\n|```', '', response).strip()
    
    # Ensure proper HTML structure
    if not email_content.strip().startswith('<!DOCTYPE') and not email_content.strip().startswith('<html'):
        if not email_content.strip().startswith('<'):
            email_content = f"<html><body>{email_content}</body></html>"

    # Update conversation history
    history.append({"prompt": prompt, "response": response})
    
    # Store results
    ti.xcom_push(key="step_3_prompt", value=prompt)
    ti.xcom_push(key="step_3_email_content", value=email_content)
    ti.xcom_push(key="conversation_history", value=history)

    logging.info(f"Step 3 completed: Professional email composed - {len(email_content)} characters")
    return email_content


def step_4_send_email_response(ti, **context):
    """Step 4: Send the final email response to the customer."""
    logging.info("=== STEP 4: SENDING EMAIL RESPONSE ===")
    
    try:
        email_data = context['dag_run'].conf.get("email_data", {})
        if not email_data:
            logging.warning("No email data received! This DAG was likely triggered manually.")
            return "No email data available - manual trigger detected"
        
        # Get email content and validation status
        email_content = ti.xcom_pull(key="step_3_email_content")
        is_customer_valid = ti.xcom_pull(key="is_customer_valid")
        sender_email = ti.xcom_pull(key="sender_email")
        
        if not email_content:
            logging.error("No email content found from Step 3")
            return "Error: No email content to send"
        
        # Authenticate Gmail service
        service = authenticate_gmail()
        if not service:
            logging.error("Gmail authentication failed, cannot send email response")
            return "Gmail authentication failed"
        
        # Prepare email metadata
        original_subject = email_data['headers'].get('Subject', 'Loan Request')
        reply_subject = f"Re: {original_subject}" if not original_subject.startswith('Re:') else original_subject
        in_reply_to = email_data["headers"].get("Message-ID", "")
        references = email_data["headers"].get("References", "")
        
        # Send email
        logging.info(f"Sending email to {sender_email} - Subject: {reply_subject}")
        result = send_email(
            service=service,
            recipient=sender_email,
            subject=reply_subject,
            body=email_content,
            in_reply_to=in_reply_to,
            references=references
        )
        
        if result:
            email_type = "access denied notification" if not is_customer_valid else "loan information response"
            success_message = f"Email sent successfully to {sender_email} - {email_type}"
            logging.info(f"✅ {success_message}")
            return success_message
        else:
            error_message = f"Failed to send email to {sender_email}"
            logging.error(f"❌ {error_message}")
            return error_message
            
    except Exception as e:
        error_message = f"Error in step_4_send_email_response: {str(e)}"
        logging.error(error_message)
        return error_message


# Load DAG documentation
readme_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'email_responder.md')
readme_content = ""
try:
    with open(readme_path, 'r') as file:
        readme_content = file.read()
except FileNotFoundError:
    readme_content = """
    # AutoFinix Loan Request Processing DAG
    
    This DAG processes loan information requests received via email with a secure 4-step validation process:
    
    1. **Extract Loan Request**: Parse email thread and retrieve loan data via API tools
    2. **Validate Customer Access**: Verify customer authorization to access requested information  
    3. **Compose Email Response**: Generate professional response based on validation result
    4. **Send Email Response**: Deliver appropriate response to customer
    
    ## Security Features
    - Customer email validation against registered accounts
    - API-based loan data retrieval (no data fabrication)
    - Access denied responses for unauthorized requests
    - Comprehensive logging and error handling
    """

# Define the DAG
with DAG(
    "autofinix_send_message_email",
    default_args=default_args,
    schedule=None,  # Triggered externally
    catchup=False,
    doc_md=readme_content,
    tags=["autofinix", "send", "email", "response"],
    description="Secure loan information request processing with customer validation"
) as dag:
    
    # Step 1: Extract loan request and retrieve data
    extract_request_task = PythonOperator(
        task_id="step_1_extract_loan_request",
        python_callable=step_1_extract_loan_request,
        doc_md="Extract loan request details from email and retrieve data using API tools"
    )
    
    # Step 2: Validate customer access
    validate_customer_task = PythonOperator(
        task_id="step_2_validate_customer_access",
        python_callable=step_2_validate_customer_access,
        doc_md="Validate customer authorization to access requested loan information"
    )
    
    # Step 3: Compose appropriate email response
    compose_email_task = PythonOperator(
        task_id="step_3_compose_email_response",
        python_callable=step_3_compose_email_response,
        doc_md="Compose professional email response based on validation result and loan data"
    )
    
    # Step 4: Send email response
    send_email_task = PythonOperator(
        task_id="step_4_send_email_response",
        python_callable=step_4_send_email_response,
        doc_md="Send final email response to customer"
    )
    
    # Define task dependencies
    extract_request_task >> validate_customer_task >> compose_email_task >> send_email_task
