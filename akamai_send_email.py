from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import base64
import json
import logging
import re
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
                body = msg.get_payload(decode=True).decode('latin-1')
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
        
        conversation = [{"role": msg["role"], "content": msg["content"]} for msg in messages_with_dates]
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

def send_email(service, to_email, subject, body_html, in_reply_to=None, references=None, thread_id=None):
    """
    Correct Gmail threading: always use messages().send() with threadId.
    """
    try:
        msg = MIMEMultipart("alternative")
        msg["From"] = CLOUD_ASSESS_FROM_ADDRESS
        msg["To"] = to_email
        msg["Subject"] = subject

        if in_reply_to:
            msg["In-Reply-To"] = in_reply_to

        # Build References chain
        ref_list = []
        if references:
            ref_list.extend(references.split())
        if in_reply_to and in_reply_to not in ref_list:
            ref_list.append(in_reply_to)

        if ref_list:
            msg["References"] = " ".join(ref_list[-20:])

        # Attach HTML
        msg.attach(MIMEText(body_html, "html", "utf-8"))

        # Encode message
        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()

        body = {"raw": raw}

        # 🔥 CORRECT WAY — Messages API allows threadId
        if thread_id:
            body["threadId"] = thread_id

        result = service.users().messages().send(
            userId="me",
            body=body
        ).execute()

        logging.info(f"Gmail message sent. Threading active. Message-ID: {result.get('id')}")
        return result

    except Exception as e:
        logging.error(f"Failed to send email: {str(e)}")
        return None

def step_1_process_email(ti, **context):
    """Step 1: Process message from email, detect attachment type, and handle attachments or errors."""
    email_data = context['dag_run'].conf.get("email_data", {})
    
    service = authenticate_gmail()
    if not service:
        logging.error("Gmail authentication failed, aborting.")
        return "Gmail authentication failed"
    
    complete_email_thread = get_email_thread(service, email_data, CLOUD_ASSESS_FROM_ADDRESS)
    image_attachments = []
    
    from_header = email_data["headers"].get("From", "Unknown <unknown@example.com>")
    sender_name = "Unknown"
    sender_email = "unknown@example.com"
    name_email_match = re.match(r'^(.*?)\s*<(.*?@.*?)>', from_header)
    if name_email_match:
        sender_name = name_email_match.group(1).strip() or "Unknown"
        sender_email = name_email_match.group(2).strip()
    elif re.match(r'^.*?@.*?$', from_header):
        sender_email = from_header.strip()
        sender_name = sender_email.split('@')[0]

    # Attachment type detection
    attachment_type = "none"
    has_excel = False
    has_pdf_or_image = False
    
    excel_mime_types = [
        "application/vnd.ms-excel",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    ]
    pdf_mime_types = ["application/pdf"]
    image_mime_types = ["image/png", "image/jpg", "image/jpeg"]
    
    if email_data.get("attachments"):
        logging.info(f"Number of attachments: {len(email_data['attachments'])}")
        for attachment in email_data["attachments"]:
            mime_type = attachment.get("mime_type", "").lower()
            logging.info(f"Checking attachment: {attachment.get('filename')} with MIME type: {mime_type}")
            
            # Detect Excel
            if mime_type in excel_mime_types:
                has_excel = True
                logging.info(f"Detected Excel attachment: {attachment['filename']}")
            # Detect PDF
            if mime_type in pdf_mime_types:
                has_pdf_or_image = True
                logging.info(f"Detected PDF attachment: {attachment['filename']}")
            # Detect Image
            elif mime_type in image_mime_types:
                attachment_type = "image"
                logging.info(f"Detected Image attachment: {attachment['filename']}")
                # Collect base64 image for AI processing
                if "base64_content" in attachment and attachment["base64_content"]:
                    image_attachments.append(attachment["base64_content"])
                    logging.info(f"Found base64 image attachment: {attachment['filename']}")

    # Determine attachment type based on what was found
    if has_excel and has_pdf_or_image:
        attachment_type = "combined"
        logging.info("Detected both Excel and PDF/Image attachments - combined flow")
    elif has_excel:
        attachment_type = "excel"
        logging.info("Detected Excel attachment only")
    elif has_pdf_or_image:
        attachment_type = "pdf_image"
        logging.info("Detected PDF or Image attachment only")
    else:
        attachment_type = "none"
        logging.warning("No valid attachments detected")

    ti.xcom_push(key="attachment_type", value=attachment_type)
    logging.info(f"Determined attachment type: {attachment_type}")

    # Extract attachment content (Markdown from Excel/PDF)
    attachment_content = ""
    has_valid_attachment = False
    if email_data.get("attachments"):
        for attachment in email_data["attachments"]:
            if "extracted_content" in attachment and "content" in attachment["extracted_content"]:
                content = attachment['extracted_content']['content']
                if content and "Unsupported" not in content and "Error" not in content:
                    attachment_content += f"\nAttachment Content from {attachment.get('filename', 'unknown')}:\n{content}\n"
                    has_valid_attachment = True
    
    # Handle no attachment edge case
    if not has_valid_attachment:
        attachment_content = "No valid Excel or PDF attachment found. Please attach the filled Excel or PDF for cloud assessment."

    # Split thread into history and current content
    if complete_email_thread:
        conversation_history = complete_email_thread[:-1]
        current_content = complete_email_thread[-1]["content"] if complete_email_thread else email_data.get("content", "").strip()
    else:
        conversation_history = []
        current_content = email_data.get("content", "").strip()
    
    # Log conversation history
    logging.info(f"Complete conversation history contains {len(conversation_history)} messages")
    logging.info(f"Current content: {current_content}...")
    for i, msg in enumerate(conversation_history[:3]):  # Log first 3 for brevity
        logging.info(f"History message {i+1}: Role={msg['role']}, Content={msg['content'][:100]}...")
    
    # Clean current content
    if current_content:
        soup = BeautifulSoup(current_content, "html.parser")
        current_content = soup.get_text(separator=" ", strip=True)
        current_content = remove_quoted_text(current_content)
    
    # Append attachment content to current content
    current_content += f"\n{attachment_content}"

    # Generate initial prompt based on attachment type
    if attachment_type == "excel":
        prompt = f"""
        Generate a **Final Assessment Report** based on the extracted details from the Excel invoice.

        Report Requirements:
        - Clear & concise
        - Bullet points
        - Highlight key findings, red flags, risks & optimization opportunities
        - Add cost insights if applicable
        - Do NOT include contact information

        Extracted Content:
        {current_content}
    """
    elif attachment_type == "pdf_image":
        prompt = f"""
        Generate a **Cost Comparison Report** based on the cost comparison analysis of the attached Invoice (PDF/Image).

        Report Requirements:
        - Clear & concise
        - Bullet points
        - Highlight key findings, cost savings, performance improvements, and migration opportunities
        - Do NOT include contact information

Extracted Content:
{current_content}
"""
    elif attachment_type == "combined":
        prompt = f"""
Generate a **Combined Assessment and Cost Comparison Report** based on both the Excel file and the PDF/Image invoice.

Report Requirements:
- Clear & concise
- Use bullet points
- Section 1: Assessment Report (from Excel data)
  - Key findings, red flags, risks & optimization opportunities
- Section 2: Cost Comparison Report (from PDF/Image invoice)
  - Cost savings, performance improvements, and migration opportunities
- Do NOT include contact information

Extracted Content:
{current_content}
"""
    else:
        prompt = f"""
The user has sent an email without valid attachments. Please provide a friendly response asking them to attach either:
- An Excel file for cloud assessment
- A PDF or Image of their invoice for cost comparison
- Both for a combined report

Email content:
{current_content}
"""

    logging.info(f"Generated prompt for AI: {prompt}...")
    
    response = get_ai_response(prompt, images=image_attachments if image_attachments else None, conversation_history=conversation_history)

    ti.xcom_push(key="step_1_prompt", value=prompt)
    ti.xcom_push(key="step_1_response", value=response)
    ti.xcom_push(key="conversation_history", value=conversation_history + [{"role": "user", "content": prompt}, {"role": "assistant", "content": response}])
    ti.xcom_push(key="email_content", value=current_content)
    ti.xcom_push(key="complete_email_thread", value=complete_email_thread)
    ti.xcom_push(key="sender_name", value=sender_name)
    ti.xcom_push(key="sender_email", value=sender_email)
    ti.xcom_push(key="image_attachments", value=image_attachments)
    
    logging.info(f"Step 1 completed: {response[:200]}...")
    return response

# ===== NEW: BRANCHING LOGIC =====
def branch_by_attachment_type(ti, **context):
    """Branch based on detected attachment type."""
    attachment_type = ti.xcom_pull(key="attachment_type")
    logging.info(f"Branching based on attachment type: {attachment_type}")
    
    if attachment_type == "excel":
        return "excel_flow_start"
    elif attachment_type == "pdf_image":
        return "cost_comparison_flow_start"
    elif attachment_type == "combined":
        return "combined_flow_start"
    else:
        return "no_attachment_flow"

def no_attachment_handler(ti, **context):
    """Handle case where no valid attachments are present."""
    step_1_response = ti.xcom_pull(key="step_1_response")
    sender_name = ti.xcom_pull(key="sender_name") or "Valued Client"
    
    prompt = f"""
Generate a professional, friendly email in American English to {sender_name} explaining that we need attachments to process their request.

Content from AI:
{step_1_response}

Requirements:
1. Greeting with their name
2. Polite explanation that attachments are needed
3. List what types of files we accept:
   - Excel files (.xlsx, .xls) for cloud assessment
   - PDF or Image files for cost comparison
   - Both types for a combined report
4. Friendly closing
5. Professional signature

Format as clean HTML suitable for email. Do not include contact information.
Return only the HTML body content without html, head, or body tags.
"""
    
    conversation_history = ti.xcom_pull(key="conversation_history") or []
    response = get_ai_response(prompt, conversation_history=conversation_history)
    
    cleaned_response = re.sub(r'```html\n|```', '', response).strip()
    if not cleaned_response.strip().startswith('<'):
        cleaned_response = f"<div>{cleaned_response}</div>"
    
    full_history = conversation_history + [
        {"role": "user", "content": prompt},
        {"role": "assistant", "content": response}
    ]
    
    ti.xcom_push(key="markdown_email_content", value=cleaned_response)
    ti.xcom_push(key="conversation_history", value=full_history)
    ti.xcom_push(key="email_type", value="no_attachment")
    
    return response

def cost_comparison_generate_report(ti, **context):
    """Generate cost comparison report for PDF/Image attachments."""
    try:
        step_1_response = ti.xcom_pull(key="step_1_response")
        email_content = ti.xcom_pull(key="email_content")
        conversation_history = ti.xcom_pull(key="conversation_history") or []
        image_attachments = ti.xcom_pull(key="image_attachments") or []
        
        prompt = f"""
        Based on the invoice details extracted from the attached PDF or image, generate a comprehensive Cost Comparison Report comparing the current costs with Akamai's offerings.
        
        Extracted Invoice Details:
        {step_1_response}
        
        Email Context:
        {email_content}
        
        Requirements:
        1. Extract all pricing information from the invoice
        2. Compare with equivalent Akamai services and pricing
        3. Highlight cost savings opportunities
        4. Provide detailed analysis of:
           - Current spending breakdown
           - Akamai equivalent services
           - Potential cost savings (percentage and absolute values)
           - Performance benefits with Akamai
           - Security and reliability improvements
        5. Include a summary section with key recommendations
        
        Format the report in clean HTML suitable for email delivery. Do not include any contact information.
        """
        
        logging.info("Generating cost comparison report...")
        response = get_ai_response(prompt, images=image_attachments if image_attachments else None, conversation_history=conversation_history)
        
        # Update conversation history
        full_history = conversation_history + [
            {"role": "user", "content": prompt},
            {"role": "assistant", "content": response}
        ]
        
        ti.xcom_push(key="cost_comparison_report", value=response)
        ti.xcom_push(key="conversation_history", value=full_history)
        
        logging.info(f"Cost comparison report generated: {response[:200]}...")
        return response
        
    except Exception as e:
        logging.error(f"Error in cost_comparison_generate_report: {str(e)}")
        raise

def cost_comparison_compose_email(ti, **context):
    """Compose email with cost comparison report."""
    try:
        cost_comparison_report = ti.xcom_pull(key="cost_comparison_report")
        sender_name = ti.xcom_pull(key="sender_name") or "Valued Client"
        
        prompt = f"""
        Generate a professional, human-like business email in American English, written in the tone of a senior AI Engineer at lowtouch.ai, to notify the client, addressed by name '{sender_name}', about the cost comparison analysis.

        Cost Comparison Report Content:
        {cost_comparison_report}

        Follow this exact structure for the email body, using clean, valid HTML without any additional wrappers like <html> or <body>. Do not omit any sections or elements listed below.

        1. Greeting: 
        <p>Dear {sender_name},</p>

        2. Opening paragraph: 
        <p>We have analyzed your invoice and completed a comprehensive cost comparison with Akamai's cloud services. This analysis identifies potential cost savings, performance improvements, and strategic opportunities for your infrastructure. Here is a detailed comparison to guide your decision-making:</p>

        3. Cost Comparison Report:
        <h2>Cost Comparison Report</h2>
        {cost_comparison_report}

        4. Signature: 
        <p>Best regards</p>

        Ensure the email is natural, professional, and includes the complete cost comparison report. Return only the HTML body content without <html>, <body>, or <head> tags.
        """
        
        conversation_history = ti.xcom_pull(key="conversation_history") or []
        response = get_ai_response(prompt, conversation_history=conversation_history)
        
        # Clean the HTML response
        cleaned_response = re.sub(r'```html\n|```', '', response).strip()
        if not cleaned_response.strip().startswith('<'):
            cleaned_response = f"<div>{cleaned_response}</div>"
        
        # Update history
        full_history = conversation_history + [
            {"role": "user", "content": prompt},
            {"role": "assistant", "content": response}
        ]
        
        ti.xcom_push(key="markdown_email_content", value=cleaned_response)
        ti.xcom_push(key="conversation_history", value=full_history)
        ti.xcom_push(key="email_type", value="cost_comparison")
        
        logging.info(f"Cost comparison email composed: {response[:200]}...")
        return response
        
    except Exception as e:
        logging.error(f"Error in cost_comparison_compose_email: {str(e)}")
        raise

def combined_excel_pdf_summary(ti, **context):
    """Generate combined assessment and cost comparison report."""
    try:
        step_1_response = ti.xcom_pull(key="step_1_response")
        email_content = ti.xcom_pull(key="email_content")
        conversation_history = ti.xcom_pull(key="conversation_history") or []
        image_attachments = ti.xcom_pull(key="image_attachments") or []
        sender_name = ti.xcom_pull(key="sender_name") or "Valued Client"

        prompt = f"""Generate a **Final Assessment Report and cost comparison Report** based on the extracted details from the Excel invoice and the attached PDF/Image.

        Report Requirements:
        - Clear & concise
        - Bullet points
        - Highlight key findings, red flags, risks & optimization opportunities
        - Add cost insights if applicable
        - Do NOT include contact information
        - Highlight key findings, **cost savings**, performance improvements, and migration opportunities
        - The report should combine both assessment and cost comparison aspects seamlessly
        - Format the report in clean Markdown suitable for email delivery
        - Do NOT include contact information

    Data extracted:
    {step_1_response}

    Additional email context:
    {email_content}

Structure the email as follows:

1. Greeting: 
<p>Dear {sender_name},</p>

2. Opening paragraph:
<p>We have completed both the cloud assessment and cost comparison analysis based on your submitted files. This comprehensive report provides insights into your current infrastructure and identifies cost optimization opportunities with Akamai's services.</p>

3. Table of Contents:
<h2>Report Contents</h2>
<ul>
<li><a href="#assessment">Cloud Assessment Report</a></li>
<li><a href="#cost-comparison">Cost Comparison Report</a></li>
</ul>

4. Assessment Report Section:
<h2 id="assessment">Cloud Assessment Report</h2>
[Generate assessment report content here]

5. Cost Comparison Section:
<h2 id="cost-comparison">Cost Comparison Report</h2>
[Generate cost comparison content here]

6. Closing and Signature:
<p>Best regards</p>

Requirements:
    - Maintain a clear, concise, and professional tone throughout the response.
    - Present all findings using bullet points for easy readability.
    - Clearly highlight key insights, risks, and opportunities identified during the analysis.
    - Emphasize cost savings, performance improvements, and efficiency gains wherever applicable.
    - Strictly exclude any contact information from the response.

Return only the HTML body content without html, head, or body tags.
"""

        logging.info("Generating combined report...")
        response = get_ai_response(prompt, images=image_attachments if image_attachments else None, conversation_history=conversation_history)
        
        cleaned_response = re.sub(r'```html\n|```', '', response).strip()
        if not cleaned_response.strip().startswith('<'):
            cleaned_response = f"<div>{cleaned_response}</div>"

        full_history = conversation_history + [
            {"role": "user", "content": prompt},
            {"role": "assistant", "content": response}
        ]

        ti.xcom_push(key="markdown_email_content", value=cleaned_response)
        ti.xcom_push(key="email_type", value="combined_report")
        ti.xcom_push(key="conversation_history", value=full_history)
        
        logging.info(f"Combined report generated: {response[:200]}...")
        return response
        
    except Exception as e:
        logging.error(f"Error in combined_excel_pdf_summary: {str(e)}")
        raise

def step_2_compose_email(ti, **context):
    """Step 2: Compose professional HTML email for Excel assessment."""
    step_1_response = ti.xcom_pull(key="step_1_response")
    sender_name = ti.xcom_pull(key="sender_name") or "Valued Client"
    content_appended = step_1_response if step_1_response else "No assessment generated."
    
    prompt = f"""
        Generate a professional, human-like business email in American English, written in the tone of a senior AI Engineer at lowtouch.ai, to notify the client, addressed by name '{sender_name}', about the cloud assessment results.

        Content: {content_appended}

        Follow this exact structure for the email body, using clean, valid HTML without any additional wrappers like <html> or <body>. Do not omit any sections or elements listed below. Use natural, professional wording but adhere strictly to the format. Extract all details from the provided Content; use 'N/A' if a value is not available. Infuse insights where appropriate to make the summary analytical and actionable, highlighting implications for the client's operations, potential risks, opportunities for optimization, and alignments with Akamai's cloud solutions or lowtouch.ai's agentic AI capabilities for automation.

        1. Greeting: <p>Dear {sender_name},</p>

        2. Opening paragraph: <p>We have received your filled details and completed the cloud assessment for Akamai. This assessment analyzes your current setup to identify strengths, areas for improvement, and strategic opportunities. Here is a insightful summary to guide next steps:</p>

3. Assessment Report:
<h2>Assessment Report</h2>
{content_appended}

4. Signature: 
<p>Best regards</p>

Ensure the email is natural, professional, and concise. Return only the HTML body content without html, head, or body tags.
"""
    
    conversation_history = ti.xcom_pull(key="conversation_history") or []
    response = get_ai_response(prompt, conversation_history=conversation_history)
    # Clean the HTML response
    cleaned_response = re.sub(r'```html\n|```', '', response).strip()
    if not cleaned_response.strip().startswith('<'):
        cleaned_response = f"<div>{cleaned_response}</div>"
    
    full_history = conversation_history + [
        {"role": "user", "content": prompt},
        {"role": "assistant", "content": response}
    ]
    
    ti.xcom_push(key="markdown_email_content", value=cleaned_response)
    ti.xcom_push(key="conversation_history", value=full_history)
    ti.xcom_push(key="email_type", value="assessment")
    
    logging.info(f"Step 2 completed: {response[:200]}...")
    return response

def step_3_convert_to_html(ti, **context):
    """Step 3: Ensure HTML is properly formatted and valid."""
    try:
        markdown_email = ti.xcom_pull(key="markdown_email_content") or "No email content available."
        
        # Check if content is already HTML
        if markdown_email.strip().startswith('<'):
            logging.info("Content is already HTML, using as-is")
            ti.xcom_push(key="final_html_content", value=markdown_email)
            return markdown_email
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
        
        logging.info("Converting content to HTML...")
        conversation_history = ti.xcom_pull(key="conversation_history") or []
        html_response = get_ai_response(prompt, conversation_history=conversation_history)
        
        cleaned_response = re.sub(r'```html\n|```', '', html_response).strip()
        if not cleaned_response.strip().startswith('<'):
            cleaned_response = f"<div>{cleaned_response}</div>"
        
        ti.xcom_push(key="final_html_content", value=cleaned_response)
        logging.info(f"HTML email content generated: {cleaned_response[:200]}...")
        return cleaned_response
        
    except Exception as e:
        logging.error(f"Error in step_3_convert_to_html: {str(e)}")
        # Fallback to original content if conversion fails
        markdown_email = ti.xcom_pull(key="markdown_email_content") or "<p>Error generating email content.</p>"
        ti.xcom_push(key="final_html_content", value=markdown_email)
        return markdown_email

def step_4_send_email(ti, **context):
    """Step 4: Send the final email."""
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

        # Get sender email safely
        sender_email = ti.xcom_pull(key="sender_email")
        if not sender_email:
            sender_email = next(
                (h["value"] for h in email_data.get("headers", []) if h.get("name", "").lower() == "from"),
                "unknown@example.com"
            )

        # === THREADING INFORMATION ===
        original_message_id = email_data["headers"].get("Message-ID", "")
        original_references = email_data["headers"].get("References", "")
        original_thread_id   = email_data.get("threadId")          # This is the Gmail thread ID

        # === BUILD SUBJECT – strip any existing "Re:" to avoid Re: Re: Re: ===
        original_subject = email_data["headers"].get("Subject", "Cloud Assessment")
        clean_subject = re.sub(r'^Re:\s*', '', original_subject, flags=re.IGNORECASE).strip()

        email_type = ti.xcom_pull(key="email_type", default="assessment")

        if email_type == "cost_comparison":
            subject = f"Re: {clean_subject} - Cost Comparison Report"
        elif email_type == "combined_report":
            subject = f"Re: {clean_subject} - Assessment & Cost Comparison"
        elif email_type == "no_attachment":
            subject = f"Re: {clean_subject} - Attachment Needed"
        else:
            subject = f"Re: {clean_subject} - Assessment Report"

        # === SEND EMAIL WITH PERFECT THREADING ===
        result = send_email(
            service=service,
            to_email=sender_email,
            subject=subject,
            body_html=final_html_content,
            in_reply_to=original_message_id,
            references=original_references,
            thread_id=original_thread_id           # guarantees perfect Gmail threading
        )
        
        if result:
            logging.info(f"Email sent successfully to {sender_email}")
            return f"Email sent successfully to {sender_email}"
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
    
    # ===== NEW: BRANCHING TASK =====
    branch_task = BranchPythonOperator(
        task_id="branch_by_attachment_type",
        python_callable=branch_by_attachment_type,
        provide_context=True
    )
    # ===== END NEW SECTION =====
    
    # ===== NEW: DUMMY TASKS FOR FLOW MARKERS =====
    excel_flow_start = DummyOperator(
        task_id="excel_flow_start"
    )
    
    cost_comparison_flow_start = DummyOperator(
        task_id="cost_comparison_flow_start"
    )
    
    combined_flow_start = DummyOperator(
        task_id="combined_flow_start"
    )
    
    no_attachment_flow = DummyOperator(
        task_id="no_attachment_flow"
    )
    
    # Excel flow (Assessment)
    task_2 = PythonOperator(
        task_id="step_2_compose_email",
        python_callable=step_2_compose_email,
        provide_context=True
    )
    
    # ===== NEW: COST COMPARISON FLOW TASKS =====
    cost_comparison_report_task = PythonOperator(
        task_id="cost_comparison_generate_report",
        python_callable=cost_comparison_generate_report,
        provide_context=True
    )
    
    cost_comparison_email_task = PythonOperator(
        task_id="cost_comparison_compose_email",
        python_callable=cost_comparison_compose_email,
        provide_context=True
    )
    combined_flow_task = PythonOperator(
        task_id="combined_excel_pdf_summary",
        python_callable=combined_excel_pdf_summary,
        provide_context=True
    )
    
    # No attachment flow
    no_attachment_task = PythonOperator(
        task_id="no_attachment_handler",
        python_callable=no_attachment_handler,
        provide_context=True
    )
    
    # HTML conversion (all flows converge here)
    task_3 = PythonOperator(
        task_id="step_3_convert_to_html",
        python_callable=step_3_convert_to_html,
        provide_context=True,
        trigger_rule="none_failed_min_one_success"  # Allow task to run from either flow
    )

    task_4 = PythonOperator(
        task_id="step_4_send_email",
        python_callable=step_4_send_email,
        provide_context=True
    )
    
    task_1 >> branch_task
    branch_task >> [excel_flow_start, cost_comparison_flow_start, combined_flow_start, no_attachment_flow]
    excel_flow_start >> task_2 >> task_3
    cost_comparison_flow_start >> cost_comparison_report_task >> cost_comparison_email_task >> task_3
    combined_flow_start >> combined_flow_task >> task_3
    no_attachment_flow >> no_attachment_task >> task_3
    task_3 >> task_4