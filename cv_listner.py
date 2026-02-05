"""
Airflow DAG for monitoring mailbox for CV submissions and screening responses.
Intelligently routes emails to appropriate DAGs based on content analysis.
Extracts candidate email from CV PDF for accurate candidate matching.
Includes full thread history extraction with PDF content integration.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
from email.utils import parseaddr
from pathlib import Path
import json
import os
import base64

import sys
import logging
from bs4 import BeautifulSoup  

try:
    sys.path.append(os.path.dirname(os.path.realpath(__file__)))
    logging.info(f"Appended to sys.path: {os.path.dirname(os.path.realpath(__file__))}")
except Exception as e:
    logging.error(f"Error appending to sys.path: {e}")
    
# Import utility functions
from agent_dags.utils.email_utils import (
    authenticate_gmail,
    fetch_unread_emails_with_attachments,
    get_last_checked_timestamp,
    update_last_checked_timestamp,
    process_email_attachments
)
from agent_dags.utils.agent_utils import get_ai_response, extract_json_from_text, extract_pdf_content

# Import Google Sheets utilities
from agent_dags.utils.sheets_utils import (
    authenticate_google_sheets,
    find_candidate_in_sheet,
    get_candidate_details
)


# ============================================================================
# THREAD HISTORY EXTRACTION WITH PDF CONTENT
# ============================================================================

def extract_message_body(payload):
    """
    Extract the text body from email payload.
    
    Args:
        payload: Gmail message payload
    
    Returns:
        str: Extracted message body
    """
    try:
        # 1. Direct body
        if "body" in payload and "data" in payload["body"]:
            body_data = payload["body"]["data"]
            decoded = base64.urlsafe_b64decode(body_data + "==").decode("utf-8", errors="ignore")
            return decoded.strip()

        # 2. Multipart - look for text parts
        if "parts" in payload:
            plain_body = None
            html_body = None

            def find_text_parts(part):
                nonlocal plain_body, html_body
                mime = part.get("mimeType", "")

                if mime == "text/plain" and "data" in part.get("body", {}):
                    plain_body = base64.urlsafe_b64decode(
                        part["body"]["data"] + "=="
                    ).decode("utf-8", errors="ignore").strip()

                elif mime == "text/html" and "data" in part.get("body", {}):
                    html_body = base64.urlsafe_b64decode(
                        part["body"]["data"] + "=="
                    ).decode("utf-8", errors="ignore")

                # Recurse into nested parts
                if "parts" in part:
                    for subpart in part["parts"]:
                        find_text_parts(subpart)

            for part in payload["parts"]:
                find_text_parts(part)

            # Preference order: plain > cleaned HTML
            if plain_body:
                return plain_body

            if html_body:
                # Convert HTML → plain text (removes tags, keeps readable content)
                soup = BeautifulSoup(html_body, "html.parser")
                text = soup.get_text(separator="\n", strip=True)
                return text

        return ""
        
    except Exception as e:
        logging.error(f"Error extracting message body: {str(e)}")
        return ""


def extract_thread_history_with_attachments(service, thread_id, attachment_dir, current_message_id=None):
    """
    Extract all messages from a Gmail thread with PDF content integrated.
    
    Args:
        service: Gmail API service instance
        thread_id: Gmail thread ID
        attachment_dir: Directory to save/process attachments
        current_message_id: ID of the current/latest message (will be excluded from history)
    
    Returns:
        dict: {
            'history': [  # All messages except the current one
                {
                    'role': 'user' or 'assistant',
                    'content': 'message body',
                    'sender': 'email@example.com',
                    'timestamp': 1234567890,
                    'has_pdf': True/False,
                    'pdf_content': 'extracted text...'
                }
            ],
            'current_message': {  # The latest message
                'content': 'message body',
                'sender': 'email@example.com',
                'pdf_content': 'extracted text...' or None
            }
        }
    """
    try:
        # Fetch the entire thread
        thread = service.users().threads().get(
            userId="me",
            id=thread_id,
            format="full"
        ).execute()
        
        messages = thread.get("messages", [])
        
        if not messages:
            logging.warning(f"No messages found in thread {thread_id}")
            return {'history': [], 'current_message': None}
        
        # Get the authenticated email address to determine role
        profile = service.users().getProfile(userId="me").execute()
        bot_email = profile.get("emailAddress", "").lower()
        
        history = []
        current_message = None
        
        # Process messages in chronological order (oldest to newest)
        for idx, msg in enumerate(messages):
            try:
                message_id = msg.get("id")
                payload = msg.get("payload", {})
                
                # Extract headers
                headers = {
                    header["name"]: header["value"]
                    for header in payload.get("headers", [])
                }
                
                sender = headers.get("From", "")
                _, sender_email = parseaddr(sender)
                sender_email = sender_email.lower()
                
                # Determine role based on sender
                role = "assistant" if sender_email == bot_email else "user"
                
                # Extract message body
                body = extract_message_body(payload)
                
                # Get timestamp
                timestamp = int(msg.get("internalDate", 0))
                
                # Process attachments (looking for PDFs)
                attachments = process_email_attachments(
                    service=service,
                    message_id=message_id,
                    payload=payload,
                    attachment_dir=attachment_dir
                )
                
                # Extract PDF content if present
                pdf_content = None
                has_pdf = False
                
                for att in attachments:
                    if att.get("mime_type") == "application/pdf":
                        has_pdf = True
                        pdf_path = att.get("path")
                        
                        if pdf_path and Path(pdf_path).exists():
                            logging.info(f"Extracting PDF content from {pdf_path} in message {message_id}")
                            pdf_data = extract_pdf_content(pdf_path)
                            
                            # Handle both dict and string returns
                            if isinstance(pdf_data, dict):
                                pdf_content = pdf_data.get("content", "")
                            else:
                                pdf_content = pdf_data
                            break  # Use first PDF found
                
                # Build message object
                message_obj = {
                    'role': role,
                    'content': body,
                    'sender': sender_email,
                    'timestamp': timestamp,
                    'has_pdf': has_pdf,
                    'pdf_content': pdf_content,
                    'message_id': message_id
                }
                
                # If this is the current message (last one), separate it
                if current_message_id and message_id == current_message_id:
                    current_message = message_obj
                    logging.info(f"Identified current message: {message_id}")
                elif idx == len(messages) - 1 and not current_message_id:
                    # If no current_message_id provided, treat last message as current
                    current_message = message_obj
                    logging.info(f"Using last message as current: {message_id}")
                else:
                    # Add to history
                    history.append(message_obj)
                    logging.info(f"Added message {message_id} to history (role: {role}, has_pdf: {has_pdf})")
            except Exception as msg_e:
                logging.error(f"Error processing message {msg.get('id')}: {str(msg_e)}", exc_info=True)
                continue
        
        logging.info(f"Extracted {len(history)} historical messages and 1 current message from thread {thread_id}")
        
        return {
            'history': history,
            'current_message': current_message
        }
        
    except Exception as e:
        logging.error(f"Error extracting thread history: {str(e)}", exc_info=True)
        return {'history': [], 'current_message': None}


def format_history_for_ai(history_data):
    """
    Format extracted thread history for AI conversation context.
    Returns list compatible with get_ai_response expectations.
    """
    formatted_history = []
    
    history = history_data.get('history', [])
    
    # Group user messages with following assistant messages, skipping empties
    i = 0
    while i < len(history):
        msg = history[i]

        if msg['role'] == 'user':
            user_content = msg['content'].strip()
            if msg.get('has_pdf') and msg.get('pdf_content'):
                user_content += f"\n\n[Attached PDF Content]:\n{msg['pdf_content']}".strip()

            if not user_content:
                i += 1
                continue

            assistant_content = ""
            if i + 1 < len(history) and history[i + 1]['role'] == 'assistant':
                ass_msg = history[i + 1]
                assistant_content = ass_msg['content'].strip()
                if ass_msg.get('has_pdf') and ass_msg.get('pdf_content'):
                    assistant_content += f"\n\n[Attached PDF Content]:\n{ass_msg['pdf_content']}".strip()
                i += 1  # skip the assistant we just processed

            # Always add the pair — even if assistant is empty
            # But log it clearly
            if not assistant_content:
                logging.warning(f"Assistant reply is empty for user message {msg.get('message_id')}")

            formatted_history.append({
                'prompt': user_content,
                'response': assistant_content
            })

        else:
            logging.debug(f"Skipping standalone assistant {msg.get('message_id')}")
            i += 1

        i += 1

    return formatted_history


def build_current_message_with_pdf(history_data):
    """
    Build the current user message with PDF content if present.
    
    Args:
        history_data: Output from extract_thread_history_with_attachments
    
    Returns:
        str: Current message content with PDF appended if present
    """
    current = history_data.get('current_message')
    
    if not current:
        return ""
    
    content = current.get('content', '')
    
    # Append PDF content if present in current message
    if current.get('has_pdf') and current.get('pdf_content'):
        content += f"\n\n[Attached PDF Content]:\n{current['pdf_content']}"
    
    return content


# ============================================================================
# ORIGINAL FUNCTIONS (ENHANCED)
# ============================================================================

def find_cv_in_thread(service, thread_id, attachment_dir):
    """
    Search all messages in a Gmail thread for CV attachments.
    Returns first CV attachment found (PDF or image), else None.
    """
    try:
        thread = service.users().threads().get(
            userId="me",
            id=thread_id,
            format="full"
        ).execute()

        # Traverse newest → oldest
        for msg in reversed(thread.get("messages", [])):
            payload = msg.get("payload", {})
            message_id = msg.get("id")

            attachments = process_email_attachments(
                service=service,
                message_id=message_id,
                payload=payload,
                attachment_dir=attachment_dir
            )

            cv_attachments = [
                att for att in attachments
                if att["mime_type"] == "application/pdf"
                or att["mime_type"].startswith("image/")
            ]

            if cv_attachments:
                logging.info(
                    f"Found CV in previous thread message {message_id}"
                )
                return cv_attachments

        return None

    except Exception as e:
        logging.error(f"Thread CV lookup failed: {str(e)}")
        return None

# Configuration constants
GMAIL_CREDENTIALS = Variable.get("ltai.v3.lowtouch.recruitment.email_credentials", default_var=None)
RECRUITMENT_FROM_ADDRESS = Variable.get("ltai.v3.lowtouch.recruitment.from_address", default_var=None)
GOOGLE_SHEETS_CREDENTIALS = Variable.get("ltai.v3.lowtouch.recruitment.sheets_credentials", default_var=None)
GOOGLE_SHEETS_ID = Variable.get("ltai.v3.lowtouch.recruitment.sheets_id", default_var=None)
LAST_PROCESSED_FILE = "/appz/cache/cv_last_processed_email.json"
ATTACHMENT_DIR = "/appz/data/cv_attachments/"

# Default DAG arguments
default_args = {
    "owner": "lowtouch.ai_developers",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 24),
    "retries": 1,
    "retry_delay": timedelta(seconds=15),
}


def extract_email_from_cv(cv_text, model_name):
    """
    Extract candidate email address from CV text using AI.
    
    Args:
        cv_text: Extracted text from CV PDF
        model_name: AI model to use for extraction
    
    Returns:
        str: Extracted email address or None if not found
    """
    try:
        prompt = f"""Extract the candidate's email address from this CV/resume text.

## CV Text:
{cv_text[:3000]}

## Instructions:
- Look for email addresses in the contact information section
- Return ONLY the email address, nothing else
- If multiple emails are found, return the primary/personal email
- If no email is found, return "NOT_FOUND"

## Output Format:
```json
{{
    "email": "<email_address or NOT_FOUND>",
    "confidence": <0-100>,
    "location": "<where in CV the email was found, e.g., 'header', 'contact section'>"
}}
```
"""
        
        response = get_ai_response(prompt, stream=False, model=model_name)
        result = extract_json_from_text(response)
        
        extracted_email = result.get('email', 'NOT_FOUND')
        confidence = result.get('confidence', 0)
        location = result.get('location', 'unknown')
        
        if extracted_email and extracted_email != 'NOT_FOUND':
            logging.info(f"Extracted email from CV: {extracted_email} (confidence: {confidence}%, location: {location})")
            return extracted_email
        else:
            logging.warning("No email found in CV text")
            return None
            
    except Exception as e:
        logging.error(f"Error extracting email from CV: {str(e)}")
        return None


def fetch_cv_emails(**kwargs):
    """
    Fetch unread emails with CV attachments from Gmail.
    Processes emails received after the last processed timestamp.
    """
    try:
        # Authenticate Gmail service
        service = authenticate_gmail(GMAIL_CREDENTIALS, RECRUITMENT_FROM_ADDRESS)
        if not service:
            logging.error("Gmail authentication failed")
            return []
        
        # Get last processed timestamp
        last_checked = get_last_checked_timestamp(LAST_PROCESSED_FILE)
        logging.info(f"Last checked timestamp: {last_checked}")
        
        # Build custom query with timestamp and unread filter
        query = f"is:unread after:{last_checked // 1000}"
        logging.info(f"Using query: {query}")
        
        # Fetch unread emails with attachments
        unread_emails = fetch_unread_emails_with_attachments(
            service=service,
            attachment_dir=ATTACHMENT_DIR,
            query=query,
            last_checked_timestamp=last_checked
        )
        
        logging.info(f"Found {len(unread_emails)} unread emails with attachments")
        
        # Update last processed timestamp if emails were found
        if unread_emails:
            max_timestamp = max(email['timestamp'] for email in unread_emails)
            update_last_checked_timestamp(LAST_PROCESSED_FILE, max_timestamp)
            logging.info(f"Updated last processed timestamp to: {max_timestamp}")
        
        # Push to XCom for downstream tasks
        kwargs['ti'].xcom_push(key='unread_emails', value=unread_emails)
        
        return unread_emails
        
    except Exception as e:
        logging.error(f"Error fetching CV emails: {str(e)}")
        raise


def classify_email_type(**kwargs):
    """
    Classify each email using AI with full thread context and PDF content.
    Extracts complete conversation history and integrates PDF content.
    """
    ti = kwargs["ti"]
    unread_emails = ti.xcom_pull(
        task_ids="fetch_cv_emails",
        key="unread_emails"
    )

    if not unread_emails:
        logging.info("No emails to classify")
        ti.xcom_push(key="classified_emails", value=[])
        return []

    # Gmail service (reuse same credentials)
    service = authenticate_gmail(
        GMAIL_CREDENTIALS,
        RECRUITMENT_FROM_ADDRESS
    )

    if not service:
        raise RuntimeError("Failed to authenticate Gmail service")

    # Google Sheets auth
    auth_type = Variable.get(
        "ltai.v3.lowtouch.recruitment.sheets_auth_type",
        default_var="oauth"
    )
    sheets_service = authenticate_google_sheets(
        GOOGLE_SHEETS_CREDENTIALS,
        auth_type=auth_type
    )
    sheets_available = bool(sheets_service)

    MODEL_NAME = Variable.get(
        "ltai.v3.lowtouch.recruitment.model_name",
        default_var="recruitment:0.3af"
    )

    classified_emails = []

    for email in unread_emails:
        try:
            email_id = email.get("id")
            thread_id = email.get("threadId")
            headers = email.get("headers", {})
            sender = headers.get("From", "Unknown")
            _, sender_email = parseaddr(sender)
            subject = headers.get("Subject", "")

            # =====================================================================
            # EXTRACT FULL THREAD HISTORY WITH PDF CONTENT
            # =====================================================================
            logging.info(f"Extracting thread history for email {email_id} in thread {thread_id}")
            
            thread_data = extract_thread_history_with_attachments(
                service=service,
                thread_id=thread_id,
                attachment_dir=ATTACHMENT_DIR,
                current_message_id=email_id
            )
            
            # Format history for AI
            conversation_history = format_history_for_ai(thread_data)
            
            # Build current message with PDF
            current_message_content = build_current_message_with_pdf(thread_data)
            logging.info(f"current message content: {current_message_content}...")
            logging.info(f"Thread context: {len(conversation_history)} previous messages")
            
            # =====================================================================
            # FIND CV IN CURRENT OR PREVIOUS MESSAGES
            # =====================================================================
            attachments = email.get("attachments", [])
            cv_attachments = [
                att for att in attachments
                if att.get("mime_type") == "application/pdf"
                or att.get("mime_type", "").startswith("image/")
            ]

            # Fallback: search previous thread messages for CV
            if not cv_attachments and thread_id:
                logging.info(f"No CV in current message. Searching thread {thread_id}")
                thread_cvs = find_cv_in_thread(
                    service=service,
                    thread_id=thread_id,
                    attachment_dir=ATTACHMENT_DIR
                )
                if thread_cvs:
                    cv_attachments = thread_cvs

            has_cv_attachment = bool(cv_attachments)

            # =====================================================================
            # EXTRACT CANDIDATE EMAIL FROM CV
            # =====================================================================
            candidate_email = None
            cv_text = None

            if cv_attachments:
                for att in cv_attachments:
                    if att.get("mime_type") == "application/pdf":
                        cv_path = att.get("path")
                        if cv_path and Path(cv_path).exists():
                            logging.info(f"Extracting CV text from {cv_path}")
                            cv_data = extract_pdf_content(cv_path)
                            
                            # Handle both dict and string returns
                            if isinstance(cv_data, dict):
                                cv_text = cv_data.get("content", "")
                            else:
                                cv_text = cv_data
                            
                            candidate_email = extract_email_from_cv(cv_text, MODEL_NAME)
                            if candidate_email:
                                break

            search_email = candidate_email or sender_email

            # =====================================================================
            # GOOGLE SHEETS LOOKUP
            # =====================================================================
            candidate_exists = False
            candidate_profile = None

            if sheets_available and GOOGLE_SHEETS_ID:
                try:
                    existing = find_candidate_in_sheet(
                        sheets_service,
                        GOOGLE_SHEETS_ID,
                        search_email,
                        sheet_name="Sheet1"  # Add this parameter
                    )

                    if existing:
                        candidate_exists = True
                        candidate_profile = get_candidate_details(
                            sheets_service,
                            GOOGLE_SHEETS_ID,
                            search_email,
                            sheet_name="Sheet1"  # Add this parameter
                        )
                except Exception as e:
                    logging.error(f"Error accessing Google Sheets: {str(e)}")
                    candidate_exists = False
                    candidate_profile = None

            # =====================================================================
            # BUILD AI CLASSIFICATION PROMPT WITH THREAD CONTEXT
            # =====================================================================
            prompt = f"""
                        Analyze this recruitment email with full conversation context.

                        ## Current Message:
                        Sender: {sender_email}
                        Candidate Email (from CV): {candidate_email or "Not extracted"}
                        Search Email Used: {search_email}
                        Subject: {subject}
                        Has CV Attachment: {has_cv_attachment}

                        Content:
                        {current_message_content}

                        ## Conversation Context:
                        Previous Messages in Thread: {len(conversation_history)}
                        """

            # Add conversation history summary
            if conversation_history:
                prompt += "\nPrevious Conversation:\n"
                for i, hist_msg in enumerate(conversation_history[-3:], 1):  # Last 3 conversation pairs
                    # Each hist_msg has 'prompt' (user) and 'response' (assistant)
                    user_msg = hist_msg.get('prompt', '')
                    assistant_msg = hist_msg.get('response', '')
                    
                    if user_msg:
                        prompt += f"{i}a. [user]: {user_msg[:200]}...\n"
                    if assistant_msg:
                        prompt += f"{i}b. [assistant]: {assistant_msg[:200]}...\n"

            prompt += f"""

## Candidate Status:
Exists in System: {candidate_exists}
"""

            if candidate_profile:
                prompt += f"""
Candidate Profile:
- Name: {candidate_profile.get("candidate_name")}
- Email: {candidate_profile.get("email")}
- Job Applied: {candidate_profile.get("job_title")}
- Status: {candidate_profile.get("status")}
- Score: {candidate_profile.get("total_score")}
"""

            prompt += """

## VERY STRICT CLASSIFICATION RULES — follow exactly:

1. NEW_CV_APPLICATION
   - There is a PDF or image attachment that looks like a resume/CV
   - OR the current message clearly says "attached my resume/CV" or similar
   - Usually the first message in thread or re-application
   → target_dag: "cv_analyse"

2. SCREENING_RESPONSE (most important rule — prioritize this when it matches)
   - Candidate ALREADY exists in system (candidate_exists = True)
   - NO new CV/resume attachment in CURRENT message
   - Current message contains ANSWERS or RESPONSES to numbered questions
     Examples of screening questions we ask:
       - work arrangement (remote/hybrid/full-time)
       - notice period / availability
       - salary expectations (range)
       - relocation willingness
       - why interested in role/company
       - years of ML/production experience
       - proficiency in Python/TensorFlow/PyTorch/etc.
       - NLP/computer vision/big data experience
       - education (Master's in Statistics?)
       - certifications
   - Message often starts with "Here are my answers:", "1. Yes,", "My responses:", or directly numbers 1–10
   - Usually a direct reply to our previous email in the thread
   → target_dag: "screening_response_analysis"

3. OTHER
   - Everything else: general questions, thank you notes, rejections, spam, unrelated follow-ups
   - Candidate exists but message does NOT contain answers to the above screening questions
   → target_dag: "none"

## Return JSON only:
{
  "email_type": "NEW_CV_APPLICATION | SCREENING_RESPONSE | OTHER",
  "confidence": 0-100,
  "reasoning": "...",
  "target_dag": "cv_analyse | screening_response_analysis | none"
}
"""
            logging.info(f"conversation history: {json.dumps(conversation_history)}")
            # =====================================================================
            # AI CLASSIFICATION WITH CONVERSATION HISTORY
            # =====================================================================
            logging.info(f"Classifying email {email_id} using AI model {MODEL_NAME}")
            ai_response = get_ai_response(
                prompt,
                conversation_history=conversation_history,
                stream=False,
                model=MODEL_NAME
            )
            
            classification = extract_json_from_text(ai_response)

            email_type = classification.get("email_type", "OTHER")
            confidence = classification.get("confidence", 0)
            reasoning = classification.get("reasoning", "")
            target_dag = classification.get("target_dag", "none")

            logging.info(f"Email {email_id} classified as {email_type} (confidence: {confidence}%)")
            logging.info(f"Reasoning: {reasoning}")

            # =====================================================================
            # ATTACH RESULTS
            # =====================================================================
            email["classification"] = {
                "type": email_type,
                "confidence": confidence,
                "reasoning": reasoning,
                "target_dag": target_dag,
                "candidate_exists": candidate_exists,
                "candidate_profile": candidate_profile
            }

            email["extracted_candidate_email"] = candidate_email
            email["search_email"] = search_email
            email["cv_text_preview"] = cv_text if cv_text else None
            email["thread_history"] = thread_data
            email["conversation_context"] = conversation_history

            classified_emails.append(email)

        except Exception as e:
            logging.error(
                f"Classification failed for email {email.get('id')}: {e}",
                exc_info=True
            )

            # Fallback classification
            email["classification"] = {
                "type": "NEW_CV_APPLICATION" if email.get("attachments") else "OTHER",
                "confidence": 50,
                "reasoning": f"Error during classification: {str(e)}",
                "target_dag": "cv_analyse" if email.get("attachments") else "none",
                "candidate_exists": False,
                "candidate_profile": None
            }

            classified_emails.append(email)

    ti.xcom_push(key="classified_emails", value=classified_emails)
    logging.info(f"Classified {len(classified_emails)} emails with thread context")

    return classified_emails


def check_for_emails(**kwargs):
    """
    Branch function to determine next task based on email presence.
    Returns task_id to execute next.
    """
    ti = kwargs['ti']
    classified_emails = ti.xcom_pull(task_ids='classify_email_type', key='classified_emails')
    
    if classified_emails and len(classified_emails) > 0:
        logging.info(f"Found {len(classified_emails)} emails, proceeding to route them")
        return 'route_emails_to_dags'
    else:
        logging.info("No new emails found")
        return 'no_emails_found'


def route_emails_to_dags(**kwargs):
    """
    Route emails to appropriate DAGs based on classification.
    Triggers cv_analyse for new applications and screening_response_analysis for responses.
    """
    ti = kwargs['ti']
    classified_emails = ti.xcom_pull(task_ids='classify_email_type', key='classified_emails')
    
    if not classified_emails:
        logging.info("No emails to route")
        return
    
    routing_summary = {
        'cv_analyse': 0,
        'screening_response_analysis': 0,
        'skipped': 0,
        'emails_extracted_from_cv': 0,
        'emails_with_thread_context': 0
    }
    
    for email in classified_emails:
        try:
            email_id = email.get('id', 'unknown')
            classification = email.get('classification', {})
            target_dag = classification.get('target_dag', 'none')
            email_type = classification.get('type', 'OTHER')
            sender = email.get('headers', {}).get('From', 'Unknown')
            candidate_exists = classification.get('candidate_exists', False)
            extracted_email = email.get('extracted_candidate_email')
            search_email = email.get('search_email')
            thread_history = email.get('thread_history', {})
            
            if extracted_email:
                routing_summary['emails_extracted_from_cv'] += 1
            
            if thread_history.get('history'):
                routing_summary['emails_with_thread_context'] += 1
            
            if target_dag == 'none' or email_type == 'OTHER':
                logging.info(f"Skipping email {email_id} (type: {email_type})")
                routing_summary['skipped'] += 1
                continue
            
            # Create unique task_id for each trigger
            task_id = f"trigger_{target_dag}_{email_id.replace('-', '_')}"
            
            logging.info(f"Routing email {email_id} from {sender} to DAG: {target_dag}")
            logging.info(f"Classification: {email_type} (confidence: {classification.get('confidence')}%)")
            logging.info(f"Candidate email used for search: {search_email}")
            logging.info(f"Candidate exists in system: {candidate_exists}")
            logging.info(f"Thread history messages: {len(thread_history.get('history', []))}")
            
            # Create and execute trigger operator
            trigger_task = TriggerDagRunOperator(
                task_id=task_id,
                trigger_dag_id=target_dag,
                conf={'email_data': email},
                wait_for_completion=False
            )
            
            trigger_task.execute(context=kwargs)
            routing_summary[target_dag] += 1
            
            logging.info(f"Successfully triggered {target_dag} for email {email_id}")
            
        except Exception as e:
            logging.error(f"Error routing email {email.get('id')}: {str(e)}")
            routing_summary['skipped'] += 1
            continue
    
    # Log routing summary
    logging.info("=" * 60)
    logging.info("Email Routing Summary:")
    logging.info(f"  - New CV Applications (cv_analyse): {routing_summary['cv_analyse']}")
    logging.info(f"  - Screening Responses (screening_response_analysis): {routing_summary['screening_response_analysis']}")
    logging.info(f"  - Emails extracted from CV: {routing_summary['emails_extracted_from_cv']}")
    logging.info(f"  - Emails with thread context: {routing_summary['emails_with_thread_context']}")
    logging.info(f"  - Skipped/Other: {routing_summary['skipped']}")
    logging.info("=" * 60)
    
    ti.xcom_push(key='routing_summary', value=routing_summary)
    
    return routing_summary


def log_no_emails(**kwargs):
    """
    Log message when no new emails are found.
    """
    logging.info("No new recruitment emails found in this polling cycle")
    return "No emails to process"


# ============================================================================
# DAG DEFINITION
# ============================================================================

with DAG(
    "cv_monitor_mailbox",
    default_args=default_args,
    schedule_interval=timedelta(minutes=2),  # Check every 2 minutes
    catchup=False,
    doc_md="""
    # Smart CV Mailbox Monitor DAG with Thread History & PDF Content
    
    This DAG monitors a Gmail mailbox for incoming recruitment emails and intelligently 
    routes them to the appropriate processing DAG. It extracts complete conversation
    history from email threads and integrates PDF content at the appropriate points.
    
    ## Workflow:
    1. Fetch unread emails with attachments from Gmail
    2. Extract full thread history with all previous messages
    3. Extract PDF content from attachments in each message
    4. Integrate PDF content into conversation history
    5. Extract candidate email from CV PDF using AI
    6. Use extracted email to search Google Sheets for candidate profile
    7. Classify each email using AI with full conversation context:
       - NEW_CV_APPLICATION: First-time CV submission or reapplication
       - SCREENING_RESPONSE: Response to screening questions
       - OTHER: General inquiries or follow-ups
    8. Route emails to appropriate DAGs:
       - `cv_analyse`: For new CV applications
       - `screening_response_analysis`: For screening responses
       - Skip: For other types
    
    ## Key Features:
    - **Full Thread History**: Extracts all messages from email conversation
    - **PDF Content Integration**: Attaches PDF content to appropriate messages in history
    - **AI-powered Email Extraction**: Extracts candidate email from CV text
    - **Context-Aware Classification**: Uses conversation history for better classification
    - **Smart Routing**: Routes to appropriate DAG based on email type
    
    """,
    tags=["cv", "monitor", "mailbox", "recruitment", "intelligent-routing", "google-sheets", "pdf-extraction", "email-extraction"]
) as dag:
    
    # Task 1: Fetch emails with attachments
    fetch_emails_task = PythonOperator(
        task_id='fetch_cv_emails',
        python_callable=fetch_cv_emails,
        provide_context=True
    )
    
    # Task 2: Classify emails using AI (with PDF email extraction)
    classify_emails_task = PythonOperator(
        task_id='classify_email_type',
        python_callable=classify_email_type,
        provide_context=True
    )
    
    # Task 3: Branch based on email presence
    branch_task = BranchPythonOperator(
        task_id='check_for_emails',
        python_callable=check_for_emails,
        provide_context=True
    )
    
    # Task 4a: Route emails to appropriate DAGs
    route_emails_task = PythonOperator(
        task_id='route_emails_to_dags',
        python_callable=route_emails_to_dags,
        provide_context=True
    )
    
    # Task 4b: Log no emails found
    no_emails_task = PythonOperator(
        task_id='no_emails_found',
        python_callable=log_no_emails,
        provide_context=True
    )
    
    # Set task dependencies
    fetch_emails_task >> classify_emails_task >> branch_task
    branch_task >> [route_emails_task, no_emails_task]