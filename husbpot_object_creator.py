import logging
import json
from datetime import datetime, timedelta, timezone
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
import base64
import os
import re
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from ollama import Client
from email import message_from_bytes
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from googleapiclient.errors import HttpError
from airflow.models import Variable
import time
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from hubspot_email_listener import get_email_thread
# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

default_args = {
    "owner": "lowtouch.ai_developers",
    "depends_on_past": False,
    "start_date": datetime(2025, 8, 22),
    "retry_delay": timedelta(seconds=15),
}

HUBSPOT_FROM_ADDRESS = Variable.get("ltai.v3.hubspot.from.address")
GMAIL_CREDENTIALS = Variable.get("ltai.v3.hubspot.gmail.credentials")
OLLAMA_HOST = Variable.get("ltai.v3.hubspot.ollama.host","http://agentomatic:8000")
TASK_THRESHOLD = 15
def authenticate_gmail():
    try:
        creds = Credentials.from_authorized_user_info(json.loads(GMAIL_CREDENTIALS))
        service = build("gmail", "v1", credentials=creds)
        profile = service.users().getProfile(userId="me").execute()
        logged_in_email = profile.get("emailAddress", "")
        if logged_in_email.lower() != HUBSPOT_FROM_ADDRESS.lower():
            raise ValueError(f"Wrong Gmail account! Expected {HUBSPOT_FROM_ADDRESS}, but got {logged_in_email}")
        logging.info(f"Authenticated Gmail account: {logged_in_email}")
        return service
    except Exception as e:
        logging.error(f"Failed to authenticate Gmail: {str(e)}")
        return None

def get_ai_response(prompt, conversation_history=None, expect_json=False):
    try:
        client = Client(host=OLLAMA_HOST, headers={'x-ltai-client': 'hubspot-v6af'})
        messages = []

        if expect_json:
            messages.append({
                "role": "system",
                "content": "You are a JSON-only API. Always respond with valid JSON objects. Never include explanatory text, HTML, or markdown formatting. Only return the requested JSON structure."
            })

        if conversation_history:
            for item in conversation_history:
                if "role" in item and "content" in item:
                    messages.append({"role": item["role"], "content": item["content"]})
                else:
                    messages.append({"role": "user", "content": item.get("prompt", "")})
                    messages.append({"role": "assistant", "content": item.get("response", "")})
                    
        messages.append({"role": "user", "content": prompt})
        response = client.chat(model='hubspot:v6af', messages=messages, stream=False)
        ai_content = response.message.content
        ai_content = re.sub(r'```(?:html|json)\n?|```', '', ai_content)

        if expect_json:
            return ai_content
        if not ai_content.strip().startswith('<!DOCTYPE') and not ai_content.strip().startswith('<html') and not ai_content.strip().startswith('{'):
            ai_content = f"<html><body>{ai_content}</body></html>"
        return ai_content.strip()
    except Exception as e:
        logging.error(f"Error in get_ai_response: {e}")
        raise

def parse_email_addresses(address_string):
    if not address_string:
        return []
    addresses = []
    for addr in address_string.split(','):
        addr = addr.strip()
        if addr:
            addresses.append(addr)
    return addresses

def extract_all_recipients(email_data):
    headers = email_data.get("headers", {})
    to_recipients = parse_email_addresses(headers.get("To", ""))
    cc_recipients = parse_email_addresses(headers.get("Cc", ""))
    bcc_recipients = parse_email_addresses(headers.get("Bcc", ""))
    return {
        "to": to_recipients,
        "cc": cc_recipients,
        "bcc": bcc_recipients
    }

def send_email(service, recipient, subject, body, in_reply_to, references, cc=None, bcc=None):
    try:
        msg = MIMEMultipart()
        msg["From"] = f"HubSpot via lowtouch.ai <{HUBSPOT_FROM_ADDRESS}>"
        msg["To"] = recipient
        
        if cc:
            cc_list = [email.strip() for email in cc.split(',') if email.strip().lower() != HUBSPOT_FROM_ADDRESS.lower()]
            cleaned_cc = ', '.join(cc_list)
            if cleaned_cc:
                msg["Cc"] = cleaned_cc
                logging.info(f"Including Cc in email: {cleaned_cc}")
            
        if bcc:
            bcc_list = [email.strip() for email in bcc.split(',') if email.strip().lower() != HUBSPOT_FROM_ADDRESS.lower()]
            cleaned_bcc = ', '.join(bcc_list)
            if cleaned_bcc:
                msg["Bcc"] = cleaned_bcc
                logging.info(f"Including Bcc in email: {cleaned_bcc}")
            
        msg["Subject"] = subject
        msg["In-Reply-To"] = in_reply_to
        msg["References"] = references
        msg.attach(MIMEText(body, "html"))
        
        raw_msg = base64.urlsafe_b64encode(msg.as_string().encode("utf-8")).decode("utf-8")
        result = service.users().messages().send(userId="me", body={"raw": raw_msg}).execute()
        logging.info(f"Email sent to {recipient}")
        return result
    except Exception as e:
        logging.error(f"Failed to send email: {e}")
        return None

# ============================================================================
# DAG TASK FUNCTIONS
# ============================================================================

def load_context_from_dag_run(ti, **context):
    """Load context from DAG run configuration passed by search or monitor DAG"""
    dag_run_conf = context['dag_run'].conf
    
    # Extract all necessary data
    thread_id = dag_run_conf.get("thread_id")
    search_results = dag_run_conf.get("search_results", {})
    email_data = dag_run_conf.get("email_data", {})
    chat_history = dag_run_conf.get("chat_history", [])
    thread_history = dag_run_conf.get("thread_history", [])
    latest_message = email_data.get("content", "")
    
    logging.info(f"=== LOADING CONTEXT FROM DAG RUN ===")
    logging.info(f"Thread ID: {thread_id}")
    logging.info(f"Chat history length: {len(chat_history)}")
    logging.info(f"Thread history length: {len(thread_history)}")
    logging.info(f"Search results available: {bool(search_results)}")
    
    # Push to XCom
    ti.xcom_push(key="thread_id", value=thread_id)
    ti.xcom_push(key="search_results", value=search_results)
    ti.xcom_push(key="email_data", value=email_data)
    ti.xcom_push(key="chat_history", value=chat_history)
    ti.xcom_push(key="thread_history", value=thread_history)
    ti.xcom_push(key="latest_message", value=latest_message)
    
    return {
        "thread_id": thread_id,
        "search_results": search_results,
        "email_data": email_data,
        "chat_history": chat_history,
        "thread_history": thread_history
    }

def analyze_user_response(ti, **context):
    """Analyze user's message to determine intent and entities using conversation history.
    If AI fails → send polite fallback email to ALL recipients with proper threading."""
   
    chat_history = ti.xcom_pull(key="chat_history", default=[])
    thread_history = ti.xcom_pull(key="thread_history", default=[])
    thread_id = ti.xcom_pull(key="thread_id")
    latest_user_message = ti.xcom_pull(key="latest_message", default="")
    email_data = ti.xcom_pull(key="email_data", default={})
    
    if not thread_id:
        logging.error("No thread_id provided")
        default_result = {
            "status": "error",
            "error_message": "No thread_id provided",
            "user_intent": "ERROR",
            "entities_to_create": {},
            "entities_to_update": {},
            "selected_entities": {},
            "tasks_to_execute": ["create_associations", "compose_response_html", "collect_and_save_results", "send_final_email"]
        }
        ti.xcom_push(key="analysis_results", value=default_result)
        return default_result
    
    # === CRITICAL FIX: Extract sender and recipients BEFORE try block ===
    headers = email_data.get("headers", {})
    sender_raw = headers.get("From", "")
    import email.utils
    sender_tuple = email.utils.parseaddr(sender_raw)
    sender_name = sender_tuple[0].strip() or "there"
    sender_email = sender_tuple[1].strip() or sender_raw
    
    # Extract all recipients for fallback email
    all_recipients = extract_all_recipients(email_data)
    
    # Build complete conversation context
    conversation_context = ""
    from bs4 import BeautifulSoup
    for idx, email in enumerate(thread_history, 1):
        content = email.get("content", "").strip()
        if content:
            soup = BeautifulSoup(content, "html.parser")
            clean_content = soup.get_text(separator=" ", strip=True)
            sender = email['headers'].get('From', 'Unknown')
            is_from_bot = email.get('from_bot', False)
            role_label = "BOT" if is_from_bot else "USER"
            conversation_context += f"[{role_label} EMAIL {idx} - From: {sender}]: {clean_content}\n\n"
   
    if not conversation_context.strip():
        logging.error("No valid conversation content found")
        default_result = {
            "status": "error",
            "error_message": "No valid conversation content found",
            "user_intent": "ERROR",
            "entities_to_create": {},
            "entities_to_update": {},
            "selected_entities": {},
            "tasks_to_execute": ["compose_response_html", "collect_and_save_results", "send_final_email"]
        }
        ti.xcom_push(key="analysis_results", value=default_result)
        return default_result
    
    # === Prompt (unchanged) ===
    from datetime import datetime
    prompt = f"""You are a HubSpot assistant analyzing an email conversation to understand what actions to take.
LATEST USER MESSAGE:
{latest_user_message}

SENDER INFO:
Name: {sender_name}
Email: {sender_email}

CRITICAL INSTRUCTIONS:
- You MUST extract entities ONLY from the conversation history above
- You cannot call any APIs or tools. You should answer based on your knowledge.
- The bot's previous messages contain tables with entity details (IDs, names, emails, etc.)
- Parse these tables to extract existing entities and proposed new entities
- The user's latest message indicates their intent (confirm, modify, select specific, etc.)

YOUR TASK:
Based on the conversation, and Latest User message identify:
1. **User Intent**: What does the user want to do?
   - PROCEED: User wants to proceed with operations (approve, confirm, go ahead, yes, etc.)
   - MODIFY: User wants to change something (update, change, modify, etc.)
   - EXCLUDE: User wants to skip/exclude something (skip, don't create, exclude, etc.)
   - CASUAL_COMMENT: Just making a comment/observation without requesting actions
   - CLARIFY: User has questions or needs clarification
   - CANCEL: User wants to stop/cancel the process

2. **Existing Entities** (from bot's previous confirmation emails):
   Extract IDs and details from tables showing:
   - "Existing Contact Details" → contactId, firstname, lastname, email, phone, address, jobtitle, contactOwnerName
   - "Existing Company Details" → companyId, name, domain, address, city, state, zip, country, phone, description, type
   - "Existing Deal Details" → dealId, dealName, dealLabelName, dealAmount, closeDate, dealOwnerName

3. **Entities to Create** (from bot's "Objects to be Created" tables):
   - New Contacts → firstname, lastname, email, phone, address, jobtitle, contactOwnerName
   - New Companies → name, domain, address, city, state, zip, country, phone, description, type
   - New Deals → dealName, dealLabelName, dealAmount, closeDate, dealOwnerName
   - Notes → note_content, timestamp, note_type, speaker_name, speaker_email
   - Tasks → task_details, task_owner_name, task_owner_id, due_date, priority, task_index
   - Meetings → meeting_title, start_time, end_time, location, outcome, attendees

4. **Entities to Update**:
   If user wants to modify existing entities, identify which entities and what changes.

5. **Selected Entities**:
   
   CRITICAL AUTO-INCLUSION RULE:
   - If an entity type has ONLY ONE item, ALWAYS include it automatically
   - Only apply selective inclusion for entity types with MULTIPLE items
   
   Example:
   - 1 Company, 2 Contacts, 2 Deals
   - User says "proceed with contact John and deal Q1"
   - Result: Include ALL of: Company (auto), Contact John (specified), Deal Q1 (specified)

6. **Casual Comment Handling**:
   - If the latest message is a casual comment (opinion, feedback, observation) with NO action requests:
     - Create a note with the comment text
     - Include speaker name and email from SENDER INFO
     - Use current timestamp
     - DO NOT create contacts, companies, deals, tasks, or meetings
     - **IMPORTANT: Still populate selected_entities with ALL existing entities id from the conversation history**
   - Examples of casual comments:
     * "It was great to have this deal and I think its an interesting one"
     * "This client is really engaged"
     * "Looking forward to working with them"
     * "Great progress on this deal"

GENERAL RULES:
- Default behavior: Include everything (all existing entities + all proposed new objects)
- If user mentions specific entities: Select only those entities, but still create all proposed new objects
- If user says to skip/exclude something: Remove only that item
- If user wants to modify: Identify the changes needed
- For casual comments: Create a note with the comment
- Current timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} 
Return ONLY valid JSON (no markdown, no explanations):
{{
     "casual_comments_detected": true|false,
    "selected_entities": {{
        "contacts": [{{"contactId": "...", "firstname": "...", "lastname": "...", "email": "...", "phone": "...", "address": "...", "jobtitle": "...", "contactOwnerName": "..."}}],
        "companies": [{{"companyId": "...", "name": "...", "domain": "...", "address": "...", "city": "...", "state": "...", "zip": "...", "country": "...", "phone": "...", "description": "...", "type": "..."}}],
        "deals": [{{"dealId": "...", "dealName": "...", "dealLabelName": "...", "dealAmount": "...", "closeDate": "...", "dealOwnerName": "..."}}]
    }},
    "entities_to_create": {{
        "contacts": [{{"firstname": "...", "lastname": "...", "email": "...", "phone": "...", "address": "...", "jobtitle": "...", "contactOwnerName": "..."}}],
        "companies": [{{"name": "...", "domain": "...", "address": "...", "city": "...", "state": "...", "zip": "...", "country": "...", "phone": "...", "description": "...", "type": "..."}}],
        "deals": [{{"dealName": "...", "dealLabelName": "...", "dealAmount": "...", "closeDate": "...", "dealOwnerName": "..."}}],
        "meetings": [{{"meeting_title": "...", "start_time": "...", "end_time": "...", "location": "...", "outcome": "...", "timestamp": "...", "attendees": [], "meeting_type": "...", "meeting_status": "..."}}],
        "notes": [{{"note_content": "...", "timestamp": "...", "note_type": "...", "speaker_name": "{sender_name}", "speaker_email": "{sender_email}"}}],
        "tasks": [{{"task_details": "...", "task_owner_name": "...", "task_owner_id": "...", "due_date": "...", "priority": "...", "task_index": 1}}]
    }},
    "entities_to_update": {{
        "contacts": [{{"contactId": "...", "updates": {{"field": "new_value"}}}}],
        "companies": [{{"companyId": "...", "updates": {{"field": "new_value"}}}}],
        "deals": [{{"dealId": "...", "dealName": "...", "dealLabelName": "...", "dealAmount": "...", "closeDate": "...", "dealOwnerName": "...", "updates": {{"field": "new_value"}}}}],
        "meetings": [],
        "notes": [],
        "tasks": [{{"taskId": "...", "taskbody": "...", "task_owner_name": "...", "task_owner_id": "...", "updates": {{"field": "new_value"}}}}]
    }},
    "reasoning": "Brief explanation of what you understood from the conversation and what actions you're taking"
}}

CRITICAL REMINDERS:
- Extract entities FROM conversation history tables, NOT by searching
- Parse HTML tables in bot messages to extract entity details
- For CASUAL_COMMENT intent: Create ONLY a note, no other entities
- For other intents: Default to including ALL entities if user confirms without specifics
- Always preserve entity IDs from existing entities
- Use empty arrays [] for entity types not mentioned
- Current timestamp format: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""

    try:
        response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
        logging.info(f"Raw AI response for user analysis: {response[:1000]}...")
        parsed_analysis = json.loads(response)
        user_intent = parsed_analysis.get("user_intent", "PROCEED")
        casual_comments_detected = parsed_analysis.get("casual_comments_detected", False)
        entities_to_create = parsed_analysis.get("entities_to_create", {})
        entities_to_update = parsed_analysis.get("entities_to_update", {})
        selected_entities = parsed_analysis.get("selected_entities", {})
       
        # Task determination
        tasks_to_execute = []
        should_determine_owner = False
        should_check_task_threshold = False
        
        if casual_comments_detected or user_intent == "CASUAL_COMMENT":
            if entities_to_create.get("notes"):
                tasks_to_execute.append("create_notes")
            tasks_to_execute.extend(["create_associations", "compose_response_html", "collect_and_save_results", "send_final_email"])
        else:
            if entities_to_create.get("deals") or entities_to_create.get("tasks"):
                should_determine_owner = True
                tasks_to_execute.append("determine_owner")
            if entities_to_create.get("tasks"):
                should_check_task_threshold = True
                tasks_to_execute.append("check_task_threshold")
            if entities_to_update.get("contacts"): tasks_to_execute.append("update_contacts")
            if entities_to_update.get("companies"): tasks_to_execute.append("update_companies")
            if entities_to_update.get("deals"): tasks_to_execute.append("update_deals")
            if entities_to_update.get("meetings"): tasks_to_execute.append("update_meetings")
            if entities_to_update.get("notes"): tasks_to_execute.append("update_notes")
            if entities_to_update.get("tasks"): tasks_to_execute.append("update_tasks")
            if entities_to_create.get("contacts"): tasks_to_execute.append("create_contacts")
            if entities_to_create.get("companies"): tasks_to_execute.append("create_companies")
            if entities_to_create.get("deals"): tasks_to_execute.append("create_deals")
            if entities_to_create.get("meetings"): tasks_to_execute.append("create_meetings")
            if entities_to_create.get("notes"): tasks_to_execute.append("create_notes")
            if entities_to_create.get("tasks"): tasks_to_execute.append("create_tasks")
       
        tasks_to_execute.extend(["create_associations", "compose_response_html", "collect_and_save_results", "send_final_email"])
       
        results = {
            "status": "success",
            "user_intent": user_intent,
            "entities_to_create": entities_to_create,
            "entities_to_update": entities_to_update,
            "selected_entities": selected_entities,
            "reasoning": parsed_analysis.get("reasoning", ""),
            "tasks_to_execute": tasks_to_execute,
            "should_determine_owner": should_determine_owner,
            "should_check_task_threshold": should_check_task_threshold,
            "casual_comments_detected": casual_comments_detected
        }
       
        logging.info(f"Analysis completed: Intent={user_intent}")
        logging.info(f"Tasks to execute: {tasks_to_execute}")
        logging.info(f"Reasoning: {results['reasoning']}")
        
    except Exception as ai_error:
        logging.error(f"AI failed in analyze_user_response for thread {thread_id}: {ai_error}", exc_info=True)
        logging.info("=== INITIATING FALLBACK EMAIL PROCEDURE ===")
        
        # === FALLBACK EMAIL - FULLY FIXED ===
        # Replace the fallback_body section in analyze_user_response function (around line 300)

        fallback_body = f"""
        <html>
        <head>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    line-height: 1.6;
                    color: #333;
                    max-width: 600px;
                    margin: 0 auto;
                    padding: 20px;
                }}
                .greeting {{
                    margin-bottom: 15px;
                }}
                .message {{
                    margin: 15px 0;
                }}
                .closing {{
                    margin-top: 15px;
                }}
                .signature {{
                    margin-top: 15px;
                    font-weight: bold;
                }}
                .company {{
                    color: #666;
                    font-size: 0.9em;
                }}
            </style>
        </head>
        <body>
            <div class="greeting">
                <p>Hello {sender_name},</p>
            </div>
            
            <div class="message">
                <p>We're currently experiencing a temporary technical issue that may affect your experience with the HubSpot Assistant.</p>
                
                <p>Our engineering team has already identified the cause and is actively working on a resolution. We expect regular service to resume shortly, and we'll update you as soon as it's fully restored.</p>
                
                <p>In the meantime, your data and configurations remain secure, and no action is required from your side.</p>
            </div>
            
            <div class="closing">
                <p>Thank you for your patience and understanding — we genuinely appreciate it.</p>
            </div>
            
            <div class="signature">
                <p>Best regards,<br>
                The HubSpot Assistant Team<br>
                <a href="http://lowtouch.ai" class="company">Lowtouch.ai</a></p>
            </div>
        </body>
        </html>
        """
        
        results = {
            "status": "fallback_sent",
            "error_message": f"AI analysis failed: {str(ai_error)}",
            "user_intent": "FALLBACK",
            "entities_to_create": {},
            "entities_to_update": {},
            "selected_entities": {},
            "reasoning": "Fallback email sent due to AI failure",
            "tasks_to_execute": [],  # Empty list to skip all downstream tasks
            "should_determine_owner": False,
            "should_check_task_threshold": False,
            "casual_comments_detected": False,
            "fallback_email_sent": True
        }
        
        try:
            logging.info("Attempting Gmail authentication for fallback email...")
            service = authenticate_gmail()
            if not service:
                logging.error("CRITICAL: Gmail auth failed during fallback - cannot send error notification")
            else:
                logging.info("Gmail authenticated successfully")
                
                # === Build threading ===
                original_message_id = headers.get("Message-ID", "")
                references = headers.get("References", "")
                if original_message_id:
                    references = f"{references} {original_message_id}".strip() if references else original_message_id
                
                subject = headers.get("Subject", "No Subject")
                if not subject.lower().startswith("re:"):
                    subject = f"Re: {subject}"
                
                # === Recipients: reply-all, exclude bot ===
                primary_recipient = sender_email
                
                # Build CC list from To and Cc fields, excluding sender and bot
                cc_recipients = []
                for addr in all_recipients["to"] + all_recipients["cc"]:
                    clean_addr = addr.strip()
                    if (clean_addr and 
                        clean_addr.lower() != sender_email.lower() and
                        HUBSPOT_FROM_ADDRESS.lower() not in clean_addr.lower() and
                        clean_addr not in cc_recipients):
                        cc_recipients.append(clean_addr)
                
                # BCC recipients (excluding bot)
                bcc_recipients = []
                for addr in all_recipients["bcc"]:
                    clean_addr = addr.strip()
                    if clean_addr and HUBSPOT_FROM_ADDRESS.lower() not in clean_addr.lower():
                        bcc_recipients.append(clean_addr)
                
                cc_string = ', '.join(cc_recipients) if cc_recipients else None
                bcc_string = ', '.join(bcc_recipients) if bcc_recipients else None
                
                logging.info(f"Fallback email recipients:")
                logging.info(f"  To: {primary_recipient}")
                logging.info(f"  Cc: {cc_string or 'None'}")
                logging.info(f"  Bcc: {bcc_string or 'None'}")
                
                # === Compose and send ===
                send_email(
                service=service,
                recipient=primary_recipient,
                subject=subject,
                body=fallback_body,
                in_reply_to=original_message_id,
                references=references,
                cc=cc_string,
                bcc=bcc_string
            )

                # === Mark as read ===
                try:
                    original_msg_id = email_data.get("id")
                    if original_msg_id:
                        service.users().messages().modify(
                            userId="me",
                            id=original_msg_id,
                            body={"removeLabelIds": ["UNREAD"]}
                        ).execute()
                        logging.info(f"✓ Marked original message {original_msg_id} as read")
                except Exception as read_err:
                    logging.warning(f"Failed to mark message as read: {read_err}")
                
        except Exception as send_error:
            logging.error(f"CRITICAL: Failed to send fallback email: {send_error}", exc_info=True)
            logging.error(f"Sender: {sender_email}")
            logging.error(f"Subject: {subject}")
            logging.error(f"Recipients - To: {primary_recipient}, Cc: {cc_string}, Bcc: {bcc_string}")
    
    ti.xcom_push(key="analysis_results", value=results)
    logging.info(f"Analysis completed for thread {thread_id}")
    return results

def determine_owner(ti, **context):
    """Determine deal and task owners from conversation"""
    chat_history = ti.xcom_pull(key="chat_history", default=[])
    analysis_results = ti.xcom_pull(key="analysis_results", default={})
    latest_user_message = ti.xcom_pull(key="latest_message", default="")

    
    # Get tasks to be created
    entities_to_create = analysis_results.get("entities_to_create", {})
    tasks_to_create = entities_to_create.get("tasks", [])

    prompt = f"""You are a HubSpot API assistant. Analyze this conversation to identify deal owner and task owners.

LATEST USER MESSAGE:
{latest_user_message}

Tasks to be created:
{json.dumps(tasks_to_create, indent=2)}

IMPORTANT: You must respond with ONLY a valid JSON object. No HTML, no explanations, no markdown formatting.

Steps:

1. Parse the contact Owner, Deal Owner and Task Owners from the email thread.
2. Invoke get_all_owners Tool to retrieve the list of available owners.
3. Parse and validate the deal owner against the available owners list:
    - If deal owner is NOT specified at all:
        - Default to: "Kishore"
        - Message: "No deal owner specified, so assigning to default owner Kishore."
    - If deal owner IS specified but NOT found in available owners list:
        - Default to: "Kishore"
        - Message: "The specified deal owner '[parsed_owner]' is not valid, so assigning to default owner Kishore."
    - If deal owner IS specified and IS found in available owners list:
        - Use the matched owner (with correct casing from the available owners list)
        - Message: "Deal owner specified as [matched_owner_name]"
4. Parse and validate each task owner against the available owners list:
    - Identify all tasks and their respective owners from the email content.
    - For each task in the tasks to be created:
        - If task owner is NOT specified for a task:
            - Default to: "Kishore"
            - Message: "No task owner specified for task [task_index], so assigning to default owner Kishore."
        - If task owner IS specified but NOT found in available owners list:
            - Default to: "Kishore"
            - Message: "The specified task owner '[parsed_owner]' for task [task_index] is not valid, so assigning to default owner Kishore."
        - If task owner IS specified and IS found in available owners list:
            - Use the matched owner (with correct casing from the available owners list)
            - Message: "Task owner for task [task_index] specified as [matched_owner_name]"
5. Parse and validate the contact owner against the available owners list:
    - If the deal details are not given and contact owner is also not specified:
        - Default to: "Kishore"
        - Message: "No contact owner specified, so assigning to default owner Kishore."
    - If the deal details are not given and also contact owner IS specified but NOT found in available owners list:
        - Default to: "Kishore"
        - Message: "The specified contact owner '[parsed_owner]' is not valid, so assigning to default owner Kishore."
    - If the deal details are given then contact owner is same as deal owner.
6. Return a list of task owners with their validation details for ALL tasks to be created.

Return this exact JSON structure:
{{
    "contact_owner_id": "71346067",
    "contact_owner_name": "Kishore",
    "contact_owner_message": "No contact owner specified, so assigning to default owner Kishore." OR "The specified contact owner '[parsed_owner]' is not valid, so assigning to default owner Kishore." OR "Contact owner specified as [name]",
    "deal_owner_id": "71346067",
    "deal_owner_name": "Kishore",
    "deal_owner_message": "No deal owner specified, so assigning to default owner Kishore." OR "The specified deal owner '[parsed_owner]' is not valid, so assigning to default owner Kishore." OR "Deal owner specified as [name]",
    "task_owners": [
        {{
            "task_index": 1,
            "task_owner_id": "71346067",
            "task_owner_name": "Kishore",
            "task_owner_message": "No task owner specified for task [task_index], so assigning to default owner Kishore." OR "The specified task owner '[parsed_owner]' for task [task_index] is not valid, so assigning to default owner Kishore." OR "Task owner for task [task_index] specified as [name]"
        }}
    ],
    "all_owners_table": [
        {{
            "id": "owner_id",
            "name": "owner_name",
            "email": "owner_email"
        }}
    ]
}}

RESPOND WITH ONLY THE JSON OBJECT - NO OTHER TEXT."""

    response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
    
    try:
        parsed_json = json.loads(response.strip())
        ti.xcom_push(key="owner_info", value=parsed_json)
        logging.info(f"Owner determined: {parsed_json.get('deal_owner_name')}")
    except Exception as e:
        logging.error(f"Error processing owner AI response: {e}")
        default_owner = {
            "deal_owner_id": "71346067",
            "deal_owner_name": "Kishore",
            "deal_owner_message": f"Error: {str(e)}, using default owner Kishore",
            "task_owners": [],
            "all_owners_table": []
        }
        ti.xcom_push(key="owner_info", value=default_owner)

def check_task_threshold(ti, **context):
    """Check if task volume exceeds threshold"""
    analysis_results = ti.xcom_pull(key="analysis_results", default={})
    chat_history = ti.xcom_pull(key="chat_history", default=[])
    owner_info = ti.xcom_pull(key="owner_info", default={})
    latest_user_message = ti.xcom_pull(key="latest_message", default="")
    
    entities_to_create = analysis_results.get("entities_to_create", {})
    tasks_to_create = entities_to_create.get("tasks", [])
    
    if not tasks_to_create:
        logging.info("No tasks to create, skipping threshold check")
        ti.xcom_push(key="task_warnings", value=[])
        ti.xcom_push(key="task_threshold_info", value={
            "task_threshold_results": {
                "dates_checked": [],
                "total_warnings": 0,
                "threshold_limit": TASK_THRESHOLD
            },
            "extracted_dates": [],
            "warnings": []
        })
        return []
    
    
    task_owners = owner_info.get('task_owners', [])
    
    # Map tasks to owners
    task_owner_mapping = []
    for idx, task in enumerate(tasks_to_create, 1):
        matching_owner = next((owner for owner in task_owners if owner.get('task_index') == idx), None)
        task_owner_id = matching_owner.get('task_owner_id', '71346067') if matching_owner else '71346067'
        task_owner_name = matching_owner.get('task_owner_name', 'Kishore') if matching_owner else 'Kishore'
        
        task_owner_mapping.append({
            'task_index': idx,
            'task_details': task.get('task_details', ''),
            'due_date': task.get('due_date', ''),
            'task_owner_id': task_owner_id,
            'task_owner_name': task_owner_name
        })

    prompt = f"""You are a HubSpot API assistant. Check task volume thresholds.

LATEST USER MESSAGE:
{latest_user_message}

Task Owner Mapping:
{json.dumps(task_owner_mapping, indent=2)}

IMPORTANT: You must respond with ONLY a valid JSON object. No HTML, no explanations, no markdown formatting.

Steps to follow:
1. For each task in the Task Owner Mapping, extract the due date and assigned owner.
2. For each unique owner and due date combination:
   - Invoke search_tasks with GTE and LTE set to the specified due date and owner name.
   - Count total tasks for that owner on that date.
   - Check if the task count exceeds the threshold of {TASK_THRESHOLD} tasks per day.
3. Generate warnings for dates that exceed the threshold for each owner.

Return this exact JSON structure:
{{
    "task_threshold_results": {{
        "dates_checked": [
            {{
                "date": "YYYY-MM-DD",
                "owner_id": "owner_id",
                "owner_name": "owner_name",
                "existing_task_count": 0,
                "exceeds_threshold": false,
                "warning": "High task volume: X tasks on YYYY-MM-DD for owner_name" or null
            }}
        ],
        "total_warnings": 0,
        "threshold_limit": {TASK_THRESHOLD}
    }},
    "extracted_dates": [
        "YYYY-MM-DD"
    ],
    "warnings": [
        "Warning message if threshold exceeded"
    ]
}}

Fill in ALL fields. Use empty arrays [] for no results.
For dates, use YYYY-MM-DD format.
If no dates found in email, check today's date as default for each owner.

RESPOND WITH ONLY THE JSON OBJECT - NO OTHER TEXT."""


    response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
    
    try:
        parsed_json = json.loads(response.strip())
        warnings = parsed_json.get("warnings", [])
        ti.xcom_push(key="task_warnings", value=warnings)
        ti.xcom_push(key="task_threshold_info", value=parsed_json)
        logging.info(f"Task threshold check completed with {len(warnings)} warnings")
    except Exception as e:
        logging.error(f"Error processing task threshold AI response: {e}")
        default_response = {
            "task_threshold_results": {
                "dates_checked": [],
                "total_warnings": 0,
                "threshold_limit": TASK_THRESHOLD
            },
            "extracted_dates": [],
            "warnings": []
        }
        ti.xcom_push(key="task_warnings", value=[])
        ti.xcom_push(key="task_threshold_info", value=default_response)
    
    return warnings


def create_contacts(ti, **context):
    """Create new contacts in HubSpot with full retry logic and clean error handling"""
    analysis_results = ti.xcom_pull(key="analysis_results")
    to_create_contacts = analysis_results.get("entities_to_create", {}).get("contacts", [])
    chat_history = ti.xcom_pull(key="chat_history", default=[])
    owner_info = ti.xcom_pull(key="owner_info", default={})

    # === Retry Context ===
    task_instance = context['task_instance']
    current_try = task_instance.try_number
    max_tries = task_instance.max_tries

    logging.info(f"=== CREATE CONTACTS - Attempt {current_try}/{max_tries} ===")

    previous_status = ti.xcom_pull(key="contact_creation_status")
    previous_response = ti.xcom_pull(key="contact_creation_response")
    is_retry = current_try > 1

    if not to_create_contacts:
        logging.info("No contacts to create")
        result = {
            "created_contacts": [],
            "contacts_errors": [],
            "contact_creation_status": {"status": "success"},
            "contact_creation_response": {"status": "success", "created_contacts": [], "errors": []},
            "contact_creation_final_status": "success"
        }
        for k, v in result.items():
            ti.xcom_push(key=k, value=v)
        return []

    # Inject owner info
    contact_owner_id = owner_info.get("contact_owner_id", "71346067")
    contact_owner_name = owner_info.get("contact_owner_name", "Kishore")
    for contact in to_create_contacts:
        contact.setdefault("contactOwnerName", contact_owner_name)
        contact.setdefault("contactOwnerId", contact_owner_id)

    # === Base Prompt (shared) ===
    base_prompt = f"""Create contacts in HubSpot.

Contact Details to Create:
{json.dumps(to_create_contacts, indent=2)}
Contact Owner: {contact_owner_name} (ID: {contact_owner_id})

Steps:
1. For each contact, invoke create_contact tool with the provided properties
2. Return the created contact ID and all properties

Return ONLY this JSON structure (no other text):
{{
    "status": "success|failure",
    "created_contacts": [
        {{
            "id": "contact_id_from_api",
            "details": {{
                "firstname": "value",
                "lastname": "value",
                "email": "value",
                "phone": "value",
                "address": "value",
                "jobtitle": "value",
                "contactOwnerName": "value"
            }}
        }}
    ],
    "errors": ["Error message 1", "Error message 2"],
    "reason": "error description if status is failure"
}}"""

    # === Retry Prompt (enhanced with previous failure) ===
    if is_retry:
        logging.info(f"RETRY DETECTED - Using retry prompt (attempt {current_try}/{max_tries})")

        prev_reason = previous_status.get("reason", "Unknown error") if previous_status else "No previous status"
        prev_resp_str = json.dumps(previous_response, indent=2) if previous_response else "No previous response"

        prompt = f"""PREVIOUS CONTACT CREATION FAILED

Previous AI Response:
{prev_resp_str}

Failure Reason: {prev_reason}

This is retry attempt {current_try} of {max_tries}.

Please fix the issue and correctly create the contacts.

Contacts to Create:
{json.dumps(to_create_contacts, indent=2)}

{base_prompt}

COMMON FIXES NEEDED:
- Invalid JSON (missing commas, quotes, brackets)
- Wrong field names or structure
- Missing contactOwnerId
- Not using create_contact tool
- Returning extra text

YOU MUST RETURN ONLY CLEAN, VALID JSON."""
    else:
        logging.info(f"INITIAL ATTEMPT {current_try}/{max_tries}")
        prompt = base_prompt

    response = None
    try:
        response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
        logging.info(f"Raw AI response: {response[:1000]}...")

        parsed = json.loads(response.strip())
        status = parsed.get("status")

        if status != "success":
            raise Exception(parsed.get("reason", "LLM returned status != success"))

        created_contacts = parsed.get("created_contacts", [])
        errors = parsed.get("errors", [])

        # === SUCCESS ===
        result = {
            "created_contacts": created_contacts,
            "contacts_errors": errors,
            "contact_creation_status": {"status": "success"},
            "contact_creation_response": parsed,
            "contact_creation_final_status": "success"
        }

        for k, v in result.items():
            ti.xcom_push(key=k, value=v)

        logging.info(f"SUCCESS: Created {len(created_contacts)} contacts on attempt {current_try}")
        return created_contacts

    except json.JSONDecodeError as e:
        error_msg = f"Invalid JSON from AI: {e}\nRaw: {response}"
        logging.error(error_msg)
        raise Exception(error_msg)

    except Exception as e:
        error_msg = str(e) or "Unknown error during contact creation"
        is_final = current_try >= max_tries

        status_type = "final_failure" if is_final else "failure"
        fallback = {
            "created_contacts": [],
            "contacts_errors": [error_msg],
            "contact_creation_status": {"status": status_type, "reason": error_msg},
            "contact_creation_response": {"raw_response": response} if response else None,
            "contact_creation_final_status": "failed" if is_final else "retrying"
        }

        for k, v in fallback.items():
            ti.xcom_push(key=k, value=v)

        if is_final:
            logging.error(f"FINAL FAILURE: Contact creation failed after {max_tries} attempts: {error_msg}")
            raise  # Mark task failed
        else:
            logging.warning(f"Contact creation failed → retrying ({current_try}/{max_tries})")
            raise  # Trigger retry

def create_companies(ti, **context):
    """Create new companies in HubSpot with full retry support and clean error handling"""
    analysis_results = ti.xcom_pull(key="analysis_results")
    to_create_companies = analysis_results.get("entities_to_create", {}).get("companies", [])
    chat_history = ti.xcom_pull(key="chat_history", default=[])

    # === Retry Context ===
    task_instance = context['task_instance']
    current_try = task_instance.try_number
    max_tries = task_instance.max_tries

    logging.info(f"=== CREATE COMPANIES - Attempt {current_try}/{max_tries} ===")

    previous_status = ti.xcom_pull(key="company_creation_status")
    previous_response = ti.xcom_pull(key="company_creation_response")
    is_retry = current_try > 1

    if not to_create_companies:
        logging.info("No companies to create")
        result = {
            "created_companies": [],
            "companies_errors": [],
            "company_creation_status": {"status": "success"},
            "company_creation_response": {"status": "success", "created_companies": [], "errors": []},
            "company_creation_final_status": "success"
        }
        for k, v in result.items():
            ti.xcom_push(key=k, value=v)
        return []

    # === Base Prompt (shared) ===
    base_prompt = f"""Create companies in HubSpot.

Company Details to Create:
{json.dumps(to_create_companies, indent=2)}

Steps:
1. For each company, invoke create_company tool with the provided properties
2. Return the created company ID and all properties

Return ONLY this JSON structure (no other text):
{{
    "status": "success|failure",
    "created_companies": [
        {{
            "id": "company_id_from_api",
            "details": {{
                "name": "value",
                "domain": "value",
                "address": "value",
                "city": "value",
                "state": "value",
                "zip": "value",
                "country": "value",
                "phone": "value",
                "description": "value",
                "type": "value"
            }}
        }}
    ],
    "errors": [],
    "reason": "error description if status is failure"
}}"""

    # === Retry Prompt (with previous failure context) ===
    if is_retry:
        logging.info(f"RETRY DETECTED - Using retry prompt (attempt {current_try}/{max_tries})")

        prev_reason = previous_status.get("reason", "Unknown error") if previous_status else "No previous status"
        prev_resp_str = json.dumps(previous_response, indent=2) if previous_response else "No previous response"

        prompt = f"""PREVIOUS COMPANY CREATION FAILED

Previous AI Response:
{prev_resp_str}

Failure Reason: {prev_reason}

This is retry attempt {current_try} of {max_tries}.

Please fix the issue and correctly create the companies.

Companies to Create:
{json.dumps(to_create_companies, indent=2)}

{base_prompt}

COMMON ISSUES TO FIX:
- Invalid or malformed JSON
- Missing commas, quotes, or brackets
- Wrong field names (e.g. "companyId" vs "id")
- Not using create_company tool
- Returning explanations instead of pure JSON
- Missing required fields like domain or type

YOU MUST RETURN ONLY CLEAN, VALID JSON."""
    else:
        logging.info(f"INITIAL ATTEMPT {current_try}/{max_tries}")
        prompt = base_prompt

    response = None
    try:
        response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
        logging.info(f"Raw AI response: {response[:1000]}...")

        parsed = json.loads(response.strip())
        status = parsed.get("status")

        if status != "success":
            raise Exception(parsed.get("reason", "LLM returned status != success"))

        created_companies = parsed.get("created_companies", [])
        errors = parsed.get("errors", [])

        # === SUCCESS ===
        result = {
            "created_companies": created_companies,
            "companies_errors": errors,
            "company_creation_status": {"status": "success"},
            "company_creation_response": parsed,
            "company_creation_final_status": "success"
        }

        for k, v in result.items():
            ti.xcom_push(key=k, value=v)

        logging.info(f"SUCCESS: Created {len(created_companies)} companies on attempt {current_try}")
        return created_companies

    except json.JSONDecodeError as e:
        error_msg = f"Invalid JSON from AI: {e}\nRaw: {response}"
        logging.error(error_msg)
        raise Exception(error_msg)

    except Exception as e:
        error_msg = str(e) or "Unknown error during company creation"
        is_final = current_try >= max_tries

        status_type = "final_failure" if is_final else "failure"
        fallback = {
            "created_companies": [],
            "companies_errors": [error_msg],
            "company_creation_status": {"status": status_type, "reason": error_msg},
            "company_creation_response": {"raw_response": response} if response else None,
            "company_creation_final_status": "failed" if is_final else "retrying"
        }

        for k, v in fallback.items():
            ti.xcom_push(key=k, value=v)

        if is_final:
            logging.error(f"FINAL FAILURE: Company creation failed after {max_tries} attempts: {error_msg}")
            raise  # Mark task failed in Airflow
        else:
            logging.warning(f"Company creation failed → retrying ({current_try}/{max_tries})")
            raise

def create_deals(ti, **context):
    """Create new deals in HubSpot with full retry logic and clean error handling"""
    analysis_results = ti.xcom_pull(key="analysis_results")
    to_create_deals = analysis_results.get("entities_to_create", {}).get("deals", [])
    chat_history = ti.xcom_pull(key="chat_history", default=[])
    owner_info = ti.xcom_pull(key="owner_info", default={})

    # === Retry Context ===
    task_instance = context['task_instance']
    current_try = task_instance.try_number
    max_tries = task_instance.max_tries

    logging.info(f"=== CREATE DEALS - Attempt {current_try}/{max_tries} ===")

    previous_status = ti.xcom_pull(key="deal_creation_status")
    previous_response = ti.xcom_pull(key="deal_creation_response")
    is_retry = current_try > 1

    if not to_create_deals:
        logging.info("No deals to create")
        result = {
            "created_deals": [],
            "deals_errors": [],
            "deal_creation_status": {"status": "success"},
            "deal_creation_response": {"status": "success", "created_deals": [], "errors": []},
            "deal_creation_final_status": "success"
        }
        for k, v in result.items():
            ti.xcom_push(key=k, value=v)
        return []

    # Inject owner info
    deal_owner_id = owner_info.get("deal_owner_id", "71346067")
    deal_owner_name = owner_info.get("deal_owner_name", "Kishore")
    for deal in to_create_deals:
        deal.setdefault("dealOwnerName", deal_owner_name)
        deal.setdefault("dealOwnerId", deal_owner_id)

    # === Base Prompt (shared) ===
    base_prompt = f"""Create deals in HubSpot.

Deal Details to Create:
{json.dumps(to_create_deals, indent=2)}

Deal Owner: {deal_owner_name} (ID: {deal_owner_id})

IMPORTANT: Respond with ONLY a valid JSON object. Always invoke create_deal.

Critical Deal Naming Rules:
1. Extract Client Name from latest user response or available details
2. Determine if it's a direct deal or partner deal from context
3. For direct deals: format as "<Client Name>-<Deal Name>"
4. For partner deals: format as "<Partner Name>-<Client Name>-<Deal Name>"
5. If Deal Name not specified, create descriptive name based on product/service mentioned
6. Never use generic names - must reflect actual client/partner and deal purpose
7. Preserve any specific deal amount, close date, or stage information provided
8. Never use commas for deal amount.
9. Use only the following for deal stage:
    - appointmentscheduled
    - qualifiedtobuy
    - presentationscheduled
    - decisionmakerboughtin
    - contractsent
    - closedwon
    - closedlost
10. Never use the hubspot owner name for calling the api, it should always be the id.

Steps:
1. Analyze user response to extract client/partner names and deal details
2. Apply naming convention strictly for each deal
3. For each deal, invoke create_deal with the properties
4. Collect created deal id, properly formatted deal name, label name, amount, close date, owner

Return JSON:
{{
    "status": "success|failure",
    "created_deals": [{{
        "id": "123", 
        "details": {{ 
            "dealName": "ClientName-DealPurpose", // or "PartnerName-ClientName-DealPurpose",
            "dealLabelName": "...",
            "dealAmount": "...",
            "closeDate": "...",
            "dealOwnerName": "..."
        }}
    }}],
    "errors": [],
    "reason": "error description if status is failure"
}}"""

    # === Retry Prompt (with previous failure) ===
    if is_retry:
        logging.info(f"RETRY DETECTED - Using retry prompt (attempt {current_try}/{max_tries})")

        prev_reason = previous_status.get("reason", "Unknown error") if previous_status else "No previous status"
        prev_resp_str = json.dumps(previous_response, indent=2) if previous_response else "No previous response"

        prompt = f"""PREVIOUS DEAL CREATION FAILED

Previous AI Response:
{prev_resp_str}

Failure Reason: {prev_reason}

This is retry attempt {current_try} of {max_tries}.

Please fix the issue and correctly create the deals.

Deals to Create:
{json.dumps(to_create_deals, indent=2)}

{base_prompt}

COMMON ISSUES TO FIX:
- Invalid or malformed JSON
- Using owner name instead of ID
- Wrong deal stage (using display name instead of internal ID)
- Bad deal name format (generic, missing client, commas, etc.)
- Not calling create_deal tool
- Extra text outside JSON

YOU MUST RETURN ONLY CLEAN, VALID JSON."""
    else:
        logging.info(f"INITIAL ATTEMPT {current_try}/{max_tries}")
        prompt = base_prompt

    response = None
    try:
        response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
        logging.info(f"Raw AI response: {response[:1000]}...")

        parsed = json.loads(response.strip())
        status = parsed.get("status")

        if status != "success":
            raise Exception(parsed.get("reason", "LLM returned status != success"))

        created_deals = parsed.get("created_deals", [])
        errors = parsed.get("errors", [])

        # === SUCCESS ===
        result = {
            "created_deals": created_deals,
            "deals_errors": errors,
            "deal_creation_status": {"status": "success"},
            "deal_creation_response": parsed,
            "deal_creation_final_status": "success"
        }

        for k, v in result.items():
            ti.xcom_push(key=k, value=v)

        logging.info(f"SUCCESS: Created {len(created_deals)} deals on attempt {current_try}")
        return created_deals

    except json.JSONDecodeError as e:
        error_msg = f"Invalid JSON from AI: {e}\nRaw: {response}"
        logging.error(error_msg)
        raise Exception(error_msg)

    except Exception as e:
        error_msg = str(e) or "Unknown error during deal creation"
        is_final = current_try >= max_tries

        status_type = "final_failure" if is_final else "failure"
        fallback = {
            "created_deals": [],
            "deals_errors": [error_msg],
            "deal_creation_status": {"status": status_type, "reason": error_msg},
            "deal_creation_response": {"raw_response": response} if response else None,
            "deal_creation_final_status": "failed" if is_final else "retrying"
        }

        for k, v in fallback.items():
            ti.xcom_push(key=k, value=v)

        if is_final:
            logging.error(f"FINAL FAILURE: Deal creation failed after {max_tries} attempts: {error_msg}")
            raise  # Task fails in Airflow
        else:
            logging.warning(f"Deal creation failed → retrying ({current_try}/{max_tries})")
            raise


def create_meetings(ti, **context):
    """Create meetings in HubSpot with full retry logic and clean error handling"""
    analysis_results = ti.xcom_pull(key="analysis_results")
    to_create_meetings = analysis_results.get("entities_to_create", {}).get("meetings", [])
    chat_history = ti.xcom_pull(key="chat_history", default=[])

    # === Retry Context ===
    task_instance = context['task_instance']
    current_try = task_instance.try_number
    max_tries = task_instance.max_tries

    logging.info(f"=== CREATE MEETINGS - Attempt {current_try}/{max_tries} ===")

    previous_status = ti.xcom_pull(key="meeting_creation_status")
    previous_response = ti.xcom_pull(key="meeting_creation_response")
    is_retry = current_try > 1

    if not to_create_meetings:
        logging.info("No meetings to create")
        result = {
            "created_meetings": [],
            "meetings_errors": [],
            "meeting_creation_status": {"status": "success"},
            "meeting_creation_response": {"status": "success", "created_meetings": [], "errors": []},
            "meeting_creation_final_status": "success"
        }
        for k, v in result.items():
            ti.xcom_push(key=k, value=v)
        return []

    # === Base Prompt (shared) ===
    base_prompt = f"""Create meetings in HubSpot.

Meeting Details to Create:
{json.dumps(to_create_meetings, indent=2)}

Steps:
1. For each meeting, invoke create_meeting tool with:
   - hs_meeting_title
   - hs_meeting_start_time
   - hs_meeting_end_time
   - hs_meeting_location
   - hs_meeting_outcome
   - hs_meeting_body (from outcome)
2. Return the created meeting ID and all properties

Return ONLY this JSON structure (no other text):
{{
    "status": "success|failure",
    "created_meetings": [
        {{
            "id": "meeting_id_from_api",
            "details": {{
                "meeting_title": "value",
                "start_time": "value",
                "end_time": "value",
                "location": "value",
                "outcome": "value",
                "attendees": ["name1", "name2"]
            }}
        }}
    ],
    "errors": [],
    "reason": "error description if status is failure"
}}"""

    # === Retry Prompt (with previous failure context) ===
    if is_retry:
        logging.info(f"RETRY DETECTED - Using retry prompt (attempt {current_try}/{max_tries})")

        prev_reason = previous_status.get("reason", "Unknown error") if previous_status else "No previous status"
        prev_resp_str = json.dumps(previous_response, indent=2) if previous_response else "No previous response"

        prompt = f"""PREVIOUS MEETING CREATION FAILED

Previous AI Response:
{prev_resp_str}

Failure Reason: {prev_reason}

This is retry attempt {current_try} of {max_tries}.

Please fix the issue and correctly create the meetings.

Meetings to Create:
{json.dumps(to_create_meetings, indent=2)}

{base_prompt}

COMMON ISSUES TO FIX:
- Invalid or malformed JSON
- Missing commas, quotes, or brackets
- Wrong field names (e.g., "title" instead of "meeting_title")
- Not using create_meeting tool
- Returning extra text or explanations
- Invalid datetime format

YOU MUST RETURN ONLY CLEAN, VALID JSON."""
    else:
        logging.info(f"INITIAL ATTEMPT {current_try}/{max_tries}")
        prompt = base_prompt

    response = None
    try:
        response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
        logging.info(f"Raw AI response: {response[:1000]}...")

        parsed = json.loads(response.strip())
        status = parsed.get("status")

        if status != "success":
            raise Exception(parsed.get("reason", "LLM returned status != success"))

        created_meetings = parsed.get("created_meetings", [])
        errors = parsed.get("errors", [])

        # === SUCCESS ===
        result = {
            "created_meetings": created_meetings,
            "meetings_errors": errors,
            "meeting_creation_status": {"status": "success"},
            "meeting_creation_response": parsed,
            "meeting_creation_final_status": "success"
        }

        for k, v in result.items():
            ti.xcom_push(key=k, value=v)

        logging.info(f"SUCCESS: Created {len(created_meetings)} meetings on attempt {current_try}")
        return created_meetings

    except json.JSONDecodeError as e:
        error_msg = f"Invalid JSON from AI: {e}\nRaw: {response}"
        logging.error(error_msg)
        raise Exception(error_msg)

    except Exception as e:
        error_msg = str(e) or "Unknown error during meeting creation"
        is_final = current_try >= max_tries

        status_type = "final_failure" if is_final else "failure"
        fallback = {
            "created_meetings": [],
            "meetings_errors": [error_msg],
            "meeting_creation_status": {"status": status_type, "reason": error_msg},
            "meeting_creation_response": {"raw_response": response} if response else None,
            "meeting_creation_final_status": "failed" if is_final else "retrying"
        }

        for k, v in fallback.items():
            ti.xcom_push(key=k, value=v)

        if is_final:
            logging.error(f"FINAL FAILURE: Meeting creation failed after {max_tries} attempts: {error_msg}")
            raise  # Mark task as failed in Airflow
        else:
            logging.warning(f"Meeting creation failed → retrying ({current_try}/{max_tries})")
            raise

def create_notes(ti, **context):
    """Create notes in HubSpot with full retry logic and consistent error handling"""
    analysis_results = ti.xcom_pull(key="analysis_results")
    to_create_notes = analysis_results.get("entities_to_create", {}).get("notes", [])
    chat_history = ti.xcom_pull(key="chat_history", default=[])

    # === Retry Context ===
    task_instance = context['task_instance']
    current_try = task_instance.try_number
    max_tries = task_instance.max_tries

    logging.info(f"=== CREATE NOTES - Attempt {current_try}/{max_tries} ===")

    previous_status = ti.xcom_pull(key="note_creation_status")
    previous_response = ti.xcom_pull(key="note_creation_response")
    is_retry = current_try > 1

    if not to_create_notes:
        logging.info("No notes to create")
        result = {
            "created_notes": [],
            "notes_errors": [],
            "note_creation_status": {"status": "success"},
            "note_creation_response": {"status": "success", "created_notes": [], "errors": []}
        }
        for k, v in result.items():
            ti.xcom_push(key=k, value=v)
        return []

    # Current UTC timestamp (for reference in prompt)
    current_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # === Base Prompt (shared) ===
    base_prompt = f"""You are a HubSpot Note Creation Assistant. Your role is to **create notes in HubSpot** using the provided note details.  
**You MUST invoke the `create_notes` API for every note in the input.**  
No parsing of user intent — assume all input notes are confirmed and ready to create.

---
Current UTC Time: {current_utc}
NOTES TO CREATE:
{json.dumps(to_create_notes, indent=2)}

---

**STRICT EXECUTION RULES:**

1. **For each note in `to_create_notes`:**
   - Format `note_content` as:  
     "[name] mentioned [note_content]" 
     (Use `name` from the note object if present; otherwise use `"User"`)

2. **Invoke HubSpot `create_notes` API** with:
   - `hs_timestamp`: Current UTC time in `YYYY-MM-DDTHH:MM:SSZ` format
   - `hs_note_body`: The formatted `note_content`
   - while using the `hs_note_body` for tool, convert the content to html format.
   - Required associations (if provided in input)

3. **On success per note:**
   - Capture: `id`, formatted `note_content`, `hs_lastmodifieddate`

4. **On failure per note:**
   - Capture error message in `errors` array

5. **Always return full JSON** — even if all fail.

---

**RETURN EXACTLY THIS JSON STRUCTURE:**
{{
    "status": "success|failure",
    "created_notes": [
        {{
            "id": "123",
            "details": {{
                "note_content": "[User] mentioned Follow up on Q4 budget approval",
                "timestamp": "2025-04-05T10:30:00Z"
            }}
        }}
    ],
    "errors": [],
    "reason": "error description if status is failure"
}}

**RULES:**
- `status`: "success" if all notes created successfully, "failure" otherwise
- `created_notes`: Array of successfully created notes
- `errors`: Array of strings for failed creations
- `reason`: Detailed error description if status is failure
- **Always invoke API** — no skipping
- Use **UTC** for all timestamps
- **RESPOND WITH ONLY THE JSON OBJECT — NO OTHER TEXT.**
"""

    # === Final Prompt (Initial vs Retry) ===
    if is_retry:
        logging.info(f"RETRY DETECTED - Using retry prompt (attempt {current_try}/{max_tries})")

        prev_reason = previous_status.get("reason", "Unknown error") if previous_status else "No previous status"
        prev_resp_str = json.dumps(previous_response, indent=2) if previous_response else "No previous response"

        prompt = f"""PREVIOUS NOTE CREATION FAILED

Previous AI Response:
{prev_resp_str}

Failure Reason: {prev_reason}

This is retry attempt {current_try} of {max_tries}.

Please fix the issue and correctly create ALL notes.

NOTES TO CREATE (again):
{json.dumps(to_create_notes, indent=2)}

{base_prompt}

COMMON ISSUES TO FIX:
- Invalid or malformed JSON
- Missing commas, brackets, or quotes
- Not calling create_notes tool
- Wrong field names (e.g. hs_note_body vs note_content)
- Returning extra text
- Using local time instead of UTC

YOU MUST RETURN ONLY CLEAN, VALID JSON."""
    else:
        logging.info(f"INITIAL ATTEMPT {current_try}/{max_tries}")
        prompt = base_prompt

    response = None
    try:
        response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
        logging.info(f"Raw AI response: {response[:1000]}...")

        parsed = json.loads(response.strip())
        status = parsed.get("status")

        if status != "success":
            raise Exception(parsed.get("reason", "LLM returned status != success"))

        created_notes = parsed.get("created_notes", [])
        errors = parsed.get("errors", [])

        # === SUCCESS ===
        result = {
            "created_notes": created_notes,
            "notes_errors": errors,
            "note_creation_status": {"status": "success"},
            "note_creation_response": parsed
        }

        for k, v in result.items():
            ti.xcom_push(key=k, value=v)

        logging.info(f"SUCCESS: Created {len(created_notes)} notes on attempt {current_try}")
        return created_notes

    except json.JSONDecodeError as e:
        error_msg = f"Invalid JSON from AI: {e}\nRaw: {response}"
        logging.error(error_msg)
        raise Exception(error_msg)

    except Exception as e:
        error_msg = str(e) or "Unknown error during note creation"
        is_final = current_try >= max_tries

        status_type = "final_failure" if is_final else "failure"
        fallback = {
            "created_notes": [],
            "notes_errors": [error_msg],
            "note_creation_status": {"status": status_type, "reason": error_msg},
            "note_creation_response": {"raw_response": response} if response else None
        }

        for k, v in fallback.items():
            ti.xcom_push(key=k, value=v)

        if is_final:
            logging.error(f"FINAL FAILURE: Note creation failed after {max_tries} attempts: {error_msg}")
            raise  # Task fails in Airflow
        else:
            logging.warning(f"Note creation failed → retrying ({current_try}/{max_tries})")
            raise


def create_tasks(ti, **context):
    """Create HubSpot tasks with correct owner assignment and full retry resilience"""
    analysis_results = ti.xcom_pull(key="analysis_results")
    owner_info = ti.xcom_pull(key="owner_info", default={})
    to_create_tasks = analysis_results.get("entities_to_create", {}).get("tasks", [])
    chat_history = ti.xcom_pull(key="chat_history", default=[])

    # === Retry Context ===
    task_instance = context['task_instance']
    current_try = task_instance.try_number
    max_tries = task_instance.max_tries

    logging.info(f"=== CREATE TASKS - Attempt {current_try}/{max_tries} ===")

    previous_status = ti.xcom_pull(key="task_creation_status")
    previous_response = ti.xcom_pull(key="task_creation_response")
    is_retry = current_try > 1

    if not to_create_tasks:
        logging.info("No tasks to create")
        result = {
            "created_tasks": [],
            "tasks_errors": [],
            "task_creation_status": {"status": "success"},
            "task_creation_response": {"status": "success", "created_tasks": [], "errors": []},
            "task_creation_final_status": "success"
        }
        for k, v in result.items():
            ti.xcom_push(key=k, value=v)
        return []

    # === Assign correct task owners (critical logic preserved) ===
    task_owners = owner_info.get("task_owners", [])
    for idx, task in enumerate(to_create_tasks, 1):
        matching_owner = next((o for o in task_owners if o.get("task_index") == idx), None)
        if matching_owner:
            task["task_owner_id"] = matching_owner.get("task_owner_id", "71346067")
            task["task_owner_name"] = matching_owner.get("task_owner_name", "Kishore")
        else:
            task.setdefault("task_owner_id", "71346067")
            task.setdefault("task_owner_name", "Kishore")

    logging.info(f"Tasks prepared with owners: {json.dumps(to_create_tasks, indent=2)}")

    # === Base Prompt (shared) ===
    base_prompt = f"""Create tasks in HubSpot.

Task Details to Create (with assigned owners):
{json.dumps(to_create_tasks, indent=2)}

CRITICAL INSTRUCTIONS:
1. You MUST use the EXACT task_owner_id specified for each task
2. DO NOT change or override the task_owner_id values
3. Each task already has the correct owner assigned - preserve it

Steps:
1. For each task, invoke create_task tool with:
   - hs_task_subject: The task_details field
   - hs_task_body: The task_details field
   - hubspot_owner_id: Use the EXACT task_owner_id from the task (DO NOT change this)
   - hs_task_status: "NOT_STARTED"
   - hs_task_priority: The priority field (HIGH/MEDIUM/LOW)
   - hs_timestamp: Convert due_date to milliseconds since epoch
2. Return the created task ID and properties including the ACTUAL owner name used

EXAMPLE for task with task_owner_id "159242825":
create_task({{
    "properties": {{
        "hs_task_subject": "Draft a proposal...",
        "hs_task_body": "Draft a proposal...",
        "hubspot_owner_id": "159242825",  // MUST use this exact ID
        "hs_task_status": "NOT_STARTED",
        "hs_task_priority": "MEDIUM",
        "hs_timestamp": "1729641600000"
    }}
}})

Return ONLY this JSON structure (no other text):
{{
    "status": "success|failure",
    "created_tasks": [
        {{
            "id": "task_id_from_api",
            "details": {{
                "task_details": "value",
                "task_owner_name": "actual_owner_name_from_api",
                "task_owner_id": "actual_owner_id_used",
                "due_date": "value",
                "priority": "value",
                "task_index": task_index_number
            }}
        }}
    ],
    "errors": [],
    "reason": "error description if status is failure"
}}

CRITICAL: Preserve the task_owner_id from the input. Do not default to Kishore (71346067) unless explicitly specified."""

    # === Build Final Prompt (Retry vs Initial) ===
    if is_retry:
        logging.info(f"RETRY ATTEMPT {current_try}/{max_tries} - Using enhanced retry prompt")

        prev_reason = previous_status.get("reason", "Unknown error") if previous_status else "No previous status"
        prev_resp_str = json.dumps(previous_response, indent=2) if previous_response else "No previous response"

        prompt = f"""PREVIOUS TASK CREATION FAILED

Previous AI Response:
{prev_resp_str}

Failure Reason: {prev_reason}

This is retry attempt {current_try} of {max_tries}.

You likely changed the task_owner_id or returned invalid JSON.

FIX IT NOW.

Tasks to Create (with CORRECT owners):
{json.dumps(to_create_tasks, indent=2)}

{base_prompt}

COMMON ISSUES TO FIX:
- Wrong hubspot_owner_id used
- Defaulted to Kishore when not allowed
- Invalid JSON (missing commas, brackets, quotes)
- Extra text outside JSON
- Wrong field names

YOU MUST:
- Preserve EVERY task_owner_id exactly as given
- Return ONLY clean, valid JSON
- Use create_task tool correctly
"""
    else:
        logging.info(f"INITIAL ATTEMPT {current_try}/{max_tries}")
        prompt = base_prompt

    response = None
    try:
        response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
        logging.info(f"Raw AI response: {response[:1000]}...")

        parsed = json.loads(response.strip())
        status = parsed.get("status")

        if status != "success":
            raise Exception(parsed.get("reason", "LLM returned status != success"))

        created_tasks = parsed.get("created_tasks", [])
        errors = parsed.get("errors", [])

        # Optional: Validate owner IDs were preserved
        for created in created_tasks:
            details = created.get("details", {})
            task_index = details.get("task_index")
            original = next((t for t in to_create_tasks if t.get("task_index") == task_index), None)
            if original and details.get("task_owner_id") != original.get("task_owner_id"):
                logging.warning(f"Owner ID mismatch on task {task_index}: expected {original.get('task_owner_id')}, got {details.get('task_owner_id')}")

        # === SUCCESS ===
        result = {
            "created_tasks": created_tasks,
            "tasks_errors": errors,
            "task_creation_status": {"status": "success"},
            "task_creation_response": parsed,
            "task_creation_final_status": "success"
        }

        for k, v in result.items():
            ti.xcom_push(key=k, value=v)

        logging.info(f"SUCCESS: Created {len(created_tasks)} tasks on attempt {current_try}")
        return created_tasks

    except json.JSONDecodeError as e:
        error_msg = f"Invalid JSON from AI: {e}\nRaw: {response}"
        logging.error(error_msg)
        raise Exception(error_msg)

    except Exception as e:
        error_msg = str(e) or "Unknown error during task creation"
        is_final = current_try >= max_tries

        status_type = "final_failure" if is_final else "failure"
        fallback = {
            "created_tasks": [],
            "tasks_errors": [error_msg],
            "task_creation_status": {"status": status_type, "reason": error_msg},
            "task_creation_response": {"raw_response": response} if response else None,
            "task_creation_final_status": "failed" if is_final else "retrying"
        }

        for k, v in fallback.items():
            ti.xcom_push(key=k, value=v)

        if is_final:
            logging.error(f"FINAL FAILURE: Task creation failed after {max_tries} attempts: {error_msg}")
            raise
        else:
            logging.warning(f"Task creation failed → retrying ({current_try}/{max_tries})")
            raise
# UPDATE FUNCTIONS (abbreviated - follow same pattern)
import json
import logging

def update_contacts(ti, **context):
    """Update existing contacts in HubSpot with full retry support"""
    analysis_results = ti.xcom_pull(key="analysis_results")
    to_update = analysis_results.get("entities_to_update", {}).get("contacts", [])
    chat_history = ti.xcom_pull(key="chat_history", default=[])

    # === Retry Context ===
    task_instance = context['task_instance']
    current_try = task_instance.try_number
    max_tries = task_instance.max_tries

    logging.info(f"=== UPDATE CONTACTS - Attempt {current_try}/{max_tries} ===")

    previous_status = ti.xcom_pull(key="contact_update_status")
    previous_response = ti.xcom_pull(key="contact_update_response")
    is_retry = current_try > 1

    if not to_update:
        logging.info("No contacts to update")
        result = {
            "updated_contacts": [],
            "contacts_update_errors": [],
            "contact_update_status": {"status": "success"},
            "contact_update_response": {"status": "success", "updated_contacts": [], "errors": []},
            "contact_update_final_status": "success"
        }
        for k, v in result.items():
            ti.xcom_push(key=k, value=v)
        return []

    # === Base Prompt ===
    base_prompt = f"""Update contacts in HubSpot.

Contacts to Update:
{json.dumps(to_update, indent=2)}

Steps:
1. For each contact, invoke update_contact with the id and changes
2. Collect the updated IDs and details

Return ONLY this JSON structure (no other text):
{{
    "status": "success|failure",
    "updated_contacts": [
        {{
            "id": "123",
            "details": {{
                "firstname": "...",
                "lastname": "...",
                "email": "...",
                "phone": "...",
                "address": "...",
                "jobtitle": "..."
            }}
        }}
    ],
    "errors": [],
    "reason": "error description if status is failure"
}}"""

    # === Retry Prompt (with full context) ===
    if is_retry:
        logging.info(f"RETRY DETECTED - Using enhanced retry prompt (attempt {current_try}/{max_tries})")

        prev_reason = previous_status.get("reason", "Unknown error") if previous_status else "No previous status"
        prev_resp_str = json.dumps(previous_response, indent=2) if previous_response else "No previous response"

        prompt = f"""PREVIOUS CONTACT UPDATE FAILED

Previous AI Response:
{prev_resp_str}

Failure Reason: {prev_reason}

This is retry attempt {current_try} of {max_tries}.

Please fix the issue and correctly update the contacts.

Contacts to Update:
{json.dumps(to_update, indent=2)}

{base_prompt}

COMMON ISSUES TO FIX:
- Invalid or malformed JSON
- Wrong field names or structure
- Using wrong contact ID
- Modifying fields not in the update list
- Returning explanations instead of pure JSON
- Missing commas, quotes, or brackets

YOU MUST RETURN ONLY CLEAN, VALID JSON."""
    else:
        logging.info(f"INITIAL ATTEMPT {current_try}/{max_tries}")
        prompt = base_prompt

    response = None
    try:
        response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
        logging.info(f"Raw AI response: {response[:1000]}...")

        parsed = json.loads(response.strip())
        status = parsed.get("status")

        if status != "success":
            raise Exception(parsed.get("reason", "LLM returned status != success"))

        updated_contacts = parsed.get("updated_contacts", [])
        errors = parsed.get("errors", [])

        # === SUCCESS ===
        result = {
            "updated_contacts": updated_contacts,
            "contacts_update_errors": errors,
            "contact_update_status": {"status": "success"},
            "contact_update_response": parsed,
            "contact_update_final_status": "success"
        }

        for k, v in result.items():
            ti.xcom_push(key=k, value=v)

        logging.info(f"SUCCESS: Updated {len(updated_contacts)} contacts on attempt {current_try}")
        return updated_contacts

    except json.JSONDecodeError as e:
        error_msg = f"Invalid JSON from AI: {e}\nRaw: {response}"
        logging.error(error_msg)
        raise Exception(error_msg)

    except Exception as e:
        error_msg = str(e) or "Unknown error during contact update"
        is_final = current_try >= max_tries

        status_type = "final_failure" if is_final else "failure"
        fallback = {
            "updated_contacts": [],
            "contacts_update_errors": [error_msg],
            "contact_update_status": {"status": status_type, "reason": error_msg},
            "contact_update_response": {"raw_response": response} if response else None,
            "contact_update_final_status": "failed" if is_final else "retrying"
        }

        for k, v in fallback.items():
            ti.xcom_push(key=k, value=v)

        if is_final:
            logging.error(f"FINAL FAILURE: Contact update failed after {max_tries} attempts: {error_msg}")
            raise  # Task fails in Airflow
        else:
            logging.warning(f"Contact update failed → retrying ({current_try}/{max_tries})")
            raise

def update_companies(ti, **context):
    """Update existing companies in HubSpot with full retry support"""
    analysis_results = ti.xcom_pull(key="analysis_results")
    to_update = analysis_results.get("entities_to_update", {}).get("companies", [])
    chat_history = ti.xcom_pull(key="chat_history", default=[])

    # === Retry Context ===
    task_instance = context['task_instance']
    current_try = task_instance.try_number
    max_tries = task_instance.max_tries

    logging.info(f"=== UPDATE COMPANIES - Attempt {current_try}/{max_tries} ===")

    previous_status = ti.xcom_pull(key="company_update_status")
    previous_response = ti.xcom_pull(key="company_update_response")
    is_retry = current_try > 1

    if not to_update:
        logging.info("No companies to update")
        result = {
            "updated_companies": [],
            "companies_update_errors": [],
            "company_update_status": {"status": "success"},
            "company_update_response": {"status": "success", "updated_companies": [], "errors": []}
        }
        for k, v in result.items():
            ti.xcom_push(key=k, value=v)
        return []

    # === Base Prompt (shared) ===
    base_prompt = f"""Update the following companies in HubSpot.

Companies to Update:
{json.dumps(to_update, indent=2)}

For each company:
- Invoke update_company(company_id, {{ "properties": {{ ... }} }})
- Only include fields that need to be changed
- Use exact property names as expected by HubSpot API

Return ONLY this exact JSON structure:
{{
    "status": "success",
    "updated_companies": [
        {{
            "id": "company_id",
            "details": {{
                "name": "...",
                "domain": "...",
                "address": "...",
                "city": "...",
                "state": "...",
                "zip": "...",
                "country": "...",
                "phone": "...",
                "description": "...",
                "type": "PROSPECT|PARTNER"
            }}
        }}
    ],
    "errors": [],
    "reason": ""
}}

If any error occurs → set "status": "failure" and explain in "reason".
"""

    # === Retry Prompt (with previous failure context) ===
    if is_retry:
        logging.info(f"RETRY DETECTED - Using retry prompt (attempt {current_try}/{max_tries})")

        prev_reason = previous_status.get("reason", "Unknown error") if previous_status else "No previous status"
        prev_resp_str = json.dumps(previous_response, indent=2) if previous_response else "No previous response"

        prompt = f"""PREVIOUS COMPANY UPDATE FAILED

Previous AI Response:
{prev_resp_str}

Failure Reason: {prev_reason}

This is retry attempt {current_try} of {max_tries}.

Please fix the issue and correctly update the companies.

Companies to Update:
{json.dumps(to_update, indent=2)}

{base_prompt}

COMMON ISSUES TO FIX:
- Invalid or malformed JSON
- Wrong field names (e.g. 'companyName' instead of 'name')
- Missing company ID in update_company call
- Returning extra text or markdown
- Not using update_company tool properly

YOU MUST RETURN ONLY CLEAN, VALID JSON."""
    else:
        logging.info(f"INITIAL ATTEMPT {current_try}/{max_tries}")
        prompt = base_prompt

    response = None
    try:
        response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
        logging.info(f"Raw AI response: {response[:1000]}...")

        parsed = json.loads(response.strip())
        status = parsed.get("status")

        if status != "success":
            raise Exception(parsed.get("reason", "LLM returned status != success"))

        updated_companies = parsed.get("updated_companies", [])
        errors = parsed.get("errors", [])

        # === SUCCESS ===
        result = {
            "updated_companies": updated_companies,
            "companies_update_errors": errors,
            "company_update_status": {"status": "success"},
            "company_update_response": parsed
        }

        for k, v in result.items():
            ti.xcom_push(key=k, value=v)

        logging.info(f"SUCCESS: Updated {len(updated_companies)} companies on attempt {current_try}")
        return updated_companies

    except json.JSONDecodeError as e:
        error_msg = f"Invalid JSON from AI: {e}\nRaw: {response}"
        logging.error(error_msg)
        raise Exception(error_msg)

    except Exception as e:
        error_msg = str(e) or "Unknown error during company update"
        is_final = current_try >= max_tries

        status_type = "final_failure" if is_final else "failure"
        fallback = {
            "updated_companies": [],
            "companies_update_errors": [error_msg],
            "company_update_status": {"status": status_type, "reason": error_msg},
            "company_update_response": {"raw_response": response} if response else None
        }

        for k, v in fallback.items():
            ti.xcom_push(key=k, value=v)

        if is_final:
            logging.error(f"FINAL FAILURE: Company update failed after {max_tries} attempts: {error_msg}")
            raise  # Task fails in Airflow
        else:
            logging.warning(f"Company update failed → retrying (attempt {current_try}/{max_tries})")
            raise

def update_deals(ti, **context):
    analysis_results = ti.xcom_pull(key="analysis_results")
    to_update = analysis_results.get("entities_to_update", {}).get("deals", [])
    chat_history = ti.xcom_pull(key="chat_history", default=[])
    # Get current attempt number
    task_instance = context['task_instance']
    current_try_number = task_instance.try_number
    max_tries = task_instance.max_tries
    
    logging.info(f"=== UPDATE DEALS - Attempt {current_try_number}/{max_tries} ===")
    
    # Check if this is a retry by pulling previous status
    previous_status = ti.xcom_pull(key="deal_update_status")
    previous_response = ti.xcom_pull(key="deal_update_response")
    
    if not to_update:
        logging.info("No deals to update")
        ti.xcom_push(key="updated_deals", value=[])
        ti.xcom_push(key="deals_update_errors", value=[])
        return []
    
    # Determine if this is a retry (attempt > 1)
    is_retry = current_try_number > 1
    
    if is_retry:
        # This is a retry - use retry prompt
        logging.info(f"RETRY DETECTED - Using retry prompt (attempt {current_try_number}/{max_tries})")
        
        if previous_status is None:
            previous_reason = "Unknown error (no previous status found)"
        else:
            previous_reason = previous_status.get("reason", "Unknown error")
        
        if previous_response is None:
            previous_response_str = "No previous response available"
        else:
            previous_response_str = json.dumps(previous_response, indent=2)
        
        prompt = f"""Previous attempt to update deals failed.

Previous Response:
{previous_response_str}

Previous Failure Reason: {previous_reason}

This is retry attempt {current_try_number} of {max_tries}.

Please analyze the error and retry updating the deals:

Deals to Update:
{json.dumps(to_update, indent=2)}

IMPORTANT: Respond with ONLY a valid JSON object.

Steps:
1. Review the previous error and identify the root cause
2. For each deal, invoke update_deal with the id and changes in the exact format:
    update_deal(deal_id, {{
     "properties": {{
       "dealName": "",
       "dealLabelName": "",
       "dealAmount": "",
       "closeDate": "",
       "dealOwnerName": ""
     }}
   }})
3. HubSpot Deal Stage Label Configuration

When updating or creating a deal in HubSpot via API (or automation), always use the following **internal IDs** as the `dealLabelName` (or equivalent field) in the request payload.  

However, when displaying the stage to users (in UI, reports, emails, dashboards, etc.), always show the corresponding **human-readable Display Name**.

| Internal ID (use in API request body) | Display Name (show to users)       |
|---------------------------------------|------------------------------------|
| appointmentscheduled                  | Appointment Scheduled              |
| qualifiedtobuy                        | Qualified To Buy                   |
| presentationscheduled                 | Presentation Scheduled             |
| decisionmakerboughtin                 | Decision Maker Bought In           |
| contractsent                          | Contract Sent                      |
| closedwon                             | Closed Won                         |
| closedlost                            | Closed Lost                        |

**Important rules:**
- The value sent in the API request **must** be the exact Internal ID (lowercase, no spaces).
- Never send the Display Name in the request body — it will cause errors or mismatches.
- Always map and display the user-friendly Display Name in any front-end interface, notifications, or reporting tools.

Example API payload snippet:
```json
{{
  "dealLabelName": "appointmentscheduled"   // correct
  // "dealLabelName": "Appointment Scheduled"  // incorrect – will fail
}}
4. Use all the properties exactly as provided in the input for updating the deal.
5. Use the deal owner id for updating the deal owner instead of name.
6. Collect the updated deal id, deal name, deal label name, close date, deal owner name in tabular format. If any details not found, show as blank in table.
7. While returning the dealLabelName should be the deal stage name instead of ID. for e.g, if contractsent then return Contract Sent.
Return JSON:
{{
    "status": "success|failure",
    "updated_deals": [{{"id": "123", "details": {{ "dealName": "...", "dealLabelName": "...", "dealAmount": "...", "closeDate": "...", "dealOwnerName": "..."}}}} ...],
    "errors": ["Error message 1", "Error message 2"],
    "reason": "error description if status is failure"
}}

If error, set status as failure, error message in reason and include individual errors in the errors array."""
    else:
        # This is the initial attempt - use initial prompt
        logging.info(f"INITIAL ATTEMPT - Using initial prompt (attempt {current_try_number}/{max_tries})")
        
        prompt = f"""Update deals: {json.dumps(to_update, indent=2)}
IMPORTANT: Respond with ONLY a valid JSON object.

Steps:
1. For each deal, invoke update_deal with the id and changes in the exact format:
    update_deal(deal_id, {{
     "properties": {{
       "dealName": "",
       "dealLabelName": "",
       "dealAmount": "",
       "closeDate": "",
       "dealOwnerName": ""
     }}
   }})
2. Deal owner Id should be used in above request body instead of name.
3. HubSpot Deal Stage Label Configuration

When updating or creating a deal in HubSpot via API (or automation), always use the following **internal IDs** as the `dealLabelName` (or equivalent field) in the request payload.  

However, when displaying the stage to users (in UI, reports, emails, dashboards, etc.), always show the corresponding **human-readable Display Name**.

| Internal ID (use in API request body) | Display Name (show to users)       |
|---------------------------------------|------------------------------------|
| appointmentscheduled                  | Appointment Scheduled              |
| qualifiedtobuy                        | Qualified To Buy                   |
| presentationscheduled                 | Presentation Scheduled             |
| decisionmakerboughtin                 | Decision Maker Bought In           |
| contractsent                          | Contract Sent                      |
| closedwon                             | Closed Won                         |
| closedlost                            | Closed Lost                        |

**Important rules:**
- The value sent in the API request **must** be the exact Internal ID (lowercase, no spaces).
- Never send the Display Name in the request body — it will cause errors or mismatches.
- Always map and display the user-friendly Display Name in any front-end interface, notifications, or reporting tools.

Example API payload snippet:
```json
{{
  "dealLabelName": "appointmentscheduled"   // correct
  // "dealLabelName": "Appointment Scheduled"  // incorrect – will fail
}}
4. Use all the properties exactly as provided in the input for updating the deal.
5. Use the deal owner id for updating the deal owner instead of name.
6. Collect the updated deal id, deal name, deal label name, close date, deal owner name in tabular format. If any details not found, show as blank in table.
7. While returning the dealLabelName should be the deal stage name instead of ID. for e.g, if contractsent then return Contract Sent.
Return JSON:
{{
    "status": "success|failure",
    "updated_deals": [{{"id": "123", "details": {{ "dealName": "...", "dealLabelName": "...", "dealAmount": "...", "closeDate": "...", "dealOwnerName": "..."}}}} ...],
    "errors": ["Error message 1", "Error message 2"],
    "reason": "error description if status is failure"
}}

If error, set status as failure, error message in reason and include individual errors in the errors array."""

    response = None
    try:
        response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
        parsed = json.loads(response)       
        status = parsed.get("status", "unknown")
        updated_deals = parsed.get("updated_deals", [])
        errors = parsed.get("errors", [])
        reason = parsed.get("reason", "")

        # Prepare final result dict (will be pushed all at once)
        result = {
            "updated_deals": updated_deals if status == "success" else [],
            "deals_update_errors": errors,
            "deal_update_status": {"status": "success", "reason": reason},
            "deal_update_response": parsed
        }

        if status == "success":
            logging.info(f"Successfully updated {len(updated_deals)} deals on attempt {current_try_number}")
            # Push all at once
            for key, value in result.items():
                ti.xcom_push(key=key, value=value)
            return updated_deals

        else:
            # LLM reported failure
            raise Exception(reason or "LLM returned status='failure'")
    except json.JSONDecodeError as e:
        error_msg = f"Invalid JSON from AI: {e}\nRaw response: {response}"
        logging.error(error_msg)
        raise Exception(error_msg)   
    except Exception as e:
        error_msg = str(e) if str(e) else "Unknown error during deal update"
        is_final_attempt = current_try_number >= max_tries
        
        status_type = "final_failure" if is_final_attempt else "failure"
        result = {
            "updated_deals": [],
            "deals_update_errors": [error_msg],
            "deal_update_status": {"status": status_type, "reason": error_msg},
            "deal_update_response": {"raw_response": response} if 'response' in locals() else None
        }

        # Push everything once
        for key, value in result.items():
            ti.xcom_push(key=key, value=value)

        if is_final_attempt:
            logging.error(f"FINAL FAILURE after {max_tries} attempts: {error_msg}")
            raise  # This will mark task as failed
        else:
            logging.warning(f"Attempt {current_try_number}/{max_tries} failed → retrying...")
            raise

def update_meetings(ti, **context):
    """Update meetings in HubSpot with full retry support"""
    analysis_results = ti.xcom_pull(key="analysis_results")
    to_update = analysis_results.get("entities_to_update", {}).get("meetings", [])
    chat_history = ti.xcom_pull(key="chat_history", default=[])

    # === Retry Context ===
    task_instance = context['task_instance']
    current_try = task_instance.try_number
    max_tries = task_instance.max_tries

    logging.info(f"=== UPDATE MEETINGS - Attempt {current_try}/{max_tries} ===")

    previous_status = ti.xcom_pull(key="meeting_update_status")
    previous_response = ti.xcom_pull(key="meeting_update_response")
    is_retry = current_try > 1

    if not to_update:
        logging.info("No meetings to update")
        result = {
            "updated_meetings": [],
            "meeting_update_status": {"status": "success"},
            "meeting_update_response": {"status": "success", "updated_meetings": []}
        }
        for k, v in result.items():
            ti.xcom_push(key=k, value=v)
        return []

    # === Base Prompt ===
    base_prompt = f"""Update the following meetings in HubSpot.

Meetings to Update:
{json.dumps(to_update, indent=2)}

Rules:
- Always use `hs_timestamp` in ISO format: YYYY-MM-DDTHH:MM:SSZ (UTC)
- Convert any EST/EDT times to UTC before updating
- Use update_meeting(meeting_id, {{ "properties": {{ ... }} }})

Return ONLY this exact JSON:
{{
    "status": "success",
    "updated_meetings": [
        {{
            "id": "123",
            "details": {{
                "meeting_title": "...",
                "start_time": "2025-04-15T14:00:00Z",
                "end_time": "2025-04-15T15:00:00Z",
                "location": "...",
                "outcome": "...",
                "timestamp": "2025-04-15T14:00:00Z",
                "attendees": ["contact123", "contact456"],
                "meeting_type": "discovery_call|demo|follow_up"
            }}
        }}
    ],
    "errors": [],
    "reason": ""
}}
"""

    if is_retry:
        logging.info(f"RETRY → Using enhanced prompt (attempt {current_try}/{max_tries})")
        prev_reason = previous_status.get("reason", "Unknown error") if previous_status else "No status"
        prev_resp = json.dumps(previous_response, indent=2) if previous_response else "None"

        prompt = f"""PREVIOUS MEETING UPDATE FAILED

Previous Response:
{prev_resp}

Failure Reason: {prev_reason}

Retry #{current_try} of {max_tries} — Fix the issue and return valid JSON.

Meetings to Update:
{json.dumps(to_update, indent=2)}

{base_prompt}

FIX THESE COMMON ISSUES:
- Invalid JSON (missing commas, wrong quotes)
- Wrong timestamp format (must be ISO UTC with Z)
- Using display time instead of hs_timestamp
- Missing id in update_meeting call
- Returning text outside JSON

RETURN ONLY CLEAN JSON."""
    else:
        logging.info(f"Initial attempt {current_try}/{max_tries}")
        prompt = base_prompt

    response = None
    try:
        response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
        logging.info(f"Raw AI response: {response[:1000]}...")

        parsed = json.loads(response.strip())
        if parsed.get("status") != "success":
            raise Exception(parsed.get("reason", "Status != success"))

        updated = parsed.get("updated_meetings", [])

        result = {
            "updated_meetings": updated,
            "meeting_update_status": {"status": "success"},
            "meeting_update_response": parsed
        }
        for k, v in result.items():
            ti.xcom_push(key=k, value=v)

        logging.info(f"SUCCESS: Updated {len(updated)} meetings on attempt {current_try}")
        return updated

    except json.JSONDecodeError as e:
        error_msg = f"Invalid JSON: {e}\nRaw: {response}"
        logging.error(error_msg)
        raise Exception(error_msg)

    except Exception as e:
        error_msg = str(e) or "Unknown error"
        is_final = current_try >= max_tries
        status_type = "final_failure" if is_final else "failure"

        fallback = {
            "updated_meetings": [],
            "meeting_update_status": {"status": status_type, "reason": error_msg},
            "meeting_update_response": {"raw_response": response} if response else None
        }
        for k, v in fallback.items():
            ti.xcom_push(key=k, value=v)

        if is_final:
            logging.error(f"FINAL FAILURE: update_meetings failed after {max_tries} attempts")
            raise
        else:
            logging.warning(f"update_meetings failed → retrying ({current_try}/{max_tries})")
            raise
def update_notes(ti, **context):
    """Update notes in HubSpot with full retry support"""
    analysis_results = ti.xcom_pull(key="analysis_results")
    to_update = analysis_results.get("entities_to_update", {}).get("notes", [])
    chat_history = ti.xcom_pull(key="chat_history", default=[])

    task_instance = context['task_instance']
    current_try = task_instance.try_number
    max_tries = task_instance.max_tries

    logging.info(f"=== UPDATE NOTES - Attempt {current_try}/{max_tries} ===")

    previous_status = ti.xcom_pull(key="note_update_status")
    previous_response = ti.xcom_pull(key="note_update_response")
    is_retry = current_try > 1

    if not to_update:
        logging.info("No notes to update")
        result = {
            "updated_notes": [],
            "note_update_status": {"status": "success"},
            "note_update_response": {"status": "success", "updated_notes": []}
        }
        for k, v in result.items():
            ti.xcom_push(key=k, value=v)
        return []

    base_prompt = f"""Update the following notes in HubSpot.

Notes to Update:
{json.dumps(to_update, indent=2)}

Rules:
- Use update_note(note_id, {{ "properties": {{ "hs_note_body": "..." }} }})
- Always set `hs_timestamp` to current UTC time in format: YYYY-MM-DDTHH:MM:SSZ

Return ONLY this JSON:
{{
    "status": "success",
    "updated_notes": [
        {{
            "id": "123",
            "details": {{
                "note_content": "Full updated note body...",
                "timestamp": "2025-04-15T10:30:00Z"
            }}
        }}
    ],
    "errors": [],
    "reason": ""
}}
"""

    if is_retry:
        logging.info(f"RETRY → Using enhanced prompt (attempt {current_try}/{max_tries})")
        prev_reason = previous_status.get("reason", "Unknown") if previous_status else "No status"
        prev_resp = json.dumps(previous_response, indent=2) if previous_response else "None"

        prompt = f"""PREVIOUS NOTE UPDATE FAILED

Previous Response:
{prev_resp}

Failure Reason: {prev_reason}

Retry #{current_try} of {max_tries}

Notes to Update:
{json.dumps(to_update, indent=2)}

{base_prompt}

YOU MUST FIX:
- Malformed JSON
- Missing hs_timestamp
- Wrong field names
- Extra text outside JSON
- Not calling update_note tool

RETURN ONLY VALID JSON."""
    else:
        logging.info(f"Initial attempt {current_try}/{max_tries}")
        prompt = base_prompt

    response = None
    try:
        response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
        logging.info(f"Raw AI response: {response[:1000]}...")

        parsed = json.loads(response.strip())
        if parsed.get("status") != "success":
            raise Exception(parsed.get("reason", "Status != success"))

        updated = parsed.get("updated_notes", [])

        result = {
            "updated_notes": updated,
            "note_update_status": {"status": "success"},
            "note_update_response": parsed
        }
        for k, v in result.items():
            ti.xcom_push(key=k, value=v)

        logging.info(f"SUCCESS: Updated {len(updated)} notes on attempt {current_try}")
        return updated

    except json.JSONDecodeError as e:
        error_msg = f"Invalid JSON: {e}\nRaw: {response}"
        logging.error(error_msg)
        raise Exception(error_msg)

    except Exception as e:
        error_msg = str(e) or "Unknown error"
        is_final = current_try >= max_tries
        status_type = "final_failure" if is_final else "failure"

        fallback = {
            "updated_notes": [],
            "note_update_status": {"status": status_type, "reason": error_msg},
            "note_update_response": {"raw_response": response} if response else None
        }
        for k, v in fallback.items():
            ti.xcom_push(key=k, value=v)

        if is_final:
            logging.error(f"FINAL FAILURE: update_notes failed after {max_tries} attempts")
            raise
        else:
            logging.warning(f"update_notes failed → retrying ({current_try}/{max_tries})")
            raise

def update_tasks(ti, **context):
    """Update HubSpot tasks with full retry support and owner preservation"""
    analysis_results = ti.xcom_pull(key="analysis_results")
    owner_info = ti.xcom_pull(key="owner_info", default={})
    to_update = analysis_results.get("entities_to_update", {}).get("tasks", [])
    chat_history = ti.xcom_pull(key="chat_history", default=[])

    # === Retry Context ===
    task_instance = context['task_instance']
    current_try = task_instance.try_number
    max_tries = task_instance.max_tries

    logging.info(f"=== UPDATE TASKS - Attempt {current_try}/{max_tries} ===")

    previous_status = ti.xcom_pull(key="task_update_status")
    previous_response = ti.xcom_pull(key="task_update_response")
    is_retry = current_try > 1

    if not to_update:
        logging.info("No tasks to update")
        result = {
            "updated_tasks": [],
            "task_update_status": {"status": "success"},
            "task_update_response": {"status": "success", "updated_tasks": []}
        }
        for k, v in result.items():
            ti.xcom_push(key=k, value=v)
        return []

    # === Build enriched task data with corrected owners ===
    selected_entities = analysis_results.get("selected_entities", {})
    current_tasks = selected_entities.get("tasks", [])
    task_owners = owner_info.get("task_owners", [])

    task_details_map = {}
    for task in current_tasks:
        task_id = task.get("taskId") or task.get("id")
        if task_id:
            task_details_map[task_id] = task

    # Enrich each task update with correct owner (from owner_info override)
    for task_update in to_update:
        task_id = task_update.get("taskId") or task_update.get("id")
        if not task_id or task_id not in task_details_map:
            continue

        original = task_details_map[task_id]
        task_index = original.get("task_index")

        # Prefer corrected owner from owner_info
        if task_index is not None:
            matched_owner = next((o for o in task_owners if o.get("task_index") == task_index), None)
            if matched_owner:
                task_update["task_owner_id"] = matched_owner.get("task_owner_id", "71346067")
                task_update["task_owner_name"] = matched_owner.get("task_owner_name", "Kishore")
            else:
                task_update["task_owner_id"] = original.get("task_owner_id", "71346067")
                task_update["task_owner_name"] = original.get("task_owner_name", "Kishore")
        else:
            task_update["task_owner_id"] = original.get("task_owner_id", "71346067")
            task_update["task_owner_name"] = original.get("task_owner_name", "Kishore")

    # === Base Prompt (shared) ===
    base_prompt = f"""Update tasks in HubSpot.

Tasks to update: {json.dumps(to_update, indent=2)}
Current task details: {json.dumps(task_details_map, indent=2)}

CRITICAL INSTRUCTIONS:
1. For each task update, call update_task with this EXACT format:
   
   update_task(task_id, {{
     "properties": {{
       "hs_timestamp": "",
       "hs_task_body": "",
       "hs_task_subject": "", 
       "hs_task_priority": "",
       "hs_task_status": "",
       "hubspot_owner_id": ""
     }}
   }})

2. PRESERVE original task descriptions from task_details_map
3. Convert due_date changes to hs_timestamp format
4. Use the task's existing owner ID from the original task details
5. Use the task's existing owner id and invoke get_all_owners to get the owners name
6. After each update, call search_tasks(task_id) to get updated details

EXAMPLE for task 197051476705:
- Current details: "Draft a one-page pilot outline for shipment tracking..."  
- If updating due_date to 2025-09-30, call:
  
  update_task("197051476705", {{
    "properties": {{
      "hs_timestamp": "2025-09-30T00:00:00Z",
      "hs_task_body": "Draft a one-page pilot outline for shipment tracking and cost reporting improvements tailored to BlueHorizon.",
      "hs_task_subject": "Draft a one-page pilot outline for shipment tracking and cost reporting improvements tailored to BlueHorizon.",
      "hs_task_priority": "HIGH", 
      "hs_task_status": "NOT_STARTED",
      "hubspot_owner_id": "159242778"
    }}
  }})

Return ONLY this JSON structure (no other text):
{{
  "status": "success|failure",
  "updated_tasks": [{{
    "id": "task_id",
    "details": {{
      "task_details": "",
      "task_owner_name": "", 
      "task_owner_id": "",
      "due_date": "",
      "priority": ""
    }}
  }}],
  "errors": [],
  "reason": "error description if status is failure"
}}"""

    # === Retry Prompt ===
    if is_retry:
        logging.info(f"RETRY → Using enhanced prompt (attempt {current_try}/{max_tries})")
        prev_reason = previous_status.get("reason", "Unknown error") if previous_status else "No status"
        prev_resp = json.dumps(previous_response, indent=2) if previous_response else "None"

        prompt = f"""PREVIOUS TASK UPDATE FAILED

Previous AI Response:
{prev_resp}

Failure Reason: {prev_reason}

This is retry #{current_try} of {max_tries} — FIX THE ISSUE.

Tasks to Update:
{json.dumps(to_update, indent=2)}

{base_prompt}

YOU MUST FIX:
- Invalid JSON (commas, quotes, brackets)
- Wrong field names (e.g. hs_task_subject vs hs_task_body)
- Missing hubspot_owner_id
- Incorrect hs_timestamp format
- Not preserving original task body
- Returning extra text

RETURN ONLY CLEAN, VALID JSON."""
    else:
        logging.info(f"Initial attempt {current_try}/{max_tries}")
        prompt = base_prompt

    response = None
    try:
        response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
        logging.info(f"Raw AI response: {response[:1000]}...")

        parsed = json.loads(response.strip())
        if parsed.get("status") != "success":
            raise Exception(parsed.get("reason", "LLM returned status != success"))

        updated_tasks = parsed.get("updated_tasks", [])

        # === Post-process: Restore missing details (safety net) ===
        for task in updated_tasks:
            task_id = task.get("id")
            if task_id in task_details_map:
                original = task_details_map[task_id]
                details = task.setdefault("details", {})
                if not details.get("task_details"):
                    details["task_details"] = original.get("task_details", "")
                if "task_index" not in details:
                    details["task_index"] = original.get("task_index")

            # Log assignment
            d = task.get("details", {})
            logging.info(f"Task {d.get('task_index', task_id)} → Assigned to {d.get('task_owner_name')} (ID: {d.get('task_owner_id')})")

        # === SUCCESS ===
        result = {
            "updated_tasks": updated_tasks,
            "task_update_status": {"status": "success"},
            "task_update_response": parsed
        }
        for k, v in result.items():
            ti.xcom_push(key=k, value=v)

        logging.info(f"SUCCESS: Updated {len(updated_tasks)} tasks on attempt {current_try}")
        return updated_tasks

    except json.JSONDecodeError as e:
        error_msg = f"Invalid JSON from AI: {e}\nRaw: {response}"
        logging.error(error_msg)
        raise Exception(error_msg)

    except Exception as e:
        error_msg = str(e) or "Unknown error during task update"
        is_final = current_try >= max_tries
        status_type = "final_failure" if is_final else "failure"

        fallback = {
            "updated_tasks": [],
            "task_update_status": {"status": status_type, "reason": error_msg},
            "task_update_response": {"raw_response": response} if response else None
        }
        for k, v in fallback.items():
            ti.xcom_push(key=k, value=v)

        if is_final:
            logging.error(f"FINAL FAILURE: update_tasks failed after {max_tries} attempts")
            raise
        else:
            logging.warning(f"update_tasks failed → retrying ({current_try}/{max_tries})")
            raise

def create_associations(ti, **context):
    """Create associations between HubSpot entities with full retry support"""
    # === Retry Context ===
    task_instance = context['task_instance']
    current_try = task_instance.try_number
    max_tries = task_instance.max_tries

    logging.info(f"=== CREATE ASSOCIATIONS - Attempt {current_try}/{max_tries} ===")

    previous_status = ti.xcom_pull(key="association_creation_status")
    previous_response = ti.xcom_pull(key="association_creation_response")
    is_retry = current_try > 1

    # === Load all data ===
    analysis_results = ti.xcom_pull(key="analysis_results")
    chat_history = ti.xcom_pull(key="chat_history", default=[])
    thread_history = ti.xcom_pull(key="thread_history", default=[])
    latest_user_message = ti.xcom_pull(key="latest_message", default="")
    # Created entities
    created_contacts = ti.xcom_pull(key="created_contacts", default=[])
    created_companies = ti.xcom_pull(key="created_companies", default=[])
    created_deals = ti.xcom_pull(key="created_deals", default=[])
    created_meetings = ti.xcom_pull(key="created_meetings", default=[])
    created_notes = ti.xcom_pull(key="created_notes", default=[])
    created_tasks = ti.xcom_pull(key="created_tasks", default=[])
    # Updated entities
    updated_contacts = ti.xcom_pull(key="updated_contacts", default=[])
    updated_companies = ti.xcom_pull(key="updated_companies", default=[])
    updated_deals = ti.xcom_pull(key="updated_deals", default=[])
    # Selected existing entities from analysis
    selected_entities = analysis_results.get("selected_entities", {})
    existing_contact_ids = [str(c.get("contactId")) for c in selected_entities.get("contacts", []) if c.get("contactId")]
    existing_company_ids = [str(c.get("companyId")) for c in selected_entities.get("companies", []) if c.get("companyId")]
    existing_deal_ids = [str(d.get("dealId")) for d in selected_entities.get("deals", []) if d.get("dealId")]

    # Extract IDs from created/updated
    new_contact_ids = [c.get("id") for c in created_contacts if c.get("id")]
    new_company_ids = [c.get("id") for c in created_companies if c.get("id")]
    new_deal_ids = [d.get("id") for d in created_deals if d.get("id")]
    new_meeting_ids = [m.get("id") for m in created_meetings if m.get("id")]
    new_note_ids = [n.get("id") for n in created_notes if n.get("id")]
    new_task_ids = [t.get("id") for t in created_tasks if t.get("id")]

    updated_contact_ids = [c.get("id") for c in updated_contacts if c.get("id")]
    updated_company_ids = [c.get("id") for c in updated_companies if c.get("id")]
    updated_deal_ids = [d.get("id") for d in updated_deals if d.get("id")]

    # Combine ALL IDs
    all_contact_ids = list(set(new_contact_ids + updated_contact_ids + existing_contact_ids))
    all_company_ids = list(set(new_company_ids + updated_company_ids + existing_company_ids))
    all_deal_ids = list(set(new_deal_ids + updated_deal_ids + existing_deal_ids))
    logging.info(f"All Contact IDs: {all_contact_ids}")
    logging.info(f"All Company IDs: {all_company_ids}")
    logging.info(f"All Deal IDs: {all_deal_ids}")
    logging.info(f"New Note/Meeting/Task IDs: {new_note_ids}, {new_meeting_ids}, {new_task_ids}")

    # Build clean conversation context
    conversation_context = ""
    for msg in chat_history:
        role = msg.get("role", "unknown").upper()
        content = msg.get("content", "")
        conversation_context += f"[{role}]: {content}\n\n"

    for idx, email in enumerate(thread_history, 1):
        content = email.get("content", "").strip()
        if content:
            soup = BeautifulSoup(content, "html.parser")
            clean_content = soup.get_text(separator=" ", strip=True)
            sender = email['headers'].get('From', 'Unknown')
            role_label = "BOT" if email.get('from_bot', False) else "USER"
            conversation_context += f"[{role_label} EMAIL {idx} - From: {sender}]: {clean_content}\n\n"

    # === Base Prompt ===
    base_prompt = f"""You are a HubSpot API assistant responsible for creating associations between entities using create_multi_association tool.

FULL CHAT HISTORY:
{conversation_context}

LATEST USER MESSAGE:
{latest_user_message}

How to create associations: Always and strictly call create_multi_association API/Tool to create association.
CRITICAL: You MUST call the create_multi_association tool. Do NOT just return JSON text. CALL THE TOOL.
AVAILABLE ENTITY IDS:
- Contact IDs (just created): {all_contact_ids}
- Company IDs (just created): {all_company_ids}
- Deal IDs (just created): {all_deal_ids}
- Meeting IDs (just created): {new_meeting_ids}
- Note IDs (just created): {new_note_ids}
- Task IDs (just created): {new_task_ids}
CRITICAL ASSOCIATION RULES:
- Associate with all available ids.
**IMPORTANT**: 
    - You can only create asssociation using tool `create_multi_association`
    - You can only create asssociation using tool `create_multi_association`
    - You MUST actually CALL the tool, not just output JSON
    - Always parse all the new and existing ids from conversation and use it in the given request bodies.
    - If any entity is blank then fill it with `''`.
    
Below request body should be used as input for create_multi_association tool:
    - Always use this exact structure when calling create_multi_association, Never deviate from this structure:
{{"single":{{"deal_id":"string1, string2","contact_id":"string1, string2,...","company_id":"string1, string2,..","note_id":"string1, string2,..","task_id":"string1, string2..","meeting_id":"string1, string2,.."}}}}
Return ONLY valid JSON:
{{
    "association_requests": [
        {{
            "single": {{
                "deal_id": "123",
                "contact_id": "456", 
                "company_id": "789",
                "note_id": "101",
                "task_id": "202",
                "meeting_id": "303"
            }}
        }}
    ],
    "ids_from_conversation": {{
        "contact_ids": [],
        "company_ids": [],
        "deal_ids": ["123"],
        "note_ids": [],
        "task_ids": [],
        "meeting_ids": []
    }},
    "errors": [],
    "error": null
}}

The "ids_from_conversation" field should list any IDs you extracted from the conversation history (not from AVAILABLE ENTITY IDS).

If error, set error message and include individual errors in the errors array.

Remember: Empty string "" for non-applicable fields, comma-separated for multiple IDs.

NOW TAKE ACTION: Based on the conversation above, CALL the create_multi_association tool with the appropriate associations.
"""

    # === Retry Prompt ===
    if is_retry:
        logging.info(f"RETRY → Using enhanced prompt (attempt {current_try}/{max_tries})")
        prev_reason = previous_status.get("reason", "Unknown error") if previous_status else "No previous status"
        prev_resp = json.dumps(previous_response, indent=2) if previous_response else "None"

        prompt = f"""PREVIOUS ASSOCIATION CREATION FAILED

Previous Response:
{prev_resp}

Failure Reason: {prev_reason}

Retry #{current_try} of {max_tries} — YOU MUST FIX THIS.

Available IDs:
Contacts: {all_contact_ids}
Companies: {all_company_ids}
Deals: {all_deal_ids}
Meetings/Notes/Tasks: {new_meeting_ids + new_note_ids + new_task_ids}

{base_prompt}

YOU MUST:
- Actually CALL create_multi_association
- Include ALL available IDs
- Return valid JSON only
- Fix malformed output, missing fields, or refusal to call tool

RETURN ONLY CLEAN JSON."""
    else:
        logging.info(f"Initial attempt {current_try}/{max_tries}")
        prompt = base_prompt

    response = None
    try:
        response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
        logging.info(f"Raw AI response: {response[:1000]}...")

        parsed = json.loads(response.strip())
        association_requests = parsed.get("association_requests", [])
        ids_from_conversation = parsed.get("ids_from_conversation", {})

        # === DEFAULT FALLBACK: Critical safety net ===
        if not association_requests and any([
            new_note_ids, new_task_ids, new_meeting_ids,
            all_contact_ids, all_company_ids, all_deal_ids
        ]):
            logging.warning("AI failed to suggest associations → creating defaults")
            association_requests = []

            for note_id in new_note_ids:
                association_requests.append({"single": {
                    "note_id": note_id,
                    "contact_id": ",".join(all_contact_ids) if all_contact_ids else "",
                    "company_id": ",".join(all_company_ids) if all_company_ids else "",
                    "deal_id": ",".join(all_deal_ids) if all_deal_ids else "",
                    "task_id": "", "meeting_id": ""
                }})
            for task_id in new_task_ids:
                association_requests.append({"single": {
                    "task_id": task_id,
                    "contact_id": ",".join(all_contact_ids) if all_contact_ids else "",
                    "company_id": ",".join(all_company_ids) if all_company_ids else "",
                    "deal_id": ",".join(all_deal_ids) if all_deal_ids else "",
                    "note_id": "", "meeting_id": ""
                }})
            for meeting_id in new_meeting_ids:
                association_requests.append({"single": {
                    "meeting_id": meeting_id,
                    "contact_id": ",".join(all_contact_ids) if all_contact_ids else "",
                    "company_id": ",".join(all_company_ids) if all_company_ids else "",
                    "deal_id": ",".join(all_deal_ids) if all_deal_ids else "",
                    "note_id": "", "task_id": ""
                }})

        # === SUCCESS ===
        result = {
            "associations_created": association_requests,
            "extracted_conversation_ids": ids_from_conversation,
            "association_creation_status": {"status": "success"},
            "association_creation_response": parsed
        }
        for k, v in result.items():
            ti.xcom_push(key=k, value=v)

        logging.info(f"SUCCESS: Created {len(association_requests)} association requests on attempt {current_try}")
        return association_requests

    except json.JSONDecodeError as e:
        error_msg = f"Invalid JSON from AI: {e}\nRaw: {response}"
        logging.error(error_msg)
        raise Exception(error_msg)

    except Exception as e:
        error_msg = str(e) or "Unknown errorError"
        is_final = current_try >= max_tries
        status_type = "final_failure" if is_final else "failure"

        fallback = {
            "associations_created": [],
            "extracted_conversation_ids": {},
            "association_creation_status": {"status": status_type, "reason": error_msg},
            "association_creation_response": {"raw_response": response} if response else None
        }
        for k, v in fallback.items():
                       ti.xcom_push(key=k, value=v)

        if is_final:
            logging.error(f"FINAL FAILURE: create_associations failed after {max_tries} attempts")
            raise
        else:
            logging.warning(f"create_associations failed → retrying ({current_try}/{max_tries})")
            raise

def collect_and_save_results(ti, **context):
    """Collect all results for final email"""
    created_contacts = ti.xcom_pull(key="created_contacts", default=[])
    created_companies = ti.xcom_pull(key="created_companies", default=[])
    created_deals = ti.xcom_pull(key="created_deals", default=[])
    created_meetings = ti.xcom_pull(key="created_meetings", default=[])
    created_notes = ti.xcom_pull(key="created_notes", default=[])
    created_tasks = ti.xcom_pull(key="created_tasks", default=[])
    
    updated_contacts = ti.xcom_pull(key="updated_contacts", default=[])
    updated_companies = ti.xcom_pull(key="updated_companies", default=[])
    updated_deals = ti.xcom_pull(key="updated_deals", default=[])
    updated_meetings = ti.xcom_pull(key="updated_meetings", default=[])
    updated_notes = ti.xcom_pull(key="updated_notes", default=[])
    updated_tasks = ti.xcom_pull(key="updated_tasks", default=[])
    
    associations_created = ti.xcom_pull(key="associations_created", default=[])
    analysis_results = ti.xcom_pull(key="analysis_results", default={})
    selected_entities = analysis_results.get("selected_entities", {})
    
    create_results = {
        "created_contacts": {"total": len(created_contacts), "results": created_contacts},
        "created_companies": {"total": len(created_companies), "results": created_companies},
        "created_deals": {"total": len(created_deals), "results": created_deals},
        "created_meetings": {"total": len(created_meetings), "results": created_meetings},
        "created_notes": {"total": len(created_notes), "results": created_notes},
        "created_tasks": {"total": len(created_tasks), "results": created_tasks},
        "updated_contacts": {"total": len(updated_contacts), "results": updated_contacts},
        "updated_companies": {"total": len(updated_companies), "results": updated_companies},
        "updated_deals": {"total": len(updated_deals), "results": updated_deals},
        "updated_meetings": {"total": len(updated_meetings), "results": updated_meetings},
        "updated_notes": {"total": len(updated_notes), "results": updated_notes},
        "updated_tasks": {"total": len(updated_tasks), "results": updated_tasks},
        "associations_created": {"total": len(associations_created), "results": associations_created},
        "selected_contacts": {"total": len(selected_entities.get("contacts", [])), "results": selected_entities.get("contacts", [])},
        "selected_companies": {"total": len(selected_entities.get("companies", [])), "results": selected_entities.get("companies", [])},
        "selected_deals": {"total": len(selected_entities.get("deals", [])), "results": selected_entities.get("deals", [])}
    }
    
    ti.xcom_push(key="create_results", value=create_results)
    logging.info(f"Collected results: {sum(r['total'] for r in create_results.values())} total operations")
    return create_results

def compose_response_html(ti, **context):
    """Compose HTML response email with all created/updated/selected entities"""
    analysis_results = ti.xcom_pull(key="analysis_results", default={})
    if analysis_results.get("fallback_email_sent", False):
        logging.info("Fallback email was already sent - skipping compose_response_html")
        ti.xcom_push(key="response_html", value=None)
        return None
    owner_info = ti.xcom_pull(key="owner_info", default={})
    task_threshold_info = ti.xcom_pull(key="task_threshold_info", default={})

    contact_creation_final_status = ti.xcom_pull(key="contact_creation_final_status")
    contact_creation_failure_reason = ti.xcom_pull(key="contact_creation_failure_reason")
    
    created_contacts = ti.xcom_pull(key="created_contacts", default=[])
    created_companies = ti.xcom_pull(key="created_companies", default=[])
    created_deals = ti.xcom_pull(key="created_deals", default=[])
    created_meetings = ti.xcom_pull(key="created_meetings", default=[])
    created_notes = ti.xcom_pull(key="created_notes", default=[])
    created_tasks = ti.xcom_pull(key="created_tasks", default=[])
    
    updated_contacts = ti.xcom_pull(key="updated_contacts", default=[])
    updated_companies = ti.xcom_pull(key="updated_companies", default=[])
    updated_deals = ti.xcom_pull(key="updated_deals", default=[])
    updated_meetings = ti.xcom_pull(key="updated_meetings", default=[])
    updated_notes = ti.xcom_pull(key="updated_notes", default=[])
    updated_tasks = ti.xcom_pull(key="updated_tasks", default=[])
    
    selected_entities = analysis_results.get("selected_entities", {})
    existing_contacts = selected_entities.get("contacts", [])
    existing_companies = selected_entities.get("companies", [])
    existing_deals = selected_entities.get("deals", [])
    
    thread_id = context['dag_run'].conf.get("thread_id")
    email_data = ti.xcom_pull(key="email_data", default={})
    from_sender = email_data.get("headers", {}).get("From", "")
    
    # Filter out updated tasks from created tasks to avoid duplication
    updated_task_ids = [task.get("id") for task in updated_tasks if task.get("id")]
    final_created_tasks = [t for t in created_tasks if t.get("id") not in updated_task_ids]
    
    email_content = f"""<!DOCTYPE html>
<html>
<head>
    <style>
        table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
        th {{ background-color: #f2f2f2; font-weight: bold; }}
        h3 {{ color: #333; margin-top: 30px; margin-bottom: 15px; }}
        .greeting {{ margin-bottom: 20px; }}
        .closing {{ margin-top: 30px; }}
    </style>
</head>
<body>
    <div class="greeting">
        <p>Hello {from_sender},</p>
        <p>I have completed the requested operations in HubSpot. Here is the summary:</p>
    </div>
"""
    
    # Existing/Used Contacts
    if existing_contacts or updated_contacts:
        email_content += """
        <h3>Contacts Used/Updated</h3>
        <table>
            <thead>
                <tr>
                    <th>ID</th>
                    <th>Firstname</th>
                    <th>Lastname</th>
                    <th>Email</th>
                    <th>Phone</th>
                    <th>Address</th>
                    <th>Job Title</th>
                    <th>Contact Owner</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
        """
        for contact in existing_contacts:
            details = contact.get("details", contact)
            email_content += f"""
                <tr>
                    <td>{contact.get("contactId", "")}</td>
                    <td>{details.get("firstname", "")}</td>
                    <td>{details.get("lastname", "")}</td>
                    <td>{details.get("email", "")}</td>
                    <td>{details.get("phone", "")}</td>
                    <td>{details.get("address", "")}</td>
                    <td>{details.get("jobtitle", "")}</td>
                    <td>{details.get("contactOwnerName", "")}</td>
                    <td>Existing</td>
                </tr>
            """
        for contact in updated_contacts:
            details = contact.get("details", {})
            email_content += f"""
                <tr>
                    <td>{contact.get("id", "")}</td>
                    <td>{details.get("firstname", "")}</td>
                    <td>{details.get("lastname", "")}</td>
                    <td>{details.get("email", "")}</td>
                    <td>{details.get("phone", "")}</td>
                    <td>{details.get("address", "")}</td>
                    <td>{details.get("jobtitle", "")}</td>
                    <td>{details.get("contactOwnerName", "")}</td>
                    <td>Updated</td>
                </tr>
            """
        email_content += "</tbody></table>"
    
    # Existing/Used Companies
    if existing_companies or updated_companies:
        email_content += """
        <h3>Companies Used/Updated</h3>
        <table>
            <thead>
                <tr>
                    <th>ID</th>
                    <th>Name</th>
                    <th>Domain</th>
                    <th>Address</th>
                    <th>City</th>
                    <th>State</th>
                    <th>ZIP</th>
                    <th>Country</th>
                    <th>Phone</th>
                    <th>Description</th>
                    <th>Type</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
        """
        for company in existing_companies:
            details = company.get("details", company)
            email_content += f"""
                <tr>
                    <td>{company.get("companyId", "")}</td>
                    <td>{details.get("name", "")}</td>
                    <td>{details.get("domain", "")}</td>
                    <td>{details.get("address", "")}</td>
                    <td>{details.get("city", "")}</td>
                    <td>{details.get("state", "")}</td>
                    <td>{details.get("zip", "")}</td>
                    <td>{details.get("country", "")}</td>
                    <td>{details.get("phone", "")}</td>
                    <td>{details.get("description", "")}</td>
                    <td>{details.get("type", "")}</td>
                    <td>Existing</td>
                </tr>
            """
        for company in updated_companies:
            details = company.get("details", {})
            email_content += f"""
                <tr>
                    <td>{company.get("id", "")}</td>
                    <td>{details.get("name", "")}</td>
                    <td>{details.get("domain", "")}</td>
                    <td>{details.get("address", "")}</td>
                    <td>{details.get("city", "")}</td>
                    <td>{details.get("state", "")}</td>
                    <td>{details.get("zip", "")}</td>
                    <td>{details.get("country", "")}</td>
                    <td>{details.get("phone", "")}</td>
                    <td>{details.get("description", "")}</td>
                    <td>{details.get("type", "")}</td>
                    <td>Updated</td>
                </tr>
            """
        email_content += "</tbody></table>"
    
    # Existing/Used Deals
    # Existing/Used Deals
    if existing_deals or updated_deals:
        # CHANGED: Filter out deals that were updated from existing_deals
        updated_deal_ids = [deal.get("id") for deal in updated_deals if deal.get("id")]
        final_existing_deals = [d for d in existing_deals if d.get("dealId") not in updated_deal_ids]
        
        # CHANGED: Only show table if there are deals to display
        if final_existing_deals or updated_deals:
            email_content += """
            <h3>Deals Used/Updated</h3>
            <table>
                <thead>
                    <tr>
                        <th>ID</th>
                        <th>Deal Name</th>
                        <th>Deal Stage Label</th>
                        <th>Deal Amount</th>
                        <th>Close Date</th>
                        <th>Deal Owner Name</th>
                        <th>Status</th>
                    </tr>
                </thead>
                <tbody>
            """
            # CHANGED: Use filtered existing deals
            for deal in final_existing_deals:
                details = deal.get("details", deal)
                email_content += f"""
                    <tr>
                        <td>{deal.get("dealId", "")}</td>
                        <td>{details.get("dealName", "")}</td>
                        <td>{details.get("dealLabelName", "")}</td>
                        <td>{details.get("dealAmount", "")}</td>
                        <td>{details.get("closeDate", "")}</td>
                        <td>{details.get("dealOwnerName", "")}</td>
                        <td>Existing</td>
                    </tr>
                """
            for deal in updated_deals:
                details = deal.get("details", {})
                email_content += f"""
                    <tr>
                        <td>{deal.get("id", "")}</td>
                        <td>{details.get("dealName", "")}</td>
                        <td>{details.get("dealLabelName", "")}</td>
                        <td>{details.get("dealAmount", "")}</td>
                        <td>{details.get("closeDate", "")}</td>
                        <td>{details.get("dealOwnerName", "")}</td>
                        <td>Updated</td>
                    </tr>
                """
            email_content += "</tbody></table>"
    # Newly Created Contacts
    if created_contacts:
        email_content += """
        <h3>Newly Created Contacts</h3>
        <table>
            <thead>
                <tr>
                    <th>ID</th>
                    <th>Firstname</th>
                    <th>Lastname</th>
                    <th>Email</th>
                    <th>Phone</th>
                    <th>Address</th>
                    <th>Job Title</th>
                    <th>Contact Owner</th>
                </tr>
            </thead>
            <tbody>
        """
        for contact in created_contacts:
            details = contact.get("details", {})
            email_content += f"""
                <tr>
                    <td>{contact.get("id", "")}</td>
                    <td>{details.get("firstname", "")}</td>
                    <td>{details.get("lastname", "")}</td>
                    <td>{details.get("email", "")}</td>
                    <td>{details.get("phone", "")}</td>
                    <td>{details.get("address", "")}</td>
                    <td>{details.get("jobtitle", "")}</td>
                    <td>{details.get("contactOwnerName", "")}</td>
                </tr>
            """
        email_content += "</tbody></table>"
    
    # Newly Created Companies
    if created_companies:
        email_content += """
        <h3>Newly Created Companies</h3>
        <table>
            <thead>
                <tr>
                    <th>ID</th>
                    <th>Name</th>
                    <th>Domain</th>
                    <th>Address</th>
                    <th>City</th>
                    <th>State</th>
                    <th>ZIP</th>
                    <th>Country</th>
                    <th>Phone</th>
                    <th>Description</th>
                    <th>Type</th>
                </tr>
            </thead>
            <tbody>
        """
        for company in created_companies:
            details = company.get("details", {})
            email_content += f"""
                <tr>
                    <td>{company.get("id", "")}</td>
                    <td>{details.get("name", "")}</td>
                    <td>{details.get("domain", "")}</td>
                    <td>{details.get("address", "")}</td>
                    <td>{details.get("city", "")}</td>
                    <td>{details.get("state", "")}</td>
                    <td>{details.get("zip", "")}</td>
                    <td>{details.get("country", "")}</td>
                    <td>{details.get("phone", "")}</td>
                    <td>{details.get("description", "")}</td>
                    <td>{details.get("type", "")}</td>
                </tr>
            """
        email_content += "</tbody></table>"
    
    # Newly Created Deals
    if created_deals:
        email_content += """
        <h3>Newly Created Deals</h3>
        <table>
            <thead>
                <tr>
                    <th>ID</th>
                    <th>Deal Name</th>
                    <th>Deal Stage Label</th>
                    <th>Deal Amount</th>
                    <th>Close Date</th>
                    <th>Deal Owner Name</th>
                </tr>
            </thead>
            <tbody>
        """
        for deal in created_deals:
            details = deal.get("details", {})
            email_content += f"""
                <tr>
                    <td>{deal.get("id", "")}</td>
                    <td>{details.get("dealName", "")}</td>
                    <td>{details.get("dealLabelName", "")}</td>
                    <td>{details.get("dealAmount", "")}</td>
                    <td>{details.get("closeDate", "")}</td>
                    <td>{details.get("dealOwnerName", "")}</td>
                </tr>
            """
        email_content += "</tbody></table>"
    
    # Meetings Created/Updated
    if created_meetings or updated_meetings:
        email_content += """
        <h3>Meetings Created/Updated</h3>
        <table>
            <thead>
                <tr>
                    <th>ID</th>
                    <th>Meeting Title</th>
                    <th>Start Time</th>
                    <th>End Time</th>
                    <th>Location</th>
                    <th>Outcome</th>
                    <th>Timestamp</th>
                    <th>Attendees</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
        """
        for meeting in created_meetings:
            details = meeting.get("details", {})
            attendees = ", ".join(details.get("attendees", []))
            email_content += f"""
                <tr>
                    <td>{meeting.get("id", "")}</td>
                    <td>{details.get("meeting_title", "")}</td>
                    <td>{details.get("start_time", "")}</td>
                    <td>{details.get("end_time", "")}</td>
                    <td>{details.get("location", "")}</td>
                    <td>{details.get("outcome", "")}</td>
                    <td>{details.get("timestamp", "")}</td>
                    <td>{attendees}</td>
                    <td>Created</td>
                </tr>
            """
        for meeting in updated_meetings:
            details = meeting.get("details", {})
            attendees = ", ".join(details.get("attendees", []))
            email_content += f"""
                <tr>
                    <td>{meeting.get("id", "")}</td>
                    <td>{details.get("meeting_title", "")}</td>
                    <td>{details.get("start_time", "")}</td>
                    <td>{details.get("end_time", "")}</td>
                    <td>{details.get("location", "")}</td>
                    <td>{details.get("outcome", "")}</td>
                    <td>{details.get("timestamp", "")}</td>
                    <td>{attendees}</td>
                    <td>Updated</td>
                </tr>
            """
        email_content += "</tbody></table>"
    
    # Notes Created/Updated
    if created_notes or updated_notes:
        email_content += """
        <h3>Notes Created/Updated</h3>
        <table>
            <thead>
                <tr>
                    <th>ID</th>
                    <th>Note Content</th>
                    <th>Timestamp</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
        """
        for note in created_notes:
            details = note.get("details", {})
            email_content += f"""
                <tr>
                    <td>{note.get("id", "")}</td>
                    <td>{details.get("note_content", "")}</td>
                    <td>{details.get("timestamp", "")}</td>
                    <td>Created</td>
                </tr>
            """
        for note in updated_notes:
            details = note.get("details", {})
            email_content += f"""
                <tr>
                    <td>{note.get("id", "")}</td>
                    <td>{details.get("note_content", "")}</td>
                    <td>{details.get("timestamp", "")}</td>
                    <td>Updated</td>
                </tr>
            """
        email_content += "</tbody></table>"
    
    # Tasks Created/Updated
    if final_created_tasks or updated_tasks:
        email_content += """
        <h3>Tasks Created/Updated</h3>
        <table>
            <thead>
                <tr>
                    <th>ID</th>
                    <th>Task Details</th>
                    <th>Owner Name</th>
                    <th>Owner ID</th>
                    <th>Due Date</th>
                    <th>Priority</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
        """
        for task in final_created_tasks:
            details = task.get("details", {})
            email_content += f"""
                <tr>
                    <td>{task.get("id", "")}</td>
                    <td>{details.get("task_details", "")}</td>
                    <td>{details.get("task_owner_name", "")}</td>
                    <td>{details.get("task_owner_id", "")}</td>
                    <td>{details.get("due_date", "")}</td>
                    <td>{details.get("priority", "")}</td>
                    <td>Created</td>
                </tr>
            """
        for task in updated_tasks:
            details = task.get("details", {})
            email_content += f"""
                <tr>
                    <td>{task.get("id", "")}</td>
                    <td>{details.get("task_details", "")}</td>
                    <td>{details.get("task_owner_name", "")}</td>
                    <td>{details.get("task_owner_id", "")}</td>
                    <td>{details.get("due_date", "")}</td>
                    <td>{details.get("priority", "")}</td>
                    <td>Updated</td>
                </tr>
            """
        email_content += "</tbody></table>"
    
    # # Task Volume Analysis
    # dates_checked = task_threshold_info.get("task_threshold_results", {}).get("dates_checked", [])
    # if final_created_tasks and dates_checked:
    #     email_content += """
    #     <h3>Task Volume Analysis</h3>
    #     <table>
    #         <thead>
    #             <tr>
    #                 <th>Date</th>
    #                 <th>Owner Name</th>
    #                 <th>Existing Tasks</th>
    #                 <th>Threshold Status</th>
    #                 <th>Warning</th>
    #             </tr>
    #         </thead>
    #         <tbody>
    #     """
    #     for date_info in dates_checked:
    #         exceeds = "Exceeds" if date_info.get("exceeds_threshold") else "Within Limit"
    #         warning = date_info.get("warning") or "None"
    #         email_content += f"""
    #             <tr>
    #                 <td>{date_info.get("date", "")}</td>
    #                 <td>{date_info.get("owner_name", "")}</td>
    #                 <td>{date_info.get("existing_task_count", 0)}</td>
    #                 <td>{exceeds}</td>
    #                 <td>{warning}</td>
    #             </tr>
    #         """
    #     email_content += """
    #         </tbody>
    #     </table>
    #     <p><em>Note: High task volumes may impact workflow performance and user productivity.</em></p>
    #     """
    
    # # Owner Assignment Section
    # has_deals_or_tasks = (
    #     (existing_deals and len(existing_deals) > 0) or
    #     (created_deals and len(created_deals) > 0) or
    #     (final_created_tasks and len(final_created_tasks) > 0)
    # )
    
    # if has_deals_or_tasks and owner_info:
    #     chosen_deal_owner_id = owner_info.get("deal_owner_id", "71346067")
    #     chosen_deal_owner_name = owner_info.get("deal_owner_name", "Kishore")
    #     deal_owner_msg = owner_info.get("deal_owner_message", "")
    #     task_owners = owner_info.get("task_owners", [])
    #     all_owners = owner_info.get("all_owners_table", [])
        
    #     email_content += "<h3>Owner Assignment Details</h3>"
        
    #     # Deal Owner Assignment
    #     if (existing_deals and len(existing_deals) > 0) or (created_deals and len(created_deals) > 0):
    #         email_content += """
    #         <h4>Deal Owner Assignment:</h4>
    #         <table>
    #             <thead>
    #                 <tr>
    #                     <th>Reason</th>
    #                     <th>Action</th>
    #                 </tr>
    #             </thead>
    #             <tbody>
    #         """
    #         deal_msg_lower = deal_owner_msg.lower()
    #         if "no deal owner specified" in deal_msg_lower:
    #             email_content += f"""
    #                 <tr>
    #                     <td>Deal owner was not specified.</td>
    #                     <td>Assigning to default owner '{chosen_deal_owner_name}'.</td>
    #                 </tr>
    #             """
    #         elif "not valid" in deal_msg_lower:
    #             email_content += f"""
    #                 <tr>
    #                     <td>Deal owner mentioned, but not found in the available owners list.</td>
    #                     <td>Assigning to default owner '{chosen_deal_owner_name}'.</td>
    #                 </tr>
    #             """
    #         else:
    #             email_content += f"""
    #                 <tr>
    #                     <td>Deal owner is valid and found in the available owners list.</td>
    #                     <td>Assigned to '{chosen_deal_owner_name}'.</td>
    #                 </tr>
    #             """
    #         email_content += "</tbody></table>"
        
    #     # Task Owner Assignments
    #     if final_created_tasks and task_owners:
    #         email_content += """
    #         <h4>Task Owner Assignments:</h4>
    #         <table>
    #             <thead>
    #                 <tr>
    #                     <th>Task Index</th>
    #                     <th>Task Details</th>
    #                     <th>Reason</th>
    #                     <th>Action</th>
    #                 </tr>
    #             </thead>
    #             <tbody>
    #         """
    #         for task_owner in task_owners:
    #             task_index = task_owner.get("task_index", 0)
    #             task_owner_name = task_owner.get("task_owner_name", "Kishore")
    #             task_owner_msg = task_owner.get("task_owner_message", "")
                
    #             task = next((t for t in final_created_tasks if t.get("details", {}).get("task_index") == task_index), None)
    #             task_details = task.get("details", {}).get("task_details", "Unknown task") if task else "Unknown task"
                
    #             task_msg_lower = task_owner_msg.lower()
                
    #             if "no task owner specified" in task_msg_lower:
    #                 email_content += f"""
    #                     <tr>
    #                         <td>{task_index}</td>
    #                         <td>{task_details}</td>
    #                         <td>Task owner was not specified.</td>
    #                         <td>Assigning to default owner '{task_owner_name}'.</td>
    #                     </tr>
    #                 """
    #             elif "not valid" in task_msg_lower:
    #                 email_content += f"""
    #                     <tr>
    #                         <td>{task_index}</td>
    #                         <td>{task_details}</td>
    #                         <td>Task owner mentioned, but not found in the available owners list.</td>
    #                         <td>Assigning to default owner '{task_owner_name}'.</td>
    #                     </tr>
    #                 """
    #             else:
    #                 email_content += f"""
    #                     <tr>
    #                         <td>{task_index}</td>
    #                         <td>{task_details}</td>
    #                         <td>Task owner is valid and found in the available owners list.</td>
    #                         <td>Assigned to '{task_owner_name}'.</td>
    #                     </tr>
    #                 """
    #         email_content += "</tbody></table>"
        
    #     # Available Owners Table
    #     if all_owners:
    #         email_content += """
    #         <h4>Available Owners:</h4>
    #         <table>
    #             <thead>
    #                 <tr>
    #                     <th>Owner ID</th>
    #                     <th>Owner Name</th>
    #                     <th>Owner Email</th>
    #                     <th>Assignment</th>
    #                 </tr>
    #             </thead>
    #             <tbody>
    #         """
            
    #         for owner in all_owners:
    #             owner_id = owner.get("id", "")
    #             owner_name = owner.get("name", "")
    #             owner_email = owner.get("email", "")
                
    #             assignments = []
    #             if owner_id == chosen_deal_owner_id and ((existing_deals and len(existing_deals) > 0) or (created_deals and len(created_deals) > 0)):
    #                 assignments.append("Deal Owner")
                
    #             if any(task_owner.get("task_owner_id") == owner_id for task_owner in task_owners) and final_created_tasks:
    #                 task_indices = [str(task_owner.get("task_index")) for task_owner in task_owners if task_owner.get("task_owner_id") == owner_id]
    #                 assignments.append(f"Task Owner (Tasks {', '.join(task_indices)})")
                
    #             assignment_text = ", ".join(assignments) if assignments else ""
                
    #             email_content += f"""
    #                 <tr>
    #                     <td>{owner_id}</td>
    #                     <td>{owner_name}</td>
    #                     <td>{owner_email}</td>
    #                     <td>{assignment_text}</td>
    #                 </tr>
    #             """
            
    #         email_content += "</tbody></table>"
    
    # Closing
    email_content += """
    <div class="closing">
        <p>Please let me know if any adjustments or corrections are needed.</p>
        <p><strong>Best regards,</strong><br>The HubSpot Assistant Team<br><a href="http://lowtouch.ai">Lowtouch.ai</a></p>
    </div>
</body>
</html>"""
    
    ti.xcom_push(key="response_html", value=email_content)
    logging.info(f"Composed response HTML for thread {thread_id}")
    
    return email_content

def send_final_email(ti, **context):
    """Send final completion email with proper recipient handling"""
    import re
    analysis_results = ti.xcom_pull(key="analysis_results", default={})
    if analysis_results.get("fallback_email_sent", False):
        logging.info("Fallback email was already sent - skipping send_final_email")
        return None
    email_data = ti.xcom_pull(key="email_data", default={})
    response_html = ti.xcom_pull(key="response_html")
    
    service = authenticate_gmail()
    if not service:
        logging.error("Gmail authentication failed")
        raise ValueError("Gmail authentication failed")
    
    # Get the full email thread
    email_thread = get_email_thread(service, email_data)
    
    if not email_thread:
        logging.warning("No thread found, using only current email data")
        email_thread = [email_data]
    
    # Extract CC recipients from the current email headers
    current_email_headers = email_data.get("headers", {})
    current_cc = current_email_headers.get("Cc", "")
    
    # Parse CC recipients from the header string
    cc_recipients = []
    if current_cc:
        email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        cc_addresses = re.findall(email_pattern, current_cc)
        cc_recipients.extend(cc_addresses)
    
    # Collect recipients from thread
    thread_to_recipients = set()
    thread_cc_recipients = set()
    
    for email in email_thread:
        thread_recipients = extract_all_recipients(email)
        thread_to_recipients.update(thread_recipients.get("to", []))
        thread_cc_recipients.update(thread_recipients.get("cc", []))
    
    # Add thread CC recipients
    thread_cc_recipients.update(cc_recipients)
    
    # Get latest email for headers
    latest_email = email_thread[-1]
    sender_email = latest_email["headers"].get("From", "")
    original_subject = latest_email['headers'].get('Subject', 'HubSpot Request')
    
    # Extract email address from "From" header (might be "Name <email@domain.com>")
    sender_match = re.search(r'<([^>]+)>', sender_email)
    if sender_match:
        primary_recipient = sender_match.group(1)
    else:
        primary_recipient = sender_email
    
    subject = f"Re: {original_subject}" if not original_subject.lower().startswith('re:') else original_subject
    in_reply_to = latest_email["headers"].get("Message-ID", "")
    references = latest_email["headers"].get("References", "")
    
    # Build final CC list (excluding sender and bot)
    final_cc_recipients = []
    all_cc_candidates = list(thread_to_recipients) + list(thread_cc_recipients)
    
    for addr in all_cc_candidates:
        clean_addr = addr.strip()
        if (clean_addr and 
            clean_addr.lower() != primary_recipient.lower() and 
            HUBSPOT_FROM_ADDRESS.lower() not in clean_addr.lower() and
            clean_addr not in final_cc_recipients):
            final_cc_recipients.append(clean_addr)
    
    cc_string = ', '.join(final_cc_recipients) if final_cc_recipients else None
    
    # Note: BCC cannot be retrieved from received emails
    # If you need BCC, it must be stored when first processing the email
    bcc_string = None
    
    logging.info(f"Sending final email (reply-all):")
    logging.info(f"Thread size: {len(email_thread)} emails")
    logging.info(f"Primary recipient: {primary_recipient}")
    logging.info(f"Cc recipients ({len(final_cc_recipients)}): {cc_string}")
    logging.info(f"Bcc recipients: {bcc_string}")
    
    # Retry logic
    retries = 3
    for attempt in range(retries):
        try:
            result = send_email(service, primary_recipient, subject, response_html, 
                              in_reply_to, references, cc=cc_string, bcc=bcc_string)
            if result:
                logging.info(f"Final email sent successfully")
                return result
            else:
                logging.error(f"Attempt {attempt+1} failed")
        except Exception as e:
            logging.error(f"Attempt {attempt+1} failed: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                raise
    
    raise ValueError("Failed to send final email")

def branch_to_creation_tasks(ti, **context):
    """Branch to appropriate creation/update tasks"""
    analysis_results = ti.xcom_pull(key="analysis_results", default={})
    
    if not analysis_results or not isinstance(analysis_results, dict):
        logging.error("Invalid analysis_results")
        return ["create_associations", "compose_response_html", "collect_and_save_results", "send_final_email"]
    if analysis_results.get("fallback_email_sent", False):
        logging.info("Fallback email was sent - skipping all downstream tasks")
        return ["end_workflow"] 
    tasks_to_execute = analysis_results.get("tasks_to_execute", [])
    
    # Ensure mandatory tasks are included
    mandatory_tasks = ["create_associations", "compose_response_html", "collect_and_save_results", "send_final_email"]
    tasks_to_execute = list(set(tasks_to_execute + mandatory_tasks))
    
    logging.info(f"Tasks to execute: {tasks_to_execute}")
    return tasks_to_execute

# ============================================================================
# DAG DEFINITION
# ============================================================================

with DAG(
    "hubspot_create_objects",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["hubspot", "create", "objects"]
) as dag:

    start_task = DummyOperator(task_id="start_workflow")

    load_context_task = PythonOperator(
        task_id="load_context_from_dag_run",
        python_callable=load_context_from_dag_run,
        provide_context=True
    )

    analyze_task = PythonOperator(
        task_id="analyze_user_response",
        python_callable=analyze_user_response,
        provide_context=True
    )

    branch_task = BranchPythonOperator(
        task_id="branch_to_creation_tasks",
        python_callable=branch_to_creation_tasks,
        provide_context=True
    )

    determine_owner_task = PythonOperator(
        task_id="determine_owner",
        python_callable=determine_owner,
        provide_context=True
    )

    check_task_threshold_task = PythonOperator(
        task_id="check_task_threshold",
        python_callable=check_task_threshold,
        provide_context=True
    )

    create_contacts_task = PythonOperator(
        task_id="create_contacts",
        python_callable=create_contacts,
        retries=2,
        trigger_rule="all_done",
        provide_context=True
    )

    create_companies_task = PythonOperator(
        task_id="create_companies",
        python_callable=create_companies,
        retries=2,
        trigger_rule="all_done",
        provide_context=True
    )

    create_deals_task = PythonOperator(
        task_id="create_deals",
        python_callable=create_deals,
        retries=2,
        trigger_rule="all_done",
        provide_context=True
    )

    create_meetings_task = PythonOperator(
        task_id="create_meetings",
        python_callable=create_meetings,
        retries=2,
        trigger_rule="all_done",
        provide_context=True
    )

    create_notes_task = PythonOperator(
        task_id="create_notes",
        python_callable=create_notes,
        retries=2,
        trigger_rule="all_done",
        provide_context=True
    )

    create_tasks_task = PythonOperator(
        task_id="create_tasks",
        python_callable=create_tasks,
        retries=2,
        trigger_rule="all_done",
        provide_context=True
    )

    update_contacts_task = PythonOperator(
        task_id="update_contacts",
        python_callable=update_contacts,
        retries=2,
        trigger_rule="all_done",
        provide_context=True
    )

    update_companies_task = PythonOperator(
        task_id="update_companies",
        python_callable=update_companies,
        retries=2,
        trigger_rule="all_done",
        provide_context=True
    )

    update_deals_task = PythonOperator(
        task_id="update_deals",
        python_callable=update_deals,
        retries=2,
        trigger_rule="all_done",
        provide_context=True
    )

    update_meetings_task = PythonOperator(
        task_id="update_meetings",
        python_callable=update_meetings,
        retries=2,
        trigger_rule="all_done",
        provide_context=True
    )

    update_notes_task = PythonOperator(
        task_id="update_notes",
        python_callable=update_notes,
        retries=2,
        trigger_rule="all_done",
        provide_context=True
    )

    update_tasks_task = PythonOperator(
        task_id="update_tasks",
        python_callable=update_tasks,
        retries=2,
        trigger_rule="all_done",
        provide_context=True
    )

    create_associations_task = PythonOperator(
        task_id="create_associations",
        python_callable=create_associations,
        provide_context=True,
        trigger_rule="all_done"
    )

    collect_results_task = PythonOperator(
        task_id="collect_and_save_results",
        python_callable=collect_and_save_results,
        provide_context=True,
        trigger_rule="all_done"
    )

    compose_response_task = PythonOperator(
        task_id="compose_response_html",
        python_callable=compose_response_html,
        provide_context=True,
        trigger_rule="all_done"
    )

    send_final_email_task = PythonOperator(
        task_id="send_final_email",
        python_callable=send_final_email,
        provide_context=True,
        trigger_rule="all_done"
    )

    end_task = DummyOperator(
        task_id="end_workflow",
        trigger_rule="all_done"
    )

    # Define task dependencies
    start_task >> load_context_task >> analyze_task >> branch_task

    creation_tasks = {
        "create_contacts": create_contacts_task,
        "create_companies": create_companies_task,
        "determine_owner": determine_owner_task,
        "check_task_threshold": check_task_threshold_task,
        "create_deals": create_deals_task,
        "create_meetings": create_meetings_task,
        "create_notes": create_notes_task,
        "create_tasks": create_tasks_task,
        "update_contacts": update_contacts_task,
        "update_companies": update_companies_task,
        "update_deals": update_deals_task,
        "update_meetings": update_meetings_task,
        "update_notes": update_notes_task,
        "update_tasks": update_tasks_task
    }

    branch_task >> [task for task in creation_tasks.values()]

    for task in creation_tasks.values():
        task >> create_associations_task
    
    branch_task >> create_associations_task
    create_associations_task >> collect_results_task >> compose_response_task >> send_final_email_task >> end_task
