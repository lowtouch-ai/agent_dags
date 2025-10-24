import logging
import json
from datetime import datetime, timedelta
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
    "retries": 1,
    "retry_delay": timedelta(seconds=15),
}

HUBSPOT_FROM_ADDRESS = Variable.get("HUBSPOT_FROM_ADDRESS")
GMAIL_CREDENTIALS = Variable.get("HUBSPOT_GMAIL_CREDENTIALS")
OLLAMA_HOST = "http://agentomatic:8000/"
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
        if expect_json:
            return json.dumps({"error": str(e)})
        return f"<html><body>Error processing AI request: {str(e)}</body></html>"

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
    """Analyze user's message to determine intent and entities using conversation history"""
    chat_history = ti.xcom_pull(key="chat_history", default=[])
    thread_history = ti.xcom_pull(key="thread_history", default=[])
    thread_id = ti.xcom_pull(key="thread_id")
    latest_user_message = ti.xcom_pull(key="latest_message", default="")
    if not thread_id:
        logging.error("No thread_id provided")
        default_result = {
            "status": "error",
            "error_message": "No thread_id provided",
            "user_intent": "ERROR",
            "confidence_level": "low",
            "entities_to_create": {},
            "entities_to_update": {},
            "selected_entities": {},
            "tasks_to_execute": ["create_associations", "compose_response_html", "collect_and_save_results", "send_final_email"]
        }
        ti.xcom_push(key="analysis_results", value=default_result)
        return default_result
    
    # Build complete conversation context
    conversation_context = ""

    
    # Add thread history (email messages with metadata)
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
            "confidence_level": "low",
            "entities_to_create": {},
            "entities_to_update": {},
            "selected_entities": {},
            "tasks_to_execute": ["compose_response_html", "collect_and_save_results", "send_final_email"]
        }
        ti.xcom_push(key="analysis_results", value=default_result)
        logging.info(f"Analysis results pushed to XCom: {default_result}")
        return default_result
    
    # Get latest user message for context
    sender_email = ""
    sender_name = ""
    for email in reversed(thread_history):
        if not email.get('from_bot', True):
            if email.get("content"):
                soup = BeautifulSoup(email.get("content", ""), "html.parser")
                sender_email = email.get('headers', {}).get('From', '')
                # Extract name from "Name <email>" format
                import re
                match = re.match(r'(.+?)\s*<(.+?)>', sender_email)
                if match:
                    sender_name = match.group(1).strip()
                    sender_email = match.group(2).strip()
                break
    
    # CRITICAL: Extract entities from conversation history, NOT search
    prompt = f"""You are an AI assistant analyzing an email conversation to determine user intent and extract HubSpot entities FROM THE CONVERSATION HISTORY ONLY.

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

ENTITY EXTRACTION RULES:
1. **Existing Entities**: Look for tables in bot messages labeled "Existing Contact Details", "Existing Company Details", "Existing Deal Details"
   - Extract: contactId, firstname, lastname, email, phone, address, jobtitle
   - Extract: companyId, name, domain, address, city, state, zip, country, phone, description, type
   - Extract: dealId, dealName, dealLabelName (stage), dealAmount, closeDate, dealOwnerName

2. **Proposed New Entities**: Look for tables labeled "Objects to be Created" with subsections for "New Contacts", "New Companies", "New Deals", "Notes", "Tasks", "Meeting Details"
   - Extract all fields from these tables exactly as shown

3. **User Intent Detection**:
   - "CONFIRM": User agrees (e.g., "proceed", "looks good", "yes", "correct", "go ahead")
   - "MODIFY": User requests changes (e.g., "change the date", "update the amount", "fix the name")
   - "CREATE_NEW": User wants new entities ignoring existing (e.g., "create new contact", "don't use existing")
   - "SELECT_SPECIFIC": User selects specific entities (e.g., "use the first contact", "select John from Acme")
   - "CLARIFY": User needs clarification or provides unclear instructions
   - "CANCEL": User wants to stop (e.g., "cancel", "never mind", "stop")

4. **Selection Logic**:
   - If user says "proceed" or "confirm" without specifying → Include ALL existing entities in `selected_entities` and ALL proposed new entities in `entities_to_create`
   - If user specifies particular entities (by name, ID, or position) → Include only those in `selected_entities`.
   - If user specifies any particular entities (by name, ID, or position) to be excluded → exclude only those and include others in `selected_entities`.
   - If user requests modifications → Include modified versions in `entities_to_update`
   - If user mentions casual comments about clients/deals → Add as notes in `entities_to_create`

5. **Casual Comment Handling**:
   - If the latest message is a casual comment (opinion, feedback, observation) with NO action requests:
     - Set intent to "CASUAL_COMMENT"
     - Create a note with the comment text
     - Include speaker name and email from SENDER INFO
     - Use current timestamp
     - DO NOT create contacts, companies, deals, tasks, or meetings
     - Set casual_comments_detected to true
   - Examples of casual comments:
     * "It was great to have this deal and I think its an interesting one"
     * "This client is really engaged"
     * "Looking forward to working with them"
     * "Great progress on this deal"


6. **Secondary Entities (meetings, notes, tasks)**:
   - For CASUAL_COMMENT intent: Only create the note with the comment, nothing else
   - If user confirms without modifications → Include all proposed secondary entities in `entities_to_create`
   - If user modifies primary entities only → Still include all secondary entities unless explicitly excluded
   - If user says "no tasks" or "skip the meeting" → Exclude those specific types

Return ONLY valid JSON:
{{
    "user_intent": "CONFIRM|MODIFY|CREATE_NEW|SELECT_SPECIFIC|CLARIFY|CANCEL",
    "confidence_level": "high|medium|low",
    "selected_entities": {{
        "contacts": [{{"contactId": "...", "firstname": "...", "lastname": "...", "email": "...", "phone": "...", "address": "...", "jobtitle": "..."}}],
        "companies": [{{"companyId": "...", "name": "...", "domain": "...", "address": "...", "city": "...", "state": "...", "zip": "...", "country": "...", "phone": "...", "description": "...", "type": "..."}}],
        "deals": [{{"dealId": "...", "dealName": "...", "dealLabelName": "...", "dealAmount": "...", "closeDate": "...", "dealOwnerName": "..."}}]
    }},
    "entities_to_create": {{
        "contacts": [{{"firstname": "...", "lastname": "...", "email": "...", "phone": "...", "address": "...", "jobtitle": "..."}}],
        "companies": [{{"name": "...", "domain": "...", "address": "...", "city": "...", "state": "...", "zip": "...", "country": "...", "phone": "...", "description": "...", "type": "..."}}],
        "deals": [{{"dealName": "...", "dealLabelName": "...", "dealAmount": "...", "closeDate": "...", "dealOwnerName": "..."}}],
        "meetings": [{{"meeting_title": "...", "start_time": "...", "end_time": "...", "location": "...", "outcome": "...", "timestamp": "...", "attendees": [], "meeting_type": "...", "meeting_status": "..."}}],
        "notes": [{{"note_content": "...", "timestamp": "...", "note_type": "...", "speaker_name": "{{sender_name}}", "speaker_email": "{{sender_email}}"}}],
        "tasks": [{{"task_details": "...", "task_owner_name": "...", "task_owner_id": "...", "due_date": "...", "priority": "...", "task_index": 1}}]
    }},
    "entities_to_update": {{
        "contacts": [{{"contactId": "...", "updates": {{"field": "new_value"}}}}],
        "companies": [{{"companyId": "...", "updates": {{"field": "new_value"}}}}],
        "deals": [{{"dealId": "...", "updates": {{"field": "new_value"}}}}],
        "meetings": [],
        "notes": [],
        "tasks": [{{"taskId": "...", "taskbody": "...", "task_owner_name": "...", "task_owner_id": "...", "updates": {{"field": "new_value"}}}}]
    }},
    "casual_comments_detected": true|false,
    "reasoning": "Brief explanation of intent, selections, and entity actions. For casual comments, explain why it's just a comment and not an action request."
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

    response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
    logging.info(f"Prompt is :{prompt}")
    logging.info(f"conversation_history is :{chat_history}")
    try:
        parsed_analysis = json.loads(response)
        user_intent = parsed_analysis.get("user_intent", "CONFIRM")
        confidence_level = parsed_analysis.get("confidence_level", "medium")
        entities_to_create = parsed_analysis.get("entities_to_create", {})
        entities_to_update = parsed_analysis.get("entities_to_update", {})
        selected_entities = parsed_analysis.get("selected_entities", {})
        
        # Determine which tasks to execute
        tasks_to_execute = []
        should_determine_owner = False
        should_check_task_threshold = False
        if user_intent == "CASUAL_COMMENT":
            # For casual comments, only create notes and associations
            if entities_to_create.get("notes"):
                tasks_to_execute.append("create_notes")
            tasks_to_execute.extend(["create_associations", "compose_response_html", "collect_and_save_results", "send_final_email"])
            
        elif user_intent in ["MODIFY", "CONFIRM", "SELECT_SPECIFIC", "CREATE_NEW"]:
            # Check for updates first
            if entities_to_update.get("tasks"):
                tasks_to_execute.append("update_tasks")
            if entities_to_update.get("contacts"):
                tasks_to_execute.append("update_contacts")
            if entities_to_update.get("companies"):
                tasks_to_execute.append("update_companies")
            if entities_to_update.get("deals"):
                tasks_to_execute.append("update_deals")
            if entities_to_update.get("meetings"):
                tasks_to_execute.append("update_meetings")
            if entities_to_update.get("notes"):
                tasks_to_execute.append("update_notes")
            
            # Check for creates
            if entities_to_create.get("contacts"):
                tasks_to_execute.append("create_contacts")
            if entities_to_create.get("companies"):
                tasks_to_execute.append("create_companies")
            if entities_to_create.get("deals"):
                tasks_to_execute.append("create_deals")
                should_determine_owner = True
            if entities_to_create.get("meetings"):
                tasks_to_execute.append("create_meetings")
            if entities_to_create.get("notes"):
                tasks_to_execute.append("create_notes")
            if entities_to_create.get("tasks"):
                tasks_to_execute.append("create_tasks")
                should_determine_owner = True
                should_check_task_threshold = True
                
            tasks_to_execute.extend(["create_associations", "compose_response_html", "collect_and_save_results", "send_final_email"])
        elif user_intent == "CLARIFY":
            tasks_to_execute = ["compose_response_html", "collect_and_save_results", "send_final_email"]
        elif user_intent == "CANCEL":
            tasks_to_execute = ["compose_response_html", "collect_and_save_results", "send_final_email"]
        else:
            tasks_to_execute = ["create_associations", "compose_response_html", "collect_and_save_results", "send_final_email"]
        
        # Insert owner determination and threshold checks at the beginning if needed
        if should_check_task_threshold:
            tasks_to_execute.insert(0, "check_task_threshold")
        if should_determine_owner:
            tasks_to_execute.insert(0, "determine_owner")
        
        results = {
            "status": "success",
            "user_intent": user_intent,
            "confidence_level": confidence_level,
            "entities_to_create": entities_to_create,
            "entities_to_update": entities_to_update,
            "selected_entities": selected_entities,
            "reasoning": parsed_analysis.get("reasoning", ""),
            "tasks_to_execute": tasks_to_execute,
            "should_determine_owner": should_determine_owner,
            "should_check_task_threshold": should_check_task_threshold,
            "casual_comments_detected": parsed_analysis.get("casual_comments_detected", False)
        }
        
        logging.info(f"Analysis completed: Intent={user_intent}, Confidence={confidence_level}")
        logging.info(f"Tasks to execute: {tasks_to_execute}")
        logging.info(f"Selected entities: Contacts={len(selected_entities.get('contacts', []))}, "
                    f"Companies={len(selected_entities.get('companies', []))}, "
                    f"Deals={len(selected_entities.get('deals', []))}")
        logging.info(f"Entities to create: Contacts={len(entities_to_create.get('contacts', []))}, "
                    f"Companies={len(entities_to_create.get('companies', []))}, "
                    f"Deals={len(entities_to_create.get('deals', []))}, "
                    f"Meetings={len(entities_to_create.get('meetings', []))}, "
                    f"Notes={len(entities_to_create.get('notes', []))}, "
                    f"Tasks={len(entities_to_create.get('tasks', []))}")
        
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse AI analysis: {e}")
        logging.error(f"Raw AI response: {response}")
        
        # Fallback analysis
        user_intent = "CONFIRM"
        if "create new" in latest_user_message.lower():
            user_intent = "CREATE_NEW"
        elif any(keyword in latest_user_message.lower() for keyword in ["modify", "change", "update", "correct"]):
            user_intent = "MODIFY"
        
        results = {
            "status": "error",
            "error_message": f"Failed to parse AI analysis: {str(e)}",
            "user_intent": user_intent,
            "confidence_level": "low",
            "entities_to_create": {},
            "entities_to_update": {},
            "selected_entities": {},
            "reasoning": "Fallback analysis due to AI parsing error",
            "tasks_to_execute": ["create_associations", "compose_response_html", "collect_and_save_results", "send_final_email"],
            "should_determine_owner": False,
            "should_check_task_threshold": False,
            "casual_comments_detected": False
        }
    
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

1. Parse the Deal Owner and Task Owners from the email thread.
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
5. Return a list of task owners with their validation details for ALL tasks to be created.

Return this exact JSON structure:
{{
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

# CREATE FUNCTIONS (abbreviated - follow same pattern)
def create_contacts(ti, **context):
    analysis_results = ti.xcom_pull(key="analysis_results")
    to_create_contacts = analysis_results.get("entities_to_create", {}).get("contacts", [])
    
    if not to_create_contacts:
        logging.info("No contacts to create")
        ti.xcom_push(key="created_contacts", value=[])
        ti.xcom_push(key="contacts_errors", value=[])
        return []
    
    prompt = f"""Create contacts in HubSpot.

Contact Details to Create:
{json.dumps(to_create_contacts, indent=2)}

Steps:
1. For each contact, invoke create_contact tool with the provided properties
2. Return the created contact ID and all properties

Return ONLY this JSON structure (no other text):
{{
    "created_contacts": [
        {{
            "id": "contact_id_from_api",
            "details": {{
                "firstname": "value",
                "lastname": "value",
                "email": "value",
                "phone": "value",
                "address": "value",
                "jobtitle": "value"
            }}
        }}
    ],
    "errors": []
}}"""

    response = get_ai_response(prompt, expect_json=True)
    
    try:
        parsed = json.loads(response)
        created = parsed.get("created_contacts", [])
        errors = parsed.get("errors", [])
        ti.xcom_push(key="created_contacts", value=created)
        ti.xcom_push(key="contacts_errors", value=errors)
        logging.info(f"Created {len(created)} contacts")
        return created
    except Exception as e:
        logging.error(f"Error creating contacts: {e}")
        logging.error(f"Raw response: {response}")
        ti.xcom_push(key="created_contacts", value=[])
        ti.xcom_push(key="contacts_errors", value=[str(e)])
        return []

def create_companies(ti, **context):
    analysis_results = ti.xcom_pull(key="analysis_results")
    to_create_companies = analysis_results.get("entities_to_create", {}).get("companies", [])
    
    if not to_create_companies:
        logging.info("No companies to create")
        ti.xcom_push(key="created_companies", value=[])
        ti.xcom_push(key="companies_errors", value=[])
        return []
    
    prompt = f"""Create companies in HubSpot.

Company Details to Create:
{json.dumps(to_create_companies, indent=2)}

Steps:
1. For each company, invoke create_company tool with the provided properties
2. Return the created company ID and all properties

Return ONLY this JSON structure (no other text):
{{
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
    "errors": []
}}"""
    
    response = get_ai_response(prompt, expect_json=True)
    
    try:
        parsed = json.loads(response)
        created = parsed.get("created_companies", [])
        errors = parsed.get("errors", [])
        ti.xcom_push(key="created_companies", value=created)
        ti.xcom_push(key="companies_errors", value=errors)
        logging.info(f"Created {len(created)} companies")
        return created
    except Exception as e:
        logging.error(f"Error creating companies: {e}")
        logging.error(f"Raw response: {response}")
        ti.xcom_push(key="created_companies", value=[])
        ti.xcom_push(key="companies_errors", value=[str(e)])
        return []

def create_deals(ti, **context):
    analysis_results = ti.xcom_pull(key="analysis_results")
    chat_history = ti.xcom_pull(key="chat_history", default=[])
    owner_info = ti.xcom_pull(key="owner_info", default={})
    to_create_deals = analysis_results.get("entities_to_create", {}).get("deals", [])
    
    if not to_create_deals:
        logging.info("No deals to create")
        ti.xcom_push(key="created_deals", value=[])
        ti.xcom_push(key="deals_errors", value=[])
        return []
    
    deal_owner_id = owner_info.get("deal_owner_id", "71346067")
    deal_owner_name = owner_info.get("deal_owner_name", "Kishore")
    
    # Add owner info to each deal
    for deal in to_create_deals:
        if not deal.get("dealOwnerName"):
            deal["dealOwnerName"] = deal_owner_name
        if not deal.get("dealOwnerId"):
            deal["dealOwnerId"] = deal_owner_id
    
    prompt = f"""Create deals in HubSpot.

Deal Details to Create:
{json.dumps(to_create_deals, indent=2)}

Deal Owner: {deal_owner_name} (ID: {deal_owner_id})

IMPORTANT: Respond with ONLY a valid JSON object. Always invoke create_deal..

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
10. Never use the hubspot owner  name for calling the api, it should always be the id.
Steps:
1. Analyze user response to extract client/partner names and deal details
2. Apply naming convention strictly for each deal
3. For each deal, invoke create_deal with the properties
4. Collect created deal id, properly formatted deal name, label name, amount, close date, owner

Return JSON:
{{
    "created_deals": [{{
        "id": "123", 
        "details": {{ 
            "dealName": "ClientName-DealPurpose",  // or "PartnerName-ClientName-DealPurpose"
            "dealLabelName": "...",
            "dealAmount": "...",
            "closeDate": "...",
            "dealOwnerName": "..."
        }}
    }}],
    "errors": ["Error message 1", "Error message 2"],
    "error": null
}}

If error, set error message and include individual errors in the errors array."""

    
    response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
    
    try:
        parsed = json.loads(response)
        created = parsed.get("created_deals", [])
        errors = parsed.get("errors", [])
        ti.xcom_push(key="created_deals", value=created)
        ti.xcom_push(key="deals_errors", value=errors)
        logging.info(f"Created {len(created)} deals")
        return created
    except Exception as e:
        logging.error(f"Error creating deals: {e}")
        logging.error(f"Raw response: {response}")
        ti.xcom_push(key="created_deals", value=[])
        ti.xcom_push(key="deals_errors", value=[str(e)])
        return []

def create_meetings(ti, **context):
    analysis_results = ti.xcom_pull(key="analysis_results")
    to_create_meetings = analysis_results.get("entities_to_create", {}).get("meetings", [])
    
    if not to_create_meetings:
        logging.info("No meetings to create")
        ti.xcom_push(key="created_meetings", value=[])
        ti.xcom_push(key="meetings_errors", value=[])
        return []
    
    prompt = f"""Create meetings in HubSpot.

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
    "errors": []
}}"""
    
    response = get_ai_response(prompt, expect_json=True)
    
    try:
        parsed = json.loads(response)
        created = parsed.get("created_meetings", [])
        errors = parsed.get("errors", [])
        ti.xcom_push(key="created_meetings", value=created)
        ti.xcom_push(key="meetings_errors", value=errors)
        logging.info(f"Created {len(created)} meetings")
        return created
    except Exception as e:
        logging.error(f"Error creating meetings: {e}")
        logging.error(f"Raw response: {response}")
        ti.xcom_push(key="created_meetings", value=[])
        ti.xcom_push(key="meetings_errors", value=[])
        return []

def create_notes(ti, **context):
    analysis_results = ti.xcom_pull(key="analysis_results")
    chat_history = ti.xcom_pull(key="chat_history", default=[])
    to_create_notes = analysis_results.get("entities_to_create", {}).get("notes", [])
    
    if not to_create_notes:
        logging.info("No notes to create")
        ti.xcom_push(key="created_notes", value=[])
        ti.xcom_push(key="notes_errors", value=[])
        return []
    
    prompt = f"""You are a HubSpot Note Creation Assistant. Your role is to **create notes in HubSpot** using the provided note details.  
**You MUST invoke the `create_notes` API for every note in the input.**  
No parsing of user intent — assume all input notes are confirmed and ready to create.

---

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
   - Required associations (if provided in input)

3. **On success per note:**
   - Capture: `id`, formatted `note_content`, `hs_lastmodifieddate`

4. **On failure per note:**
   - Capture error message in `errors` array

5. **Always return full JSON** — even if all fail.

---

**RETURN EXACTLY THIS JSON STRUCTURE:**
{{
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
    "error": null
}}

**RULES:**
- `created_notes`: Array of successfully created notes
- `errors`: Array of strings for failed creations
- `error`: `null` unless catastrophic failure
- **Always invoke API** — no skipping
- Use **UTC** for all timestamps
- **RESPOND WITH ONLY THE JSON OBJECT — NO OTHER TEXT.**
"""
    
    response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
    
    try:
        parsed = json.loads(response)
        created = parsed.get("created_notes", [])
        errors = parsed.get("errors", [])
        ti.xcom_push(key="created_notes", value=created)
        ti.xcom_push(key="notes_errors", value=errors)
        logging.info(f"Created {len(created)} notes")
        return created
    except Exception as e:
        logging.error(f"Error creating notes: {e}")
        logging.error(f"Raw response: {response}")
        ti.xcom_push(key="created_notes", value=[])
        ti.xcom_push(key="notes_errors", value=[])
        return []

def create_tasks(ti, **context):
    analysis_results = ti.xcom_pull(key="analysis_results")
    owner_info = ti.xcom_pull(key="owner_info", default={})
    to_create_tasks = analysis_results.get("entities_to_create", {}).get("tasks", [])
    
    if not to_create_tasks:
        logging.info("No tasks to create")
        ti.xcom_push(key="created_tasks", value=[])
        ti.xcom_push(key="tasks_errors", value=[])
        return []
    
    task_owners = owner_info.get("task_owners", [])
    
    # Map task owners to tasks - CRITICAL: Ensure each task has correct owner
    for idx, task in enumerate(to_create_tasks, 1):
        matching_owner = next((owner for owner in task_owners if owner.get("task_index") == idx), None)
        if matching_owner:
            task["task_owner_id"] = matching_owner.get("task_owner_id", "71346067")
            task["task_owner_name"] = matching_owner.get("task_owner_name", "Kishore")
        else:
            # If no matching owner found, keep existing values or use defaults
            if "task_owner_id" not in task:
                task["task_owner_id"] = "71346067"
            if "task_owner_name" not in task:
                task["task_owner_name"] = "Kishore"
    
    # Log the tasks with their assigned owners for debugging
    logging.info(f"Tasks with mapped owners: {json.dumps(to_create_tasks, indent=2)}")
    
    prompt = f"""Create tasks in HubSpot.

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
    "errors": []
}}

CRITICAL: Preserve the task_owner_id from the input. Do not default to Kishore (71346067) unless explicitly specified."""
    
    response = get_ai_response(prompt, expect_json=True)
    
    try:
        parsed = json.loads(response)
        created = parsed.get("created_tasks", [])
        errors = parsed.get("errors", [])
        
        # Verify owners were assigned correctly
        for task in created:
            task_index = task.get("details", {}).get("task_index")
            original_task = next((t for t in to_create_tasks if t.get("task_index") == task_index), None)
            if original_task:
                expected_owner_id = original_task.get("task_owner_id")
                actual_owner_id = task.get("details", {}).get("task_owner_id")
                if expected_owner_id != actual_owner_id:
                    logging.warning(f"Task {task_index}: Expected owner {expected_owner_id}, got {actual_owner_id}")
        
        ti.xcom_push(key="created_tasks", value=created)
        ti.xcom_push(key="tasks_errors", value=errors)
        logging.info(f"Created {len(created)} tasks with owners: {[(t.get('details', {}).get('task_owner_name'), t.get('details', {}).get('task_index')) for t in created]}")
        return created
    except Exception as e:
        logging.error(f"Error creating tasks: {e}")
        logging.error(f"Raw response: {response}")
        ti.xcom_push(key="created_tasks", value=[])
        ti.xcom_push(key="tasks_errors", value=[str(e)])
        return []

# UPDATE FUNCTIONS (abbreviated - follow same pattern)
def update_contacts(ti, **context):
    analysis_results = ti.xcom_pull(key="analysis_results")
    to_update = analysis_results.get("entities_to_update", {}).get("contacts", [])
    if not to_update:
        ti.xcom_push(key="updated_contacts", value=[])
        return []
    prompt = f"""Update contacts: {json.dumps(to_update, indent=2)}
    IMPORTANT: Respond with ONLY a valid JSON object.

Steps:
1. For each contact, invoke update_contact with the id and changes.
2. Collect the updated IDs and details in tabular format. If any details missing, leave it blank in table.

Return JSON:
{{
    "updated_contacts": [{{"id": "123", "details": {{ "firstname": "...", "lastname": "...", "email": "...", "phone": "...", "address": "...", "jobtitle": "..."}}}} ...],
    "errors": ["Error message 1", "Error message 2"],
    "error": null
}}

If error, set error message and include individual errors in the errors array."""

    response = get_ai_response(prompt, expect_json=True)
    try:
        parsed = json.loads(response)
        updated = parsed.get("updated_contacts", [])
        ti.xcom_push(key="updated_contacts", value=updated)
        return updated
    except Exception as e:
        ti.xcom_push(key="updated_contacts", value=[])
        return []

def update_companies(ti, **context):
    analysis_results = ti.xcom_pull(key="analysis_results")
    to_update = analysis_results.get("entities_to_update", {}).get("companies", [])
    if not to_update:
        ti.xcom_push(key="updated_companies", value=[])
        return []
    prompt = f"""Update companies: {json.dumps(to_update, indent=2)}
    IMPORTANT: Respond with ONLY a valid JSON object.

Steps:
1. For each company, invoke update_company with the id and changes.
2. Collect the updated company id, company name, domain, state, city, country, phone, type and display in tabular format. If any details not found, show as blank in table.

Return JSON:
{{
    "updated_companies": [{{"id": "123", "details": {{ "name": "...", "domain": "...", "address": "...", "city": "...", "state": "...", "zip": "...", "country": "...", "phone": "...", "description": "...", "type": "..."}}}} ...],
    "errors": ["Error message 1", "Error message 2"],
    "error": null
}}

If error, set error message and include individual errors in the errors array."""
    response = get_ai_response(prompt, expect_json=True)
    try:
        parsed = json.loads(response)
        ti.xcom_push(key="updated_companies", value=parsed.get("updated_companies", []))
        return parsed.get("updated_companies", [])
    except:
        ti.xcom_push(key="updated_companies", value=[])
        return []

def update_deals(ti, **context):
    analysis_results = ti.xcom_pull(key="analysis_results")
    to_update = analysis_results.get("entities_to_update", {}).get("deals", [])
    if not to_update:
        ti.xcom_push(key="updated_deals", value=[])
        return []
    prompt = f"""Update deals: {json.dumps(to_update, indent=2)}
    IMPORTANT: Respond with ONLY a valid JSON object.

Steps:
1. For each deal, invoke update_deal with the id and changes.
2. Collect the updated deal id, deal name, deal label name, close date, deal owner name in tabular format. If any details not found, show as blank in table.

Return JSON:
{{
    "updated_deals": [{{"id": "123", "details": {{ "dealName": "...", "dealLabelName": "...", "dealAmount": "...", "closeDate": "...", "dealOwnerName": "..."}}}} ...],
    "errors": ["Error message 1", "Error message 2"],
    "error": null
}}

If error, set error message and include individual errors in the errors array."""
    response = get_ai_response(prompt, expect_json=True)
    try:
        parsed = json.loads(response)
        ti.xcom_push(key="updated_deals", value=parsed.get("updated_deals", []))
        return parsed.get("updated_deals", [])
    except:
        ti.xcom_push(key="updated_deals", value=[])
        return []

def update_meetings(ti, **context):
    analysis_results = ti.xcom_pull(key="analysis_results")
    to_update = analysis_results.get("entities_to_update", {}).get("meetings", [])
    if not to_update:
        ti.xcom_push(key="updated_meetings", value=[])
        return []
    prompt = f"""Update meetings: {json.dumps(to_update, indent=2)}
    IMPORTANT: Respond with ONLY a valid JSON object.

Steps:
1. For each meeting, invoke update_meeting with the id and changes.
2. Collect the updated ID, Title, Start Time (EST), End Time (EST), Location, Outcome in tabular format.
3. Always use `hs_timestamp` in YYYY-MM-DDTHH:MM:SSZ format while updating meetings.
Return JSON:
{{
    "updated_meetings": [{{"id": "123", "details": {{ "meeting_title": "...", "start_time": "...", "end_time": "...", "location": "...", "outcome": "...", "timestamp": "...", "attendees": [], "meeting_type": "..."}}}} ...],
    "errors": ["Error message 1", "Error message 2"],
    "error": null
}}

If error, set error message and include individual errors in the errors array."""

    response = get_ai_response(prompt, expect_json=True)
    try:
        parsed = json.loads(response)
        ti.xcom_push(key="updated_meetings", value=parsed.get("updated_meetings", []))
        return parsed.get("updated_meetings", [])
    except:
        ti.xcom_push(key="updated_meetings", value=[])
        return []

def update_notes(ti, **context):
    analysis_results = ti.xcom_pull(key="analysis_results")
    to_update = analysis_results.get("entities_to_update", {}).get("notes", [])
    if not to_update:
        ti.xcom_push(key="updated_notes", value=[])
        return []
    prompt = f"""Update notes: {json.dumps(to_update, indent=2)}
    IMPORTANT: Respond with ONLY a valid JSON object.

Steps:
1. For each note, invoke update_note with the id and changes.
2. Collect the updated Note id, Note body, last modified date in tabular format.
3. Always use `hs_timestamp` in YYYY-MM-DDTHH:MM:SSZ format while updating notes.
Return JSON:
{{
    "updated_notes": [{{"id": "123", "details": {{ "note_content": "...", "timestamp": "..."}}}} ...],
    "errors": ["Error message 1", "Error message 2"],
    "error": null
}}

If error, set error message and include individual errors in the errors array."""

    response = get_ai_response(prompt, expect_json=True)
    try:
        parsed = json.loads(response)
        ti.xcom_push(key="updated_notes", value=parsed.get("updated_notes", []))
        return parsed.get("updated_notes", [])
    except:
        ti.xcom_push(key="updated_notes", value=[])
        return []

def update_tasks(ti, **context):
    analysis_results = ti.xcom_pull(key="analysis_results")
    owner_info = ti.xcom_pull(key="owner_info", default={})
    to_update = analysis_results.get("entities_to_update", {}).get("tasks", [])
    if not to_update:
        ti.xcom_push(key="updated_tasks", value=[])
        return []
    
    # Get current task details to preserve original information
    selected_entities = analysis_results.get("selected_entities", {})
    current_tasks = selected_entities.get("tasks", [])
    
    # Get task owners from owner_info (corrected owners)
    task_owners = owner_info.get("task_owners", [])
    
    # Build task details map with corrected owner information
    task_details_map = {}
    for task in current_tasks:
        task_id = task.get("taskId") or task.get("id")
        if task_id:
            task_details_map[task_id] = task
    
    # Map corrected owners to tasks being updated
    for task_update in to_update:
        task_id = task_update.get("taskId") or task_update.get("id")
        if task_id and task_id in task_details_map:
            # Get task_index from the original task
            original_task = task_details_map[task_id]
            task_index = original_task.get("task_index")
            
            # Find matching corrected owner
            if task_index:
                matching_owner = next((owner for owner in task_owners if owner.get("task_index") == task_index), None)
                if matching_owner:
                    # Add corrected owner info to the update
                    task_update["task_owner_id"] = matching_owner.get("task_owner_id", "71346067")
                    task_update["task_owner_name"] = matching_owner.get("task_owner_name", "Kishore")
                else:
                    # Use existing owner info if no corrected owner found
                    task_update["task_owner_id"] = original_task.get("task_owner_id", "71346067")
                    task_update["task_owner_name"] = original_task.get("task_owner_name", "Kishore")
    
    prompt = f"""Update tasks in HubSpot.

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
4. Use the task's existing owner ID from the original task details.
5. Use the task's existing owner id and invoke get_all_owners to get the owners name.
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

Return ONLY this JSON format:
{{
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
  "error": null
}}"""


    
    response = get_ai_response(prompt, expect_json=True)
    try:
        parsed = json.loads(response)
        updated = parsed.get("updated_tasks", [])
        
        # Validate and restore missing details
        for task in updated:
            task_id = task.get("id")
            if task_id in task_details_map:
                original = task_details_map[task_id]
                details = task.get("details", {})
                
                # Preserve task description if not updated
                if not details.get("task_details"):
                    details["task_details"] = original.get("task_details", "")
                
                # Ensure task_index is preserved
                if not details.get("task_index"):
                    details["task_index"] = original.get("task_index")
        
        # Log owner assignments for debugging
        for task in updated:
            task_index = task.get("details", {}).get("task_index")
            owner_name = task.get("details", {}).get("task_owner_name")
            owner_id = task.get("details", {}).get("task_owner_id")
            logging.info(f"Updated task {task_index}: Assigned to {owner_name} (ID: {owner_id})")
        
        ti.xcom_push(key="updated_tasks", value=updated)
        return updated
    except Exception as e:
        logging.error(f"Error updating tasks: {e}")
        logging.error(f"Raw response: {response}")
        ti.xcom_push(key="updated_tasks", value=[])
        return []

def create_associations(ti, **context):
    analysis_results = ti.xcom_pull(key="analysis_results")
    search_results = ti.xcom_pull(key="search_results", default={})
    chat_history = ti.xcom_pull(key="chat_history", default=[])
    thread_history = ti.xcom_pull(key="thread_history", default=[])
    latest_user_message = ti.xcom_pull(key="latest_message", default="")
    
    # Get all created entities
    created_contacts = ti.xcom_pull(key="created_contacts", default=[])
    created_companies = ti.xcom_pull(key="created_companies", default=[])
    created_deals = ti.xcom_pull(key="created_deals", default=[])
    created_meetings = ti.xcom_pull(key="created_meetings", default=[])
    created_notes = ti.xcom_pull(key="created_notes", default=[])
    created_tasks = ti.xcom_pull(key="created_tasks", default=[])
    
    # Get updated entities
    updated_contacts = ti.xcom_pull(key="updated_contacts", default=[])
    updated_companies = ti.xcom_pull(key="updated_companies", default=[])
    updated_deals = ti.xcom_pull(key="updated_deals", default=[])
    
    # Get selected existing entities from analysis
    selected_entities = analysis_results.get("selected_entities", {})
    existing_contact_ids = [str(c.get("contactId")) for c in selected_entities.get("contacts", [])]
    existing_company_ids = [str(c.get("companyId")) for c in selected_entities.get("companies", [])]
    existing_deal_ids = [str(d.get("dealId")) for d in selected_entities.get("deals", [])]
    
    # FIXED: Collect ALL relevant IDs (new, updated, AND existing)
    new_contact_ids = [c.get("id") for c in created_contacts]
    new_company_ids = [c.get("id") for c in created_companies]
    new_deal_ids = [d.get("id") for d in created_deals]
    new_meeting_ids = [m.get("id") for m in created_meetings]
    new_note_ids = [n.get("id") for n in created_notes]
    new_task_ids = [t.get("id") for t in created_tasks]
    
    updated_contact_ids = [c.get("id") for c in updated_contacts]
    updated_company_ids = [c.get("id") for c in updated_companies]
    updated_deal_ids = [d.get("id") for d in updated_deals]
    
    # FIXED: Combine all IDs instead of using OR logic
    all_contact_ids = list(set(new_contact_ids + updated_contact_ids + existing_contact_ids))
    all_company_ids = list(set(new_company_ids + updated_company_ids + existing_company_ids))
    all_deal_ids = list(set(new_deal_ids + updated_deal_ids + existing_deal_ids))
    
    logging.info(f"All Contact IDs for association: {all_contact_ids}")
    logging.info(f"All Company IDs for association: {all_company_ids}")
    logging.info(f"All Deal IDs for association: {all_deal_ids}")
    logging.info(f"New Note IDs: {new_note_ids}")
    logging.info(f"New Task IDs: {new_task_ids}")
    logging.info(f"New Meeting IDs: {new_meeting_ids}")
    
    # Build conversation context
    conversation_context = ""
    for msg in chat_history:
        role = msg.get("role", "unknown")
        content = msg.get("content", "")
        conversation_context += f"[{role.upper()}]: {content}\n\n"
    
    for idx, email in enumerate(thread_history, 1):
        content = email.get("content", "").strip()
        if content:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(content, "html.parser")
            clean_content = soup.get_text(separator=" ", strip=True)
            sender = email['headers'].get('From', 'Unknown')
            is_from_bot = email.get('from_bot', False)
            role_label = "BOT" if is_from_bot else "USER"
            conversation_context += f"[{role_label} EMAIL {idx} - From: {sender}]: {clean_content}\n\n"
    
    # FIXED: Enhanced prompt with clearer instructions
    prompt = f"""You are a HubSpot API assistant responsible for creating associations between entities using create_multi_association tool.

FULL CHAT HISTORY:
{conversation_context}

LATEST USER MESSAGE:
{latest_user_message}

How to create associations: Always and strictly call create_multi_association API/Tool to create association.

AVAILABLE ENTITY IDS:
- NEW Contact IDs (just created): {new_contact_ids}
- NEW Company IDs (just created): {new_company_ids}
- NEW Deal IDs (just created): {new_deal_ids}
- NEW Meeting IDs (just created): {new_meeting_ids}
- NEW Note IDs (just created): {new_note_ids}
- NEW Task IDs (just created): {new_task_ids}
- EXISTING Contact IDs (from conversation): {existing_contact_ids}
- EXISTING Company IDs (from conversation): {existing_company_ids}
- EXISTING Deal IDs (from conversation): {existing_deal_ids}
CRITICAL ASSOCIATION RULES:

1. **Associate NEW entities with EXISTING entities**:
   - If a new note/task/meeting is created, associate it with ALL relevant existing contacts/companies/deals from the conversation
   - If a new contact is created, associate it with existing companies/deals if mentioned

2. **Extract Additional IDs from Conversation**:
   - Look for entity IDs in HTML tables from bot responses
   - Look for IDs mentioned by user (e.g., "associate with deal 12345")
   - Common patterns: "contactId: 12345", "Deal ID: 67890", "associate with contact 99999"

3. **Create Comprehensive Associations**:
   - New notes should be associated with ALL relevant entities (both new and existing)
   - New tasks should be associated with relevant deals/contacts/companies
   - Don't just associate new with new - connect to the full context

4. **Association Priority**:
   - If user mentions specific entities to associate → Use those
   - If context implies relationships → Create those associations
   - If a note mentions people/companies → Associate with their records

EXAMPLE SCENARIOS:

Scenario 1: New note created about existing deal
- Existing Deal ID: 11223344 (from conversation)
- New Note ID: 234458809069
- Action: Associate note with deal

Scenario 2: New task created for existing contact
- Existing Contact ID: 556677 (from conversation)
- New Task ID: 998877
- Action: Associate task with contact

**IMPORTANT**: 
    - For each new entity created, think about what existing entities it should be connected to based on the conversation context.
    - You can only create asssociation using tool `create_multi_association`
Return ONLY valid JSON with multiple association requests if needed:
{{
    "association_requests": [
        {{
            "description": "What this association is for",
            "single": {{
                "deal_id": "existing_or_new_deal_id",
                "contact_id": "existing_or_new_contact_id",
                "company_id": "existing_or_new_company_id",
                "note_id": "new_note_id",
                "task_id": "new_task_id",
                "meeting_id": "new_meeting_id"
            }}
        }}
    ],
    "reasoning": "Explain why these associations make sense",
    "extracted_ids_from_conversation": {{
        "contact_ids": [],
        "company_ids": [],
        "deal_ids": []
    }},
    "errors": [],
    "error": null
}}

Remember: Empty string "" for non-applicable fields, comma-separated for multiple IDs.
"""
    
    response = get_ai_response(prompt, expect_json=True)
    
    try:
        parsed = json.loads(response)
        associations = parsed.get("association_requests", [])
        extracted_ids = parsed.get("extracted_ids_from_conversation", {})
        reasoning = parsed.get("reasoning", "")
        
        # Log for debugging
        if reasoning:
            logging.info(f"Association reasoning: {reasoning}")
        if extracted_ids and any(extracted_ids.values()):
            logging.info(f"Additional IDs extracted from conversation: {extracted_ids}")
        
        # FIXED: Ensure we're creating associations even if no new associations are suggested
        # This handles cases where the AI might not recognize the need for associations
        if not associations and (new_note_ids or new_task_ids or new_meeting_ids):
            logging.warning("No associations suggested by AI, creating default associations")
            
            # Create default associations for new engagement objects with existing entities
            default_associations = []
            
            # Associate new notes with existing entities
            for note_id in new_note_ids:
                if all_contact_ids or all_company_ids or all_deal_ids:
                    default_associations.append({
                        "description": "Associate new note with existing entities",
                        "single": {
                            "note_id": note_id,
                            "contact_id": ",".join(all_contact_ids) if all_contact_ids else "",
                            "company_id": ",".join(all_company_ids) if all_company_ids else "",
                            "deal_id": ",".join(all_deal_ids) if all_deal_ids else "",
                            "task_id": "",
                            "meeting_id": ""
                        }
                    })
            
            # Associate new tasks with existing entities
            for task_id in new_task_ids:
                if all_contact_ids or all_company_ids or all_deal_ids:
                    default_associations.append({
                        "description": "Associate new task with existing entities",
                        "single": {
                            "task_id": task_id,
                            "contact_id": ",".join(all_contact_ids) if all_contact_ids else "",
                            "company_id": ",".join(all_company_ids) if all_company_ids else "",
                            "deal_id": ",".join(all_deal_ids) if all_deal_ids else "",
                            "note_id": "",
                            "meeting_id": ""
                        }
                    })
            
            # Associate new meetings with existing entities
            for meeting_id in new_meeting_ids:
                if all_contact_ids or all_company_ids or all_deal_ids:
                    default_associations.append({
                        "description": "Associate new meeting with existing entities",
                        "single": {
                            "meeting_id": meeting_id,
                            "contact_id": ",".join(all_contact_ids) if all_contact_ids else "",
                            "company_id": ",".join(all_company_ids) if all_company_ids else "",
                            "deal_id": ",".join(all_deal_ids) if all_deal_ids else "",
                            "note_id": "",
                            "task_id": ""
                        }
                    })
            
            if default_associations:
                associations = default_associations
                logging.info(f"Created {len(default_associations)} default associations")
        
        ti.xcom_push(key="associations_created", value=associations)
        ti.xcom_push(key="extracted_conversation_ids", value=extracted_ids)
        logging.info(f"Total associations to create: {len(associations)}")
        
        # Log each association for debugging
        for idx, assoc in enumerate(associations):
            logging.info(f"Association {idx + 1}: {assoc}")
        
        return associations
        
    except Exception as e:
        logging.error(f"Error creating associations: {e}")
        logging.error(f"Raw AI response: {response}")
        ti.xcom_push(key="associations_created", value=[])
        return []

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
    owner_info = ti.xcom_pull(key="owner_info", default={})
    task_threshold_info = ti.xcom_pull(key="task_threshold_info", default={})
    
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
    if existing_deals or updated_deals:
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
        for deal in existing_deals:
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
        <p>Best regards,<br>
        HubSpot Agent<br>
        hubspot-agent-9201@lowtouch.ai</p>
    </div>
</body>
</html>"""
    
    ti.xcom_push(key="response_html", value=email_content)
    logging.info(f"Composed response HTML for thread {thread_id}")
    
    return email_content

def send_final_email(ti, **context):
    """Send final completion email with proper recipient handling"""
    import re
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
        provide_context=True
    )

    create_companies_task = PythonOperator(
        task_id="create_companies",
        python_callable=create_companies,
        provide_context=True
    )

    create_deals_task = PythonOperator(
        task_id="create_deals",
        python_callable=create_deals,
        provide_context=True
    )

    create_meetings_task = PythonOperator(
        task_id="create_meetings",
        python_callable=create_meetings,
        provide_context=True
    )

    create_notes_task = PythonOperator(
        task_id="create_notes",
        python_callable=create_notes,
        provide_context=True
    )

    create_tasks_task = PythonOperator(
        task_id="create_tasks",
        python_callable=create_tasks,
        provide_context=True
    )

    update_contacts_task = PythonOperator(
        task_id="update_contacts",
        python_callable=update_contacts,
        provide_context=True
    )

    update_companies_task = PythonOperator(
        task_id="update_companies",
        python_callable=update_companies,
        provide_context=True
    )

    update_deals_task = PythonOperator(
        task_id="update_deals",
        python_callable=update_deals,
        provide_context=True
    )

    update_meetings_task = PythonOperator(
        task_id="update_meetings",
        python_callable=update_meetings,
        provide_context=True
    )

    update_notes_task = PythonOperator(
        task_id="update_notes",
        python_callable=update_notes,
        provide_context=True
    )

    update_tasks_task = PythonOperator(
        task_id="update_tasks",
        python_callable=update_tasks,
        provide_context=True
    )

    create_associations_task = PythonOperator(
        task_id="create_associations",
        python_callable=create_associations,
        provide_context=True,
        trigger_rule="none_failed_min_one_success"
    )

    collect_results_task = PythonOperator(
        task_id="collect_and_save_results",
        python_callable=collect_and_save_results,
        provide_context=True,
        trigger_rule="none_failed_min_one_success"
    )

    compose_response_task = PythonOperator(
        task_id="compose_response_html",
        python_callable=compose_response_html,
        provide_context=True,
        trigger_rule="none_failed_min_one_success"
    )

    send_final_email_task = PythonOperator(
        task_id="send_final_email",
        python_callable=send_final_email,
        provide_context=True,
        trigger_rule="none_failed_min_one_success"
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
