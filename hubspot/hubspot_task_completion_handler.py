import base64
import logging
import json
import re
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from ollama import Client
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from hubspot_email_listener import send_fallback_email_on_failure, send_hubspot_slack_alert

# ============================================================================
# CONFIGURATION
# ============================================================================
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
def clear_retry_tracker_on_success(context):
    dag_run = context['dag_run']
    original_run_id = dag_run.conf.get('original_run_id')
    original_dag_id = dag_run.conf.get('original_dag_id', dag_run.dag_id)
    
    if not original_run_id or not dag_run.conf.get('retry_attempt'):
        return  # Not a retry run
    
    tracker_key = f"{original_dag_id}:{original_run_id}"
    
    retry_tracker = Variable.get("hubspot_retry_tracker", default_var={}, deserialize_json=True)
    
    if tracker_key in retry_tracker:
        del retry_tracker[tracker_key]
        Variable.set("hubspot_retry_tracker", json.dumps(retry_tracker))
        logging.info(f"Retry succeeded - cleared tracker for {tracker_key}")

def update_retry_tracker_on_failure(context):
    dag_run = context['dag_run']
    original_run_id = dag_run.conf.get('original_run_id')
    original_dag_id = dag_run.conf.get('original_dag_id', dag_run.dag_id)
    
    if not original_run_id or not dag_run.conf.get('retry_attempt'):
        return  # Not a retry run
    
    tracker_key = f"{original_dag_id}:{original_run_id}"
    
    retry_tracker = Variable.get("hubspot_retry_tracker", default_var={}, deserialize_json=True)
    
    if tracker_key in retry_tracker:
        retry_tracker[tracker_key]["status"] = "failed"
        Variable.set("hubspot_retry_tracker", json.dumps(retry_tracker))
        logging.info(f"Retry failed - updated status for {tracker_key}")

default_args = {
    "owner": "lowtouch.ai_developers",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retry_delay": timedelta(minutes=5),
    'on_failure_callback': [
        send_fallback_email_on_failure,  # Sends email to user
        send_hubspot_slack_alert          # Sends Slack alert to team
    ]
}

HUBSPOT_FROM_ADDRESS = Variable.get("ltai.v3.hubspot.from.address")
GMAIL_CREDENTIALS = Variable.get("ltai.v3.hubspot.gmail.credentials")
OLLAMA_HOST = Variable.get("ltai.v3.hubspot.ollama.host", "http://agentomatic:8000")
HUBSPOT_API_KEY = Variable.get("ltai.v3.husbpot.api.key")
HUBSPOT_BASE_URL = Variable.get("ltai.v3.hubspot.url")
HUBSPOT_UI_URL = Variable.get("ltai.v3.hubspot.ui.url")
HUBSPOT_TASK_UI_URL = Variable.get("ltai.v3.hubspot.task.ui.url")

# Global cache for deal stage mappings
DEAL_STAGE_CACHE = {}

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================
def authenticate_gmail():
    """Authenticate Gmail API"""
    try:
        creds = Credentials.from_authorized_user_info(json.loads(GMAIL_CREDENTIALS))
        service = build("gmail", "v1", credentials=creds)
        profile = service.users().getProfile(userId="me").execute()
        logged_in_email = profile.get("emailAddress", "")
        if logged_in_email.lower() != HUBSPOT_FROM_ADDRESS.lower():
            raise ValueError(f"Wrong Gmail account! Expected {HUBSPOT_FROM_ADDRESS}, got {logged_in_email}")
        logging.info(f"Authenticated Gmail account: {logged_in_email}")
        return service
    except Exception as e:
        logging.error(f"Failed to authenticate Gmail: {str(e)}")
        return None

def get_ai_response(prompt, conversation_history=None, expect_json=False):
    """Get response from AI model"""
    try:
        client = Client(host=OLLAMA_HOST, headers={'x-ltai-client': 'hubspot-v6af_cl'})
        messages = []

        if expect_json:
            messages.append({
                "role": "system",
                "content": "You are a JSON-only API. Always respond with valid JSON objects. Never include explanatory text, HTML, or markdown formatting. Only return the requested JSON structure."
            })

        if conversation_history:
            for item in conversation_history:
                messages.append({"role": "user", "content": item["prompt"]})
                messages.append({"role": "assistant", "content": item["response"]})
        
        messages.append({"role": "user", "content": prompt})
        response = client.chat(model='hubspot:v6af_cl', messages=messages, stream=False)
        ai_content = response.message.content
        ai_content = re.sub(r'```(?:html|json)\n?|```', '', ai_content)
        return ai_content.strip()
    except Exception as e:
        raise

def extract_json_from_text(text):
    """Extract JSON from text with markdown or other wrappers."""
    try:
        text = text.strip()
        text = re.sub(r'```json\s*', '', text)
        text = re.sub(r'```\s*', '', text)

        # Try parsing the whole text first
        try:
            return json.loads(text)
        except (json.JSONDecodeError, ValueError):
            pass

        # Find balanced braces to handle nested JSON
        start = text.find('{')
        if start == -1:
            return None
        depth = 0
        for i in range(start, len(text)):
            if text[i] == '{':
                depth += 1
            elif text[i] == '}':
                depth -= 1
                if depth == 0:
                    try:
                        return json.loads(text[start:i+1])
                    except (json.JSONDecodeError, ValueError):
                        pass
                    break
        return None
    except Exception as e:
        logging.error(f"Error extracting JSON: {e}")
        return None

def mark_message_as_read(service, message_id):
    """Mark email as read - with proper error handling"""
    try:
        service.users().messages().modify(
            userId='me',
            id=message_id,
            body={'removeLabelIds': ['UNREAD']}
        ).execute()
        logging.info(f"Marked message {message_id} as read")
        return True
    except Exception as e:
        # Log the error but don't fail the task
        logging.warning(f"Could not mark message {message_id} as read: {e}")
        logging.info("This may be due to Gmail API permissions - the email will remain unread")
        return False

def extract_all_recipients(email_data):
    """Extract all recipients (To, Cc, Bcc) from email headers."""
    headers = email_data.get("headers", {})
    
    def parse_addresses(header_value):
        if not header_value:
            return []
        addresses = []
        for addr in header_value.split(','):
            addr = addr.strip()
            email_match = re.search(r'<([^>]+)>', addr)
            if email_match:
                addresses.append(email_match.group(1).strip())
            elif '@' in addr:
                addresses.append(addr.strip())
        return addresses
    
    to_recipients = parse_addresses(headers.get("To", ""))
    cc_recipients = parse_addresses(headers.get("Cc", ""))
    bcc_recipients = parse_addresses(headers.get("Bcc", ""))
    
    return {
        "to": to_recipients,
        "cc": cc_recipients,
        "bcc": bcc_recipients
    }

# ============================================================================
# HUBSPOT API FUNCTIONS
# ============================================================================
def get_owner_id_by_name(query):
    """Get HubSpot owner ID by name or email"""
    try:
        endpoint = f"{HUBSPOT_BASE_URL}/crm/v3/owners"
        headers = {"Authorization": f"Bearer {HUBSPOT_API_KEY}"}
        params = {}
        if '@' in query:
            params["email"] = query
        
        response = requests.get(endpoint, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        
        owners = response.json().get("results", [])
        
        query_lower = query.lower()
        
        if '@' in query:
            for owner in owners:
                if owner.get("email", "").lower() == query_lower:
                    logging.info(f"Found owner by email: {owner.get('id')} ({query})")
                    return owner.get("id")
        else:
            for owner in owners:
                full_name = f"{owner.get('firstName', '')} {owner.get('lastName', '')}".strip().lower()
                if query_lower == full_name or query_lower in full_name.split():
                    logging.info(f"Found owner by name: {owner.get('id')} ({full_name})")
                    return owner.get("id")
        
        logging.warning(f"No owner found for query: {query}")
        return None
        
    except Exception as e:
        logging.error(f"Failed to get owner: {e}")
        return None
def get_task_details(task_id):
    """Get full details of a HubSpot task"""
    try:
        endpoint = f"{HUBSPOT_BASE_URL}/crm/v3/objects/tasks/{task_id}"
        headers = {
            "Authorization": f"Bearer {HUBSPOT_API_KEY}",
            "Content-Type": "application/json"
        }
        properties = [
            "hs_task_subject", "hs_task_body", "hs_timestamp", 
            "hs_task_priority", "hs_task_status", "hubspot_owner_id"
        ]
        props_param = "&".join([f"properties={p}" for p in properties])
        
        response = requests.get(f"{endpoint}?{props_param}", headers=headers, timeout=30)
        response.raise_for_status()
        
        task_data = response.json()
        logging.info(f"Retrieved task {task_id}: {task_data.get('properties', {}).get('hs_task_subject')}")
        return task_data
        
    except Exception as e:
        logging.error(f"Failed to get task {task_id}: {e}")
        return None

def update_task_status(task_id, status=None, due_date=None):
    """Update task status and/or due date in HubSpot"""
    try:
        endpoint = f"{HUBSPOT_BASE_URL}/crm/v3/objects/tasks/{task_id}"
        headers = {
            "Authorization": f"Bearer {HUBSPOT_API_KEY}",
            "Content-Type": "application/json"
        }
        
        properties = {}
        
        # Update status if provided
        if status:
            properties["hs_task_status"] = status
        
        # Update due date if provided
        if due_date:
            # Convert datetime to milliseconds timestamp
            due_timestamp = int(due_date.timestamp() * 1000)
            properties["hs_timestamp"] = due_timestamp
        
        if not properties:
            logging.warning(f"No properties to update for task {task_id}")
            return False
        
        payload = {"properties": properties}
        
        response = requests.patch(endpoint, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        
        updates = []
        if status:
            updates.append(f"status to {status}")
        if due_date:
            updates.append(f"due date to {due_date.strftime('%Y-%m-%d')}")
        
        logging.info(f"✓ Updated task {task_id}: {', '.join(updates)}")
        return True
        
    except Exception as e:
        logging.error(f"Failed to update task {task_id}: {e}")
        return False

def create_followup_task(original_task, due_date=None, task_subject=None, owner_name=None):
    """Create a follow-up task based on original task"""
    try:
        original_props = original_task.get("properties", {})
        
        # Calculate due date (default: 2 weeks from now)
        if not due_date:
            due_date = datetime.now(timezone.utc) + timedelta(weeks=2)
        
        # Convert to milliseconds timestamp
        due_timestamp = int(due_date.timestamp() * 1000)
        
        # Use AI-suggested task subject, or fallback to "Follow-Up"
        if not task_subject or task_subject.strip() == "":
            task_subject = "Follow-Up"
        
        # FIXED: Task body is now same as task subject
        task_body = task_subject
        
        owner_id = original_props.get("hubspot_owner_id")
        if owner_name:
            new_owner_id = get_owner_id_by_name(owner_name)
            if new_owner_id:
                owner_id = new_owner_id
                logging.info(f"Using specified owner ID {owner_id} for {owner_name}")
            else:
                logging.warning(f"Specified owner '{owner_name}' not found, defaulting to original owner {owner_id}")
       
        endpoint = f"{HUBSPOT_BASE_URL}/crm/v3/objects/tasks"
        headers = {
            "Authorization": f"Bearer {HUBSPOT_API_KEY}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "properties": {
                "hs_task_subject": task_subject,
                "hs_task_body": task_body,
                "hs_timestamp": due_timestamp,
                "hs_task_priority": original_props.get("hs_task_priority", "MEDIUM"),
                "hs_task_status": "NOT_STARTED",
                "hubspot_owner_id": owner_id
            }
        }
        
        response = requests.post(endpoint, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        
        new_task = response.json()
        new_task_id = new_task.get("id")
        
        logging.info(f"✓ Created follow-up task {new_task_id} with subject/body: {task_subject}")
        
        # Copy associations from original task
        copy_task_associations(original_task.get("id"), new_task_id)
        
        return new_task
        
    except Exception as e:
        logging.error(f"Failed to create follow-up task: {e}")
        return None

def copy_task_associations(source_task_id, target_task_id):
    """Copy associations from source task to target task"""
    try:
        # Get associations from source task
        for assoc_type in ["contacts", "companies", "deals"]:
            endpoint = f"{HUBSPOT_BASE_URL}/crm/v4/objects/tasks/{source_task_id}/associations/{assoc_type}"
            headers = {"Authorization": f"Bearer {HUBSPOT_API_KEY}"}
            
            response = requests.get(endpoint, headers=headers, timeout=30)
            if response.status_code == 200:
                results = response.json().get("results", [])
                
                # Create associations for target task
                for result in results:
                    assoc_id = result.get("toObjectId")
                    assoc_type_id = result.get("associationTypes", [{}])[0].get("typeId")
                    
                    if assoc_id and assoc_type_id:
                        create_endpoint = f"{HUBSPOT_BASE_URL}/crm/v4/objects/tasks/{target_task_id}/associations/{assoc_type}/{assoc_id}"
                        create_payload = [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": assoc_type_id}]
                        
                        requests.put(create_endpoint, headers=headers, json=create_payload, timeout=30)
                
                logging.info(f"✓ Copied {len(results)} {assoc_type} associations")
                
    except Exception as e:
        logging.warning(f"Failed to copy associations: {e}")

def get_associated_deal_id(task_id):
    try:
        endpoint = f"{HUBSPOT_BASE_URL}/crm/v4/objects/tasks/{task_id}/associations/deals"
        headers = {"Authorization": f"Bearer {HUBSPOT_API_KEY}"}
        response = requests.get(endpoint, headers=headers, timeout=30)
        response.raise_for_status()
        results = response.json().get("results", [])
        if results:
            deal_id = results[0].get("toObjectId")
            logging.info(f"Found associated deal {deal_id} for task {task_id}")
            return deal_id
        return None
    except Exception as e:
        logging.warning(f"No associated deal for task {task_id}: {e}")
        return None

def fetch_deal_stages():
    """Fetch deal stages from all pipelines and cache them"""
    global DEAL_STAGE_CACHE
    if "default" in DEAL_STAGE_CACHE:
        return DEAL_STAGE_CACHE["default"]
    
    try:
        endpoint = f"{HUBSPOT_BASE_URL}/crm/v3/pipelines/deals"
        headers = {"Authorization": f"Bearer {HUBSPOT_API_KEY}"}
        response = requests.get(endpoint, headers=headers, timeout=30)
        response.raise_for_status()
        
        pipelines = response.json().get("results", [])
        if not pipelines:
            logging.warning("No deal pipelines found")
            return {}
        
        stage_map = {}
        for pipeline in pipelines:
            pipe_label = pipeline.get("label", "Unknown")
            stages = pipeline.get("stages", [])
            logging.info(f"Loading stages from pipeline: {pipe_label}")
            
            for stage in stages:
                stage_id = stage.get("id")
                label = stage.get("label", "").strip()
                if not label or not stage_id:
                    continue
                
                lowered = label.lower()
                stage_map[lowered] = stage_id
                stage_map[lowered.replace(" ", "")] = stage_id
                stage_map[lowered.replace(" ", "_")] = stage_id
                stage_map[re.sub(r'\s+', ' ', lowered)] = stage_id
        
        DEAL_STAGE_CACHE["default"] = stage_map
        logging.info(f"Cached {len(stage_map)} total stages across {len(pipelines)} pipeline(s)")
        return stage_map
    
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            logging.error("Deal pipelines endpoint not found. Check API version and permissions.")
        else:
            logging.error(f"HTTP error fetching deal stages: {e}")
        return {}
    except Exception as e:
        logging.error(f"Failed to fetch deal stages: {e}")
        return {}

def update_deal(deal_id, properties):
    try:
        endpoint = f"{HUBSPOT_BASE_URL}/crm/v3/objects/deals/{deal_id}"
        headers = {
            "Authorization": f"Bearer {HUBSPOT_API_KEY}",
            "Content-Type": "application/json"
        }
        payload = {"properties": properties}
        
        response = requests.patch(endpoint, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        
        logging.info(f"✓ Updated deal {deal_id} with: {properties}")
        return True
    except Exception as e:
        logging.error(f"Failed to update deal {deal_id}: {e}")
        return False

def get_deal_details(deal_id):
    """Fetch current deal properties"""
    if not deal_id:
        return None
    try:
        endpoint = f"{HUBSPOT_BASE_URL}/crm/v3/objects/deals/{deal_id}"
        headers = {"Authorization": f"Bearer {HUBSPOT_API_KEY}"}
        properties = ["dealstage", "amount", "closedate", "pipeline"]
        props_param = "&".join([f"properties={p}" for p in properties])
        response = requests.get(f"{endpoint}?{props_param}", headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        props = data.get("properties", {})
        current_stage_id = props.get("dealstage")
        amount = props.get("amount")
        closedate_ms = props.get("closedate")
        
        stage_label = None
        stage_map = fetch_deal_stages()
        for label, sid in stage_map.items():
            if sid == current_stage_id:
                stage_label = label
                break
        
        closedate_str = None
        if closedate_ms:
            try:
                closedate_str = datetime.fromtimestamp(int(closedate_ms)/1000, tz=timezone.utc).strftime("%Y-%m-%d")
            except:
                pass
        
        return {
            "current_stage_label": stage_label or current_stage_id or "Unknown",
            "current_amount": amount,
            "current_closedate": closedate_str
        }
    except Exception as e:
        logging.error(f"Failed to fetch deal details for {deal_id}: {e}")
        return None

# ============================================================================
# TASK PROCESSING FUNCTIONS
# ============================================================================
def analyze_task_completion_request(**kwargs):
    """Analyze if email is a task completion request and extract details"""
    email_data = kwargs['dag_run'].conf.get('email_data', {})
    
    if not email_data:
        raise ValueError("No email data provided in dag_run.conf")
    
    email_content = email_data.get("content", "")
    
    # Find task_id from thread history if not in direct headers
    task_id = None
    
    # First try direct headers
    headers = email_data.get("headers", {})
    task_id = headers.get("X-Task-ID")
    
    if task_id:
        logging.info(f"Found task ID {task_id} in direct headers")
    
    # If not found and it's a reply, search thread history for bot message
    if not task_id and email_data.get("is_reply", False):
        thread_history = email_data.get("thread_history", [])
        logging.info(f"Searching thread history ({len(thread_history)} messages) for bot reminder")
        for msg in reversed(thread_history):
            if msg.get("from_bot", False):
                msg_headers = msg.get("headers", {})
                candidate_id = msg_headers.get("X-Task-ID")
                candidate_type = msg_headers.get("X-Task-Type")
                if candidate_id and candidate_type == "daily-reminder":
                    task_id = candidate_id
                    logging.info(f"✓ Found task ID {task_id} in bot message from thread history")
                    break
    
    if not task_id:
        raise ValueError("No task ID found in email headers or thread history")

    # Get task details for context
    original_task = get_task_details(task_id)
    task_context = ""
    if original_task:
        task_props = original_task.get("properties", {})
        task_context = f"""
Current Task Details:
- Subject: {task_props.get('hs_task_subject', 'Follow-Up')}
- Body: {task_props.get('hs_task_body', 'Follow-Up')}
- Priority: {task_props.get('hs_task_priority', 'Follow-Up')}
"""

    deal_id = get_associated_deal_id(task_id)
    deal_details = get_deal_details(deal_id) if deal_id else None
    
    deal_context = ""
    if deal_details:
        deal_context = f"""
Associated Deal ID: {deal_id}
Current Stage: {deal_details.get('current_stage_label', 'Unknown')}
Current Amount: {deal_details.get('current_amount', 'Not set')}
Current Close Date: {deal_details.get('current_closedate', 'Not set')}
"""
    else:
        deal_context = "No associated deal found."
    
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    # FIXED: Simplified AI prompt focused on task subject extraction
    analysis_prompt = f"""Today's date is {today}. Analyze this email reply to determine if the user wants to:
1. Mark a task as completed, Whenever user says "done", "completed", "this task is complete", "task done"etc with a clear indication of completion. Note that change the due date does NOT imply completion.
2. Create a follow-up task along with a due date and optionally subject and owner always means to create followup. Whenever user says "create a follow-up", "new task", "follow-up to", "next task", "another task", etc. Here the user is not meaning to update the existing task but create a new one.
3. Update the associated deal
4. Update the current task's due date

{task_context}

Email content: "{email_content}"
Deal Context:
{deal_context}

IMPORTANT TASK DUE DATE RULES:
- If user says "extend the due date", "postpone to", "move to", "reschedule to" → extract new due date
- If user mentions a new deadline for THIS task (not a follow-up) → extract that date
- Examples: "extend to next week", "move this to Friday", "postpone until January 15"

CRITICAL FOLLOW-UP TASK SUBJECT EXTRACTION RULES FOR NEW FOLLOW-UP TASK:
Extract the follow-up task subject from the user's email to create a new task. Follow these rules strictly:

1. **Look for task descriptions or actions** the user mentions:
   - Examples: "schedule a call with the client", "send them the pricing document", "check if they made a decision"
   - Extract: "Schedule call with client", "Send pricing document", "Check client decision"
   
2. **Clean and format the subject**:
   - Keep it concise (under 100 characters)
   - Use title case or sentence case
   - Remove unnecessary words like "please", "can you", etc.
   - Focus on the ACTION and OBJECT

3. **Multi-line parsing**:
   - User might say: "this task is done\nCreate a follow-up to send them the updated proposal"
   - Extract: "Send updated proposal"

4. **Use "Follow-Up" as default task subject**:
   - User only says: "done", "completed", "task done", "yes", "create a follow-up" with NOTHING else specific as task subject.
   - In these cases: set followup_task_subject to empty string (will default to "Follow-Up")

5. **Important**:
   - DO NOT invent context that isn't in the user's message
   - Extract EXACTLY what the user wants done
   - Be concise and action-oriented

IMPORTANT DEAL UPDATE RULES:
- If user says "we lost the deal", "lost this deal", or similar → set dealstage_label to "Closed Lost"
- If user says "we won", "closed the deal", or similar → set dealstage_label to "Closed Won"
- If user mentions updating the deal stage/status → extract the exact stage name
- If user mentions deal amount changes → extract the new amount
- If user mentions close date changes → extract the new date
- If ANY deal field should be updated, set update_deal to TRUE
- Only set update_deal to FALSE if there are NO deal-related changes mentioned

Calculate dates relative to today ({today}):
- tomorrow = {(datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")}
- next week = {(datetime.now(timezone.utc) + timedelta(weeks=1)).strftime("%Y-%m-%d")}
- next month = {(datetime.now(timezone.utc) + timedelta(days=30)).strftime("%Y-%m-%d")}

Return ONLY valid JSON with NO additional text:
{{
    "mark_completed": true/false,
    "update_task_due_date": true/false,
    "new_task_due_date": "YYYY-MM-DD or null",
    "create_followup": true/false,
    "followup_due_date": "YYYY-MM-DD or null",
    "followup_task_subject": "concise action-oriented subject or empty string",
    "followup_owner_name": "name/email or empty",
    "update_deal": true/false,
    "deal_updates": {{
        "dealstage_label": "Exact stage name like 'Closed Lost' or null",
        "amount": "number as string or null",
        "closedate": "YYYY-MM-DD or null"
    }}
}}
"""
    try:
        ai_response = get_ai_response(analysis_prompt, expect_json=True)
    except Exception as e:
        raise

    try:
        analysis = json.loads(ai_response)
    except:
        analysis = extract_json_from_text(ai_response)
    
    if not analysis:
        raise ValueError("Failed to parse AI analysis response")

    if "deal_updates" not in analysis or not isinstance(analysis["deal_updates"], dict):
        analysis["deal_updates"] = {}
    
    followup_due = analysis.get("followup_due_date", "")
    if not followup_due or followup_due.lower() in ["null", "default"]:
        followup_due = (datetime.now(timezone.utc) + timedelta(weeks=2)).strftime("%Y-%m-%d")
    
    # Get AI-suggested task subject
    followup_task_subject = analysis.get("followup_task_subject", "")
    if not followup_task_subject or followup_task_subject.strip() == "":
        # Fallback: if no subject provided, use "Follow-Up"
        followup_task_subject = "Follow-Up"
    
    result = {
        "task_id": task_id,
        "email_data": email_data,
        "mark_completed": analysis.get("mark_completed", False),
        "update_task_due_date": analysis.get("update_task_due_date", False),
        "new_task_due_date": analysis.get("new_task_due_date"),
        "create_followup": analysis.get("create_followup", False),
        "followup_due_date": followup_due,
        "followup_task_subject": followup_task_subject,
        "followup_owner_name": analysis.get("followup_owner_name", ""),
        "update_deal": analysis.get("update_deal", False),
        "deal_updates": analysis.get("deal_updates", {}),
        "deal_id": deal_id
    }
    
    kwargs['ti'].xcom_push(key="task_completion_analysis", value=result)
    logging.info(f"Analysis result: {result}")
    
    return result

def process_task_completion(**kwargs):
    """Process task completion, due date updates, and follow-up creation"""
    ti = kwargs['ti']
    analysis = ti.xcom_pull(key="task_completion_analysis", task_ids="analyze_request")
    
    if not analysis:
        logging.error("No analysis data found")
        return {"success": False, "error": "No analysis data"}
    
    task_id = analysis["task_id"]
    deal_id = analysis.get("deal_id")
    
    results = {
        "task_id": task_id,
        "completed": False,
        "due_date_updated": False,
        "new_due_date": None,
        "followup_created": False,
        "followup_task_id": None,
        "deal_updated": False,
        "deal_id": deal_id,
        "deal_update_details": {},
        "error": None
    }
    
    # Get original task details
    original_task = get_task_details(task_id)
    
    # ✅ FIX: Handle missing tasks gracefully instead of exiting
    if not original_task:
        logging.warning(f"Task {task_id} not found (404) - likely already deleted. Creating follow-up with defaults.")
        
        # Extract task info from the email thread history if available
        email_data = analysis.get("email_data", {})
        thread_history = email_data.get("thread_history", [])
        
        # Try to find task details from the bot's reminder email
        task_subject = "Completed Task"
        task_owner_id = None
        
        for msg in thread_history:
            if msg.get("from_bot"):
                content = msg.get("content", "")
                # Extract subject from reminder email
                subject_match = re.search(r'<li><strong>Task Name:</strong>\s*([^<]+)</li>', content)
                if subject_match:
                    task_subject = subject_match.group(1).strip()
                    logging.info(f"Extracted task subject from email: {task_subject}")
                break
        
        # Create a minimal original task structure
        original_task = {
            "id": task_id,
            "properties": {
                "hs_task_subject": task_subject,
                "hs_task_body": "",
                "hs_task_priority": "MEDIUM",
                "hubspot_owner_id": task_owner_id
            }
        }
        
        results["error"] = "Original task not found (404) - proceeding with available data"
        # Don't return early - continue to create follow-up
    
    # Prepare task updates (status and/or due date)
    status_to_set = "COMPLETED" if analysis.get("mark_completed") else None
    due_date_to_set = None
    
    if analysis.get("update_task_due_date"):
        new_due_str = analysis.get("new_task_due_date")
        if new_due_str:
            try:
                due_date_to_set = datetime.strptime(new_due_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except Exception as e:
                logging.error(f"Failed to parse due date {new_due_str}: {e}")
                if not results["error"]:
                    results["error"] = f"Invalid due date format: {new_due_str}"
    
    # Update task (status and/or due date in single call) - only if task exists
    if original_task.get("id") and (status_to_set or due_date_to_set):
        # Check if task still exists before trying to update
        if get_task_details(task_id):  # Verify task still exists
            success = update_task_status(task_id, status=status_to_set, due_date=due_date_to_set)
            if status_to_set:
                results["completed"] = success
            if due_date_to_set:
                results["due_date_updated"] = success
                if success:
                    results["new_due_date"] = analysis.get("new_task_due_date")
            if not success and not results["error"]:
                results["error"] = "Failed to update task"
        else:
            logging.warning(f"Skipping task update - task {task_id} no longer exists")
            results["completed"] = False  # Can't mark as completed if it doesn't exist
    
    # Create follow-up task if requested
    if analysis.get("create_followup"):
        due_date_str = analysis.get("followup_due_date", "default")
        
        # Parse due date
        if due_date_str == "default":
            due_date = None # Will use default 2 weeks
        else:
            try:
                due_date = datetime.strptime(due_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except:
                due_date = None
       
        # Create follow-up task
        followup_task = create_followup_task(
            original_task, 
            due_date,
            task_subject=analysis.get("followup_task_subject"),
            owner_name=analysis.get("followup_owner_name") if analysis.get("followup_owner_name") else None
        )
       
        if followup_task:
            results["followup_created"] = True
            results["followup_task_id"] = followup_task.get("id")
            
            # Update error message if follow-up was created successfully despite missing original
            if results["error"] and "not found" in results["error"]:
                results["error"] = "Original task not found but follow-up created successfully"
    
    # Deal Update Logic (unchanged)
    if deal_id and analysis.get("update_deal"):
        updates = analysis.get("deal_updates") or {}
        props = {}

        stage_map = fetch_deal_stages()

        stage_label_raw = updates.get("dealstage_label")
        stage_label = (stage_label_raw or "").strip() if stage_label_raw is not None else ""
        
        if stage_label:
            normalized = stage_label.lower()
            no_space = stage_label.lower().replace(" ", "")
            underscor = stage_label.lower().replace(" ", "_")
            clean = re.sub(r'\s+', ' ', stage_label).lower()
            
            stage_id = (
                stage_map.get(normalized) or
                stage_map.get(no_space) or
                stage_map.get(underscor) or
                stage_map.get(clean)
            )
            
            if stage_id:
                props["dealstage"] = stage_id
                logging.info(f"✓ Mapped '{stage_label}' → stage ID {stage_id}")
            else:
                logging.warning(f"Stage not found: '{stage_label}'. Available stages: {list(stage_map.keys())[:10]}")
        
        if updates.get("amount"):
            try:
                amt = str(updates["amount"]).replace("$", "").replace(",", "").strip()
                props["amount"] = str(int(float(amt)))
            except:
                logging.warning(f"Could not parse amount: {updates.get('amount')}")
        
        if updates.get("closedate"):
            try:
                cd = datetime.strptime(updates["closedate"], "%Y-%m-%d")
                props["closedate"] = int(cd.timestamp() * 1000)
            except:
                logging.warning(f"Could not parse closedate: {updates.get('closedate')}")
        
        if props:
            results["deal_updated"] = update_deal(deal_id, props)
            results["deal_update_details"] = props
            logging.info(f"Deal update attempted with props: {props}")
        else:
            logging.warning(f"update_deal was TRUE but no valid properties extracted from: {updates}")
    
    ti.xcom_push(key="processing_results", value=results)
    logging.info(f"Processing results: {results}")
    
    return results

def send_confirmation_email(**kwargs):
    """Send clean, professional confirmation email with Task IDs and actual follow-up due date"""
    ti = kwargs['ti']
    analysis = ti.xcom_pull(key="task_completion_analysis", task_ids="analyze_request")
    results = ti.xcom_pull(key="processing_results", task_ids="process_task")
    
    if not analysis or not results:
        logging.error("Missing data for confirmation email")
        return
    
    email_data = analysis["email_data"]
    headers = email_data.get("headers", {})
    sender_email = headers.get("From", "")
    task_id = results["task_id"]
    
    name_match = re.search(r'^([^<]+)', sender_email)
    sender_name = name_match.group(1).strip().split()[0] if name_match else "there"
    
    orig_task = get_task_details(task_id)
    orig_props = orig_task.get("properties", {}) if orig_task else {}
    orig_subject = orig_props.get("hs_task_subject", "Task")
    orig_priority = orig_props.get("hs_task_priority", "MEDIUM").title()
    
    # Task status
    task_status = "Completed" if results.get("completed") else "In Progress"
    
    # Due date section
    due_date_section = ""
    if results.get("due_date_updated"):
        new_due = results.get("new_due_date")
        try:
            formatted_due = datetime.strptime(new_due, "%Y-%m-%d").strftime("%B %d, %Y")
            due_date_section = f"<li><strong>New Due Date:</strong> {formatted_due}</li>"
        except:
            due_date_section = f"<li><strong>New Due Date:</strong> {new_due}</li>"
    
    followup_id = results.get("followup_task_id")
    fp_subject = "Follow-up task"
    fp_due_formatted = None
    if followup_id:
        # Try to get actual due date from created task
        fp_task = get_task_details(followup_id)
        if fp_task:
            fp_props = fp_task.get("properties", {})
            fp_subject = fp_props.get("hs_task_subject", fp_subject)
            fp_ms = fp_props.get("hs_timestamp")
            if fp_ms:
                try:
                    fp_due_formatted = datetime.fromtimestamp(int(fp_ms)/1000, tz=timezone.utc).strftime("%B %d, %Y")
                    logging.info(f"Retrieved follow-up due date from task: {fp_due_formatted}")
                except Exception as e:
                    logging.warning(f"Failed to parse follow-up timestamp: {e}")
        
        # Fallback: use the date from analysis if API fetch failed
        if not fp_due_formatted:
            due_date_str = analysis.get("followup_due_date", "")
            if due_date_str and due_date_str != "default":
                try:
                    fp_due_formatted = datetime.strptime(due_date_str, "%Y-%m-%d").strftime("%B %d, %Y")
                    logging.info(f"Using follow-up due date from analysis: {fp_due_formatted}")
                except Exception as e:
                    logging.warning(f"Failed to parse analysis due date: {e}")
            
            # Final fallback: calculate default (2 weeks from now)
            if not fp_due_formatted:
                default_due = datetime.now(timezone.utc) + timedelta(weeks=2)
                fp_due_formatted = default_due.strftime("%B %d, %Y")
                logging.info(f"Using default due date (2 weeks): {fp_due_formatted}")

    followup_section = ""
    if results.get("followup_created"):
        followup_section = f"""
<p style="margin-top:25px;"><strong>New Follow-Up Task Created:</strong></p>
<div style="margin-left:20px;">
    <ul><li><strong>Task:</strong> {fp_subject}</li>
        <li><strong>ID:</strong> {followup_id}</li>
        <li><strong>Due:</strong> {fp_due_formatted}</li>
        <li><strong>Link:</strong> <a href="{HUBSPOT_TASK_UI_URL}/view/all/task/{followup_id}">Open Task</a></li>
    </ul>
</div>"""

    deal_section = ""
    if results.get("deal_id"):
        deal_id = results["deal_id"]
        if results.get("deal_updated"):
            det = results.get("deal_update_details", {})
            lines = []
            if "dealstage" in det:
                lines.append(f"<li>Stage updated</li>")
            if "amount" in det:
                lines.append(f"<li>Amount updated to ${int(float(det.get('amount', 0))):,}</li>")
            if "closedate" in det:
                try:
                    dt = datetime.fromtimestamp(det["closedate"]/1000, tz=timezone.utc)
                    lines.append(f"<li>Close date: {dt.strftime('%B %d, %Y')}</li>")
                except:
                    pass
            deal_section = f"""
<p style="margin-top:25px;"><strong>Associated Deal Updated:</strong></p>
<div style="margin-left:20px;">
    <ul><li><strong>ID:</strong> {deal_id}</li>
        <li><strong>Link:</strong> <a href="{HUBSPOT_UI_URL}/deals/{deal_id}">Open Deal</a></li>
        {''.join(lines)}
    </ul>
</div>"""
        elif analysis.get("update_deal"):
            deal_section = "<p><em>Deal update requested but could not be applied.</em></p>"

    email_html = f"""<html><head><style>
    body {{font-family:Arial,sans-serif;line-height:1.6;color:#333;max-width:600px;margin:auto;padding:20px;}}
    ul {{padding-left:20px;}}
    </style></head><body>
    <p>Hello {sender_name},</p>
    <p>Thank you for your update. Your request has been processed.</p>
    
    <p><strong>Current Task:</strong></p>
    <ul><li><strong>Task:</strong> {orig_subject}</li>
        <li><strong>ID:</strong> {task_id}</li>
        <li><strong>Priority:</strong> {orig_priority}</li>
        <li><strong>Status:</strong> {task_status}</li>
        {due_date_section}
        <li><strong>Link:</strong> <a href="{HUBSPOT_TASK_UI_URL}/view/all/task/{task_id}">Open Task</a></li>
    </ul>
    
    {followup_section}
    {deal_section}
    
    <p>Reply to this email if you need changes.</p>
    <p>Best Regards,<br>HubSpot Task Assistant<br><a href="http://lowtouch.ai">lowtouch.ai</a></p>
    </body></html>"""

    try:
        service = authenticate_gmail()
        if not service:
            raise ValueError("Gmail authentication failed in send_completion_email")
        
        recipients = extract_all_recipients(email_data)
        msg = MIMEMultipart('alternative')
        msg["From"] = f"HubSpot Task Assistant <{HUBSPOT_FROM_ADDRESS}>"
        msg["To"] = sender_email
        if recipients["cc"]:
            msg["Cc"] = ', '.join(recipients["cc"])
        
        subject = headers.get("Subject", "Task Update")
        if not subject.lower().startswith("re:"):
            subject = f"Re: {subject}"
        msg["Subject"] = subject
        
        if headers.get("Message-ID"):
            msg["In-Reply-To"] = headers["Message-ID"]
            msg["References"] = headers.get("References", "") + " " + headers["Message-ID"]
        
        msg.attach(MIMEText(email_html, "html"))
        
        raw = base64.urlsafe_b64encode(msg.as_string().encode()).decode()
        service.users().messages().send(userId="me", body={"raw": raw}).execute()
        
        mark_message_as_read(service, email_data.get("id"))
        logging.info("Confirmation email sent")
        
    except Exception as e:
        logging.error(f"Email send failed: {e}")

# ============================================================================
# DAG DEFINITION
# ============================================================================
with DAG(
    "hubspot_task_completion_handler",
    default_args=default_args,
    schedule_interval=None,  # Triggered by email listener
    catchup=False,
    tags=["hubspot", "tasks", "deal", "completion"],
    on_success_callback=clear_retry_tracker_on_success,
    on_failure_callback=update_retry_tracker_on_failure,
    description="Handle task completion + deal updates via email with dynamic stage mapping"
) as dag:

    analyze = PythonOperator(task_id="analyze_request", python_callable=analyze_task_completion_request, provide_context=True)
    process = PythonOperator(task_id="process_task", python_callable=process_task_completion, provide_context=True)
    confirm = PythonOperator(task_id="send_confirmation", python_callable=send_confirmation_email, provide_context=True)

    analyze >> process >> confirm