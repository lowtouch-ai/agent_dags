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

# ============================================================================
# CONFIGURATION
# ============================================================================
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

default_args = {
    "owner": "lowtouch.ai_developers",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retry_delay": timedelta(minutes=5),
}

HUBSPOT_FROM_ADDRESS = Variable.get("ltai.v3.hubspot.from.address")
GMAIL_CREDENTIALS = Variable.get("ltai.v3.hubspot.gmail.credentials")
OLLAMA_HOST = Variable.get("ltai.v3.hubspot.ollama.host", "http://agentomatic:8000")
HUBSPOT_API_KEY = Variable.get("ltai.v3.husbpot.api.key")
HUBSPOT_BASE_URL = Variable.get("ltai.v3.hubspot.url")
HUBSPOT_UI_URL = Variable.get("ltai.v3.hubspot.ui.url")

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
        client = Client(host=OLLAMA_HOST, headers={'x-ltai-client': 'hubspot-v6af'})
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
        response = client.chat(model='hubspot:v6af', messages=messages, stream=False)
        ai_content = response.message.content
        ai_content = re.sub(r'```(?:html|json)\n?|```', '', ai_content)
        return ai_content.strip()
    except Exception as e:
        logging.error(f"Error in get_ai_response: {e}")
        if expect_json:
            return f'{{"error": "Error processing AI request: {str(e)}"}}'
        else:
            return f"<html><body>Error processing AI request: {str(e)}</body></html>"

def extract_json_from_text(text):
    """Extract JSON from text with markdown or other wrappers."""
    try:
        text = text.strip()
        text = re.sub(r'```json\s*', '', text)
        text = re.sub(r'```\s*', '', text)
        
        match = re.search(r'\{[^{}]*\}', text, re.DOTALL)
        if match:
            return json.loads(match.group())
        return None
    except Exception as e:
        logging.error(f"Error extracting JSON: {e}")
        return None

def mark_message_as_read(service, message_id):
    """Mark email as read"""
    try:
        service.users().messages().modify(
            userId='me',
            id=message_id,
            body={'removeLabelIds': ['UNREAD']}
        ).execute()
        logging.info(f"Marked message {message_id} as read")
        return True
    except Exception as e:
        logging.error(f"Error marking message {message_id} as read: {e}")
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

def update_task_status(task_id, status="COMPLETED"):
    """Update task status in HubSpot"""
    try:
        endpoint = f"{HUBSPOT_BASE_URL}/crm/v3/objects/tasks/{task_id}"
        headers = {
            "Authorization": f"Bearer {HUBSPOT_API_KEY}",
            "Content-Type": "application/json"
        }
        payload = {
            "properties": {
                "hs_task_status": status
            }
        }
        
        response = requests.patch(endpoint, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        
        logging.info(f"✓ Updated task {task_id} status to {status}")
        return True
        
    except Exception as e:
        logging.error(f"Failed to update task {task_id}: {e}")
        return False

def create_followup_task(original_task, due_date=None):
    """Create a follow-up task based on original task"""
    try:
        original_props = original_task.get("properties", {})
        
        # Calculate due date (default: 2 weeks from now)
        if not due_date:
            due_date = datetime.now(timezone.utc) + timedelta(weeks=2)
        
        # Convert to milliseconds timestamp
        due_timestamp = int(due_date.timestamp() * 1000)
        
        # Prepare new task properties
        task_subject = f"Follow-up: {original_props.get('hs_task_subject', 'Task')}"
        task_body = f"Follow-up task created from completed task.\n\nOriginal task: {original_props.get('hs_task_subject', '')}\nOriginal notes: {original_props.get('hs_task_body', '')}"
        
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
                "hubspot_owner_id": original_props.get("hubspot_owner_id")
            }
        }
        
        response = requests.post(endpoint, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        
        new_task = response.json()
        new_task_id = new_task.get("id")
        
        logging.info(f"✓ Created follow-up task {new_task_id}")
        
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

# ============================================================================
# TASK PROCESSING FUNCTIONS
# ============================================================================
def analyze_task_completion_request(**kwargs):
    """Analyze if email is a task completion request and extract details"""
    email_data = kwargs['dag_run'].conf.get('email_data', {})
    
    if not email_data:
        logging.error("No email data provided")
        return None
    
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
        logging.warning("No task ID found in email headers or thread history")
        return None
    
    # Get current date for AI context
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    # Use AI to analyze the user's intent
    analysis_prompt = f"""Today's date is {today}. Analyze this email reply to determine if the user wants to mark a task as completed and/or create a follow-up task.

Email content: "{email_content}"

Calculate dates relative to today ({today}):
- tomorrow = {(datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")}
- day after tomorrow = {(datetime.now(timezone.utc) + timedelta(days=2)).strftime("%Y-%m-%d")}
- next week = {(datetime.now(timezone.utc) + timedelta(weeks=1)).strftime("%Y-%m-%d")}
- next month = {(datetime.now(timezone.utc) + timedelta(days=30)).strftime("%Y-%m-%d")}

Extract the following information:
1. Does the user want to mark the task as completed? (yes/no)
2. Does the user want to create a follow-up task? (yes/no)
3. If follow-up requested, what is the due date? Calculate the actual date in YYYY-MM-DD format.
4. Any additional notes or context for the follow-up task?

Important:
 - If user only asks to mark as completed, ALWAYS create a follow-up task with due date 2 weeks from today: {(datetime.now(timezone.utc) + timedelta(weeks=2)).strftime("%Y-%m-%d")}
 - If the user explicitly says "no follow-up", then do not create a follow-up task.
 - Convert all relative dates (tomorrow, next week, etc.) to absolute YYYY-MM-DD format
 - If no specific date is mentioned, use 2 weeks from today: {(datetime.now(timezone.utc) + timedelta(weeks=2)).strftime("%Y-%m-%d")}

Return ONLY valid JSON with NO additional text:
{{
    "mark_completed": true/false,
    "create_followup": true/false,
    "followup_due_date": "YYYY-MM-DD",
    "followup_notes": "any additional notes"
}}
"""
    
    ai_response = get_ai_response(analysis_prompt, expect_json=True)
    
    try:
        analysis = json.loads(ai_response)
    except:
        analysis = extract_json_from_text(ai_response)
    
    if not analysis:
        logging.error("Failed to parse AI analysis")
        return None
    
    # Validate and fix the date if needed
    followup_due = analysis.get("followup_due_date", "default")
    if followup_due == "default" or not followup_due:
        followup_due = (datetime.now(timezone.utc) + timedelta(weeks=2)).strftime("%Y-%m-%d")
        analysis["followup_due_date"] = followup_due
    
    result = {
        "task_id": task_id,
        "email_data": email_data,
        "mark_completed": analysis.get("mark_completed", False),
        "create_followup": analysis.get("create_followup", False),
        "followup_due_date": followup_due,
        "followup_notes": analysis.get("followup_notes", "")
    }
    
    kwargs['ti'].xcom_push(key="task_completion_analysis", value=result)
    logging.info(f"Task completion analysis: {result}")
    
    return result

def process_task_completion(**kwargs):
    """Process task completion and follow-up creation"""
    ti = kwargs['ti']
    analysis = ti.xcom_pull(key="task_completion_analysis", task_ids="analyze_request")
    
    if not analysis:
        logging.error("No analysis data found")
        return {"success": False, "error": "No analysis data"}
    
    task_id = analysis.get("task_id")
    results = {
        "task_id": task_id,
        "completed": False,
        "followup_created": False,
        "followup_task_id": None,
        "error": None
    }
    
    # Get original task details
    original_task = get_task_details(task_id)
    if not original_task:
        results["error"] = "Failed to retrieve original task"
        ti.xcom_push(key="processing_results", value=results)
        return results
    
    # Mark task as completed if requested
    if analysis.get("mark_completed"):
        success = update_task_status(task_id, "COMPLETED")
        results["completed"] = success
        if not success:
            results["error"] = "Failed to mark task as completed"
    
    # Create follow-up task if requested
    if analysis.get("create_followup"):
        due_date_str = analysis.get("followup_due_date", "default")
        
        # Parse due date
        if due_date_str == "default":
            due_date = None  # Will use default 2 weeks
        else:
            try:
                due_date = datetime.strptime(due_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except:
                due_date = None
        
        followup_task = create_followup_task(original_task, due_date)
        
        if followup_task:
            results["followup_created"] = True
            results["followup_task_id"] = followup_task.get("id")
        else:
            if not results["error"]:
                results["error"] = "Failed to create follow-up task"
    
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
    
    email_data = analysis.get("email_data", {})
    headers = email_data.get("headers", {})
    sender_email = headers.get("From", "")
    task_id = results.get("task_id")
    
    # Extract sender first name for greeting
    sender_name = "there"
    name_match = re.search(r'^([^<]+)', sender_email)
    if name_match:
        sender_name = name_match.group(1).strip().split()[0]
    
    # Get original task details
    original_task = get_task_details(task_id)
    if not original_task:
        logging.error("Could not retrieve original task for confirmation email")
        return
    
    orig_props = original_task.get("properties", {})
    orig_subject = orig_props.get("hs_task_subject", "Task")
    orig_due_ms = orig_props.get("hs_timestamp")
    orig_priority = orig_props.get("hs_task_priority", "MEDIUM").title()
    orig_previous_status = orig_props.get("hs_task_status", "NOT_STARTED").replace("_", " ").title()
    
    # Format original due date
    orig_due_formatted = "Not specified"
    if orig_due_ms:
        try:
            due_dt = datetime.fromtimestamp(int(orig_due_ms) / 1000, tz=timezone.utc)
            orig_due_formatted = due_dt.strftime("%B %d, %Y")
        except:
            pass

    # Follow-up task details - Initialize variables with defaults
    followup_id = None
    fp_subject = "Follow-up task"
    fp_due_formatted = "Not specified"
    
    if results.get("followup_created"):
        followup_id = results.get("followup_task_id")
        if followup_id:
            followup_task = get_task_details(followup_id)
            if followup_task:
                fp_props = followup_task.get("properties", {})
                fp_subject = fp_props.get("hs_task_subject", "Follow-up Task")
                fp_due_ms = fp_props.get("hs_timestamp")
                
                # Calculate actual follow-up due date
                if fp_due_ms:
                    try:
                        fp_due_dt = datetime.fromtimestamp(int(fp_due_ms) / 1000, tz=timezone.utc)
                        fp_due_formatted = fp_due_dt.strftime("%B %d, %Y")
                    except Exception as e:
                        logging.error(f"Error formatting follow-up due date: {e}")
                        # Fallback: try to get from analysis
                        followup_date_str = analysis.get("followup_due_date")
                        if followup_date_str and followup_date_str != "default":
                            try:
                                fp_due_dt = datetime.strptime(followup_date_str, "%Y-%m-%d")
                                fp_due_formatted = fp_due_dt.strftime("%B %d, %Y")
                            except:
                                pass
            else:
                logging.warning(f"Could not retrieve follow-up task {followup_id}")
                # Fallback: try to get from analysis
                followup_date_str = analysis.get("followup_due_date")
                if followup_date_str and followup_date_str != "default":
                    try:
                        fp_due_dt = datetime.strptime(followup_date_str, "%Y-%m-%d")
                        fp_due_formatted = fp_due_dt.strftime("%B %d, %Y")
                    except:
                        pass

    # Build task links
    orig_task_link = f"{HUBSPOT_UI_URL}/tasks/{task_id}"
    followup_task_link = f"{HUBSPOT_UI_URL}/tasks/{followup_id}" if followup_id else "#"

    # Build follow-up section
    followup_section = ""
    if results.get("followup_created") and followup_id:
        followup_section = f"""
                    <p style="margin-top: 25px;"><strong>New Follow-Up Task Created:</strong></p>
                    <ul>
                        <li><strong>Task:</strong> {fp_subject}</li>
                        <li><strong>Task ID:</strong> {followup_id}</li>
                        <li><strong>Due Date:</strong> {fp_due_formatted}</li>
                        <li><strong>Status:</strong> Not Started</li>
                        <li><strong>Priority:</strong> {orig_priority}</li>
                        <li><strong>HubSpot Link:</strong> <a href="{followup_task_link}">Open Follow-Up Task</a></li>
                    </ul>
"""

    # Error note
    error_note = ""
    if results.get("error"):
        error_note = f"<p><em>Note: {results.get('error')}</em></p>"

    # Simple, clean email HTML matching your format
    email_html = f"""<html>
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
                ul {{
                    list-style-type: disc;
                    padding-left: 20px;
                    margin: 10px 0;
                }}
                ul li {{
                    margin: 8px 0;
                }}
            </style>
        </head>
        <body>
            <div class="greeting">
                <p>Hello {sender_name},</p>
            </div>
            
            <div class="message">
                <p>Thank you for your update. Your request has been successfully processed in HubSpot.</p>
                
                <p><strong>Completed Task:</strong></p>
                <ul>
                    <li><strong>Task:</strong> {orig_subject}</li>
                    <li><strong>Task ID:</strong> {task_id}</li>
                    <li><strong>Priority:</strong> {orig_priority}</li>
                    <li><strong>Previous Status:</strong> {orig_previous_status}</li>
                    <li><strong>Current Status:</strong> Completed</li>
                    <li><strong>Original Due Date:</strong> {orig_due_formatted}</li>
                    <li><strong>HubSpot Link:</strong> <a href="{orig_task_link}">Open Task</a></li>
                </ul>
                
                {followup_section}
                
                {error_note}
            </div>
            
            <div class="closing">
                <p>If you need any changes or have additional instructions, simply reply to this email.</p>
            </div>
            
            <div class="signature">
                <p>Best regards,<br>
                HubSpot Task Assistant<br>
                <a href="http://lowtouch.ai" class="company">lowtouch.ai</a></p>
            </div>
        </body>
        </html>
        """

    # Send the email
    try:
        service = authenticate_gmail()
        if not service:
            logging.error("Gmail authentication failed")
            return
        
        all_recipients = extract_all_recipients(email_data)
        primary_recipient = sender_email
        cc_recipients = [
            addr for addr in all_recipients["to"] + all_recipients["cc"]
            if addr.lower() != sender_email.lower() and HUBSPOT_FROM_ADDRESS.lower() not in addr.lower()
        ]
        
        msg = MIMEMultipart('alternative')
        msg["From"] = f"HubSpot Task Assistant <{HUBSPOT_FROM_ADDRESS}>"
        msg["To"] = primary_recipient
        if cc_recipients:
            msg["Cc"] = ', '.join(cc_recipients)
        
        subject = headers.get("Subject", "Task Update Confirmation")
        if not subject.lower().startswith("re:"):
            subject = f"Re: {subject}"
        msg["Subject"] = subject
        
        # Threading
        original_message_id = headers.get("Message-ID", "")
        if original_message_id:
            msg["In-Reply-To"] = original_message_id
            references = headers.get("References", "")
            msg["References"] = f"{references} {original_message_id}".strip() if references else original_message_id
        
        # Attach HTML
        msg.attach(MIMEText(email_html, "html"))
        
        raw_msg = base64.urlsafe_b64encode(msg.as_string().encode("utf-8")).decode("utf-8")
        service.users().messages().send(userId="me", body={"raw": raw_msg}).execute()
        
        mark_message_as_read(service, email_data.get("id"))
        logging.info(f"✓ Sent confirmation email to {sender_email} (Task: {task_id}, Follow-up: {followup_id})")
        
    except Exception as e:
        logging.error(f"Failed to send confirmation email: {e}")

# ============================================================================
# DAG DEFINITION
# ============================================================================
with DAG(
    "hubspot_task_completion_handler",
    default_args=default_args,
    schedule_interval=None,  # Triggered by email listener
    catchup=False,
    tags=["hubspot", "tasks", "completion", "followup"],
    description="Handle task completion requests and create follow-up tasks"
) as dag:

    analyze_request_task = PythonOperator(
        task_id="analyze_request",
        python_callable=analyze_task_completion_request,
        provide_context=True,
    )

    process_task_task = PythonOperator(
        task_id="process_task",
        python_callable=process_task_completion,
        provide_context=True,
    )

    send_confirmation_task = PythonOperator(
        task_id="send_confirmation",
        python_callable=send_confirmation_email,
        provide_context=True,
    )

    analyze_request_task >> process_task_task >> send_confirmation_task