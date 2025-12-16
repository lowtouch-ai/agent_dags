
import base64
import logging
import json
import os
import re
from datetime import datetime, timedelta, timezone, time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import pytz

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
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
DEFAULT_OWNER_ID = Variable.get("ltai.v3.hubspot.default.owner.id")
DEFAULT_OWNER_NAME = Variable.get("ltai.v3.hubspot.default.owner.name")
DEFAULT_OWNER_DETAILS = Variable.get("ltai.v3.hubspot.task.owners")

# Email spacing configuration
EMAIL_SPACING_MINUTES = 3
OVERDUE_THRESHOLD_DAYS = 3
DEFAULT_FOLLOWUP_WEEKS = 2

# Timezone for delivery window (11:00 AM owner timezone)
DELIVERY_START_HOUR = 9

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_configured_owners():
    """Load task owners from Airflow Variables instead of HubSpot API"""
    try:
        owners_json = DEFAULT_OWNER_DETAILS
        owners = json.loads(owners_json)
        
        if not owners:
            logging.warning("No task owners configured in ltai.v3.hubspot.task.owners variable")
            logging.warning("Using default owner as fallback")
            owners = [{
                "id": DEFAULT_OWNER_DETAILS,
                "name": DEFAULT_OWNER_DETAILS,
                "email": DEFAULT_OWNER_DETAILS,
                "timezone": DEFAULT_OWNER_DETAILS
            }]
        
        # Validate owner structure
        for owner in owners:
            required_fields = ["id", "name", "email", "timezone"]
            for field in required_fields:
                if field not in owner:
                    raise ValueError(f"Owner missing required field '{field}': {owner}")
        
        logging.info(f"Loaded {len(owners)} configured task owner(s)")
        for owner in owners:
            logging.info(f"  - {owner['name']} ({owner['email']}) in {owner['timezone']}")
        
        return owners
        
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON in ltai.v3.hubspot.task.owners variable: {e}")
        raise
    except Exception as e:
        logging.error(f"Failed to load configured owners: {e}")
        raise

def get_today_key(suffix=""):
    """Generate today's key with optional suffix"""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if suffix:
        return f"{suffix}_{today}"
    return today

def get_initiated_owners_today(ti):
    """Get set of owner IDs who already had reminders initiated today - NO SQL"""
    try:
        key = get_today_key("initiated_owners")
        
        # Try current run XCom first (from any task in this DAG run)
        initiated_list = ti.xcom_pull(key=key, task_ids=None, include_prior_dates=False)
        if initiated_list is not None:
            initiated_set = set(initiated_list)
            logging.info(f"📋 Loaded {len(initiated_set)} initiated owner(s) from current run XCom")
            return initiated_set

        # Try from previous DAG runs today (cross-run persistence)
        initiated_list = ti.xcom_pull(
            key=key,
            task_ids=None,
            include_prior_dates=True,
            dag_id=ti.dag_id
        )
        if initiated_list is not None:
            initiated_set = set(initiated_list)
            logging.info(f"📋 Loaded {len(initiated_set)} initiated owner(s) from prior runs today")
            return initiated_set

        logging.info("📋 No initiated owners found today (empty set)")
        return set()

    except Exception as e:
        logging.warning(f"Failed to load initiated owners, treating as empty: {e}")
        return set()

def mark_owner_initiated(ti, owner_id):
    """Mark that daily reminders have been initiated for this owner today"""
    try:
        key = get_today_key("initiated_owners")
        current_initiated = list(get_initiated_owners_today(ti))
        if owner_id not in current_initiated:
            current_initiated.append(owner_id)
            ti.xcom_push(key=key, value=current_initiated)
            logging.info(f"✓ Marked owner {owner_id} as initiated today (total: {len(current_initiated)})")
        else:
            logging.debug(f"Owner {owner_id} already marked as initiated today")
    except Exception as e:
        logging.error(f"Failed to mark owner {owner_id} as initiated: {e}")

def get_sent_reminders_today(ti):
    """Safely get set of task IDs sent today - NO SQL"""
    try:
        key = get_today_key("sent_reminders")
        
        # Try current run XCom first
        sent_list = ti.xcom_pull(key=key, task_ids=None, include_prior_dates=False)
        if sent_list is not None:
            sent_set = set(sent_list)
            logging.info(f"📋 Loaded {len(sent_set)} sent reminder(s) from current run XCom")
            return sent_set

        # Try from previous DAG runs today (cross-run persistence)
        sent_list = ti.xcom_pull(
            key=key,
            task_ids=None,
            include_prior_dates=True,
            dag_id=ti.dag_id
        )
        if sent_list is not None:
            sent_set = set(sent_list)
            logging.info(f"📋 Loaded {len(sent_set)} sent reminder(s) from prior runs today")
            return sent_set

        logging.info("📋 No sent reminders found today (empty set)")
        return set()

    except Exception as e:
        logging.warning(f"Failed to load sent reminders, treating as empty: {e}")
        return set()

def mark_reminder_sent(ti, task_id):
    """Atomically add task_id to today's sent list"""
    try:
        key = get_today_key("sent_reminders")
        current_sent = list(get_sent_reminders_today(ti))
        if task_id not in current_sent:
            current_sent.append(task_id)
            ti.xcom_push(key=key, value=current_sent)
            logging.info(f"✓ Marked task {task_id} as sent today (total: {len(current_sent)})")
        else:
            logging.debug(f"Task {task_id} already marked as sent today")
    except Exception as e:
        logging.error(f"Failed to mark task {task_id} as sent: {e}")

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
                "content": "You are a JSON-only API. Always respond with valid JSON objects. Never include explanatory text, HTML, or markdown formatting."
            })

        if conversation_history:
            for item in conversation_history:
                if "role" in item and "content" in item:
                    messages.append({"role": item["role"], "content": item["content"]})

        messages.append({"role": "user", "content": prompt})
        response = client.chat(model='hubspot:v6af', messages=messages, stream=False)
        ai_content = response.message.content
        ai_content = re.sub(r'```(?:html|json)\n?|```', '', ai_content)
        return ai_content.strip()
    except Exception as e:
        logging.error(f"Error in get_ai_response: {e}")
        raise

def send_task_reminder_email(service, task, owner_info, thread_id=None, in_reply_to=None):
    """Send individual task reminder email"""
    try:
        owner_email = owner_info.get("email", "")
        owner_name = owner_info.get("name", "Unknown")
        
        task_subject = task.get("hs_task_subject", "Task Reminder")
        task_body = task.get("hs_task_body", "")
        task_id = task.get("id", "")
        due_date = task.get("hs_timestamp", "")
        priority = task.get("hs_task_priority", "MEDIUM")
        status = task.get("hs_task_status", "NOT_STARTED")
        
        # Format due date
        try:
            due_dt = datetime.fromisoformat(due_date.replace('Z', '+00:00'))
            formatted_due = due_dt.strftime("%B %d, %Y")
        except:
            formatted_due = due_date
        
        # Determine if overdue
        is_overdue = False
        days_overdue = 0
        if due_date:
            try:
                due_dt = datetime.fromisoformat(due_date.replace('Z', '+00:00'))
                now = datetime.now(timezone.utc)
                if due_dt < now:
                    is_overdue = True
                    days_overdue = (now - due_dt).days
            except:
                pass
        
        # Build email subject
        if thread_id and in_reply_to:
            subject = f"Re: Task Reminder - {task_subject}"
        else:
            subject = f"Task Reminder - {task_subject}"
        
        # Build email body
        priority_color = {
            "HIGH": "#dc3545",
            "MEDIUM": "#ffc107",
            "LOW": "#28a745"
        }.get(priority, "#6c757d")
        
        overdue_notice = ""
        if is_overdue:
            overdue_notice = f"""
            <div style="background-color: #fff3cd; border-left: 4px solid #ffc107; padding: 12px; margin-bottom: 20px;">
                <strong>⚠️ This task is {days_overdue} day(s) overdue</strong>
            </div>
            """
        
        email_html = f"""
<html>
<head>
    <style>
        body {{
            font-family: Arial, Helvetica, sans-serif;
            line-height: 1.6;
            color: #333333;
            max-width: 600px;
            margin: 0 auto;
            padding: 20px;
            background-color: #ffffff;
        }}
        h2 {{
            color: #333333;
            margin-bottom: 20px;
            font-size: 20px;
        }}
        h3 {{
            color: #333333;
            margin-bottom: 15px;
            font-size: 16px;
        }}
        .task-section {{
            border: 1px solid #dddddd;
            border-radius: 4px;
            padding: 20px;
            margin: 25px 0;
            background-color: #f9f9f9;
        }}
        table {{
            width: 100%;
            margin: 15px 0;
            border-collapse: collapse;
        }}
        td {{
            padding: 8px 0;
            vertical-align: top;
        }}
        td.label {{
            width: 30%;
            font-weight: bold;
            color: #333333;
        }}
        .instructions {{
            border-left: 4px solid #dddddd;
            padding-left: 15px;
            margin: 25px 0;
            color: #444444;
        }}
        .signature {{
            margin-top: 40px;
            padding-top: 20px;
            border-top: 1px solid #dddddd;
            font-size: 14px;
            color: #666666;
        }}
        a {{
            color: #2c5aa0;
            text-decoration: none;
        }}
        a:hover {{
            text-decoration: underline;
        }}
        ol {{
            padding-left: 20px;
        }}
        li {{
            margin-bottom: 8px;
        }}
        .followup {{
            margin: 20px 0;
            font-style: italic;
        }}
    </style>
</head>
<body>
    <h2>Daily Task Reminder</h2>
    
    <p>Dear {owner_name},</p>
    
    {overdue_notice}
    
    <div class="task-section">
        <h3>{task_subject}</h3>
        
        <table>
            <tr>
                <td class="label">Task ID:</td>
                <td>{task_id}</td>
            </tr>
            <tr>
                <td class="label">Due Date:</td>
                <td>{formatted_due}</td>
            </tr>
            <tr>
                <td class="label">Priority:</td>
                <td>{priority}</td>
            </tr>
            <tr>
                <td class="label">Current Status:</td>
                <td>{status}</td>
            </tr>
        </table>
        
        <p><strong>Description:</strong><br>
        {task_body}</p>
    </div>
    
    <div class="instructions">
        <h4>How to Update This Task</h4>
        <p>Please reply to this email with the following information:</p>
        <ol>
            <li><strong>Current Status:</strong> Indicate the progress (e.g., Completed, In Progress, Blocked).</li>
            <li><strong>Follow-up Actions:</strong> Any required next steps (optional).</li>
        </ol>
        
        <p><strong>Note:</strong> If no reply is received by the end of the day and no follow-up actions are specified, a follow-up task will be automatically scheduled for two weeks from today.</p>
    </div>
    
    <p>If you have any questions or require assistance, please do not hesitate to reply to this email.</p>
    
    <div class="signature">
        <p>Best regards,<br>
        <strong>HubSpot Task Assistant</strong><br>
        <a href="http://lowtouch.ai">Lowtouch.ai</a></p>
    </div>
</body>
</html>
"""
        
        # Create message
        msg = MIMEMultipart()
        msg["From"] = f"HubSpot Task Reminder <{HUBSPOT_FROM_ADDRESS}>"
        msg["To"] = owner_email
        msg["Subject"] = subject
        
        # Add threading headers if this is part of existing thread
        if thread_id and in_reply_to:
            msg["In-Reply-To"] = in_reply_to
            msg["References"] = in_reply_to
        
        # Add custom headers for task tracking
        msg["X-Task-ID"] = task_id
        msg["X-Task-Type"] = "daily-reminder"
        
        msg.attach(MIMEText(email_html, "html"))
        
        # Send email
        raw_msg = base64.urlsafe_b64encode(msg.as_string().encode("utf-8")).decode("utf-8")
        result = service.users().messages().send(userId="me", body={"raw": raw_msg}).execute()
        
        logging.info(f"✓ Sent task reminder for task {task_id} to {owner_email}")
        return {
            "success": True,
            "message_id": result.get("id"),
            "task_id": task_id,
            "owner_email": owner_email,
            "thread_id": result.get("threadId")
        }
        
    except Exception as e:
        logging.error(f"Failed to send task reminder: {e}")
        return {
            "success": False,
            "error": str(e),
            "task_id": task.get("id"),
            "owner_email": owner_info.get("email")
        }

# ============================================================================
# DAG TASK FUNCTIONS
# ============================================================================

def get_all_task_owners(ti, **context):
    """Load task owners from Airflow Variables (not HubSpot API)"""
    try:
        owners = get_configured_owners()
        logging.info(f"✓ Loaded {len(owners)} configured task owner(s) from Airflow Variables")
        ti.xcom_push(key="all_owners", value=owners)
        return owners
    except Exception as e:
        logging.error(f"Failed to get task owners: {e}")
        ti.xcom_push(key="all_owners", value=[])
        return []

def check_delivery_window(ti, **context):
    """Check if we're in the delivery window (11 AM or later) for any timezone"""
    try:
        owners = ti.xcom_pull(key="all_owners", default=[])
        
        if not owners:
            logging.info("No owners found, skipping delivery window check")
            ti.xcom_push(key="in_delivery_window", value=False)
            return "skip_task_collection"
        
        # Get already initiated owners today
        initiated_owners = get_initiated_owners_today(ti)
        
        current_utc = datetime.now(timezone.utc)
        in_window_count = 0
        owners_to_process = []
        
        # Check each owner's timezone
        for owner in owners:
            owner_id = owner.get("id")
            tz_str = owner.get("timezone", "America/New_York")  
            
            # Skip if already initiated today
            if owner_id in initiated_owners:
                logging.info(f"⏭️  Owner {owner.get('name')} already initiated today - skipping")
                continue
            
            try:
                owner_tz = pytz.timezone(tz_str)
                owner_time = current_utc.astimezone(owner_tz)
                owner_hour = owner_time.hour
                logging.info(f"owner timezone is:{owner_tz}, {tz_str}")
                logging.info(f"current owner time:{owner_time}")
                logging.info(f"current hour:{owner_hour}")
                
                if DELIVERY_START_HOUR <= owner_hour:
                    in_window_count += 1
                    owners_to_process.append(owner)
                    logging.info(f"Owner {owner.get('name')} ({tz_str}) is in delivery window: {owner_time.strftime('%I:%M %p')}")
            except Exception as e:
                logging.warning(f"Invalid timezone {tz_str} for owner {owner.get('name')}: {e}")
        
        # Store owners that need processing
        ti.xcom_push(key="owners_to_process", value=owners_to_process)
        
        in_window = in_window_count > 0
        ti.xcom_push(key="in_delivery_window", value=in_window)
        
        if in_window:
            logging.info(f"✓ {in_window_count} owner(s) in delivery window and not yet initiated - proceeding")
            return "collect_due_tasks"
        else:
            logging.info("✗ No owners need processing - all either outside window or already initiated")
            return "skip_task_collection"
            
    except Exception as e:
        logging.error(f"Error checking delivery window: {e}")
        ti.xcom_push(key="in_delivery_window", value=False)
        return "skip_task_collection"

def collect_due_tasks(ti, **context):
    """Collect tasks due today OR overdue by MORE than 72 hours (excludes already sent today)"""
    try:
        # Get only owners that need processing (not already initiated)
        owners = ti.xcom_pull(key="owners_to_process", default=[])
        
        if not owners:
            logging.info("No owners to process")
            ti.xcom_push(key="tasks_by_owner", value={})
            return {}
        
        # Mark all these owners as initiated IMMEDIATELY to prevent duplicate runs
        for owner in owners:
            mark_owner_initiated(ti, owner.get("id"))
        
        logging.info(f"✓ Marked {len(owners)} owner(s) as initiated for today")
        
        # Get tasks already sent today
        sent_today = get_sent_reminders_today(ti)
        
        # Get current date/time in UTC
        now_utc = datetime.now(timezone.utc)
        today_utc = now_utc.date()
        
        # Calculate date boundaries
        today_start = datetime.combine(today_utc, time.min).replace(tzinfo=timezone.utc)
        today_end = datetime.combine(today_utc, time.max).replace(tzinfo=timezone.utc)
        
        # Tasks overdue by MORE than 72 hours (3 days)
        overdue_cutoff = today_start - timedelta(days=3)
        
        logging.info(f"📅 Today: {today_utc.isoformat()}")
        logging.info(f"📅 Collecting tasks:")
        logging.info(f"   1. Due TODAY: {today_utc.isoformat()}")
        logging.info(f"   2. Overdue by >72 hours: Before {overdue_cutoff.date().isoformat()}")
        logging.info(f"   ❌ EXCLUDING: Tasks already sent today ({len(sent_today)} tasks)")
        
        tasks_by_owner = {}
        
        for owner in owners:
            owner_id = owner.get("id")
            owner_name = owner.get("name")
            owner_email = owner.get("email")
            tz_str = owner.get("timezone", "America/New_York")
            
            try:
                logging.info(f"✓ Processing {owner_name}...")
                
                # Query 1: Tasks due TODAY
                prompt_today = f"""You are a HubSpot API assistant. Find tasks for owner {owner_id} that are due TODAY ONLY.

Owner ID: {owner_id}
Owner Name: {owner_name}
Today's Date: {today_utc.isoformat()}

Steps:
1. Invoke search_tasks with filters:
   - hubspot_owner_id = {owner_id}
   - hs_task_status = "COMPLETED" with Operator = NOT_CONTAINS_TOKEN
   - hs_timestamp (due date) GTE {today_start.isoformat()} AND LTE {today_end.isoformat()}
2. Return all matching tasks with these properties:
   - id, hs_task_subject, hs_task_body, hs_timestamp, hs_task_priority, hs_task_status, hubspot_owner_id

Important: Always include the filter `hs_task_status` as `COMPLETED` with operator "NOT_CONTAINS_TOKEN".
Return ONLY valid JSON:
{{
    "tasks": [],
    "total": 0,
    "query_type": "due_today"
}}"""

                # Query 2: Tasks overdue by MORE than 72 hours
                prompt_overdue = f"""You are a HubSpot API assistant. Find tasks for owner {owner_id} that are overdue by MORE than 72 hours.

Owner ID: {owner_id}
Owner Name: {owner_name}
Overdue Cutoff: {overdue_cutoff.date().isoformat()} (tasks due BEFORE this date)

Steps:
1. Invoke search_tasks with filters:
   - hubspot_owner_id = {owner_id}
   - hs_task_status = "COMPLETED" with Operator = NOT_CONTAINS_TOKEN
   - hs_timestamp (due date) LT {overdue_cutoff.isoformat()}
2. Return all matching tasks with these properties:
   - id, hs_task_subject, hs_task_body, hs_timestamp, hs_task_priority, hs_task_status, hubspot_owner_id
Important: Always include the filter `hs_task_status` as `COMPLETED` with operator "NOT_CONTAINS_TOKEN".
Return ONLY valid JSON:
{{
    "tasks": [],
    "total": 0,
    "query_type": "overdue_72h"
}}"""

                # Execute both queries
                response_today = get_ai_response(prompt_today, expect_json=True)
                response_overdue = get_ai_response(prompt_overdue, expect_json=True)
                
                parsed_today = json.loads(response_today.strip())
                parsed_overdue = json.loads(response_overdue.strip())
                
                tasks_today = parsed_today.get("tasks", [])
                tasks_overdue = parsed_overdue.get("tasks", [])
                
                # Combine both lists (remove duplicates if any)
                all_tasks = tasks_today + tasks_overdue
                unique_tasks = {task["id"]: task for task in all_tasks}.values()
                
                # Filter out tasks already sent today
                tasks = [task for task in unique_tasks if task["id"] not in sent_today]
                
                if tasks:
                    tasks_by_owner[owner_id] = {
                        "owner_info": {
                            "id": owner_id,
                            "name": owner_name,
                            "email": owner_email,
                            "timezone": tz_str
                        },
                        "tasks": tasks
                    }
                    filtered_count = len(all_tasks) - len(tasks)
                    logging.info(f"Found {len(tasks)} NEW task(s) for {owner_name}:")
                    logging.info(f"  - {len(tasks_today)} due today")
                    logging.info(f"  - {len(tasks_overdue)} overdue >72 hours")
                    if filtered_count > 0:
                        logging.info(f"  - Filtered out {filtered_count} already sent today")
                
            except Exception as e:
                logging.error(f"Error collecting tasks for owner {owner_name}: {e}")
                continue
        
        ti.xcom_push(key="tasks_by_owner", value=tasks_by_owner)
        logging.info(f"✓ Collected tasks for {len(tasks_by_owner)} owner(s)")
        return tasks_by_owner
        
    except Exception as e:
        logging.error(f"Failed to collect due tasks: {e}")
        ti.xcom_push(key="tasks_by_owner", value={})
        return {}

def send_spaced_reminders(ti, **context):
    """Send task reminder emails with 3-minute spacing - ONE EMAIL PER TASK"""
    try:
        tasks_by_owner = ti.xcom_pull(key="tasks_by_owner", default={})
        
        if not tasks_by_owner:
            logging.info("No tasks to send reminders for")
            ti.xcom_push(key="sent_reminders", value=[])
            return []
        
        service = authenticate_gmail()
        if not service:
            raise ValueError("Gmail authentication failed")
        
        sent_reminders = []
        
        # Flatten all tasks with owner info
        all_task_items = []
        for owner_id, data in tasks_by_owner.items():
            owner_info = data["owner_info"]
            for task in data["tasks"]:
                all_task_items.append({
                    "task": task,
                    "owner_info": owner_info
                })
        
        total_tasks = len(all_task_items)
        logging.info(f"📧 Sending {total_tasks} SEPARATE email(s) with {EMAIL_SPACING_MINUTES}-minute spacing")
        
        # Log multi-task owners
        owner_task_counts = {}
        for item in all_task_items:
            owner_name = item["owner_info"]["name"]
            owner_task_counts[owner_name] = owner_task_counts.get(owner_name, 0) + 1
        
        for owner_name, count in owner_task_counts.items():
            if count > 1:
                logging.info(f"⚠️  {owner_name} has {count} tasks → will receive {count} SEPARATE emails")
        
        # Send each task as a separate email
        for idx, item in enumerate(all_task_items, 1):
            task = item["task"]
            owner_info = item["owner_info"]
            task_id = task.get("id")
            
            logging.info(f"📨 [{idx}/{total_tasks}] Sending email for task: {task.get('hs_task_subject', 'Untitled')} to {owner_info['name']}")
            
            # Send email
            result = send_task_reminder_email(
                service=service,
                task=task,
                owner_info=owner_info
            )
            
            # Mark as sent if successful
            if result.get("success"):
                mark_reminder_sent(ti, task_id)
            
            sent_reminders.append(result)
            
            # Wait between emails (except for last one)
            if idx < total_tasks:
                import time
                wait_seconds = EMAIL_SPACING_MINUTES * 60
                logging.info(f"⏳ Waiting {EMAIL_SPACING_MINUTES} minutes before next email...")
                time.sleep(wait_seconds)
        
        # Track sent reminders for monitoring replies
        ti.xcom_push(key="sent_reminders", value=sent_reminders)
        logging.info(f"✅ Successfully sent {len(sent_reminders)} task reminder emails")
        
        return sent_reminders
        
    except Exception as e:
        logging.error(f"Failed to send spaced reminders: {e}")
        ti.xcom_push(key="sent_reminders", value=[])
        return []

def skip_task_collection(ti, **context):
    """Dummy task when outside delivery window"""
    logging.info("Skipped task collection - outside delivery window or already initiated")
    return "Outside delivery window or already initiated"

def safe_json_loads(text, default=None):
    if default is None:
        default = {}
    if not text:
        return default
    text = text.strip()
    # Try to extract JSON block if wrapped in ```
    match = re.search(r'\{.*\}', text, re.DOTALL)
    if match:
        text = match.group(0)
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        logging.warning(f"JSON decode failed: {e}\nRaw output: {text!r}")
        return default

def create_default_followups(ti, **context):
    """Create default follow-up tasks for reminders without same-day replies"""
    try:
        # This task runs at end of day to check which reminders didn't get replies
        sent_reminders = ti.xcom_pull(key="sent_reminders", default=[])
        
        if not sent_reminders:
            logging.info("No sent reminders to check")
            return []
        
        # Get current date
        today = datetime.now(timezone.utc).date()
        
        created_followups = []
        
        for reminder in sent_reminders:
            if not reminder.get("success"):
                continue
                
            task_id = reminder.get("task_id")
            message_id = reminder.get("message_id")
            thread_id = reminder.get("thread_id")
            
            # Check if this task got a reply today
            check_prompt = f"""Check if task {task_id} received an email reply today.

Task ID: {task_id}
Email thread ID: {thread_id}
Date to check: {today.isoformat()}

Steps:
1. Search for emails in thread {thread_id} received today
2. Check if any email references task ID {task_id}
3. Return whether reply was received

Return ONLY valid JSON:
{{
    "reply_received": true|false,
    "reply_time": "ISO timestamp or null"
}}"""

            response = get_ai_response(check_prompt, expect_json=True)
            parsed = safe_json_loads(response, default={})
            
            # If no reply, create follow-up task
            if not parsed.get("reply_received", False):
                followup_due = datetime.now() + timedelta(weeks=DEFAULT_FOLLOWUP_WEEKS)
                
                create_prompt = f"""Create default follow-up task for task {task_id}.

Original task ID: {task_id}
Due date: {followup_due.isoformat()}
Reason: No reply received to daily reminder

Steps:
1. Get original task details from task {task_id}
2. Create new task with:
   - hs_task_subject: "Follow-up: [original task subject]"
   - hs_task_body: "Follow-up task - no response to daily reminder for task {task_id}"
   - hs_timestamp: {followup_due.isoformat()}
   - hubspot_owner_id: [same as original]
   - hs_task_priority: [same as original]
3. Add note: "Auto-created: No response to daily task reminder by end of day"
4. Associate with same contacts/deals as original task

Return new task ID."""

                create_response = get_ai_response(create_prompt, expect_json=True)
                create_parsed = json.loads(create_response.strip())
                
                created_followups.append({
                    "original_task_id": task_id,
                    "followup_task_id": create_parsed.get("task_id"),
                    "reason": "no_reply"
                })
                
                logging.info(f"✓ Created default follow-up for task {task_id}")
        
        ti.xcom_push(key="created_followups", value=created_followups)
        logging.info(f"✓ Created {len(created_followups)} default follow-up tasks")
        return created_followups
        
    except Exception as e:
        logging.error(f"Failed to create default followups: {e}")
        return []

# ============================================================================
# DAG DEFINITION
# ============================================================================

with DAG(
    "hubspot_daily_task_reminders",
    default_args=default_args,
    schedule_interval="*/15 * * * *",  # Run every 15 minutes to catch delivery windows
    catchup=False,
    tags=["hubspot", "tasks", "reminders", "daily"]
) as dag:

    # Task definitions
    get_owners = PythonOperator(
        task_id="get_all_task_owners",
        python_callable=get_all_task_owners,
        provide_context=True,
    )

    check_window = BranchPythonOperator(
        task_id="check_delivery_window",
        python_callable=check_delivery_window,
        provide_context=True,
    )

    collect_tasks = PythonOperator(
        task_id="collect_due_tasks",
        python_callable=collect_due_tasks,
        provide_context=True,
    )

    send_reminders = PythonOperator(
        task_id="send_spaced_reminders",
        python_callable=send_spaced_reminders,
        provide_context=True,
    )

    skip_collection = PythonOperator(
        task_id="skip_task_collection",
        python_callable=skip_task_collection,
        provide_context=True,
    )

    create_followups = PythonOperator(
        task_id="create_default_followups",
        python_callable=create_default_followups,
        provide_context=True,
    )

    # Dependencies
    get_owners >> check_window
    check_window >> collect_tasks >> send_reminders >> create_followups
    check_window >> skip_collection