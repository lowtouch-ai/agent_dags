import base64
import logging
import json
import re
from datetime import datetime, timedelta, timezone
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import pytz
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from ollama import Client
import holidays

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
HUBSPOT_API_KEY = Variable.get("ltai.v3.husbpot.api.key")  # Note: original variable name had typo
HUBSPOT_BASE_URL = Variable.get("ltai.v3.hubspot.url")
# Email spacing configuration
EMAIL_SPACING_MINUTES = 3
DELIVERY_START_HOUR = 9  # Start sending at 9 AM local time

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def is_business_day(owner_country, check_date):
    """Check if the given date is a business day (not weekend or public holiday) for the owner's country"""
    if not owner_country:
        logging.info("No country specified for owner - assuming business day")
        return check_date.weekday() < 5  # Only exclude weekends

    try:
        # Parse country code: support "US" or "US_NY"
        parts = owner_country.split("_", 1)
        country = parts[0]
        subdiv = parts[1] if len(parts) > 1 else None

        # Create holidays object (years: current + next for safety)
        year = check_date.year
        hol = holidays.country_holidays(country, subdiv=subdiv, years=[year, year + 1])

        is_holiday = check_date in hol
        is_weekend = check_date.weekday() >= 5

        if is_holiday:
            logging.info(f"{check_date} is a public holiday in {owner_country}: {hol.get(check_date)}")
        if is_weekend:
            logging.info(f"{check_date} is a weekend")

        return not (is_weekend or is_holiday)

    except Exception as e:
        logging.warning(f"Failed to check holidays for country {owner_country}: {e}. Falling back to weekend-only check.")
        return check_date.weekday() < 5

def get_holiday_countries():
    """Load holiday country config from Airflow Variable (optional override)"""
    try:
        config_json = Variable.get("ltai.v3.hubspot.task.holiday_countries", default_var="{}")
        config = json.loads(config_json)
        logging.info(f"Loaded holiday config for countries: {list(config.keys())}")
        return config
    except Exception as e:
        logging.warning(f"Failed to load holiday countries config: {e}. No custom holidays.")
        return {}


def get_configured_owners():
    """Load task owners from Airflow Variables"""
    try:
        owners_json = DEFAULT_OWNER_DETAILS
        owners = json.loads(owners_json)
        
        if not owners:
            logging.warning("No task owners configured in ltai.v3.hubspot.task.owners variable")
            logging.warning("Using default owner as fallback")
            owners = [{
                "id": DEFAULT_OWNER_ID,
                "name": DEFAULT_OWNER_NAME,
                "email": HUBSPOT_FROM_ADDRESS,
                "timezone": "America/New_York"
            }]
        
        # Validate owner structure
        for owner in owners:
            required_fields = ["id", "name", "email", "timezone"]
            for field in required_fields:
                if field not in owner:
                    raise ValueError(f"Owner missing required field '{field}': {owner}")
        
        logging.info(f"Loaded {len(owners)} configured task owner(s)")
        for owner in owners:
            logging.info(f" - {owner['name']} ({owner['email']}) in {owner['timezone']}")
        
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

def get_initiated_owners_today(ti, owner_id, owner_timezone):
    """Check if this specific owner was initiated today in THEIR timezone"""
    try:
        owner_tz = pytz.timezone(owner_timezone)
        owner_local_date = datetime.now(timezone.utc).astimezone(owner_tz).strftime("%Y-%m-%d")
        
        key = f"initiated_owners_{owner_local_date}_{owner_id}"
        
        initiated = ti.xcom_pull(key=key, task_ids=None, include_prior_dates=True, dag_id=ti.dag_id)
        return initiated is not None
    except Exception as e:
        logging.warning(f"Failed to check initiated status: {e}")
        return False

def mark_owner_initiated(ti, owner_id, owner_timezone):
    """Mark that daily reminders have been initiated for this owner today (in their timezone)"""
    try:
        # Use owner's local date, not UTC
        owner_tz = pytz.timezone(owner_timezone)
        owner_local_date = datetime.now(timezone.utc).astimezone(owner_tz).strftime("%Y-%m-%d")
        
        key = f"initiated_owners_{owner_local_date}_{owner_id}"  # Include owner_id in key
        
        ti.xcom_push(key=key, value=True)
        logging.info(f"Marked owner {owner_id} as initiated on {owner_local_date}")
    except Exception as e:
        logging.error(f"Failed to mark owner {owner_id} as initiated: {e}")

def get_sent_reminders_today(ti, owner_id, owner_timezone):
    """Get set of task IDs sent today for this owner in THEIR timezone"""
    try:
        owner_tz = pytz.timezone(owner_timezone)
        owner_local_date = datetime.now(timezone.utc).astimezone(owner_tz).strftime("%Y-%m-%d")
        
        key = f"sent_reminders_{owner_local_date}_{owner_id}"

        # Try current run XCom first
        sent_list = ti.xcom_pull(key=key, task_ids=None, include_prior_dates=False)
        if sent_list is not None:
            sent_set = set(sent_list)
            logging.info(f" Loaded {len(sent_set)} sent reminder(s) for owner {owner_id} from current run")
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
            logging.info(f" Loaded {len(sent_set)} sent reminder(s) for owner {owner_id} from prior runs")
            return sent_set

        logging.info(f" No sent reminders found for owner {owner_id} today")
        return set()

    except Exception as e:
        logging.warning(f"Failed to load sent reminders for owner {owner_id}: {e}")
        return set()

def mark_reminder_sent(ti, task_id, owner_id, owner_timezone):
    """Mark that this task was sent today for this owner in THEIR timezone"""
    try:
        owner_tz = pytz.timezone(owner_timezone)
        owner_local_date = datetime.now(timezone.utc).astimezone(owner_tz).strftime("%Y-%m-%d")
        
        key = f"sent_reminders_{owner_local_date}_{owner_id}"
        
        current_sent = list(get_sent_reminders_today(ti, owner_id, owner_timezone))
        if task_id not in current_sent:
            current_sent.append(task_id)
            ti.xcom_push(key=key, value=current_sent)
            logging.info(f"✓ Marked task {task_id} as sent for owner {owner_id} on {owner_local_date} (total: {len(current_sent)})")
        else:
            logging.debug(f"Task {task_id} already marked as sent for owner {owner_id}")
    except Exception as e:
        logging.error(f"Failed to mark task {task_id} as sent for owner {owner_id}: {e}")

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
                "content": "You are a JSON-only API. Always respond with valid JSON objects. Never include explanatory text, HTML, or markdown formatting."
            })

        if conversation_history:
            for item in conversation_history:
                if "role" in item and "content" in item:
                    messages.append({"role": item["role"], "content": item["content"]})

        messages.append({"role": "user", "content": prompt})
        response = client.chat(model='hubspot:v6af_cl', messages=messages, stream=False)
        ai_content = response.message.content
        ai_content = re.sub(r'```(?:html|json)\n?|```', '', ai_content)
        return ai_content.strip()
    except Exception as e:
        logging.error(f"Error in get_ai_response: {e}")
        raise

def send_task_reminder_email(service, task, owner_info):
    """Send clean HubSpot task reminder with unified recent activity"""
    try:
        owner_email = owner_info.get("email", "").strip()
        owner_first_name = owner_info.get("name", "User").split()[0].strip() or "there"
        owner_full_name = owner_info.get("name", "The HubSpot Assistant Team")
        owner_timezone = owner_info.get("timezone", "America/New_York")

        if not owner_email:
            raise ValueError("Owner email missing")

        props = task.get("properties", {})
        task_id = task.get("id", "N/A")

        # Task Name
        task_name = props.get("hs_task_subject", "").strip() or props.get("hs_task_body", "").strip().split('\n')[0][:100] or "Unnamed Task"

        # Due Date - full date for display - CHECK AGAINST OWNER'S LOCAL DATE
        due_date_ms = props.get("hs_timestamp")
        formatted_due = "Not specified"
        is_overdue = False
        
        # Use owner's local date for overdue check
        owner_tz = pytz.timezone(owner_timezone)
        owner_local_date = datetime.now(timezone.utc).astimezone(owner_tz).date()

        if due_date_ms:
            try:
                if isinstance(due_date_ms, str):
                    due_dt = datetime.fromisoformat(due_date_ms.replace('Z', '+00:00'))
                else:
                    due_dt = datetime.fromtimestamp(int(due_date_ms) / 1000, tz=timezone.utc)
                
                # Convert due date to owner's timezone for comparison
                due_dt_owner_tz = due_dt.astimezone(owner_tz)
                formatted_due = due_dt_owner_tz.strftime("%B %d, %Y")
                
                # Check if overdue based on owner's local date
                if due_dt_owner_tz.date() < owner_local_date:
                    is_overdue = True
            except Exception as e:
                logging.warning(f"Failed to parse due date: {e}")

        priority = props.get("hs_task_priority", "Not set").title()

        # Associations
        associations = task.get("associations", {})

        # Contacts
        contact_section = ""
        contact_lines = []
        for contact in associations.get("contacts", []):
            cp = contact.get("properties", {})
            contact_id = contact.get("id", "N/A")
            name = f"{cp.get('firstname', '')} {cp.get('lastname', '')}".strip() or "Unknown"
            email = cp.get("email", "")
    
            contact_lines.append(f'<li><strong>Contact ID:</strong> {contact_id}</li>')
            contact_lines.append(f'<li><strong>Name:</strong> {name}</li>')
            if email:
                contact_lines.append(f'<li><strong>Email:</strong> <a href="mailto:{email}">{email}</a></li>')

        if contact_lines:
            contact_section = f"""
            <li><strong>Contacts:</strong>
                <ul>{''.join(contact_lines)}</ul>
            </li>
            """

        # Company
        company_section = ""
        companies = associations.get("companies", [])
        if companies:
            company_name = companies[0].get("properties", {}).get("name", "Unknown Company").strip()
            company_id = companies[0].get("id", "N/A")
            company_section = f"""
            <li><strong>Company:</strong>
                <ul>
                    <li><strong>Company ID:</strong> {company_id}</li>
                    <li><strong>Company Name:</strong> {company_name}</li>
                </ul>
            </li>
            """

        # Deal - only if exists
        deal_section = ""
        deals = associations.get("deals", [])
        if deals:
            deal = deals[0].get("properties", {})
            deal_id = deals[0].get("id", "N/A")
            deal_name = deal.get("dealname", "Unknown Deal")
            amount = deal.get("amount", "")
            if amount:
                try:
                    amount = f"${float(amount):,.0f}"
                except:
                    pass
            close_date_raw = deal.get("closedate")
            close_date = ""
            if close_date_raw:
                try:
                    if isinstance(close_date_raw, str):
                        cd_dt = datetime.fromisoformat(close_date_raw.replace('Z', '+00:00'))
                    else:
                        cd_dt = datetime.fromtimestamp(int(close_date_raw) / 1000, tz=timezone.utc)
                    # Convert to owner's timezone
                    cd_dt_owner_tz = cd_dt.astimezone(owner_tz)
                    close_date = cd_dt_owner_tz.strftime("%B %d, %Y")
                except:
                    close_date = ""

            deal_lines = []
            deal_lines.append(f"<li><strong>Deal ID:</strong> {deal_id}</li>")
            deal_lines.append(f"<li><strong>Deal Name:</strong> {deal_name}</li>")
            if amount:
                deal_lines.append(f"<li><strong>Amount:</strong> {amount}</li>")
            if close_date:
                deal_lines.append(f"<li><strong>Close Date:</strong> {close_date}</li>")

            deal_section = f"""
            <li><strong>Deal:</strong>
                <ul>{''.join(deal_lines)}</ul>
            </li>
            """

        # Recent Activity (Last 1 Month) - clean text, no HTML wrappers
        activity_lines = []
        activities = sorted(
            task.get("activities", []),
            key=lambda x: x.get("properties", {}).get("hs_timestamp", 0) or 0,
            reverse=True
        )

        for act in activities:
            props = act.get("properties", {})
            timestamp = props.get("hs_timestamp")
            act_date = ""
            if timestamp:
                try:
                    act_dt = datetime.fromtimestamp(int(timestamp) / 1000, tz=timezone.utc)
                    # Convert to owner's timezone
                    act_dt_owner_tz = act_dt.astimezone(owner_tz)
                    act_date = act_dt_owner_tz.strftime("%B %d, %Y")
                except:
                    pass
            prefix = f"{act_date} - " if act_date else ""

            parts = []

            # Get and clean note body if present
            if "hs_note_body" in props:
                raw_body = props["hs_note_body"].strip()
                if raw_body:
                    # Remove full <html><head>...</head><body>...</body></html> wrapper if present
                    if raw_body.lower().startswith("<html"):
                        # Simple extraction: find content between <body> and </body>
                        body_match = re.search(r'<body[^>]*>(.*?)</body>', raw_body, re.DOTALL | re.IGNORECASE)
                        if body_match:
                            cleaned = body_match.group(1).strip()
                        else:
                            # Fallback: strip all tags
                            cleaned = re.sub(r'<[^>]+>', '', raw_body).strip()
                    else:
                        cleaned = raw_body

                    # Final cleanup: unescape common entities and remove extra whitespace
                    cleaned = cleaned.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
                    cleaned = re.sub(r'\s+', ' ', cleaned).strip()

                    if cleaned:
                        parts.append(cleaned)

            # Add task subject if present
            if "hs_task_subject" in props:
                subject = props["hs_task_subject"].strip()
                if subject:
                    status = props.get("hs_task_status", "").title()
                    if status and status.lower() not in ["not started", ""]:
                        subject += f" ({status})"
                    parts.append(subject)

            # Fallback: task body first line
            elif "hs_task_body" in props:
                body_line = props["hs_task_body"].strip().split('\n')[0].strip()
                if body_line:
                    status = props.get("hs_task_status", "").title()
                    if status and status.lower() not in ["not started", ""]:
                        body_line += f" ({status})"
                    parts.append(body_line)

            # Combine all parts
            if parts:
                content = " • ".join(parts)
                # Final safe HTML escaping for email
                content = content.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                activity_lines.append(f"<li>{prefix}{content}</li>")

        # AI Summary
        summary_html = task.get("summary_html", "").strip()
        if not summary_html:
            summary_html = "No additional summary available."

        # Subject: due today or overdue
        status_text = "overdue" if is_overdue else "due today"

        subject = f"Task Reminder: \"{task_name}\" is {status_text}"
        if is_overdue:
            subject = f" OVERDUE: {subject}"
        else:
            subject = f" DUE TODAY: {subject}"

        # Final HTML
        email_html = f"""
<html>
<body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333; max-width: 700px;">
    <p>Hi {owner_full_name},</p>
    <p>This is a reminder that the following task is <strong>{status_text}</strong>. Please find the details below.</p>

    <p><strong>Task Details</strong></p>
    <ul>
        <li><strong>Task Name:</strong> {task_name}</li>
        <li><strong>Task ID:</strong> {task_id}</li>
        <li><strong>Due Date:</strong> {formatted_due}</li>
        <li><strong>Priority:</strong> {priority}</li>
        <li><strong>Owner:</strong> {owner_full_name}</li>
    </ul>

    <p><strong>Associated Records</strong></p>
    <ul>
        {contact_section}
        {company_section}
        {deal_section}
    </ul>

    <p><strong>Recent Activity (Last 1 Month)</strong></p>
    <ul>
        {''.join(activity_lines) or '<li>No recent activity</li>'}
    </ul>

    <p><strong>Summary</strong></p>
    <p>{summary_html}</p>

    <p>Please let me know if you need any additional details or support.</p>

    <p>Best regards,<br>
    The HubSpot Assistant Team<br>
    <a href="http://lowtouch.ai" class="company">lowtouch.ai</a></p>
</body>
</html>
"""

        msg = MIMEMultipart()
        msg["From"] = f"HubSpot Task Reminder via lowtouch.ai <{HUBSPOT_FROM_ADDRESS}>"
        msg["To"] = owner_email
        msg["Subject"] = subject
        msg["X-Task-ID"] = task_id
        msg["X-Task-Type"] = "daily-reminder"
        msg.attach(MIMEText(email_html, "html"))

        raw_msg = base64.urlsafe_b64encode(msg.as_string().encode("utf-8")).decode("utf-8")
        result = service.users().messages().send(userId="me", body={"raw": raw_msg}).execute()

        logging.info(f"Sent reminder: Task {status_text} - {task_name} to {owner_email}")
        return {"success": True, "task_id": task_id}

    except Exception as e:
        logging.error(f"Send failed for task {task.get('id', 'unknown')}: {str(e)}")
        return {"success": False, "error": str(e)}

def search_hubspot_tasks(owner_id, filters):
    try:
        endpoint = f"{HUBSPOT_BASE_URL}/crm/v3/objects/tasks/search"
        headers = {"Authorization": f"Bearer {HUBSPOT_API_KEY}", "Content-Type": "application/json"}
        payload = {
            "filterGroups": [{"filters": filters}],
            "properties": ["hs_task_subject", "hs_task_body", "hs_timestamp", "hs_task_priority", "hs_task_status", "hubspot_owner_id"],
            "limit": 100
        }
        response = requests.post(endpoint, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.error(f"Failed to search tasks for owner {owner_id}: {e}")
        return {"results": []}

def get_task_associations(task_id):
    associations = {"contacts": [], "companies": [], "deals": []}
    headers = {"Authorization": f"Bearer {HUBSPOT_API_KEY}", "Content-Type": "application/json"}
    for assoc_type in ["contacts", "companies", "deals"]:
        try:
            endpoint = f"{HUBSPOT_BASE_URL}/crm/v4/objects/tasks/{task_id}/associations/{assoc_type}"
            response = requests.get(endpoint, headers=headers, timeout=30)
            if response.status_code == 200:
                results = response.json().get("results", [])
                for result in results:
                    assoc_id = result.get("toObjectId")
                    if assoc_id:
                        details = get_object_details(assoc_type, assoc_id)
                        if details:
                            associations[assoc_type].append(details)
        except Exception as e:
            logging.warning(f"Failed to get {assoc_type} associations: {e}")
    return associations

def get_object_details(object_type, object_id):
    try:
        headers = {"Authorization": f"Bearer {HUBSPOT_API_KEY}"}
        properties = {"contacts": ["firstname", "lastname", "email"], "companies": ["name", "domain"], "deals": ["dealname", "dealstage", "amount"]}
        props = properties.get(object_type, [])
        props_param = "&".join([f"properties={p}" for p in props])
        endpoint = f"{HUBSPOT_BASE_URL}/crm/v3/objects/{object_type}/{object_id}?{props_param}"
        response = requests.get(endpoint, headers=headers, timeout=30)
        if response.status_code == 200:
            data = response.json()
            return {"id": object_id, "properties": data.get("properties", {})}
        return None
    except Exception as e:
        logging.warning(f"Failed to get details for {object_type} {object_id}: {e}")
        return None

def get_deal_activities(deal_id, start_date, end_date):
    try:
        activities = {"notes": [], "tasks": []}
        headers = {"Authorization": f"Bearer {HUBSPOT_API_KEY}", "Content-Type": "application/json"}
        # Notes
        notes_payload = {
            "filterGroups": [{"filters": [
                {"propertyName": "associations.deal", "operator": "CONTAINS_TOKEN", "value": str(deal_id)},
                {"propertyName": "hs_timestamp", "operator": "GTE", "value": start_date},
                {"propertyName": "hs_timestamp", "operator": "LTE", "value": end_date}
            ]}],
            "properties": ["hs_note_body", "hs_timestamp"],
            "sorts": [{"propertyName": "hs_timestamp", "direction": "DESCENDING"}],
            "limit": 100
        }
        notes_resp = requests.post(f"{HUBSPOT_BASE_URL}/crm/v3/objects/notes/search", headers=headers, json=notes_payload, timeout=30)
        if notes_resp.status_code == 200:
            activities["notes"] = notes_resp.json().get("results", [])

        # Tasks
        tasks_payload = notes_payload.copy()
        tasks_payload["properties"] = ["hs_task_subject", "hs_task_body", "hs_timestamp", "hs_task_status"]
        tasks_resp = requests.post(f"{HUBSPOT_BASE_URL}/crm/v3/objects/tasks/search", headers=headers, json=tasks_payload, timeout=30)
        if tasks_resp.status_code == 200:
            activities["tasks"] = tasks_resp.json().get("results", [])

        return activities
    except Exception as e:
        logging.error(f"Failed to get activities for deal {deal_id}: {e}")
        return {"notes": [], "tasks": []}

# ============================================================================
# TASK FUNCTIONS
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
    """Check if we're in the delivery window AND it's a business day for any owner"""
    try:
        owners = ti.xcom_pull(key="all_owners", default=[])

        if not owners:
            logging.info("No owners found, skipping delivery window check")
            ti.xcom_push(key="in_delivery_window", value=False)
            return "skip_task_collection"
        holiday_config = get_holiday_countries()  # Load once

        current_utc = datetime.now(timezone.utc)
        today_utc_date = current_utc.date()

        in_window_count = 0
        owners_to_process = []

        for owner in owners:
            owner_id = owner.get("id")
            tz_str = owner.get("timezone", "America/New_York")
            
            # CHANGE THIS - check per owner with their timezone
            if get_initiated_owners_today(ti, owner_id, tz_str):
                logging.info(f"⏭️  Owner {owner.get('name')} already initiated today - skipping")
                continue

            owner_country = owner.get("country", "US")  # New field

            try:
                owner_tz = pytz.timezone(tz_str)
                owner_local_time = current_utc.astimezone(owner_tz)
                owner_local_date = owner_local_time.date()
                owner_hour = owner_local_time.hour

                # Check if it's a business day in owner's local date
                if not is_business_day(owner_country, owner_local_date):
                    logging.info(f"Owner {owner.get('name')} ({owner_country}) - today {owner_local_date} is not a business day - skipping reminders")
                    continue

                # Check delivery hour window
                if DELIVERY_START_HOUR <= owner_hour:
                    in_window_count += 1
                    owners_to_process.append(owner)
                    logging.info(f"Owner {owner.get('name')} ({tz_str}, {owner_country}) is in delivery window on business day: {owner_local_time.strftime('%I:%M %p %Z on %Y-%m-%d')}")
                else:
                    logging.info(f"Owner {owner.get('name')} - too early ({owner_hour}:00, need >= {DELIVERY_START_HOUR})")

            except Exception as e:
                logging.warning(f"Error processing owner {owner.get('name')}: {e}")

        ti.xcom_push(key="owners_to_process", value=owners_to_process)
        in_window = in_window_count > 0
        ti.xcom_push(key="in_delivery_window", value=in_window)

        if in_window:
            logging.info(f"✓ {in_window_count} owner(s) eligible (business day + in window) - proceeding")
            return "collect_due_tasks"
        else:
            logging.info("✗ No owners eligible today (holiday/weekend or outside window or already initiated)")
            return "skip_task_collection"

    except Exception as e:
        logging.error(f"Error checking delivery window: {e}")
        ti.xcom_push(key="in_delivery_window", value=False)
        return "skip_task_collection"

def collect_due_tasks(ti, **context):
    """Collect tasks due today or overdue for each owner, using their local timezone"""
    owners = ti.xcom_pull(key="owners_to_process", default=[])
    if not owners:
        ti.xcom_push(key="tasks_by_owner", value={})
        return {}

    # Mark all owners as initiated for today (in their timezone)
    for owner in owners:
        mark_owner_initiated(ti, owner["id"], owner["timezone"])

    now_utc = datetime.now(timezone.utc)
    one_month_ago = now_utc - timedelta(days=30)

    tasks_by_owner = {}

    for owner in owners:
        owner_id = owner["id"]
        owner_name = owner["name"]
        owner_email = owner["email"]
        tz = owner["timezone"]

        logging.info(f"Collecting tasks for {owner_name}")
        sent_today = get_sent_reminders_today(ti, owner_id=owner_id, owner_timezone=tz)

        # Calculate date boundaries in OWNER'S timezone
        owner_tz = pytz.timezone(tz)
        owner_local_now = now_utc.astimezone(owner_tz)
        owner_local_date = owner_local_now.date()
        
        # Start of owner's local day (converted to UTC for API query)
        owner_day_start = owner_tz.localize(
            datetime(owner_local_date.year, owner_local_date.month, owner_local_date.day)
        ).astimezone(timezone.utc)
        
        # End of owner's local day (converted to UTC for API query)
        owner_day_end = owner_day_start + timedelta(days=1) - timedelta(seconds=1)
        
        # Overdue cutoff: 3 days before owner's local day start
        owner_overdue_cutoff = owner_day_start - timedelta(days=3)

        logging.info(f"Owner {owner_name} local date: {owner_local_date}, UTC range: {owner_day_start} to {owner_day_end}")

        # Due today (in owner's timezone)
        filters_today = [
            {"propertyName": "hubspot_owner_id", "operator": "EQ", "value": owner_id},
            {"propertyName": "hs_task_status", "operator": "NOT_CONTAINS_TOKEN", "value": "COMPLETED"},
            {"propertyName": "hs_timestamp", "operator": "GTE", "value": int(owner_day_start.timestamp() * 1000)},
            {"propertyName": "hs_timestamp", "operator": "LTE", "value": int(owner_day_end.timestamp() * 1000)}
        ]
        today_resp = search_hubspot_tasks(owner_id, filters_today)

        # Overdue >3 days (before owner's local day)
        filters_overdue = [
            {"propertyName": "hubspot_owner_id", "operator": "EQ", "value": owner_id},
            {"propertyName": "hs_task_status", "operator": "NOT_CONTAINS_TOKEN", "value": "COMPLETED"},
            {"propertyName": "hs_timestamp", "operator": "LT", "value": int(owner_overdue_cutoff.timestamp() * 1000)}
        ]
        overdue_resp = search_hubspot_tasks(owner_id, filters_overdue)

        all_tasks = today_resp.get("results", []) + overdue_resp.get("results", [])
        unique_tasks = {t["id"]: t for t in all_tasks}.values()
        tasks = [t for t in unique_tasks if t["id"] not in sent_today]

        for task in tasks:
            task_id = task["id"]
            logging.info(f"Processing task {task_id}")
            task["associations"] = get_task_associations(task_id)
            activities = []
            for deal in task["associations"].get("deals", []):
                deal_id = deal["id"]
                deal_acts = get_deal_activities(deal_id, int(one_month_ago.timestamp() * 1000), int(now_utc.timestamp() * 1000))
                activities.extend(deal_acts.get("notes", []))
                activities.extend(deal_acts.get("tasks", []))
            task["activities"] = activities

            prompt = f"""Summarize the following task and its context for a reminder email in clean HTML:

Task: {json.dumps(task)}
Associations: {json.dumps(task['associations'])}
Recent activities: {json.dumps(activities)}
Important Instructions:
    - Based on the task details, associations, and recent activities, generate a concise HTML summary that says what the task is about and associated deal and what has been done in the last month. Keep it brief and to the point.
    - Do not include the full details again - just a high-level summary.
    - Do not include any Task, Associated Records, or Recent Activity sections - only the summary.
    - the summary should be 2-3 lines max in one paragraph.
    - Do not include any sub heading like task summary, association summary, notes summary, etc.

Use <h4> for headings and concise paragraphs/lists."""
            task["summary_html"] = get_ai_response(prompt)

        if tasks:
            tasks_by_owner[owner_id] = {
                "owner_info": {"id": owner_id, "name": owner_name, "email": owner_email, "timezone": tz},
                "tasks": tasks
            }

    ti.xcom_push(key="tasks_by_owner", value=tasks_by_owner)
    logging.info(f"Collected tasks for {len(tasks_by_owner)} owner(s)")
    return tasks_by_owner

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
                mark_reminder_sent(ti, task_id, owner_info["id"], owner_info["timezone"])

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

# ============================================================================
# DAG DEFINITION
# ============================================================================
with DAG(
    "hubspot_daily_task_reminders",
    default_args=default_args,
    schedule_interval="0 * * * *",
    catchup=False,
    tags=["hubspot", "tasks", "reminders", "daily"],
    description="Send daily HubSpot task reminders (one email per task) during business hours"
) as dag:

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

    get_owners >> check_window
    check_window >> [collect_tasks, skip_collection]
    collect_tasks >> send_reminders