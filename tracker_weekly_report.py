from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import logging
import re
from airflow.models import Variable
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from ollama import Client
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import base64
import requests
from requests.auth import HTTPBasicAuth
import smtplib

# ============================================================================
# CONFIGURATION
# ============================================================================

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# SMTP Configuration from Airflow Variables (following your reference pattern)
SMTP_USER = Variable.get("ltai.v1.tracker.SMTP_USER")
SMTP_PASSWORD = Variable.get("ltai.v1.tracker.SMTP_PASSWORD")
SMTP_HOST = Variable.get("ltai.v1.tracker.SMTP_HOST", default_var="mail.authsmtp.com")
SMTP_PORT = int(Variable.get("ltai.v1.tracker.SMTP_PORT", default_var="2525"))
SMTP_SUFFIX = Variable.get("ltai.v1.tracker.SMTP_FROM_SUFFIX", default_var="via lowtouch.ai <webmaster@ecloudcontrol.com>")
SENDER_EMAIL = Variable.get("ltai.v1.tracker.FROM_ADDRESS", default_var=SMTP_USER)
# Ollama Configuration
OLLAMA_HOST = Variable.get("ltai.v1.tracker.OLLAMA_HOST", "http://agentomatic:8000/")

# Mantis API Configuration
MANTIS_API_BASE = Variable.get("ltai.v1.tracker.MANTIS_API_BASE")
MANTIS_API_KEY = Variable.get("ltai.v1.tracker.MANTIS_API_KEY")
# Add to CONFIGURATION section
MANTIS_TIMESHEET_API_URL = Variable.get("ltai.v1.tracker.MANTIS_TIMESHEET_API_URL")
# Whitelisted Users from Airflow Variable
WHITELISTED_USERS = Variable.get("ltai.v1.tracker.whitelisted_users", deserialize_json=True)
hr_email = Variable.get("ltai.v1.tracker.whitelisted.hr_email", default_var=None)
manager_email = Variable.get("ltai.v1.tracker.whitelisted.manager_email", default_var=None)
admin_email = Variable.get("ltai.v1.tracker.whitelisted.admin_email", default_var=None)
# Evaluation Criteria (embedded)
EVALUATION_CRITERIA = {
    "min_hours": 40,
    "max_hours": 45,
    "max_task_hours": 4,
    "min_daily_updates": 5
}

# Evaluation Prompt Template (embedded)
EVALUATION_PROMPT_TEMPLATE = """You are an expert timesheet auditor. Evaluate the provided timesheet data against these specific criteria:
YOU ARE A JSON-ONLY API. 
DO NOT WRITE ANY TEXT, EXPLANATION, OR NARRATIVE.
IMMEDIATELY OUTPUT THE RAW JSON AND NOTHING ELSE.
1. **Total Time Utilization** (Min 40 hrs/week)
   - Below 40 hrs: 1-2 stars
   - 40-42 hrs: 3 stars
   - 42-45 hrs: 4-5 stars
   - Above 45 hrs: Flag as potential overwork

2. **Task Description Quality**
   - Check for objective/goal statements
   - Check for outcome/result descriptions
   - Assess clarity and completeness
   - For example : If a bug is fixed, the description should include the issue, the fix applied, and the result (e.g., "Fixed login bug by updating authentication flow, resulting in successful logins for 100% of users tested.")

3. **Daily Updates**
   - Count created_at or updated_at with entries and count the number of unique days with updates.
   - Expected: 5 days/week minimum
   - Rate based on consistency

4. **Task Granularity**
   - Identify tasks > 4 hours or the entries that exceeds
   - Count violations
   - Rate based on percentage of compliant tasks.

Return your analysis in this exact JSON structure:
{{
  "week_info": "Week X of Month",
  "ratings": {{
    "total_hours": 3,
    "task_description": 4,
    "daily_updates": 3,
    "task_granularity": 2,
    "time_accuracy": 4
  }},
  "findings": {{
    "total_hours": "Total logged time is X hours, below the minimum expectation.",
    "task_description": "Descriptions are detailed but lack clear outcomes.",
    "daily_updates": "Entries were made across 4 days, not daily.",
    "task_granularity": "Multiple tasks exceed 4 hours in single entries.",
  }},
  "recommendations": {{
    "total_hours": "Target 42–45 hours weekly for consistent compliance.",
    "task_description": "Add objective and result for each task entry.",
    "daily_updates": "Update the timesheet every working day.",
    "task_granularity": "Split work into smaller subtasks.",
  }},
  "final_score": 3.5
}}

RESPOND WITH ONLY THE JSON OBJECT - NO OTHER TEXT."""

# Email Template (embedded)
EMAIL_TEMPLATE = """<!DOCTYPE html>
<html>
<head>
    <style>
        body {{
            font-family: Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 700px;
            margin: 0 auto;
            padding: 20px;
        }}
        .criterion {{
            margin: 20px 0;
            padding: 15px;
            border-left: 4px solid #007bff;
            background-color: #f8f9fa;
        }}
        .stars {{
            color: #FFD700;
            font-size: 20px;
        }}
        .score {{
            font-weight: bold;
            color: #007bff;
        }}
        .findings {{
            margin: 10px 0;
            color: #555;
        }}
        .recommendation {{
            margin: 10px 0;
            color: #0066cc;
            font-style: italic;
        }}
        .final-score {{
            text-align: center;
            font-size: 24px;
            margin: 30px 0;
            padding: 20px;
            background-color: #e7f3ff;
            border-radius: 8px;
        }}
    </style>
</head>
<body>
    <p>Dear {username},</p>
    
    <div class="week-header">
        <h3>{week_info}</h3>
        <p>Review Period: {week_start} to {week_end}</p>
    </div>
    
    <p>As part of our weekly timesheet review process, your timesheet for the above period has been evaluated based on defined productivity, logging, and reporting standards. Please find the summary below.</p>
    
    <div class="criterion">
        <strong>Total Time Utilization (Min 40 hrs/week):</strong> 
        <span class="stars">{total_hours_stars}</span> 
        <span class="score">({total_hours_score}/5)</span>
        <div class="findings"><strong>Findings:</strong> {total_hours_findings}</div>
        <div class="recommendation"><strong>Recommendation:</strong> {total_hours_recommendation}</div>
    </div>
    <!--
    <div class="criterion">
        <strong>Usage of AI / Prompting:</strong> 
        <span class="stars">{ai_usage_stars}</span> 
        <span class="score">({ai_usage_score}/5)</span>
        <div class="findings"><strong>Findings:</strong> {ai_usage_findings}</div>
        <div class="recommendation"><strong>Recommendation:</strong> {ai_usage_recommendation}</div>
    </div>
    -->
    <div class="criterion">
        <strong>Task Details / Description Quality:</strong> 
        <span class="stars">{task_description_stars}</span> 
        <span class="score">({task_description_score}/5)</span>
        <div class="findings"><strong>Findings:</strong> {task_description_findings}</div>
        <div class="recommendation"><strong>Recommendation:</strong> {task_description_recommendation}</div>
    </div>
    
    <div class="criterion">
        <strong>Timely (Daily) Timesheet Updates:</strong> 
        <span class="stars">{daily_updates_stars}</span> 
        <span class="score">({daily_updates_score}/5)</span>
        <div class="findings"><strong>Findings:</strong> {daily_updates_findings}</div>
        <div class="recommendation"><strong>Recommendation:</strong> {daily_updates_recommendation}</div>
    </div>
    
    <div class="criterion">
        <strong>Task Granularity (No Task > 4 Hours):</strong> 
        <span class="stars">{task_granularity_stars}</span> 
        <span class="score">({task_granularity_score}/5)</span>
        <div class="findings"><strong>Findings:</strong> {task_granularity_findings}</div>
        <div class="recommendation"><strong>Recommendation:</strong> {task_granularity_recommendation}</div>
    </div>
    <!--
    <div class="criterion">
        <strong>Time Recording Accuracy (Minutes vs Hours):</strong> 
        <span class="stars">{time_accuracy_stars}</span> 
        <span class="score">({time_accuracy_score}/5)</span>
        <div class="findings"><strong>Findings:</strong> {time_accuracy_findings}</div>
        <div class="recommendation"><strong>Recommendation:</strong> {time_accuracy_recommendation}</div>
    </div>
    -->
    <div class="final-score">
        <strong>Final Score:</strong> 
        <span class="stars">{final_score_stars}</span> 
        <span class="score">({final_score} / 5)</span>
    </div>
    
    <p>Please review the above and report any discrepancies or mistakes in this evaluation within the next 2 working days.</p>
    
    <p>Best Regards,<br>
    --<br>
    Tracker Agent<br>
    HR Manager – (HR & Admin)<br>
    Lowtouch.ai (CLOUD CONTROL SOLUTIONS)<br>
    USA | INDIA | UAE (www.ecloudcontrol.com)</p>
</body>
</html>"""

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_ai_response(prompt, conversation_history=None, expect_json=False):
    """Get response from AI model (Ollama) - following your pattern"""
    try:
        client = Client(host=OLLAMA_HOST, headers={'x-ltai-client': 'timesheet-review'})
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
        response = client.chat(model='appz/tracker:0.4af', messages=messages, stream=False)
        ai_content = response.message.content
        
        # Clean response
        ai_content = re.sub(r'```(?:html|json)\n?|```', '', ai_content)
        return ai_content.strip()
    except Exception as e:
        logging.error(f"AI response error: {str(e)}")
        raise

def get_week_range(execution_date):
    """Calculate the previous week's date range"""
    week_end = execution_date - timedelta(days=1)  # Sunday
    week_start = week_end - timedelta(days=6)  # Monday
    
    # Calculate week number within the month (not ISO week of year)
    week_of_month = (week_start.day - 1) // 7 + 1
    
    return {
        'start': week_start.strftime('%Y-%m-%d'),
        'end': week_end.strftime('%Y-%m-%d'),
        'week_number': week_of_month,  # Week within the month (1-5)
        'month': week_start.strftime('%B'),
        'year': week_start.year
    }

def send_email_smtp(recipient, subject, body_html, cc_list=None):
    """
    Send email via SMTP with CC support - following your reference pattern exactly
    
    Args:
        recipient: Primary recipient email address
        subject: Email subject
        body_html: HTML body content
        cc_list: List of CC email addresses (optional)
    """
    try:
        # Initialize SMTP connection
        logging.info(f"Connecting to SMTP server {SMTP_HOST}:{SMTP_PORT} as {SMTP_USER}")
        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30)
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)

        # Prepare email
        msg = MIMEMultipart("mixed")
        msg["Subject"] = subject
        msg["From"] = f"Tracker Agent {SMTP_SUFFIX}"
        msg["To"] = recipient
        
        # Add CC if provided
        if cc_list and len(cc_list) > 0:
            msg["Cc"] = ", ".join(cc_list)
            logging.info(f"CC: {', '.join(cc_list)}")

        # HTML Part
        msg.attach(MIMEText(body_html, "html"))

        # Prepare recipient list (To + CC)
        all_recipients = [recipient]
        if cc_list:
            all_recipients.extend(cc_list)

        # Send the email
        server.sendmail(SENDER_EMAIL, all_recipients, msg.as_string())
        server.quit()

        logging.info(f"Email sent successfully to {recipient}" + (f" (CC: {', '.join(cc_list)})" if cc_list else ""))
        return True

    except Exception as e:
        logging.error(f"Failed to send email via SMTP: {str(e)}")
        raise

# ============================================================================
# MANTIS API FUNCTIONS
# ============================================================================

def fetch_mantis_timesheet_data(user_email, week_start, week_end):
    """
    Fetch timesheet data from Mantis API for a specific user and week.
    
    This is a placeholder - replace with your actual Mantis API integration.
    """
    try:
        # Example API call - adjust to your Mantis API structure
        endpoint = f"{MANTIS_API_BASE}/timesheets"
        headers = {
            "Authorization": f"Bearer {MANTIS_API_KEY}",
            "Content-Type": "application/json"
        }
        
        params = {
            "user_email": user_email,
            "start_date": week_start,
            "end_date": week_end
        }
        
        logging.info(f"Fetching from {endpoint} for {user_email}")
        response = requests.get(endpoint, headers=headers, params=params, timeout=120)
        response.raise_for_status()
        
        data = response.json()
        
        # Transform to expected format
        entries = data.get("entries", [])
        
        timesheet_data = {
            "user_email": user_email,
            "week_start": week_start,
            "week_end": week_end,
            "total_hours": data.get("total_hours", 0),
            "entries": entries,
            "unique_dates": len(set(entry.get("date") for entry in entries if entry.get("date"))),
            "tasks_over_4_hours": sum(1 for entry in entries if entry.get("hours", 0) > 4),
            "entry_count": len(entries)
        }
        
        logging.info(f"Fetched timesheet for {user_email}: {timesheet_data['total_hours']} hours, {len(entries)} entries")
        return timesheet_data
        
    except requests.exceptions.RequestException as e:
        error_msg = f"Failed to fetch Mantis data for {user_email}: {str(e)}"
        logging.error(error_msg)
        return {
            "user_email": user_email,
            "week_start": week_start,
            "week_end": week_end,
            "total_hours": 0,
            "entries": [],
            "unique_dates": 0,
            "tasks_over_4_hours": 0,
            "entry_count": 0,
            "error": error_msg
        }
    except Exception as e:
        error_msg = f"Unexpected error fetching Mantis data for {user_email}: {str(e)}"
        logging.error(error_msg)
        return {
            "user_email": user_email,
            "week_start": week_start,
            "week_end": week_end,
            "total_hours": 0,
            "entries": [],
            "unique_dates": 0,
            "tasks_over_4_hours": 0,
            "entry_count": 0,
            "error": error_msg
        }

def make_api_request(url, method="GET", params=None, json=None, retries=3):
    """Helper function for API requests with timeout and retry logic"""
    from requests.adapters import HTTPAdapter
    from requests.packages.urllib3.util.retry import Retry
    
    session = requests.Session()
    retry_strategy = Retry(
        total=retries,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    try:
        if method == "GET":
            response = session.get(url, params=params, timeout=120)
        elif method == "PUT":
            response = session.put(url, json=json, params=params, timeout=120)
        elif method == "POST":
            response = session.post(url, json=json, params=params, timeout=120)
        else:
            raise ValueError(f"Unsupported method: {method}")

        response.raise_for_status()
        return response.json() if response.content else {}
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed: {str(e)}")
        raise

def fetch_issue_details_from_mantis_api(issue_id):
    """
    Fetch issue details directly from Mantis API.
    
    Args:
        issue_id: The Mantis issue ID
        
    Returns:
        dict: Issue details including notes, or None if failed
    """
    try:
        # Construct the API endpoint using the base URL
        endpoint = f"{MANTIS_API_BASE}/api/rest/issues/{issue_id}"
        
        headers = {
            "Authorization": MANTIS_API_KEY,
            "Content-Type": "application/json"
        }
        
        logging.info(f"    Fetching issue {issue_id} from Mantis API: {endpoint}")
        
        response = requests.get(endpoint, headers=headers, timeout=60)
        response.raise_for_status()
        
        issue_data = response.json()
        
        # Extract the issue object (API returns wrapper with "issues" array)
        if isinstance(issue_data, dict) and "issues" in issue_data:
            issues = issue_data.get("issues", [])
            if issues and len(issues) > 0:
                issue = issues[0]
            else:
                logging.warning(f"    No issue found in response for {issue_id}")
                return None
        else:
            issue = issue_data
        
        # Transform to our expected format
        transformed_issue = {
            "issue_id": str(issue.get("id", issue_id)),
            "title": issue.get("summary", ""),
            "description": issue.get("description", ""),
            "status": issue.get("status", {}).get("name", "") if isinstance(issue.get("status"), dict) else str(issue.get("status", "")),
            "priority": issue.get("priority", {}).get("name", "") if isinstance(issue.get("priority"), dict) else str(issue.get("priority", "")),
            "category": issue.get("category", {}).get("name", "") if isinstance(issue.get("category"), dict) else str(issue.get("category", "")),
            "notes": issue.get("notes", [])
        }
        
        logging.info(f"    ✓ Fetched issue {issue_id}: {transformed_issue['title'][:50]}... ({len(transformed_issue['notes'])} notes)")
        
        return transformed_issue
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            logging.warning(f"    Issue {issue_id} not found (404)")
        else:
            logging.error(f"    HTTP error fetching issue {issue_id}: {e}")
        return None
    except requests.exceptions.RequestException as e:
        logging.error(f"    Request failed for issue {issue_id}: {e}")
        return None
    except Exception as e:
        logging.error(f"    Unexpected error fetching issue {issue_id}: {e}")
        return None

# ============================================================================
# DAG TASK FUNCTIONS
# ============================================================================

def load_whitelisted_users(ti, **context):
    """
    Task 1: Load whitelisted users from Airflow Variables
    """
    import pendulum
    
    try:
        users = WHITELISTED_USERS
        
        if not users or not isinstance(users, list):
            raise ValueError("WHITELISTED_USERS variable is not properly configured")
        
        # Validate user structure
        required_fields = ['user_id', 'mantis_email', 'name', 'email']
        for user in users:
            missing_fields = [field for field in required_fields if field not in user]
            if missing_fields:
                raise ValueError(f"User {user.get('user_id', 'unknown')} missing fields: {missing_fields}")
            
            # Set default values for optional fields
            if 'hr_name' not in user:
                user['hr_name'] = 'HR_Manager'
                logging.warning(f"User {user['name']} missing hr_name, using default 'HR_Manager'")
            if 'hr_email' not in user:
                user['hr_email'] = None
                logging.warning(f"User {user['name']} missing hr_email, no HR CC will be sent")
            if 'manager_name' not in user:
                user['manager_name'] = None
            if 'manager_email' not in user:
                user['manager_email'] = None
            if 'admin_name' not in user:
                user['admin_name'] = None
            if 'admin_email' not in user:
                user['admin_email'] = None
            
            # Handle timezone - default to Asia/Kolkata if not specified
            if 'timezone' not in user:
                user['timezone'] = 'Asia/Kolkata'
                logging.warning(f"User {user['name']} missing timezone, using default 'Asia/Kolkata'")
            
            # Validate timezone
            try:
                tz = pendulum.timezone(user['timezone'])
                user['timezone_obj'] = tz  # Store the timezone object for later use
            except Exception as e:
                logging.warning(f"Invalid timezone '{user['timezone']}' for user {user['name']}, using 'Asia/Kolkata': {e}")
                user['timezone'] = 'Asia/Kolkata'
                user['timezone_obj'] = pendulum.timezone('Asia/Kolkata')
        
        # Push to XCom
        ti.xcom_push(key="whitelisted_users", value=users)
        ti.xcom_push(key="total_users", value=len(users))
        
        logging.info(f"=" * 80)
        logging.info(f"LOADED WHITELISTED USERS")
        logging.info(f"=" * 80)
        logging.info(f"Total users: {len(users)}")
        for user in users:
            cc_info = []
            if user.get('hr_email'):
                cc_info.append(f"HR: {user.get('hr_name', 'N/A')} <{user['hr_email']}>")
            if user.get('manager_email'):
                cc_info.append(f"Manager: {user.get('manager_name', 'N/A')} <{user['manager_email']}>")
            if user.get('admin_email'):
                cc_info.append(f"Admin: {user.get('admin_name', 'N/A')} <{user['admin_email']}>")
            
            cc_text = f" (CC: {'; '.join(cc_info)})" if cc_info else ""
            logging.info(f"  - {user['name']} ({user['email']}) [TZ: {user['timezone']}]{cc_text}")
        logging.info(f"=" * 80)
        
        return len(users)
        
    except Exception as e:
        logging.error(f"Failed to load whitelisted users: {e}")
        raise

def get_initiated_users_today(ti, user_id, user_timezone):
    """Check if this specific user was initiated today (Monday) in THEIR timezone"""
    try:
        import pendulum
        
        user_tz = pendulum.timezone(user_timezone)
        user_local_datetime = pendulum.now(user_tz)
        user_local_date = user_local_datetime.format('YYYY-MM-DD')
        
        # Check if it's Monday in user's timezone
        if user_local_datetime.day_of_week != pendulum.MONDAY:
            logging.debug(f"Not Monday in {user_timezone} (day {user_local_datetime.day_of_week})")
            return False
        
        key = f"initiated_users_{user_local_date}_{user_id}"
        
        # Check if already initiated today
        initiated = ti.xcom_pull(key=key, task_ids=None, include_prior_dates=True, dag_id=ti.dag_id)
        
        if initiated is not None:
            logging.info(f"User {user_id} already initiated on {user_local_date}")
            return True
        
        return False
        
    except Exception as e:
        logging.warning(f"Failed to check initiated status for user {user_id}: {e}")
        return False

def mark_user_initiated(ti, user_id, user_timezone):
    """Mark that weekly review has been initiated for this user today (in their timezone)"""
    try:
        import pendulum
        
        # Use user's local date, not UTC
        user_tz = pendulum.timezone(user_timezone)
        user_local_date = pendulum.now(user_tz).format('YYYY-MM-DD')
        
        key = f"initiated_users_{user_local_date}_{user_id}"  # Include user_id in key
        
        ti.xcom_push(key=key, value=True)
        logging.info(f"✓ Marked user {user_id} as initiated on {user_local_date}")
        
    except Exception as e:
        logging.error(f"Failed to mark user {user_id} as initiated: {e}")

def calculate_week_range(ti, **context):
    """
    Task 2: Calculate the week range to process
    """
    execution_date = context['execution_date']
    week_range = get_week_range(execution_date)
    
    ti.xcom_push(key="week_range", value=week_range)
    
    logging.info(f"=" * 80)
    logging.info(f"WEEK RANGE CALCULATION")
    logging.info(f"=" * 80)
    logging.info(f"Processing: Week {week_range['week_number']} of {week_range['month']} {week_range['year']}")
    logging.info(f"Date range: {week_range['start']} to {week_range['end']}")
    logging.info(f"=" * 80)
    
    return week_range

def filter_users_by_timezone(ti, **context):
    """
    Task 1.5: Filter users whose local time is 8:00 AM or later on MONDAY
    AND who have not been initiated yet today.
    Window: 8 AM - 11:59 PM on Monday (user's local time)
    """
    import pendulum
    
    users = ti.xcom_pull(key="whitelisted_users", task_ids="load_whitelisted_users")
    execution_date = context['execution_date']
    
    if not users:
        logging.warning("No users to filter")
        ti.xcom_push(key="users_to_process", value=[])
        ti.xcom_push(key="total_users_to_process", value=0)
        return []
    
    # Get current UTC time from execution
    current_utc = execution_date
    
    users_to_process = []
    
    logging.info(f"=" * 80)
    logging.info(f"FILTERING USERS BY TIMEZONE (MONDAY 8 AM+ CHECK)")
    logging.info(f"=" * 80)
    logging.info(f"Current UTC time: {current_utc.format('YYYY-MM-DD HH:mm:ss')}")
    logging.info(f"Looking for users where:")
    logging.info(f"  1. Local day is MONDAY")
    logging.info(f"  2. Local time is >= 8:00 AM (any time from 8 AM until midnight)")
    logging.info(f"  3. NOT already initiated today")
    logging.info(f"")
    
    for user in users:
        username = user.get('name')
        user_id = user.get('user_id')
        user_timezone_str = user.get('timezone', 'Asia/Kolkata')
        
        try:
            # Convert current UTC time to user's timezone
            user_tz = pendulum.timezone(user_timezone_str)
            user_local_time = current_utc.in_timezone(user_tz)
            user_hour = user_local_time.hour
            user_day_of_week = user_local_time.day_of_week
            
            # Check if already initiated today
            if get_initiated_users_today(ti, user_id, user_timezone_str):
                logging.info(f"  ⏭️  {username} ({user_timezone_str}): "
                           f"Already initiated on {user_local_time.format('YYYY-MM-DD')} - SKIPPING")
                continue
            
            # Check if it's Monday (pendulum.MONDAY = 0)
            if user_day_of_week != pendulum.MONDAY:
                day_name = user_local_time.format('dddd')
                logging.info(f"  ✗ {username} ({user_timezone_str}): "
                           f"Local day is {day_name}, not Monday - SKIPPING")
                continue
            
            # Check if user's local time is 8 AM or later (>= 8)
            if user_hour >= 8:
                users_to_process.append(user)
                logging.info(f"  ✓ {username} ({user_timezone_str}): "
                           f"Local time is {user_local_time.format('YYYY-MM-DD HH:mm:ss')} "
                           f"(Monday, {user_hour}:00 - window open) - PROCESSING")
                
                # Mark user as initiated for today
                mark_user_initiated(ti, user_id, user_timezone_str)
            else:
                logging.info(f"  ✗ {username} ({user_timezone_str}): "
                           f"Local time is {user_local_time.format('YYYY-MM-DD HH:mm:ss')} "
                           f"(Hour: {user_hour}, need >= 8) - TOO EARLY")
        
        except Exception as e:
            logging.error(f"  ✗ Error processing timezone for {username}: {e}")
            continue
    
    logging.info(f"")
    logging.info(f"Users to process: {len(users_to_process)} out of {len(users)}")
    logging.info(f"=" * 80)
    
    # Push filtered users to XCom
    ti.xcom_push(key="users_to_process", value=users_to_process)
    ti.xcom_push(key="total_users_to_process", value=len(users_to_process))
    
    return users_to_process

def fetch_all_timesheets(ti, **context):
    """
    Task 3: Loop through filtered users (8 AM in their timezone) and fetch their timesheet data via API
    """
    # Changed from whitelisted_users to users_to_process
    users = ti.xcom_pull(key="users_to_process", task_ids="filter_users_by_timezone")
    week_range = ti.xcom_pull(key="week_range", task_ids="calculate_week_range")
    
    if not users:
        logging.warning("No users to process (none at 8 AM in their timezone)")
        ti.xcom_push(key="timesheet_data_all", value=[])
        return []
    
    logging.info(f"=" * 80)
    logging.info(f"FETCHING TIMESHEETS VIA API - {len(users)} users (filtered by timezone)")
    logging.info(f"=" * 80)
    
    all_timesheet_data = []
    
    # Sequential loop through users
    for idx, user in enumerate(users, 1):
        user_email = user.get("mantis_email")
        username = user.get("name")
        user_timezone = user.get("timezone", "Asia/Kolkata")
        
        logging.info(f"[{idx}/{len(users)}] Fetching timesheet for {username} ({user_email}) [TZ: {user_timezone}]")
        
        try:
            # Use direct API call instead of AI
            timesheet_data = fetch_mantis_timesheet_data_direct_api(
                name=username,
                week_start=week_range['start'],
                week_end=week_range['end']
            )
            
            # Add user metadata
            timesheet_data['user_id'] = user.get('user_id')
            timesheet_data['username'] = username
            timesheet_data['user_email_address'] = user.get('email')
            timesheet_data['user_timezone'] = user_timezone
            timesheet_data['hr_name'] = user.get('hr_name', 'HR_Manager')
            timesheet_data['hr_email'] = user.get('hr_email')
            timesheet_data['manager_name'] = user.get('manager_name')
            timesheet_data['manager_email'] = user.get('manager_email')
            timesheet_data['admin_name'] = user.get('admin_name')
            timesheet_data['admin_email'] = user.get('admin_email')
            timesheet_data['week_info'] = f"Week {week_range['week_number']} of {week_range['month']}"
            
            # Push individual user data to XCom
            ti.xcom_push(key=f"timesheet_data_{user.get('user_id')}", value=timesheet_data)
            
            all_timesheet_data.append(timesheet_data)
            
            if 'error' in timesheet_data:
                logging.warning(f"  ✗ Error: {timesheet_data['error']}")
            else:
                logging.info(f"  ✓ Success: {timesheet_data.get('total_hours', 0)} hours, {timesheet_data.get('entry_count', 0)} entries")
            
        except Exception as e:
            logging.error(f"  ✗ Failed to fetch timesheet for {username}: {e}")
            # Store error state
            error_data = {
                'user_id': user.get('user_id'),
                'username': username,
                'user_email': user_email,
                'user_email_address': user.get('email'),
                'user_timezone': user_timezone,
                'hr_name': user.get('hr_name', 'HR_Manager'),
                'hr_email': user.get('hr_email'),
                'manager_name': user.get('manager_name'),
                'manager_email': user.get('manager_email'),
                'admin_name': user.get('admin_name'),
                'admin_email': user.get('admin_email'),
                'week_start': week_range['start'],
                'week_end': week_range['end'],
                'total_hours': 0,
                'entries': [],
                'unique_dates': 0,
                'tasks_over_4_hours': 0,
                'entry_count': 0,
                'error': str(e),
                'week_info': f"Week {week_range['week_number']} of {week_range['month']}"
            }
            ti.xcom_push(key=f"timesheet_data_{user.get('user_id')}", value=error_data)
            all_timesheet_data.append(error_data)
    
    # Push consolidated data
    ti.xcom_push(key="timesheet_data_all", value=all_timesheet_data)
    
    logging.info(f"=" * 80)
    logging.info(f"FETCH SUMMARY")
    logging.info(f"=" * 80)
    logging.info(f"Total processed: {len(all_timesheet_data)}")
    logging.info(f"Successful: {sum(1 for d in all_timesheet_data if 'error' not in d)}")
    logging.info(f"Failed: {sum(1 for d in all_timesheet_data if 'error' in d)}")
    logging.info(f"=" * 80)
    
    return all_timesheet_data


def fetch_mantis_timesheet_data_direct_api(name, week_start, week_end):
    """
    Fetch timesheet data directly from Mantis API using connector endpoint.
    Handles the actual API response format with summary data.
    
    NOTE: This API doesn't return individual dates. Unique dates will be 
    calculated later in enrich_timesheets_with_issue_details from issue notes.
    """
    try:
        # Construct API endpoint
        endpoint = f"{MANTIS_TIMESHEET_API_URL}/user_time_tracking"
    
        params = {
            "username": name,
            "start_date": week_start,
            "end_date": week_end
        }
        
        logging.info(f"  Fetching timesheet from API: {endpoint}")
        logging.info(f"  Parameters: {params}")
        
        # Make API request with retry logic
        response_data = make_api_request(
            url=endpoint,
            method="GET",
            params=params,
            retries=3
        )
        
        logging.info(f"  API Response: {json.dumps(response_data, indent=2)}")
        
        # Extract data from the actual API response format
        total_time_str = response_data.get("total_time", "0:0")
        projects = response_data.get("project", [])
        tickets = response_data.get("tickets_worked", [])
        
        # Parse total hours from "HH:MM" format
        hours, minutes = map(int, total_time_str.split(':'))
        total_hours = hours + (minutes / 60.0)
        
        # Transform tickets into entries format expected by downstream tasks
        entries = []
        for ticket in tickets:
            issue_id = ticket.get("issue_id")
            issue_time_str = ticket.get("issue_time", "0:0")
            
            # Parse time for this ticket
            t_hours, t_minutes = map(int, issue_time_str.split(':'))
            ticket_hours = t_hours + (t_minutes / 60.0)
            
            # Create entry (dates will be populated during enrichment from issue notes)
            entry = {
                "issue_id": issue_id,
                "task_id": issue_id,  # Alias
                "hours": ticket_hours,
                "time_str": issue_time_str,
            }
            entries.append(entry)
        
        # Count tasks over 4 hours
        tasks_over_4_hours = sum(1 for entry in entries if entry.get("hours", 0) > 4)
        
        timesheet_data = {
            "username": name,
            "week_start": week_start,
            "week_end": week_end,
            "total_hours": round(total_hours, 2),
            "total_time_str": total_time_str,
            "projects": projects,
            "entries": entries,
            "unique_dates": 0,  # Will be calculated during enrichment from issue notes
            "tasks_over_4_hours": tasks_over_4_hours,
            "entry_count": len(entries),
        }
        
        logging.info(f"  API returned: {total_hours:.2f} hours, {len(entries)} ticket entries")
        logging.info(f"  Note: unique_dates will be calculated from issue notes during enrichment")
        
        return timesheet_data
        
    except requests.exceptions.RequestException as e:
        error_msg = f"API request failed: {str(e)}"
        logging.error(f"  {error_msg}")
        return {
            "username": name,
            "week_start": week_start,
            "week_end": week_end,
            "total_hours": 0,
            "entries": [],
            "unique_dates": 0,
            "tasks_over_4_hours": 0,
            "entry_count": 0,
            "error": error_msg
        }
    except ValueError as e:
        error_msg = f"Failed to parse time format: {str(e)}"
        logging.error(f"  {error_msg}")
        return {
            "username": name,
            "week_start": week_start,
            "week_end": week_end,
            "total_hours": 0,
            "entries": [],
            "unique_dates": 0,
            "tasks_over_4_hours": 0,
            "entry_count": 0,
            "error": error_msg
        }
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logging.error(f"  {error_msg}")
        return {
            "username": name,
            "week_start": week_start,
            "week_end": week_end,
            "total_hours": 0,
            "entries": [],
            "unique_dates": 0,
            "tasks_over_4_hours": 0,
            "entry_count": 0,
            "error": error_msg
        }

def extract_json_from_response(text):
    # Try to find the first complete JSON object
    try:
        # Remove any leading markdown fences
        cleaned = re.sub(r'^```json\s*|\s*```$', '', text.strip(), flags=re.MULTILINE)
        # Find first { ... }
        match = re.search(r'\{.*\}', cleaned, re.DOTALL)
        if match:
            return match.group(0)
        return cleaned
    except:
        return text
    
def enrich_timesheets_with_issue_details(ti, **context):
    """
    Task 4: Fetch issue details for each timesheet entry and enrich the data
    - Fetches ONE issue at a time via DIRECT API CALL instead of AI/LLM
    - Extracts dates from issue notes' created_at and updated_at to calculate unique_dates
    """
    all_timesheet_data = ti.xcom_pull(key="timesheet_data_all", task_ids="fetch_all_timesheets")
    
    if not all_timesheet_data:
        logging.warning("No timesheet data to enrich with issue details")
        ti.xcom_push(key="enriched_timesheet_data_all", value=[])
        return []
    
    logging.info(f"=" * 80)
    logging.info(f"ENRICHING TIMESHEETS WITH ISSUE DETAILS - {len(all_timesheet_data)} users")
    logging.info(f"=" * 80)
    
    enriched_timesheet_data = []
    
    # Sequential loop through each user's timesheet
    for idx, timesheet_data in enumerate(all_timesheet_data, 1):
        user_id = timesheet_data.get('user_id')
        username = timesheet_data.get('username')
        week_start = timesheet_data.get('week_start')
        week_end = timesheet_data.get('week_end')
        
        logging.info(f"[{idx}/{len(all_timesheet_data)}] Enriching timesheet for {username}")
        
        # Skip if there was an error fetching the base timesheet
        if 'error' in timesheet_data:
            logging.warning(f"  ✗ Skipping enrichment - fetch error: {timesheet_data['error']}")
            enriched_timesheet_data.append(timesheet_data)
            continue
        
        try:
            entries = timesheet_data.get('entries', [])
            
            if not entries:
                logging.info(f"  → No entries to enrich for {username}")
                enriched_timesheet_data.append(timesheet_data)
                continue
            
            # Extract unique issue IDs
            issue_ids = set()
            for entry in entries:
                issue_id = entry.get('issue_id') or entry.get('task_id')
                if issue_id:
                    issue_ids.add(str(issue_id))
            
            if not issue_ids:
                logging.info(f"  → No issue IDs found in entries for {username}")
                enriched_timesheet_data.append(timesheet_data)
                continue
            
            logging.info(f"  → Will fetch details for {len(issue_ids)} issues via direct API")
            
            # ────────────────────────────────────────────────────────────────
            # Fetch ONE issue at a time via DIRECT API CALL
            # ────────────────────────────────────────────────────────────────
            issue_details_map = {}
            all_work_dates = set()  # To track unique dates across all issues
            
            for issue_id in sorted(issue_ids):  # sorted → nicer log order
                try:
                    logging.info(f"    Fetching issue {issue_id} via Mantis API...")
                    
                    # Use the new direct API function instead of AI
                    issue_data = fetch_issue_details_from_mantis_api(issue_id)
                    
                    if issue_data is None:
                        logging.warning(f"    ✗ Failed to fetch issue {issue_id} from API")
                        continue
                    
                    # ────────────────────────────────────────────────────────────────
                    # Extract dates from notes' created_at and updated_at for this user
                    # ────────────────────────────────────────────────────────────────
                    notes = issue_data.get('notes', [])
                    issue_work_dates = set()
                    
                    for note in notes:
                        # Check if this note is from the target user
                        reporter = note.get('reporter', {})
                        reporter_name = reporter.get('name', '') if isinstance(reporter, dict) else str(reporter)
                        
                        # Match reporter to username (case-insensitive)
                        if reporter_name.lower() == username.lower():
                            # Extract dates from BOTH created_at AND updated_at
                            timestamps = []
                            
                            if note.get('created_at'):
                                timestamps.append(note.get('created_at'))
                            
                            if note.get('updated_at'):
                                timestamps.append(note.get('updated_at'))
                            
                            for timestamp in timestamps:
                                if timestamp:
                                    try:
                                        # Parse timestamp - assuming ISO format like "2025-01-27T10:30:00Z"
                                        # Adjust parsing based on your actual timestamp format
                                        from datetime import datetime
                                        
                                        # Handle different timestamp formats
                                        if 'T' in str(timestamp):
                                            date_obj = datetime.fromisoformat(str(timestamp).replace('Z', '+00:00'))
                                        else:
                                            date_obj = datetime.strptime(str(timestamp), '%Y-%m-%d')
                                        
                                        work_date = date_obj.date().isoformat()
                                        
                                        # Check if date is within the week range
                                        if week_start <= work_date <= week_end:
                                            issue_work_dates.add(work_date)
                                            all_work_dates.add(work_date)
                                            logging.debug(f"      Found work date: {work_date} for issue {issue_id} from timestamp {timestamp}")
                                    
                                    except (ValueError, AttributeError) as e:
                                        logging.debug(f"      Could not parse timestamp '{timestamp}': {e}")
                                        continue
                    
                    # Add work dates to issue data
                    issue_data['work_dates'] = sorted(list(issue_work_dates))
                    issue_data['work_date_count'] = len(issue_work_dates)
                    
                    issue_details_map[issue_id] = issue_data
                    logging.info(f"    ✓ Got issue {issue_id}: {issue_data.get('title', 'no title')} "
                                 f"({len(issue_data.get('notes', []))} notes, {len(issue_work_dates)} work dates)")
                
                except Exception as e:
                    logging.error(f"    Failed to fetch/enrich issue {issue_id}: {e}")
                    continue
            
            # ────────────────────────────────────────────────────────────────
            # Enrich the original entries using the collected details
            # ────────────────────────────────────────────────────────────────
            enriched_entries = []
            for entry in entries:
                enriched_entry = entry.copy()
                issue_id = str(entry.get('issue_id') or entry.get('task_id', ''))
                
                if issue_id and issue_id in issue_details_map:
                    issue_details = issue_details_map[issue_id]
                    enriched_entry['issue_details'] = issue_details
                    
                    # Add work dates to the entry
                    work_dates = issue_details.get('work_dates', [])
                    if work_dates:
                        enriched_entry['work_dates'] = work_dates
                    
                    logging.debug(f"    Enriched entry with issue {issue_id}")
                
                enriched_entries.append(enriched_entry)
            
            # Build final enriched timesheet object
            enriched_timesheet = timesheet_data.copy()
            enriched_timesheet['entries'] = enriched_entries
            enriched_timesheet['enriched'] = True
            enriched_timesheet['issues_fetched'] = len(issue_details_map)
            enriched_timesheet['issues_attempted'] = len(issue_ids)
            
            # ────────────────────────────────────────────────────────────────
            # UPDATE unique_dates based on extracted dates from notes
            # ────────────────────────────────────────────────────────────────
            unique_dates_count = len(all_work_dates)
            enriched_timesheet['unique_dates'] = unique_dates_count
            enriched_timesheet['work_dates_list'] = sorted(list(all_work_dates))
            
            logging.info(f"  ✓ Enrichment complete: {len(issue_details_map)} / {len(issue_ids)} issues enriched")
            logging.info(f"  ✓ Unique work dates identified: {unique_dates_count} days - {sorted(list(all_work_dates))}")
            
            # Push to XCom for this user
            ti.xcom_push(key=f"enriched_timesheet_{user_id}", value=enriched_timesheet)
            
            enriched_timesheet_data.append(enriched_timesheet)
        
        except Exception as e:
            logging.error(f"  ✗ Failed to enrich timesheet for {username}: {e}")
            timesheet_data['enrichment_error'] = str(e)
            enriched_timesheet_data.append(timesheet_data)
    
    # Push consolidated enriched data for downstream tasks
    ti.xcom_push(key="enriched_timesheet_data_all", value=enriched_timesheet_data)
    
    logging.info(f"=" * 80)
    logging.info(f"ENRICHMENT SUMMARY")
    logging.info(f"=" * 80)
    logging.info(f"Total processed: {len(enriched_timesheet_data)}")
    logging.info(f"Successfully enriched: {sum(1 for d in enriched_timesheet_data if d.get('enriched'))}")
    logging.info(f"Skipped/Failed: {sum(1 for d in enriched_timesheet_data if not d.get('enriched'))}")
    logging.info(f"=" * 80)
    
    return enriched_timesheet_data


def analyze_all_timesheets_with_ai(ti, **context):
    """
    Task 5: Loop through all enriched timesheets and analyze each with AI
    """
    all_timesheet_data = ti.xcom_pull(key="enriched_timesheet_data_all", task_ids="enrich_timesheets_with_issue_details")
    
    if not all_timesheet_data:
        logging.warning("No timesheet data to analyze")
        ti.xcom_push(key="analysis_results_all", value=[])
        return []
    
    logging.info(f"=" * 80)
    logging.info(f"ANALYZING TIMESHEETS - {len(all_timesheet_data)} users")
    logging.info(f"=" * 80)
    
    all_analysis_results = []
    
    # Sequential loop through each user's timesheet
    for idx, timesheet_data in enumerate(all_timesheet_data, 1):
        user_id = timesheet_data.get('user_id')
        username = timesheet_data.get('username')
        
        logging.info(f"[{idx}/{len(all_timesheet_data)}] Analyzing timesheet for {username}")
        
        # Skip if there was an error fetching
        if 'error' in timesheet_data:
            logging.warning(f"  ✗ Skipping analysis - fetch error: {timesheet_data['error']}")
            error_analysis = {
                'user_id': user_id,
                'username': username,
                'user_email_address': timesheet_data.get('user_email_address'),
                'hr_name': timesheet_data.get('hr_name', 'HR_Manager'),
                'hr_email': timesheet_data.get('hr_email'),
                'error': timesheet_data['error'],
                'analysis_skipped': True
            }
            ti.xcom_push(key=f"analysis_{user_id}", value=error_analysis)
            all_analysis_results.append(error_analysis)
            continue
        
        try:
            # Prepare prompt with timesheet data
            full_prompt = f"{EVALUATION_PROMPT_TEMPLATE}\n\nTIMESHEET DATA TO ANALYZE:\n{json.dumps(timesheet_data, indent=2)}"
            
            # Get AI analysis using the get_ai_response function (not direct API call)
            logging.info(f"  Calling AI for analysis...")
            response = get_ai_response(full_prompt, expect_json=True)
            
            # Parse JSON response
            analysis = json.loads(response)
            
            # Validate required fields
            required_fields = ['ratings', 'findings', 'recommendations', 'final_score']
            missing_fields = [field for field in required_fields if field not in analysis]
            if missing_fields:
                raise ValueError(f"AI response missing fields: {missing_fields}")
            
            # Add user metadata
            analysis['user_id'] = user_id
            analysis['username'] = username
            analysis['user_email_address'] = timesheet_data.get('user_email_address')
            analysis['hr_name'] = timesheet_data.get('hr_name', 'HR_Manager')
            analysis['hr_email'] = timesheet_data.get('hr_email')
            analysis['manager_name'] = timesheet_data.get('manager_name')
            analysis['manager_email'] = timesheet_data.get('manager_email')
            analysis['admin_name'] = timesheet_data.get('admin_name')
            analysis['admin_email'] = timesheet_data.get('admin_email')
            analysis['week_info'] = timesheet_data.get('week_info')
            
            # Push individual analysis result to XCom
            ti.xcom_push(key=f"analysis_{user_id}", value=analysis)
            
            all_analysis_results.append(analysis)
            
            final_score = analysis.get('final_score', 0)
            logging.info(f"  ✓ Analysis complete: Final Score {final_score}/5")
            
        except Exception as e:
            logging.error(f"  ✗ Failed to analyze: {e}")
            error_analysis = {
                'user_id': user_id,
                'username': username,
                'user_email_address': timesheet_data.get('user_email_address'),
                'hr_name': timesheet_data.get('hr_name', 'HR_Manager'),
                'hr_email': timesheet_data.get('hr_email'),
                'manager_name': timesheet_data.get('manager_name'),
                'manager_email': timesheet_data.get('manager_email'),
                'admin_name': timesheet_data.get('admin_name'),
                'admin_email': timesheet_data.get('admin_email'),
                'error': str(e),
                'analysis_failed': True
            }
            ti.xcom_push(key=f"analysis_{user_id}", value=error_analysis)
            all_analysis_results.append(error_analysis)
    
    # Push consolidated analysis results
    ti.xcom_push(key="analysis_results_all", value=all_analysis_results)
    
    logging.info(f"=" * 80)
    logging.info(f"ANALYSIS SUMMARY")
    logging.info(f"=" * 80)
    logging.info(f"Total analyzed: {len(all_analysis_results)}")
    logging.info(f"Successful: {sum(1 for a in all_analysis_results if 'error' not in a and not a.get('analysis_skipped'))}")
    logging.info(f"Failed/Skipped: {sum(1 for a in all_analysis_results if 'error' in a or a.get('analysis_skipped') or a.get('analysis_failed'))}")
    logging.info(f"=" * 80)
    
    return all_analysis_results

def compose_and_send_all_emails(ti, **context):
    """
    Task 6: Loop through all analysis results, compose emails, and send to respective users with CC to HR, Manager, and Admin
    """
    week_range = ti.xcom_pull(key="week_range", task_ids="calculate_week_range")
    all_analysis_results = ti.xcom_pull(key="analysis_results_all", task_ids="analyze_all_timesheets_with_ai")
    
    if not all_analysis_results:
        logging.warning("No analysis results to process for emails")
        ti.xcom_push(key="email_send_results", value=[])
        return []
    
    logging.info(f"=" * 80)
    logging.info(f"COMPOSING AND SENDING EMAILS - {len(all_analysis_results)} users")
    logging.info(f"=" * 80)
    
    email_send_results = []
    
    # Sequential loop through each analysis result
    for idx, analysis in enumerate(all_analysis_results, 1):
        user_id = analysis.get('user_id')
        username = analysis.get('username')
        user_email = analysis.get('user_email_address')
        hr_name = analysis.get('hr_name', 'HR_Manager')
        hr_email = analysis.get('hr_email')
        manager_name = analysis.get('manager_name')
        manager_email = analysis.get('manager_email')
        admin_name = analysis.get('admin_name')
        admin_email = analysis.get('admin_email')
        week_info = analysis.get('week_info', 'this week')
        
        logging.info(f"[{idx}/{len(all_analysis_results)}] Processing email for {username}")
        
        # Skip if analysis failed
        if analysis.get('error') or analysis.get('analysis_skipped') or analysis.get('analysis_failed'):
            logging.warning(f"  ✗ Skipping email - analysis error: {analysis.get('error', 'Analysis failed')}")
            email_send_results.append({
                'user_id': user_id,
                'username': username,
                'user_email': user_email,
                'status': 'skipped',
                'reason': analysis.get('error', 'Analysis failed')
            })
            continue
        
        try:
            # Generate star ratings helper
            def stars(rating):
                rating = int(rating) if rating else 0
                return '⭐' * rating + '☆' * (5 - rating)
            
            # Extract data from analysis
            ratings = analysis.get('ratings', {})
            findings = analysis.get('findings', {})
            recommendations = analysis.get('recommendations', {})
            final_score = analysis.get('final_score', 0)
            from datetime import datetime
            week_start_formatted = datetime.strptime(week_range['start'], '%Y-%m-%d').strftime('%B %d, %Y')
            week_end_formatted = datetime.strptime(week_range['end'], '%Y-%m-%d').strftime('%B %d, %Y')
            # Compose email body
            email_body = EMAIL_TEMPLATE.format(
                username=username,
                week_info=week_info,
                week_start=week_start_formatted,
                week_end=week_end_formatted,
                total_hours_stars=stars(ratings.get('total_hours', 0)),
                total_hours_score=ratings.get('total_hours', 0),
                total_hours_findings=findings.get('total_hours', ''),
                total_hours_recommendation=recommendations.get('total_hours', ''),
                ai_usage_stars=stars(ratings.get('ai_usage', 0)),
                ai_usage_score=ratings.get('ai_usage', 0),
                ai_usage_findings=findings.get('ai_usage', ''),
                ai_usage_recommendation=recommendations.get('ai_usage', ''),
                task_description_stars=stars(ratings.get('task_description', 0)),
                task_description_score=ratings.get('task_description', 0),
                task_description_findings=findings.get('task_description', ''),
                task_description_recommendation=recommendations.get('task_description', ''),
                daily_updates_stars=stars(ratings.get('daily_updates', 0)),
                daily_updates_score=ratings.get('daily_updates', 0),
                daily_updates_findings=findings.get('daily_updates', ''),
                daily_updates_recommendation=recommendations.get('daily_updates', ''),
                task_granularity_stars=stars(ratings.get('task_granularity', 0)),
                task_granularity_score=ratings.get('task_granularity', 0),
                task_granularity_findings=findings.get('task_granularity', ''),
                task_granularity_recommendation=recommendations.get('task_granularity', ''),
                time_accuracy_stars=stars(ratings.get('time_accuracy', 0)),
                time_accuracy_score=ratings.get('time_accuracy', 0),
                time_accuracy_findings=findings.get('time_accuracy', ''),
                time_accuracy_recommendation=recommendations.get('time_accuracy', ''),
                final_score_stars=stars(final_score),
                final_score=final_score,
                hr_name=hr_name
            )
            
            # Send email via SMTP with CC to HR, Manager, and Admin
            subject = f"Weekly Timesheet Review - {week_info} ({week_start_formatted} - {week_end_formatted})"
            
            # Prepare CC list (add HR, Manager, and Admin emails if available)
            cc_list = []
            cc_names = []
            
            if hr_email:
                cc_list.append(hr_email)
                cc_names.append(f"HR ({hr_name})")
            
            if manager_email:
                cc_list.append(manager_email)
                cc_names.append(f"Manager ({manager_name})")
            
            if admin_email:
                cc_list.append(admin_email)
                cc_names.append(f"Admin ({admin_name})")
            
            if cc_names:
                logging.info(f"  Adding to CC: {', '.join(cc_names)}")
            
            success = send_email_smtp(
                recipient=user_email,
                subject=subject,
                body_html=email_body,
                cc_list=cc_list if cc_list else None
            )
            
            if success:
                logging.info(f"  ✓ Email sent successfully")
                email_send_results.append({
                    'user_id': user_id,
                    'username': username,
                    'user_email': user_email,
                    'hr_email': hr_email,
                    'manager_email': manager_email,
                    'admin_email': admin_email,
                    'status': 'sent',
                    'final_score': final_score
                })
            else:
                logging.error(f"  ✗ Failed to send email")
                email_send_results.append({
                    'user_id': user_id,
                    'username': username,
                    'user_email': user_email,
                    'hr_email': hr_email,
                    'manager_email': manager_email,
                    'admin_email': admin_email,
                    'status': 'failed',
                    'reason': 'Email send failed'
                })
                
        except Exception as e:
            logging.error(f"  ✗ Error composing/sending email: {e}")
            email_send_results.append({
                'user_id': user_id,
                'username': username,
                'user_email': user_email,
                'hr_email': hr_email,
                'manager_email': manager_email,
                'admin_email': admin_email,
                'status': 'failed',
                'reason': str(e)
            })
    
    # Push email results
    ti.xcom_push(key="email_send_results", value=email_send_results)
    
    logging.info(f"=" * 80)
    logging.info(f"EMAIL SEND SUMMARY")
    logging.info(f"=" * 80)
    logging.info(f"Total processed: {len(email_send_results)}")
    logging.info(f"Successfully sent: {sum(1 for r in email_send_results if r['status'] == 'sent')}")
    logging.info(f"Failed: {sum(1 for r in email_send_results if r['status'] == 'failed')}")
    logging.info(f"Skipped: {sum(1 for r in email_send_results if r['status'] == 'skipped')}")
    logging.info(f"=" * 80)
    
    return email_send_results

def log_execution_summary(ti, **context):
    """
    Task 7: Log summary of entire execution
    """
    email_results = ti.xcom_pull(key="email_send_results", task_ids="compose_and_send_all_emails")
    
    if not email_results:
        logging.warning("No email results to summarize")
        return
    
    # Calculate metrics
    total_users = len(email_results)
    emails_sent = sum(1 for r in email_results if r['status'] == 'sent')
    emails_failed = sum(1 for r in email_results if r['status'] == 'failed')
    emails_skipped = sum(1 for r in email_results if r['status'] == 'skipped')
    
    # Calculate average score (only for successfully sent emails)
    scores = [r.get('final_score', 0) for r in email_results if 'final_score' in r]
    average_score = sum(scores) / len(scores) if scores else 0
    
    summary = {
        'total_users': total_users,
        'emails_sent': emails_sent,
        'emails_failed': emails_failed,
        'emails_skipped': emails_skipped,
        'average_score': round(average_score, 2)
    }
    
    ti.xcom_push(key="execution_summary", value=summary)
    
    logging.info(f"")
    logging.info(f"=" * 80)
    logging.info(f"WEEKLY TIMESHEET REVIEW - EXECUTION SUMMARY")
    logging.info(f"=" * 80)
    logging.info(f"Total Users Processed: {summary['total_users']}")
    logging.info(f"Emails Sent: {summary['emails_sent']}")
    logging.info(f"Emails Failed: {summary['emails_failed']}")
    logging.info(f"Emails Skipped: {summary['emails_skipped']}")
    logging.info(f"Average Score: {summary['average_score']:.2f}/5")
    logging.info(f"=" * 80)
    logging.info(f"")
    
    return summary

# ============================================================================
# DAG DEFINITION
# ============================================================================

default_args = {
    'owner': 'lowtouch.ai_developers',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'weekly_timesheet_review',
    default_args=default_args,
    description='Weekly timesheet review with timezone-based delivery',
    schedule_interval='0 * * * 1',  # Every hour on Monday
    catchup=False,
    tags=['timesheet', 'weekly', 'review', 'mantis', 'timezone'],
) as dag:
    
    # Task 1: Load whitelisted users
    load_users_task = PythonOperator(
        task_id='load_whitelisted_users',
        python_callable=load_whitelisted_users
    )
    
    # Task 2: Calculate week range
    calc_week_task = PythonOperator(
        task_id='calculate_week_range',
        python_callable=calculate_week_range
    )
    
    # Task 2.5: Filter users by timezone (8 AM check)
    filter_timezone_task = PythonOperator(
        task_id='filter_users_by_timezone',
        python_callable=filter_users_by_timezone
    )
    
    # Task 3: Fetch timesheets for filtered users
    fetch_timesheets_task = PythonOperator(
        task_id='fetch_all_timesheets',
        python_callable=fetch_all_timesheets
    )
    
    # Task 4: Enrich timesheets with issue details
    enrich_timesheets_task = PythonOperator(
        task_id='enrich_timesheets_with_issue_details',
        python_callable=enrich_timesheets_with_issue_details
    )
    
    # Task 5: Analyze timesheets with AI
    analyze_timesheets_task = PythonOperator(
        task_id='analyze_all_timesheets_with_ai',
        python_callable=analyze_all_timesheets_with_ai
    )
    
    # Task 6: Compose and send emails
    send_emails_task = PythonOperator(
        task_id='compose_and_send_all_emails',
        python_callable=compose_and_send_all_emails
    )
    
    # Task 7: Log execution summary
    log_summary_task = PythonOperator(
        task_id='log_execution_summary',
        python_callable=log_execution_summary
    )
    
    # Define task dependencies
    load_users_task >> calc_week_task >> filter_timezone_task >> fetch_timesheets_task >> enrich_timesheets_task >> analyze_timesheets_task >> send_emails_task >> log_summary_task
