from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
from googleapiclient.discovery import build
import base64
from google.oauth2.credentials import Credentials
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import json
import re
from ollama import Client
import os
import time
from email.utils import parsedate_to_datetime
from bs4 import BeautifulSoup



# Configuration variables
BIGQUERY_SRE_FROM_ADDRESS = Variable.get("BIGQUERY_SRE_FROM_ADDRESS")
GMAIL_CREDENTIALS = Variable.get("BIGQUERY_SRE_GMAIL_CREDENTIALS")
LAST_PROCESSED_EMAIL_FILE = "/appz/cache/last_processed_bigquery_email.json"

# Reuse from your original code: authenticate_gmail, get_ai_response, check_execution_count, check_slot_usage, check_execution_time, check_memory_usage, compile_analysis_report, convert_to_html
# Assuming they are imported or defined here.

OLLAMA_HOST = Variable.get("BigQuery_OLLAMA_HOST", "http://agentomatic:8000/")
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

default_args = {
    "owner": "sre_team",
    "depends_on_past": False,
    "start_date": datetime(2025, 10, 16),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def authenticate_gmail():
    try:
        creds = Credentials.from_authorized_user_info(json.loads(Variable.get("BIGQUERY_SRE_GMAIL_CREDENTIALS", {})))
        service = build("gmail", "v1", credentials=creds)
        profile = service.users().getProfile(userId="me").execute()
        logged_in_email = profile.get("emailAddress", "")
        sender_email = Variable.get("BIGQUERY_SRE_FROM_ADDRESS", "")
        if logged_in_email.lower() != sender_email.lower():
            raise ValueError(f"Wrong Gmail account! Expected {sender_email}, but got {logged_in_email}")
        logging.info(f"Authenticated Gmail account: {logged_in_email}")
        return service
    except Exception as e:
        logging.error(f"Failed to authenticate Gmail: {str(e)}")
        return None

def get_ai_response(prompt, conversation_history=None):
    try:
        logging.debug(f"Query received: {prompt}")
        if not prompt or not isinstance(prompt, str):
            raise ValueError("Invalid prompt provided.")

        client = Client(host=OLLAMA_HOST)
        logging.debug(f"Connecting to Ollama at {OLLAMA_HOST} with model 'bigquery/sre/ailab:0.3'")

        messages = []
        if conversation_history:
            # Ensure conversation history has correct structure
            for msg in conversation_history:
                if isinstance(msg, dict) and "role" in msg and "content" in msg:
                    messages.append({"role": msg["role"], "content": msg["content"]})
                else:
                    logging.warning(f"Skipping malformed message in history: {msg}")
        
        messages.append({"role": "user", "content": prompt})
        
        logging.debug(f"Messages being sent to agent: {json.dumps(messages, indent=2)}")

        response = client.chat(
            model='bigquery/sre/ailab:0.3',
            messages=messages,
            stream=False
        )
        logging.info(f"Raw response from agent insided ai_response: {response}...")
        logging.info(f"Response type: {type(response)}")

        # Handle Ollama ChatResponse object
        ai_content = None
        
        # Check if it's an Ollama ChatResponse object with message attribute
        if hasattr(response, 'message') and hasattr(response.message, 'content'):
            ai_content = response.message.content.strip()
            logging.info(f"Extracted content from ChatResponse object: {ai_content}***...")
        # Fallback: try dictionary access
        elif isinstance(response, dict):
            if 'message' in response and isinstance(response['message'], dict):
                ai_content = response['message'].get('content', '').strip()
            elif 'content' in response:
                ai_content = response['content'].strip()
        
        if not ai_content:
            logging.error(f"Could not extract content from response. Response attributes: {dir(response)}")
            raise ValueError("Could not extract content from AI response.")

        logging.info(f"Successfully extracted AI content: {ai_content[:200]}...")
        return ai_content
        
    except Exception as e:
        logging.error(f"Error in get_ai_response: {str(e)}", exc_info=True)
        raise


# New helper: Get or create label ID for "PROCESSED"
def get_or_create_label(service, label_name="PROCESSED"):
    labels = service.users().labels().list(userId="me").execute().get("labels", [])
    for label in labels:
        if label["name"] == label_name:
            return label["id"]
    new_label = service.users().labels().create(userId="me", body={"name": label_name, "labelListVisibility": "labelShow", "messageListVisibility": "show"}).execute()
    return new_label["id"]

# New: Extract new content from email body (remove quotes)
def extract_new_content(body):
    lines = body.split('\n')
    new_lines = [line for line in lines if not line.startswith('>')]
    return '\n'.join(new_lines).strip()

# New: Build conversation history from thread
def build_conversation_history(service, thread_id, self_email):
    """Retrieve and format the email thread as a conversation list."""
    try:
        from email.utils import parsedate_to_datetime
        
        thread = service.users().threads().get(userId="me", id=thread_id).execute()
        conversation = []
        logging.info(f"Retrieved thread with ID: {thread_id} containing {len(thread.get('messages', []))} messages")
        messages_with_dates = []     

        for msg in thread.get("messages", []):
            headers = {h['name']: h['value'] for h in msg['payload'].get('headers', [])}
            sender = headers.get('From', '').split('<')[-1].rstrip('>').lower()
            content = decode_email_payload(msg)
            
            if not content.strip():
                logging.warning(f"No content in message {msg.get('id')}")
                continue
            
            # Clean HTML
            soup = BeautifulSoup(content, "html.parser")
            content = soup.get_text(separator=" ", strip=True)
            
            # ONLY remove lines that start with '>' (actual quote markers)
            lines = content.split('\n')
            cleaned_lines = [line for line in lines if not line.strip().startswith('>')]
            content = '\n'.join(cleaned_lines).strip()
            
            if not content.strip():
                logging.warning(f"No content after cleaning in message {msg.get('id')}")
                continue
            
            role = "user" if sender != self_email.lower() else "assistant"
            date_str = headers.get("Date", "")
            
            messages_with_dates.append({
                "role": role,
                "content": content,
                "date": date_str,
                "message_id": headers.get("Message-ID", "")
            })

        # Sort by date
        try:
            messages_with_dates.sort(
                key=lambda x: parsedate_to_datetime(x["date"]) if x["date"] else datetime.min
            )
        except Exception as e:
            logging.warning(f"Error sorting by date, using original order: {str(e)}")

        # Create clean conversation list
        for msg in messages_with_dates:
            conversation.append({
                "role": msg["role"],
                "content": msg["content"]
            })

        logging.info(f"Built conversation history with {len(conversation)} messages")
        if conversation:
            logging.info(f"Last message preview: {conversation[-1]['content'][:200]}...")
        
        return conversation
        
    except Exception as e:
        logging.error(f"Error building conversation history: {str(e)}")
        return []


# Default DAG arguments
default_args = {
    "owner": "sre_team",
    "depends_on_past": False,
    "start_date": datetime(2025, 10, 16),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# Logging setup
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


def decode_email_payload(msg):
    """Decode email payload, handling single-part and multipart emails."""
    try:
        if 'parts' in msg['payload']:
            for part in msg['payload']['parts']:
                content_type = part.get('mimeType', '')
                if content_type in ["text/plain", "text/html"]:
                    try:
                        if 'data' in part.get('body', {}):
                            body = base64.urlsafe_b64decode(part['body']['data']).decode('utf-8')
                            return body
                    except UnicodeDecodeError:
                        body = base64.urlsafe_b64decode(part['body']['data']).decode('latin-1')
                        return body
        else:
            try:
                if 'data' in msg['payload']['body']:
                    body = base64.urlsafe_b64decode(msg['payload']['body']['data']).decode('utf-8')
                    return body
            except UnicodeDecodeError:
                body = base64.urlsafe_b64decode(msg['payload']['body']['data']).decode('latin-1')
                return body
        return ""
    except Exception as e:
        logging.error(f"Error decoding email payload: {str(e)}")
        return ""

def remove_quoted_text(text):
    """Remove quoted email thread history from the email body."""
    try:
        logging.info("Removing quoted text from email body")
        patterns = [
            r'On\s.*?\swrote:',  # Standard 'On ... wrote:'
            r'-{2,}\s*Original Message\s*-{2,}',  # Outlook-style
            r'_{2,}\s*',  # Some clients use underscores
            r'From:\s*.*?\n',  # Quoted 'From:' headers
            r'>.*?\n'  # Lines starting with '>'
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
            if match:
                text = text[:match.start()].strip()
        lines = text.split('\n')
        cleaned_lines = [line for line in lines if not line.strip().startswith('>')]
        text = '\n'.join(cleaned_lines).strip()
        logging.info(f"Text after removing quoted text: {text[:100] if text else ''}...")
        return text if text else "No content after removing quoted text"
    except Exception as e:
        logging.error(f"Error in remove_quoted_text: {str(e)}")
        return text.strip()

def get_last_checked_timestamp():
    if os.path.exists(LAST_PROCESSED_EMAIL_FILE):
        with open(LAST_PROCESSED_EMAIL_FILE, "r") as f:
            last_checked = json.load(f).get("last_processed", None)
            if last_checked:
                return last_checked
    current_timestamp_ms = int(time.time() * 1000)
    update_last_checked_timestamp(current_timestamp_ms)
    return current_timestamp_ms

def update_last_checked_timestamp(timestamp):
    os.makedirs(os.path.dirname(LAST_PROCESSED_EMAIL_FILE), exist_ok=True)
    with open(LAST_PROCESSED_EMAIL_FILE, "w") as f:
        json.dump({"last_processed": timestamp}, f)

def fetch_reply_emails(**kwargs):
    service = authenticate_gmail()
    if not service:
        raise ValueError("Gmail authentication failed")

    last_checked_timestamp = get_last_checked_timestamp()
    query = f'is:unread subject:"BigQuery SRE Report" after:{last_checked_timestamp // 1000}'
    results = service.users().messages().list(userId="me", labelIds=["INBOX"], q=query).execute()
    messages = results.get("messages", [])
    reply_emails = []
    max_timestamp = last_checked_timestamp
    processed_label_id = get_or_create_label(service)

    for msg in messages:
        msg_data = service.users().messages().get(userId="me", id=msg["id"]).execute()
        labels = msg_data.get("labelIds", [])
        if processed_label_id in labels:
            logging.info(f"Message {msg['id']} already processed, skipping.")
            continue

        headers = {h["name"]: h["value"] for h in msg_data["payload"]["headers"]}
        sender = headers.get("From", "").lower()
        subject = headers.get("Subject", "")
        timestamp = int(msg_data["internalDate"])

        if BIGQUERY_SRE_FROM_ADDRESS.lower() in sender or timestamp <= last_checked_timestamp:
            logging.info(f"Message {msg['id']} is from self or old (timestamp: {timestamp}), skipping.")
            continue

        thread_id = msg_data["threadId"]
        thread = service.users().threads().get(userId="me", id=thread_id).execute()
        message_count = len(thread.get("messages", []))

        # Check if email is a reply (part of thread with >1 message or has "Re:" in subject)
        is_reply = message_count > 1 or subject.lower().startswith('re:')

        if is_reply:
            email_object = {
                "id": msg["id"],
                "threadId": thread_id,
                "headers": headers,
                "content": msg_data.get("snippet", ""),
                "timestamp": timestamp
            }
            reply_emails.append(email_object)
            if timestamp > max_timestamp:
                max_timestamp = timestamp
            # Mark as processed
            service.users().messages().modify(userId="me", id=msg['id'], body={"addLabelIds": [processed_label_id]}).execute()
        else:
            logging.info(f"Message {msg['id']} is not a reply (subject: {subject}, thread messages: {message_count}), skipping.")

    if reply_emails:
        update_last_checked_timestamp(max_timestamp)

    kwargs['ti'].xcom_push(key="reply_emails", value=reply_emails)
    return reply_emails

def branch_function(**kwargs):
    ti = kwargs['ti']
    reply_emails = ti.xcom_pull(task_ids="fetch_reply_emails", key="reply_emails")
    if reply_emails and len(reply_emails) > 0:
        logging.info("Reply emails found, proceeding to trigger response task.")
        return "trigger_reply_response_task"
    else:
        logging.info("No reply emails found, proceeding to no_email_found_task.")
        return "no_email_found_task"

def trigger_response_tasks(**kwargs):
    ti = kwargs['ti']
    reply_emails = ti.xcom_pull(task_ids="fetch_reply_emails", key="reply_emails")
    if not reply_emails:
        logging.info("No reply emails to process.")
        return

    for email in reply_emails:
        task_id = f"trigger_response_{email['id'].replace('-', '_')}"
        trigger_task = TriggerDagRunOperator(
            task_id=task_id,
            trigger_dag_id="bigquery_email_responder",
            conf={"thread_id": email["threadId"]},
        )
        trigger_task.execute(context=kwargs)
    ti.xcom_push(key="reply_emails", value=[])

def no_email_found(**kwargs):
    logging.info("No new reply emails found to process.")

with DAG(
    "bigquery_mailbox_monitor",
    default_args=default_args,
    schedule="*/1 * * * *",  # Every minute
    catchup=False,
    tags=["sre", "bigquery", "monitoring", "replies"]
) as monitor_dag:

    fetch_emails_task = PythonOperator(
        task_id="fetch_reply_emails",
        python_callable=fetch_reply_emails,
    )

    branch_task = BranchPythonOperator(
        task_id="branch_task",
        python_callable=branch_function,
    )

    trigger_reply_response_task = PythonOperator(
        task_id="trigger_reply_response_task",
        python_callable=trigger_response_tasks,
    )

    no_email_found_task = PythonOperator(
        task_id="no_email_found_task",
        python_callable=no_email_found,
    )

    # Task dependencies
    fetch_emails_task >> branch_task
    branch_task >> [trigger_reply_response_task, no_email_found_task]

# Processing functions for the reply responder DAG


# New: Categorize prompt
def categorize_prompt(ti, **context):
    dag_run = context.get('dag_run')
    thread_id = dag_run.conf.get('thread_id') if dag_run else None
    
    if not thread_id:
        raise ValueError("No thread_id provided in DAG configuration")
    
    logging.info(f"Processing thread ID: {thread_id}")
    
    service = authenticate_gmail()
    if not service:
        raise ValueError("Gmail authentication failed")
    
    self_email = Variable.get("BIGQUERY_SRE_FROM_ADDRESS", "")
    
    # Build conversation history (including all messages)
    history = build_conversation_history(service, thread_id, self_email)
    logging.info(f"Built conversation history with {len(history)} messages")
    
    # Get the current prompt from the LAST message in history (already cleaned)
    if not history:
        raise ValueError("No messages found in thread")
    
    # The last message in history IS the current prompt (already cleaned by build_conversation_history)
    current_message = history[-1]
    prompt = current_message['content']

    # Extract sender's name from the thread
    thread = service.users().threads().get(userId="me", id=thread_id).execute()
    messages = thread['messages']
    last_msg = messages[-1]
    headers = {h['name']: h['value'] for h in last_msg['payload']['headers']}
    sender_from = headers.get('From', '')
    logging.info(f"sender from address for the debug : {sender_from}")
    # Extract name from "Name <email>" or just use email
    sender_name = ""
    if '<' in sender_from:
        sender_name = sender_from.split('<')[0].strip().strip('"')
    else:
        sender_name = sender_from.split('@')[0]  # Use email prefix as fallback
    logging.info(f"sender name is {sender_name}")
    ti.xcom_push(key="sender_name", value=sender_name)
    
    if not prompt or not prompt.strip():
        # Fallback: try to get directly from thread
        thread = service.users().threads().get(userId="me", id=thread_id).execute()
        messages = thread['messages']
        last_msg = messages[-1]
        
        # Get snippet as last resort
        prompt = last_msg.get('snippet', '')
        logging.warning(f"Using snippet as fallback: {prompt[:100]}")
    
    if not prompt or not prompt.strip():
        raise ValueError("Could not extract prompt from the latest message")
    
    logging.info(f"prompt for the debugging: {prompt}")
    
    # Push only JSON-serializable data
    ti.xcom_push(key="conversation_history", value=history[:-1] if history else [])
    ti.xcom_push(key="current_prompt", value=prompt)
    
    cat_prompt = f"""
        Classify the following user query into exactly one of these categories based on its intent in the context of BigQuery SRE analysis, which includes query performance, Prometheus metrics (e.g., slot usage, execution time, errors, data scanned), cost optimization, schema alignment, and system reliability:

        1. Ask for Details - The user is requesting a detailed explanation, clarification, or deeper insight into a specific piece of information, metric, query, or finding from the report or context. Examples: "Explain why this query has high slot usage?", "What does the peak memory usage mean in this context?", "Provide more details on the recommended partitioning for the order table."

        2. Cost Analysis - The user is inquiring about cost-related metrics such as bytes processed, bytes billed, estimated cost, on-demand vs flat-rate pricing impact, or cost trends over time. This includes requests for cost breakdowns, savings opportunities, or optimization recommendations. Examples: "How much did this query cost?", "Can you estimate the $$ savings when I apply the recommendations assuming that these calls are each executed 3000 times a day", .. etc

        3. Non-Relevant Questions - The query does not fit into the above categories, is unrelated to BigQuery SRE metrics/analysis/optimization, or is invalid/incomplete in the BigQuery context (e.g., off-topic, vague without context, or not actionable). Examples: "What's the weather today?", "Tell me a joke", "How to install Airflow? ".

        Respond ONLY with the number (1, 2, or 3) corresponding to the best-fit category. Do not include any additional text, explanations, or reasoning.
        Query: {prompt}
    """
    
    # Pass history without the current message
    response = get_ai_response(cat_prompt, history[:-1] if len(history) > 1 else [])
    logging.info(f"AI response that we got is : {response}")
    # Extract the category as a digit from the response
    match = re.search(r'\b[1-3]\b', response)
    logging.info(f"matching text is : {match}")
    category = match.group(0) if match else '1'  # Default to '1' if no valid category is found
    if category not in ['1', '2', '3']:
        logging.info("random category provided")
        category = '1'  # Fallback
    
    # Push simple string values only
    ti.xcom_push(key="response", value=response)
    ti.xcom_push(key="category", value=category)
    
    logging.info(f"Categorized as {category}")
    return category

# New: Branch based on category
def branch_on_category(**context):
    category = context['ti'].xcom_pull(key="category")
    if category == '1':
        return 'ask_for_details'
    elif category == '2':
        return 'usage_analyzer'
    elif category == '3':
        return 'non_relevant_question'


# New: Send reply-all in thread
def send_reply(ti, **context):
    html_report = ti.xcom_pull(key="html_report") or ""
    if not html_report:
        raise ValueError("No HTML report")
    
    cleaned_response = re.sub(r'```html\n|```', '', html_report).strip()
    if not cleaned_response.startswith('<html>'):
        cleaned_response = f"<html><body>{cleaned_response}</body></html>"
    
    thread_id = context['params']['thread_id']
    service = authenticate_gmail()
    if not service:
        raise ValueError("Gmail authentication failed")
    
    thread = service.users().threads().get(userId="me", id=thread_id).execute()
    messages = thread['messages']
    last_msg = messages[-1]
    headers = {h['name']: h['value'] for h in last_msg['payload']['headers']}
    
    # Collect unique participants
    all_emails = set()
    for msg in messages:
        msg_headers = {h['name']: h['value'] for h in msg['payload']['headers']}
        all_emails.add(re.search(r'<(.*)>', msg_headers.get('From', '')).group(1) if re.search(r'<(.*)>', msg_headers.get('From', '')) else msg_headers.get('From'))
        all_emails.update([e.strip() for e in msg_headers.get('To', '').split(',') if e])
        all_emails.update([e.strip() for e in msg_headers.get('Cc', '').split(',') if e])
    
    self_email = Variable.get("BIGQUERY_SRE_FROM_ADDRESS", "")
    all_emails.discard(self_email)
    
    last_sender = re.search(r'<(.*)>', headers.get('From', '')).group(1) if re.search(r'<(.*)>', headers.get('From', '')) else headers.get('From')
    cc_emails = ', '.join(all_emails - {last_sender})
    
    msg = MIMEMultipart()
    msg["From"] = f"BigQuery SRE agent via lowtouch.ai <{self_email}>"
    msg["To"] = last_sender
    if cc_emails:
        msg["Cc"] = cc_emails
    msg["Subject"] = headers.get('Subject', f"Re: BigQuery SRE Report - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    msg["In-Reply-To"] = headers.get('Message-ID')
    msg["References"] = headers.get('Message-ID')
    msg.attach(MIMEText(cleaned_response, "html"))
    
    raw_msg = base64.urlsafe_b64encode(msg.as_string().encode("utf-8")).decode("utf-8")
    send_body = {"raw": raw_msg, "threadId": thread_id}
    result = service.users().messages().send(userId="me", body=send_body).execute()
    logging.info(f"Reply sent in thread {thread_id}")
    
    # Mark the latest message as processed
    processed_label_id = get_or_create_label(service)
    service.users().messages().modify(userId="me", id=last_msg['id'], body={"addLabelIds": [processed_label_id]}).execute()
    
    return f"Reply sent in thread {thread_id}"

def ask_for_details(ti, **context):
    history = ti.xcom_pull(key="conversation_history") or []
    prompt = ti.xcom_pull(key="current_prompt")
    sender_name = ti.xcom_pull(key="sender_name")
    detail_prompt = f"""
    The user asked: {prompt}
    Provide a detailed explanation regarding their request about BigQuery SRE metrics. 
    Structure the response as a reply email starting with a greeting (eg : Hi {sender_name}), followed by a detailed paragraph explanation, and include a few bullet points summarizing the key aspects. 
    Ensure the response is clear, concise, and formatted appropriately for an email body.
    ends the email body with 
    Thanks,
    
    BigQuery SRE agent @lowtouch.ai
    """
    response = get_ai_response(detail_prompt,history)
    ti.xcom_push(key="analysis_report", value=response)
    return response

def usage_analyzer(ti, **context):
    history = ti.xcom_pull(key="conversation_history") or []
    prompt = ti.xcom_pull(key="current_prompt")
    sender_name = ti.xcom_pull(key="sender_name")
    usage_prompt = f"""
        You are a **BigQuery Cost Analyst AI** at **lowtouch.ai**.

        The user asked: {prompt}

        Goal: Send a concise, **mathematically correct cost-savings email** that shows:
        (1) Savings for the **current small workload**, and
        (2) A **concrete assumed larger-scale example** (not multipliers), using explicit per-query bytes and slot usage.

        ----------------
        STRICT FORMATTING RULES (CRITICAL)
        ----------------
        1. **NO LATEX or MathJax**: Do not use symbols like \\frac, \\times, \\approx, or $$. 
        2. **Use Plain Text Arithmetic**: 
           - Use "*" for multiplication (e.g., "100 * 5 = 500").
           - Use "/" for division (e.g., "100 / 20 = 5").
           - Use "=" for equals.
           - Use plain text for units (e.g., "MB", "TiB").
        3. **Do not put calculations inside code blocks** (no ``` or `). Keep them as standard text lines.

        ----------------
        Billing Logic
        ----------------
        - If billing model is unknown, default to **on-demand** ($6 per **TiB** scanned; 1 TiB = 1024 GiB; 1 GiB = 1024 MiB).  
        If a different rate is given (e.g., regional discount or $5/TB legacy), use that.
        - If the SRE context indicates **capacity/editions (slot-based)** billing and slot/runtime metrics exist, also show a **slot-hours** view using these edition prices **if exact price is not provided**:
        • Standard: $0.04/slot-hour • Enterprise: $0.06/slot-hour • Enterprise Plus: $0.10/slot-hour
        - Bytes-scanned drives on-demand cost; **slot-hours** (concurrency × runtime) drive capacity cost.

        ----------------
        What to Calculate
        ----------------
        A) **Current Workload (Small-Scale)**
        - Inputs (use actuals if given): queries/day, bytes/query, efficiency gain (% reduction from optimizations).
        - On-demand: show **before vs after** daily & annual costs and savings with explicit unit conversions (MB→GB→TiB).
        - If slot/runtime metrics AND capacity billing context exist, also show slot-based before vs after (daily & annual).
            • If only bytes-reduction% is known, assume slot-ms (or slot-hours) reduce by the same % unless better signals are present.

        B) **Assumed Larger-Scale Example (Concrete, not multipliers)**
        - Write a clearly labeled example block with **explicit assumptions**, e.g.:
            **Estimated Cost Savings (Assumed Larger-Scale Example):**
            - **Assumptions**: Provide all assumptions as a single paragraph, including (change if better org defaults exist):
            • Per-query bytes (e.g., assume 10 GB/query if not specified, to reflect a larger enterprise workload).  
            • Query frequency (e.g., mentioned query or a high volume query count for a high-volume BigQuery user).  
            • Efficiency gain after optimizations (e.g., 40% or use % from SRE report).  
            • On-demand rate (e.g., $6/TiB; override if org rate differs).  
            • Capacity (if applicable): assume avg runtime (e.g., 60s/query) at concurrent slots (e.g., 200; edit if real slot/run data exists).
            - Provide calculations as bullet points showing transparent steps (no hidden steps) and round sensibly:
            • For **on-demand math**: Calculate current daily bytes, current daily cost, after reduction bytes and cost, then daily and annual savings.
            • For **capacity (slot-based) math** (if applicable): Calculate slot-seconds/query, slot-hours/query, current slot-hours/day, cost/day (show range for edition prices if unknown), after reduction slot-hours/day (reduce by efficiency gain % or use measured runtime deltas if available), then daily and annual savings for each edition price.
        ----------------
        What to Write (Email format)
        ----------------
        Start:
        Hi {sender_name},

        Sections:
        1) **Executive Summary** — one paragraph with the headline daily & annual savings for the current workload (small) and the **assumed larger-scale example**.
        2) **Current Scenario** — inputs and before/after costs (on-demand; add slot-based if applicable).
        3) **After Optimization** — list key actions (partitioning, clustering, selective filters, query refactor/materialized views if relevant) and efficiency%.
        4) **Cost Savings** — bullet/table with formulas and results (daily & annual).
        5) **Assumed Larger-Scale Example** — the concrete example block above (on-demand and, if applicable, capacity).
        6) **Notes**
        - If current total < $1/day or < $100/year, add:
            *Note: The current dataset is small, so visible savings are modest. In production, the same optimizations yield materially larger savings.*
        - Clarify pricing drivers (bytes for on-demand; slot-hours for capacity).

        End with:
        Thanks,

        BigQuery SRE Agent @lowtouch.ai

        ----------------
        Quality Bar
        ----------------
        - All numbers must be **internally consistent** with explicit unit conversions. 
        - If any input is missing, **state the assumption** clearly and proceed.
        - Prefer simple, readable math for the assumed section so non-SRE readers can follow.
        - Use American English spelling and grammar.
        """
    response = get_ai_response(usage_prompt,history)
    ti.xcom_push(key="analysis_report", value=response)
    return response

def non_relevant_question(ti, **context):
    sender_name = ti.xcom_pull(key="sender_name")
    response = f"""
    <html>
        <body>
            <p>Hi {sender_name},</p>
            <p>Thank you for reaching out. Unfortunately, I am not trained to answer your query at this time.</p>
            <p>If you have any other questions or need assistance, feel free to let us know.</p>
            <p>Best regards,<br><br>BigQuery SRE agent @lowtouch.ai</p>
        </body>
    </html>
    """
    ti.xcom_push(key="html_report", value=response)
    return response

def convert_to_html(ti, **context):
    report = ti.xcom_pull(key="checked_report")
    sender_name = ti.xcom_pull(key="sender_name")
    html_prompt = f"""
    Convert the following email body into valid HTML format for a professional reply email.

    Requirements:
    - Start with a greeting like "Hi {sender_name}," or "Hello {sender_name}," .
    - Preserve all structure using only basic HTML tags (<p>, <b>, <h1>-<h3>, <ul>, <li>, <code>, <br>).
    - Do NOT include any inline styles, colors, padding, or CSS.
    - **CRITICAL: Format calculations as plain text.** - Do NOT use LaTeX (no \\frac, \\times).
    - Convert headings, bullet points, and bold text appropriately.
    - At the end of the email, append exactly:
        Thanks,<br><br>
        
        BigQuery SRE agent @lowtouch.ai
    - Do NOT include an email subject line or any commentary.

    email content:
    {report}

    Output ONLY the pure HTML code for the email body.
    """
    response = get_ai_response(html_prompt)
    ti.xcom_push(key="html_report", value=response)
    return response

def response_checker(ti, **context):
    prompt = ti.xcom_pull(key="current_prompt")
    report = ti.xcom_pull(key="analysis_report")
    history = ti.xcom_pull(key="conversation_history") or []
    logging.info(f"prompt : {prompt}")
    if not report:
        raise ValueError("No analysis report available")
    
    for i in range(3):
        check_prompt = f"""
        You are an evaluator. Compare the user's Query to the proposed email body (Report) and judge how well the Report addresses the Query.
        Focus on relevance, completeness, accuracy, actionable guidance, clarity, and professional tone for a BigQuery SRE reply.

        Inputs:
        Query: {prompt}
        Report: {report}

        Scoring rules:
        - Score is an integer 1-10 (1 = no coverage, 10 = fully covers all required aspects).
        - "areas_to_improve" is a short array of specific actionable items (concise phrases).

        Requirements:
        - Output ONLY valid JSON (no explanation, no markdown, no commentary).
        - Use this exact JSON schema and only these keys: {{ "score": int, "areas_to_improve": [string] }}
        - Use double quotes for JSON keys and string values.
        - Be concise and concrete in the arrays.

        Now evaluate and output the JSON.
        """
        response = get_ai_response(check_prompt,history)
        try:
            
            # Clean response by removing possible JSON code block markers
            cleaned_response = response.replace('```json', '').replace('```', '').strip()
            json_resp = json.loads(cleaned_response)
            score = int(json_resp.get("score", 0))
            areas_to_improve = json_resp.get("areas_to_improve", "")
        except (json.JSONDecodeError, ValueError):
            logging.error("Invalid JSON from AI in response_checker")
            score = 0
            areas_to_improve = "Parsing error - improve overall structure"
        
        logging.info(f"Iteration {i+1}: Score {score}, Areas: {areas_to_improve}")
        
        if score >= 8 or i == 2:
            break
        
        improve_prompt = f"""
        The following email body needs improvement based on the specified areas. Ensure that all sections in the areas of improvement are addressed comprehensively without omitting any current content. Refine the language, structure, and formatting to enhance clarity, professionalism, and readability.

        Areas to improve: {areas_to_improve}
        
        Original email body:
        {report}
        
        Provide the fully improved email body as a complete and polished response.
        """
        new_report = get_ai_response(improve_prompt,history)
        report = new_report if new_report else report  # Fallback to original if empty
    
    ti.xcom_push(key="checked_report", value=report)
    logging.info(f"Final checked report: {report[:200]}...")
    return report

# Processing DAG: Handles reply logic
with DAG(
    "bigquery_email_responder",
    default_args=default_args,
    schedule=None,  # Triggered only
    catchup=False,
    tags=["sre", "bigquery", "email", "responder"]
) as processor_dag:
    
    categorize = PythonOperator(
        task_id="categorize_prompt",
        python_callable=categorize_prompt,
    )
    
    branch = BranchPythonOperator(
        task_id="branch_on_category",
        python_callable=branch_on_category,
    )
    
    t1 = PythonOperator(
        task_id="ask_for_details",
        python_callable=ask_for_details,
    )
    
    t2 = PythonOperator(
        task_id="usage_analyzer",
        python_callable=usage_analyzer,
    )
    
    t3 = PythonOperator(
        task_id="non_relevant_question",
        python_callable=non_relevant_question,
    )
    
    # t4 = PythonOperator(
    #     task_id="check_memory_usage",
    #     python_callable=check_memory_usage,
    #     provide_context=True
    # )
    
    t5 = PythonOperator(
        task_id="response_checker",
        python_callable=response_checker,  # Modified version
        provide_context=True,
        trigger_rule='one_success'
    )
    
    t6 = PythonOperator(
        task_id="convert_to_html",
        python_callable=convert_to_html,
        provide_context=True,
        trigger_rule='one_success'
    )
    
    t7 = PythonOperator(
        task_id="send_reply",
        python_callable=send_reply,
        provide_context=True,
        trigger_rule='one_success'
    )
    
    categorize >> branch >> [t1, t2, t3]  # Branch connects to possible starts
    t1 >> t5
    t2 >> t5
    t3 >> t7
    t5 >> t6 >> t7
