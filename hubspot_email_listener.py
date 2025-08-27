import base64
from email import message_from_bytes
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import os
import json
import time
import logging
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

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
LAST_PROCESSED_EMAIL_FILE = "/appz/cache/hubspot_last_processed_email.json"
THREAD_CONTEXT_FILE = "/appz/cache/hubspot_thread_context.json"

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

def get_thread_context():
    try:
        if os.path.exists(THREAD_CONTEXT_FILE):
            with open(THREAD_CONTEXT_FILE, "r") as f:
                return json.load(f)
    except Exception as e:
        logging.error(f"Error reading {THREAD_CONTEXT_FILE}: {e}")
        return {}

def update_thread_context(thread_id, context_update):
    """Update the thread context for a given thread_id with the provided context_update dict."""
    try:
        os.makedirs(os.path.dirname(THREAD_CONTEXT_FILE), exist_ok=True)
        contexts = {}
        if os.path.exists(THREAD_CONTEXT_FILE):
            with open(THREAD_CONTEXT_FILE, "r") as f:
                contexts = json.load(f)
        if thread_id not in contexts:
            contexts[thread_id] = {}
        contexts[thread_id].update(context_update)
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)
        logging.info(f"Updated thread context for thread_id={thread_id}")
    except Exception as e:
        logging.error(f"Error updating thread context for thread_id={thread_id}: {e}")

def debug_thread_context():
    """Debug function to log current thread context state"""
    try:
        contexts = get_thread_context()
        logging.info("=== CURRENT THREAD CONTEXTS ===")
        for thread_id, context in contexts.items():
            logging.info(f"Thread {thread_id}:")
            logging.info(f"  - confirmation_needed: {context.get('confirmation_needed', 'NOT SET')}")
            logging.info(f"  - confirmation_sent: {context.get('confirmation_sent', 'NOT SET')}")
            logging.info(f"  - awaiting_reply: {context.get('awaiting_reply', 'NOT SET')}")
            logging.info(f"  - has search_results: {bool(context.get('search_results', {}))}")
            logging.info(f"  - timestamp: {context.get('confirmation_timestamp', 'NOT SET')}")
        logging.info("=== END THREAD CONTEXTS ===")
        if not contexts:
            logging.info("No thread contexts found!")
    except Exception as e:
        logging.error(f"Error reading thread contexts: {e}")

# Add this function to your monitor DAG (hubspot_monitor_mailbox.py)

# Add this function to your monitor DAG (hubspot_monitor_mailbox.py)

def get_email_thread(service, email_data):
    """Retrieve the full email thread history, ensuring all messages are included."""
    try:
        # Validate email_data structure
        if not email_data or "headers" not in email_data or not isinstance(email_data.get("headers"), dict):
            logging.error("Invalid email_data: 'headers' key missing or not a dictionary")
            return []

        thread_id = email_data.get("threadId")
        message_id = email_data["headers"].get("Message-ID", "")
        email_id = email_data.get("id", "")

        # Check stored thread context for existing thread history
        stored_context = get_thread_context().get(thread_id, {})
        stored_thread_history = stored_context.get("thread_history", [])
        if stored_thread_history:
            logging.info(f"Using stored thread history for thread {thread_id} with {len(stored_thread_history)} messages")
            return stored_thread_history

        # If no thread ID, try to find it using the message ID
        if not thread_id:
            logging.warning(f"No thread ID provided for message ID {message_id}. Querying Gmail API.")
            query_result = service.users().messages().list(userId="me", q=f"rfc822msgid:{message_id}").execute()
            messages = query_result.get("messages", [])
            if messages:
                message = service.users().messages().get(userId="me", id=messages[0]["id"]).execute()
                thread_id = message.get("threadId")
                logging.info(f"Resolved thread ID: {thread_id} for message ID {message_id}")

        # If still no thread ID, treat as a single email
        if not thread_id:
            logging.warning(f"No thread ID resolved for message ID {message_id}. Treating as single email.")
            raw_message = service.users().messages().get(userId="me", id=email_id, format="raw").execute()
            msg = message_from_bytes(base64.urlsafe_b64decode(raw_message["raw"]))
            content = decode_email_payload(msg)
            headers = email_data.get("headers", {})
            from_address = headers.get("From", "").lower()
            is_from_bot = HUBSPOT_FROM_ADDRESS.lower() in from_address
            email_entry = {
                "headers": headers,
                "content": content.strip(),
                "timestamp": int(email_data.get("internalDate", 0)),
                "from_bot": is_from_bot,
                "message_id": email_id
            }
            logging.info(f"Single email processed: message_id={email_id}, from={headers.get('From', 'Unknown')}, timestamp={email_entry['timestamp']}")
            return [email_entry]

        # Fetch the full thread with pagination
        email_thread = []
        page_token = None
        while True:
            thread_request = service.users().threads().get(userId="me", id=thread_id)
            if page_token:
                thread_request = thread_request.pageToken(page_token)
            thread = thread_request.execute()
            messages = thread.get("messages", [])
            email_thread.extend(messages)
            
            page_token = thread.get("nextPageToken")
            if not page_token:
                break

        logging.info(f"Processing thread {thread_id} with {len(email_thread)} messages")
        processed_thread = []
        for msg in email_thread:
            # Fetch raw message if not included
            raw_msg = base64.urlsafe_b64decode(msg["raw"]) if "raw" in msg else None
            if not raw_msg:
                raw_message = service.users().messages().get(userId="me", id=msg["id"], format="raw").execute()
                raw_msg = base64.urlsafe_b64decode(raw_message["raw"])

            email_msg = message_from_bytes(raw_msg)
            headers = {h["name"]: h["value"] for h in msg.get("payload", {}).get("headers", [])}
            content = decode_email_payload(email_msg)
            from_address = headers.get("From", "").lower()
            is_from_bot = HUBSPOT_FROM_ADDRESS.lower() in from_address

            processed_thread.append({
                "headers": headers,
                "content": content.strip(),
                "timestamp": int(msg.get("internalDate", 0)),
                "from_bot": is_from_bot,
                "message_id": msg.get("id", "")
            })

        # Sort by timestamp (ascending) to ensure correct conversation order
        processed_thread.sort(key=lambda x: x.get("timestamp", 0))
        
        # Log thread details for debugging
        logging.info(f"Retrieved thread {thread_id} with {len(processed_thread)} messages")
        for idx, email in enumerate(processed_thread, 1):
            logging.info(f"Email {idx}: message_id={email['message_id']}, from={email['headers'].get('From', 'Unknown')}, timestamp={email['timestamp']}, from_bot={email['from_bot']}, content_preview={email['content'][:100]}...")

        # Store thread history in context
        update_thread_context(thread_id, {"thread_history": processed_thread})
        return processed_thread

    except Exception as e:
        logging.error(f"Error retrieving email thread for thread_id={thread_id}: {e}", exc_info=True)
        return []

def decode_email_payload(msg):
    """Decode email payload - ALSO MISSING"""
    try:
        if msg.is_multipart():
            for part in msg.walk():
                content_type = part.get_content_type()
                if content_type in ["text/plain", "text/html"]:
                    try:
                        return part.get_payload(decode=True).decode()
                    except UnicodeDecodeError:
                        return part.get_payload(decode=True).decode('latin-1')
        else:
            try:
                return msg.get_payload(decode=True).decode()
            except UnicodeDecodeError:
                return msg.get_payload(decode=True).decode('latin-1')
        return ""
    except Exception as e:
        logging.error(f"Error decoding email payload: {e}")
        return ""

def fetch_unread_emails(**kwargs):
    """FIXED VERSION - Prevents duplicate email processing"""
    service = authenticate_gmail()
    if not service:
        logging.error("Gmail authentication failed, skipping email fetch.")
        kwargs['ti'].xcom_push(key="unread_emails", value=[])
        return []
    
    last_checked_timestamp = get_last_checked_timestamp()
    
    # FIX 1: Use proper timestamp format for Gmail API (seconds, not milliseconds)
    last_checked_seconds = last_checked_timestamp // 1000 if last_checked_timestamp > 1000000000000 else last_checked_timestamp
    
    # FIX 2: Add more specific query to avoid edge cases
    query = f"is:unread after:{last_checked_seconds}"
    
    logging.info(f"Using query: {query}")
    logging.info(f"Last checked timestamp: {last_checked_timestamp} (converted: {last_checked_seconds})")
    
    try:
        results = service.users().messages().list(userId="me", labelIds=["INBOX"], q=query).execute()
        messages = results.get("messages", [])
    except Exception as e:
        logging.error(f"Error fetching messages: {e}")
        kwargs['ti'].xcom_push(key="unread_emails", value=[])
        return []
    
    unread_emails = []
    max_timestamp = last_checked_timestamp
    thread_context = get_thread_context()
    
    # FIX 3: Track processed message IDs to avoid duplicates within same run
    processed_message_ids = set()
    
    logging.info(f"Found {len(messages)} unread messages to process")
    logging.info(f"Current thread contexts: {list(thread_context.keys())}")
    
    for msg in messages:
        msg_id = msg["id"]
        
        # FIX 4: Skip if already processed in this run
        if msg_id in processed_message_ids:
            logging.info(f"Skipping already processed message in this run: {msg_id}")
            continue
            
        try:
            msg_data = service.users().messages().get(userId="me", id=msg_id).execute()
        except Exception as e:
            logging.error(f"Error fetching message {msg_id}: {e}")
            continue
            
        headers = {h["name"]: h["value"] for h in msg_data["payload"]["headers"]}
        sender = headers.get("From", "").lower()
        timestamp = int(msg_data["internalDate"])
        thread_id = msg_data.get("threadId", "")
        
        logging.info(f"Processing message ID: {msg_id}, From: {sender}, Timestamp: {timestamp}, Thread ID: {thread_id}")
        
        # FIX 5: More precise timestamp filtering to prevent reprocessing
        if timestamp <= last_checked_timestamp:
            logging.info(f"Skipping message {msg_id} - timestamp {timestamp} <= last_checked {last_checked_timestamp}")
            continue
        
        # Skip no-reply emails and old emails
        if "no-reply" in sender:
            logging.info(f"Skipping no-reply email from: {sender}")
            continue
        
        # Skip emails from our own bot to avoid loops
        if sender == HUBSPOT_FROM_ADDRESS.lower():
            logging.info(f"Skipping email from bot itself: {sender}")
            continue
            
        email_object = {
            "id": msg_id,
            "threadId": thread_id,
            "headers": headers,
            "content": msg_data.get("snippet", ""),
            "timestamp": timestamp
        }
        
        # Reply detection logic (keeping the existing sophisticated logic)
        is_reply = False
        thread_context_data = {}
        
        if thread_id:
            thread_context_data = thread_context.get(thread_id, {})
            
            # Check email headers for reply indicators
            subject = headers.get("Subject", "")
            in_reply_to = headers.get("In-Reply-To", "")
            references = headers.get("References", "")
            
            # Multiple ways to detect replies
            is_subject_reply = subject.lower().startswith("re:")
            has_reply_headers = bool(in_reply_to or references)
            
            # Context-based detection
            has_confirmation_sent = thread_context_data.get("confirmation_sent", False)
            is_awaiting_reply = thread_context_data.get("awaiting_reply", False)
            
            # Reply detection logic
            context_based_reply = has_confirmation_sent and is_awaiting_reply and (is_subject_reply or has_reply_headers)
            stored_context_reply = (is_subject_reply or has_reply_headers) and thread_id in thread_context
            fallback_header_reply = is_subject_reply or has_reply_headers
            
            is_reply = context_based_reply or stored_context_reply or fallback_header_reply
            
            logging.info(f"Thread {thread_id} analysis:")
            logging.info(f"  - has_confirmation_sent: {has_confirmation_sent}")
            logging.info(f"  - is_awaiting_reply: {is_awaiting_reply}")
            logging.info(f"  - is_subject_reply: {is_subject_reply}")
            logging.info(f"  - has_reply_headers: {has_reply_headers}")
            logging.info(f"  - FINAL is_reply: {is_reply}")
        else:
            logging.info(f"No thread_id for message {msg_id}")
        
        email_object["is_reply"] = is_reply
        email_object["thread_context"] = thread_context_data
        
        unread_emails.append(email_object)
        processed_message_ids.add(msg_id)  # FIX 6: Track processed IDs
        
        # FIX 7: Update max_timestamp properly
        if timestamp > max_timestamp:
            max_timestamp = timestamp
            
    # FIX 8: Always update timestamp, even if no new emails (prevents stuck state)
    if messages:  # Only update if there were messages to process
        # Add small buffer to prevent boundary issues
        update_timestamp = max_timestamp + 1
        update_last_checked_timestamp(update_timestamp)
        logging.info(f"Updated last checked timestamp to: {update_timestamp}")
    else:
        logging.info("No messages found, timestamp unchanged")
        
    kwargs['ti'].xcom_push(key="unread_emails", value=unread_emails)
    logging.info(f"Final results: {len(unread_emails)} unread emails, {sum(1 for e in unread_emails if e['is_reply'])} are replies")
    
    return unread_emails


# ADDITIONAL HELPER FUNCTIONS TO PREVENT ISSUES

def get_last_checked_timestamp_safe():
    """Safe wrapper for getting last checked timestamp"""
    try:
        timestamp = get_last_checked_timestamp()
        # Ensure we have a valid timestamp
        if not timestamp or timestamp <= 0:
            # If no valid timestamp, start from 24 hours ago
            import time
            timestamp = int((time.time() - 86400) * 1000)  # 24 hours ago in milliseconds
            logging.info(f"No valid timestamp found, starting from 24h ago: {timestamp}")
        return timestamp
    except Exception as e:
        logging.error(f"Error getting last checked timestamp: {e}")
        import time
        return int((time.time() - 86400) * 1000)


def update_last_checked_timestamp_safe(timestamp):
    """Safe wrapper for updating timestamp with validation"""
    try:
        if not timestamp or timestamp <= 0:
            logging.error(f"Invalid timestamp provided: {timestamp}")
            return False
            
        # Ensure timestamp is reasonable (not too far in future)
        import time
        current_time = int(time.time() * 1000)
        if timestamp > current_time + 3600000:  # More than 1 hour in future
            logging.warning(f"Timestamp {timestamp} seems too far in future, using current time")
            timestamp = current_time
            
        update_last_checked_timestamp(timestamp)
        logging.info(f"Successfully updated timestamp to: {timestamp}")
        return True
    except Exception as e:
        logging.error(f"Error updating timestamp: {e}")
        return False


def mark_message_as_read(service, message_id):
    """Mark a processed message as read to prevent reprocessing"""
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

def branch_function(**kwargs):
    ti = kwargs['ti']
    unread_emails = ti.xcom_pull(task_ids="fetch_unread_emails", key="unread_emails")
    
    if not unread_emails:
        logging.info("No unread emails found, proceeding to no_email_found_task.")
        return "no_email_found_task"
    
    # Separate replies from new emails
    replies = [email for email in unread_emails if email.get("is_reply", False)]
    new_emails = [email for email in unread_emails if not email.get("is_reply", False)]
    
    logging.info(f"Email classification: {len(replies)} replies, {len(new_emails)} new emails")
    
    # Process replies first - they have higher priority
    if replies:
        logging.info(f"Found {len(replies)} replies, triggering continuation DAG")
        # Store only replies for processing
        ti.xcom_push(key="reply_emails", value=replies)
        return "trigger_continuation_dag"
    
    # If no replies, process new meeting minutes requests
    if new_emails:
        logging.info(f"Found {len(new_emails)} new emails, proceeding to trigger meeting minutes tasks")
        # Store only new emails for processing
        ti.xcom_push(key="new_emails", value=new_emails)
        return "trigger_meeting_minutes"
    
    logging.info("No actionable emails found, proceeding to no_email_found_task")
    return "no_email_found_task"

def trigger_meeting_minutes(**kwargs):
    ti = kwargs['ti']
    # Get new emails from branch function
    new_emails = ti.xcom_pull(task_ids="branch_task", key="new_emails") or []
    
    if not new_emails:
        logging.info("No new emails to process for meeting minutes")
        return
    
    for email in new_emails:
        task_id = f"trigger_response_{email['id'].replace('-', '_')}"
        trigger_task = TriggerDagRunOperator(
            task_id=task_id,
            trigger_dag_id="hubspot_search_entities",
            conf={"email_data": email},
        )
        trigger_task.execute(context=kwargs)
    
    logging.info(f"Triggered meeting minutes search for {len(new_emails)} new emails")

def trigger_continuation_dag(**kwargs):
    ti = kwargs['ti']
    reply_emails = ti.xcom_pull(task_ids="branch_task", key="reply_emails") or []
    unread_emails = ti.xcom_pull(task_ids="fetch_unread_emails", key="unread_emails") or []
    
    if not reply_emails:
        logging.info("No reply emails to process for continuation")
        return

    service = authenticate_gmail()
    if not service:
        logging.error("Gmail authentication failed, cannot fetch thread history")
        return

    thread_context = get_thread_context()
    
    for email in reply_emails:
        reply_thread_id = email["threadId"]
        reply_message_id = email["headers"].get("Message-ID", "")
        reply_references = email["headers"].get("References", "")
        
        # Find the original thread_id by matching References or Message-ID
        original_thread_id = None
        for thread_id, context in thread_context.items():
            stored_references = context.get("references", "")
            stored_original_message_id = context.get("original_message_id", "")
            stored_confirmation_message_id = context.get("confirmation_message_id", "")
            if (stored_original_message_id and stored_original_message_id in reply_references) or \
               (stored_confirmation_message_id and stored_confirmation_message_id in reply_references) or \
               (reply_message_id and reply_message_id in stored_references):
                original_thread_id = thread_id
                break
        
        if not original_thread_id:
            logging.error(f"No matching thread context found for reply email {email['id']} with threadId={reply_thread_id}")
            logging.info(f"Reply email References: {reply_references}")
            logging.info(f"Available thread_context keys: {list(thread_context.keys())}")
            # Fallback: Fetch thread history using reply_thread_id
            full_thread_history = get_email_thread(service, email)
            if full_thread_history:
                original_thread_id = reply_thread_id
                logging.info(f"Using reply_thread_id={reply_thread_id} as fallback")
            else:
                logging.error(f"Failed to fetch thread history for reply email {email['id']}")
                continue
        
        logging.info(f"Matched reply email {email['id']} to original thread_id={original_thread_id} (reply threadId={reply_thread_id})")
        
        # Retrieve stored context for the original thread_id
        stored_context = thread_context.get(original_thread_id, {})
        if not stored_context:
            logging.error(f"No stored context found for original thread_id={original_thread_id}")
            continue
        
        # Retrieve thread history
        full_thread_history = stored_context.get("thread_history", [])
        if not full_thread_history:
            logging.info(f"No stored thread history for thread {original_thread_id}, fetching from Gmail API")
            full_thread_history = get_email_thread(service, email)
        
        if not full_thread_history:
            logging.error(f"Failed to fetch thread history for thread {original_thread_id}")
            continue

        # Log thread history for debugging
        logging.info(f"Thread {original_thread_id} full history: {len(full_thread_history)} emails")
        for idx, msg in enumerate(full_thread_history, 1):
            logging.info(f"Thread {original_thread_id} Email {idx}: message_id={msg['message_id']}, from={msg['headers'].get('From', 'Unknown')}, timestamp={msg['timestamp']}, content_preview={msg['content'][:100]}...")

        # Prepare configuration for continuation DAG
        continuation_conf = {
            "email_data": email,
            "search_results": stored_context.get("search_results", {}),
            "thread_id": original_thread_id,
            "full_thread_history": full_thread_history,
            "original_confirmation_email": stored_context.get("confirmation_email", ""),
            "user_response_email": email,
            "unread_emails": unread_emails
        }

        # Log continuation_conf for debugging
        logging.info(f"Continuation conf for thread {original_thread_id}:")
        logging.info(f"  - email_data: {json.dumps({k: v for k, v in email.items() if k != 'content'}, indent=2)}")
        logging.info(f"  - search_results: {json.dumps(continuation_conf['search_results'], indent=2)}")
        logging.info(f"  - full_thread_history: {len(continuation_conf['full_thread_history'])} emails")
        logging.info(f"  - original_confirmation_email: {continuation_conf['original_confirmation_email'][:100] if continuation_conf['original_confirmation_email'] else 'None'}")
        logging.info(f"  - user_response_email: {json.dumps({k: v for k, v in email.items() if k != 'content'}, indent=2)}")
        logging.info(f"  - thread_id: {original_thread_id}")

        # Trigger continuation DAG
        task_id = f"trigger_continuation_{email['id'].replace('-', '_')}"
        trigger_task = TriggerDagRunOperator(
            task_id=task_id,
            trigger_dag_id="hubspot_meeting_minutes_continue",
            conf=continuation_conf,
        )
        trigger_task.execute(context=kwargs)
        
        logging.info(f"Triggered continuation DAG for thread {original_thread_id}")

    logging.info(f"Triggered continuation for {len(reply_emails)} reply emails")

def no_email_found(**kwargs):
    logging.info("No new emails or replies found to process.")

readme_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'hubspot_monitor_meeting_minutes.md')
readme_content = "HubSpot Meeting Minutes Mailbox Monitor DAG"
try:
    with open(readme_path, 'r') as file:
        readme_content = file.read()
except FileNotFoundError:
    pass

with DAG(
    "hubspot_monitor_mailbox",
    default_args=default_args,
    schedule_interval=timedelta(minutes=1),
    catchup=False,
    doc_md=readme_content,
    tags=["hubspot", "monitor", "email", "mailbox"]
) as dag:

    fetch_emails_task = PythonOperator(
        task_id="fetch_unread_emails",
        python_callable=fetch_unread_emails,
        provide_context=True
    )

    branch_task = BranchPythonOperator(
        task_id="branch_task",
        python_callable=branch_function,
        provide_context=True
    )

    trigger_meeting_minutes_task = PythonOperator(
        task_id="trigger_meeting_minutes",
        python_callable=trigger_meeting_minutes,
        provide_context=True
    )

    trigger_continuation_task = PythonOperator(
        task_id="trigger_continuation_dag",
        python_callable=trigger_continuation_dag,
        provide_context=True
    )

    no_email_found_task = PythonOperator(
        task_id="no_email_found_task",
        python_callable=no_email_found,
        provide_context=True
    )

    fetch_emails_task >> branch_task >> [trigger_meeting_minutes_task, trigger_continuation_task, no_email_found_task]
