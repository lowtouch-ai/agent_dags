import base64
from email import message_from_bytes
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from airflow.models import Variable
from datetime import datetime, timedelta, timezone
import os
import json
import time
import logging
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import re
from ollama import Client
import requests
import uuid
import openpyxl
import pandas as pd
from email.mime.base import MIMEBase
from email import encoders
from openpyxl.utils import get_column_letter
from openpyxl.styles import Font
import pytz

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
LAST_PROCESSED_EMAIL_FILE = "/appz/cache/hubspot_last_processed_email.json"
OLLAMA_HOST = Variable.get("ltai.v3.hubspot.ollama.host","http://agentomatic:8000")
SERVER_URL = Variable.get("ltai.v3.hubspot.server.url")
HUBSPOT_API_KEY = Variable.get("ltai.v3.husbpot.api.key")
HUBSPOT_BASE_URL = Variable.get("ltai.v3.hubspot.url")
REPORT_ARCHIVE_DIR = "/appz/cache/archive/reports/hubspot"

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

def decode_email_payload(msg):
    """Decode email payload to extract text content."""
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

def is_email_whitelisted(sender_email):
    """Check if sender email is in the whitelist."""
    try:
        whitelisted = json.loads(Variable.get("ltai.v3.hubspot.email.whitelist", default_var="[]"))
        # Extract email from "Name <email@domain.com>" format
        email_match = re.search(r'<(.+?)>', sender_email)
        clean_email = email_match.group(1).lower() if email_match else sender_email.lower()
        
        is_allowed = clean_email in [email.lower() for email in whitelisted]
        logging.info(f"Email validation - Sender: {clean_email}, Whitelisted: {is_allowed}")
        return is_allowed
    except Exception as e:
        logging.error(f"Error checking whitelist: {e}")
        return False

def get_email_thread(service, email_data):
    """
    Retrieve full email thread by reconstructing from References header and searching for each message.
    """
    try:
        if not email_data or "headers" not in email_data:
            logging.error("Invalid email_data: 'headers' key missing")
            return []

        thread_id = email_data.get("threadId")
        headers = email_data.get("headers", {})
        current_message_id = headers.get("Message-ID", "")
        email_id = email_data.get("id", "")
        
        logging.info(f"Processing email: email_id={email_id}, thread_id={thread_id}")

        # ✅ Extract all message IDs from References header
        references = headers.get("References", "")
        in_reply_to = headers.get("In-Reply-To", "")
        
        # Parse message IDs from References
        message_ids = []
        if references:
            # References contains space-separated Message-IDs
            message_ids = re.findall(r'<([^>]+)>', references)
        if in_reply_to:
            reply_to_id = re.search(r'<([^>]+)>', in_reply_to)
            if reply_to_id and reply_to_id.group(1) not in message_ids:
                message_ids.append(reply_to_id.group(1))
        
        # Add current message ID
        current_id = re.search(r'<([^>]+)>', current_message_id)
        if current_id:
            message_ids.append(current_id.group(1))
        
        logging.info(f"Found {len(message_ids)} message IDs in thread from References header")
        
        # Now fetch each message by searching for its Message-ID
        processed_thread = []
        for msg_id in message_ids:
            try:
                # Search for message by RFC822 Message-ID
                search_query = f'rfc822msgid:{msg_id}'
                search_result = service.users().messages().list(
                    userId="me",
                    q=search_query,
                    maxResults=1
                ).execute()
                
                messages = search_result.get("messages", [])
                if not messages:
                    logging.warning(f"Could not find message with ID: {msg_id}")
                    continue
                
                # Fetch the full message
                gmail_msg_id = messages[0]["id"]
                raw_message = service.users().messages().get(
                    userId="me",
                    id=gmail_msg_id,
                    format="raw"
                ).execute()
                
                raw_msg = base64.urlsafe_b64decode(raw_message["raw"])
                email_msg = message_from_bytes(raw_msg)
                
                # Get metadata for headers
                metadata = service.users().messages().get(
                    userId="me",
                    id=gmail_msg_id,
                    format="metadata",
                    metadataHeaders=["From", "Subject", "Date", "Message-ID", "In-Reply-To", "References"]
                ).execute()
                
                msg_headers = {}
                for h in metadata.get("payload", {}).get("headers", []):
                    msg_headers[h["name"]] = h["value"]
                
                content = decode_email_payload(email_msg)
                from_address = msg_headers.get("From", "").lower()
                is_from_bot = HUBSPOT_FROM_ADDRESS.lower() in from_address
                timestamp = int(metadata.get("internalDate", 0))

                processed_thread.append({
                    "headers": msg_headers,
                    "content": content.strip(),
                    "timestamp": timestamp,
                    "from_bot": is_from_bot,
                    "message_id": gmail_msg_id,
                    "role": "assistant" if is_from_bot else "user"
                })
                
                logging.info(f"✓ Retrieved message {len(processed_thread)}/{len(message_ids)}: {msg_id[:30]}...")
                
            except Exception as e:
                logging.error(f"Error fetching message {msg_id}: {e}")
                continue

        # Sort by timestamp
        processed_thread.sort(key=lambda x: x.get("timestamp", 0))
        
        logging.info(f"✓ Processed thread {thread_id} with {len(processed_thread)} messages")
        for idx, email in enumerate(processed_thread, 1):
            role = email['role']
            from_email = email['headers'].get('From', 'Unknown')[:40]
            preview = email['content'][:60].replace('\n', ' ')
            timestamp = email['timestamp']
            logging.info(f"  [{idx}] {role} @ {timestamp}: from={from_email}, preview={preview}...")

        return processed_thread

    except Exception as e:
        logging.error(f"Error retrieving thread: {e}", exc_info=True)
        return []

def format_chat_history(thread_history):
    """
    Convert thread history to chat history format compatible with the agent.
    Format matches agent.py structure with HumanMessage and AIMessage.
    
    Args:
        thread_history: List of email messages with role, content, etc.
    
    Returns:
        List of formatted messages for agent consumption
    """
    chat_history = []
    
    for msg in thread_history[:-1]:  # Exclude the latest message (will be sent as current prompt)
        # Create message in format similar to agent.py
        message = {
            "role": msg["role"],  # "user" or "assistant"
            "content": msg["content"]
        }
        chat_history.append(message)
    
    logging.info(f"Formatted chat history with {len(chat_history)} messages")
    return chat_history


def extract_all_recipients(email_data):
    """Extract all recipients (To, Cc, Bcc) from email headers."""
    headers = email_data.get("headers", {})
    
    def parse_addresses(header_value):
        """Parse email addresses from header string"""
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

def fetch_unread_emails(**kwargs):
    """Fetch unread emails and extract full thread history for each."""
    service = authenticate_gmail()
    if not service:
        logging.error("Gmail authentication failed, skipping email fetch.")
        kwargs['ti'].xcom_push(key="unread_emails", value=[])
        return []
    
    last_checked_timestamp = get_last_checked_timestamp()
    last_checked_seconds = last_checked_timestamp // 1000 if last_checked_timestamp > 1000000000000 else last_checked_timestamp
    
    query = f"is:unread after:{last_checked_seconds}"
    logging.info(f"Fetching emails with query: {query}")
    
    try:
        results = service.users().messages().list(userId="me", labelIds=["INBOX"], q=query).execute()
        messages = results.get("messages", [])
    except Exception as e:
        logging.error(f"Error fetching messages: {e}")
        kwargs['ti'].xcom_push(key="unread_emails", value=[])
        return []
    
    unread_emails = []
    max_timestamp = last_checked_timestamp
    processed_message_ids = set()
    
    logging.info(f"Found {len(messages)} unread messages to process")
    
    for msg in messages:
        msg_id = msg["id"]
        
        if msg_id in processed_message_ids:
            logging.info(f"Skipping already processed message: {msg_id}")
            continue
            
        try:
            # ⭐ CHANGED: Fetch FULL metadata including To, Cc, Bcc headers
            msg_data = service.users().messages().get(
                userId="me", 
                id=msg_id, 
                format="metadata",
                metadataHeaders=["From", "To", "Cc", "Bcc", "Subject", "Date", 
                               "Message-ID", "References", "In-Reply-To"]
            ).execute()
        except Exception as e:
            logging.error(f"Error fetching message {msg_id}: {e}")
            continue
            
        headers = {h["name"]: h["value"] for h in msg_data["payload"]["headers"]}
        sender = headers.get("From", "")
        
        # Validate sender is whitelisted
        if not is_email_whitelisted(sender):
            logging.warning(f"Unauthorized email from {sender} - marking as read and skipping")
            mark_message_as_read(service, msg_id)
            continue
        
        sender = sender.lower()
        timestamp = int(msg_data["internalDate"])
        thread_id = msg_data.get("threadId", "")
        
        logging.info(f"Processing message ID: {msg_id}, From: {sender}, Timestamp: {timestamp}, Thread ID: {thread_id}")
        
        # Skip old messages
        if timestamp <= last_checked_timestamp:
            logging.info(f"Skipping old message {msg_id} - timestamp {timestamp} <= {last_checked_timestamp}")
            continue
        
        # Skip no-reply emails
        if "no-reply" in sender or "noreply" in sender:
            logging.info(f"Skipping no-reply email from: {sender}")
            continue
        
        # Skip emails from bot itself
        if sender == HUBSPOT_FROM_ADDRESS.lower():
            logging.info(f"Skipping email from bot: {sender}")
            continue
        logging.info(f"Processing message ID: {msg_id}, From: {sender}, Timestamp: {timestamp}, Thread ID: {thread_id}")
        
        
        # ✅ Construct email_object with all headers INCLUDING recipient headers
        email_object = {
            "id": msg_id,
            "threadId": thread_id,
            "headers": headers,  # Now includes To, Cc, Bcc
            "content": "",  # Will be filled by get_email_thread
            "timestamp": timestamp,
            "internalDate": timestamp
        }
        
        # ⭐ Extract all recipients from this email
        all_recipients = extract_all_recipients(email_object)
        
        logging.info(f"Extracted recipients - To: {all_recipients.get('to', [])}, "
                    f"Cc: {all_recipients.get('cc', [])}, Bcc: {all_recipients.get('bcc', [])}")
        
        # Get thread history
        thread_history = get_email_thread(service, email_object)
        
        if not thread_history:
            logging.error(f"Failed to retrieve thread history for message {msg_id}")
            continue
        
        # Format chat history (all messages except the latest)
        chat_history = format_chat_history(thread_history)
        
        # Extract latest message content (current user prompt)
        latest_message = thread_history[-1]
        latest_message_content = extract_latest_reply(latest_message["content"])
        
        # Update email_object with the latest message content
        email_object["content"] = latest_message_content
        
        # Determine if this is a reply based on thread length and email headers
        is_reply = len(thread_history) > 1
        subject = headers.get("Subject", "")
        is_reply = is_reply or subject.lower().startswith("re:") or bool(headers.get("In-Reply-To")) or bool(headers.get("References"))
        
        # ✅ Add all required fields to email_object including recipients
        email_object.update({
            "is_reply": is_reply,
            "thread_history": thread_history,
            "chat_history": chat_history,
            "thread_length": len(thread_history),
            "all_recipients": all_recipients  # ⭐ Store recipients in email object
        })
        
        logging.info(f"Email {msg_id}: is_reply={is_reply}, thread_length={len(thread_history)}, "
                    f"chat_history_length={len(chat_history)}, recipients={len(all_recipients.get('to', [])) + len(all_recipients.get('cc', [])) + len(all_recipients.get('bcc', []))}")
        
        unread_emails.append(email_object)
        processed_message_ids.add(msg_id)
        
        if timestamp > max_timestamp:
            max_timestamp = timestamp
            
    # Update timestamp
    if messages:
        update_timestamp = max_timestamp + 1
        update_last_checked_timestamp(update_timestamp)
        logging.info(f"Updated last checked timestamp to: {update_timestamp}")
    else:
        logging.info("No messages found, timestamp unchanged")
        
    kwargs['ti'].xcom_push(key="unread_emails", value=unread_emails)
    logging.info(f"Processed {len(unread_emails)} emails ({sum(1 for e in unread_emails if e['is_reply'])} replies)")
    
    return unread_emails

def mark_message_as_read(service, message_id):
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
                messages.append({"role": "user", "content": item["prompt"]})
                messages.append({"role": "assistant", "content": item["response"]})
        messages.append({"role": "user", "content": prompt})
        response = client.chat(model='hubspot:v6af', messages=messages, stream=False)
        ai_content = response.message.content

        ai_content = re.sub(r'```(?:html|json)\n?|```', '', ai_content)

        if not expect_json and not ai_content.strip().startswith('<!DOCTYPE') and not ai_content.strip().startswith('<html') and not ai_content.strip().startswith('{'):
            ai_content = f"<html><body>{ai_content}</body></html>"

        return ai_content.strip()
    except Exception as e:
        logging.error(f"Error in get_ai_response: {e}")
        if expect_json:
            return f'{{"error": "Error processing AI request: {str(e)}"}}'
        else:
            return f"<html><body>Error processing AI request: {str(e)}</body></html>"

def search_hubspot_entities(entity_type, filters=None, properties=None, limit=100, sort=None, max_results=None):
    """
    Search HubSpot CRM entities with automatic pagination support.
    
    Args:
        entity_type (str): Type of entity - 'contacts', 'companies', or 'deals'
        filters (list): List of filter groups for search
        properties (list): List of properties to return
        limit (int): Results per page (default: 100, max: 200)
        sort (dict): Sorting configuration
        max_results (int): Maximum total results to fetch (None = fetch all)
    
    Returns:
        dict: Search results with 'results' containing list of all entities
    """
    try:
        endpoint = f"{HUBSPOT_BASE_URL}/crm/v3/objects/{entity_type}/search"
        
        headers = {
            "Authorization": f"Bearer {HUBSPOT_API_KEY}",
            "Content-Type": "application/json"
        }
        
        # Build base search payload
        payload = {
            "limit": min(limit, 200),
            "properties": properties or []
        }
        
        if filters:
            payload["filterGroups"] = [{"filters": filters}]
        
        if sort:
            payload["sorts"] = [sort]
        
        all_results = []
        after = None
        page = 1
        
        while True:
            # Add pagination parameter if available
            if after:
                payload["after"] = after
            
            logging.info(f"Fetching page {page} for {entity_type} (after={after})")
            
            # Make API request
            response = requests.post(endpoint, headers=headers, json=payload, timeout=30)
            response.raise_for_status()
            
            results = response.json()
            current_batch = results.get('results', [])
            all_results.extend(current_batch)
            
            logging.info(f"Page {page}: Retrieved {len(current_batch)} {entity_type} (Total: {len(all_results)})")
            
            # Check if we've reached max_results limit
            if max_results and len(all_results) >= max_results:
                all_results = all_results[:max_results]
                logging.info(f"Reached max_results limit of {max_results}")
                break
            
            # Check for pagination
            paging = results.get('paging', {})
            next_page = paging.get('next', {})
            after = next_page.get('after')
            
            # Break if no more pages
            if not after:
                logging.info(f"No more pages. Total {entity_type} fetched: {len(all_results)}")
                break
            
            page += 1
        
        return {"results": all_results, "total": len(all_results)}
        
    except requests.exceptions.RequestException as e:
        logging.error(f"HubSpot API error for {entity_type}: {str(e)}")
        if hasattr(e, 'response') and e.response is not None:
            logging.error(f"Response content: {e.response.text}")
        return {"results": [], "error": str(e)}
    except Exception as e:
        logging.error(f"Unexpected error searching {entity_type}: {str(e)}")
        return {"results": [], "error": str(e)}


def search_contacts(filters=None, properties=None, limit=200, max_results=None):
    """
    Search HubSpot contacts with pagination support.
    
    Args:
        filters (list): Filter criteria
        properties (list): Properties to return
        limit (int): Results per page (max 200)
        max_results (int): Maximum total results (None = fetch all)
    
    Returns:
        dict: Search results with all pages combined
    """
    default_properties = [
        'firstname', 'lastname', 'email', 'phone', 
        'jobtitle', 'address', 'city', 'state', 'zip', 'country',
        'createdate', 'lastmodifieddate', 'hs_object_id', 'hubspot_owner_id'
    ]
    
    return search_hubspot_entities(
        entity_type='contacts',
        filters=filters,
        properties=properties or default_properties,
        limit=limit,
        max_results=max_results
    )


def search_companies(filters=None, properties=None, limit=200, max_results=None):
    """
    Search HubSpot companies with pagination support.
    
    Args:
        filters (list): Filter criteria
        properties (list): Properties to return
        limit (int): Results per page (max 200)
        max_results (int): Maximum total results (None = fetch all)
    
    Returns:
        dict: Search results with all pages combined
    """
    default_properties = [
        'name', 'domain', 'phone',
        'address', 'city', 'state', 'zip', 'country',
        'industry', 'type', 'hs_object_id'
    ]
    
    return search_hubspot_entities(
        entity_type='companies',
        filters=filters,
        properties=properties or default_properties,
        limit=limit,
        max_results=max_results
    )


def search_deals(filters=None, properties=None, limit=200, max_results=None):
    """
    Search HubSpot deals with pagination support.
    
    Args:
        filters (list): Filter criteria
        properties (list): Properties to return
        limit (int): Results per page (max 200)
        max_results (int): Maximum total results (None = fetch all)
    
    Returns:
        dict: Search results with all pages combined
    """
    default_properties = [
        'dealname', 'amount', 'dealstage', 'pipeline', 'closedate',
        'createdate', 'hubspot_owner_id', 'hs_object_id'
    ]
    final_properties = list(set(default_properties + (properties or [])))
    
    return search_hubspot_entities(
        entity_type='deals',
        filters=filters,
        properties=final_properties,
        limit=limit,
        max_results=max_results
    )

def export_to_file(data, export_format='excel', filename=None, export_dir='/appz/cache/exports', hyperlinks=None):
    try:
        os.makedirs(export_dir, exist_ok=True)
        if filename is None:
            filename = f"export_{uuid.uuid4()}"
        filename = os.path.splitext(filename)[0]

        filepath = os.path.join(export_dir, f"{filename}.xlsx")

        if isinstance(data, dict):
            data = list(data.values())[0] if len(data) == 1 else data

        if isinstance(data, (list, dict)):
            df = pd.DataFrame(data)
        elif isinstance(data, pd.DataFrame):
            df = data.copy()
        else:
            raise ValueError("Unsupported data type")

        # CRITICAL FIX: Sanitize all string columns to remove illegal Excel characters
        def sanitize_excel_value(val):
            """Remove illegal characters that openpyxl cannot write to Excel."""
            if pd.isna(val):
                return val
            if not isinstance(val, str):
                return val
            
            # Remove illegal XML characters (control characters except tab, newline, carriage return)
            # Excel XML spec doesn't allow: 0x00-0x08, 0x0B-0x0C, 0x0E-0x1F, 0x7F-0x9F
            illegal_chars = re.compile(
                r'[\x00-\x08\x0B-\x0C\x0E-\x1F\x7F-\x9F]'
            )
            sanitized = illegal_chars.sub('', val)
            
            # Also replace other problematic characters with safe alternatives
            sanitized = sanitized.replace('\x0B', ' ')  # Vertical tab
            sanitized = sanitized.replace('\x0C', ' ')  # Form feed
            
            return sanitized

        # Apply sanitization to all columns
        for col in df.columns:
            if df[col].dtype == 'object':  # String columns
                df[col] = df[col].apply(sanitize_excel_value)

        # Keep helper columns for hyperlinks
        df_with_helpers = df.copy()

        # Define which columns to display (exclude helper columns)
        display_columns = [col for col in df.columns if not col.startswith('_')]
        df_display = df[display_columns]

        # Write to Excel
        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            df_display.to_excel(writer, sheet_name='Sheet1', index=False)
            worksheet = writer.sheets['Sheet1']

            # Apply hyperlinks if specified
            if hyperlinks and 'Sheet1' in hyperlinks:
                link_config = hyperlinks['Sheet1']
                for display_col_name, config in link_config.items():
                    if display_col_name not in df_display.columns:
                        logging.warning(f"Column {display_col_name} not found for hyperlink")
                        continue

                    url_col = config.get('url_column', '_hubspot_url')
                    if url_col not in df_with_helpers.columns:
                        logging.warning(f"URL column {url_col} not found")
                        continue

                    col_idx = df_display.columns.get_loc(display_col_name) + 1  # +1 for Excel
                    col_letter = get_column_letter(col_idx)

                    for row_idx in range(len(df_with_helpers)):
                        url = df_with_helpers.iloc[row_idx][url_col]
                        if pd.notna(url) and str(url).strip():
                            cell = worksheet[f"{col_letter}{row_idx + 2}"]  # +2: header + 0-index
                            cell.hyperlink = str(url)
                            cell.font = Font(color="0563C1", underline="single")
                            cell.style = "Hyperlink"

        logging.info(f"Exported with working hyperlinks: {filepath}")
        return {
            "success": True,
            "filepath": filepath,
            "filename": f"{filename}.xlsx",
            "format": "excel"
        }
    except Exception as e:
        logging.error(f"Export failed: {e}", exc_info=True)
        return {"success": False, "error": str(e)}

def branch_function(**kwargs):
    ti = kwargs['ti']
    unread_emails = ti.xcom_pull(task_ids="fetch_unread_emails", key="unread_emails")

    if not unread_emails:
        logging.info("No unread emails found, proceeding to no_email_found_task.")
        return "no_email_found_task"

    # Build email details for AI analysis with FULL chat history context
    email_details = []
    for idx, email in enumerate(unread_emails, 1):
        headers = email.get("headers", {})
        
        email_info = {
            "email_number": idx,
            "from": headers.get("From", "Unknown"),
            "subject": headers.get("Subject", "No Subject"),
            "latest_message": email.get("content", ""),
            "is_reply": email.get("is_reply", False)
        }
        email_details.append(email_info)
        logging.info(f"email details is: {email_details}")
    
    # Enhanced routing prompt with REPORT INTENT capability
    prompt = f"""You are an AI email router that determines which workflow to execute based on user messages.

ANALYZE THE LATEST MESSAGE AND ROUTE APPROPRIATELY:

EMAILS TO CLASSIFY:
{email_details}

Important Instructions:
    - You are not capable of calling any APIs or tools.
    - You should only answer based on your knowledge and the provided email details.
    - Hubspot functions include: searching and creating contacts, companies, deals, meetings, tasks, logging notes, and summarizing engagements, generating reports, casual comments based on context etc.

SEARCH_DAG CAPABILITIES:
- Searches for existing contacts, companies, deals in HubSpot only if followup entities like notes meetings, tasks or another contact or company or deal needs to be created based on user request.
- Extracts entity information from conversations
- Determines what needs to be created vs what exists
- Generates engagement summaries for meetings
- Prepares confirmation emails for user review
- In context if the user has already created some entities and want to add more entities like additional contact or new deal then a search is needed to check if the entity exists before creating new ones.
- Search for all contacts mentioned by user in the prompt even if there are multiple contacts.If the contact doesnt exist return the response as objects to be created and if there is an existing contact return the existing contact details in the response.
- Parse the email context and check wether the user is selecting entities based on the confirmation email sent, if yes then ignore those, you dont have the capability in such scenario.
CONTINUATION_DAG CAPABILITIES:
- Creates new contacts, companies, deals in HubSpot
- Updates existing entities based on user modifications. for example, if the deal exists and we need to change the deal amount, or we need to change the task due date to a different date. These are taken as modification.
- Logs meeting notes and minutes
- Creates tasks with owners and due dates
- Records engagements and associations
- Handles user confirmations and modifications
- Parses the latest_message and if it is casual comment for the conversation history add it as notes.
2. Parse the context other than latest_message and if a confirmation email has already been sent in this thread (you can see it in the chat history), then:
   - Any user reply that includes modifications (like changing owner, adding contact, updating amount, etc.) with respect to confirmation mail entities should be treated as implicit confirmation to proceed with those changes immediately. Do NOT wait for explicit confirmation keywords like "yes", "proceed", or "confirm". Just apply the changes and send the final updated email.

REPORT_DAG CAPABILITIES:
- Only when the user explictly requests to generate report.

NO_ACTION CAPABILITIES:
- Recognizes greetings, closings, and simple acknowledgments
- Outputs friendly responses without further action
- Handles questions about bot capabilities or general chat
- Does not perform any HubSpot operations. Only answer the hubspot queries.
- Ignore casual comments about hubspot context.
- Handles blank emails without content or context.
- Handles any direct queries including hubspot. For example IS there a deal called X in hubspot? or what is the status of deal Y in hubspot? These do not require any action, just a friendly response.
- Hubspot tasks due today.

ROUTING DECISION TREE:

1. **NO ACTION NEEDED** (Return: no_action)
   - Greetings: "hi", "hello", "good morning", "hey there"
   - Closings: "thanks", "thank you", "goodbye", "bye", "have a good day"
   - Simple acknowledgments: "ok", "got it", "understood", "sounds good"
   - Questions about bot capabilities or general chat
   - If the email lacks content or context.
   - Information retrieving questions from hubspot database.
   - Information out of hubspot.
   → Response: {{"task_type": "no_action", "message": "friendly_response"}}

2. **GENERATE REPORT** (Route to: report_dag)
   When user explicitly requests:
   - Reports from HubSpot data
   
   Keywords: "report", "generate report"
   Examples:
   - "Generate a report of all deals closed this quarter"
   - "Create a sales pipeline report"
   
   → Response: {{"task_type": "report_dag", "reasoning": "..."}}

3. **SEARCH & ANALYZE** (Route to: search_dag)
   When user needs to:
   - Search for existing contacts, companies, or deals only if followup entities like notes meetings, tasks or another contact or company or deal needs to be created based on user request.
   - Create NEW entities (deals, contacts, companies, meetings, tasks)
   - Log meeting minutes or notes from discussions
   - Request summaries of clients/deals before meetings
   - Any FIRST message in a new conversation thread other than greetings or general chats.
   
   Keywords: "create", "add", "new", "log meeting", "find", "search", "summarize", "what do we know about"
   → Response: {{"task_type": "search_dag", "reasoning": "..."}}

4. **CONFIRM & EXECUTE** (Route to: continuation_dag)
   When user is:
   - Responding to bot's confirmation request ("proceed", "yes", "confirm", "looks good")
   - Making corrections to bot's proposed actions
   - Adding casual comments about existing deals/clients (no new entities) or between the conversation.
   - Updating existing records without creating new ones
   - If the creation of new entities is mentioned instead of proceed after the confirmation mail, then treat it as direct creation without search.
   
   Keywords: "proceed", "confirm", "yes", "update", "modify", "change"
   → Response: {{"task_type": "continuation_dag", "reasoning": "..."}}
   - Do not rely solely on **keywords**; interpret user intent in context and act accordingly.
   - After the initial confirmation email is sent by the agent, any user response that **does not include the specified keywords** consider that as **implicit confirmation**, and the agent must send the final updated email **without waiting for any further explicit approval**.

DECISION LOGIC:
- Check if message requires ANY action (if not → no_action)
- For action requests: Is this explicitly asking for a REPORT? → report_dag
- For action requests: Is this creating/searching NEW entities? → search_dag
- For action requests: Is this confirming/modifying bot's proposal? → continuation_dag
- When unclear: Default to search_dag for safety

Return ONLY valid JSON:
{{"task_type": "no_action|report_dag|search_dag|continuation_dag", "reasoning": "brief explanation"}}
"""
    
    logging.info(f"Sending routing prompt to AI with {len(email_details)} emails and conversation context")
    
    conversation_history_for_ai = []
    for email in unread_emails:
        chat_history = email.get("chat_history", [])
        for i in range(0, len(chat_history), 2):
            if i + 1 < len(chat_history):
                user_msg = chat_history[i]
                assistant_msg = chat_history[i + 1]
                if user_msg["role"] == "user" and assistant_msg["role"] == "assistant":
                    conversation_history_for_ai.append({
                        "prompt": user_msg["content"],
                        "response": assistant_msg["content"]
                    })

    response = get_ai_response(
        prompt=prompt,
        conversation_history=conversation_history_for_ai,
        expect_json=True
    )
    logging.info(f"AI routing response: {response}")   
    # Parse JSON response
    try:
        json_response = json.loads(response)
    except json.JSONDecodeError:
        json_response = extract_json_from_text(response)

    # Route based on AI decision
    if json_response and "task_type" in json_response:
        task_type = json_response["task_type"].lower()
        reasoning = json_response.get("reasoning", "No reasoning provided")
        logging.info(f"✓ AI DECISION: {task_type}, REASONING: {reasoning}")
        
        if "report" in task_type:
            ti.xcom_push(key="report_emails", value=unread_emails)
            logging.info(f"→ Routing {len(unread_emails)} emails to report_dag")
            return "trigger_report_dag"
            
        elif "continuation" in task_type:
            ti.xcom_push(key="reply_emails", value=unread_emails)
            logging.info(f"→ Routing {len(unread_emails)} emails to continuation_dag")
            return "trigger_continuation_dag"
            
        elif "search" in task_type:
            ti.xcom_push(key="new_emails", value=unread_emails)
            logging.info(f"→ Routing {len(unread_emails)} emails to search_dag")
            return "trigger_meeting_minutes"
        
        elif "no_action" in task_type:
            ti.xcom_push(key="no_action_emails", value=unread_emails)
            logging.info("→ No action needed for the emails")
            return "handle_general_queries"


def extract_latest_reply(email_content):
    """
    Extract only the latest reply from an email, removing quoted conversation history.
    
    Args:
        email_content: Full email body text
    
    Returns:
        String containing only the latest message content
    """
    if not email_content:
        return ""
    
    # Split by common reply separators
    separators = [
        '\r\n\r\nOn ',  # Gmail style: "On Wed, Oct 22, 2025..."
        '\n\nOn ',
        '\r\n\r\n>',  # Quoted text starting with >
        '\n\n>',
        '\r\n\r\nFrom:',  # Outlook style
        '\n\nFrom:',
        '________________________________',  # Outlook horizontal line
    ]
    
    latest_content = email_content
    earliest_position = len(email_content)
    
    # Find the earliest occurrence of any separator
    for separator in separators:
        pos = email_content.find(separator)
        if pos != -1 and pos < earliest_position:
            earliest_position = pos
            latest_content = email_content[:pos]
    
    # Clean up the extracted content
    latest_content = latest_content.strip()
    
    # Remove leading ">" quoted lines if any remain
    lines = latest_content.split('\n')
    clean_lines = []
    for line in lines:
        stripped = line.lstrip()
        if not stripped.startswith('>'):
            clean_lines.append(line)
        else:
            break  # Stop at first quoted line
    
    return '\n'.join(clean_lines).strip()
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

def trigger_meeting_minutes(**kwargs):
    ti = kwargs['ti']
    new_emails = ti.xcom_pull(task_ids="branch_task", key="new_emails") or []
    
    if not new_emails:
        logging.info("No new emails to process")
        return
    
    for email in new_emails:
        # Pass email with chat_history to search DAG
        trigger_conf = {
            "email_data": email,
            "chat_history": email.get("chat_history", []),
            "thread_history": email.get("thread_history", []),
            "thread_id": email.get("threadId", ""),  # ADD THIS
            "message_id": email.get("id", "")
        }
        
        task_id = f"trigger_search_{email['id'].replace('-', '_')}"
        trigger_task = TriggerDagRunOperator(
            task_id=task_id,
            trigger_dag_id="hubspot_search_entities",
            conf=trigger_conf,
        )
        trigger_task.execute(context=kwargs)
    
    logging.info(f"Triggered search DAG for {len(new_emails)} emails")

def trigger_continuation_dag(**kwargs):
    ti = kwargs['ti']
    reply_emails = ti.xcom_pull(task_ids="branch_task", key="reply_emails") or []
    
    if not reply_emails:
        logging.info("No reply emails to process")
        return

    for email in reply_emails:
        # Pass email with full chat_history to continuation DAG
        trigger_conf = {
            "email_data": email,
            "chat_history": email.get("chat_history", []),
            "thread_history": email.get("thread_history", []),
            "thread_id": email.get("threadId"),
            "thread_id": email.get("threadId", ""),  # ADD THIS
            "message_id": email.get("id", "")
        }
        
        task_id = f"trigger_continuation_{email['id'].replace('-', '_')}"
        trigger_task = TriggerDagRunOperator(
            task_id=task_id,
            trigger_dag_id="hubspot_create_objects",
            conf=trigger_conf,
        )
        trigger_task.execute(context=kwargs)
        
        logging.info(f"✓ Triggered continuation DAG for thread {email.get('threadId')}")

    logging.info(f"Triggered continuation for {len(reply_emails)} emails")

def handle_general_queries(**kwargs):
    """Send a friendly AI response if possible.
    If AI fails → send a polite 'technical issue' apology."""
    
    ti = kwargs['ti']
    unread_emails = ti.xcom_pull(task_ids="branch_task", key="no_action_emails") or []
    
    if not unread_emails:
        logging.info("No emails requiring friendly response")
        return
    
    service = authenticate_gmail()
    if not service:
        logging.error("Gmail authentication failed, cannot send responses")
        return
    
    # Define signature once
    EMAIL_SIGNATURE = "HubSpot via lowtouch.ai Team"
    AGENT_NAME = "HubSpot Assistant"

    for email in unread_emails:
        try:
            headers = email.get("headers", {})
            sender_email = headers.get("From", "")
            email_id = email.get("id", "unknown")
            
            # Extract sender name
            sender_name = "there"
            name_match = re.search(r'^([^<]+)', sender_email)
            if name_match:
                sender_name = name_match.group(1).strip()

            # === STEP 1: Try AI-generated response ===
            ai_response = None
            try:
                prompt = f"""You are a friendly HubSpot email assistant. You can answer generic questions about hubspot also causal greetings and Thanking mails.
User message: "{email.get("content", "").strip()}"

Reply in 1-2 short, polite, professional sentences.
- If greeting: acknowledge warmly.
- If out of context: say you're HubSpot-focused.
- If no content: ask for clarification.
- If user asks for HubSpot info: provide concise, accurate answers.
- Hubspot query rules:
    - If user asks about any contact or contact details call the tool `search_contacts` and answer the question based on the output. The output should be in HTML - table Format .
      * Ensure that the output strictly returns all of the following fields: Contact ID,Firstname,Lastname,Email,Phone,Job Title,Contact Owner Name,Last Modified Date in table format.
      * When only first name or last name is given,search based on that and if multiple contacts are found ask for clarification to choose a specific one after returning the table of contacts found.
      * Keep each phone number, email, and job title in a single line (no text wrapping across lines).
      * Ensure phone numbers follow a uniform formatting standard (e.g., +91 98765 43210 or +1 312 555 7241).
      * Avoid unnecessary line breaks in any field.
      * Do not follow the pagination rule in email response.If there are 100 matches of a contact,return all 100 in the email itself. 
    - If user asks about any company or company details call the tool `search_companies` and answer the question based on the output. The output should be in HTML - table Format .
      * Ensure that the output strictly returns all of the following fields: Company Id, Company Name,Domain,Company Owner,Lifecycle Stage,Associated Deals (IDs + names + stages) in table format.
      * Do not follow the pagination rule in email response.If there are 100 matches of a company,return all 100 in the email itself.
    - If user asks about any deal or deal details call the tool `search_deals` and answer the question based on the output. The output should be in HTML - table Format .
      * Ensure that the output strictly returns all of the following fields: Deal ID, Deal Name, Deal Stage(Dont take the deal stage id,take the deal stage name(for example if deal stage id is appoinmentschedule,then the deal stage will be APPOINTMENT SCHEDULE)), Deal Owner, Deal Amount, Expected Close Date, Associated Company, and Associated Contacts in table format.
      * Do not merge, or concatenate the stage name — preserve all spaces, casing, and formatting.
      * Treat current system date as **NOW**.
      * Exclude all deals whose Expected Close Date is prior to NOW.
      * The result set must be sorted on Expected Close Date in ascending order, prioritizing deals with the earliest closing dates
      * If the user does not specify a time period → return all deals with close date today or later.
      * If a date range or timeframe is mentioned in the query:
      * Convert natural language into a valid start_date → end_date range.
      * Return only deals whose Expected Close Date lies within that period.
      * Include today's date when filtering.(Example if the  Query is to check the "deals expiring by this month end" then date Range will be 1 Dec 2025 to 31 Dec 2025)
      * When the user asks for deals expiring by this month end, ALWAYS apply the date filter as follows:
        - gte must be strictly set to today's date (current system date)
        - lte must be strictly set to the last date of the current month
        - You must NEVER include or return any deals with dates earlier than today.
        - Even if the user does not explicitly mention 'from today', you must assume that the date range ALWAYS starts from today.
        - Do NOT include any past dates under any circumstances.(Example: If today is 03-Dec-2025, then the filter MUST be:gte = 03-Dec-2025,lte = 31-Dec-2025 and the response MUST only include deals whose expiry date lies within this range)
      * Do not follow the pagination rule in email response.If there are 100 matches for deals,return all 100 in the email itself. 
    - If user asking a entity detail along with a timeperiod use LTE, GTE or both based on user request. The output should be in HTML - table Format.
    - If the user asks about their tasks parse the senders name and invoke `get_all_owners` to get the hubspot owner id and then invoke `search_tasks` to get the tasks assigned to the owner on the sepcified date. The output should be in HTML - table Format.
      * Ensure that the output strictly returns all of the following fields:Task_ID,Task Subject,Due Date,Status,Priority.
      * Do not follow the pagination rule in email response.If there are 100 matches for tasks,return all 100 in the email itself without any followp response.
- All the **dates** should be in YYYY-MM-DD format and do not include time.
- If a column has no data for a particular record, give the value for that as **N/A**.Do not leave any coloumn blank.
- Always maintain a friendly and professional tone.
Your final response must be in below format:
```
        <html>
        <head>
            <style>
        table {{width: 100%;border-collapse: collapse;margin: 20px 0;background: #ffffff;border: 1px solid #e0e0e0;font-size: 14px;}}
        th {{background-color: #f3f6fc;color: #333;padding: 10px;border: 1px solid #d0d7e2;text-align: left;font-weight: bold;white-space: nowrap;}}
        td {{padding: 10px;border: 1px solid #e0e0e0;text-align: left;white-space: nowrap;}}
        h3 {{color: #333;margin-top: 30px;margin-bottom: 15px;}}

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
                <p>"Here your resposne should be replaced."</p>
            </div>
            
            <div class="closing">
                <p>If you need additional information, please don't hesitate to ask.</p>
            </div>
            
            <div class="signature">
                <p>Best regards,<br>
                The HubSpot Assistant Team<br>
                <a href="http://lowtouch.ai" class="company">Lowtouch.ai</a></p>
            </div>
        </body>
        </html>```
"""

                ai_response = get_ai_response(prompt=prompt, expect_json=False)
                logging.info(f"AI generated response before cleaning: {ai_response}")   
                match = re.search(r'```html.*?\n(.*?)```', ai_response, re.DOTALL)
                if match:
                    ai_response = match.group(1).strip()
                else:
                    # No HTML code block found, use response directly
                    ai_response = ai_response.strip()
                logging.info(f"AI generated response :{ai_response}")
                if not ai_response or len(ai_response) < 5 or "error" in ai_response.lower():
                    raise ValueError("Invalid AI response")

            except Exception as ai_error:
                logging.warning(f"AI failed for {email_id}: {ai_error} → using technical fallback")
                ai_response = None  # Force fallback

            # === STEP 2: Decide final response ===
            if ai_response:
                final_response = ai_response
                log_prefix = "AI"
            else:
                final_response = f"""
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
                log_prefix = "Fallback"

            # === STEP 3: Build and Send Email ===
            try:
                logging.info(f"{log_prefix} response → sending to {sender_email}")

                # Threading
                original_message_id = headers.get("Message-ID", "")
                references = headers.get("References", "")
                if original_message_id:
                    references = f"{references} {original_message_id}".strip() if references else original_message_id

                subject = headers.get("Subject", "No Subject")
                if not subject.lower().startswith("re:"):
                    subject = f"Re: {subject}"

                # Recipients
                all_recipients = extract_all_recipients(email)
                primary_recipient = sender_email

                cc_recipients = [
                    addr for addr in all_recipients["to"] + all_recipients["cc"]
                    if addr.lower() != sender_email.lower()
                    and HUBSPOT_FROM_ADDRESS.lower() not in addr.lower()
                ]
                bcc_recipients = [
                    addr for addr in all_recipients["bcc"]
                    if HUBSPOT_FROM_ADDRESS.lower() not in addr.lower()
                ]

                cc_string = ', '.join(cc_recipients) if cc_recipients else None
                bcc_string = ', '.join(bcc_recipients) if bcc_recipients else None

                # Compose
                msg = MIMEMultipart()
                msg["From"] = f"HubSpot via lowtouch.ai <{HUBSPOT_FROM_ADDRESS}>"
                msg["To"] = primary_recipient
                if cc_string: msg["Cc"] = cc_string
                if bcc_string: msg["Bcc"] = bcc_string
                msg["Subject"] = subject
                if original_message_id: msg["In-Reply-To"] = original_message_id
                if references: msg["References"] = references
                msg.attach(MIMEText(final_response, "html"))

                # Send
                raw_msg = base64.urlsafe_b64encode(msg.as_string().encode("utf-8")).decode("utf-8")
                result = service.users().messages().send(
                    userId="me",
                    body={"raw": raw_msg}
                ).execute()

                mark_message_as_read(service, email_id)
                logging.info(f"Sent {log_prefix.lower()} response for email {email_id}")

            except Exception as send_error:
                logging.error(f"Failed to send email for {email_id}: {send_error}", exc_info=True)
                mark_message_as_read(service, email_id)

        except Exception as e:
            logging.error(f"Unexpected error for email {email.get('id', 'unknown')}: {e}", exc_info=True)
            try:
                mark_message_as_read(service, email["id"])
            except:
                pass
            continue

def get_deal_stage_labels():
    """
    Fetch all pipelines and stages from HubSpot and return a mapping:
    { 'appointmentscheduled': 'Appointment Scheduled', ... }
    """
    try:
        endpoint = f"{HUBSPOT_BASE_URL}/crm/v3/pipelines/deals"
        headers = {
            "Authorization": f"Bearer {HUBSPOT_API_KEY}",
            "Content-Type": "application/json"
        }
        
        response = requests.get(endpoint, headers=headers, timeout=30)
        response.raise_for_status()
        pipelines_data = response.json().get("results", [])

        stage_mapping = {}
        for pipeline in pipelines_data:
            pipeline_label = pipeline.get("label", "")
            stages = pipeline.get("stages", [])
            for stage in stages:
                stage_id = stage.get("id")
                stage_label = stage.get("label")
                if stage_id and stage_label:
                    stage_mapping[stage_id] = stage_label
                    # Optional: include pipeline name for context
                    # stage_mapping[stage_id] = f"{stage_label} ({pipeline_label})"

        logging.info(f"Loaded {len(stage_mapping)} deal stage labels from HubSpot")
        return stage_mapping

    except Exception as e:
        logging.error(f"Failed to fetch deal stage labels: {e}")
        return {}

def archive_report(report_data):
    """
    Archive report payload to disk for audit trail.
    
    Args:
        report_data (dict): Complete report payload matching schema
        
    Returns:
        dict: Archive result with success status and filepath
    """
    try:
        # Create archive directory if it doesn't exist
        os.makedirs(REPORT_ARCHIVE_DIR, exist_ok=True)
        
        report_id = report_data.get("report_id")
        if not report_id:
            raise ValueError("report_id is required for archiving")
        
        # Archive filepath
        archive_path = os.path.join(REPORT_ARCHIVE_DIR, f"{report_id}.json")
        
        # Write report to archive
        with open(archive_path, 'w') as f:
            json.dump(report_data, f, indent=2)
        
        logging.info(f"✓ Archived report {report_id} to {archive_path}")
        
        return {
            "success": True,
            "filepath": archive_path,
            "report_id": report_id
        }
        
    except Exception as e:
        logging.error(f"Failed to archive report: {e}", exc_info=True)
        return {
            "success": False,
            "error": str(e)
        }

def build_report_payload(entity_type, filters, results_data, requester_email, 
                         original_query, timezone_str="EST"):
    """
    Build complete report payload matching audit schema.
    
    Args:
        entity_type (str): Type of entity (deals, contacts, companies)
        filters (list): Applied filters
        results_data (list): Query results
        requester_email (str): Email of person requesting report
        original_query (str): Original user query text
        timezone_str (str): Timezone for timestamps
        
    Returns:
        dict: Complete report payload for archiving
    """
    try:
        # Generate unique report ID
        report_id = str(uuid.uuid4())
        
        # Get timezone-aware timestamps
        tz = pytz.timezone(timezone_str)
        request_time = datetime.now(tz)
        
        # Format timestamps as ISO 8601 with timezone
        request_timestamp = request_time.isoformat()
        
        # Simulate data retrieval time (in real scenario, capture actual query time)
        data_as_of = datetime.now(tz).isoformat()
        
        # Extract requester_id from email
        requester_id = f"user:{requester_email}"
        
        # Build filter representation
        filter_dict = {}
        for f in filters:
            prop_name = f.get("propertyName")
            operator = f.get("operator")
            value = f.get("value")
            
            # Convert to report-friendly format
            if operator == "GT":
                filter_dict[f"{prop_name}_gt"] = value
            elif operator == "GTE":
                filter_dict[f"{prop_name}_from"] = value
            elif operator == "LT":
                filter_dict[f"{prop_name}_lt"] = value
            elif operator == "LTE":
                filter_dict[f"{prop_name}_to"] = value
            elif operator == "EQ":
                filter_dict[prop_name] = value
            else:
                filter_dict[f"{prop_name}_{operator.lower()}"] = value
        
        # Format results based on entity type
        formatted_results = []
        total_value = 0
        highest_deal = None
        
        for record in results_data:
            props = record.get("properties", {})
            record_id = record.get("id")
            
            if entity_type == "deals":
                amount = float(props.get("amount", 0) or 0)
                formatted_record = {
                    "dealId": record_id,
                    "dealName": props.get("dealname", ""),
                    "amount": amount,
                    "currency": props.get("deal_currency_code", "INR"),
                    "amount_converted": amount,  # Assuming INR, no conversion needed
                    "close_date": props.get("closedate", ""),
                    "stage": props.get("dealstage", ""),
                    "pipeline": props.get("pipeline", ""),
                    "ownerName": props.get("hubspot_owner_name", ""),
                    "ownerEmail": props.get("hubspot_owner_email", ""),
                    "companyName": props.get("company_name", ""),
                    "dealUrl": f"https://app.hubspot.com/contacts/deals/{record_id}"
                }
                
                total_value += amount
                if highest_deal is None or amount > highest_deal["amount_converted"]:
                    highest_deal = {
                        "dealId": record_id,
                        "amount_converted": amount
                    }
                    
            elif entity_type == "contacts":
                formatted_record = {
                    "contactId": record_id,
                    "firstName": props.get("firstname", ""),
                    "lastName": props.get("lastname", ""),
                    "email": props.get("email", ""),
                    "phone": props.get("phone", ""),
                    "jobTitle": props.get("jobtitle", ""),
                    "ownerName": props.get("hubspot_owner_name", ""),
                    "contactUrl": f"https://app.hubspot.com/contacts/contact/{record_id}"
                }
                
            elif entity_type == "companies":
                formatted_record = {
                    "companyId": record_id,
                    "companyName": props.get("name", ""),
                    "domain": props.get("domain", ""),
                    "industry": props.get("industry", ""),
                    "phone": props.get("phone", ""),
                    "companyUrl": f"https://app.hubspot.com/contacts/company/{record_id}"
                }
            
            formatted_results.append(formatted_record)
        
        # Build aggregates (deals only)
        aggregates = {}
        if entity_type == "deals":
            aggregates = {
                "total_deals": len(formatted_results),
                "total_value_converted": total_value,
                "highest_deal": highest_deal
            }
        else:
            aggregates = {
                f"total_{entity_type}": len(formatted_results)
            }
        
        # Build complete report payload
        report_payload = {
            "report_id": report_id,
            "report_type": f"hubspot_{entity_type}",
            "requester_id": requester_id,
            "requester_email": requester_email,
            "request_timestamp": request_timestamp,
            "data_as_of": data_as_of,
            "timezone": timezone_str,
            "original_query": original_query,
            "filters": filter_dict,
            "results_count": len(formatted_results),
            "results": formatted_results,
            "aggregates": aggregates,
            "pagination": {
                "page_size": 500,
                "next_cursor": None
            },
            "partial": False,
            "failures": []
        }
        
        logging.info(f"✓ Built report payload: report_id={report_id}, "
                    f"entity_type={entity_type}, results_count={len(formatted_results)}")
        
        return report_payload
        
    except Exception as e:
        logging.error(f"Failed to build report payload: {e}", exc_info=True)
        return None

def trigger_report_dag(**kwargs):
    """Enhanced trigger_report_dag with professional report email formatting"""
    DEAL_STAGE_LABELS = get_deal_stage_labels()
    ti = kwargs['ti']
    report_emails = ti.xcom_pull(task_ids="branch_task", key="report_emails") or []
    
    if not report_emails:
        logging.info("No report emails to process")
        return
    
    service = authenticate_gmail()
    if not service:
        logging.error("Gmail authentication failed, cannot send reports")
        return
    
    owner_cache = {}
    
    def get_owner_name(owner_id):
        if not owner_id:
            return "Unassigned"
        if owner_id in owner_cache:
            return owner_cache[owner_id]
        
        try:
            url = f"{HUBSPOT_BASE_URL}/crm/v3/owners/{owner_id}"
            headers = {"Authorization": f"Bearer {HUBSPOT_API_KEY}"}
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                name = f"{data.get('firstName', '')} {data.get('lastName', '')}".strip()
                owner_cache[owner_id] = name or "Unknown Owner"
                return owner_cache[owner_id]
            else:
                owner_cache[owner_id] = "Unknown Owner"
                return "Unknown Owner"
        except Exception as e:
            logging.warning(f"Failed to fetch owner {owner_id}: {e}")
            owner_cache[owner_id] = "Unknown Owner"
            return "Unknown Owner"
    
    def format_date(timestamp_ms):
        if not timestamp_ms:
            return ""
        try:
            dt = datetime.fromtimestamp(int(timestamp_ms) / 1000)
            return dt.strftime("%Y-%m-%d")
        except:
            return str(timestamp_ms)
    
    def format_currency(amount):
        """Format amount as currency"""
        if not amount:
            return "₹0"
        try:
            return f"₹{int(float(amount)):,}"
        except:
            return f"₹{amount}"
    
    def get_filter_summary(filters, entity_type):
        """Generate human-readable filter summary"""
        if not filters:
            return "No filters applied"
        
        summaries = []
        for f in filters:
            prop = f.get("propertyName", "")
            op = f.get("operator", "")
            val = f.get("value", "")
            
            # Readable operator names
            op_map = {
                "EQ": "equals",
                "NEQ": "not equals",
                "GT": "greater than",
                "GTE": "greater than or equal to",
                "LT": "less than",
                "LTE": "less than or equal to",
                "CONTAINS_TOKEN": "contains",
                "NOT_CONTAINS_TOKEN": "does not contain"
            }
            
            # Special handling for deal stages
            if prop == "dealstage" and entity_type == "deals":
                val = DEAL_STAGE_LABELS.get(val, val)
            
            summaries.append(f"{prop} {op_map.get(op, op)} {val}")
        
        return " • ".join(summaries)
    
    for email in report_emails:
        try:
            headers = email.get("headers", {})
            sender_email = headers.get("From", "")
            email_id = email.get("id", "unknown")
            email_match = re.search(r'<(.+?)>', sender_email)
            clean_sender_email = email_match.group(1) if email_match else sender_email.lower()
            sender_name = "there"
            name_match = re.search(r'^([^<]+)', sender_email)
            if name_match:
                sender_name = name_match.group(1).strip()
            
            ai_response = None
            report_filepath = None
            report_filename = None
            report_success = False
            report_payload = None
            
            try:
                # Get conversation history
                conversation_history_for_ai = []
                chat_history = email.get("chat_history", [])
                for i in range(0, len(chat_history), 2):
                    if i + 1 < len(chat_history):
                        user_msg = chat_history[i]
                        assistant_msg = chat_history[i + 1]
                        if user_msg["role"] == "user" and assistant_msg["role"] == "assistant":
                            conversation_history_for_ai.append({
                                "prompt": user_msg["content"],
                                "response": assistant_msg["content"]
                            })
                
                # AI Analysis
                analysis_prompt = f"""You are a HubSpot data analyst. Analyze this request and determine what data to search for.

User request: "{email.get("content", "").strip()}"

Determine:
1. Entity type: contacts, companies, or deals
2. Filters needed (date ranges, statuses, etc.)
3. Properties to include in the report
4. Sort order if specified
5. If the user asks question like which all deals expire by this month or this year, then always take the current date or todays date as GTE and the month end or year end date as LTE.
6. Dates should only be given as YYYY-MM-DD.
7. Include `hubspot_owner_id` in the request body of contact entity type.

Return ONLY a JSON object with this structure:
{{
    "entity_type": "deals",
    "filters": [
        {{"propertyName": "dealstage", "operator": "EQ", "value": "appointmentscheduled"}}
    ],
    "properties": ["dealname", "amount", "dealstage", "closedate"],
    "sort": {{"propertyName": "closedate", "direction": "ASCENDING"}},
    "report_title": "Deals Filtered by Deal Stage"
}}

Supported operators: EQ, NEQ, LT, LTE, GT, GTE, CONTAINS_TOKEN, NOT_CONTAINS_TOKEN.
"""
                
                analysis_response = get_ai_response(
                    prompt=analysis_prompt,
                    conversation_history=conversation_history_for_ai,
                    expect_json=True
                )
                
                try:
                    analysis_data = json.loads(analysis_response)
                except:
                    analysis_data = extract_json_from_text(analysis_response)
                
                if not analysis_data or "entity_type" not in analysis_data:
                    raise ValueError("Invalid analysis response from AI")
                
                entity_type = analysis_data.get("entity_type", "contacts").lower()
                filters = analysis_data.get("filters", [])
                properties = analysis_data.get("properties", [])
                sort = analysis_data.get("sort")
                report_title = analysis_data.get("report_title", "HubSpot Report")
                
                # Default properties
                base_properties = {
                    "contacts": ['firstname', 'lastname', 'email', 'phone', 'jobtitle', 'address', 'city', 'state', 'country', 'hs_object_id', 'hubspot_owner_id'],
                    "companies": ['name', 'domain', 'phone', 'address', 'city', 'state', 'zip', 'country', 'industry', 'type', 'hs_object_id'],
                    "deals": ['dealname', 'amount', 'closedate', 'dealstage', 'pipeline', 'hubspot_owner_id', 'hs_object_id']
                }
                
                default_props = base_properties.get(entity_type, [])
                final_properties = list(set(default_props + (properties or [])))
                
                # Execute search
                if entity_type == "contacts":
                    search_results = search_contacts(filters=filters, properties=final_properties)
                elif entity_type == "companies":
                    search_results = search_companies(filters=filters, properties=final_properties)
                elif entity_type == "deals":
                    search_results = search_deals(filters=filters, properties=final_properties)
                else:
                    raise ValueError(f"Invalid entity type: {entity_type}")
                
                results_data = search_results.get("results", [])
                
                if not results_data:
                    raise ValueError("No data found matching the search criteria")

                report_payload = build_report_payload(
                    entity_type=entity_type,
                    filters=filters,
                    results_data=results_data,
                    requester_email=clean_sender_email,
                    original_query=email.get("content", "").strip(),
                    timezone_str="EST"
                )

                if not report_payload:
                    raise ValueError("Failed to build report payload")
                
                # ⭐ NEW: Archive report immediately
                archive_result = archive_report(report_payload)
                if not archive_result.get("success"):
                    logging.warning(f"Failed to archive report: {archive_result.get('error')}")
                
                # Extract report_id for logging
                report_id = report_payload.get("report_id")
                logging.info(f"✓ Report archived: report_id={report_id}, "
                           f"archive_path={archive_result.get('filepath')}")
                
                # Calculate summary statistics
                record_count = len(results_data)
                total_value = 0
                unique_owners = set()
                
                # Format data with helper columns
                formatted_data = []
                for record in results_data:
                    props = record.get("properties", {})
                    record_id = record.get("id")
                    row = {"_record_id": record_id}
                    
                    if entity_type == "contacts":
                        full_name = f"{props.get('firstname', '')} {props.get('lastname', '')}".strip()
                        owner_name = get_owner_name(props.get("hubspot_owner_id"))
                        if props.get("hubspot_owner_id"):
                            unique_owners.add(props.get("hubspot_owner_id"))
                        
                        row.update({
                            "Contact ID": record_id,
                            "Contact Name": full_name or "No Name",
                            "Job Title": props.get("jobtitle", ""),
                            "Email": props.get("email", ""),
                            "Phone": props.get("phone", ""),
                            "Street Address": props.get("address", ""),
                            "City": props.get("city", ""),
                            "State/Region": props.get("state", ""),
                            "Postal Code": props.get("zip", ""),
                            "Country": props.get("country", ""),
                            "Contact Owner": owner_name,
                            "_hubspot_url": f"https://app.hubspot.com/contacts/contact/{record_id}"
                        })
                    
                    elif entity_type == "companies":
                        row.update({
                            "Company ID": record_id,
                            "Company Name": props.get("name", "Unnamed Company"),
                            "Domain": props.get("domain", ""),
                            "Street Address": props.get("address", ""),
                            "City": props.get("city", ""),
                            "State/Region": props.get("state", ""),
                            "Country": props.get("country", ""),
                            "Phone": props.get("phone", ""),
                            "Type": props.get("type", ""),
                            "_hubspot_url": f"https://app.hubspot.com/contacts/company/{record_id}"
                        })
                    
                    elif entity_type == "deals":
                        owner_name = get_owner_name(props.get("hubspot_owner_id"))
                        if props.get("hubspot_owner_id"):
                            unique_owners.add(props.get("hubspot_owner_id"))
                        
                        stage_id = props.get("dealstage")
                        stage_label = DEAL_STAGE_LABELS.get(stage_id, "Unknown Stage")
                        
                        # Add to total value
                        amount = props.get("amount")
                        if amount:
                            try:
                                total_value += float(amount)
                            except:
                                pass
                        
                        row.update({
                            "Deal ID": record_id,
                            "Deal Name": props.get("dealname", "Untitled Deal"),
                            "Amount": props.get("amount", ""),
                            "Close Date": format_date(props.get("closedate")),
                            "Deal Stage": stage_label,
                            "Deal Owner": owner_name,
                            "_hubspot_url": f"https://app.hubspot.com/deals/deal/{record_id}"
                        })
                    
                    formatted_data.append(row)
                
                # Column order
                column_order = {
                    "contacts": ["Contact ID", "Contact Name", "Job Title", "Email", "Phone", "Street Address", "City", "State/Region", "Country", "Contact Owner"],
                    "companies": ["Company ID", "Company Name", "Domain", "Street Address", "City", "State/Region", "Country", "Phone", "Type"],
                    "deals": ["Deal ID", "Deal Name", "Amount", "Deal Stage", "Close Date", "Deal Owner"]
                }
                
                df = pd.DataFrame(formatted_data)
                ordered_cols = column_order.get(entity_type, df.columns.tolist())
                
                # Reorder only visible columns, keep helper columns
                visible_cols = [col for col in ordered_cols if col in df.columns]
                df_final = df[visible_cols + ['_hubspot_url']]
                
                # Make sure the ID column is first
                id_col = "Deal ID" if entity_type == "deals" else \
                         "Contact ID" if entity_type == "contacts" else "Company ID"
                
                if id_col in df_final.columns:
                    cols = [id_col] + [c for c in df_final.columns if c != id_col and c != '_hubspot_url'] + ['_hubspot_url']
                    df_final = df_final[cols]
                
                # Export with hyperlinks
                export_result = export_to_file(
                    data=df_final,
                    export_format='excel',
                    filename=f"hubspot_{entity_type}_report",
                    export_dir='/appz/cache/exports',
                    hyperlinks={
                        'Sheet1': {
                            id_col: {
                                'url_column': '_hubspot_url'
                            }
                        }
                    }
                )
                
                if not export_result.get("success"):
                    raise ValueError(f"Export failed: {export_result.get('error', 'Unknown error')}")
                
                report_filepath = export_result["filepath"]
                report_filename = export_result["filename"]
                report_success = True
                
                logging.info(f"✓ Report generated successfully: {report_filepath}")
                
                # Generate timestamp with timezone
                from datetime import timezone
                import pytz
                
                timezone_str = "Asia/Kolkata"
                tz = pytz.timezone(timezone_str)
                current_time = datetime.now(tz)
                timestamp_str = current_time.strftime("%Y-%m-%dT%H:%M:%S%z")
                # Format as +05:30 instead of +0530
                timestamp_str = timestamp_str[:-2] + ':' + timestamp_str[-2:]
                
                # Build filter summary
                filter_summary = get_filter_summary(filters, entity_type)
                
                # Owner summary
                owner_summary = f"{len(unique_owners)} owner(s)" if unique_owners else "All owners"
                
                # Success HTML - Professional, clean format
                ai_response = f"""
<html>
<head>
    <style>
        body {{
            font-family: Arial, 'Helvetica Neue', Helvetica, sans-serif;
            line-height: 1.6;
            color: #000000;
            max-width: 600px;
            margin: 0 auto;
            padding: 20px;
        }}
        .greeting {{
            margin-bottom: 20px;
        }}
        .report-title {{
            font-size: 18px;
            font-weight: bold;
            color: #000000;
            margin: 20px 0 10px 0;
            border-bottom: 2px solid #000000;
            padding-bottom: 8px;
        }}
        .metadata {{
            font-size: 13px;
            color: #333333;
            margin: 10px 0 20px 0;
        }}
        .stats-section {{
            margin: 25px 0;
        }}
        .stat-row {{
            display: flex;
            justify-content: space-between;
            padding: 8px 0;
            border-bottom: 1px solid #e0e0e0;
        }}
        .stat-label {{
            font-weight: 600;
            color: #000000;
        }}
        .stat-value {{
            color: #000000;
            font-weight: bold;
        }}
        .message {{
            margin: 20px 0;
            color: #000000;
        }}
        .closing {{
            margin-top: 25px;
            color: #000000;
        }}
        .signature {{
            margin-top: 30px;
            padding-top: 15px;
            border-top: 1px solid #cccccc;
        }}
        .signature-line {{
            margin: 3px 0;
            color: #000000;
        }}
        .company {{
            color: #666666;
            font-size: 13px;
            margin-top: 8px;
        }}
        .company a {{
            color: #0066cc;
            text-decoration: none;
        }}
    </style>
</head>
<body>
    <div class="greeting">
        <p>Hello {sender_name},</p>
    </div>
    
    <div class="report-title">
        {report_title}
    </div>

    <div class="metadata">
        Report ID: {report_id}<br>
        Data as of: {report_payload.get('data_as_of')} (timezone: {report_payload.get('timezone')})<br>
        Generated: {report_payload.get('request_timestamp')}
    </div>
    
    <div class="stats-section">
        <div class="stat-row">
            <span class="stat-label">TOTAL {entity_type.upper()}</span>
            <span class="stat-value">{record_count}</span>
        </div>
        {"" if entity_type != "deals" else f'''
        <div class="stat-row">
            <span class="stat-label">TOTAL VALUE</span>
            <span class="stat-value">{format_currency(total_value)}</span>
        </div>
        '''}
        <div class="stat-row">
            <span class="stat-label">OWNER FILTER</span>
            <span class="stat-value">{owner_summary}</span>
        </div>
    </div>
    
    <div class="message">
        <p>Your HubSpot report has been generated successfully. The attached Excel file includes clickable links in the <strong>{id_col}</strong> column that will take you directly to each record in HubSpot.</p>
    </div>
    
    <div class="closing">
        <p>If you need this data in a different format, with additional filters, or have any questions about the results, just reply to this email.</p>
    </div>
    
    <div class="signature">
        <div class="signature-line"><strong>Best regards,</strong></div>
        <div class="signature-line">The HubSpot Assistant Team</div>
        <div class="company">
            Powered by <a href="http://lowtouch.ai">lowtouch.ai</a>
        </div>
    </div>
</body>
</html>
"""
            
            except Exception as report_error:
                logging.error(f"Report generation failed for email {email_id}: {report_error}", exc_info=True)
                if report_payload:
                    report_payload["failures"].append({
                        "error": str(report_error),
                        "timestamp": datetime.now(pytz.timezone("EST")).isoformat()
                    })
                    report_payload["partial"] = True
                    archive_report(report_payload)  # Archive failed attempt
                
                report_success = False
                ai_response = None
                report_filepath = None
            
            # Determine which response to use
            if report_success and ai_response:
                final_response = ai_response
                log_prefix = "SUCCESS Report"
            else:
                # Fallback error message (keep your existing fallback)
                final_response = f"""
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
        <p>Thank you for requesting a report from the HubSpot Assistant.</p>
        
        <p>We're currently experiencing a temporary technical issue that is preventing us from generating your report at this time. Our engineering team has been notified and is actively working on a resolution.</p>
        
        <p>Your request has been logged, and we'll process it as soon as the system is back online. We apologize for any inconvenience this may cause.</p>
    </div>
    
    <div class="closing">
        <p>If this is urgent, please feel free to reach out directly, and we'll assist you manually.</p>
    </div>
    
    <div class="signature">
        <p>Best regards,<br>
        The HubSpot Assistant Team<br>
        <a href="http://lowtouch.ai" class="company">Lowtouch.ai</a></p>
    </div>
</body>
</html>
"""
                log_prefix = "FALLBACK Report"
            
            # Build and send email (keep your existing send logic)
            try:
                msg = MIMEMultipart()
                msg["From"] = f"HubSpot via lowtouch.ai <{HUBSPOT_FROM_ADDRESS}>"
                msg["To"] = sender_email
                
                subject = headers.get("Subject", "Your HubSpot Report")
                if not subject.lower().startswith("re:"):
                    subject = f"Re: {subject}"
                msg["Subject"] = subject
                
                original_message_id = headers.get("Message-ID", "")
                if original_message_id:
                    msg["In-Reply-To"] = original_message_id
                    references = headers.get("References", "")
                    msg["References"] = f"{references} {original_message_id}".strip() if references else original_message_id
                
                msg.attach(MIMEText(final_response, "html"))
                
                if report_success and report_filepath and os.path.exists(report_filepath):
                    with open(report_filepath, "rb") as f:
                        part = MIMEBase("application", "vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                        part.set_payload(f.read())
                    encoders.encode_base64(part)
                    part.add_header("Content-Disposition", f"attachment; filename={report_filename}")
                    msg.attach(part)
                    logging.info(f"✓ Attached report file: {report_filename}")
                
                raw = base64.urlsafe_b64encode(msg.as_string().encode()).decode()
                service.users().messages().send(userId="me", body={"raw": raw}).execute()
                mark_message_as_read(service, email_id)
                
                logging.info(f"✓ {log_prefix} email sent to {sender_email}")
            
            except Exception as send_error:
                logging.error(f"Failed to send email for {email_id}: {send_error}", exc_info=True)
                mark_message_as_read(service, email_id)
        
        except Exception as outer_error:
            logging.error(f"Unexpected error processing report email {email.get('id', 'unknown')}: {outer_error}", exc_info=True)
            try:
                mark_message_as_read(service, email.get("id", ""))
            except:
                pass
    
    logging.info(f"✓ Report DAG completed processing {len(report_emails)} emails")

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

    handle_general_queries_task = PythonOperator(
    task_id="handle_general_queries",
    python_callable=handle_general_queries,
    provide_context=True
    )

    trigger_report_task = PythonOperator(
        task_id="trigger_report_dag",
        python_callable=trigger_report_dag,
        provide_context=True
    )

    no_email_found_task = PythonOperator(
        task_id="no_email_found_task",
        python_callable=no_email_found,
        provide_context=True
    )

    fetch_emails_task >> branch_task >> [trigger_meeting_minutes_task, trigger_continuation_task, handle_general_queries_task, trigger_report_task, no_email_found_task]
