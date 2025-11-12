import base64
from email import message_from_bytes
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from airflow.models import Variable
from datetime import datetime, timedelta
import os
import json
import time
import logging
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import re
from ollama import Client

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
OLLAMA_HOST = "http://agentomatic:8000/"
PENDING_VAR_KEY = "ltai.v3.hubspot.pending_reprocess"

def _get_pending_records():
    try:
        data = Variable.get(PENDING_VAR_KEY, default_var="[]")
        return json.loads(data)
    except Exception as e:
        logging.error(f"Failed to load pending records: {e}")
        return []

def _set_pending_records(records):
    try:
        Variable.set(PENDING_VAR_KEY, json.dumps(records))
        return True
    except Exception as e:
        logging.error(f"Failed to save pending records: {e}")
        return False

def _add_pending_record(record):
    records = _get_pending_records()
    key = (record.get("message_id"), record.get("dag_id"))
    records = [r for r in records if (r.get("message_id"), r.get("dag_id")) != key]
    records.append(record)
    _set_pending_records(records)

def _remove_pending_records(keys):
    if not keys:
        return
    records = _get_pending_records()
    remaining = [r for r in records if (r.get("message_id"), r.get("dag_id")) not in keys]
    _set_pending_records(remaining)
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
            "latest_message": email.get("content", ""),  # Full latest user message
            "is_reply": email.get("is_reply", False)
        }
        email_details.append(email_info)
        logging.info(f"email details is: {email_details}")
    
    # Enhanced routing prompt with FULL conversation context
    prompt = f"""You are an AI email router that determines which workflow to execute based on user messages.

ANALYZE THE LATEST MESSAGE AND ROUTE APPROPRIATELY:

EMAILS TO CLASSIFY:
{email_details}
Important Instructions:
    - You are not capable of calling any APIs or tools.
    - You should only answer based on your knowledge and the provided email details.
    - Hubspot functions include: searching and creating contacts, companies, deals, meetings, tasks, logging notes, and summarizing engagements, casual comments based on context etc.
SEARCH_DAG CAPABILITIES:
- Searches for existing contacts, companies, deals in HubSpot
- Extracts entity information from conversations
- Determines what needs to be created vs what exists
- Generates engagement summaries for meetings
- Prepares confirmation emails for user review
- In context if the user has already created some entities and want to add more entities like additional contact or new deal then a search is needed to check if the entity exists before creating new ones.

CONTINUATION_DAG CAPABILITIES:
- Creates new contacts, companies, deals in HubSpot
- Updates existing entities based on user modifications. for example, if the deal exists and we need to change the deal amount, or we need to change the task due date to a different date. These are taken as modification. 
- Logs meeting notes and minutes
- Creates tasks with owners and due dates
- Records engagements and associations
- Handles user confirmations and modifications
- Parses the latest_message and if it is casual comment for the conversation history add it as notes.

NO_ACTION CAPABILITIES:
- Recognizes greetings, closings, and simple acknowledgments
- Outputs friendly responses without further action
- Handles questions about bot capabilities or general chat
- Does not perform any HubSpot operations
- Ignore casual comments about hubspot context.
- Handles blank emails without content or context. 
- Handles any queries out of hubspot.
ROUTING DECISION TREE:

1. **NO ACTION NEEDED** (Return: no_action)
   - Greetings: "hi", "hello", "good morning", "hey there"
   - Closings: "thanks", "thank you", "goodbye", "bye", "have a good day"
   - Simple acknowledgments: "ok", "got it", "understood", "sounds good"
   - Questions about bot capabilities or general chat
   - If the email lacks content or context.
   - If the email is about topics outside HubSpot functionalities. 
   → Response: {{"task_type": "no_action", "message": "friendly_response"}}

2. **SEARCH & ANALYZE** (Route to: search_dag)
   When user needs to:
   - Search for existing contacts, companies, or deals
   - Create NEW entities (deals, contacts, companies, meetings, tasks)
   - Log meeting minutes or notes from discussions
   - Request summaries of clients/deals before meetings
   - Any FIRST message in a new conversation thread other than greetings or general chats.
   
   Keywords: "create", "add", "new", "log meeting", "find", "search", "summarize", "what do we know about"
   → Response: {{"task_type": "search_dag", "reasoning": "..."}}

3. **CONFIRM & EXECUTE** (Route to: continuation_dag)  
   When user is:
   - Responding to bot's confirmation request ("proceed", "yes", "confirm", "looks good")
   - Making corrections to bot's proposed actions
   - Adding casual comments about existing deals/clients (no new entities) or between the conversation.
   - Updating existing records without creating new ones
   - If the creation of new entities is mentioned instead of proceed after the confirmation mail, then treat it as direct creation without search.
   
   Keywords: "proceed", "confirm", "yes", "update", "modify", "change"
   → Response: {{"task_type": "continuation_dag", "reasoning": "..."}}

DECISION LOGIC:
- Check if message requires ANY action (if not → no_action)
- For action requests: Is this creating/searching NEW entities? → search_dag
- For action requests: Is this confirming/modifying bot's proposal? → continuation_dag
- When unclear: Default to search_dag for safety

Return ONLY valid JSON:
{{"task_type": "no_action|search_dag|continuation_dag", "reasoning": "brief explanation"}}
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
        
        if "continuation" in task_type:
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
    

    logging.warning("AI failed to provide valid response, using fallback routing to no_action")
    ti.xcom_push(key="no_action_emails", value=unread_emails)
    logging.info(f"→ Fallback: Routing {len(unread_emails)} email(s) to handle_general_queries")
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
                prompt = f"""You are a friendly HubSpot email assistant.
User message: "{email.get("content", "").strip()}"

Reply in 1-2 short, polite, professional sentences.
- If greeting: acknowledge warmly.
- If out of context: say you're HubSpot-focused.
- If no content: ask for clarification.
- sign with:
    Best regards, The HubSpot Assistant Team 
    Lowtouch.ai
Return plain text only. No HTML."""

                ai_response = get_ai_response(prompt=prompt, expect_json=False)
                ai_response = re.sub(r'<[^>]+>', '', ai_response).strip()

                if not ai_response or len(ai_response) < 5 or "error" in ai_response.lower():
                    raise ValueError("Invalid AI response")

            except Exception as ai_error:
                logging.warning(f"AI failed for {email_id}: {ai_error} → using technical fallback")
                ai_response = None  # Force fallback
                try:
                    _add_pending_record({
                        "dag_id": "hubspot_monitor_mailbox",
                        "pending_type": "no_action_response",
                        "thread_id": email.get("threadId", ""),
                        "message_id": email.get("id", ""),
                        "email_data": email,
                        "chat_history": email.get("chat_history", []),
                        "thread_history": email.get("thread_history", []),
                        "timestamp": int(time.time() * 1000),
                        "reason": "ai_error"
                    })
                    logging.info(f"Saved pending no_action reprocess for message {email_id}")
                except Exception as save_err:
                    logging.error(f"Failed to save pending no_action reprocess: {save_err}")

            # === STEP 2: Decide final response ===
            if ai_response:
                final_response = ai_response
                log_prefix = "AI"
            else:
                final_response = f"""Hello {sender_name},

We’re currently experiencing a temporary technical issue that may affect your experience with the {AGENT_NAME}. Our engineering team has already identified the cause and is actively working on a resolution.

We expect regular service to resume shortly, and we’ll update you as soon as it’s fully restored. In the meantime, your data and configurations remain secure, and no action is required from your side.

Thank you for your patience and understanding — we genuinely appreciate it.

Best regards,
The HubSpot Assistant Team
Lowtouch.ai"""
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
                msg.attach(MIMEText(final_response, "plain"))

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
def no_email_found(**kwargs):
    logging.info("No new emails or replies found to process.")

def reprocess_pending_requests(**kwargs):
    try:
        pending = _get_pending_records()
        if not pending:
            logging.info("No pending reprocess records found")
            return

        service = authenticate_gmail()
        processed_keys = []

        for rec in pending:
            dag_id = rec.get("dag_id")
            message_id = rec.get("message_id")
            thread_id = rec.get("thread_id")
            email_data = rec.get("email_data", {})
            chat_history = rec.get("chat_history", [])
            thread_history = rec.get("thread_history", [])

            try:
                if dag_id == "hubspot_search_entities":
                    trigger_conf = {
                        "email_data": email_data,
                        "chat_history": chat_history,
                        "thread_history": thread_history,
                        "thread_id": thread_id,
                        "message_id": message_id
                    }
                    task_id = f"retrigger_search_{message_id.replace('-', '_')}"
                    TriggerDagRunOperator(
                        task_id=task_id,
                        trigger_dag_id="hubspot_search_entities",
                        conf=trigger_conf
                    ).execute(context=kwargs)
                    logging.info(f"Re-triggered search DAG for message {message_id}")
                    processed_keys.append((message_id, dag_id))

                elif dag_id == "hubspot_create_objects":
                    trigger_conf = {
                        "email_data": email_data,
                        "chat_history": chat_history,
                        "thread_history": thread_history,
                        "thread_id": thread_id,
                        "message_id": message_id
                    }
                    task_id = f"retrigger_create_{message_id.replace('-', '_')}"
                    TriggerDagRunOperator(
                        task_id=task_id,
                        trigger_dag_id="hubspot_create_objects",
                        conf=trigger_conf
                    ).execute(context=kwargs)
                    logging.info(f"Re-triggered create DAG for message {message_id}")
                    processed_keys.append((message_id, dag_id))

                elif dag_id == "hubspot_monitor_mailbox":
                    if not service:
                        logging.error("Gmail authentication failed, cannot re-send responses")
                        continue

                    headers = email_data.get("headers", {})
                    sender_email = headers.get("From", "")

                    prompt = f"""You are a friendly HubSpot email assistant.
User message: "{email_data.get("content", "").strip()}"

Reply in 1-2 short, polite, professional sentences.
- If greeting: acknowledge warmly.
- If out of context: say you're HubSpot-focused.
- If no content: ask for clarification.
- sign with:
    Best regards, The HubSpot Assistant Team 
    Lowtouch.ai
Return plain text only. No HTML."""

                    ai_response = get_ai_response(prompt=prompt, expect_json=False)
                    ai_response = re.sub(r'<[^>]+>', '', ai_response).strip()
                    if not ai_response or len(ai_response) < 5 or "error" in ai_response.lower():
                        logging.warning(f"AI still failing for message {message_id}")
                        continue

                    original_message_id = headers.get("Message-ID", "")
                    references = headers.get("References", "")
                    if original_message_id:
                        references = f"{references} {original_message_id}".strip() if references else original_message_id
                    subject = headers.get("Subject", "No Subject")
                    if not subject.lower().startswith("re:"):
                        subject = f"Re: {subject}"

                    all_recipients = extract_all_recipients(email_data)
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

                    msg = MIMEMultipart()
                    msg["From"] = f"HubSpot via lowtouch.ai <{HUBSPOT_FROM_ADDRESS}>"
                    msg["To"] = primary_recipient
                    if cc_string: msg["Cc"] = cc_string
                    if bcc_string: msg["Bcc"] = bcc_string
                    msg["Subject"] = subject
                    if original_message_id: msg["In-Reply-To"] = original_message_id
                    if references: msg["References"] = references
                    msg.attach(MIMEText(ai_response, "plain"))

                    raw_msg = base64.urlsafe_b64encode(msg.as_string().encode("utf-8")).decode("utf-8")
                    service.users().messages().send(userId="me", body={"raw": raw_msg}).execute()
                    logging.info(f"Re-sent AI friendly response for message {message_id}")

                    processed_keys.append((message_id, dag_id))

            except Exception as e:
                logging.error(f"Error reprocessing record {message_id}/{dag_id}: {e}", exc_info=True)

        if processed_keys:
            _remove_pending_records(processed_keys)
            logging.info(f"Removed {len(processed_keys)} processed pending records")
    except Exception as outer_e:
        logging.error(f"reprocess_pending_requests failed: {outer_e}", exc_info=True)

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

    reprocess_pending_requests_task = PythonOperator(
        task_id="reprocess_pending_requests",
        python_callable=reprocess_pending_requests,
        provide_context=True
    )

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

    no_email_found_task = PythonOperator(
        task_id="no_email_found_task",
        python_callable=no_email_found,
        provide_context=True
    )

    reprocess_pending_requests_task >> fetch_emails_task >> branch_task >> [trigger_meeting_minutes_task, trigger_continuation_task, handle_general_queries_task,no_email_found_task]