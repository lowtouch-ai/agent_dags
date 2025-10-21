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
            msg_data = service.users().messages().get(userId="me", id=msg_id, format="metadata",
                                                      metadataHeaders=["From", "Subject", "Date", "Message-ID", "References", "In-Reply-To"]).execute()
        except Exception as e:
            logging.error(f"Error fetching message {msg_id}: {e}")
            continue
            
        headers = {h["name"]: h["value"] for h in msg_data["payload"]["headers"]}
        sender = headers.get("From", "").lower()
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
        
        # ✅ FIX: Construct email_object BEFORE calling get_email_thread
        email_object = {
            "id": msg_id,
            "threadId": thread_id,
            "headers": headers,
            "content": "",  # Will be filled by get_email_thread
            "timestamp": timestamp,
            "internalDate": timestamp  # Add this for get_email_thread compatibility
        }
        
        # ✅ NOW we can safely call get_email_thread with email_object
        thread_history = get_email_thread(service, email_object)
        
        if not thread_history:
            logging.error(f"Failed to retrieve thread history for message {msg_id}")
            continue
        
        # Format chat history (all messages except the latest)
        chat_history = format_chat_history(thread_history)
        
        # Extract latest message content (current user prompt)
        latest_message = thread_history[-1]
        
        # Update email_object with the latest message content
        email_object["content"] = latest_message["content"]
        
        # Determine if this is a reply based on thread length and email headers
        is_reply = len(thread_history) > 1
        subject = headers.get("Subject", "")
        is_reply = is_reply or subject.lower().startswith("re:") or bool(headers.get("In-Reply-To")) or bool(headers.get("References"))
        
        # ✅ Add all required fields to email_object
        email_object.update({
            "is_reply": is_reply,
            "thread_history": thread_history,  # Full thread for reference
            "chat_history": chat_history,  # Formatted for agent
            "thread_length": len(thread_history)
        })
        
        logging.info(f"Email {msg_id}: is_reply={is_reply}, thread_length={len(thread_history)}, chat_history_length={len(chat_history)}")
        
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
        chat_history = email.get("chat_history", [])
        logging.info(f"Chat history is: {chat_history}")
        # Format ALL messages in chat history for context
        conversation_history = []
        for msg in chat_history:  # ALL messages, not just last 3
            conversation_history.append({
                "role": msg.get("role", "unknown"),
                "content": msg.get("content", ""),
                "from": msg.get("from", "Unknown"),
                "timestamp": msg.get("timestamp", 0)
            })
        
        email_info = {
            "email_number": idx,
            "from": headers.get("From", "Unknown"),
            "subject": headers.get("Subject", "No Subject"),
            "latest_message": email.get("content", ""),  # Full latest user message
            "is_reply": email.get("is_reply", False),
            "thread_length": email.get("thread_length", 1),
            "conversation_history": conversation_history  # COMPLETE conversation context
        }
        email_details.append(email_info)
    
    # Enhanced routing prompt with FULL conversation context
    prompt = f'''You are an AI assistant that routes emails to the appropriate workflow. You have access to the COMPLETE conversation history and the user's latest message. Analyze the full context to determine the correct action.

EMAILS TO CLASSIFY:
{json.dumps(email_details, indent=2)}

ROUTING RULES (Execute in priority order - ANALYZE CONTENT IN CONTEXT):

1. **NEW ACTION REQUEST (Search first, then create)** ⚠️ HIGHEST PRIORITY
   - User requests to CREATE new entities: deals, contacts, companies, meetings
   - User wants to LOG meeting minutes or discussion notes
   - User assigns NEW TASKS with due dates and owners
   - Keywords: "create", "add", "open", "log meeting", "schedule task", "new deal", "add contact", "second deal", "another deal"
   - Example: Even in a reply thread, if user says "open a second deal" or "create another task" → Route to search_dag
   → Route to: search_dag

2. **CONTINUATION (Reply to confirmation or follow-up actions)**
   - User is responding to bot's confirmation request (check conversation_history for bot's questions)
   - User provides clarifications, corrections, or approvals to previous bot messages
   - Simple confirmations: "proceed", "confirm", "looks good", "yes", "correct", "that's right"
   - User making minor corrections to bot's proposed actions
   - Check conversation_history: if bot asked for confirmation and user is responding → continuation_dag
   → Route to: continuation_dag

3. **CASUAL COMMENTS (Add as notes)**
   - Informal updates about clients/deals/meetings without action requests
   - No explicit action verbs
   - Conversational tone without specific requests
   → Route to: continuation_dag

4. **UPDATE EXISTING ENTITIES**
   - User explicitly asks to UPDATE or MODIFY existing records
   - Keywords: "update", "modify", "change", "edit", "correct"
   - References specific entity IDs or names
   - NO new entity creation requested
   → Route to: continuation_dag

5. **ENGAGEMENT SUMMARY REQUEST**
   - User requests summary of contact/deal before meeting
   - Keywords: "summarize", "summary for", "prepare summary", "what do we know about"
   → Route to: search_dag

CRITICAL DECISION LOGIC:
- Look at conversation_history to understand what the bot previously said/asked
- Look at latest_message to see what the user is requesting NOW
- If latest_message contains NEW creation requests (deals, tasks, contacts) → search_dag (even if is_reply=true)
- If latest_message is confirming/responding to bot's previous question → continuation_dag
- If latest_message has no conversation_history (thread_length=1) → search_dag (new conversation)
- When in doubt between new vs continuation, prefer search_dag for action requests

RESPONSE FORMAT (JSON only):
{{"task_type": "continuation_dag", "reasoning": "User confirming bot's previous confirmation request"}}
{{"task_type": "search_dag", "reasoning": "User requesting new deal creation in follow-up message"}}

Analyze the conversation context and respond with JSON:'''
    
    logging.info(f"Sending routing prompt to AI with {len(email_details)} emails and conversation context")
    
    response = get_ai_response(prompt, expect_json=True)
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
    
    # Fallback logic
    logging.warning("AI failed to provide valid response, using fallback")
    replies = [e for e in unread_emails if e.get("is_reply", False)]
    new_emails = [e for e in unread_emails if not e.get("is_reply", False)]
    
    if replies:
        ti.xcom_push(key="reply_emails", value=replies)
        return "trigger_continuation_dag"
    if new_emails:
        ti.xcom_push(key="new_emails", value=new_emails)
        return "trigger_meeting_minutes"

    return "no_email_found_task"

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
