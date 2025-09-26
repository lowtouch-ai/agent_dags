import logging
import json
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
import base64
import os
import re
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from ollama import Client
from email import message_from_bytes
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from googleapiclient.errors import HttpError
from airflow.models import Variable
import time

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
OLLAMA_HOST = "http://agentomatic:8000/"
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

def get_thread_context():
    try:
        if os.path.exists(THREAD_CONTEXT_FILE):
            with open(THREAD_CONTEXT_FILE, "r") as f:
                return json.load(f)
    except Exception as e:
        logging.error(f"Error reading {THREAD_CONTEXT_FILE}: {e}")
        return {}

def update_thread_context(thread_id, context_data):
    os.makedirs(os.path.dirname(THREAD_CONTEXT_FILE), exist_ok=True)
    try:
        contexts = get_thread_context()
        contexts[thread_id] = context_data
        # Validate JSON before writing
        json_string = json.dumps(contexts, indent=2)
        json.loads(json_string)  # Ensure it’s valid JSON
        with open(THREAD_CONTEXT_FILE, "w") as f:
            f.write(json_string)
        logging.info(f"Updated thread context for thread_id={thread_id}")
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON format when writing to {THREAD_CONTEXT_FILE}: {e}")
        raise
    except Exception as e:
        logging.error(f"Error writing to {THREAD_CONTEXT_FILE}: {e}")
        raise

def decode_email_payload(msg):
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
    try:
        if not email_data or "headers" not in email_data or not isinstance(email_data.get("headers"), dict):
            logging.error("Invalid email_data: 'headers' key missing or not a dictionary")
            return []

        thread_id = email_data.get("threadId")
        message_id = email_data["headers"].get("Message-ID", "")
        email_id = email_data.get("id", "")

        if not thread_id:
            logging.warning(f"No thread ID provided for message ID {message_id}. Querying Gmail API.")
            query_result = service.users().messages().list(userId="me", q=f"rfc822msgid:{message_id}").execute()
            messages = query_result.get("messages", [])
            if messages:
                message = service.users().messages().get(userId="me", id=messages[0]["id"]).execute()
                thread_id = message.get("threadId")
                logging.info(f"Resolved thread ID: {thread_id} for message ID {message_id}")

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

        email_thread = []
        page_token = None
        retries = 3
        for attempt in range(retries):
            try:
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
                break
            except HttpError as e:
                logging.error(f"Attempt {attempt+1} failed to fetch thread {thread_id}: {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    logging.error(f"Failed to fetch thread {thread_id} after {retries} attempts")
                    return []

        logging.info(f"Processing thread {thread_id} with {len(email_thread)} messages")
        processed_thread = []
        for msg in email_thread:
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

        processed_thread.sort(key=lambda x: x.get("timestamp", 0))

        logging.info(f"Retrieved thread {thread_id} with {len(processed_thread)} messages")
        for idx, email in enumerate(processed_thread, 1):
            logging.info(f"Email {idx}: message_id={email['message_id']}, from={email['headers'].get('From', 'Unknown')}, timestamp={email['timestamp']}, from_bot={email['from_bot']}, content_preview={email['content'][:100]}...")
        return processed_thread
    except Exception as e:
        logging.error(f"Error retrieving email thread for thread_id={thread_id}: {e}", exc_info=True)
        return []

def get_ai_response(prompt, conversation_history=None, expect_json=False):
    try:
        client = Client(host=OLLAMA_HOST, headers={'x-ltai-client': 'HubSpotWorkflow'})
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
        response = client.chat(model='hubspot:v4', messages=messages, stream=False)
        ai_content = response.message.content
        ai_content = re.sub(r'```(?:html|json)\n?|```', '', ai_content)

        if expect_json:
            return ai_content
        if not ai_content.strip().startswith('<!DOCTYPE') and not ai_content.strip().startswith('<html') and not ai_content.strip().startswith('{'):
            ai_content = f"<html><body>{ai_content}</body></html>"
        return ai_content.strip()
    except Exception as e:
        logging.error(f"Error in get_ai_response: {e}")
        if expect_json:
            return json.dumps({"error": str(e)})
        return f"<html><body>Error processing AI request: {str(e)}</body></html>"

import re

def parse_email_addresses(address_string):
    """Parse email addresses from To/Cc/Bcc header strings"""
    if not address_string:
        return []
    
    # Split by comma and clean up each address
    addresses = []
    for addr in address_string.split(','):
        addr = addr.strip()
        if addr:
            addresses.append(addr)
    return addresses

def extract_all_recipients(email_data):
    """Extract all recipients from To, Cc, and Bcc headers"""
    headers = email_data.get("headers", {})
    
    to_recipients = parse_email_addresses(headers.get("To", ""))
    cc_recipients = parse_email_addresses(headers.get("Cc", ""))
    bcc_recipients = parse_email_addresses(headers.get("Bcc", ""))
    
    return {
        "to": to_recipients,
        "cc": cc_recipients,
        "bcc": bcc_recipients
    }

def send_email(service, recipient, subject, body, in_reply_to, references, cc=None, bcc=None):
    """Updated send_email function with BCC support"""
    try:
        msg = MIMEMultipart()
        msg["From"] = f"HubSpot via lowtouch.ai <{HUBSPOT_FROM_ADDRESS}>"
        msg["To"] = recipient
        
        if cc:
            # Clean Cc: Remove bot's own email if present, and ensure it's a string
            cc_list = [email.strip() for email in cc.split(',') if email.strip().lower() != HUBSPOT_FROM_ADDRESS.lower()]
            cleaned_cc = ', '.join(cc_list)
            if cleaned_cc:
                msg["Cc"] = cleaned_cc
                logging.info(f"Including Cc in email: {cleaned_cc}")
            else:
                logging.info("Cc provided but empty after cleaning, skipping.")
        else:
            logging.info("No Cc provided.")
            
        if bcc:
            # Clean Bcc: Remove bot's own email if present
            bcc_list = [email.strip() for email in bcc.split(',') if email.strip().lower() != HUBSPOT_FROM_ADDRESS.lower()]
            cleaned_bcc = ', '.join(bcc_list)
            if cleaned_bcc:
                msg["Bcc"] = cleaned_bcc
                logging.info(f"Including Bcc in email: {cleaned_bcc}")
            else:
                logging.info("Bcc provided but empty after cleaning, skipping.")
        else:
            logging.info("No Bcc provided.")
            
        msg["Subject"] = subject
        msg["In-Reply-To"] = in_reply_to
        msg["References"] = references
        msg.attach(MIMEText(body, "html"))
        
        raw_msg = base64.urlsafe_b64encode(msg.as_string().encode("utf-8")).decode("utf-8")
        result = service.users().messages().send(userId="me", body={"raw": raw_msg}).execute()
        logging.info(f"Email sent to {recipient} (Cc: {cc if cc else 'None'}, Bcc: {bcc if bcc else 'None'})")
        return result
    except Exception as e:
        logging.error(f"Failed to send email: {e}")
        return None

def analyze_user_response(ti, **context):
    conf = context["dag_run"].conf
    thread_id = conf.get("thread_id")
    search_results = conf.get("search_results", {})
    create_results = conf.get("create_results", {})
    logging.info(f"Search results: {search_results}")
    logging.info(f"Create results: {create_results}")
    
    if not thread_id:
        logging.error("No thread_id provided in dag_run.conf")
        results = {
            "status": "error",
            "error_message": "No thread_id provided in dag_run.conf",
            "next_steps": ["Provide a valid thread_id"],
            "entities_to_create": {},
            "entities_to_update": {},
            "user_intent": "ERROR",
            "confidence_level": "low",
            "tasks_to_execute": ["create_associations", "compose_response_html", "collect_and_save_results", "send_final_email"]
        }
        ti.xcom_push(key="analysis_results", value=results)
        return results

    thread_context = get_thread_context().get(thread_id, {})
    email_data = conf.get("email_data", thread_context.get("email_data", {}))
    full_thread_history = conf.get("full_thread_history", thread_context.get("thread_history", []))
    user_response_email = conf.get("user_response_email", email_data)

    logging.info(f"=== CONTINUATION DAG INPUT DEBUG ===")
    logging.info(f"Conf keys: {list(conf.keys())}")
    logging.info(f"Thread ID: {thread_id}")
    logging.info(f"Full thread history length: {len(full_thread_history)}")
    logging.info(f"Search results new_entity_details: {search_results.get('new_entity_details', {})}")
    logging.info(f"Create results: {create_results}")

    service = authenticate_gmail()
    if not service:
        logging.error("Gmail authentication failed, skipping analysis.")
        results = {
            "status": "error",
            "error_message": "Gmail authentication failed",
            "next_steps": ["Retry authentication"],
            "entities_to_create": {},
            "entities_to_update": {},
            "selected_entities": search_results.get("selected_entities", {}),
            "user_intent": "ERROR",
            "confidence_level": "low",
            "tasks_to_execute": ["create_associations", "compose_response_html", "collect_and_save_results", "send_final_email"]
        }
        ti.xcom_push(key="analysis_results", value=results)
        return results

    if not full_thread_history:
        logging.warning(f"No thread history in conf for thread_id={thread_id}, fetching from Gmail API")
        full_thread_history = get_email_thread(service, email_data)
        if not full_thread_history:
            logging.error(f"Failed to fetch thread history for thread_id={thread_id}")
            results = {
                "status": "error",
                "error_message": "Failed to fetch thread history",
                "next_steps": ["Verify thread_id and Gmail API access"],
                "entities_to_create": {},
                "entities_to_update": {},
                "selected_entities": search_results.get("selected_entities", {}),
                "user_intent": "ERROR",
                "confidence_level": "low",
                "tasks_to_execute": ["create_associations", "compose_response_html", "collect_and_save_results", "send_final_email"]
            }
            ti.xcom_push(key="analysis_results", value=results)
            return results

    # Normalize original entities from search_results
    original_entities_normalized = {
        "contacts": search_results.get("new_entity_details", {}).get("contacts", []),
        "companies": search_results.get("new_entity_details", {}).get("companies", []),
        "deals": search_results.get("new_entity_details", {}).get("deals", []),
        "meetings": [search_results.get("new_entity_details", {}).get("meeting_details", {})] if search_results.get("new_entity_details", {}).get("meeting_details") else [],
        "notes": search_results.get("new_entity_details", {}).get("notes", []),
        "tasks": search_results.get("new_entity_details", {}).get("tasks", [])
    }

    # Get entities from search_results - THESE ARE THE EXISTING ENTITIES FOUND
    existing_entities = {
        "contacts": search_results.get("contact_results", {}).get("results", []),
        "companies": search_results.get("company_results", {}).get("results", []),
        "deals": search_results.get("deal_results", {}).get("results", []),
        "meetings": [],
        "notes": [],
        "tasks": []
    }

    prior_analysis_results = thread_context.get("analysis_results", {})
    prior_selected_entities = prior_analysis_results.get("selected_entities", {}) if prior_analysis_results else {}
    # Initialize selected entities (will be filtered based on user selection)
    selected_entities = {
        "contacts": prior_selected_entities.get("contacts", existing_entities["contacts"].copy()),
        "companies": prior_selected_entities.get("companies", existing_entities["companies"].copy()),
        "deals": prior_selected_entities.get("deals", existing_entities["deals"].copy()),
        "meetings": prior_selected_entities.get("meetings", []),
        "notes": prior_selected_entities.get("notes", []),
        "tasks": prior_selected_entities.get("tasks", [])
    }
    logging.info(f"Initialized selected_entities from prior context: Contacts={len(selected_entities['contacts'])}, Companies={len(selected_entities['companies'])}, Deals={len(selected_entities['deals'])}")

    # Merge create_results into selected_entities (treat as existing entities)
    if create_results:
        previous_contacts = create_results.get("created_contacts", {}).get("results", []) + create_results.get("updated_contacts", {}).get("results", [])
        previous_companies = create_results.get("created_companies", {}).get("results", []) + create_results.get("updated_companies", {}).get("results", [])
        previous_deals = create_results.get("created_deals", {}).get("results", []) + create_results.get("updated_deals", {}).get("results", [])
        previous_meetings = create_results.get("created_meetings", {}).get("results", []) + create_results.get("updated_meetings", {}).get("results", [])
        previous_notes = create_results.get("created_notes", {}).get("results", []) + create_results.get("updated_notes", {}).get("results", [])
        previous_tasks = create_results.get("created_tasks", {}).get("results", []) + create_results.get("updated_tasks", {}).get("results", [])

        selected_entities["contacts"].extend([{"contactId": c.get("id"), **c.get("details", {})} for c in previous_contacts])
        selected_entities["companies"].extend([{"companyId": c.get("id"), **c.get("details", {})} for c in previous_companies])
        selected_entities["deals"].extend([{"dealId": d.get("id"), **d.get("details", {})} for d in previous_deals])
        selected_entities["meetings"].extend([{"meetingId": m.get("id"), **m.get("details", {})} for m in previous_meetings])
        selected_entities["notes"].extend([{"noteId": n.get("id"), **n.get("details", {})} for n in previous_notes])
        selected_entities["tasks"].extend([{"taskId": t.get("id"), **t.get("details", {})} for t in previous_tasks])

        logging.info(f"Merged previous create_results into selected_entities. Updated selected_entities: {selected_entities}")

    thread_content = ""
    bot_messages = []
    user_messages = []
    latest_user_response = ""

    for idx, email in enumerate(full_thread_history, 1):
        content = email.get("content", "").strip()
        if content:
            soup = BeautifulSoup(content, "html.parser")
            clean_content = soup.get_text(separator=" ", strip=True)
            sender = email['headers'].get('From', 'Unknown')
            timestamp = email.get('timestamp', 0)
            is_from_bot = email.get('from_bot', False)

            if is_from_bot:
                bot_messages.append({
                    "content": clean_content,
                    "timestamp": timestamp,
                    "email_index": idx
                })
                thread_content += f"### Bot Message {idx} (From: {sender})\n{clean_content}\n\n"
            else:
                user_messages.append({
                    "content": clean_content,
                    "timestamp": timestamp,
                    "email_index": idx,
                    "sender": sender
                })
                thread_content += f"### User Message {idx} (From: {sender})\n{clean_content}\n\n"
                if idx == len(full_thread_history):
                    latest_user_response = clean_content

    if not thread_content:
        logging.error("No valid thread content found")
        results = {
            "status": "error",
            "error_message": "No valid thread content found. Unable to process.",
            "next_steps": ["Send a new email to restart the workflow"],
            "entities_to_create": {},
            "entities_to_update": {},
            "selected_entities": selected_entities,
            "user_intent": "ERROR",
            "confidence_level": "low",
            "tasks_to_execute": ["create_associations", "compose_response_html", "collect_and_save_results", "send_final_email"]
        }
        ti.xcom_push(key="analysis_results", value=results)
        return results

    # Updated analysis prompt to rely on AI interpretation
    analysis_prompt = f"""
You are an AI assistant tasked with analyzing the user's latest response in the context of an email thread to determine their intent and decide which entities to use, create, or update. Focus on the semantic meaning of the user's response, not just specific keywords. Use the full conversation thread, existing entities, and proposed new entities to make informed decisions.

EXISTING ENTITIES FOUND IN SEARCH (presented to the user as options):
CONTACTS: {json.dumps(existing_entities["contacts"], indent=2)}
COMPANIES: {json.dumps(existing_entities["companies"], indent=2)}  
DEALS: {json.dumps(existing_entities["deals"], indent=2)}

ORIGINAL PROPOSED NEW ENTITIES (suggested for creation in the initial workflow):
{json.dumps(original_entities_normalized, indent=2)}

PREVIOUSLY CREATED/UPDATED ENTITIES (from prior actions, for reference):
{json.dumps({
    "created_contacts": create_results.get("created_contacts", {}).get("results", []),
    "created_companies": create_results.get("created_companies", {}).get("results", []),
    "created_deals": create_results.get("created_deals", {}).get("results", []),
    "created_meetings": create_results.get("created_meetings", {}).get("results", []),
    "created_notes": create_results.get("created_notes", {}).get("results", []),
    "created_tasks": create_results.get("created_tasks", {}).get("results", [])
}, indent=2)}

FULL CONVERSATION THREAD:
{thread_content}

LATEST USER RESPONSE:
{latest_user_response}

INSTRUCTIONS:
1. **Determine User Intent**: Based on the latest user response and thread context, identify the user's intent. Possible intents are:
   - "CONFIRM": User agrees to proceed with proposed entities (existing or new).
   - "MODIFY": User requests changes to existing or proposed entities.
   - "CREATE_NEW": User wants to create new entities, ignoring existing ones.
   - "SELECT_SPECIFIC": User selects specific existing entities from multiple options.
   - "CLARIFY": User needs clarification or provides unclear instructions.
   - "CANCEL": User wants to stop the workflow.
   Focus on the meaning of the response, not just keywords. For example, "use the first contact" or "I meant John from Acme" indicates SELECT_SPECIFIC, while "looks good" implies CONFIRM.
2. **Casual Comment Detection**: ADDITIONALLY, check if the user's response contains casual comments, observations, or informal updates about clients, deals, companies, or business relationships that should be captured as notes. Examples:
   - "We already kicked off that CRM integration deal with Rohit… feels like things are moving there."
   - "Had a good call with the team at Acme yesterday"
   - "John seems happy with our progress so far"
   - "The client mentioned they want to expand next quarter"
   If casual comments are found, create notes for them in addition to other requested actions. **Include the speaker's name/email from the email headers in the note content.**
   
3. **Entity Handling**:
   - **Primary Entities (contacts, companies, deals)**:
     - If the user selects specific existing entities (by name, ID, or details), include only those in `entities_to_use_existing`.
     - If the user requests new primary entities, include them in `entities_to_create`.
     - If the user confirms without specifying, include all existing primary entities in `entities_to_use_existing`.
   - **Secondary Entities (meetings, notes, tasks)**:
     - If PREVIOUSLY CREATED/UPDATED ENTITIES are NOT present (no prior creations/updates), always include all proposed secondary entities from ORIGINAL PROPOSED NEW ENTITIES in `entities_to_create` unless the user explicitly states to exclude them (e.g., "do not create tasks") or requests specific modifications.
     - If PREVIOUSLY CREATED/UPDATED ENTITIES are present, follow the user's intent: 
       - For "CONFIRM" or no mention: include all proposed secondary entities in `entities_to_create` (or `entities_to_update` if modifications are implied).
       - For "MODIFY" or "SELECT_SPECIFIC": include only the specified or modified versions in `entities_to_create` or `entities_to_update`.
       - For "CREATE_NEW": include all in `entities_to_create`, overriding priors.
       - For "CLARIFY" or "CANCEL": do not include any.
     - If the user response only addresses primary entities (e.g., confirms contacts/companies), default to including all proposed secondary entities in `entities_to_create` unless explicitly excluded.
4. **Selection Logic**:
   - If multiple entities of a type (e.g., contacts) were presented, and the user specifies which to use (e.g., by name, ID, or position like "first one"), only include those in `entities_to_use_existing`.
   - If the user provides vague references (e.g., "use John"), match to the most likely entity based on details like name or company.
   - If no specific selection is made but the user confirms, include all existing entities.

5. **Confidence Level**: Assign "high", "medium", or "low" based on how clear the user's intent and selections are. Use "low" for ambiguous responses requiring clarification.

6. **Reasoning**: Provide a brief explanation of how you determined the intent, selections, and entity actions.


Return ONLY valid JSON:
{{
    "user_intent": "...",
    "confidence_level": "...",
    "entity_selections": {{
        "contacts": [{{"contactId": "...", "reason": "User specified this contact by name/details"}}],
        "companies": [{{"companyId": "...", "reason": "User specified this company"}}],
        "deals": [{{"dealId": "...", "reason": "User specified this deal"}}]
    }},
    "requested_changes": {{
        "contacts": [],
        "companies": [],
        "deals": [],
        "meetings": [],
        "notes": [],
        "tasks": []
    }},
    "entities_to_use_existing": {{
        "contacts": [],
        "companies": [],
        "deals": [],
        "meetings": [],
        "notes": [],
        "tasks": []
    }},
    "entities_to_create": {{
        "contacts": [],
        "companies": [],
        "deals": [],
        "meetings": [],
        "notes": [
        "note_content": "[sender_name] mentioned [casual_comment_text]",
            "timestamp": "2025-01-XX...",
            "speaker_name": "[extracted_from_From_header]",
            "speaker_email": "[extracted_email]"], //
        "tasks": []
    }},
    "entities_to_update": {{
        "contacts": [],
        "companies": [],
        "deals": [],
        "meetings": [],
        "notes": [],
        "tasks": []
    }},
    "casual_comments_detected": true/false,
    "reasoning": "..."
}}
"""

    ai_analysis = get_ai_response(analysis_prompt, expect_json=True)
    
    try:
        parsed_analysis = json.loads(ai_analysis)
        user_intent = parsed_analysis.get("user_intent", "CONFIRM")
        confidence_level = parsed_analysis.get("confidence_level", "medium")
        entity_selections = parsed_analysis.get("entity_selections", {})
        requested_changes = parsed_analysis.get("requested_changes", {})
        entities_to_use_existing = parsed_analysis.get("entities_to_use_existing", {})
        entities_to_create = parsed_analysis.get("entities_to_create", {})
        entities_to_update = parsed_analysis.get("entities_to_update", {})
        reasoning = parsed_analysis.get("reasoning", "")
        
        # Apply entity filtering based on user selections
        filtered_selected_entities = {
            "contacts": [],
            "companies": [],
            "deals": [],
            "meetings": selected_entities["meetings"],  # These typically don't have multiple options
            "notes": selected_entities["notes"],
            "tasks": selected_entities["tasks"]
        }
        
        # Filter contacts based on user selection
        if entity_selections.get("contacts"):
            selected_contact_ids = [sel["contactId"] for sel in entity_selections["contacts"]]
            filtered_selected_entities["contacts"] = [
                contact for contact in selected_entities["contacts"] 
                if contact.get("contactId") in selected_contact_ids
            ]
            logging.info(f"Filtered contacts to user selection: {len(filtered_selected_entities['contacts'])} of {len(selected_entities['contacts'])}")
        else:
            filtered_selected_entities["contacts"] = prior_selected_entities.get("contacts", selected_entities["contacts"])
            logging.info(f"No new contact selection; using prior filtered: {len(filtered_selected_entities['contacts'])}")
            
        # Filter companies based on user selection  
        if entity_selections.get("companies"):
            selected_company_ids = [sel["companyId"] for sel in entity_selections["companies"]]
            filtered_selected_entities["companies"] = [
                company for company in selected_entities["companies"]
                if company.get("companyId") in selected_company_ids
            ]
            logging.info(f"Filtered companies to user selection: {len(filtered_selected_entities['companies'])} of {len(selected_entities['companies'])}")
        else:
            filtered_selected_entities["companies"] = prior_selected_entities.get("companies", selected_entities["companies"])
            logging.info(f"No new company selection; using prior filtered: {len(filtered_selected_entities['companies'])}")
        # Filter deals based on user selection
        if entity_selections.get("deals"):
            selected_deal_ids = [sel["dealId"] for sel in entity_selections["deals"]]
            filtered_selected_entities["deals"] = [
                deal for deal in selected_entities["deals"]
                if deal.get("dealId") in selected_deal_ids  
            ]
            logging.info(f"Filtered deals to user selection: {len(filtered_selected_entities['deals'])} of {len(selected_entities['deals'])}")
        else:
            filtered_selected_entities["deals"] = selected_entities["deals"]
        
        # Update selected_entities to use the filtered version
        selected_entities = filtered_selected_entities
        
        logging.info(f"AI Analysis Results:")
        logging.info(f"User Intent: {user_intent}")
        logging.info(f"Confidence: {confidence_level}")
        logging.info(f"Entity Selections: {json.dumps(entity_selections, indent=2)}")
        logging.info(f"Reasoning: {reasoning}")
        logging.info(f"Final Selected Entities Count - Contacts: {len(selected_entities['contacts'])}, Companies: {len(selected_entities['companies'])}, Deals: {len(selected_entities['deals'])}")
        
        should_determine_owner = False
        should_check_task_threshold = False

        if entities_to_create.get("deals") or entities_to_create.get("tasks"):
            should_determine_owner = True
        if entities_to_create.get("tasks"):
            should_check_task_threshold = True

        
        # Define tasks to execute based on user intent
        tasks_to_execute = []
        if user_intent in ["MODIFY", "CONFIRM", "SELECT_SPECIFIC"]:
            if entities_to_update.get("tasks"):
                tasks_to_execute.append("update_tasks")
            if entities_to_update.get("contacts"):
                tasks_to_execute.append("update_contacts")
            if entities_to_update.get("companies"):
                tasks_to_execute.append("update_companies")
            if entities_to_update.get("deals"):
                tasks_to_execute.append("update_deals")
            if entities_to_update.get("meetings"):
                tasks_to_execute.append("update_meetings")
            if entities_to_update.get("notes"):
                tasks_to_execute.append("update_notes")
            if entities_to_create.get("contacts"):
                tasks_to_execute.append("create_contacts")
            if entities_to_create.get("companies"):
                tasks_to_execute.append("create_companies")
            if entities_to_create.get("deals"):
                tasks_to_execute.append("create_deals")
            if entities_to_create.get("meetings"):
                tasks_to_execute.append("create_meetings")
            if entities_to_create.get("notes"):
                tasks_to_execute.append("create_notes")
            if entities_to_create.get("tasks"):
                tasks_to_execute.append("create_tasks")
            tasks_to_execute.extend(["create_associations", "compose_response_html", "collect_and_save_results", "send_final_email"])
        elif user_intent == "CREATE_NEW":
            tasks_to_execute = ["create_contacts", "create_companies", "create_deals", "create_meetings", "create_notes", "create_tasks", "create_associations", "compose_response_html", "collect_and_save_results", "send_final_email"]
        elif user_intent == "CLARIFY":
            tasks_to_execute = ["compose_response_html", "collect_and_save_results", "send_final_email"]
        elif user_intent == "CANCEL":
            tasks_to_execute = ["compose_response_html", "collect_and_save_results", "send_final_email"]

        # If no tasks are to be executed, set mandatory tasks
        if not tasks_to_execute:
            logging.info("No tasks to execute, proceeding with mandatory tasks only.")
            tasks_to_execute = ["create_associations", "compose_response_html", "collect_and_save_results", "send_final_email"]

        # Define results dictionary for successful AI analysis
        results = {
            "status": "success",
            "error_message": "",
            "next_steps": ["Proceed with entity updates/creation based on intent and selections"],
            "user_intent": user_intent,
            "confidence_level": confidence_level,
            "entity_selections": entity_selections,
            "requested_changes": requested_changes,
            "entities_to_create": entities_to_create,
            "entities_to_update": entities_to_update,
            "selected_entities": selected_entities,  # This now contains filtered entities
            "reasoning": reasoning,
            "tasks_to_execute": tasks_to_execute
        }
        results["should_determine_owner"] = should_determine_owner
        results["should_check_task_threshold"] = should_check_task_threshold
        
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse AI analysis: {e}")
        logging.error(f"Raw AI response: {ai_analysis}")
        # Fallback
        user_intent = "CONFIRM"
        if "PROCEED WITH EXISTING" in latest_user_response.upper():
            user_intent = "CONFIRM"
        elif "CREATE NEW" in latest_user_response.upper():
            user_intent = "CREATE_NEW"
        elif any(keyword in latest_user_response.lower() for keyword in ["modify", "change", "update", "correct"]):
            user_intent = "MODIFY"
        
        tasks_to_execute = ["create_associations", "compose_response_html", "collect_and_save_results", "send_final_email"]
        
        results = {
            "status": "error",
            "error_message": f"Failed to parse AI analysis: {str(e)}",
            "next_steps": ["Retry AI analysis or clarify user response"],
            "user_intent": user_intent,
            "confidence_level": "low",
            "entity_selections": {},
            "requested_changes": {},
            "entities_to_create": {},
            "entities_to_update": {},
            "selected_entities": selected_entities,
            "reasoning": "Fallback analysis due to AI parsing error",
            "tasks_to_execute": tasks_to_execute
        }

    # Push results to XCom
    ti.xcom_push(key="analysis_results", value=results)

    # Update thread context
    updated_context = thread_context.copy()
    updated_context.update({
        "analysis_results": results,
        "full_thread_history": full_thread_history,
        "latest_user_response": latest_user_response,
        "analysis_timestamp": datetime.now().isoformat(),
        "workflow_status": "analysis_completed",
        "awaiting_reply": False,
        "persistent_selected_entities": selected_entities
    })
    update_thread_context(thread_id, updated_context)

    logging.info(f"Analysis completed for thread {thread_id}")
    logging.info(f"User intent: {user_intent}, Tasks to execute: {tasks_to_execute}")
    return results

def determine_owner(ti, **context):
    """Fixed version that extracts thread content and tasks properly"""
    # Get analysis results to extract thread content and tasks
    analysis_results = ti.xcom_pull(key="analysis_results", default={})
    thread_id = context['dag_run'].conf.get("thread_id")
    
    # Get thread content from DAG run conf or thread context
    conf = context["dag_run"].conf
    full_thread_history = conf.get("full_thread_history", [])
    
    # Build thread content from full history
    thread_content = ""
    for idx, email in enumerate(full_thread_history, 1):
        content = email.get("content", "").strip()
        if content:

            soup = BeautifulSoup(content, "html.parser")
            clean_content = soup.get_text(separator=" ", strip=True)
            sender = email['headers'].get('From', 'Unknown')
            is_from_bot = email.get('from_bot', False)
            
            if is_from_bot:
                thread_content += f"### Bot Message {idx} (From: {sender})\n{clean_content}\n\n"
            else:
                thread_content += f"### User Message {idx} (From: {sender})\n{clean_content}\n\n"
    
    # Get tasks from analysis results
    entities_to_create = analysis_results.get("entities_to_create", {})
    tasks_to_create = entities_to_create.get("tasks", [])
    
    # If no thread content, log warning
    if not thread_content:
        logging.warning(f"No thread content available for owner determination in thread {thread_id}")
        thread_content = "No thread content available for analysis."

    prompt = f"""You are a HubSpot API assistant. Analyze this email thread to identify the deal owner and task owners.

Email thread content:
{thread_content}

Tasks to be created:
{json.dumps(tasks_to_create, indent=2)}

IMPORTANT: You must respond with ONLY a valid JSON object. No HTML, no explanations, no markdown formatting.

Steps:

1. Parse the Deal Owner and Task Owners from the email thread.
2. Invoke get_all_owners Tool to retrieve the list of available owners.
3. Parse and validate the deal owner against the available owners list:
    - If deal owner is NOT specified at all:
        - Default to: "liji"
        - Message: "No deal owner specified, so assigning to default owner liji."
    - If deal owner IS specified but NOT found in available owners list:
        - Default to: "liji"
        - Message: "The specified deal owner '[parsed_owner]' is not valid, so assigning to default owner liji."
    - If deal owner IS specified and IS found in available owners list:
        - Use the matched owner (with correct casing from the available owners list)
        - Message: "Deal owner specified as [matched_owner_name]"
4. Parse and validate each task owner against the available owners list:
    - Identify all tasks and their respective owners from the email content.
    - For each task in the tasks to be created:
        - If task owner is NOT specified for a task:
            - Default to: "liji"
            - Message: "No task owner specified for task [task_index], so assigning to default owner liji."
        - If task owner IS specified but NOT found in available owners list:
            - Default to: "liji"
            - Message: "The specified task owner '[parsed_owner]' for task [task_index] is not valid, so assigning to default owner liji."
        - If task owner IS specified and IS found in available owners list:
            - Use the matched owner (with correct casing from the available owners list)
            - Message: "Task owner for task [task_index] specified as [matched_owner_name]"
5. Return a list of task owners with their validation details for ALL tasks to be created.

Return this exact JSON structure:
{{
    "deal_owner_id": "159242778",
    "deal_owner_name": "liji",
    "deal_owner_message": "No deal owner specified, so assigning to default owner liji." OR "The specified deal owner '[parsed_owner]' is not valid, so assigning to default owner liji." OR "Deal owner specified as [name]",
    "task_owners": [
        {{
            "task_index": 1,
            "task_owner_id": "159242778",
            "task_owner_name": "liji",
            "task_owner_message": "No task owner specified for task [task_index], so assigning to default owner liji." OR "The specified task owner '[parsed_owner]' for task [task_index] is not valid, so assigning to default owner liji." OR "Task owner for task [task_index] specified as [name]"
        }}
    ],
    "all_owners_table": [
        {{
            "id": "owner_id",
            "name": "owner_name",
            "email": "owner_email"
        }}
    ]
}}

RESPOND WITH ONLY THE JSON OBJECT - NO OTHER TEXT."""

    response = get_ai_response(prompt, expect_json=True)
    logging.info(f"Raw AI response for owner: {response[:1000]}...")

    try:
        parsed_json = json.loads(response.strip())
        ti.xcom_push(key="owner_info", value=parsed_json)

        # Store in thread context
        contexts = get_thread_context()
        if thread_id not in contexts:
            contexts[thread_id] = {}
        contexts[thread_id]["owner_info"] = parsed_json
        contexts[thread_id]["prompt_owner"] = prompt
        contexts[thread_id]["response_owner"] = response
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)

        logging.info(f"Owner determination completed for thread {thread_id}")
        logging.info(f"Deal owner: {parsed_json.get('deal_owner_name', 'unknown')}")
        logging.info(f"Task owners: {len(parsed_json.get('task_owners', []))}")

    except Exception as e:
        logging.error(f"Error processing owner AI response: {e}")
        default_owner = {
            "deal_owner_id": "159242778",
            "deal_owner_name": "liji",
            "deal_owner_message": f"Error occurred: {str(e)}, so assigning to default owner liji.",
            "task_owners": [],
            "all_owners_table": []
        }
        ti.xcom_push(key="owner_info", value=default_owner)

TASK_THRESHOLD = 15

def check_task_threshold(ti, **context):
    """Fixed version that properly extracts task information"""
    analysis_results = ti.xcom_pull(key="analysis_results", default={})
    thread_id = context['dag_run'].conf.get("thread_id")
    
    # Get entities to create from analysis results
    entities_to_create = analysis_results.get("entities_to_create", {})
    tasks_to_create = entities_to_create.get("tasks", [])
    
    if not tasks_to_create:
        logging.info("No tasks to create, skipping task threshold check")
        ti.xcom_push(key="task_warnings", value=[])
        ti.xcom_push(key="task_threshold_info", value={
            "task_threshold_results": {
                "dates_checked": [],
                "total_warnings": 0,
                "threshold_limit": TASK_THRESHOLD
            },
            "extracted_dates": [],
            "warnings": []
        })
        return []
    
    # Get thread content from DAG run conf
    conf = context["dag_run"].conf
    full_thread_history = conf.get("full_thread_history", [])
    
    # Build thread content from full history
    thread_content = ""
    for idx, email in enumerate(full_thread_history, 1):
        content = email.get("content", "").strip()
        if content:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(content, "html.parser")
            clean_content = soup.get_text(separator=" ", strip=True)
            sender = email['headers'].get('From', 'Unknown')
            is_from_bot = email.get('from_bot', False)
            
            if is_from_bot:
                thread_content += f"### Bot Message {idx} (From: {sender})\n{clean_content}\n\n"
            else:
                thread_content += f"### User Message {idx} (From: {sender})\n{clean_content}\n\n"
    
    # Get owner info if available
    owner_info = ti.xcom_pull(key="owner_info", default={})
    task_owners = owner_info.get('task_owners', [])
    
    # Map tasks to their owners and due dates
    task_owner_mapping = []
    for idx, task in enumerate(tasks_to_create, 1):
        task_details = task.get('task_details', '')
        due_date = task.get('due_date', '')
        task_owner_id = task.get('task_owner_id', '159242778')
        task_owner_name = task.get('task_owner_name', 'liji')
        
        # Find the corresponding task owner info if available
        matching_owner = next((owner for owner in task_owners if owner.get('task_index') == idx), None)
        if matching_owner:
            task_owner_id = matching_owner.get('task_owner_id', '159242778')
            task_owner_name = matching_owner.get('task_owner_name', 'liji')
        
        task_owner_mapping.append({
            'task_index': idx,
            'task_details': task_details,
            'due_date': due_date,
            'task_owner_id': task_owner_id,
            'task_owner_name': task_owner_name
        })

    if not thread_content:
        logging.warning(f"No thread content available for task threshold check in thread {thread_id}")
        thread_content = "No thread content available for analysis."

    prompt = f"""You are a HubSpot API assistant. Check task volume thresholds based on this email thread.

Email thread content:
{thread_content}

Task Owner Mapping:
{json.dumps(task_owner_mapping, indent=2)}

IMPORTANT: You must respond with ONLY a valid JSON object. No HTML, no explanations, no markdown formatting.

Steps to follow:
1. For each task in the Task Owner Mapping, extract the due date and assigned owner.
2. For each unique owner and due date combination:
   - Invoke search_tasks with GTE and LTE set to the specified due date and owner name.
   - Count total tasks for that owner on that date.
   - Check if the task count exceeds the threshold of {TASK_THRESHOLD} tasks per day.
3. Generate warnings for dates that exceed the threshold for each owner.

Return this exact JSON structure:
{{
    "task_threshold_results": {{
        "dates_checked": [
            {{
                "date": "YYYY-MM-DD",
                "owner_id": "owner_id",
                "owner_name": "owner_name",
                "existing_task_count": 0,
                "exceeds_threshold": false,
                "warning": "High task volume: X tasks on YYYY-MM-DD for owner_name" or null
            }}
        ],
        "total_warnings": 0,
        "threshold_limit": {TASK_THRESHOLD}
    }},
    "extracted_dates": [
        "YYYY-MM-DD"
    ],
    "warnings": [
        "Warning message if threshold exceeded"
    ]
}}

Fill in ALL fields. Use empty arrays [] for no results.
For dates, use YYYY-MM-DD format.
If no dates found in email, check today's date as default for each owner.

RESPOND WITH ONLY THE JSON OBJECT - NO OTHER TEXT."""

    response = get_ai_response(prompt, expect_json=True)
    logging.info(f"Raw AI response for task threshold: {response[:1000]}...")

    try:
        parsed_json = json.loads(response.strip())

        # Extract warnings from the response
        warnings = parsed_json.get("warnings", [])

        ti.xcom_push(key="task_warnings", value=warnings)
        ti.xcom_push(key="task_threshold_info", value=parsed_json)

        # Store in thread context
        contexts = get_thread_context()
        if thread_id not in contexts:
            contexts[thread_id] = {}
        contexts[thread_id]["task_warnings"] = warnings
        contexts[thread_id]["task_threshold_info"] = parsed_json
        contexts[thread_id]["prompt_task_threshold"] = prompt
        contexts[thread_id]["response_task_threshold"] = response

        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)

        logging.info(f"Task threshold check completed with {len(warnings)} warnings for {len(task_owner_mapping)} tasks")

    except Exception as e:
        logging.error(f"Error processing task threshold AI response: {e}")
        default_warnings = []
        default_response = {
            "task_threshold_results": {
                "dates_checked": [],
                "total_warnings": 0,
                "threshold_limit": TASK_THRESHOLD
            },
            "extracted_dates": [],
            "warnings": []
        }

        ti.xcom_push(key="task_warnings", value=default_warnings)
        ti.xcom_push(key="task_threshold_info", value=default_response)

        # Store error case in context
        contexts = get_thread_context()
        if thread_id not in contexts:
            contexts[thread_id] = {}
        contexts[thread_id]["task_warnings"] = default_warnings

        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)

    return warnings

def create_contacts(ti, **context):
    analysis_results = ti.xcom_pull(key="analysis_results")
    thread_id = context['dag_run'].conf.get("thread_id")
    to_create_contacts = analysis_results.get("entities_to_create", {}).get("contacts", [])

    if not to_create_contacts:
        logging.info("No contacts to create, skipping.")
        ti.xcom_push(key="created_contacts", value=[])
        ti.xcom_push(key="contacts_errors", value=[])
        return []

    prompt = f"""Create contacts in HubSpot based on the provided details.

Details to create:
{json.dumps(to_create_contacts, indent=2)}

IMPORTANT: Respond with ONLY a valid JSON object.

Steps:
1. For each contact detail object, invoke create_contact with the properties.
2. Collect the created IDs, contact name, email, phone, address and display in tabular format. If any details missing, leave it blank in table.

Return JSON:
{{
    "created_contacts": [{{"id": "123", "details": {{ "firstname": "...", "lastname": "...", "email": "...", "phone": "...", "address": "...", "jobtitle": "..."}}}} ...],
    "errors": ["Error message 1", "Error message 2"],
    "error": null
}}

If error, set error message and include individual errors in the errors array."""

    response = get_ai_response(prompt, expect_json=True)

    try:
        parsed = json.loads(response)
        created = parsed.get("created_contacts", [])
        errors = parsed.get("errors", [])

        ti.xcom_push(key="created_contacts", value=created)
        ti.xcom_push(key="contacts_errors", value=errors)

        contexts = get_thread_context()
        if thread_id not in contexts:
            contexts[thread_id] = {}
        contexts[thread_id]["created_contacts"] = created
        contexts[thread_id]["contacts_errors"] = errors
        contexts[thread_id]["create_contacts_prompt"] = prompt
        contexts[thread_id]["create_contacts_response"] = response
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)

        logging.info(f"Created {len(created)} contacts with {len(errors)} errors")
    except Exception as e:
        logging.error(f"Error creating contacts: {e}")
        ti.xcom_push(key="created_contacts", value=[])
        ti.xcom_push(key="contacts_errors", value=[str(e)])
        contexts = get_thread_context()
        if thread_id not in contexts:
            contexts[thread_id] = {}
        contexts[thread_id]["create_contacts_error"] = str(e)
        contexts[thread_id]["contacts_errors"] = [str(e)]
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)

    return created

def create_companies(ti, **context):
    analysis_results = ti.xcom_pull(key="analysis_results")
    thread_id = context['dag_run'].conf.get("thread_id")
    to_create_companies = analysis_results.get("entities_to_create", {}).get("companies", [])

    if not to_create_companies:
        logging.info("No companies to create, skipping.")
        ti.xcom_push(key="created_companies", value=[])
        ti.xcom_push(key="companies_errors", value=[])
        return []

    prompt = f"""Create companies in HubSpot based on the provided details.

Details to create:
{json.dumps(to_create_companies, indent=2)}

IMPORTANT: Respond with ONLY a valid JSON object.

Steps:
1. For each company detail object, invoke create_company with the properties.
2. In the properties the `type` should be one of "PARTNER", "PROSPECT". If not specified, set to "PROSPECT".
3. Collect the created company id, company name, domain, state, city, country, phone, type and display in tabular format. If any details not found, show as blank in table.

Return JSON:
{{
    "created_companies": [{{"id": "123", "details": {{ "name": "...", "domain": "...", "address": "...", "city": "...", "state": "...", "zip": "...", "country": "...", "phone": "...", "description": "...", "type": "..."}}}} ...],
    "errors": ["Error message 1", "Error message 2"],
    "error": null
}}

If error, set error message and include individual errors in the errors array."""

    response = get_ai_response(prompt, expect_json=True)

    try:
        parsed = json.loads(response)
        created = parsed.get("created_companies", [])
        errors = parsed.get("errors", [])

        ti.xcom_push(key="created_companies", value=created)
        ti.xcom_push(key="companies_errors", value=errors)

        contexts = get_thread_context()
        if thread_id not in contexts:
            contexts[thread_id] = {}
        contexts[thread_id]["created_companies"] = created
        contexts[thread_id]["companies_errors"] = errors
        contexts[thread_id]["create_companies_prompt"] = prompt
        contexts[thread_id]["create_companies_response"] = response
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)

        logging.info(f"Created {len(created)} companies with {len(errors)} errors")
    except Exception as e:
        logging.error(f"Error creating companies: {e}")
        ti.xcom_push(key="created_companies", value=[])
        ti.xcom_push(key="companies_errors", value=[str(e)])
        contexts = get_thread_context()
        if thread_id not in contexts:
            contexts[thread_id] = {}
        contexts[thread_id]["create_companies_error"] = str(e)
        contexts[thread_id]["companies_errors"] = [str(e)]
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)

    return created

def create_deals(ti, **context):
    analysis_results = ti.xcom_pull(key="analysis_results")
    thread_id = context['dag_run'].conf.get("thread_id")
    to_create_deals = analysis_results.get("entities_to_create", {}).get("deals", [])
    
    # Get the latest user response from the context
    full_thread_history = context['dag_run'].conf.get("full_thread_history", [])
    latest_user_response = ""
    for email in reversed(full_thread_history):
        if not email.get('from_bot', True):  # Find latest non-bot email
            if email.get("content"):
                soup = BeautifulSoup(email.get("content", ""), "html.parser")
                latest_user_response = soup.get_text(separator=" ", strip=True)
                break

    if not to_create_deals:
        logging.info("No deals to create, skipping.")
        ti.xcom_push(key="created_deals", value=[])
        ti.xcom_push(key="deals_errors", value=[])
        return []

    prompt = f"""Create deals in HubSpot based on the provided details and the user's response.

Latest User Response:
{latest_user_response}

Details to create:
{json.dumps(to_create_deals, indent=2)}

IMPORTANT: Respond with ONLY a valid JSON object.

Critical Deal Naming Rules:
1. Extract Client Name from latest user response or available details
2. Determine if it's a direct deal or partner deal from context
3. For direct deals: format as "<Client Name>-<Deal Name>"
4. For partner deals: format as "<Partner Name>-<Client Name>-<Deal Name>"
5. If Deal Name not specified, create descriptive name based on product/service mentioned
6. Never use generic names - must reflect actual client/partner and deal purpose
7. Preserve any specific deal amount, close date, or stage information provided

Steps:
1. Analyze user response to extract client/partner names and deal details
2. Apply naming convention strictly for each deal
3. For each deal, invoke create_deal with the properties
4. Collect created deal id, properly formatted deal name, label name, amount, close date, owner

Return JSON:
{{
    "created_deals": [{{
        "id": "123", 
        "details": {{ 
            "dealName": "ClientName-DealPurpose",  // or "PartnerName-ClientName-DealPurpose"
            "dealLabelName": "...",
            "dealAmount": "...",
            "closeDate": "...",
            "dealOwnerName": "..."
        }}
    }}],
    "errors": ["Error message 1", "Error message 2"],
    "error": null
}}

If error, set error message and include individual errors in the errors array."""

    response = get_ai_response(prompt, expect_json=True)

    try:
        parsed = json.loads(response)
        created = parsed.get("created_deals", [])
        errors = parsed.get("errors", [])

        # Validate deal names follow convention
        for deal in created:
            deal_name = deal.get("details", {}).get("dealName", "")
            if not deal_name or deal_name.count("-") < 1:
                error_msg = f"Deal {deal.get('id', 'unknown')} has invalid name format: {deal_name}"
                logging.error(error_msg)
                errors.append(error_msg)

        ti.xcom_push(key="created_deals", value=created)
        ti.xcom_push(key="deals_errors", value=errors)

        contexts = get_thread_context()
        if thread_id not in contexts:
            contexts[thread_id] = {}
        contexts[thread_id]["created_deals"] = created
        contexts[thread_id]["deals_errors"] = errors
        contexts[thread_id]["create_deals_prompt"] = prompt
        contexts[thread_id]["create_deals_response"] = response
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)

        logging.info(f"Created {len(created)} deals with {len(errors)} errors")
        for deal in created:
            logging.info(f"Created deal: {deal.get('details', {}).get('dealName', 'Unknown')}")
            
    except Exception as e:
        error_msg = f"Error creating deals: {str(e)}"
        logging.error(error_msg)
        ti.xcom_push(key="created_deals", value=[])
        ti.xcom_push(key="deals_errors", value=[error_msg])
        contexts = get_thread_context()
        if thread_id not in contexts:
            contexts[thread_id] = {}
        contexts[thread_id]["create_deals_error"] = error_msg
        contexts[thread_id]["deals_errors"] = [error_msg]
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)

    return created

def create_meetings(ti, **context):
    analysis_results = ti.xcom_pull(key="analysis_results")
    thread_id = context['dag_run'].conf.get("thread_id")
    to_create_meetings = analysis_results.get("entities_to_create", {}).get("meetings", [])

    if not to_create_meetings:
        logging.info("No meetings to create, skipping.")
        ti.xcom_push(key="created_meetings", value=[])
        ti.xcom_push(key="meetings_errors", value=[])
        return []
    
    prompt = f"""Create meetings in HubSpot based on the provided details.

Meeting details:
{json.dumps(to_create_meetings, indent=2)}

IMPORTANT: Respond with ONLY a valid JSON object.

Steps:
1. For each meeting, invoke create_meetings with the properties (date, attendees, summary).
2. Collect the created ID, Title, Start Time (EST), End Time (EST), Location, Outcome in tabular format.
3. Always use `hs_timestamp` in YYYY-MM-DDTHH:MM:SSZ format while creating meetings.
Return JSON:
{{
    "created_meetings": [{{"id": "123", "details": {{ "meeting_title": "...", "start_time": "...", "end_time": "...", "location": "...", "outcome": "...", "timestamp": "...", "attendees": [], "meeting_type": "..."}}}} ...],
    "errors": ["Error message 1", "Error message 2"],
    "error": null
}}

If error, set error message and include individual errors in the errors array."""

    response = get_ai_response(prompt, expect_json=True)

    try:
        parsed = json.loads(response)
        created = parsed.get("created_meetings", [])
        errors = parsed.get("errors", [])

        ti.xcom_push(key="created_meetings", value=created)
        ti.xcom_push(key="meetings_errors", value=errors)

        contexts = get_thread_context()
        if thread_id not in contexts:
            contexts[thread_id] = {}
        contexts[thread_id]["created_meetings"] = created
        contexts[thread_id]["meetings_errors"] = errors
        contexts[thread_id]["create_meetings_prompt"] = prompt
        contexts[thread_id]["create_meetings_response"] = response
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)

        logging.info(f"Created {len(created)} meetings with {len(errors)} errors")
    except Exception as e:
        logging.error(f"Error creating meetings: {e}")
        ti.xcom_push(key="created_meetings", value=[])
        ti.xcom_push(key="meetings_errors", value=[str(e)])
        contexts = get_thread_context()
        if thread_id not in contexts:
            contexts[thread_id] = {}
        contexts[thread_id]["create_meetings_error"] = str(e)
        contexts[thread_id]["meetings_errors"] = [str(e)]
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)

    return created

def create_notes(ti, **context):
    analysis_results = ti.xcom_pull(key="analysis_results")
    thread_id = context['dag_run'].conf.get("thread_id")
    to_create_notes = analysis_results.get("entities_to_create", {}).get("notes", [])

    if not to_create_notes:
        logging.info("No notes to create, skipping.")
        ti.xcom_push(key="created_notes", value=[])
        ti.xcom_push(key="notes_errors", value=[])
        return []

    prompt = f"""Create notes in HubSpot based on the provided details.

Notes:
{json.dumps(to_create_notes, indent=2)}

IMPORTANT: Respond with ONLY a valid JSON object.

Steps:
1. For each note, invoke create_notes with the content.
2. Format note content as: "Speaker: [name] mentioned [note_content]"
2. Collect the created Note id, Note body, last modified date in tabular format.
3. Always use `hs_timestamp` in YYYY-MM-DDTHH:MM:SSZ format while creating notes.
Return JSON:
{{
    "created_notes": [{{"id": "123", "details": {{ "note_content": "...", "timestamp": "..."}}}} ...],
    "errors": ["Error message 1", "Error message 2"],
    "error": null
}}

If error, set error message and include individual errors in the errors array."""  # Existing prompt

    response = get_ai_response(prompt, expect_json=True)

    try:
        parsed = json.loads(response)
        created = parsed.get("created_notes", [])
        errors = parsed.get("errors", [])

        ti.xcom_push(key="created_notes", value=created)
        ti.xcom_push(key="notes_errors", value=errors)

        contexts = get_thread_context()
        # Initialize thread_id in contexts if it doesn’t exist
        if thread_id not in contexts:
            contexts[thread_id] = {}
        contexts[thread_id]["created_notes"] = created
        contexts[thread_id]["notes_errors"] = errors
        contexts[thread_id]["create_notes_prompt"] = prompt
        contexts[thread_id]["create_notes_response"] = response
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)

        logging.info(f"Created {len(created)} notes with {len(errors)} errors")
    except Exception as e:
        logging.error(f"Error creating notes: {e}")
        ti.xcom_push(key="created_notes", value=[])
        ti.xcom_push(key="notes_errors", value=[str(e)])
        contexts = get_thread_context()
        # Initialize thread_id in contexts if it doesn’t exist
        if thread_id not in contexts:
            contexts[thread_id] = {}
        contexts[thread_id]["create_notes_error"] = str(e)
        contexts[thread_id]["notes_errors"] = [str(e)]
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)

    return created

def create_tasks(ti, **context):
    analysis_results = ti.xcom_pull(key="analysis_results")
    thread_id = context['dag_run'].conf.get("thread_id")
    to_create_tasks = analysis_results.get("entities_to_create", {}).get("tasks", [])

    if not to_create_tasks:
        logging.info("No tasks to create, skipping.")
        ti.xcom_push(key="created_tasks", value=[])
        ti.xcom_push(key="tasks_errors", value=[])
        return []

    prompt = f"""Create tasks in HubSpot based on the provided details.

Tasks:
{json.dumps(to_create_tasks, indent=2)}

IMPORTANT: Respond with ONLY a valid JSON object.

Steps:
1. For each task, invoke create_tasks with hs_timestamp, hs_task_body, hs_task_subject, hs_task_status, hs_task_priority, hs_task_type, hubspot_owner_id.
2. Collect the created Task id, task body, last modified date, due date, task owner name in tabular format.
3. Always use `hs_timestamp` in YYYY-MM-DDTHH:MM:SSZ format while creating tasks.
4. Important task_owner_name should be always the name. For example rekha is the owner then rekha should be displayed in the owner name field and not the id 8765.
Return JSON:
{{
    "created_tasks": [{{"id": "123", "details": {{ "task_details": "...", "task_owner_name": "...", "task_owner_id": "...", "due_date": "...", "priority": "..."}}}} ...],
    "errors": ["Error message 1", "Error message 2"],
    "error": null
}}

If error, set error message and include individual errors in the errors array."""

    response = get_ai_response(prompt, expect_json=True)

    try:
        parsed = json.loads(response)
        created = parsed.get("created_tasks", [])
        errors = parsed.get("errors", [])

        ti.xcom_push(key="created_tasks", value=created)
        ti.xcom_push(key="tasks_errors", value=errors)

        contexts = get_thread_context()
        contexts[thread_id]["created_tasks"] = created
        contexts[thread_id]["tasks_errors"] = errors
        contexts[thread_id]["create_tasks_prompt"] = prompt
        contexts[thread_id]["create_tasks_response"] = response
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)

        logging.info(f"Created {len(created)} tasks with {len(errors)} errors")
    except Exception as e:
        logging.error(f"Error creating tasks: {e}")
        ti.xcom_push(key="created_tasks", value=[])
        ti.xcom_push(key="tasks_errors", value=[str(e)])
        contexts = get_thread_context()
        contexts[thread_id]["create_tasks_error"] = str(e)
        contexts[thread_id]["tasks_errors"] = [str(e)]
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)

    return created

def update_contacts(ti, **context):
    analysis_results = ti.xcom_pull(key="analysis_results")
    thread_id = context['dag_run'].conf.get("thread_id")
    to_update_contacts = analysis_results.get("entities_to_update", {}).get("contacts", [])

    if not to_update_contacts:
        logging.info("No contacts to update, skipping.")
        ti.xcom_push(key="updated_contacts", value=[])
        ti.xcom_push(key="contacts_update_errors", value=[])
        return []

    prompt = f"""Update contacts in HubSpot based on the provided details.

Details to update:
{json.dumps(to_update_contacts, indent=2)}

IMPORTANT: Respond with ONLY a valid JSON object.

Steps:
1. For each contact, invoke update_contact with the id and changes.
2. Collect the updated IDs and details in tabular format. If any details missing, leave it blank in table.

Return JSON:
{{
    "updated_contacts": [{{"id": "123", "details": {{ "firstname": "...", "lastname": "...", "email": "...", "phone": "...", "address": "...", "jobtitle": "..."}}}} ...],
    "errors": ["Error message 1", "Error message 2"],
    "error": null
}}

If error, set error message and include individual errors in the errors array."""

    response = get_ai_response(prompt, expect_json=True)

    try:
        parsed = json.loads(response)
        updated = parsed.get("updated_contacts", [])
        errors = parsed.get("errors", [])

        ti.xcom_push(key="updated_contacts", value=updated)
        ti.xcom_push(key="contacts_update_errors", value=errors)

        contexts = get_thread_context()
        contexts[thread_id]["updated_contacts"] = updated
        contexts[thread_id]["contacts_update_errors"] = errors
        contexts[thread_id]["update_contacts_prompt"] = prompt
        contexts[thread_id]["update_contacts_response"] = response
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)

        logging.info(f"Updated {len(updated)} contacts with {len(errors)} errors")
    except Exception as e:
        logging.error(f"Error updating contacts: {e}")
        ti.xcom_push(key="updated_contacts", value=[])
        ti.xcom_push(key="contacts_update_errors", value=[str(e)])
        contexts = get_thread_context()
        contexts[thread_id]["update_contacts_error"] = str(e)
        contexts[thread_id]["contacts_update_errors"] = [str(e)]
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)

    return updated

def update_companies(ti, **context):
    analysis_results = ti.xcom_pull(key="analysis_results")
    thread_id = context['dag_run'].conf.get("thread_id")
    to_update_companies = analysis_results.get("entities_to_update", {}).get("companies", [])

    if not to_update_companies:
        logging.info("No companies to update, skipping.")
        ti.xcom_push(key="updated_companies", value=[])
        ti.xcom_push(key="companies_update_errors", value=[])
        return []

    prompt = f"""Update companies in HubSpot based on the provided details.

Details to update:
{json.dumps(to_update_companies, indent=2)}

IMPORTANT: Respond with ONLY a valid JSON object.

Steps:
1. For each company, invoke update_company with the id and changes.
2. Collect the updated company id, company name, domain, state, city, country, phone, type and display in tabular format. If any details not found, show as blank in table.

Return JSON:
{{
    "updated_companies": [{{"id": "123", "details": {{ "name": "...", "domain": "...", "address": "...", "city": "...", "state": "...", "zip": "...", "country": "...", "phone": "...", "description": "...", "type": "..."}}}} ...],
    "errors": ["Error message 1", "Error message 2"],
    "error": null
}}

If error, set error message and include individual errors in the errors array."""

    response = get_ai_response(prompt, expect_json=True)

    try:
        parsed = json.loads(response)
        updated = parsed.get("updated_companies", [])
        errors = parsed.get("errors", [])

        ti.xcom_push(key="updated_companies", value=updated)
        ti.xcom_push(key="companies_update_errors", value=errors)

        contexts = get_thread_context()
        contexts[thread_id]["updated_companies"] = updated
        contexts[thread_id]["companies_update_errors"] = errors
        contexts[thread_id]["update_companies_prompt"] = prompt
        contexts[thread_id]["update_companies_response"] = response
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)

        logging.info(f"Updated {len(updated)} companies with {len(errors)} errors")
    except Exception as e:
        logging.error(f"Error updating companies: {e}")
        ti.xcom_push(key="updated_companies", value=[])
        ti.xcom_push(key="companies_update_errors", value=[str(e)])
        contexts = get_thread_context()
        contexts[thread_id]["update_companies_error"] = str(e)
        contexts[thread_id]["companies_update_errors"] = [str(e)]
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)

    return updated

def update_deals(ti, **context):
    analysis_results = ti.xcom_pull(key="analysis_results")
    thread_id = context['dag_run'].conf.get("thread_id")
    to_update_deals = analysis_results.get("entities_to_update", {}).get("deals", [])

    if not to_update_deals:
        logging.info("No deals to update, skipping.")
        ti.xcom_push(key="updated_deals", value=[])
        ti.xcom_push(key="deals_update_errors", value=[])
        return []

    prompt = f"""Update deals in HubSpot based on the provided details.

Details to update:
{json.dumps(to_update_deals, indent=2)}

IMPORTANT: Respond with ONLY a valid JSON object.

Steps:
1. For each deal, invoke update_deal with the id and changes.
2. Collect the updated deal id, deal name, deal label name, close date, deal owner name in tabular format. If any details not found, show as blank in table.

Return JSON:
{{
    "updated_deals": [{{"id": "123", "details": {{ "dealName": "...", "dealLabelName": "...", "dealAmount": "...", "closeDate": "...", "dealOwnerName": "..."}}}} ...],
    "errors": ["Error message 1", "Error message 2"],
    "error": null
}}

If error, set error message and include individual errors in the errors array."""

    response = get_ai_response(prompt, expect_json=True)

    try:
        parsed = json.loads(response)
        updated = parsed.get("updated_deals", [])
        errors = parsed.get("errors", [])

        ti.xcom_push(key="updated_deals", value=updated)
        ti.xcom_push(key="deals_update_errors", value=errors)

        contexts = get_thread_context()
        contexts[thread_id]["updated_deals"] = updated
        contexts[thread_id]["deals_update_errors"] = errors
        contexts[thread_id]["update_deals_prompt"] = prompt
        contexts[thread_id]["update_deals_response"] = response
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)

        logging.info(f"Updated {len(updated)} deals with {len(errors)} errors")
    except Exception as e:
        logging.error(f"Error updating deals: {e}")
        ti.xcom_push(key="updated_deals", value=[])
        ti.xcom_push(key="deals_update_errors", value=[str(e)])
        contexts = get_thread_context()
        contexts[thread_id]["update_deals_error"] = str(e)
        contexts[thread_id]["deals_update_errors"] = [str(e)]
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)

    return updated

def update_meetings(ti, **context):
    analysis_results = ti.xcom_pull(key="analysis_results")
    thread_id = context['dag_run'].conf.get("thread_id")
    to_update_meetings = analysis_results.get("entities_to_update", {}).get("meetings", [])

    if not to_update_meetings:
        logging.info("No meetings to update, skipping.")
        ti.xcom_push(key="updated_meetings", value=[])
        ti.xcom_push(key="meetings_update_errors", value=[])
        return []

    prompt = f"""Update meetings in HubSpot based on the provided details.

Meeting details to update:
{json.dumps(to_update_meetings, indent=2)}

IMPORTANT: Respond with ONLY a valid JSON object.

Steps:
1. For each meeting, invoke update_meeting with the id and changes.
2. Collect the updated ID, Title, Start Time (EST), End Time (EST), Location, Outcome in tabular format.
3. Always use `hs_timestamp` in YYYY-MM-DDTHH:MM:SSZ format while updating meetings.
Return JSON:
{{
    "updated_meetings": [{{"id": "123", "details": {{ "meeting_title": "...", "start_time": "...", "end_time": "...", "location": "...", "outcome": "...", "timestamp": "...", "attendees": [], "meeting_type": "..."}}}} ...],
    "errors": ["Error message 1", "Error message 2"],
    "error": null
}}

If error, set error message and include individual errors in the errors array."""

    response = get_ai_response(prompt, expect_json=True)

    try:
        parsed = json.loads(response)
        updated = parsed.get("updated_meetings", [])
        errors = parsed.get("errors", [])

        ti.xcom_push(key="updated_meetings", value=updated)
        ti.xcom_push(key="meetings_update_errors", value=errors)

        contexts = get_thread_context()
        contexts[thread_id]["updated_meetings"] = updated
        contexts[thread_id]["meetings_update_errors"] = errors
        contexts[thread_id]["update_meetings_prompt"] = prompt
        contexts[thread_id]["update_meetings_response"] = response
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)

        logging.info(f"Updated {len(updated)} meetings with {len(errors)} errors")
    except Exception as e:
        logging.error(f"Error updating meetings: {e}")
        ti.xcom_push(key="updated_meetings", value=[])
        ti.xcom_push(key="meetings_update_errors", value=[str(e)])
        contexts = get_thread_context()
        contexts[thread_id]["update_meetings_error"] = str(e)
        contexts[thread_id]["meetings_update_errors"] = [str(e)]
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)

    return updated

def update_notes(ti, **context):
    analysis_results = ti.xcom_pull(key="analysis_results")
    thread_id = context['dag_run'].conf.get("thread_id")
    to_update_notes = analysis_results.get("entities_to_update", {}).get("notes", [])

    if not to_update_notes:
        logging.info("No notes to update, skipping.")
        ti.xcom_push(key="updated_notes", value=[])
        ti.xcom_push(key="notes_update_errors", value=[])
        return []

    prompt = f"""Update notes in HubSpot based on the provided details.

Notes to update:
{json.dumps(to_update_notes, indent=2)}

IMPORTANT: Respond with ONLY a valid JSON object.

Steps:
1. For each note, invoke update_note with the id and changes.
2. Collect the updated Note id, Note body, last modified date in tabular format.
3. Always use `hs_timestamp` in YYYY-MM-DDTHH:MM:SSZ format while updating notes.
Return JSON:
{{
    "updated_notes": [{{"id": "123", "details": {{ "note_content": "...", "timestamp": "..."}}}} ...],
    "errors": ["Error message 1", "Error message 2"],
    "error": null
}}

If error, set error message and include individual errors in the errors array."""

    response = get_ai_response(prompt, expect_json=True)

    try:
        parsed = json.loads(response)
        updated = parsed.get("updated_notes", [])
        errors = parsed.get("errors", [])

        ti.xcom_push(key="updated_notes", value=updated)
        ti.xcom_push(key="notes_update_errors", value=errors)

        contexts = get_thread_context()
        contexts[thread_id]["updated_notes"] = updated
        contexts[thread_id]["notes_update_errors"] = errors
        contexts[thread_id]["update_notes_prompt"] = prompt
        contexts[thread_id]["update_notes_response"] = response
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)

        logging.info(f"Updated {len(updated)} notes with {len(errors)} errors")
    except Exception as e:
        logging.error(f"Error updating notes: {e}")
        ti.xcom_push(key="updated_notes", value=[])
        ti.xcom_push(key="notes_update_errors", value=[str(e)])
        contexts = get_thread_context()
        contexts[thread_id]["update_notes_error"] = str(e)
        contexts[thread_id]["notes_update_errors"] = [str(e)]
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)

    return updated

def update_tasks(ti, **context):
    from json import JSONDecodeError
    analysis_results = ti.xcom_pull(key="analysis_results")
    thread_id = context['dag_run'].conf.get("thread_id")
    to_update_tasks = analysis_results.get("entities_to_update", {}).get("tasks", [])

    if not to_update_tasks:
        logging.info("No tasks to update, skipping.")
        ti.xcom_push(key="updated_tasks", value=[])
        ti.xcom_push(key="tasks_update_errors", value=[])
        return []

    # Get current task details from selected_entities to preserve original information
    selected_entities = analysis_results.get("selected_entities", {})
    current_tasks = selected_entities.get("tasks", [])
    
    # Create a mapping of task IDs to their current details
    task_details_map = {}
    for task in current_tasks:
        task_id = task.get("taskId") or task.get("id")
        if task_id:
            task_details_map[task_id] = {
                "task_details": task.get("task_details", ""),
                "task_owner_name": task.get("task_owner_name", ""),
                "task_owner_id": task.get("task_owner_id", ""),
                "due_date": task.get("due_date", ""),
                "priority": task.get("priority", "")
            }

    # Prepare the update prompt with explicit API call format
    prompt = f"""Update tasks in HubSpot using the update_task function. You must follow the exact API format.

Tasks to update:
{json.dumps(to_update_tasks, indent=2)}

Current task details (preserve these unless specifically updating):
{json.dumps(task_details_map, indent=2)}

CRITICAL INSTRUCTIONS:
1. For each task update, call update_task with this EXACT format:
   
   update_task(task_id, {{
     "properties": {{
       "hs_timestamp": "YYYY-MM-DDTHH:MM:SSZ",
       "hs_task_body": "preserved_original_task_description",
       "hs_task_subject": "preserved_original_task_description", 
       "hs_task_priority": "HIGH",
       "hs_task_status": "NOT_STARTED",
       "hubspot_owner_id": "owner_id_number"
     }}
   }})

2. PRESERVE original task descriptions from task_details_map
3. Convert due_date changes to hs_timestamp format
4. Use the task's existing owner ID
5. After each update, call search_tasks(task_id) to get updated details

EXAMPLE for task 197051476705:
- Current details: "Draft a one-page pilot outline for shipment tracking..."  
- If updating due_date to 2025-09-30, call:
  
  update_task("197051476705", {{
    "properties": {{
      "hs_timestamp": "2025-09-30T00:00:00Z",
      "hs_task_body": "Draft a one-page pilot outline for shipment tracking and cost reporting improvements tailored to BlueHorizon.",
      "hs_task_subject": "Draft a one-page pilot outline for shipment tracking and cost reporting improvements tailored to BlueHorizon.",
      "hs_task_priority": "HIGH", 
      "hs_task_status": "NOT_STARTED",
      "hubspot_owner_id": "159242778"
    }}
  }})

Return ONLY this JSON format:
{{
  "updated_tasks": [{{
    "id": "task_id",
    "details": {{
      "task_details": "original_description_preserved",
      "task_owner_name": "owner_name", 
      "task_owner_id": "owner_id",
      "due_date": "updated_date",
      "priority": "priority_level"
    }}
  }}],
  "errors": [],
  "error": null
}}"""

    try:
        response = get_ai_response(prompt, expect_json=True)
        parsed = json.loads(response)
        updated = parsed.get("updated_tasks", [])
        errors = parsed.get("errors", [])

        # Validate and fix any missing task details
        for task in updated:
            task_id = task.get("id")
            details = task.get("details", {})
            
            if task_id in task_details_map:
                original = task_details_map[task_id]
                
                # Restore original task details if missing or generic
                if not details.get("task_details") or details.get("task_details") in ["INTEGRATION", ""]:
                    details["task_details"] = original.get("task_details", "")
                    logging.info(f"Restored task_details for {task_id}: {details['task_details']}")
                
                # Restore other missing fields
                if not details.get("task_owner_name"):
                    details["task_owner_name"] = original.get("task_owner_name", "")
                if not details.get("task_owner_id"):
                    details["task_owner_id"] = original.get("task_owner_id", "")
                if not details.get("priority"):
                    details["priority"] = original.get("priority", "high")

        ti.xcom_push(key="updated_tasks", value=updated)
        ti.xcom_push(key="tasks_update_errors", value=errors)

        # Save to thread context
        contexts = get_thread_context()
        if thread_id not in contexts:
            contexts[thread_id] = {}
        contexts[thread_id]["updated_tasks"] = updated
        contexts[thread_id]["tasks_update_errors"] = errors
        contexts[thread_id]["update_tasks_prompt"] = prompt
        contexts[thread_id]["update_tasks_response"] = response
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)

        logging.info(f"Updated {len(updated)} tasks with {len(errors)} errors")
        
        # Enhanced logging for debugging
        for task in updated:
            task_id = task.get("id", "unknown")
            task_desc = task.get("details", {}).get("task_details", "NO_DETAILS")[:50]
            due_date = task.get("details", {}).get("due_date", "NO_DATE")
            logging.info(f"Task {task_id}: '{task_desc}...' due: {due_date}")
            
        return updated
        
    except JSONDecodeError as e:
        error_msg = f"Failed to parse AI response: {str(e)}"
        logging.error(error_msg)
        logging.error(f"Raw AI response: {response}")
        
        ti.xcom_push(key="updated_tasks", value=[])
        ti.xcom_push(key="tasks_update_errors", value=[error_msg])
        
        contexts = get_thread_context()
        if thread_id not in contexts:
            contexts[thread_id] = {}
        contexts[thread_id]["update_tasks_error"] = error_msg
        contexts[thread_id]["tasks_update_errors"] = [error_msg]
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)
        
        return []
        
    except Exception as e:
        error_msg = f"Error updating tasks: {str(e)}"
        logging.error(error_msg)
        
        ti.xcom_push(key="updated_tasks", value=[])
        ti.xcom_push(key="tasks_update_errors", value=[error_msg])
        
        contexts = get_thread_context()
        if thread_id not in contexts:
            contexts[thread_id] = {}
        contexts[thread_id]["update_tasks_error"] = error_msg
        contexts[thread_id]["tasks_update_errors"] = [error_msg]
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)
        
        return []

def create_associations(ti, **context):
    analysis_results = ti.xcom_pull(key="analysis_results")
    thread_id = context['dag_run'].conf.get("thread_id")
    conf = context["dag_run"].conf
    search_results = conf.get("search_results", {})

    if not analysis_results:
        logging.error("No analysis_results found in XCom. Cannot proceed with associations.")
        ti.xcom_push(key="associations_created", value=[])
        ti.xcom_push(key="associations_errors", value=["Missing analysis_results"])
        contexts = get_thread_context()
        contexts[thread_id]["associations_created"] = []
        contexts[thread_id]["associations_errors"] = ["Missing analysis_results"]
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)
        return []

    # Get newly created entities
    created_contacts = ti.xcom_pull(key="created_contacts", default=[])
    created_companies = ti.xcom_pull(key="created_companies", default=[])
    created_deals = ti.xcom_pull(key="created_deals", default=[])
    created_meetings = ti.xcom_pull(key="created_meetings", default=[])
    created_notes = ti.xcom_pull(key="created_notes", default=[])
    created_tasks = ti.xcom_pull(key="created_tasks", default=[])

    # Get updated entities (IDs remain the same)
    updated_contacts = ti.xcom_pull(key="updated_contacts", default=[])
    updated_companies = ti.xcom_pull(key="updated_companies", default=[])
    updated_deals = ti.xcom_pull(key="updated_deals", default=[])
    updated_meetings = ti.xcom_pull(key="updated_meetings", default=[])
    updated_notes = ti.xcom_pull(key="updated_notes", default=[])
    updated_tasks = ti.xcom_pull(key="updated_tasks", default=[])

    # Use filtered selected_entities from analysis_results
    selected_entities = analysis_results.get("selected_entities", {})
    logging.info(f"Using filtered selected_entities from analysis: {json.dumps(selected_entities, indent=2)}")

    # Extract existing entity IDs from selected_entities
    existing_contact_ids = [str(contact.get("contactId")) for contact in selected_entities.get("contacts", []) if contact.get("contactId")]
    existing_company_ids = [str(company.get("companyId")) for company in selected_entities.get("companies", []) if company.get("companyId")]
    existing_deal_ids = [str(deal.get("dealId")) for deal in selected_entities.get("deals", []) if deal.get("dealId")]

    # Fallback to raw search_results if selected_entities is empty
    if not existing_contact_ids:
        existing_contact_ids = [str(contact.get("contactId")) for contact in search_results.get("contact_results", {}).get("results", []) if contact.get("contactId")]
        logging.warning("selected_entities had no contacts; falling back to raw search_results")
    if not existing_company_ids:
        existing_company_ids = [str(company.get("companyId")) for company in search_results.get("company_results", {}).get("results", []) if company.get("companyId")]
        logging.warning("selected_entities had no companies; falling back to raw search_results")
    if not existing_deal_ids:
        existing_deal_ids = [str(deal.get("dealId")) for deal in search_results.get("deal_results", {}).get("results", []) if deal.get("dealId")]
        logging.warning("selected_entities had no deals; falling back to raw search_results")

    # Get updated entity IDs
    updated_contact_ids = [c.get("id", "") for c in updated_contacts if c.get("id")]
    updated_company_ids = [c.get("id", "") for c in updated_companies if c.get("id")]
    updated_deal_ids = [d.get("id", "") for d in updated_deals if d.get("id")]
    updated_meeting_ids = [m.get("id", "") for m in updated_meetings if m.get("id")]
    updated_note_ids = [n.get("id", "") for n in updated_notes if n.get("id")]
    updated_task_ids = [t.get("id", "") for t in updated_tasks if t.get("id")]

    # New IDs
    new_contact_ids = [c.get("id", "") for c in created_contacts if c.get("id")]
    new_company_ids = [c.get("id", "") for c in created_companies if c.get("id")]
    new_deal_ids = [d.get("id", "") for d in created_deals if d.get("id")]
    new_meeting_ids = [m.get("id", "") for m in created_meetings if m.get("id")]
    new_note_ids = [n.get("id", "") for n in created_notes if n.get("id")]
    new_task_ids = [t.get("id", "") for t in created_tasks if t.get("id")]

    # Priority logic: Use new IDs first, then updated IDs, then selected existing IDs
    final_contact_ids = new_contact_ids if new_contact_ids else (updated_contact_ids if updated_contact_ids else existing_contact_ids)
    final_company_ids = new_company_ids if new_company_ids else (updated_company_ids if updated_company_ids else existing_company_ids)
    final_deal_ids = new_deal_ids if new_deal_ids else (updated_deal_ids if updated_deal_ids else existing_deal_ids)
    final_meeting_ids = new_meeting_ids if new_meeting_ids else updated_meeting_ids
    final_note_ids = new_note_ids if new_note_ids else updated_note_ids
    final_task_ids = new_task_ids if new_task_ids else updated_task_ids

    # Combine all IDs for association tracking
    all_entity_ids = {
        "final_contacts": final_contact_ids,
        "final_companies": final_company_ids,
        "final_deals": final_deal_ids,
        "final_meetings": final_meeting_ids,
        "final_notes": final_note_ids,
        "final_tasks": final_task_ids
    }

    # Log the entity IDs to be associated
    logging.info(f"Associating entities for thread {thread_id}:")
    logging.info(f"Final Contact IDs: {final_contact_ids}")
    logging.info(f"Final Company IDs: {final_company_ids}")
    logging.info(f"Final Deal IDs: {final_deal_ids}")
    logging.info(f"Final Meeting IDs: {final_meeting_ids}")
    logging.info(f"Final Note IDs: {final_note_ids}")
    logging.info(f"Final Task IDs: {final_task_ids}")

    # Check if we have any entities to associate
    total_entities = (len(final_contact_ids) + len(final_company_ids) + len(final_deal_ids) +
                      len(final_meeting_ids) + len(final_note_ids) + len(final_task_ids))

    if total_entities == 0:
        logging.warning("No entities available to associate, creating empty associations list.")
        ti.xcom_push(key="associations_created", value=[])
        ti.xcom_push(key="associations_errors", value=[])
        contexts = get_thread_context()
        contexts[thread_id]["associations_created"] = []
        contexts[thread_id]["associations_errors"] = []
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)
        return []

    prompt = f"""Always associate the `AVAILABLE ENTITY IDS` by using `create_multi_association` tool.
AVAILABLE ENTITY IDS:
- Contact IDs: {final_contact_ids}
- Company IDs: {final_company_ids}  
- Deal IDs: {final_deal_ids}
- Meeting IDs: {final_meeting_ids}
- Note IDs: {final_note_ids}
- Task IDs: {final_task_ids}

You can only associate entities by calling the tool: `create_multi_association`.
Use the below format for each association request:
{{
    "single": {{
        "deal_id": "string",
        "contact_id": "string", 
        "company_id": "string",
        "note_id": "string",
        "task_id": "string",
        "meeting_id": "string"
    }}
}}

Rules:
1. Each association request should include relevant entity IDs (leave as empty string "" if not applicable)
2. Use comma separation for multiple IDs in a field if needed. example: "contact_id": "123,456".

Return JSON:
{{
    "association_requests": [
        {{
            "single": {{
                "deal_id": "123",
                "contact_id": "456", 
                "company_id": "789",
                "note_id": "",
                "task_id": "",
                "meeting_id": ""
            }}
        }},
        {{
            "single": {{
                "deal_id": "",
                "contact_id": "456",
                "company_id": "789", 
                "note_id": "101",
                "task_id": "202",
                "meeting_id": "303"
            }}
        }}
    ],
    "errors": ["Error message 1", "Error message 2"],
    "error": null
}}

If error, set error message and include individual errors in the errors array."""

    response = get_ai_response(prompt, expect_json=True)
    association_requests = []  # Initialize to avoid UnboundLocalError

    try:
        # Log raw response for debugging
        logging.info(f"Raw AI response: {response}")
        # Validate response before parsing
        if not response or response.strip() == "":
            raise ValueError("Empty response from AI service")
        if response.strip().startswith(('<', '[', '{')):
            parsed = json.loads(response)
            association_requests = parsed.get("association_requests", [])
            errors = parsed.get("errors", [])
            if parsed.get("error"):
                logging.warning(f"Association creation returned error: {parsed['error']}")
        else:
            raise ValueError(f"Invalid JSON response: {response[:100]}...")

        ti.xcom_push(key="associations_created", value=association_requests)
        ti.xcom_push(key="associations_errors", value=errors)

        contexts = get_thread_context()
        contexts[thread_id]["associations_created"] = association_requests
        contexts[thread_id]["associations_errors"] = errors
        contexts[thread_id]["create_associations_prompt"] = prompt
        contexts[thread_id]["create_associations_response"] = response
        contexts[thread_id]["all_entity_ids_used"] = all_entity_ids
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)

        logging.info(f"Created {len(association_requests)} association requests with {len(errors)} errors")
    except Exception as e:
        error_msg = f"Error creating associations: {str(e)}"
        logging.error(error_msg)
        ti.xcom_push(key="associations_created", value=[])
        ti.xcom_push(key="associations_errors", value=[error_msg])
        contexts = get_thread_context()
        contexts[thread_id]["create_associations_error"] = error_msg
        contexts[thread_id]["associations_errors"] = [error_msg]
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)

    return association_requests
def collect_and_save_results(ti, **context):
    thread_id = context['dag_run'].conf.get("thread_id")
    
    # Pull all created and updated entities from XCom
    created_contacts = ti.xcom_pull(key="created_contacts") or []
    created_companies = ti.xcom_pull(key="created_companies") or []
    created_deals = ti.xcom_pull(key="created_deals") or []
    created_meetings = ti.xcom_pull(key="created_meetings") or []
    created_notes = ti.xcom_pull(key="created_notes") or []
    created_tasks = ti.xcom_pull(key="created_tasks") or []
    
    updated_contacts = ti.xcom_pull(key="updated_contacts") or []
    updated_companies = ti.xcom_pull(key="updated_companies") or []
    updated_deals = ti.xcom_pull(key="updated_deals") or []
    updated_meetings = ti.xcom_pull(key="updated_meetings") or []
    updated_notes = ti.xcom_pull(key="updated_notes") or []
    updated_tasks = ti.xcom_pull(key="updated_tasks") or []
    
    associations_created = ti.xcom_pull(key="associations_created") or []
    
    # NEW: Pull analysis_results to get selected_entities
    analysis_results = ti.xcom_pull(key="analysis_results") or {}
    selected_entities = analysis_results.get("selected_entities", {})
    
    # Structure create_results to mirror search_results
    create_results = {
        "thread_id": thread_id,
        "created_contacts": {
            "total": len(created_contacts),
            "results": created_contacts  # List of created contact dicts with id and details
        },
        "created_companies": {
            "total": len(created_companies),
            "results": created_companies
        },
        "created_deals": {
            "total": len(created_deals),
            "results": created_deals
        },
        "created_meetings": {
            "total": len(created_meetings),
            "results": created_meetings
        },
        "created_notes": {
            "total": len(created_notes),
            "results": created_notes
        },
        "created_tasks": {
            "total": len(created_tasks),
            "results": created_tasks
        },
        "updated_contacts": {
            "total": len(updated_contacts),
            "results": updated_contacts
        },
        "updated_companies": {
            "total": len(updated_companies),
            "results": updated_companies
        },
        "updated_deals": {
            "total": len(updated_deals),
            "results": updated_deals
        },
        "updated_meetings": {
            "total": len(updated_meetings),
            "results": updated_meetings
        },
        "updated_notes": {
            "total": len(updated_notes),
            "results": updated_notes
        },
        "updated_tasks": {
            "total": len(updated_tasks),
            "results": updated_tasks
        },
        "associations_created": {
            "total": len(associations_created),
            "results": associations_created
        }
    }
    
    # NEW: Add selected_entities as the "used" or "selected" results for contacts, companies, deals
    create_results["selected_contacts"] = {
        "total": len(selected_entities.get("contacts", [])),
        "results": selected_entities.get("contacts", [])
    }
    create_results["selected_companies"] = {
        "total": len(selected_entities.get("companies", [])),
        "results": selected_entities.get("companies", [])
    }
    create_results["selected_deals"] = {
        "total": len(selected_entities.get("deals", [])),
        "results": selected_entities.get("deals", [])
    }
    
    # Update thread context with create_results
    try:
        contexts = get_thread_context()
        if thread_id not in contexts:
            contexts[thread_id] = {}
        contexts[thread_id]["create_results"] = create_results
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)
        logging.info(f"Saved create_results for thread {thread_id}")
    except Exception as e:
        logging.error(f"Error saving create_results for thread {thread_id}: {e}")


def compose_response_html(ti, **context):
    analysis_results = ti.xcom_pull(key="analysis_results")
    created_contacts = ti.xcom_pull(key="created_contacts", default=[])
    created_companies = ti.xcom_pull(key="created_companies", default=[])
    created_deals = ti.xcom_pull(key="created_deals", default=[])
    created_meetings = ti.xcom_pull(key="created_meetings", default=[])
    created_notes = ti.xcom_pull(key="created_notes", default=[])
    created_tasks = ti.xcom_pull(key="created_tasks", default=[])

    updated_contacts = ti.xcom_pull(key="updated_contacts", default=[])
    updated_companies = ti.xcom_pull(key="updated_companies", default=[])
    updated_deals = ti.xcom_pull(key="updated_deals", default=[])
    updated_meetings = ti.xcom_pull(key="updated_meetings", default=[])
    updated_notes = ti.xcom_pull(key="updated_notes", default=[])
    updated_tasks = ti.xcom_pull(key="updated_tasks", default=[])
    owner_info = ti.xcom_pull(key="owner_info", default={})
    task_threshold_info = ti.xcom_pull(key="task_threshold_info", default={})
    associations_created = ti.xcom_pull(key="associations_created", default=[])

    thread_id = context['dag_run'].conf.get("thread_id")
    email_data = context['dag_run'].conf.get("email_data", {})
    user_response_email = context['dag_run'].conf.get("user_response_email", email_data)
    selected_entities = analysis_results.get("selected_entities", {"contacts": [], "companies": [], "deals": []})
    existing_contacts = selected_entities.get("contacts", [])
    existing_companies = selected_entities.get("companies", [])
    existing_deals = selected_entities.get("deals", [])
    from_sender = user_response_email.get("headers", {}).get("From", email_data.get("headers", {}).get("From", ""))

    ### NEW: Load prior create_results from conf to include previous creations in email
    prior_create_results = context['dag_run'].conf.get("create_results", {})
    prior_created_notes = prior_create_results.get("created_notes", {}).get("results", [])
    prior_created_tasks = prior_create_results.get("created_tasks", {}).get("results", [])
    prior_created_meetings = prior_create_results.get("created_meetings", {}).get("results", [])
    # Merge priors + current (priors first for history order)
    all_created_notes = prior_created_notes + created_notes
    all_created_tasks = prior_created_tasks + created_tasks
    all_created_meetings = prior_created_meetings + created_meetings
    logging.info(f"Merged prior creations for email: Notes={len(all_created_notes)}, Tasks={len(all_created_tasks)}, Meetings={len(all_created_meetings)}")

    updated_task_ids = [task.get("id") for task in updated_tasks if task.get("id")]
    final_created_tasks = [t for t in all_created_tasks if t.get("id") not in updated_task_ids]
    email_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
            th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
            th {{ background-color: #f2f2f2; font-weight: bold; }}
            h3 {{ color: #333; margin-top: 30px; margin-bottom: 15px; }}
            .greeting {{ margin-bottom: 20px; }}
            .closing {{ margin-top: 30px; }}
            .warning {{ background-color: #fff3cd; border: 1px solid #ffeeba; color: #856404; padding: 10px; border-radius: 5px; margin: 10px 0; }}
        </style>
    </head>
    <body>
        <div class="greeting">
            <p>Hello {from_sender},</p>
            <p>I have completed the requested operations in HubSpot. Here is the summary:</p>
        </div>
    """

    # FIXED: Check for actual data, not just empty lists
    if (existing_contacts and len(existing_contacts) > 0) or (updated_contacts and len(updated_contacts) > 0):
        email_content += """
        <h3>Contacts Used/Updated</h3>
        <table>
            <thead>
                <tr>
                    <th>ID</th>
                    <th>Firstname</th>
                    <th>Lastname</th>
                    <th>Email</th>
                    <th>Phone Number</th>
                    <th>Address</th>
                    <th>Job Title</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
        """
        for contact in existing_contacts:
            details = contact.get("details", contact)
            email_content += f"""
                <tr>
                    <td>{contact.get("contactId", "")}</td>
                    <td>{details.get("firstname", "")}</td>
                    <td>{details.get("lastname", "")}</td>
                    <td>{details.get("email", "")}</td>
                    <td>{details.get("phone", "")}</td>
                    <td>{details.get("address", "")}</td>
                    <td>{details.get("jobtitle", "")}</td>
                    <td>Existing</td>
                </tr>
            """
        for contact in updated_contacts:
            details = contact.get("details", {})
            email_content += f"""
                <tr>
                    <td>{contact.get("id", "")}</td>
                    <td>{details.get("firstname", "")}</td>
                    <td>{details.get("lastname", "")}</td>
                    <td>{details.get("email", "")}</td>
                    <td>{details.get("phone", "")}</td>
                    <td>{details.get("address", "")}</td>
                    <td>{details.get("jobtitle", "")}</td>
                    <td>Updated</td>
                </tr>
            """
        email_content += """
            </tbody>
        </table>
        """

    # FIXED: Check for actual data, not just empty lists
    if (existing_companies and len(existing_companies) > 0) or (updated_companies and len(updated_companies) > 0):
        email_content += """
        <h3>Companies Used/Updated</h3>
        <table>
            <thead>
                <tr>
                    <th>ID</th>
                    <th>Name</th>
                    <th>Domain</th>
                    <th>Address</th>
                    <th>City</th>
                    <th>State</th>
                    <th>ZIP</th>
                    <th>Country</th>
                    <th>Phone</th>
                    <th>Description</th>
                    <th>Type</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
        """
        for company in existing_companies:
            details = company.get("details", company)
            email_content += f"""
                <tr>
                    <td>{company.get("companyId", "")}</td>
                    <td>{details.get("name", "")}</td>
                    <td>{details.get("domain", "")}</td>
                    <td>{details.get("address", "")}</td>
                    <td>{details.get("city", "")}</td>
                    <td>{details.get("state", "")}</td>
                    <td>{details.get("zip", "")}</td>
                    <td>{details.get("country", "")}</td>
                    <td>{details.get("phone", "")}</td>
                    <td>{details.get("description", "")}</td>
                    <td>{details.get("type", "")}</td>
                    <td>Existing</td>
                </tr>
            """
        for company in updated_companies:
            details = company.get("details", {})
            email_content += f"""
                <tr>
                    <td>{company.get("id", "")}</td>
                    <td>{details.get("name", "")}</td>
                    <td>{details.get("domain", "")}</td>
                    <td>{details.get("address", "")}</td>
                    <td>{details.get("city", "")}</td>
                    <td>{details.get("state", "")}</td>
                    <td>{details.get("zip", "")}</td>
                    <td>{details.get("country", "")}</td>
                    <td>{details.get("phone", "")}</td>
                    <td>{details.get("description", "")}</td>
                    <td>{details.get("type", "")}</td>
                    <td>Updated</td>
                </tr>
            """
        email_content += """
            </tbody>
        </table>
        """

    # FIXED: Check for actual data, not just empty lists
    if (existing_deals and len(existing_deals) > 0) or (updated_deals and len(updated_deals) > 0):
        email_content += """
        <h3>Deals Used/Updated</h3>
        <table>
            <thead>
                <tr>
                    <th>ID</th>
                    <th>Deal Name</th>
                    <th>Deal Stage Label</th>
                    <th>Deal Amount</th>
                    <th>Close Date</th>
                    <th>Deal Owner Name</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
        """
        for deal in existing_deals:
            details = deal.get("details", deal)
            email_content += f"""
                <tr>
                    <td>{deal.get("dealId", "")}</td>
                    <td>{details.get("dealName", "")}</td>
                    <td>{details.get("dealLabelName", "")}</td>
                    <td>{details.get("dealAmount", "")}</td>
                    <td>{details.get("closeDate", "")}</td>
                    <td>{details.get("dealOwnerName", "")}</td>
                    <td>Existing</td>
                </tr>
            """
        for deal in updated_deals:
            details = deal.get("details", {})
            email_content += f"""
                <tr>
                    <td>{deal.get("id", "")}</td>
                    <td>{details.get("dealName", "")}</td>
                    <td>{details.get("dealLabelName", "")}</td>
                    <td>{details.get("dealAmount", "")}</td>
                    <td>{details.get("closeDate", "")}</td>
                    <td>{details.get("dealOwnerName", "")}</td>
                    <td>Updated</td>
                </tr>
            """
        email_content += """
            </tbody>
        </table>
        """

    # Show newly created entities only if data exists
    if created_contacts and len(created_contacts) > 0:
        email_content += """
        <h3>Newly Created Contacts</h3>
        <table>
            <thead>
                <tr>
                    <th>ID</th>
                    <th>Firstname</th>
                    <th>Lastname</th>
                    <th>Email</th>
                    <th>Phone Number</th>
                    <th>Address</th>
                    <th>Job Title</th>
                </tr>
            </thead>
            <tbody>
        """
        for contact in created_contacts:
            details = contact.get("details", {})
            email_content += f"""
                <tr>
                    <td>{contact.get("id", "")}</td>
                    <td>{details.get("firstname", "")}</td>
                    <td>{details.get("lastname", "")}</td>
                    <td>{details.get("email", "")}</td>
                    <td>{details.get("phone", "")}</td>
                    <td>{details.get("address", "")}</td>
                    <td>{details.get("jobtitle", "")}</td>
                </tr>
            """
        email_content += """
            </tbody>
        </table>
        """

    if created_companies and len(created_companies) > 0:
        email_content += """
        <h3>Newly Created Companies</h3>
        <table>
            <thead>
                <tr>
                    <th>ID</th>
                    <th>Name</th>
                    <th>Domain</th>
                    <th>Address</th>
                    <th>City</th>
                    <th>State</th>
                    <th>ZIP</th>
                    <th>Country</th>
                    <th>Phone</th>
                    <th>Description</th>
                    <th>Type</th>
                </tr>
            </thead>
            <tbody>
        """
        for company in created_companies:
            details = company.get("details", {})
            email_content += f"""
                <tr>
                    <td>{company.get("id", "")}</td>
                    <td>{details.get("name", "")}</td>
                    <td>{details.get("domain", "")}</td>
                    <td>{details.get("address", "")}</td>
                    <td>{details.get("city", "")}</td>
                    <td>{details.get("state", "")}</td>
                    <td>{details.get("zip", "")}</td>
                    <td>{details.get("country", "")}</td>
                    <td>{details.get("phone", "")}</td>
                    <td>{details.get("description", "")}</td>
                    <td>{details.get("type", "")}</td>
                </tr>
            """
        email_content += """
            </tbody>
        </table>
        """

    if created_deals and len(created_deals) > 0:
        email_content += """
        <h3>Newly Created Deals</h3>
        <table>
            <thead>
                <tr>
                    <th>ID</th>
                    <th>Deal Name</th>
                    <th>Deal Stage Label</th>
                    <th>Deal Amount</th>
                    <th>Close Date</th>
                    <th>Deal Owner Name</th>
                </tr>
            </thead>
            <tbody>
        """
        for deal in created_deals:
            details = deal.get("details", {})
            email_content += f"""
                <tr>
                    <td>{deal.get("id", "")}</td>
                    <td>{details.get("dealName", "")}</td>
                    <td>{details.get("dealLabelName", "")}</td>
                    <td>{details.get("dealAmount", "")}</td>
                    <td>{details.get("closeDate", "")}</td>
                    <td>{details.get("dealOwnerName", "")}</td>
                </tr>
            """
        email_content += """
            </tbody>
        </table>
        """

    # Rest of the function continues with meetings, notes, tasks...
    # (Keep the existing logic for the remaining sections)
    
    ### UPDATED: Use all_created_* for secondaries to include priors
    if all_created_meetings or updated_meetings:
        email_content += """
        <h3>Meetings Created/Updated</h3>
        <table>
            <thead>
                <tr>
                    <th>ID</th>
                    <th>Meeting Title</th>
                    <th>Start Time</th>
                    <th>End Time</th>
                    <th>Location</th>
                    <th>Outcome</th>
                    <th>Timestamp</th>
                    <th>Attendees</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
        """
        for meeting in all_created_meetings:
            details = meeting.get("details", {})
            attendees = ", ".join(details.get("attendees", []))
            email_content += f"""
                <tr>
                    <td>{meeting.get("id", "")}</td>
                    <td>{details.get("meeting_title", "")}</td>
                    <td>{details.get("start_time", "")}</td>
                    <td>{details.get("end_time", "")}</td>
                    <td>{details.get("location", "")}</td>
                    <td>{details.get("outcome", "")}</td>
                    <td>{details.get("timestamp", "")}</td>
                    <td>{attendees}</td>
                    <td>Created</td>
                </tr>
            """
        for meeting in updated_meetings:
            details = meeting.get("details", {})
            attendees = ", ".join(details.get("attendees", []))
            email_content += f"""
                <tr>
                    <td>{meeting.get("id", "")}</td>
                    <td>{details.get("meeting_title", "")}</td>
                    <td>{details.get("start_time", "")}</td>
                    <td>{details.get("end_time", "")}</td>
                    <td>{details.get("location", "")}</td>
                    <td>{details.get("outcome", "")}</td>
                    <td>{details.get("timestamp", "")}</td>
                    <td>{attendees}</td>
                    <td>Updated</td>
                </tr>
            """
        email_content += """
            </tbody>
        </table>
        """

    if all_created_notes or updated_notes:
        email_content += """
        <h3>Notes Created/Updated</h3>
        <table>
            <thead>
                <tr>
                    <th>ID</th>
                    <th>Note Content</th>
                    <th>Timestamp</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
        """
        for note in all_created_notes:
            details = note.get("details", {})
            email_content += f"""
                <tr>
                    <td>{note.get("id", "")}</td>
                    <td>{details.get("note_content", "")}</td>
                    <td>{details.get("timestamp", "")}</td>
                    <td>Created</td>
                </tr>
            """
        for note in updated_notes:
            details = note.get("details", {})
            email_content += f"""
                <tr>
                    <td>{note.get("id", "")}</td>
                    <td>{details.get("note_content", "")}</td>
                    <td>{details.get("timestamp", "")}</td>
                    <td>Updated</td>
                </tr>
            """
        email_content += """
            </tbody>
        </table>
        """

    if final_created_tasks  or updated_tasks :
        email_content += """
        <h3>Tasks Created/Updated</h3>
        <table>
            <thead>
                <tr>
                    <th>ID</th>
                    <th>Task Details</th>
                    <th>Owner Name</th>
                    <th>Due Date</th>
                    <th>Priority</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
        """
        # CHANGE: Use final_created_tasks instead of all_created_tasks
        for task in final_created_tasks:
            details = task.get("details", {})
            email_content += f"""
                <tr>
                    <td>{task.get("id", "")}</td>
                    <td>{details.get("task_details", "")}</td>
                    <td>{details.get("task_owner_name", "")}</td>
                    <td>{details.get("due_date", "")}</td>
                    <td>{details.get("priority", "")}</td>
                    <td>Created</td>
                </tr>
            """
        for task in updated_tasks:
            details = task.get("details", {})
            email_content += f"""
                <tr>
                    <td>{task.get("id", "")}</td>
                    <td>{details.get("task_details", "")}</td>
                    <td>{details.get("task_owner_name", "")}</td>
                    <td>{details.get("due_date", "")}</td>
                    <td>{details.get("priority", "")}</td>
                    <td>Updated</td>
                </tr>
            """
        email_content += """
            </tbody>
        </table>
        """
    # ...existing code in compose_response_html...

    # --- Task Volume Analysis Section ---
    dates_checked = task_threshold_info.get("task_threshold_results", {}).get("dates_checked", [])
    if final_created_tasks and dates_checked:
        email_content += """
        <h3>Task Volume Analysis</h3>
        <table>
            <thead>
                <tr>
                    <th>Date</th>
                    <th>Owner Name</th>
                    <th>Existing Tasks</th>
                    <th>Threshold Status</th>
                    <th>Warning</th>
                </tr>
            </thead>
            <tbody>
        """
        for date_info in dates_checked:
            date = date_info.get("date", "")
            owner_name = date_info.get("owner_name", "")
            task_count = date_info.get("existing_task_count", 0)
            exceeds = "Exceeds" if date_info.get("exceeds_threshold") else "Within Limit"
            warning = date_info.get("warning") or "None"
            email_content += f"""
                <tr>
                    <td>{date}</td>
                    <td>{owner_name}</td>
                    <td>{task_count}</td>
                    <td>{exceeds}</td>
                    <td>{warning}</td>
                </tr>
            """
        email_content += """
            </tbody>
        </table>
        <p><em>Note: High task volumes may impact workflow performance and user productivity.</em></p>
        <hr>
        """

    # --- Owner Assignment Section ---
    has_deals_or_tasks = (
        (existing_deals and len(existing_deals) > 0) or
        (created_deals and len(created_deals) > 0) or
        (final_created_tasks and len(final_created_tasks) > 0)
    )
    if has_deals_or_tasks and owner_info:
        chosen_deal_owner_id = owner_info.get("deal_owner_id", "159242778")
        chosen_deal_owner_name = owner_info.get("deal_owner_name", "liji")
        deal_owner_msg = owner_info.get("deal_owner_message", "")
        task_owners = owner_info.get("task_owners", [])
        all_owners = owner_info.get("all_owners_table", [])

        email_content += """
        <h3>Owner Assignment Details</h3>
        """

        # Deal Owner Assignment
        if (existing_deals and len(existing_deals) > 0) or (created_deals and len(created_deals) > 0):
            email_content += "<div style='margin-bottom: 15px;'>"
            email_content += "<h4 style='color: #2c5aa0; margin-bottom: 5px;'>Deal Owner Assignment:</h4>"
            deal_msg_lower = deal_owner_msg.lower()
            if "no deal owner specified" in deal_msg_lower:
                email_content += f"""
                <p style='background-color: #d1ecf1; padding: 10px; border-left: 4px solid #17a2b8;'>
                    <strong>Reason:</strong> Deal owner was not specified.
                    <br><strong>Action:</strong> Assigning to default owner '{chosen_deal_owner_name}'.
                </p>
                """
            elif ("not valid" in deal_msg_lower and "deal owner" in deal_msg_lower):
                email_content += f"""
                <p style='background-color: #f8d7da; padding: 10px; border-left: 4px solid #dc3545;'>
                    <strong>Reason:</strong> Deal owner mentioned, but not found in the available owners list.
                    <br><strong>Action:</strong> Assigning to default owner '{chosen_deal_owner_name}'.
                </p>
                """
            elif ("deal owner specified as" in deal_msg_lower or "specified" in deal_msg_lower):
                email_content += f"""
                <p style='background-color: #d4edda; padding: 10px; border-left: 4px solid #28a745;'>
                    <strong>Reason:</strong> Deal owner is valid and found in the available owners list.
                    <br><strong>Action:</strong> Assigned to '{chosen_deal_owner_name}'.
                </p>
                """
            else:
                email_content += f"""
                <p style='background-color: #d4edda; padding: 10px; border-left: 4px solid #28a745;'>
                    <strong>Reason:</strong> Deal owner assignment processed.
                    <br><strong>Action:</strong> Assigned to '{chosen_deal_owner_name}'.
                </p>
                """
            email_content += "</div>"

        # Task Owner Assignment
        if final_created_tasks and task_owners:
            email_content += "<div style='margin-bottom: 15px;'>"
            email_content += "<h4 style='color: #2c5aa0; margin-bottom: 5px;'>Task Owner Assignments:</h4>"
            for task_owner in task_owners:
                task_index = task_owner.get("task_index", 0)
                task_owner_name = task_owner.get("task_owner_name", "liji")
                task_owner_msg = task_owner.get("task_owner_message", "")
                task = next((t for t in final_created_tasks if t.get("details", {}).get("task_index") == task_index), None)
                task_details = task.get("details", {}).get("task_details", "Unknown task") if task else "Unknown task"
                task_msg_lower = task_owner_msg.lower()
                if "no task owner specified" in task_msg_lower:
                    email_content += f"""
                    <p style='background-color: #d1ecf1; padding: 10px; border-left: 4px solid #17a2b8;'>
                        <strong>Task {task_index}:</strong> {task_details}
                        <br><strong>Reason:</strong> {task_owner_msg}
                        <br><strong>Action:</strong> Assigning to default owner '{task_owner_name}'.
                    </p>
                    """
                elif "not valid" in task_msg_lower:
                    email_content += f"""
                    <p style='background-color: #f8d7da; padding: 10px; border-left: 4px solid #dc3545;'>
                        <strong>Task {task_index}:</strong> {task_details}
                        <br><strong>Reason:</strong> {task_owner_msg}
                        <br><strong>Action:</strong> Assigning to default owner '{task_owner_name}'.
                    </p>
                    """
                else:
                    email_content += f"""
                    <p style='background-color: #d4edda; padding: 10px; border-left: 4px solid #28a745;'>
                        <strong>Task {task_index}:</strong> {task_details}
                        <br><strong>Reason:</strong> {task_owner_msg}
                        <br><strong>Action:</strong> Assigned to '{task_owner_name}'.
                    </p>
                    """
            email_content += "</div>"

        # Available Owners Table
        if all_owners:
            email_content += """
            <h4 style='color: #2c5aa0; margin-bottom: 10px;'>Available Owners:</h4>
            <table style='border-collapse: collapse; width: 100%; margin-bottom: 20px;'>
                <thead>
                    <tr style='background-color: #e3f2fd;'>
                        <th style='border: 1px solid #ddd; padding: 8px; text-align: left;'>Owner ID</th>
                        <th style='border: 1px solid #ddd; padding: 8px; text-align: left;'>Owner Name</th>
                        <th style='border: 1px solid #ddd; padding: 8px; text-align: left;'>Owner Email</th>
                        <th style='border: 1px solid #ddd; padding: 8px; text-align: left;'>Assignment</th>
                    </tr>
                </thead>
                <tbody>
            """
            for owner in all_owners:
                owner_id = owner.get("id", "")
                owner_name = owner.get("name", "")
                owner_email = owner.get("email", "")
                assignments = []
                if owner_id == chosen_deal_owner_id and ((existing_deals and len(existing_deals) > 0) or (created_deals and len(created_deals) > 0)):
                    assignments.append("Deal Owner")
                if any(task_owner.get("task_owner_id") == owner_id for task_owner in task_owners) and final_created_tasks:
                    task_indices = [str(task_owner.get("task_index")) for task_owner in task_owners if task_owner.get("task_owner_id") == owner_id]
                    assignments.append(f"Task Owner (Tasks {', '.join(task_indices)})")
                assignment_text = ", ".join(assignments) if assignments else ""
                if "Deal Owner" in assignments and "Task Owner" in assignments:
                    row_style = ' style="background-color: #d1ecf1; border-left: 4px solid #0c5460;"'
                elif "Deal Owner" in assignments:
                    row_style = ' style="background-color: #d4edda; border-left: 4px solid #28a745;"'
                elif "Task Owner" in assignments:
                    row_style = ' style="background-color: #fff3cd; border-left: 4px solid #ffc107;"'
                else:
                    row_style = ' style="background-color: #f8f9fa;"'
                email_content += f"""
                    <tr{row_style}>
                        <td style='border: 1px solid #ddd; padding: 8px;'>{owner_id}</td>
                        <td style='border: 1px solid #ddd; padding: 8px;'><strong>{owner_name}</strong></td>
                        <td style='border: 1px solid #ddd; padding: 8px;'>{owner_email}</td>
                        <td style='border: 1px solid #ddd; padding: 8px;'><strong>{assignment_text}</strong></td>
                    </tr>
                """
            email_content += """
                </tbody>
            </table>
            """
        email_content += "<hr>"

# ...rest of your compose_response_html code...

    email_content += """
        <div class="closing">
            <p>Please let me know if any adjustments or corrections are needed.</p>
            <p>Best regards,<br>
            HubSpot Agent<br>
            hubspot-agent-9201@lowtouch.ai</p>
        </div>
    </body>
    </html>
    """

    ti.xcom_push(key="response_html", value=email_content)
    contexts = get_thread_context()
    contexts[thread_id]["response_html"] = email_content
    with open(THREAD_CONTEXT_FILE, "w") as f:
        json.dump(contexts, f)

    logging.info(f"Composed response HTML for thread {thread_id}")
    return email_content

def send_final_email(ti, **context):
    """Updated send_final_email with multi-recipient support"""
    email_data = context['dag_run'].conf.get("email_data", {})
    user_response_email = context['dag_run'].conf.get("user_response_email", email_data)
    response_html = ti.xcom_pull(key="response_html")
    thread_id = context['dag_run'].conf.get("thread_id")

    service = authenticate_gmail()
    if not service:
        logging.error("Gmail authentication failed, cannot send final email.")
        contexts = get_thread_context()
        contexts[thread_id]["final_email_error"] = "Gmail authentication failed"
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)
        raise ValueError("Gmail authentication failed")

    # Get thread context to retrieve all_recipients from confirmation email
    thread_context = get_thread_context().get(thread_id, {})
    stored_all_recipients = thread_context.get("all_recipients", {})
    
    # If no stored recipients, extract from current user response
    if not stored_all_recipients:
        stored_all_recipients = extract_all_recipients(user_response_email)

    # Get sender from user response
    sender_email = user_response_email["headers"].get("From", "")
    original_subject = user_response_email['headers'].get('Subject', 'Meeting Minutes Request')

    if not original_subject.lower().startswith('re:'):
        subject = f"Re: {original_subject}"
    else:
        subject = original_subject

    in_reply_to = user_response_email["headers"].get("Message-ID", "")
    references = user_response_email["headers"].get("References", "")

    # Prepare recipients for reply-all functionality
    primary_recipient = sender_email
    
    # For Cc: Include recipients from user response + stored recipients
    current_recipients = extract_all_recipients(user_response_email)
    cc_recipients = []
    
    # Add current Cc recipients
    for cc_addr in current_recipients["cc"]:
        if (HUBSPOT_FROM_ADDRESS.lower() not in cc_addr.lower() and 
            cc_addr not in cc_recipients):
            cc_recipients.append(cc_addr)
    
    # Add current To recipients (except sender and bot)
    for to_addr in current_recipients["to"]:
        if (to_addr.lower() != sender_email.lower() and 
            HUBSPOT_FROM_ADDRESS.lower() not in to_addr.lower() and
            to_addr not in cc_recipients):
            cc_recipients.append(to_addr)
    
    # Add original recipients from stored context if they're not already included
    for to_addr in stored_all_recipients.get("to", []):
        if (to_addr.lower() != sender_email.lower() and 
            HUBSPOT_FROM_ADDRESS.lower() not in to_addr.lower() and
            to_addr not in cc_recipients):
            cc_recipients.append(to_addr)
    
    for cc_addr in stored_all_recipients.get("cc", []):
        if (HUBSPOT_FROM_ADDRESS.lower() not in cc_addr.lower() and 
            cc_addr not in cc_recipients):
            cc_recipients.append(cc_addr)

    # For Bcc: Include current + stored Bcc recipients
    bcc_recipients = []
    for bcc_addr in current_recipients.get("bcc", []):
        if (HUBSPOT_FROM_ADDRESS.lower() not in bcc_addr.lower() and
            bcc_addr not in bcc_recipients):
            bcc_recipients.append(bcc_addr)
    
    for bcc_addr in stored_all_recipients.get("bcc", []):
        if (HUBSPOT_FROM_ADDRESS.lower() not in bcc_addr.lower() and
            bcc_addr not in bcc_recipients):
            bcc_recipients.append(bcc_addr)

    # Convert to strings
    cc_string = ', '.join(cc_recipients) if cc_recipients else None
    bcc_string = ', '.join(bcc_recipients) if bcc_recipients else None

    logging.info(f"Sending final email:")
    logging.info(f"Primary recipient: {primary_recipient}")
    logging.info(f"Cc recipients: {cc_string}")
    logging.info(f"Bcc recipients: {bcc_string}")

    retries = 3
    for attempt in range(retries):
        try:
            result = send_email(service, primary_recipient, subject, response_html, 
                              in_reply_to, references, cc=cc_string, bcc=bcc_string)
            if result:
                logging.info(f"Final workflow completion email sent to all recipients")
                contexts = get_thread_context()
                contexts[thread_id].update({
                    "final_email_sent": True,
                    "final_email_timestamp": datetime.now().isoformat(),
                    "workflow_status": "completed"
                })
                with open(THREAD_CONTEXT_FILE, "w") as f:
                    json.dump(contexts, f)
                return result
            else:
                logging.error(f"Attempt {attempt+1} failed to send email")
        except Exception as e:
            logging.error(f"Attempt {attempt+1} failed to send email: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                logging.error(f"Failed to send final email after {retries} attempts")
                contexts = get_thread_context()
                contexts[thread_id]["final_email_error"] = f"Failed to send final email: {str(e)}"
                with open(THREAD_CONTEXT_FILE, "w") as f:
                    json.dump(contexts, f)
                raise

    return None
def branch_to_creation_tasks(ti, **context):
    analysis_results = ti.xcom_pull(task_ids='analyze_user_response', key='analysis_results')
    mandatory_tasks = ["create_associations", "compose_response_html", "collect_and_save_results", "send_final_email"]
    
    if not analysis_results or not isinstance(analysis_results, dict):
        logging.error("Invalid or missing analysis_results from analyze_user_response")
        logging.info(f"Proceeding with mandatory tasks: {mandatory_tasks}")
        return mandatory_tasks

    # Use tasks_to_execute from analysis_results
    tasks_to_execute = analysis_results.get("tasks_to_execute", [])
    
    # Ensure mandatory tasks are always included
    tasks_to_execute = list(set(tasks_to_execute + mandatory_tasks))
    
    if analysis_results.get("should_determine_owner"):
        tasks_to_execute.append("determine_owner")
    if analysis_results.get("should_check_task_threshold"):
        tasks_to_execute.append("check_task_threshold")
    # Validate task IDs
    valid_task_ids = [
        "create_contacts", "create_companies", "determine_owner", "check_task_threshold",  # <-- add these two
        "create_deals", "create_meetings", "create_notes", "create_tasks",
        "update_contacts", "update_companies", "update_deals", "update_meetings", "update_notes", "update_tasks",
        "create_associations", "compose_response_html", "collect_and_save_results", "send_final_email"
    ]
    invalid_tasks = [task for task in tasks_to_execute if task not in valid_task_ids]
    if invalid_tasks:
        logging.error(f"Invalid task IDs found: {invalid_tasks}. Proceeding with mandatory tasks only.")
        tasks_to_execute = mandatory_tasks

    logging.info(f"Tasks to execute: {tasks_to_execute}")
    return tasks_to_execute

with DAG(
    "hubspot_create_objects",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["hubspot", "create", "objects"]
) as dag:

    start_task = DummyOperator(task_id="start_workflow")

    analyze_task = PythonOperator(
        task_id="analyze_user_response",
        python_callable=analyze_user_response,
        provide_context=True
    )

    branch_task = BranchPythonOperator(
        task_id="branch_to_creation_tasks",
        python_callable=branch_to_creation_tasks,
        provide_context=True
    )

    determine_owner_task = PythonOperator(
    task_id="determine_owner",
    python_callable=determine_owner,
    provide_context=True
    )

    check_task_threshold_task = PythonOperator(
    task_id="check_task_threshold",
    python_callable=check_task_threshold,
    provide_context=True
    )

    create_contacts_task = PythonOperator(
        task_id="create_contacts",
        python_callable=create_contacts,
        provide_context=True
    )

    create_companies_task = PythonOperator(
        task_id="create_companies",
        python_callable=create_companies,
        provide_context=True
    )

    create_deals_task = PythonOperator(
        task_id="create_deals",
        python_callable=create_deals,
        provide_context=True
    )

    create_meetings_task = PythonOperator(
        task_id="create_meetings",
        python_callable=create_meetings,
        provide_context=True
    )

    create_notes_task = PythonOperator(
        task_id="create_notes",
        python_callable=create_notes,
        provide_context=True
    )

    create_tasks_task = PythonOperator(
        task_id="create_tasks",
        python_callable=create_tasks,
        provide_context=True
    )

    update_contacts_task = PythonOperator(
        task_id="update_contacts",
        python_callable=update_contacts,
        provide_context=True
    )

    update_companies_task = PythonOperator(
        task_id="update_companies",
        python_callable=update_companies,
        provide_context=True
    )

    update_deals_task = PythonOperator(
        task_id="update_deals",
        python_callable=update_deals,
        provide_context=True
    )

    update_meetings_task = PythonOperator(
        task_id="update_meetings",
        python_callable=update_meetings,
        provide_context=True
    )

    update_notes_task = PythonOperator(
        task_id="update_notes",
        python_callable=update_notes,
        provide_context=True
    )

    update_tasks_task = PythonOperator(
        task_id="update_tasks",
        python_callable=update_tasks,
        provide_context=True
    )

    create_associations_task = PythonOperator(
        task_id="create_associations",
        python_callable=create_associations,
        provide_context=True,
        trigger_rule="none_failed_min_one_success"
    )

    collect_results_task = PythonOperator(
    task_id="collect_and_save_results",
    python_callable=collect_and_save_results,
    provide_context=True,
    trigger_rule="none_failed_min_one_success"
    )

    compose_response_task = PythonOperator(
        task_id="compose_response_html",
        python_callable=compose_response_html,
        provide_context=True,
        trigger_rule="none_failed_min_one_success"
    )

    send_final_email_task = PythonOperator(
        task_id="send_final_email",
        python_callable=send_final_email,
        provide_context=True,
        trigger_rule="none_failed_min_one_success"
    )

    end_task = DummyOperator(
        task_id="end_workflow",
        trigger_rule="all_done"
    )

    start_task >> analyze_task >> branch_task

    creation_tasks = {
        "create_contacts": create_contacts_task,
        "create_companies": create_companies_task,
        "determine_owner": determine_owner_task,
        "check_task_threshold": check_task_threshold_task,
        "create_deals": create_deals_task,
        "create_meetings": create_meetings_task,
        "create_notes": create_notes_task,
        "create_tasks": create_tasks_task,
        "update_contacts": update_contacts_task,
        "update_companies": update_companies_task,
        "update_deals": update_deals_task,
        "update_meetings": update_meetings_task,
        "update_notes": update_notes_task,
        "update_tasks": update_tasks_task
    }

    branch_task >> [task for task in creation_tasks.values()]

    for task in creation_tasks.values():
            task >> create_associations_task
    branch_task >> create_associations_task
    create_associations_task >> compose_response_task >> collect_results_task >> send_final_email_task >> end_task
