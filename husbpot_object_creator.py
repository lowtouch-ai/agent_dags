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
        creds = Credentials.from_authorized_user_info(GMAIL_CREDENTIALS)
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

def send_email(service, recipient, subject, body, in_reply_to, references):
    try:
        msg = MIMEMultipart()
        msg["From"] = f"HubSpot via lowtouch.ai <{HUBSPOT_FROM_ADDRESS}>"
        msg["To"] = recipient
        msg["Subject"] = subject
        msg["In-Reply-To"] = in_reply_to
        msg["References"] = references
        msg.attach(MIMEText(body, "html"))
        raw_msg = base64.urlsafe_b64encode(msg.as_string().encode("utf-8")).decode("utf-8")
        result = service.users().messages().send(userId="me", body={"raw": raw_msg}).execute()
        logging.info(f"Email sent to {recipient}")
        return result
    except Exception as e:
        logging.error(f"Failed to send email: {e}")
        raise  # Raise exception to trigger retry in send_final_email

def analyze_user_response(ti, **context):
    conf = context["dag_run"].conf
    thread_id = conf.get("thread_id")
    search_results = conf.get("search_results", {})
    logging.info(f"Search results: {search_results}")
    if not thread_id:
        logging.error("No thread_id provided in dag_run.conf")
        results = {
            "status": "error",
            "error_message": "No thread_id provided in dag_run.conf",
            "next_steps": ["Provide a valid thread_id"],
            "entities_to_create": {},
            "entities_to_update": {},
            "user_intent": "ERROR",
            "confidence_level": "low"
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
            "confidence_level": "low"
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
                "confidence_level": "low"
            }
            ti.xcom_push(key="analysis_results", value=results)
            return results

    # Normalize original entities
    original_entities_normalized = {
        "contacts": search_results.get("new_entity_details", {}).get("contacts", []),
        "companies": search_results.get("new_entity_details", {}).get("companies", []),
        "deals": search_results.get("new_entity_details", {}).get("deals", []),
        "meetings": [search_results.get("new_entity_details", {}).get("meeting_details", {})] if search_results.get("new_entity_details", {}).get("meeting_details") else [],
        "notes": search_results.get("new_entity_details", {}).get("notes", []),
        "tasks": search_results.get("new_entity_details", {}).get("tasks", [])
    }

    selected_entities = {
        "contacts": search_results.get("contact_results", {}).get("results", []),
        "companies": search_results.get("company_results", {}).get("results", []),
        "deals": search_results.get("deal_results", {}).get("results", []),
    }

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
            "confidence_level": "low"
        }
        ti.xcom_push(key="analysis_results", value=results)
        return results

    analysis_prompt = f"""
Analyze this user response to determine their intent and extract any requested changes.

ORIGINAL PROPOSED ENTITIES (these are suggestions for NEW creations only):
{json.dumps(original_entities_normalized, indent=2)}

EXISTING ENTITIES TO BE USED (prefer these over creating new unless user explicitly requests new ones):
{json.dumps(selected_entities, indent=2)}

FULL CONVERSATION THREAD:
{thread_content}

LATEST USER RESPONSE:
{latest_user_response}

Analyze the user's response and return a JSON object with:
1. user_intent: "CONFIRM" (proceed, using existing where available), "MODIFY" (apply specific changes), "CREATE_NEW" (create all new, ignoring existing), "CLARIFY", or "CANCEL"
2. confidence_level: "high", "medium", or "low"
3. requested_changes: Object with ONLY specific modifications requested, per entity type (empty if no change for type). For each change, specify the entity index or id and the field changes.
4. entities_to_create: Final NEW entities to create. Start from ORIGINAL PROPOSED ENTITIES, apply any requested_changes to the appropriate fields in each entity dict. Include all original unless user requests to remove.
5. entities_to_update: Existing entities to update. For types with EXISTING ENTITIES, if changes requested for them, include the entity with id and the changes dict.
6. reasoning: Brief explanation

If the user confirms without changes, set entities_to_create to ORIGINAL PROPOSED for types without existing, apply no changes.
For "MODIFY", apply changes to the relevant entities in entities_to_create or entities_to_update, and preserve unchanged entities.
For "CREATE_NEW", set entities_to_create to ORIGINAL PROPOSED (or user specified new), ignore existing, apply changes.

Return ONLY valid JSON:
{{
    "user_intent": "...",
    "confidence_level": "...",
    "requested_changes": {{
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
        "notes": [],
        "tasks": []
    }},
    "entities_to_update": {{
        "contacts": [{{ "id": "123", "changes": {{ "firstname": "new_value" }} }}],
        "companies": [],
        "deals": [],
        "meetings": [],
        "notes": [],
        "tasks": []
    }},
    "reasoning": "..."
}}
"""

    ai_analysis = get_ai_response(analysis_prompt, expect_json=True)
    
    try:
        parsed_analysis = json.loads(ai_analysis)
        user_intent = parsed_analysis.get("user_intent", "CONFIRM")
        confidence_level = parsed_analysis.get("confidence_level", "medium")
        requested_changes = parsed_analysis.get("requested_changes", {})
        entities_to_create = parsed_analysis.get("entities_to_create", {})
        entities_to_update = parsed_analysis.get("entities_to_update", {})
        reasoning = parsed_analysis.get("reasoning", "")
        
        logging.info(f"AI Analysis Results:")
        logging.info(f"User Intent: {user_intent}")
        logging.info(f"Confidence: {confidence_level}")
        logging.info(f"Reasoning: {reasoning}")
        logging.info(f"Requested Changes: {json.dumps(requested_changes, indent=2)}")
        logging.info(f"Entities to Create: {json.dumps(entities_to_create, indent=2)}")
        logging.info(f"Entities to Update: {json.dumps(entities_to_update, indent=2)}")
        
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
        
        entities_to_create = original_entities_normalized
        entities_to_update = {}
        confidence_level = "low"
        requested_changes = {}
        reasoning = "Fallback analysis due to AI parsing error"

    # Safeguard: If "CONFIRM" or "MODIFY", ensure entities_to_create has original for types without existing
    if user_intent in ["CONFIRM", "MODIFY"]:
        for type_ in ['meetings', 'notes', 'tasks']:
            if not entities_to_create.get(type_):
                entities_to_create[type_] = original_entities_normalized.get(type_, [])
        for type_ in ['contacts', 'companies', 'deals']:
            if not selected_entities.get(type_):
                if not entities_to_create.get(type_):
                    entities_to_create[type_] = original_entities_normalized.get(type_, [])

    # Determine tasks to execute
    tasks_to_execute = []
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
    if entities_to_update.get("tasks"):
        tasks_to_execute.append("update_tasks")

    mandatory_tasks = ["create_associations", "compose_response_html", "send_final_email"]
    
    results = {
        "status": "success",
        "error_message": None,
        "next_steps": list(set(tasks_to_execute + mandatory_tasks)),
        "entities_to_create": entities_to_create,
        "entities_to_update": entities_to_update,
        "selected_entities": selected_entities,
        "user_intent": user_intent,
        "confidence_level": confidence_level,
        "requested_changes": requested_changes,
        "ai_reasoning": reasoning,
        "thread_context_length": len(full_thread_history),
        "latest_response_length": len(latest_user_response),
        "analysis_timestamp": datetime.now().isoformat()
    }

    ti.xcom_push(key="analysis_results", value=results)

    # Update thread context
    updated_context = thread_context.copy()
    updated_context.update({
        "analysis_results": results,
        "full_thread_history": full_thread_history,
        "latest_user_response": latest_user_response,
        "analysis_timestamp": datetime.now().isoformat(),
        "workflow_status": "analysis_completed",
        "awaiting_reply": False
    })
    update_thread_context(thread_id, updated_context)

    logging.info(f"Analysis completed for thread {thread_id}")
    logging.info(f"User intent: {user_intent}, Tasks to execute: {tasks_to_execute}")
    return results

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

    if not to_create_deals:
        logging.info("No deals to create, skipping.")
        ti.xcom_push(key="created_deals", value=[])
        ti.xcom_push(key="deals_errors", value=[])
        return []

    prompt = f"""Create deals in HubSpot based on the provided details.

Details to create:
{json.dumps(to_create_deals, indent=2)}

IMPORTANT: Respond with ONLY a valid JSON object.

Steps:
1. For each deal detail object, invoke create_deal with the properties.
2. Collect the created deal id, deal name, deal label name, close date, deal owner name in tabular format. If any details not found, show as blank in table.

Return JSON:
{{
    "created_deals": [{{"id": "123", "details": {{ "dealName": "...", "dealLabelName": "...", "dealAmount": "...", "closeDate": "...", "dealOwnerName": "..."}}}} ...],
    "errors": ["Error message 1", "Error message 2"],
    "error": null
}}

If error, set error message and include individual errors in the errors array."""

    response = get_ai_response(prompt, expect_json=True)

    try:
        parsed = json.loads(response)
        created = parsed.get("created_deals", [])
        errors = parsed.get("errors", [])

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
    except Exception as e:
        logging.error(f"Error creating deals: {e}")
        ti.xcom_push(key="created_deals", value=[])
        ti.xcom_push(key="deals_errors", value=[str(e)])
        contexts = get_thread_context()
        if thread_id not in contexts:
            contexts[thread_id] = {}
        contexts[thread_id]["create_deals_error"] = str(e)
        contexts[thread_id]["deals_errors"] = [str(e)]
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
1. For each task, invoke create_tasks with content, due_date, owner_id.
2. Collect the created Task id, task body, last modified date, due date, task owner name in tabular format.
3. Always use `hs_timestamp` in YYYY-MM-DDTHH:MM:SSZ format while creating tasks.
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
    analysis_results = ti.xcom_pull(key="analysis_results")
    thread_id = context['dag_run'].conf.get("thread_id")
    to_update_tasks = analysis_results.get("entities_to_update", {}).get("tasks", [])

    if not to_update_tasks:
        logging.info("No tasks to update, skipping.")
        ti.xcom_push(key="updated_tasks", value=[])
        ti.xcom_push(key="tasks_update_errors", value=[])
        return []

    prompt = f"""Update tasks in HubSpot based on the provided details.

Tasks to update:
{json.dumps(to_update_tasks, indent=2)}

IMPORTANT: Respond with ONLY a valid JSON object.

Steps:
1. For each task, invoke update_task with id and changes.
2. Collect the updated Task id, task body, last modified date, due date, task owner name in tabular format.
3. Always use `hs_timestamp` in YYYY-MM-DDTHH:MM:SSZ format while updating tasks.
Return JSON:
{{
    "updated_tasks": [{{"id": "123", "details": {{ "task_details": "...", "task_owner_name": "...", "task_owner_id": "...", "due_date": "...", "priority": "..."}}}} ...],
    "errors": ["Error message 1", "Error message 2"],
    "error": null
}}

If error, set error message and include individual errors in the errors array."""

    response = get_ai_response(prompt, expect_json=True)

    try:
        parsed = json.loads(response)
        updated = parsed.get("updated_tasks", [])
        errors = parsed.get("errors", [])

        ti.xcom_push(key="updated_tasks", value=updated)
        ti.xcom_push(key="tasks_update_errors", value=errors)

        contexts = get_thread_context()
        contexts[thread_id]["updated_tasks"] = updated
        contexts[thread_id]["tasks_update_errors"] = errors
        contexts[thread_id]["update_tasks_prompt"] = prompt
        contexts[thread_id]["update_tasks_response"] = response
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)

        logging.info(f"Updated {len(updated)} tasks with {len(errors)} errors")
    except Exception as e:
        logging.error(f"Error updating tasks: {e}")
        ti.xcom_push(key="updated_tasks", value=[])
        ti.xcom_push(key="tasks_update_errors", value=[str(e)])
        contexts = get_thread_context()
        contexts[thread_id]["update_tasks_error"] = str(e)
        contexts[thread_id]["tasks_update_errors"] = [str(e)]
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)

    return updated

def create_associations(ti, **context):
    analysis_results = ti.xcom_pull(key="analysis_results")
    thread_id = context['dag_run'].conf.get("thread_id")
    conf = context["dag_run"].conf
    search_results = conf.get("search_results", {})

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

    # Extract existing entity IDs from search_results
    existing_contact_ids = [str(contact.get("contactId")) for contact in search_results.get("contact_results", {}).get("results", []) if contact.get("contactId")]
    existing_company_ids = [str(company.get("companyId")) for company in search_results.get("company_results", {}).get("results", []) if company.get("companyId")]
    existing_deal_ids = [str(deal.get("dealId")) for deal in search_results.get("deal_results", {}).get("results", []) if deal.get("dealId")]

    # Get updated entity IDs (these should be used instead of existing when available)
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

    # Priority logic: Use new IDs first, then updated IDs, then existing IDs
    # For contacts: new -> updated -> existing
    final_contact_ids = new_contact_ids if new_contact_ids else (updated_contact_ids if updated_contact_ids else existing_contact_ids)
    
    # For companies: new -> updated -> existing  
    final_company_ids = new_company_ids if new_company_ids else (updated_company_ids if updated_company_ids else existing_company_ids)
    
    # For deals: new -> updated -> existing
    final_deal_ids = new_deal_ids if new_deal_ids else (updated_deal_ids if updated_deal_ids else existing_deal_ids)
    
    # For meetings, notes, tasks: new -> updated
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

    prompt = f"""Create associations between all available HubSpot entities using the specific JSON format required by the client API.

AVAILABLE ENTITY IDS:
- Contact IDs: {final_contact_ids}
- Company IDs: {final_company_ids}  
- Deal IDs: {final_deal_ids}
- Meeting IDs: {final_meeting_ids}
- Note IDs: {final_note_ids}
- Task IDs: {final_task_ids}

Create associations using this exact JSON structure for each association request:
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
2. Create logical associations (e.g., associate contacts with companies, deals with contacts/companies, notes/tasks/meetings with related entities)
3. Use comma separation for multiple IDs in a field if needed. example: "contact_id": "123,456".

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

    try:
        parsed = json.loads(response)
        association_requests = parsed.get("association_requests", [])
        errors = parsed.get("errors", [])

        if parsed.get("error"):
            logging.warning(f"Association creation returned error: {parsed['error']}")

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
        logging.error(f"Error creating associations: {e}")
        ti.xcom_push(key="associations_created", value=[])
        ti.xcom_push(key="associations_errors", value=[str(e)])
        contexts = get_thread_context()
        contexts[thread_id]["create_associations_error"] = str(e)
        contexts[thread_id]["associations_errors"] = [str(e)]
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)

    return association_requests

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

    associations_created = ti.xcom_pull(key="associations_created", default=[])

    thread_id = context['dag_run'].conf.get("thread_id")
    email_data = context['dag_run'].conf.get("email_data", {})
    user_response_email = context['dag_run'].conf.get("user_response_email", email_data)
    selected_entities = analysis_results.get("selected_entities", {"contacts": [], "companies": [], "deals": []})
    existing_contacts = selected_entities.get("contacts", [])
    existing_companies = selected_entities.get("companies", [])
    existing_deals = selected_entities.get("deals", [])
    from_sender = user_response_email.get("headers", {}).get("From", email_data.get("headers", {}).get("From", ""))
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
            <p>Hello, {from_sender}</p>
            <p>I reviewed your request and prepared the following summary of the actions to be taken in HubSpot:</p>
        </div>
    """

    # Show existing/selected entities first (unchanged or updated) only if data exists
    if existing_contacts or updated_contacts:
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

    if existing_companies or updated_companies:
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

    if existing_deals or updated_deals:
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
    if created_contacts:
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

    if created_companies:
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

    if created_deals:
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

    if created_meetings or updated_meetings:
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
        for meeting in created_meetings:
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

    if created_notes or updated_notes:
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
        for note in created_notes:
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

    if created_tasks or updated_tasks:
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
        for task in created_tasks:
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

    sender_email = user_response_email["headers"].get("From", "")
    original_subject = user_response_email['headers'].get('Subject', 'Meeting Minutes Request')

    if not original_subject.lower().startswith('re:'):
        subject = f"Re: {original_subject}"
    else:
        subject = original_subject

    in_reply_to = user_response_email["headers"].get("Message-ID", "")
    references = user_response_email["headers"].get("References", "")

    retries = 3
    for attempt in range(retries):
        try:
            result = send_email(service, sender_email, subject, response_html, in_reply_to, references)
            if result:
                logging.info(f"Final workflow completion email sent to {sender_email}")
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
                logging.error(f"Attempt {attempt+1} failed to send email to {sender_email}")
        except Exception as e:
            logging.error(f"Attempt {attempt+1} failed to send email: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                logging.error(f"Failed to send final email to {sender_email} after {retries} attempts")
                contexts = get_thread_context()
                contexts[thread_id]["final_email_error"] = f"Failed to send final email: {str(e)}"
                with open(THREAD_CONTEXT_FILE, "w") as f:
                    json.dump(contexts, f)
                raise

    return None

def branch_to_creation_tasks(ti, **context):
    analysis_results = ti.xcom_pull(task_ids='analyze_user_response', key='analysis_results')
    mandatory_tasks = ["create_associations", "compose_response_html", "send_final_email"]
    if not analysis_results or not isinstance(analysis_results, dict):
        logging.error("Invalid or missing analysis_results from analyze_user_response")
        logging.info(f"Proceeding with mandatory tasks: {mandatory_tasks}")
        return mandatory_tasks

    tasks_to_execute = analysis_results.get("next_steps", [])
    # Ensure mandatory tasks are always included
    tasks_to_execute = list(set(tasks_to_execute + mandatory_tasks))

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
    create_associations_task >> compose_response_task >> send_final_email_task >> end_task
