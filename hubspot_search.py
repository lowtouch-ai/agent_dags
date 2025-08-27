from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import base64
import logging
import json
import os
import re
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from ollama import Client
from email import message_from_bytes
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from bs4 import BeautifulSoup
from airflow.models import Variable
from airflow.api.common.trigger_dag import trigger_dag

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
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)
    except Exception as e:
        logging.error(f"Error writing to {THREAD_CONTEXT_FILE}: {e}")

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

        thread = service.users().threads().get(userId="me", id=thread_id).execute()
        email_thread = []

        logging.info(f"Processing thread {thread_id} with {len(thread.get('messages', []))} messages")
        for msg in thread.get("messages", []):
            raw_msg = base64.urlsafe_b64decode(msg["raw"]) if "raw" in msg else None
            if not raw_msg:
                raw_message = service.users().messages().get(userId="me", id=msg["id"], format="raw").execute()
                raw_msg = base64.urlsafe_b64decode(raw_message["raw"])

            email_msg = message_from_bytes(raw_msg)
            headers = {h["name"]: h["value"] for h in msg.get("payload", {}).get("headers", [])}
            content = decode_email_payload(email_msg)
            from_address = headers.get("From", "").lower()
            is_from_bot = HUBSPOT_FROM_ADDRESS.lower() in from_address

            email_thread.append({
                "headers": headers,
                "content": content.strip(),
                "timestamp": int(msg.get("internalDate", 0)),
                "from_bot": is_from_bot,
                "message_id": msg.get("id", "")
            })

        email_thread.sort(key=lambda x: x.get("timestamp", 0))
        
        logging.info(f"Retrieved thread {thread_id} with {len(email_thread)} messages")
        for idx, email in enumerate(email_thread, 1):
            logging.info(f"Email {idx}: message_id={email['message_id']}, from={email['headers'].get('From', 'Unknown')}, timestamp={email['timestamp']}, from_bot={email['from_bot']}, content_preview={email['content'][:100]}...")

        return email_thread

    except Exception as e:
        logging.error(f"Error retrieving email thread for thread_id={thread_id}: {e}", exc_info=True)
        return []

def get_ai_response(prompt, conversation_history=None, expect_json=False):
    try:
        client = Client(host=OLLAMA_HOST, headers={'x-ltai-client': 'hubspot-v4'})
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

        if not expect_json and not ai_content.strip().startswith('<!DOCTYPE') and not ai_content.strip().startswith('<html') and not ai_content.strip().startswith('{'):
            ai_content = f"<html><body>{ai_content}</body></html>"
        else:
            pass

        return ai_content.strip()
    except Exception as e:
        logging.error(f"Error in get_ai_response: {e}")
        if expect_json:
            return f'{{"error": "Error processing AI request: {str(e)}"}}'
        else:
            return f"<html><body>Error processing AI request: {str(e)}</body></html>"

def send_email(service, recipient, subject, body, in_reply_to, references):
    try:
        msg = MIMEMultipart()
        msg["From"] = f"HubSpot Workflow <{HUBSPOT_FROM_ADDRESS}>"
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
        return None

def fetch_thread(ti, **context):
    email_data = context['dag_run'].conf.get("email_data", {})
    
    if not email_data or "id" not in email_data:
        logging.error("Invalid or missing email_data")
        ti.xcom_push(key="search_results", value={"error": "Invalid or missing email_data"})
        ti.xcom_push(key="confirmation_needed", value=False)
        return {"error": "Invalid or missing email_data"}
    
    service = authenticate_gmail()
    if not service:
        logging.error("Gmail authentication failed, skipping entity search.")
        ti.xcom_push(key="search_results", value={"error": "Gmail authentication failed"})
        ti.xcom_push(key="confirmation_needed", value=False)
        return {"error": "Gmail authentication failed"}
    
    thread_history = get_email_thread(service, email_data)
    thread_content = ""
    for idx, email in enumerate(thread_history, 1):
        content = email.get("content", "").strip()
        if content:
            soup = BeautifulSoup(content, "html.parser")
            content = soup.get_text(separator=" ", strip=True)
        thread_content += f"Email {idx} (From: {email['headers'].get('From', 'Unknown')}):\n{content}\n\n"
    
    ti.xcom_push(key="thread_content", value=thread_content)
    ti.xcom_push(key="thread_history", value=thread_history)
    ti.xcom_push(key="thread_id", value=email_data.get("threadId", "unknown"))
    ti.xcom_push(key="email_data", value=email_data)
    
    logging.info(f"Fetched thread content for thread {email_data.get('threadId')}")

def determine_owner(ti, **context):
    thread_content = ti.xcom_pull(key="thread_content")
    thread_id = ti.xcom_pull(key="thread_id")
    
    prompt = f"""You are a HubSpot API assistant. Analyze this email thread to identify the deal owner.

Email thread content:
{thread_content}

IMPORTANT: You must respond with ONLY a valid JSON object. No HTML, no explanations, no markdown formatting.

Steps to follow:
1. Invoke get_all_owners to identify the deal owner (default to 'liji' and get the owner id from get_all_owners if not specified)
2. Get the list of all available owners in tabular format

Return this exact JSON structure:
{{
    "owner_id": "159242778",
    "owner_name": "liji",
    "invalid_owner_message": "The specified owner is not valid, so assigning to default owner.",
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
        
        update_thread_context(thread_id, {
            "owner_info": parsed_json,
            "prompt_owner": prompt,
            "response_owner": response
        })
        
    except Exception as e:
        logging.error(f"Error processing owner AI response: {e}")
        default_owner = {
            "owner_id": "159242778",
            "owner_name": "liji",
            "invalid_owner_message": str(e),
            "all_owners_table": []
        }
        ti.xcom_push(key="owner_info", value=default_owner)

def search_deals(ti, **context):
    thread_content = ti.xcom_pull(key="thread_content")
    owner_info = ti.xcom_pull(key="owner_info")
    thread_id = ti.xcom_pull(key="thread_id")
    
    prompt = f"""You are a HubSpot API assistant. Search for deals based on this email thread.

Email thread content:
{thread_content}

Owner ID: {owner_info.get('owner_id', '159242778')}

IMPORTANT: You must respond with ONLY a valid JSON object. No HTML, no explanations, no markdown formatting.

Steps to follow:
1. Invoke search_deals with deal name. If deal found display deal id, deal name, deal label name, deal amount, close date, deal owner name in results.
2. If no deals found, extract potential details for new deals from the email content (e.g., deal name, amount, close date, deal owner).

Return this exact JSON structure:
{{
    "deal_results": {{
        "total": 0,
        "results": [
            {{
                "dealId": "deal_id",
                "dealName": "deal_name", 
                "dealLabelName": "deal_stage",
                "dealAmount": "amount",
                "closeDate": "close_date",
                "dealOwnerName": "owner_name"
            }}
        ]
    }},
    "new_deals": [
        {{
            "dealName": "proposed_name",
            "dealLabelName": "proposed_stage", 
            "dealAmount": "proposed_amount",
            "closeDate": "proposed_close_date",
            "dealOwnerName": "owner_name"
        }}
    ]
}}

Fill in ALL fields for each deal. Use empty string "" for missing values.

RESPOND WITH ONLY THE JSON OBJECT - NO OTHER TEXT."""
    
    response = get_ai_response(prompt, expect_json=True)
    logging.info(f"Raw AI response for deals: {response[:1000]}...")
    
    try:
        parsed_json = json.loads(response.strip())
        ti.xcom_push(key="deal_info", value=parsed_json)
        
        contexts = get_thread_context()
        contexts[thread_id]["deal_info"] = parsed_json
        contexts[thread_id]["prompt_deals"] = prompt
        contexts[thread_id]["response_deals"] = response
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)
        
    except Exception as e:
        logging.error(f"Error processing deals AI response: {e}")
        default = {
            "deal_results": {"total": 0, "results": []},
            "new_deals": []
        }
        ti.xcom_push(key="deal_info", value=default)

def search_contacts(ti, **context):
    thread_content = ti.xcom_pull(key="thread_content")
    thread_id = ti.xcom_pull(key="thread_id")
    
    prompt = f"""You are a HubSpot API assistant. Search for contacts based on this email thread.

Email thread content:
{thread_content}

IMPORTANT: You must respond with ONLY a valid JSON object. No HTML, no explanations, no markdown formatting.

Steps to follow:
1. Invoke search_contacts with contacts firstname, lastname. If contact found display ALL the contact details in results.
2. If no contacts found, extract potential details for new contacts from the email content.

Return this exact JSON structure:
{{
    "contact_results": {{
        "total": 0,
        "results": [
            {{
                "id": "contact_id",
                "firstname": "first_name",
                "lastname": "last_name", 
                "email": "email_address",
                "phone": "phone_number",
                "address": "full_address",
                "jobtitle": "job_title"
            }}
        ]
    }},
    "new_contacts": [
        {{
            "firstname": "proposed_first_name",
            "lastname": "proposed_last_name",
            "email": "proposed_email",
            "phone": "proposed_phone",
            "address": "proposed_address", 
            "jobtitle": "proposed_job_title"
        }}
    ]
}}

Fill in ALL fields for each contact. Use empty string "" for missing values.

RESPOND WITH ONLY THE JSON OBJECT - NO OTHER TEXT."""
    
    response = get_ai_response(prompt, expect_json=True)
    logging.info(f"Raw AI response for contacts: {response[:1000]}...")
    
    try:
        parsed_json = json.loads(response.strip())
        ti.xcom_push(key="contact_info", value=parsed_json)
        
        contexts = get_thread_context()
        contexts[thread_id]["contact_info"] = parsed_json
        contexts[thread_id]["prompt_contacts"] = prompt
        contexts[thread_id]["response_contacts"] = response
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)
        
    except Exception as e:
        logging.error(f"Error processing contacts AI response: {e}")
        default = {
            "contact_results": {"total": 0, "results": []},
            "new_contacts": []
        }
        ti.xcom_push(key="contact_info", value=default)

def search_companies(ti, **context):
    thread_content = ti.xcom_pull(key="thread_content")
    thread_id = ti.xcom_pull(key="thread_id")
    
    prompt = f"""You are a HubSpot API assistant. Search for companies based on this email thread.

Email thread content:
{thread_content}

IMPORTANT: You must respond with ONLY a valid JSON object. No HTML, no explanations, no markdown formatting.

Steps to follow:
1. Invoke search_companies with company name. If company found display ALL company details in results.
2. If companies found, check if the company is a partner (use relevant properties or functions).
3. If no companies found, extract potential details for new companies from the email content.

Return this exact JSON structure:
{{
    "company_results": {{
        "total": 0,
        "results": [
            {{
                "id": "company_id",
                "name": "company_name",
                "domain": "company_domain",
                "address": "full_address",
                "city": "city_name",
                "state": "state_name", 
                "zip": "zip_code",
                "country": "country_name",
                "phone": "phone_number",
                "description": "company_description",
                "type": "company_type"
            }}
        ]
    }},
    "new_companies": [
        {{
            "name": "proposed_name",
            "domain": "proposed_domain",
            "address": "proposed_address",
            "city": "proposed_city",
            "state": "proposed_state",
            "zip": "proposed_zip", 
            "country": "proposed_country",
            "phone": "proposed_phone",
            "description": "proposed_description",
            "type": "PARTNER_OR_DIRECT"
        }}
    ],
    "partner_status": null
}}

Fill in ALL fields for each company. Use empty string "" for missing values.
For partner_status, set to true/false if checked, else null.

RESPOND WITH ONLY THE JSON OBJECT - NO OTHER TEXT."""
    
    response = get_ai_response(prompt, expect_json=True)
    logging.info(f"Raw AI response for companies: {response[:1000]}...")
    
    try:
        parsed_json = json.loads(response.strip())
        ti.xcom_push(key="company_info", value=parsed_json)
        
        contexts = get_thread_context()
        contexts[thread_id]["company_info"] = parsed_json
        contexts[thread_id]["prompt_companies"] = prompt
        contexts[thread_id]["response_companies"] = response
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)
        
    except Exception as e:
        logging.error(f"Error processing companies AI response: {e}")
        default = {
            "company_results": {"total": 0, "results": []},
            "new_companies": [],
            "partner_status": None
        }
        ti.xcom_push(key="company_info", value=default)
    
def parse_notes_tasks_meeting(ti, **context):
    thread_content = ti.xcom_pull(key="thread_content")
    thread_id = ti.xcom_pull(key="thread_id")
    owner_info = ti.xcom_pull(key="owner_info", default={})
    
    prompt = f"""You are a HubSpot API assistant. Analyze this email thread to extract notes, tasks, and meeting details.

Email thread content:
{thread_content}

Default Owner ID: {owner_info.get('owner_id', '159242778')}
Default Owner Name: {owner_info.get('owner_name', 'liji')}

IMPORTANT: You must respond with ONLY a valid JSON object. No HTML, no explanations, no markdown formatting.

Extract the following information:
1. Notes - Any important discussion points, decisions made, or general notes
2. Tasks - Action items with owner and due dates
3. Meeting Details - If this is about a meeting, extract meeting information

Return this exact JSON structure:
{{
    "notes": [
        {{
            "note_content": "detailed note content",
            "timestamp": "YYYY-MM-DD HH:MM:SS",
            "note_type": "meeting_note|discussion|decision|general",
        }}
    ],
    "tasks": [
        {{
            "task_details": "detailed task description",
            "task_owner_name": "owner_name",
            "task_owner_id": "owner_id",
            "due_date": "YYYY-MM-DD",
            "priority": "high|medium|low",
        }}
    ],
    "meeting_details": {{
        "meeting_title": "meeting title",
        "start_time": "YYYY-MM-DD HH:MM:SS",
        "end_time": "YYYY-MM-DD HH:MM:SS",
        "location": "meeting location or virtual link",
        "outcome": "meeting outcome summary",
        "timestamp": "YYYY-MM-DD HH:MM:SS",
        "attendees": ["attendee1", "attendee2"],
        "meeting_type": "sales_meeting|follow_up|demo|presentation|other",
        "meeting_status": "scheduled|completed|cancelled",
}}

Guidelines:
- If no specific owner is mentioned for tasks, use the default owner
- Extract dates in proper format, use current date if not specified
- For missing information, use empty string "" or empty array []
- If no meeting details are found, return empty object for meeting_details
- Categorize notes and tasks appropriately

RESPOND WITH ONLY THE JSON OBJECT - NO OTHER TEXT."""
    
    response = get_ai_response(prompt, expect_json=True)
    logging.info(f"Raw AI response for notes/tasks/meeting: {response[:1000]}...")
    
    try:
        parsed_json = json.loads(response.strip())
        ti.xcom_push(key="notes_tasks_meeting", value=parsed_json)
        
        contexts = get_thread_context()
        if thread_id not in contexts:
            contexts[thread_id] = {}
        contexts[thread_id]["notes_tasks_meeting"] = parsed_json
        contexts[thread_id]["prompt_notes_tasks_meeting"] = prompt
        contexts[thread_id]["response_notes_tasks_meeting"] = response
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)
        
        logging.info(f"Successfully parsed {len(parsed_json.get('notes', []))} notes, {len(parsed_json.get('tasks', []))} tasks, and meeting details")
        
    except Exception as e:
        logging.error(f"Error processing notes/tasks/meeting AI response: {e}")
        default = {
            "notes": [],
            "tasks": [],
            "meeting_details": {}
        }
        ti.xcom_push(key="notes_tasks_meeting", value=default)

def compile_search_results(ti, **context):
    owner_info = ti.xcom_pull(key="owner_info")
    deal_info = ti.xcom_pull(key="deal_info")
    contact_info = ti.xcom_pull(key="contact_info")
    company_info = ti.xcom_pull(key="company_info")
    notes_tasks_meeting = ti.xcom_pull(key="notes_tasks_meeting")
    thread_id = ti.xcom_pull(key="thread_id")
    email_data = ti.xcom_pull(key="email_data")
    thread_history = ti.xcom_pull(key="thread_history")

    if not thread_history:
        logging.error(f"No thread history for thread_id={thread_id}")
        service = authenticate_gmail()
        if service:
            thread_history = get_email_thread(service, email_data)
            logging.info(f"Re-fetched thread history for thread_id={thread_id}, {len(thread_history)} emails")

    search_results = {
        "thread_id": thread_id,
        "deal_results": deal_info.get("deal_results", {"total": 0, "results": []}),
        "contact_results": contact_info.get("contact_results", {"total": 0, "results": []}),
        "company_results": company_info.get("company_results", {"total": 0, "results": []}),
        "new_entity_details": {
            "deals": deal_info.get("new_deals", []),
            "contacts": contact_info.get("new_contacts", []),
            "companies": company_info.get("new_companies", []),
            "notes": notes_tasks_meeting.get("notes", []),
            "tasks": notes_tasks_meeting.get("tasks", []),
            "meeting_details": notes_tasks_meeting.get("meeting_details", {})
        },
        "owner_id": owner_info.get("owner_id", "159242778"),
        "invalid_owner_message": owner_info.get("invalid_owner_message", None),
        "all_owners_table": owner_info.get("all_owners_table", []),
        "partner_status": company_info.get("partner_status", None)
    }

    has_existing_entities = (
        search_results["deal_results"]["total"] > 0 or
        search_results["contact_results"]["total"] > 0 or
        search_results["company_results"]["total"] > 0
    )
    has_new_entities = (
        len(search_results["new_entity_details"]["deals"]) > 0 or
        len(search_results["new_entity_details"]["contacts"]) > 0 or
        len(search_results["new_entity_details"]["companies"]) > 0 or
        len(search_results["new_entity_details"]["notes"]) > 0 or
        len(search_results["new_entity_details"]["tasks"]) > 0 or
        bool(search_results["new_entity_details"]["meeting_details"])
    )

    confirmation_needed = has_existing_entities or has_new_entities
    search_results["confirmation_needed"] = confirmation_needed

    if not has_existing_entities and not has_new_entities:
        confirmation_needed = True
        search_results["confirmation_needed"] = True
        search_results["response_html"] = "<html><body>No entities found. Please provide details for the deal, contact, company, notes, tasks, or meeting to proceed.</body></html>"

    ti.xcom_push(key="search_results", value=search_results)
    ti.xcom_push(key="confirmation_needed", value=confirmation_needed)

    try:
        update_thread_context(thread_id, {
            "search_results": search_results,
            "email_data": email_data,
            "thread_history": thread_history,
            "original_message_id": email_data["headers"].get("Message-ID", ""),
            "references": email_data["headers"].get("References", "")
        })
        stored_context = get_thread_context().get(thread_id, {})
        if not stored_context.get("thread_history"):
            logging.error(f"Failed to store thread history for thread_id={thread_id}")
        else:
            logging.info(f"Stored thread history for thread_id={thread_id} with {len(stored_context['thread_history'])} emails")
            for idx, email in enumerate(stored_context["thread_history"], 1):
                logging.info(f"Stored Email {idx}: message_id={email['message_id']}, from={email['headers'].get('From', 'Unknown')}")
    except Exception as e:
        logging.error(f"Error updating thread context for thread_id={thread_id}: {e}")
        raise

    logging.info(f"Compiled search results: {json.dumps(search_results, indent=2)}")
    return search_results

def compose_confirmation_email(ti, **context):
    search_results = ti.xcom_pull(key="search_results")
    email_data = ti.xcom_pull(key="email_data")
    confirmation_needed = ti.xcom_pull(key="confirmation_needed", default=False)
    
    if not confirmation_needed:
        logging.info("No confirmation needed, proceeding to trigger next DAG.")
        return "No confirmation needed"
    
    email_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            table { border-collapse: collapse; width: 100%; margin: 20px 0; }
            th, td { border: 1px solid #ddd; padding: 12px; text-align: left; }
            th { background-color: #f2f2f2; font-weight: bold; }
            h3 { color: #333; margin-top: 30px; margin-bottom: 15px; }
            .greeting { margin-bottom: 20px; }
            .closing { margin-top: 30px; }
        </style>
    </head>
    <body>
        <div class="greeting">
            <p>Hello,</p>
            <p>Thank you for your request to log meeting minutes. I've analyzed the email thread and found the following entities and details in HubSpot. Please review and confirm the details below:</p>
        </div>
    """
    
    # Existing Contact Details Section
    contact_results = search_results.get("contact_results", {})
    
    if contact_results.get("total", 0) > 0:
        email_content += """
        <h3>Existing Contact Details</h3>
        <table>
            <thead>
                <tr>
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
        
        for contact in contact_results.get("results", []):
            firstname = contact.get("firstname", "")
            lastname = contact.get("lastname", "")
            email = contact.get("email", "")
            phone = contact.get("phone", "")
            address = contact.get("address", "")
            jobtitle = contact.get("jobtitle", "")
            
            email_content += f"""
                <tr>
                    <td>{firstname}</td>
                    <td>{lastname}</td>
                    <td>{email}</td>
                    <td>{phone}</td>
                    <td>{address}</td>
                    <td>{jobtitle}</td>
                </tr>
            """
        
        email_content += """
            </tbody>
        </table>
        <hr>
        """
    
    # Existing Company Details Section
    company_results = search_results.get("company_results", {})
    
    if company_results.get("total", 0) > 0:
        email_content += """
        <h3>Existing Company Details</h3>
        <table>
            <thead>
                <tr>
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
        
        for company in company_results.get("results", []):
            name = company.get("name", "")
            domain = company.get("domain", "")
            address = company.get("address", "")
            city = company.get("city", "")
            state = company.get("state", "")
            zip_code = company.get("zip", "")
            country = company.get("country", "")
            phone = company.get("phone", "")
            description = company.get("description", "")
            comp_type = company.get("type", "")
            
            email_content += f"""
                <tr>
                    <td>{name}</td>
                    <td>{domain}</td>
                    <td>{address}</td>
                    <td>{city}</td>
                    <td>{state}</td>
                    <td>{zip_code}</td>
                    <td>{country}</td>
                    <td>{phone}</td>
                    <td>{description}</td>
                    <td>{comp_type}</td>
                </tr>
            """
        
        email_content += """
            </tbody>
        </table>
        <hr>
        """
    
    # Existing Deal Details Section
    deal_results = search_results.get("deal_results", {})
    
    if deal_results.get("total", 0) > 0:
        email_content += """
        <h3>Existing Deal Details</h3>
        <table>
            <thead>
                <tr>
                    <th>Deal Name</th>
                    <th>Deal Stage Label</th>
                    <th>Deal Amount</th>
                    <th>Close Date</th>
                    <th>Deal Owner Name</th>
                </tr>
            </thead>
            <tbody>
        """
        
        for deal in deal_results.get("results", []):
            deal_name = deal.get("dealName", "")
            stage_label = deal.get("dealLabelName", "")
            amount = deal.get("dealAmount", "")
            close_date = deal.get("closeDate", "")
            owner_name = deal.get("dealOwnerName", "")
            
            email_content += f"""
                <tr>
                    <td>{deal_name}</td>
                    <td>{stage_label}</td>
                    <td>{amount}</td>
                    <td>{close_date}</td>
                    <td>{owner_name}</td>
                </tr>
            """
        
        email_content += """
            </tbody>
        </table>
        <hr>
        """
    
    # Objects to be Created Section
    new_contacts = search_results.get("new_entity_details", {}).get("contacts", [])
    new_companies = search_results.get("new_entity_details", {}).get("companies", [])
    new_deals = search_results.get("new_entity_details", {}).get("deals", [])
    notes = search_results.get("new_entity_details", {}).get("notes", [])
    tasks = search_results.get("new_entity_details", {}).get("tasks", [])
    meeting_details = search_results.get("new_entity_details", {}).get("meeting_details", {})
    
    if new_contacts or new_companies or new_deals or notes or tasks or meeting_details:
        email_content += """
        <h3>Objects to be Created</h3>
        """
        
        # New Contacts
        if new_contacts:
            email_content += """
            <h4>New Contacts</h4>
            <table>
                <thead>
                    <tr>
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
            
            for contact in new_contacts:
                firstname = contact.get("firstname", "")
                lastname = contact.get("lastname", "")
                email = contact.get("email", "")
                phone = contact.get("phone", "")
                address = contact.get("address", "")
                jobtitle = contact.get("jobtitle", "")
                
                email_content += f"""
                    <tr>
                        <td>{firstname}</td>
                        <td>{lastname}</td>
                        <td>{email}</td>
                        <td>{phone}</td>
                        <td>{address}</td>
                        <td>{jobtitle}</td>
                    </tr>
                """
            
            email_content += """
                </tbody>
            </table>
            <hr>
            """
        
        # New Companies
        if new_companies:
            email_content += """
            <h4>New Companies</h4>
            <table>
                <thead>
                    <tr>
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
            
            for company in new_companies:
                name = company.get("name", "")
                domain = company.get("domain", "")
                address = company.get("address", "")
                city = company.get("city", "")
                state = company.get("state", "")
                zip_code = company.get("zip", "")
                country = company.get("country", "")
                phone = company.get("phone", "")
                description = company.get("description", "")
                comp_type = company.get("type", "")
                
                email_content += f"""
                    <tr>
                        <td>{name}</td>
                        <td>{domain}</td>
                        <td>{address}</td>
                        <td>{city}</td>
                        <td>{state}</td>
                        <td>{zip_code}</td>
                        <td>{country}</td>
                        <td>{phone}</td>
                        <td>{description}</td>
                        <td>{comp_type}</td>
                    </tr>
                """
            
            email_content += """
                </tbody>
            </table>
            <hr>
            """
        
        # New Deals
        if new_deals:
            email_content += """
            <h4>New Deals</h4>
            <table>
                <thead>
                    <tr>
                        <th>Deal Name</th>
                        <th>Deal Stage Label</th>
                        <th>Deal Amount</th>
                        <th>Close Date</th>
                        <th>Deal Owner Name</th>
                    </tr>
                </thead>
                <tbody>
            """
            
            for deal in new_deals:
                deal_name = deal.get("dealName", "")
                stage_label = deal.get("dealLabelName", "")
                amount = deal.get("dealAmount", "")
                close_date = deal.get("closeDate", "")
                owner_name = deal.get("dealOwnerName", "")
                
                email_content += f"""
                    <tr>
                        <td>{deal_name}</td>
                        <td>{stage_label}</td>
                        <td>{amount}</td>
                        <td>{close_date}</td>
                        <td>{owner_name}</td>
                    </tr>
                """
            
            email_content += """
                </tbody>
            </table>
            <hr>
            """
        
        # Notes
        if notes:
            email_content += """
            <h4>Notes</h4>
            <table>
                <thead>
                    <tr>
                        <th>Note Content</th>
                        <th>Timestamp</th>
                    </tr>
                </thead>
                <tbody>
            """
            
            for note in notes:
                note_content = note.get("note_content", "")
                timestamp = note.get("timestamp", "")
                
                email_content += f"""
                    <tr>
                        <td>{note_content}</td>
                        <td>{timestamp}</td>
                    </tr>
                """
            
            email_content += """
                </tbody>
            </table>
            <hr>
            """
        
        # Tasks
        if tasks:
            email_content += """
            <h4>Tasks</h4>
            <table>
                <thead>
                    <tr>
                        <th>Task Details</th>
                        <th>Owner Name</th>
                        <th>Due Date</th>
                        <th>Priority</th>
                        <th>Status</th>
                    </tr>
                </thead>
                <tbody>
            """
            
            for task in tasks:
                task_details = task.get("task_details", "")
                task_owner_name = task.get("task_owner_name", "")
                due_date = task.get("due_date", "")
                priority = task.get("priority", "")
                status = task.get("status", "")
                
                email_content += f"""
                    <tr>
                        <td>{task_details}</td>
                        <td>{task_owner_name}</td>
                        <td>{due_date}</td>
                        <td>{priority}</td>
                        <td>{status}</td>
                    </tr>
                """
            
            email_content += """
                </tbody>
            </table>
            <hr>
            """
        
        # Meeting Details
        if meeting_details:
            email_content += """
            <h4>Meeting Details</h4>
            <table>
                <thead>
                    <tr>
                        <th>Meeting Title</th>
                        <th>Start Time</th>
                        <th>End Time</th>
                        <th>Location</th>
                        <th>Outcome</th>
                        <th>Timestamp</th>
                        <th>Attendees</th>
                    </tr>
                </thead>
                <tbody>
            """
            
            meeting_title = meeting_details.get("meeting_title", "")
            start_time = meeting_details.get("start_time", "")
            end_time = meeting_details.get("end_time", "")
            location = meeting_details.get("location", "")
            outcome = meeting_details.get("outcome", "")
            timestamp = meeting_details.get("timestamp", "")
            attendees = ", ".join(meeting_details.get("attendees", []))
            
            email_content += f"""
                <tr>
                    <td>{meeting_title}</td>
                    <td>{start_time}</td>
                    <td>{end_time}</td>
                    <td>{location}</td>
                    <td>{outcome}</td>
                    <td>{timestamp}</td>
                    <td>{attendees}</td>
                </tr>
            """
            
            email_content += """
                </tbody>
            </table>
            <hr>
            """
    
    # Reason for Deal Owner Chosen Section
    all_owners = search_results.get("all_owners_table", [])
    chosen_owner_id = search_results.get("owner_id", "159242778")
    invalid_owner_msg = search_results.get("invalid_owner_message")
    
    email_content += """
        <h3>Reason for Deal Owner Chosen</h3>
    """
    
    if invalid_owner_msg:
        email_content += f"<p><strong>Note:</strong> {invalid_owner_msg}</p>"
    
    if all_owners:
        email_content += """
        <p>Available owners from get_all_owners:</p>
        <table>
            <thead>
                <tr>
                    <th>Owner ID</th>
                    <th>Owner Name</th>
                    <th>Owner Email</th>
                </tr>
            </thead>
            <tbody>
        """
        
        for owner in all_owners:
            owner_id = owner.get("id", "")
            owner_name = owner.get("name", "")
            owner_email = owner.get("email", "")
            
            row_style = ' style="background-color: #ffffcc;"' if owner_id == chosen_owner_id else ""
            
            email_content += f"""
                <tr{row_style}>
                    <td>{owner_id}</td>
                    <td>{owner_name}</td>
                    <td>{owner_email}</td>
                </tr>
            """
        
        email_content += """
            </tbody>
        </table>
        """
    
    email_content += """
        <div class="closing">
            <p><strong>Instructions:</strong></p>
            <ul>
                <li>If you want to proceed with the existing entities and create the new objects shown above, please reply with "PROCEED WITH EXISTING"</li>
                <li>If you want to create new entities or modify the proposed objects, please reply with "CREATE NEW" along with any corrections to the details</li>
                <li>If you need to modify any information, please specify the changes in your reply</li>
            </ul>
            <p>The workflow will continue once you confirm your choice.</p>
            <p>If you have any questions or need further assistance, please don't hesitate to ask.</p>
            <p>Best regards,<br>
            HubSpot Workflow Agent<br>
            hubspot-agent-9201@lowtouch.ai</p>
        </div>
    </body>
    </html>
    """
    
    ti.xcom_push(key="confirmation_email", value=email_content)
    logging.info(f"Confirmation email composed with structured tables including notes, tasks, meeting details, and new entities under 'Objects to be Created'")
    return email_content

def send_confirmation_email(ti, **context):
    email_data = ti.xcom_pull(key="email_data")
    confirmation_email = ti.xcom_pull(key="confirmation_email")
    confirmation_needed = ti.xcom_pull(key="confirmation_needed", default=False)
    search_results = ti.xcom_pull(key="search_results", default={})
    thread_id = search_results.get("thread_id", email_data.get("threadId", "unknown"))
    
    if not confirmation_needed:
        return "No confirmation email needed"
    
    service = authenticate_gmail()
    if not service:
        logging.error("Gmail authentication failed, cannot send confirmation email.")
        return "Gmail authentication failed"
    
    sender_email = email_data["headers"].get("From", "")
    original_message_id = email_data["headers"].get("Message-ID", "")
    references = email_data["headers"].get("References", "")
    
    if original_message_id and original_message_id not in references:
        references = f"{references} {original_message_id}".strip()
    
    subject = f"Re: {email_data['headers'].get('Subject', 'Meeting Minutes Request')}"
    
    result = send_email(service, sender_email, subject, confirmation_email, original_message_id, references)
    if result:
        logging.info(f"Confirmation email sent to {sender_email}")
        
        confirmation_thread_id = result.get("threadId", thread_id)
        confirmation_message_id = result.get("id", "")
        
        updated_context = {
            "search_results": search_results,
            "email_data": email_data,
            "confirmation_needed": True,
            "confirmation_sent": True,
            "confirmation_email": confirmation_email,
            "confirmation_timestamp": datetime.now().isoformat(),
            "awaiting_reply": True,
            "original_subject": email_data['headers'].get('Subject', ''),
            "original_message_id": original_message_id,
            "confirmation_message_id": confirmation_message_id,
            "confirmation_thread_id": confirmation_thread_id,
            "references": references,
            "sender_email": sender_email
        }
        
        update_thread_context(thread_id, updated_context)
        if confirmation_thread_id != thread_id:
            update_thread_context(confirmation_thread_id, updated_context)
            logging.info(f"Copied context to new thread_id={confirmation_thread_id}")
        
        logging.info(f"Thread context updated for thread {thread_id}")
        logging.info(f"Context flags: confirmation_sent=True, awaiting_reply=True")
        
        try:
            contexts = get_thread_context()
            if thread_id in contexts:
                logging.info(f"Verified thread context stored: {json.dumps(contexts[thread_id], indent=2)}")
            else:
                logging.error(f"Thread context not found after update!")
        except Exception as e:
            logging.error(f"Error verifying thread context: {e}")
            
    else:
        logging.error("Failed to send confirmation email")
    return result

def decide_trigger(ti, **context):
    confirmation_needed = ti.xcom_pull(key="confirmation_needed", default=False)
    if not confirmation_needed:
        search_results = ti.xcom_pull(key="search_results")
        thread_id = ti.xcom_pull(key="thread_id")
        logging.info("Triggering continuation DAG without confirmation")
        ti.xcom_push(key="trigger_conf", value={
            "search_results": search_results,
            "thread_id": thread_id,
            "email_data": context['dag_run'].conf.get("email_data", {})
        })
        return "trigger_next_dag"
    logging.info("Confirmation needed, ending DAG")
    return "end_workflow"

def trigger_continuation_dag(ti, **context):
    trigger_conf = ti.xcom_pull(task_ids='decide_trigger', key='trigger_conf')
    if not trigger_conf:
        logging.error("No trigger_conf found in XCom")
        raise ValueError("No trigger_conf found in XCom")
    
    logging.info(f"Triggering hubspot_meeting_minutes_continue with conf: {trigger_conf}")
    trigger_dag(
        dag_id="hubspot_meeting_minutes_continue",
        run_id=f"triggered_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        conf=trigger_conf,
        execution_date=None,
        replace_microseconds=False
    )
    logging.info("Successfully triggered hubspot_meeting_minutes_continue")

readme_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'hubspot_search_entities.md')
readme_content = "HubSpot Meeting Minutes Search and Confirmation DAG"
try:
    with open(readme_path, 'r') as file:
        readme_content = file.read()
except FileNotFoundError:
    pass

with DAG(
    "hubspot_search_entities",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    doc_md=readme_content,
    tags=["hubspot", "meeting_minutes", "search"]
) as dag:

    fetch_thread_task = PythonOperator(
        task_id="fetch_thread",
        python_callable=fetch_thread,
        provide_context=True
    )

    determine_owner_task = PythonOperator(
        task_id="determine_owner",
        python_callable=determine_owner,
        provide_context=True
    )

    search_deals_task = PythonOperator(
        task_id="search_deals",
        python_callable=search_deals,
        provide_context=True
    )

    search_contacts_task = PythonOperator(
        task_id="search_contacts",
        python_callable=search_contacts,
        provide_context=True
    )

    search_companies_task = PythonOperator(
        task_id="search_companies",
        python_callable=search_companies,
        provide_context=True
    )

    parse_notes_tasks_meeting_task = PythonOperator(
        task_id="parse_notes_tasks_meeting",
        python_callable=parse_notes_tasks_meeting,
        provide_context=True
    )

    compile_results_task = PythonOperator(
        task_id="compile_search_results",
        python_callable=compile_search_results,
        provide_context=True
    )

    compose_email_task = PythonOperator(
        task_id="compose_confirmation_email",
        python_callable=compose_confirmation_email,
        provide_context=True
    )

    send_email_task = PythonOperator(
        task_id="send_confirmation_email",
        python_callable=send_confirmation_email,
        provide_context=True
    )

    decide_trigger_task = BranchPythonOperator(
        task_id="decide_trigger",
        python_callable=decide_trigger,
        provide_context=True
    )

    trigger_task = PythonOperator(
        task_id="trigger_next_dag",
        python_callable=trigger_continuation_dag,
        provide_context=True
    )

    end_workflow = DummyOperator(
        task_id="end_workflow"
    )

    fetch_thread_task >> determine_owner_task >> search_deals_task >> search_contacts_task >> search_companies_task >> parse_notes_tasks_meeting_task >> compile_results_task >> compose_email_task >> send_email_task >> decide_trigger_task
    decide_trigger_task >> trigger_task
    decide_trigger_task >> end_workflow
