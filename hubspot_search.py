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
TASK_THRESHOLD = 15
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
                content = f.read().strip()
                if content:  # Check if file is not empty
                    return json.loads(content)
                else:
                    logging.warning(f"{THREAD_CONTEXT_FILE} is empty, returning empty dict")
                    return {}
        else:
            logging.info(f"{THREAD_CONTEXT_FILE} does not exist, returning empty dict")
            return {}
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON in {THREAD_CONTEXT_FILE}: {e}")
        return {}
    except Exception as e:
        logging.error(f"Error reading {THREAD_CONTEXT_FILE}: {e}")
        return {}

def update_thread_context(thread_id, context_data):
    os.makedirs(os.path.dirname(THREAD_CONTEXT_FILE), exist_ok=True)
    try:
        contexts = get_thread_context()
        if contexts is None:
            logging.warning(f"get_thread_context returned None, initializing empty dict")
            contexts = {}
        contexts[thread_id] = context_data
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)
    except Exception as e:
        logging.error(f"Error writing to {THREAD_CONTEXT_FILE}: {e}")
        raise  # Re-raise to ensure the task fails and is retried

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

def analyze_thread_entities(ti, **context):
    """Analyze thread content to determine which entities need to be searched and if a summary is requested."""
    thread_content = ti.xcom_pull(key="thread_content")
    thread_id = ti.xcom_pull(key="thread_id")
    
    prompt = f"""You are a HubSpot API assistant. Analyze this email thread to determine which entities (deals, contacts, companies) are mentioned or need to be processed, and whether the user is requesting a summary of a client or deal before their next meeting.

Email thread content:
{thread_content}

IMPORTANT: You must respond with ONLY a valid JSON object. No HTML, no explanations, no markdown formatting.

Analyze the content and determine:
1. Is the user requesting a summary of a client or deal before their next meeting? Look for phrases like "summarize details for contact name", "summary for deal name", or explicit mentions of preparing for an upcoming meeting.
2. If a summary is requested, set ALL other flags (search_deals, search_contacts, search_companies, parse_notes, parse_tasks, parse_meetings) to false.
3. If no summary is requested, determine the following:
   - Are deals mentioned, discussed, or need to be created? Deals are only created if the client is interested to move forward.
   - Parse the contact name; if found, contacts need to be processed.
   - Are companies mentioned, discussed, or need to be created? If a company is specified, it needs to be processed even if not explicitly asked. Even if companies are already created or associated, they need to be processed.
   - Do we need to create notes? Notes are created only if there is a discussion held with the client.
   - Do we need to create tasks? Tasks are created only if there is a follow-up action required with the client.
   - Do we need to create meetings? Meetings are created only if a meeting was held and meeting details are given (e.g., date, time, duration, timezone).

Return this exact JSON structure:
{{
    "search_deals": true/false,
    "search_contacts": true/false,
    "search_companies": true/false,
    "parse_notes": true/false,
    "parse_tasks": true/false,
    "parse_meetings": true/false,
    "request_summary": true/false,
    "deals_reason": "explanation why deals need processing or not",
    "contacts_reason": "explanation why contacts need processing or not",
    "companies_reason": "explanation why companies need processing or not",
    "notes_reason": "explanation why notes need processing or not",
    "tasks_reason": "explanation why tasks need processing or not",
    "meetings_reason": "explanation why meetings need processing or not",
    "summary_reason": "explanation why a summary is requested or not"
}}

RESPOND WITH ONLY THE JSON OBJECT - NO OTHER TEXT."""

    response = get_ai_response(prompt, expect_json=True)
    logging.info(f"Raw AI response for entity analysis: {response[:1000]}...")

    try:
        parsed_json = json.loads(response.strip())
        ti.xcom_push(key="entity_search_flags", value=parsed_json)
        
        # Store in thread context
        contexts = get_thread_context()
        if thread_id not in contexts:
            contexts[thread_id] = {}
        contexts[thread_id]["entity_search_flags"] = parsed_json
        contexts[thread_id]["prompt_entity_analysis"] = prompt
        contexts[thread_id]["response_entity_analysis"] = response
        
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)
            
        logging.info(f"Entity search flags: deals={parsed_json.get('search_deals')}, contacts={parsed_json.get('search_contacts')}, companies={parsed_json.get('search_companies')}, request_summary={parsed_json.get('request_summary')}")
        
    except Exception as e:
        logging.error(f"Error processing entity analysis AI response: {e}")
        # Default to searching all entities and no summary if analysis fails
        default = {
            "search_deals": True,
            "search_contacts": True,
            "search_companies": True,
            "parse_notes": True,
            "parse_tasks": True,
            "parse_meetings": True,
            "request_summary": False,
            "deals_reason": "Analysis failed, defaulting to search",
            "contacts_reason": "Analysis failed, defaulting to search",
            "companies_reason": "Analysis failed, defaulting to search",
            "notes_reason": "Analysis failed, defaulting to parse",
            "tasks_reason": "Analysis failed, defaulting to parse",
            "meetings_reason": "Analysis failed, defaulting to parse",
            "summary_reason": "Analysis failed, no summary requested"
        }
        ti.xcom_push(key="entity_search_flags", value=default)

def summarize_engagement_details(ti, **context):
    """Retrieve and summarize engagement details based on thread content and email data."""
    entity_flags = ti.xcom_pull(key="entity_search_flags", default={})
    if not entity_flags.get("request_summary", False):
        logging.info("No summary requested, skipping engagement summary")
        ti.xcom_push(key="engagement_summary", value={})  # Or {"skipped": True}
        return
    thread_content = ti.xcom_pull(key="thread_content")
    thread_id = ti.xcom_pull(key="thread_id")
    email_data = ti.xcom_pull(key="email_data", default={})

    prompt = f"""You are a HubSpot API assistant. Summarize engagement details based on the provided email thread content. Parse the contact name, deal ID (if specified), company name, and other relevant details directly from the thread content.

Thread Content:
{thread_content}

Email Subject:
{email_data.get("headers", {}).get("Subject", "")}

IMPORTANT: Respond with ONLY a valid JSON object. No HTML, no explanations, no markdown formatting.

Steps:
- **When the user requests "Summarize details for {{contact name to search}}", retrieve the `contactId` for the specified contact**.
  - Check if the user has provided a specific `dealId` or deal identifier in the request.
  - If a specific `dealId` is provided, retrieve only the engagements associated with that `dealId` and summarize the details in the specified format for that deal.
  - Invoke `get_engagements_by_object_id` tool to get engagement of a perticular `dealId`
  - List the Associated Deals If no `dealId` is provided and the contact has only one associated deal, retrieve the engagements for that `dealId` and summarize the details in the specified format.
  - Ensure the agent does not retrieve or process engagements for any deals other than the user-specified `dealId` or the single deal when applicable.
  - Summarize the details in the bellow format, ensuring clarity and relevance for the selected deal only.
  **Output format** :
    - **Contact**: {{contact_name}}, Email: {{email}}, Company: {{company_name}} in tabular format.
    - **Deal**: {{Deal_name}}, Stage: {{Deal_stage}}, Amount: {{Deal_Amount}}, Close Date: {{Deal_close_date}} in tabular format.
    - **Company**: {{Company_name}}, Domain: {{email}} in tabular format
    - **Engagements**:Generate a engagement summary based on the notes retrieved, including the discussed things. Ensure the summary is structured, concise yet thorough, and includes at least 5-7 sentences to cover all critical aspects.
      - Display the latest meeting held, if available.
      - Do not show the retrieved noted as it is. Analyze the notes and make a summary then display the generated summary.
      - Never show tasks.
      - Never show Note ID.      
    - **Detailed Deal Summary**: Generate a comprehensive deal summary for each engagement, including the company name, deal stage, deal amount, key stakeholders, timeline, and any relevant risks or opportunities. Ensure the summary is structured, concise yet thorough, and includes at least 3-5 sentences to cover all critical aspects of the deal.
    - **Comprehensive Call Strategy**: Develop a detailed call strategy for pitching the Pro plan, tailored to the specific company and deal context. Include the following:
      - A clear outline of the pitch, highlighting the Pro plan’s key features and benefits relevant to the company’s needs.
      - The deal amount and how it aligns with the client’s budget or value proposition.
      - Reference to previous interactions (e.g., prior calls, emails, or meetings) to personalize the approach and build continuity.
      - Anticipated objections and tailored responses to address them.
      - A step-by-step plan for the call, including opening, value proposition, handling questions, and closing with clear next steps.
      - Ensure the strategy is actionable, spans at least 3-5 paragraphs, and incorporates specific examples or data where applicable.

Return this exact JSON structure:
{{
    "contact_summary": {{
        "contact_name": "parsed_contact_name",
        "email": "inferred_email_from_thread",
        "company_name": "inferred_company_name"
    }},
    "deal_summary": {{
        "deal_name": "inferred_deal_name",
        "stage": "inferred_deal_stage",
        "amount": "inferred_amount",
        "close_date": "inferred_close_date"
    }},
    "company_summary": {{
        "company_name": "inferred_company_name",
        "domain": "inferred_domain"
    }},
    "engagement_summary": "5-7 sentence summary of engagements, including discussed topics and latest meeting if available",
    "detailed_deal_summary": "3-5 sentence detailed summary of the deal",
    "call_strategy": "3-5 paragraph call strategy for pitching the Pro plan"
}}

Guidelines:
- Parse contact name, deal ID (if any), company name, and other details directly from thread content or email subject.
- Infer all fields from thread content and email data; use empty string "" for missing values.
- Do not use contact, company, or deal info from XCom; rely solely on thread content and email data.
- If no contact name is found, return {{"error": "No contact name found in thread content"}}.
- Ensure summaries and call strategy are tailored to the context in the thread.

RESPOND WITH ONLY THE JSON OBJECT - NO OTHER TEXT."""

    response = get_ai_response(prompt, expect_json=True)
    logging.info(f"Raw AI response for engagement summary: {response[:1000]}...")

    try:
        parsed_json = json.loads(response.strip())
        ti.xcom_push(key="engagement_summary", value=parsed_json)

        # Update thread context
        contexts = get_thread_context()
        contexts[thread_id]["engagement_summary"] = parsed_json
        contexts[thread_id]["prompt_engagement_summary"] = prompt
        contexts[thread_id]["response_engagement_summary"] = response
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)

        logging.info(f"Engagement summary generated for thread_id: {thread_id}")
    except Exception as e:
        logging.error(f"Error processing engagement summary AI response: {e}")
        ti.xcom_push(key="engagement_summary", value={"error": f"Error processing engagement summary: {str(e)}"})

def determine_owner(ti, **context):
    thread_content = ti.xcom_pull(key="thread_content")
    thread_id = ti.xcom_pull(key="thread_id")

    prompt = f"""You are a HubSpot API assistant. Analyze this email thread to identify the deal owner and task owners.

Email thread content:
{thread_content}

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
    - For each task owner:
        - If task owner is NOT specified for a task:
            - Default to: "liji"
            - Message: "No task owner specified for task [task_index], so assigning to default owner liji."
        - If task owner IS specified but NOT found in available owners list:
            - Default to: "liji"
            - Message: "The specified task owner '[parsed_owner]' for task [task_index] is not valid, so assigning to default owner liji."
        - If task owner IS specified and IS found in available owners list:
            - Use the matched owner (with correct casing from the available owners list)
            - Message: "Task owner for task [task_index] specified as [matched_owner_name]"
5. Return a list of task owners with their validation details.

Return this exact JSON structure:
{{
    "deal_owner_id": "159242778",
    "deal_owner_name": "liji",
    "deal_owner_message": "No deal owner specified, so assigning to default owner liji." OR "The specified deal owner is not valid, so assigning to default owner liji." OR "Deal owner specified as [name]",
    "task_owners": [
        {{
            "task_index": 1,
            "task_owner_id": "159242778",
            "task_owner_name": "liji",
            "task_owner_message": "No task owner specified for task [task_index], so assigning to default owner liji." OR "The specified task owner is not valid for task [task_index], so assigning to default owner liji." OR "Task owner for task [task_index] specified as [name]"
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

        update_thread_context(thread_id, {
            "owner_info": parsed_json,
            "prompt_owner": prompt,
            "response_owner": response
        })

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

def search_deals(ti, **context):
    entity_flags = ti.xcom_pull(key="entity_search_flags", default={})
    if not entity_flags.get("search_deals", True):
        logging.info(f"Skipping deals search: {entity_flags.get('deals_reason', 'Not mentioned in thread')}")
        ti.xcom_push(key="deal_info", value={
            "deal_results": {"total": 0, "results": []},
            "new_deals": []
        })
        return
    thread_content = ti.xcom_pull(key="thread_content")
    owner_info = ti.xcom_pull(key="owner_info")
    thread_id = ti.xcom_pull(key="thread_id")
    deal_owner_id = owner_info.get('deal_owner_id', '159242778')
    deal_owner_name = owner_info.get('deal_owner_name', 'liji')

    prompt = f"""You are a HubSpot API assistant. Your task is to search for existing deals based on the email thread content and, if none are found, propose new deal details only if the email clearly indicates a new opportunity (e.g., the client shows interest in moving forward). You dont have the ability to create deals.

Email thread content:
{thread_content}
Validated Deal Owner ID: {deal_owner_id}
Validated Deal Owner Name: {deal_owner_name}

IMPORTANT: Respond with ONLY a valid JSON object. No explanations, no markdown, no other text.

Steps:
1. Search for existing deals using the deal name extracted from the email content.
2. If deals are found, include them in 'deal_results' with: dealId, dealName, dealLabelName (e.g., 'Appointment Scheduled' for stage 'appointmentscheduled'), dealAmount, closeDate, dealOwnerName.
3. If no deals are found, check if the email clearly indicates a new deal. If yes, propose new deals in 'new_deals'. If not, leave 'new_deals' as an empty list.
4. Strictly follow these rules, for new deal names, :
   - Extract the Client Name (company or individual being sold to) from the email.
   - Check if it's a direct deal (no partner) or partner deal (partner or intermediary mentioned).
   - Direct deal: <Client Name>-<Deal Name>
   - Partner deal: <Partner Name>-<Client Name>-<Deal Name>
   - Use the Deal Name from the email if specified; otherwise, create a concise one based on the description (e.g., product or service discussed).
5. For new deals, use the validated deal owner name in dealOwnerName.
6. Propose an additional new deal if the email explicitly requests opening a second deal, even if one exists.
7. Use dealLabelName for deal stages (e.g., 'Appointment Scheduled').
8. Fill all fields in the JSON. Use empty string "" for any missing values.

Return exactly this JSON structure:
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
            "dealName": "<Client Name>-<Deal Name>" OR "<Partner Name>-<Client Name>-<Deal Name>",
            "dealLabelName": "proposed_stage",
            "dealAmount": "proposed_amount",
            "closeDate": "proposed_close_date",
            "dealOwnerName": "{deal_owner_name}"
        }}
    ]
}}

RESPOND WITH ONLY THE JSON OBJECT."""

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
    entity_flags = ti.xcom_pull(key="entity_search_flags", default={})
    if not entity_flags.get("search_contacts", True):
        logging.info(f"Skipping contacts search: {entity_flags.get('contacts_reason', 'Not mentioned in thread')}")
        ti.xcom_push(key="contact_info", value={
            "contact_results": {"total": 0, "results": []},
            "new_contacts": []
        })
        return
    thread_content = ti.xcom_pull(key="thread_content")
    thread_id = ti.xcom_pull(key="thread_id")

    prompt = f"""You are a HubSpot API assistant. Search for contacts based on this email thread.

Email thread content:
{thread_content}

IMPORTANT: You must respond with ONLY a valid JSON object. No HTML, no explanations, no markdown formatting. You are only capable of searching contacts amd you do not have the ability to create contacts.

Important Steps to follow (execute in order):
1. Extract potential contact names from the thread. Apply these exclusion rules:
   - EXCLUDE deal owners mentioned with "assign to", "owner", or similar assignment language
   - EXCLUDE internal team members, senders, or system users (e.g., skip "From: John Doe <john@company.com>")
   - EXCLUDE names that are clearly role/department indicators in parentheses like "(Ops)", "(Finance)", "(IT)"
   - INCLUDE actual contact names that appear to be external stakeholders or clients
   
   For valid contacts:
   - Parse contact names and handle role indicators properly:
     * "Neha (Ops)" → firstname="Neha", lastname="" (ignore the role indicator)
     * "Riya (Finance)" → firstname="Riya", lastname="" (ignore the role indicator)
     * "John Smith" → firstname="John", lastname="Smith"
   - Split names into firstname/lastname:
     * Single word (e.g., "Neha"): firstname="Neha", lastname="" (empty string)
     * Two+ words (e.g., "Neha Khan" or "Riya Priya Sharma"): firstname=first word ("Neha" or "Riya"), lastname=rest joined ("Khan" or "Priya Sharma")
     * Multiple contacts: List separately, e.g., [{{"firstname": "Neha", "lastname": "Khan"}}, {{"firstname": "Riya", "lastname": ""}}]
   - If no valid contact names found after exclusions, skip to step 6.

2. For each extracted name, decide search criteria:
   - If lastname is non-empty: Use 'both' template (exact match on both fields)
   - If lastname is empty: Use 'firstname_only' template (search on firstname only)
   - Output one decision per contact in reasoning_summary.

3. Always invoke HubSpot search_contacts API for each contact using the chosen template. Use EQ operator for exact matches (better precision than CONTAINS_TOKEN). Assume API returns matching contacts or empty if none.
   - Both template example (replace {{{{extracted_firstname}}}} and {{{{extracted_lastname}}}}):
     {{{{
         "filterGroups": [
             {{{{
                 "filters": [
                     {{{{
                         "propertyName": "firstname",
                         "operator": "EQ",
                         "value": "{{{{extracted_firstname}}}}"
                     }}}},
                     {{{{
                         "propertyName": "lastname", 
                         "operator": "EQ",
                         "value": "{{{{extracted_lastname}}}}"
                     }}}}
                 ]
             }}}}
         ],
         "properties": [
             "hs_object_id",
             "firstname", 
             "lastname",
             "email",
             "phone",
             "jobtitle",
             "createdate",
             "lastmodifieddate"
         ],
         "sorts": [
             {{{{
                 "propertyName": "lastmodifieddate",
                 "direction": "DESCENDING"
             }}}}
         ],
         "limit": 10,
         "after": null
     }}}}
   - Firstname_only template example (replace {{{{extracted_firstname}}}}):
     {{{{
         "filterGroups": [
             {{{{
                 "filters": [
                     {{{{
                         "propertyName": "firstname",
                         "operator": "CONTAINS_TOKEN",
                         "value": "{{{{extracted_firstname}}}}"
                     }}}}
                 ]
             }}}}
         ],
         "properties": [
             "hs_object_id",
             "firstname", 
             "lastname",
             "email",
             "phone",
             "jobtitle",
             "createdate",
             "lastmodifieddate"
         ],
         "sorts": [
             {{{{
                 "propertyName": "lastmodifieddate",
                 "direction": "DESCENDING"
             }}}}
         ],
         "limit": 10,
         "after": null
     }}}}

4. For each simulated search:
   - If matches found (up to 10): Populate contact_results with details from the "API response". Use exact fields; set missing to "".
   - Total = number of unique results across all searches.
   - Deduplicate by hs_object_id.

5. If no matches for any contact: Move those to new_contacts, extracting proposed details (firstname/lastname from step 1, email/phone/jobtitle/address from thread context like signatures). For contacts with role indicators, populate jobtitle appropriately (e.g., if "(Ops)" was mentioned, set jobtitle to operations-related role). Fill ALL fields; use "" for missing.

6. If no contacts extracted: Set contact_results total=0, results=[], new_contacts=[].

Return this exact JSON structure (include reasoning_summary for internal logging; ignore it in processing):
{{{{
    "reasoning_summary": {{{{
        "extracted_names": [
            {{"firstname": "example_first", "lastname": "example_last", "template_used": "both|firstname_only", "num_results": 1}}
        ],
        "excluded_names": [
            {{"name": "Amy Thomas", "reason": "deal_owner"}},
            {{"name": "role_indicator", "reason": "department_tag"}}
        ],
        "total_extracted": 2,
        "search_notes": "Brief notes on decisions, e.g., 'Used firstname_only for Olivia as no lastname found, excluded Amy Thomas as deal owner'"
    }}}},
    "contact_results": {{{{
        "total": 0,
        "results": [
            {{{{
                "contactId": "hs_object_id",
                "firstname": "first_name",
                "lastname": "last_name",
                "email": "email_address",
                "phone": "phone_number",
                "address": "full_address",
                "jobtitle": "job_title"
            }}}}
        ]
    }}}},
    "new_contacts": [
        {{{{
            "firstname": "proposed_first_name",
            "lastname": "proposed_last_name",
            "email": "proposed_email",
            "phone": "proposed_phone",
            "address": "proposed_address",
            "jobtitle": "proposed_job_title"
        }}}}
    ]
}}}}

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
    entity_flags = ti.xcom_pull(key="entity_search_flags", default={})
    if not entity_flags.get("search_companies", True):
        logging.info(f"Skipping companies search: {entity_flags.get('companies_reason', 'Not mentioned in thread')}")
        ti.xcom_push(key="company_info", value={
            "company_results": {"total": 0, "results": []},
            "new_companies": [],
            "partner_status": None
        })
        return
    thread_content = ti.xcom_pull(key="thread_content")
    thread_id = ti.xcom_pull(key="thread_id")

    prompt = f"""You are a HubSpot API assistant. Search for companies based on this email thread.

Email thread content:
{thread_content}

IMPORTANT: You must respond with ONLY a valid JSON object. No HTML, no explanations, no markdown formatting. You are only capable of searching comapnies and you do not have the ability to create companies.

Steps to follow:
1. Parse the company name.
2. Invoke search_companies with company name . If company found display all company details in results along with company type wether `PARTNER` or `PROSPECT`.
3. If no companies found, extract potential details for new companies from the email content.
4. `type` should be one of  "PARTNER" OR "PROSPECT". If not specified, default to "PROSPECT".
5. Never parse the same company details to be created again if it already exists in results.
Return this exact JSON structure:
{{
    "company_results": {{
        "total": 0,
        "results": [
            {{
                "companyId": "company_id",
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
            "type": "company_type"
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


def check_task_threshold(ti, **context):
    entity_flags = ti.xcom_pull(key="entity_search_flags", default={})
    if not entity_flags.get("parse_tasks", True):
        logging.info(f"Skipping task threshold check: {entity_flags.get('tasks_reason', '')}")
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
    
    thread_content = ti.xcom_pull(key="thread_content")
    thread_id = ti.xcom_pull(key="thread_id")
    owner_info = ti.xcom_pull(key="owner_info", default={})
    notes_tasks_meeting = ti.xcom_pull(key="notes_tasks_meeting", default={})
    
    # Get task owners and their respective tasks
    task_owners = owner_info.get('task_owners', [])
    tasks = notes_tasks_meeting.get('tasks', [])
    
    # Map tasks to their owners and due dates
    task_owner_mapping = []
    for task in tasks:
        task_details = task.get('task_details', '')
        due_date = task.get('due_date', '')
        task_owner_id = task.get('task_owner_id', '159242778')
        task_owner_name = task.get('task_owner_name', 'liji')
        
        # Find the corresponding task owner info
        task_index = tasks.index(task) + 1
        matching_owner = next((owner for owner in task_owners if owner.get('task_index') == task_index), None)
        if matching_owner:
            task_owner_id = matching_owner.get('task_owner_id', '159242778')
            task_owner_name = matching_owner.get('task_owner_name', 'liji')
        
        task_owner_mapping.append({
            'task_details': task_details,
            'due_date': due_date,
            'task_owner_id': task_owner_id,
            'task_owner_name': task_owner_name
        })

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

        logging.info(f"Task threshold check completed with {len(warnings)} warnings")

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

def parse_notes_tasks_meeting(ti, **context):
    entity_flags = ti.xcom_pull(key="entity_search_flags", default={})
    
    # Check individual flags
    should_parse_notes = entity_flags.get("parse_notes", True)
    should_parse_tasks = entity_flags.get("parse_tasks", True)
    should_parse_meetings = entity_flags.get("parse_meetings", True)
    
    # If none of the flags are True, skip parsing entirely
    if not (should_parse_notes or should_parse_tasks or should_parse_meetings):
        logging.info(f"Skipping all parsing: notes={entity_flags.get('notes_reason', '')}, tasks={entity_flags.get('tasks_reason', '')}, meetings={entity_flags.get('meetings_reason', '')}")
        ti.xcom_push(key="notes_tasks_meeting", value={
            "notes": [],
            "tasks": [],
            "meeting_details": {}
        })
        return
    
    thread_content = ti.xcom_pull(key="thread_content")
    thread_id = ti.xcom_pull(key="thread_id")
    owner_info = ti.xcom_pull(key="owner_info", default={})

    # Extract validated owner information
    task_owners = owner_info.get('task_owners', [])
    default_task_owner_id = "159242778"
    default_task_owner_name = "liji"

    # Build conditional parsing instructions
    parsing_instructions = []
    if should_parse_notes:
        parsing_instructions.append("1. Notes - Any important discussion points, decisions made, or general notes")
    if should_parse_tasks:
        parsing_instructions.append("2. Tasks - Action items with owner and due dates. Next steps with owner and due dates. Adding up the entities in HubSpot is not considered as a task.")
    if should_parse_meetings:
        parsing_instructions.append("3. Meeting Details - Meeting title, start time, end time, location, outcome, timestamp, attendees.")

    prompt = f"""You are a HubSpot API assistant. Analyze this email thread to extract the following based on the analysis flags:

Email thread content:
{thread_content}

Task Owners:
{json.dumps(task_owners, indent=2)}

PARSING INSTRUCTIONS (only parse what's listed below):
{chr(10).join(parsing_instructions)}

IMPORTANT: You must respond with ONLY a valid JSON object. No HTML, no explanations, no markdown formatting.

For notes (only if note parsing is enabled):
- Extract any important discussion points, decisions made, or general notes.

For meetings (only if meeting parsing is enabled):
- Extract meeting title, start time, end time, location, outcome, timestamp, attendees, meeting type, and meeting status.
- If "I" is mentioned for attendees that refers to the email sender.

For tasks (only if task parsing is enabled):
- Identify all tasks and their respective owners from the email content.
- For each task:
  - Match the task to the corresponding owner in the provided Task Owners list by task_index (1-based indexing).
  - If a specific task owner is mentioned in the email and matches an entry in the Task Owners list, use that owner's name and ID.
  - If a specific task owner is mentioned but does not match any entry in the Task Owners list, use the default task owner: {default_task_owner_name} (ID: {default_task_owner_id}).
  - If no task owner is specified, use the default task owner: {default_task_owner_name} (ID: {default_task_owner_id}).
  - If no due date is specified, use the date three business days from the current date.
  - Assign a priority (high, medium, low) based on context; default to 'medium' if not specified.

Return this exact JSON structure:
{{
    "notes": {[] if not should_parse_notes else '[{"note_content": "detailed note content", "timestamp": "YYYY-MM-DD HH:MM:SS", "note_type": "meeting_note|discussion|decision|general"}]'},
    "tasks": {[] if not should_parse_tasks else '[{"task_details": "detailed task description", "task_owner_name": "owner_name", "task_owner_id": "owner_id", "due_date": "YYYY-MM-DD", "priority": "high|medium|low", "task_index": 1}]'},
    "meeting_details": {{}} if not should_parse_meetings else {{"meeting_title": "meeting title", "start_time": "YYYY-MM-DD HH:MM:SS", "end_time": "YYYY-MM-DD HH:MM:SS", "location": "meeting location or virtual link", "outcome": "meeting outcome summary", "timestamp": "YYYY-MM-DD HH:MM:SS", "attendees": ["attendee1", "attendee2"], "meeting_type": "sales_meeting|follow_up|demo|presentation|other", "meeting_status": "scheduled|completed|cancelled"}}
}}

Guidelines:
- ONLY extract and populate data for the categories that are enabled in the parsing instructions above.
- For tasks, use the task_owner_name and task_owner_id from the Task Owners list when available, matching by task_index.
- Extract dates in proper format, use current date + 3 business days if not specified.
- For missing information, use empty string "" or empty array [].
- If no meeting details are found, return empty object for meeting_details.
- Categorize notes and tasks appropriately.
- Never create meetings, notes, or tasks beyond what is specified in the email.
- If parsing is disabled for a category, return empty array/object for that category.
- Include task_index in each task to map to the Task Owners list.

RESPOND WITH ONLY THE JSON OBJECT - NO OTHER TEXT."""

    response = get_ai_response(prompt, expect_json=True)
    logging.info(f"Raw AI response for notes/tasks/meeting: {response[:1000]}...")

    try:
        parsed_json = json.loads(response.strip())
        
        # Apply parsing flags to the result - force empty arrays/objects for disabled categories
        if not should_parse_notes:
            parsed_json["notes"] = []
            logging.info("Notes parsing was disabled - forcing empty notes array")
            
        if not should_parse_tasks:
            parsed_json["tasks"] = []
            logging.info("Tasks parsing was disabled - forcing empty tasks array")
            
        if not should_parse_meetings:
            parsed_json["meeting_details"] = {}
            logging.info("Meetings parsing was disabled - forcing empty meeting_details")
        
        # Ensure all tasks use the validated owner information from task_owners (only if tasks are enabled)
        if should_parse_tasks:
            for task in parsed_json.get("tasks", []):
                task_index = task.get("task_index", 0)
                # Find matching task owner by task_index
                matching_owner = next((owner for owner in task_owners if owner.get("task_index") == task_index), None)
                if matching_owner:
                    task["task_owner_id"] = matching_owner.get("task_owner_id", default_task_owner_id)
                    task["task_owner_name"] = matching_owner.get("task_owner_name", default_task_owner_name)
                else:
                    task["task_owner_id"] = default_task_owner_id
                    task["task_owner_name"] = default_task_owner_name
                    logging.warning(f"No matching task owner found for task_index {task_index}, using default owner {default_task_owner_name}")
                # Ensure task_index is included
                if "task_index" not in task:
                    task["task_index"] = task_index or (parsed_json["tasks"].index(task) + 1)
        
        ti.xcom_push(key="notes_tasks_meeting", value=parsed_json)

        contexts = get_thread_context()
        if thread_id not in contexts:
            contexts[thread_id] = {}
        contexts[thread_id]["notes_tasks_meeting"] = parsed_json
        contexts[thread_id]["prompt_notes_tasks_meeting"] = prompt
        contexts[thread_id]["response_notes_tasks_meeting"] = response
        contexts[thread_id]["parsing_flags"] = {
            "parse_notes": should_parse_notes,
            "parse_tasks": should_parse_tasks, 
            "parse_meetings": should_parse_meetings
        }
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)

        logging.info(f"Successfully parsed {len(parsed_json.get('notes', []))} notes, {len(parsed_json.get('tasks', []))} tasks, and meeting details. Flags: notes={should_parse_notes}, tasks={should_parse_tasks}, meetings={should_parse_meetings}")

    except Exception as e:
        logging.error(f"Error processing notes/tasks/meeting AI response: {e}")
        default = {
            "notes": [] if not should_parse_notes else [],
            "tasks": [] if not should_parse_tasks else [],
            "meeting_details": {} if not should_parse_meetings else {}
        }
        ti.xcom_push(key="notes_tasks_meeting", value=default)

def compile_search_results(ti, **context):
    owner_info = ti.xcom_pull(key="owner_info")
    deal_info = ti.xcom_pull(key="deal_info")
    contact_info = ti.xcom_pull(key="contact_info")
    company_info = ti.xcom_pull(key="company_info")
    notes_tasks_meeting = ti.xcom_pull(key="notes_tasks_meeting")
    task_threshold_info = ti.xcom_pull(key="task_threshold_info", default={})
    thread_id = ti.xcom_pull(key="thread_id")
    email_data = ti.xcom_pull(key="email_data")
    thread_history = ti.xcom_pull(key="thread_history")

    # Get previous results from DAG configuration
    conf = context.get("dag_run", {}).conf or {}
    previous_search_results = conf.get("previous_search_results", {})
    previous_create_results = conf.get("previous_create_results", {})
    is_followup_creation = conf.get("is_followup_creation", False)

    if not thread_history:
        logging.error(f"No thread history for thread_id={thread_id}")
        service = authenticate_gmail()
        if service:
            thread_history = get_email_thread(service, email_data)
            logging.info(f"Re-fetched thread history for thread_id={thread_id}, {len(thread_history)} emails")

    # Merge current results with previous results to avoid duplicates
    merged_deal_results = deal_info.get("deal_results", {"total": 0, "results": []})
    merged_contact_results = contact_info.get("contact_results", {"total": 0, "results": []})
    merged_company_results = company_info.get("company_results", {"total": 0, "results": []})

    # If this is a followup creation, merge with previous search results
    if is_followup_creation and previous_search_results:
        # Merge existing entities (avoid duplicates by ID)
        prev_deals = previous_search_results.get("deal_results", {}).get("results", [])
        prev_contacts = previous_search_results.get("contact_results", {}).get("results", [])
        prev_companies = previous_search_results.get("company_results", {}).get("results", [])

        # Merge deals (avoid duplicates by dealId)
        existing_deal_ids = {deal.get("dealId") for deal in merged_deal_results["results"]}
        for prev_deal in prev_deals:
            if prev_deal.get("dealId") not in existing_deal_ids:
                merged_deal_results["results"].append(prev_deal)
                merged_deal_results["total"] += 1

        # Merge contacts (avoid duplicates by contactId)
        existing_contact_ids = {contact.get("contactId") for contact in merged_contact_results["results"]}
        for prev_contact in prev_contacts:
            if prev_contact.get("contactId") not in existing_contact_ids:
                merged_contact_results["results"].append(prev_contact)
                merged_contact_results["total"] += 1

        # Merge companies (avoid duplicates by companyId)
        existing_company_ids = {company.get("companyId") for company in merged_company_results["results"]}
        for prev_company in prev_companies:
            if prev_company.get("companyId") not in existing_company_ids:
                merged_company_results["results"].append(prev_company)
                merged_company_results["total"] += 1

        logging.info(f"Merged with previous results - Deals: {len(prev_deals)} previous + {deal_info.get('deal_results', {}).get('total', 0)} new")
        logging.info(f"Contacts: {len(prev_contacts)} previous + {contact_info.get('contact_results', {}).get('total', 0)} new")
        logging.info(f"Companies: {len(prev_companies)} previous + {company_info.get('company_results', {}).get('total', 0)} new")

    # Also include previous created entities as "existing" entities for selection
    if is_followup_creation and previous_create_results:
        # Add previously created entities to the "existing" results for user selection
        prev_created_contacts = previous_create_results.get("created_contacts", {}).get("results", [])
        prev_created_companies = previous_create_results.get("created_companies", {}).get("results", [])
        prev_created_deals = previous_create_results.get("created_deals", {}).get("results", [])

        # Convert created entities to search result format
        for created_contact in prev_created_contacts:
            contact_details = created_contact.get("details", {})
            formatted_contact = {
                "contactId": created_contact.get("id"),
                "firstname": contact_details.get("firstname", ""),
                "lastname": contact_details.get("lastname", ""),
                "email": contact_details.get("email", ""),
                "phone": contact_details.get("phone", ""),
                "address": contact_details.get("address", ""),
                "jobtitle": contact_details.get("jobtitle", "")
            }
            if created_contact.get("id") not in {c.get("contactId") for c in merged_contact_results["results"]}:
                merged_contact_results["results"].append(formatted_contact)
                merged_contact_results["total"] += 1

        # Similar for companies and deals...
        for created_company in prev_created_companies:
            company_details = created_company.get("details", {})
            formatted_company = {
                "companyId": created_company.get("id"),
                "name": company_details.get("name", ""),
                "domain": company_details.get("domain", ""),
                "address": company_details.get("address", ""),
                "city": company_details.get("city", ""),
                "state": company_details.get("state", ""),
                "zip": company_details.get("zip", ""),
                "country": company_details.get("country", ""),
                "phone": company_details.get("phone", ""),
                "description": company_details.get("description", ""),
                "type": company_details.get("type", "")
            }
            if created_company.get("id") not in {c.get("companyId") for c in merged_company_results["results"]}:
                merged_company_results["results"].append(formatted_company)
                merged_company_results["total"] += 1

        for created_deal in prev_created_deals:
            deal_details = created_deal.get("details", {})
            formatted_deal = {
                "dealId": created_deal.get("id"),
                "dealName": deal_details.get("dealName", ""),
                "dealLabelName": deal_details.get("dealLabelName", ""),
                "dealAmount": deal_details.get("dealAmount", ""),
                "closeDate": deal_details.get("closeDate", ""),
                "dealOwnerName": deal_details.get("dealOwnerName", "")
            }
            if created_deal.get("id") not in {d.get("dealId") for d in merged_deal_results["results"]}:
                merged_deal_results["results"].append(formatted_deal)
                merged_deal_results["total"] += 1

        logging.info(f"Added previously created entities to existing results for selection")

    search_results = {
        "thread_id": thread_id,
        "deal_results": merged_deal_results,
        "contact_results": merged_contact_results,
        "company_results": merged_company_results,
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
        "partner_status": company_info.get("partner_status", None),
        "task_threshold_info": task_threshold_info,
        "is_followup_creation": is_followup_creation,  # Flag for downstream processing
        "previous_search_results": previous_search_results,
        "previous_create_results": previous_create_results
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
        search_results["response_html"] = "<html><body>No new entities found to add. Please provide details for additional deals, contacts, companies, notes, tasks, or meetings to proceed.</body></html>"

    ti.xcom_push(key="search_results", value=search_results)
    ti.xcom_push(key="confirmation_needed", value=confirmation_needed)

    try:
        contexts = get_thread_context()
        if contexts is None:
            logging.warning(f"get_thread_context returned None, initializing empty dict")
            contexts = {}
        contexts[thread_id] = {
            "search_results": search_results,
            "email_data": email_data,
            "thread_history": thread_history,
            "original_message_id": email_data["headers"].get("Message-ID", ""),
            "references": email_data["headers"].get("References", "")
        }
        with open(THREAD_CONTEXT_FILE, "w") as f:
            json.dump(contexts, f)

        stored_context = contexts.get(thread_id, {})
        if not stored_context.get("thread_history"):
            logging.error(f"Failed to store thread history for thread_id={thread_id}")
        else:
            logging.info(f"Stored thread history for thread_id={thread_id} with {len(stored_context['thread_history'])} emails")
    except Exception as e:
        logging.error(f"Error updating thread context for thread_id={thread_id}: {e}")
        raise

    logging.info(f"Compiled search results with previous context: {json.dumps(search_results, indent=2)}")
    return search_results


def compose_confirmation_email(ti, **context):
    search_results = ti.xcom_pull(key="search_results")
    email_data = ti.xcom_pull(key="email_data")
    confirmation_needed = ti.xcom_pull(key="confirmation_needed", default=False)
    owner_info = ti.xcom_pull(key="owner_info")
    task_owners = owner_info.get("task_owners", [])
    all_owners = owner_info.get("all_owners_table", [])
    chosen_deal_owner_id = owner_info.get("deal_owner_id", "159242778")
    chosen_deal_owner_name = owner_info.get("deal_owner_name", "liji")
    task_owners = owner_info.get("task_owners", [])  # This is the array of task owners
    all_owners = owner_info.get("all_owners_table", [])
    chosen_deal_owner_id = owner_info.get("deal_owner_id", "159242778")
    chosen_deal_owner_name = owner_info.get("deal_owner_name", "liji")
    deal_owner_msg = owner_info.get("deal_owner_message", "")
    task_owner_msg = owner_info.get("task_owner_message", "")
    engagement_summary = ti.xcom_pull(key="engagement_summary", default={})
    
    if not confirmation_needed:
        logging.info("No confirmation needed, proceeding to trigger next DAG.")
        return "No confirmation needed"

    # Helper function to check if an entity has meaningful data
    def has_meaningful_data(entity, required_fields):
        """Check if entity has at least one non-empty required field"""
        if not entity or not isinstance(entity, dict):
            return False
        return any(entity.get(field, "").strip() for field in required_fields)

    # Helper function to filter meaningful entities
    def filter_meaningful_entities(entities, required_fields):
        """Filter out entities that don't have meaningful data"""
        if not entities:
            return []
        return [entity for entity in entities if has_meaningful_data(entity, required_fields)]
    from_email = email_data["headers"].get("From", "")
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
            <p>Hello {from_email},</p>
            <p>I reviewed your request and prepared the following summary of the actions to be taken in HubSpot:</p>
        </div>
    """


    # Check if any content sections will be displayed
    has_content_sections = False

    # Existing Contact Details Section (only if contacts exist)
    contact_results = search_results.get("contact_results", {})
    if contact_results.get("total", 0) > 0:
        has_content_sections = True
        email_content += """
        <h3>Existing Contact Details</h3>
        <table>
            <thead>
                <tr>
                    <th>id</th>
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
            contactId = contact.get("contactId", "")
            firstname = contact.get("firstname", "")
            lastname = contact.get("lastname", "")
            email = contact.get("email", "")
            phone = contact.get("phone", "")
            address = contact.get("address", "")
            jobtitle = contact.get("jobtitle", "")

            email_content += f"""
                <tr>
                    <td>{contactId}</td>
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

    # Existing Company Details Section (only if companies exist)
    company_results = search_results.get("company_results", {})
    if company_results.get("total", 0) > 0:
        has_content_sections = True
        email_content += """
        <h3>Existing Company Details</h3>
        <table>
            <thead>
                <tr>
                    <th>id</th>
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
            companyId = company.get("companyId", "")
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
                    <td>{companyId}</td>
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

    # Existing Deal Details Section (only if deals exist)
    deal_results = search_results.get("deal_results", {})
    if deal_results.get("total", 0) > 0:
        has_content_sections = True
        email_content += """
        <h3>Existing Deal Details</h3>
        <table>
            <thead>
                <tr>
                    <th>id</th>
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
            dealId = deal.get("dealId", "")
            deal_name = deal.get("dealName", "")
            stage_label = deal.get("dealLabelName", "")
            amount = deal.get("dealAmount", "")
            close_date = deal.get("closeDate", "")
            owner_name = deal.get("dealOwnerName", "")

            email_content += f"""
                <tr>
                    <td>{dealId}</td>
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

    # Objects to be Created Section (only if there are meaningful new entities)
    raw_new_contacts = search_results.get("new_entity_details", {}).get("contacts", [])
    raw_new_companies = search_results.get("new_entity_details", {}).get("companies", [])
    raw_new_deals = search_results.get("new_entity_details", {}).get("deals", [])
    notes = search_results.get("new_entity_details", {}).get("notes", [])
    tasks = search_results.get("new_entity_details", {}).get("tasks", [])
    meeting_details = search_results.get("new_entity_details", {}).get("meeting_details", {})

    # Filter out empty/meaningless entities
    new_contacts = filter_meaningful_entities(raw_new_contacts, ["firstname", "lastname", "email"])
    new_companies = filter_meaningful_entities(raw_new_companies, ["name", "domain"])
    new_deals = filter_meaningful_entities(raw_new_deals, ["dealName", "dealAmount"])
    
    # Filter meaningful notes and tasks
    meaningful_notes = [note for note in notes if note.get("note_content", "").strip()]
    meaningful_tasks = [task for task in tasks if task.get("task_details", "").strip()]
    
    # Check if meeting details has meaningful content
    meaningful_meeting = bool(meeting_details and any(str(v).strip() for v in meeting_details.values() if v is not None))

    # Check if there are any meaningful objects to be created
    has_new_objects = bool(new_contacts or new_companies or new_deals or meaningful_notes or meaningful_tasks or meaningful_meeting)

    if has_new_objects:
        has_content_sections = True
        email_content += """
        <h3>Objects to be Created</h3>
        """

        # New Contacts (only if they exist and have meaningful data)
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
            """

        # New Companies (only if they exist and have meaningful data)
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
            """

        # New Deals (only if they exist and have meaningful data)
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
                deal_owner_name = deal.get("dealOwnerName", "")

                email_content += f"""
                    <tr>
                        <td>{deal_name}</td>
                        <td>{stage_label}</td>
                        <td>{amount}</td>
                        <td>{close_date}</td>
                        <td>{deal_owner_name}</td>
                    </tr>
                """

            email_content += """
                </tbody>
            </table>
            """

        # Notes (only if they have meaningful content)
        if meaningful_notes:
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

            for note in meaningful_notes:
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
            """

        # Tasks (only if they have meaningful content)
        if meaningful_tasks:
            email_content += """
            <h4>Tasks</h4>
            <table>
                <thead>
                    <tr>
                        <th>Task Details</th>
                        <th>Owner Name</th>
                        <th>Due Date</th>
                        <th>Priority</th>
                    </tr>
                </thead>
                <tbody>
            """

            for task in meaningful_tasks:
                task_details = task.get("task_details", "")
                task_owner_name = task.get("task_owner_name", "")
                due_date = task.get("due_date", "")
                priority = task.get("priority", "")

                email_content += f"""
                    <tr>
                        <td>{task_details}</td>
                        <td>{task_owner_name}</td>
                        <td>{due_date}</td>
                        <td>{priority}</td>
                    </tr>
                """

            email_content += """
                </tbody>
            </table>
            """

        # Meeting Details (only if meaningful meeting details exist)
        if meaningful_meeting:
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
            """

        # Add HR separator only if there were new objects
        email_content += "<hr>"

    # Task Volume Analysis Section (ONLY if there are meaningful tasks to be created AND dates were checked)
    task_threshold_info = search_results.get("task_threshold_info", {})
    dates_checked = task_threshold_info.get("task_threshold_results", {}).get("dates_checked", [])
    
    # Only show task volume analysis if:
    # 1. There are meaningful tasks to be created, AND 
    # 2. There are dates that were actually checked
    if meaningful_tasks and dates_checked:
        has_content_sections = True
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
# Engagement Summary Section (only if engagement summary exists and no error)
    if engagement_summary and "error" not in engagement_summary:
        has_content_sections = True
        
        # Extract the actual values from the engagement summary
        contact_name = engagement_summary.get('contact_summary', {}).get('contact_name', '')
        contact_email = engagement_summary.get('contact_summary', {}).get('email', '')
        company_name = engagement_summary.get('contact_summary', {}).get('company_name', '')
        
        deal_name = engagement_summary.get('deal_summary', {}).get('deal_name', '')
        deal_stage = engagement_summary.get('deal_summary', {}).get('stage', '')
        deal_amount = engagement_summary.get('deal_summary', {}).get('amount', '')
        close_date = engagement_summary.get('deal_summary', {}).get('close_date', '')
        
        company_summary_name = engagement_summary.get('company_summary', {}).get('company_name', '')
        company_domain = engagement_summary.get('company_summary', {}).get('domain', '')
        
        engagement_text = engagement_summary.get('engagement_summary', '')
        detailed_deal_text = engagement_summary.get('detailed_deal_summary', '')
        call_strategy_text = engagement_summary.get('call_strategy', '')
        
        email_content += f"""
        <h3>Engagement Summary for Requested Contact</h3>
        <h4>Contact Summary</h4>
        <table>
            <thead>
                <tr>
                    <th>Contact Name</th>
                    <th>Email</th>
                    <th>Company Name</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td>{contact_name}</td>
                    <td>{contact_email}</td>
                    <td>{company_name}</td>
                </tr>
            </tbody>
        </table>
        
        <h4>Deal Summary</h4>
        <table>
            <thead>
                <tr>
                    <th>Deal Name</th>
                    <th>Stage</th>
                    <th>Amount</th>
                    <th>Close Date</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td>{deal_name}</td>
                    <td>{deal_stage}</td>
                    <td>{deal_amount}</td>
                    <td>{close_date}</td>
                </tr>
            </tbody>
        </table>
        
        <h4>Company Summary</h4>
        <table>
            <thead>
                <tr>
                    <th>Company Name</th>
                    <th>Domain</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td>{company_summary_name}</td>
                    <td>{company_domain}</td>
                </tr>
            </tbody>
        </table>
        
        <h4>Engagement Summary</h4>
        <p>{engagement_text}</p>
        
        <h4>Detailed Deal Summary</h4>
        <p>{detailed_deal_text}</p>
        
        <h4>Comprehensive Call Strategy</h4>
        <p>{call_strategy_text}</p>
        <hr>
        """
# Fixed Owner Assignment Section Logic
# This should replace the corresponding section in your compose_confirmation_email function

# Debug version with exact string matching and logging
# Add this to your compose_confirmation_email function

# Check if we should show the owner assignment section
# Replace the Task Owner Assignment section in compose_confirmation_email function with this code:

# Check if we should show the owner assignment section
    has_deals_or_tasks = (
        deal_results.get("total", 0) > 0 or
        len(new_deals) > 0 or
        len(meaningful_tasks) > 0
    )

    if has_deals_or_tasks:
        has_content_sections = True
        
        email_content += """
        <h3>Owner Assignment Details</h3>
        """
        
        # Deal Owner Assignment - show for both existing and new deals
        if deal_results.get("total", 0) > 0 or len(new_deals) > 0:
            email_content += "<div style='margin-bottom: 15px;'>"
            email_content += "<h4 style='color: #2c5aa0; margin-bottom: 5px;'>Deal Owner Assignment:</h4>"
            
            # Debug the exact message content
            deal_msg_lower = deal_owner_msg.lower()
            logging.info(f"DEBUG - deal_owner_msg.lower(): '{deal_msg_lower}'")
            
            if "no deal owner specified" in deal_msg_lower:
                logging.info("DEBUG - Matched 'no deal owner specified' condition")
                email_content += f"""
                <p style='background-color: #d1ecf1; padding: 10px; border-left: 4px solid #17a2b8;'>
                    <strong>Reason:</strong> Deal owner was not specified.
                    <br><strong>Action:</strong> Assigning to default owner '{chosen_deal_owner_name}'.
                </p>
                """
            elif ("not valid" in deal_msg_lower and "deal owner" in deal_msg_lower):
                logging.info("DEBUG - Matched 'not valid' condition")
                email_content += f"""
                <p style='background-color: #f8d7da; padding: 10px; border-left: 4px solid #dc3545;'>
                    <strong>Reason:</strong> Deal owner mentioned, but not found in the available owners list.
                    <br><strong>Action:</strong> Assigning to default owner '{chosen_deal_owner_name}'.
                </p>
                """
            elif ("deal owner specified as" in deal_msg_lower or "specified" in deal_msg_lower):
                logging.info("DEBUG - Matched 'specified' condition (valid owner)")
                email_content += f"""
                <p style='background-color: #d4edda; padding: 10px; border-left: 4px solid #28a745;'>
                    <strong>Reason:</strong> Deal owner is valid and found in the available owners list.
                    <br><strong>Action:</strong> Assigned to '{chosen_deal_owner_name}'.
                </p>
                """
            else:
                logging.info("DEBUG - No condition matched - using default case")
                email_content += f"""
                <p style='background-color: #d4edda; padding: 10px; border-left: 4px solid #28a745;'>
                    <strong>Reason:</strong> Deal owner assignment processed.
                    <br><strong>Action:</strong> Assigned to '{chosen_deal_owner_name}'.
                </p>
                """
            
            email_content += "</div>"
        
        # Task Owner Assignment - show for each task owner if there are meaningful tasks
            # Task Owner Assignment - show for each task owner if there are meaningful tasks
            if len(meaningful_tasks) > 0:
                email_content += "<div style='margin-bottom: 15px;'>"
                email_content += "<h4 style='color: #2c5aa0; margin-bottom: 5px;'>Task Owner Assignments:</h4>"
                
                for task_owner in task_owners:
                    task_index = task_owner.get("task_index", 0)
                    task_owner_name = task_owner.get("task_owner_name", "liji")
                    task_owner_msg = task_owner.get("task_owner_message", "")
                    
                    # Find the corresponding task details
                    task = next((t for t in meaningful_tasks if t.get("task_index") == task_index), None)
                    task_details = task.get("task_details", "Unknown task") if task else "Unknown task"
                    
                    task_msg_lower = task_owner_msg.lower()
                    logging.info(f"DEBUG - task_owner_msg.lower() for task {task_index}: '{task_msg_lower}'")
                    
                    # Use exact task_owner_message with appropriate styling
                    if "no task owner specified" in task_msg_lower:
                        logging.info(f"DEBUG - Matched 'no task owner specified' condition for task {task_index}")
                        email_content += f"""
                        <p style='background-color: #d1ecf1; padding: 10px; border-left: 4px solid #17a2b8;'>
                            <strong>Task {task_index}:</strong> {task_details}
                            <br><strong>Reason:</strong> {task_owner_msg}
                            <br><strong>Action:</strong> Assigning to default owner '{task_owner_name}'.
                        </p>
                        """
                    elif "not valid" in task_msg_lower:
                        logging.info(f"DEBUG - Matched 'not valid' condition for task {task_index}")
                        email_content += f"""
                        <p style='background-color: #f8d7da; padding: 10px; border-left: 4px solid #dc3545;'>
                            <strong>Task {task_index}:</strong> {task_details}
                            <br><strong>Reason:</strong> {task_owner_msg}
                            <br><strong>Action:</strong> Assigning to default owner '{task_owner_name}'.
                        </p>
                        """
                    else:
                        logging.info(f"DEBUG - Matched valid owner condition for task {task_index}")
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
                        
                        # FIXED: Determine assignment status for multiple task owners
                        assignments = []
                        
                        # Check if this owner is the deal owner
                        if owner_id == chosen_deal_owner_id and (deal_results.get("total", 0) > 0 or len(new_deals) > 0):
                            assignments.append("Deal Owner")
                        
                        # Check if this owner is assigned to any tasks
                        if any(task_owner.get("task_owner_id") == owner_id for task_owner in task_owners) and len(meaningful_tasks) > 0:
                            task_indices = [str(task_owner.get("task_index")) for task_owner in task_owners if task_owner.get("task_owner_id") == owner_id]
                            assignments.append(f"Task Owner (Tasks {', '.join(task_indices)})")
                        
                        assignment_text = ", ".join(assignments) if assignments else ""
                        
                        # Enhanced highlighting with different colors for different assignments
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
                    
                    email_content += """
                        </tbody>
                    </table>
                    """
        
        email_content += "<hr>"
        

    # Closing section
    email_content += """
        <div class="closing">
            <p><strong>Instructions:</strong></p>
            <ul>
                <li>If you want to proceed with the existing entities and create the new objects shown above, please reply with "PROCEED WITH EXISTING"</li>
                <li>If you want to create new entities or modify the proposed objects, please reply with "CREATE NEW" along with any corrections to the details</li>
                <li>If you need to modify any information, please specify the changes in your reply</li>
            </ul>
            <p>Please confirm whether this summary looks correct before I proceed with the requested actions.</p>
            <p>Best regards,<br>
            HubSpot Agent<br>
            hubspot-agent-9201@lowtouch.ai</p>
        </div>
    </body>
    </html>
    """

    ti.xcom_push(key="confirmation_email", value=email_content)
    logging.info(f"Confirmation email composed with conditional section display. Content sections included: {has_content_sections}")
    return email_content



def send_confirmation_email(ti, **context):
    """Updated send_confirmation_email with multi-recipient support"""
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

    # Extract all recipients from original email
    all_recipients = extract_all_recipients(email_data)
    
    # Get the sender (who will be the primary recipient)
    sender_email = email_data["headers"].get("From", "")
    original_message_id = email_data["headers"].get("Message-ID", "")
    references = email_data["headers"].get("References", "")

    if original_message_id and original_message_id not in references:
        references = f"{references} {original_message_id}".strip()

    subject = f"Re: {email_data['headers'].get('Subject', 'Meeting Minutes Request')}"

    # Prepare recipients for reply-all functionality
    # Primary recipient is the sender
    primary_recipient = sender_email
    
    # For Cc: Include all original To recipients (except sender) + all original Cc recipients
    cc_recipients = []
    
    # Add all original To recipients except the sender and bot
    for to_addr in all_recipients["to"]:
        if (to_addr.lower() != sender_email.lower() and 
            HUBSPOT_FROM_ADDRESS.lower() not in to_addr.lower()):
            cc_recipients.append(to_addr)
    
    # Add all original Cc recipients except bot
    for cc_addr in all_recipients["cc"]:
        if (HUBSPOT_FROM_ADDRESS.lower() not in cc_addr.lower() and 
            cc_addr not in cc_recipients):  # Avoid duplicates
            cc_recipients.append(cc_addr)
    
    # For Bcc: Include all original Bcc recipients (Gmail API allows this)
    bcc_recipients = []
    for bcc_addr in all_recipients["bcc"]:
        if HUBSPOT_FROM_ADDRESS.lower() not in bcc_addr.lower():
            bcc_recipients.append(bcc_addr)

    # Convert lists to comma-separated strings
    cc_string = ', '.join(cc_recipients) if cc_recipients else None
    bcc_string = ', '.join(bcc_recipients) if bcc_recipients else None

    logging.info(f"Sending confirmation email:")
    logging.info(f"Primary recipient: {primary_recipient}")
    logging.info(f"Cc recipients: {cc_string}")
    logging.info(f"Bcc recipients: {bcc_string}")

    result = send_email(service, primary_recipient, subject, confirmation_email, 
                       original_message_id, references, cc=cc_string, bcc=bcc_string)
    
    if result:
        logging.info(f"Confirmation email sent to all recipients")

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
            "sender_email": sender_email,
            "all_recipients": all_recipients  # Store for future replies
        }

        update_thread_context(thread_id, updated_context)
        if confirmation_thread_id != thread_id:
            update_thread_context(confirmation_thread_id, updated_context)
            logging.info(f"Copied context to new thread_id={confirmation_thread_id}")

        logging.info(f"Thread context updated for thread {thread_id}")
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
        dag_id="hubspot_create_objects",
        run_id=f"triggered_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        conf=trigger_conf,
        execution_date=None,
        replace_microseconds=False
    )
    logging.info("Successfully triggered hubspot_create_objects")

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
    tags=["hubspot", "search", "entities"]
) as dag:

    fetch_thread_task = PythonOperator(
        task_id="fetch_thread",
        python_callable=fetch_thread,
        provide_context=True
    )

    analyze_thread_entities_task = PythonOperator(
        task_id="analyze_thread_entities",
        python_callable=analyze_thread_entities,
        provide_context=True
    )

    summarize_engagement_task = PythonOperator(
    task_id="summarize_engagement_details",
    python_callable=summarize_engagement_details,
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

    check_task_threshold_task = PythonOperator(
        task_id="check_task_threshold",
        python_callable=check_task_threshold,
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

    fetch_thread_task >> analyze_thread_entities_task >> summarize_engagement_task >> determine_owner_task >> search_deals_task >> search_contacts_task >> search_companies_task >> check_task_threshold_task >> parse_notes_tasks_meeting_task >> compile_results_task >> compose_email_task >> send_email_task >> decide_trigger_task
    decide_trigger_task >> trigger_task
    decide_trigger_task >> end_workflow
