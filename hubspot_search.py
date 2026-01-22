from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
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
from airflow.sdk import Variable
from airflow.api.common.trigger_dag import trigger_dag
import requests
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from hubspot_email_listener import send_fallback_email_on_failure, send_hubspot_slack_alert
# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
def clear_retry_tracker_on_success(context):
    dag_run = context['dag_run']
    original_run_id = dag_run.conf.get('original_run_id')
    original_dag_id = dag_run.conf.get('original_dag_id', dag_run.dag_id)
    
    if not original_run_id or not dag_run.conf.get('retry_attempt'):
        return  # Not a retry run
    
    tracker_key = f"{original_dag_id}:{original_run_id}"
    
    retry_tracker = Variable.get("hubspot_retry_tracker", default_var={}, deserialize_json=True)
    
    if tracker_key in retry_tracker:
        del retry_tracker[tracker_key]
        Variable.set("hubspot_retry_tracker", json.dumps(retry_tracker))
        logging.info(f"Retry succeeded - cleared tracker for {tracker_key}")

def update_retry_tracker_on_failure(context):
    dag_run = context['dag_run']
    original_run_id = dag_run.conf.get('original_run_id')
    original_dag_id = dag_run.conf.get('original_dag_id', dag_run.dag_id)
    
    if not original_run_id or not dag_run.conf.get('retry_attempt'):
        return  # Not a retry run
    
    tracker_key = f"{original_dag_id}:{original_run_id}"
    
    retry_tracker = Variable.get("hubspot_retry_tracker", default_var={}, deserialize_json=True)
    
    if tracker_key in retry_tracker:
        retry_tracker[tracker_key]["status"] = "failed"
        Variable.set("hubspot_retry_tracker", json.dumps(retry_tracker))
        logging.info(f"Retry failed - updated status for {tracker_key}")

default_args = {
    "owner": "lowtouch.ai_developers",
    "depends_on_past": False,
    "start_date": datetime(2025, 8, 22),
    "retry_delay": timedelta(seconds=15),
    'on_failure_callback': [
        send_fallback_email_on_failure,  # Sends email to user
        send_hubspot_slack_alert          # Sends Slack alert to team
    ]
}

HUBSPOT_FROM_ADDRESS = Variable.get("ltai.v3.hubspot.from.address")
GMAIL_CREDENTIALS = Variable.get("ltai.v3.hubspot.gmail.credentials")
OLLAMA_HOST = Variable.get("ltai.v3.hubspot.ollama.host")
HUBSPOT_API_KEY = Variable.get("ltai.v3.husbpot.api.key")
HUBSPOT_BASE_URL = Variable.get("ltai.v3.hubspot.url")
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

def get_ai_response(prompt, conversation_history=None, expect_json=False, model='hubspot:v6af', stream=True):
    try:
        client = Client(host=OLLAMA_HOST, headers={'x-ltai-client': 'hubspot-v6af'})
        messages = []
        
        if expect_json and model != "hubspot:v7-perplexity":
            # Stronger prompt to enforce pure JSON
            messages.append({
                "role": "system",
                "content": (
                    "You are a strict JSON-only API. Respond with ONLY a valid JSON object. "
                    "Never include any text before or after the JSON. "
                    "No explanations, no markdown, no code blocks, no HTML, no <think> tags, no thinking steps. "
                    "Start directly with { and end with }. "
                    "Ensure the JSON is parsable and complete."
                )
            })

        if conversation_history:
            for item in conversation_history:
                if "role" in item and "content" in item:
                    messages.append({"role": item["role"], "content": item["content"]})
                else:
                    messages.append({"role": "user", "content": item.get("prompt", "")})
                    messages.append({"role": "assistant", "content": item.get("response", "")})
        
        messages.append({"role": "user", "content": prompt})
        
        response = client.chat(model=model, messages=messages, stream=stream)
        
        # Handle streaming or non-streaming response
        if stream:
            ai_content = ""
            try:
                for chunk in response:
                    if hasattr(chunk, 'message') and hasattr(chunk.message, 'content'):
                        ai_content += chunk.message.content
                    else:
                        logging.warning("Chunk lacks expected 'message.content' structure")
            except Exception as stream_error:
                logging.error(f"Streaming error: {stream_error}")
                raise
        else:
            if not (hasattr(response, 'message') and hasattr(response.message, 'content')):
                logging.error("Response lacks expected 'message.content' structure")
                return handle_json_error(expect_json)
            ai_content = response.message.content

        # === ROBUST CLEANUP & JSON EXTRACTION ===
        ai_content = ai_content.strip()

        # Remove markdown code fences
        ai_content = re.sub(r'```(?:json|html)?\n?', '', ai_content)
        ai_content = re.sub(r'```', '', ai_content)

        # Remove <think>...</think> tags (common in some models)
        ai_content = re.sub(r'<think>.*?</think>', '', ai_content, flags=re.DOTALL | re.IGNORECASE)
        ai_content = re.sub(r'<think>.*', '', ai_content, flags=re.DOTALL | re.IGNORECASE)  # if unclosed

        # If expecting JSON, extract the first valid JSON object
        if expect_json:
            # Find the first { ... } block
            json_match = re.search(r'\{.*\}', ai_content, re.DOTALL)
            if json_match:
                ai_content = json_match.group(0)
            else:
                # Last resort: try to find any JSON-like structure
                logging.warning(f"No JSON object found in AI response. Raw: {ai_content[:500]}")
                return '{"error": "No valid JSON found in response"}'

            # Final validation
            try:
                json.loads(ai_content)  # Test if valid
            except json.JSONDecodeError as e:
                logging.error(f"Invalid JSON after cleanup: {e}\nContent: {ai_content[:500]}")
                return '{"error": "Invalid JSON format from AI"}'

        # Add HTML wrapper only if not JSON and not already HTML
        if not expect_json:
            if not ai_content.strip().startswith('<!DOCTYPE') and \
               not ai_content.strip().startswith('<html') and \
               not ai_content.strip().startswith('{'):
                ai_content = f"<html><body>{ai_content}</body></html>"

        return ai_content.strip()

    except Exception as e:
        raise


def handle_json_error(expect_json):
    """Helper to return consistent error format"""
    if expect_json:
        return '{"error": "Error processing AI request - invalid or empty response"}'
    else:
        return "<html><body>Error processing AI request. Please try again later.</body></html>"

def parse_email_addresses(address_string):
    if not address_string:
        return []
    addresses = []
    for addr in address_string.split(','):
        addr = addr.strip()
        if addr:
            addresses.append(addr)
    return addresses

def extract_all_recipients(email_data):
    headers = email_data.get("headers", {})
    to_recipients = parse_email_addresses(headers.get("To", ""))
    cc_recipients = parse_email_addresses(headers.get("Cc", ""))
    bcc_recipients = parse_email_addresses(headers.get("Bcc", ""))
    return {
        "to": to_recipients,
        "cc": cc_recipients,
        "bcc": bcc_recipients
    }

def get_owner_name_from_id(owner_id, all_owners_table):
    """Convert owner ID to owner name using the owners table"""
    if not owner_id or not all_owners_table:
        return "Kishore"  # Default
    
    # Search for matching owner
    for owner in all_owners_table:
        if str(owner.get("id", "")) == str(owner_id):
            return owner.get("name", "Kishore")
    
    return "Kishore"

def send_email(service, recipient, subject, body, in_reply_to, references, cc=None, bcc=None):
    try:
        msg = MIMEMultipart()
        msg["From"] = f"HubSpot via lowtouch.ai <{HUBSPOT_FROM_ADDRESS}>"
        msg["To"] = recipient
        
        if cc:
            cc_list = [email.strip() for email in cc.split(',') if email.strip().lower() != HUBSPOT_FROM_ADDRESS.lower()]
            cleaned_cc = ', '.join(cc_list)
            if cleaned_cc:
                msg["Cc"] = cleaned_cc
                logging.info(f"Including Cc in email: {cleaned_cc}")
        
        if bcc:
            bcc_list = [email.strip() for email in bcc.split(',') if email.strip().lower() != HUBSPOT_FROM_ADDRESS.lower()]
            cleaned_bcc = ', '.join(bcc_list)
            if cleaned_bcc:
                msg["Bcc"] = cleaned_bcc
                logging.info(f"Including Bcc in email: {cleaned_bcc}")
        
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

# ============================================================================
# DAG TASK FUNCTIONS
# ============================================================================

def load_context_from_dag_run(ti, **context):
    """Load all necessary context from DAG run configuration"""
    dag_run_conf = context['dag_run'].conf
    
    email_data = dag_run_conf.get("email_data", {})
    chat_history = dag_run_conf.get("chat_history", [])
    logging.info(f"chat history:{chat_history}")
    logging.info(f"dag run config:{dag_run_conf}")
    thread_history = dag_run_conf.get("thread_history", [])
    
    thread_id = email_data.get("threadId", "unknown")
    latest_message = email_data.get("content", "")
    
    logging.info(f"=== LOADING CONTEXT FROM DAG RUN ===")
    logging.info(f"Thread ID: {thread_id}")
    logging.info(f"Chat history length: {len(chat_history)}")
    logging.info(f"Thread history length: {len(thread_history)}")
    logging.info(f"Latest message preview: {latest_message[:100]}...")
    
    ti.xcom_push(key="email_data", value=email_data)
    ti.xcom_push(key="chat_history", value=chat_history)
    ti.xcom_push(key="thread_history", value=thread_history)
    ti.xcom_push(key="thread_id", value=thread_id)
    ti.xcom_push(key="latest_message", value=latest_message)
    
    return {
        "email_data": email_data,
        "chat_history": chat_history,
        "thread_history": thread_history,
        "thread_id": thread_id,
        "latest_message": latest_message
    }

def generate_and_inject_spelling_variants(ti, **context):
    latest_message = ti.xcom_pull(key="latest_message", task_ids = "load_context_from_dag_run",default="")
    chat_history = ti.xcom_pull(key="chat_history",  task_ids = "load_context_from_dag_run", default=[])
    logging.info(f"chat hsitory:{chat_history}")

    recent_context = ""
    if chat_history is not None: 
        for msg in chat_history[-4:]:  # Last few messages
            recent_context += f"{msg['role'].upper()}: {msg['content']}\n\n"
    recent_context += f"USER: {latest_message}"
    logging.info(f"recent context:{recent_context}")

    variant_prompt = f"""You are a helpful assistant that detects potential spelling mistakes in names mentioned in business emails.
    You cannot create or update any records, your only job is to identify possible typos in names based on the conversation.

latest user message:
{latest_message}

Your task: Extract any contact names, company names, or deal names that might have typos from {latest_message}.
For each, list the original + 3–5 plausible spelling variants (include common misspellings).

Examples:
- "Neah" → ["neah", "neha", "neeha", "nehha", "nehaa"]
- "Microsft" → ["microsft", "microsoft", "micrsoft", "microsfot"]
- "Pryia" → ["pryia", "priya", "priyaa", "priiya"]

Rules:
- Always include the original spelling first.
- Only suggest variants if the name looks like a likely typo.
- Max 5 variants per name.
- Use lowercase for consistency.

CONVERSATION:
{recent_context}

Return ONLY valid JSON:
{{
    "potential_variants": {{
        "contacts": [{{"original": "Neah", "variants": ["neah", "neha", "neeha", "nehha"]}}],
        "companies": [{{"original": "Microsft", "variants": ["microsft", "microsoft", "micrsoft"]}}],
        "deals": []
    }},
    "has_potential_typos": true/false
}}
"""

    try:
        response = get_ai_response(variant_prompt,conversation_history=chat_history, expect_json=True)
        logging.info(f"Raw AI response for spelling variants: {response}")
    except Exception as e:
        raise
    try:
        variants_data = json.loads(response.strip())

        ti.xcom_push(key="spelling_variants", value=variants_data.get("potential_variants", {}))
        ti.xcom_push(key="has_potential_typos", value=variants_data.get("has_potential_typos", False))

        logging.info(f"Spelling variants generated and injected: {variants_data}")

    except Exception as e:
        logging.warning(f"Failed to generate spelling variants: {e}")
        ti.xcom_push(key="spelling_variants", value={})
        ti.xcom_push(key="has_potential_typos", value=False)

def analyze_thread_entities(ti, **context):
    """Analyze thread to determine which entities to search and actions to take"""
    chat_history = ti.xcom_pull(key="chat_history", task_ids = "load_context_from_dag_run", default=[])
    latest_message = ti.xcom_pull(key="latest_message", task_ids = "load_context_from_dag_run" ,default="")
    email_data = ti.xcom_pull(key="email_data", task_ids = "load_context_from_dag_run", default={})
    thread_id = ti.xcom_pull(key="thread_id", task_ids = "load_context_from_dag_run",default="unknown")
    spelling_variants = ti.xcom_pull(key="spelling_variants",  task_ids = "generate_spelling_variants",default={})

    variants_section = ""
    if spelling_variants:
        variants_section = f"""
                    IMPORTANT: The user may have made spelling mistakes in names.
                    Here are detected potential typos and suggested variants (use the most likely correct one when deciding entities):

                    CONTACTS: {json.dumps(spelling_variants.get("contacts", []))}
                    COMPANIES: {json.dumps(spelling_variants.get("companies", []))}
                    DEALS: {json.dumps(spelling_variants.get("deals", []))}

                    When extracting names for search_contacts, search_companies, or search_deals:
                    - Prefer the most plausible correct spelling from the variants list.
                    - But always search using the best match, not just the original typo.
                    """
    # === Extract sender and headers (same as email_listener) ===
    headers = email_data.get("headers", {})
    sender_raw = headers.get("From", "")
    import email.utils
    sender_tuple = email.utils.parseaddr(sender_raw)
    sender_name = sender_tuple[0].strip() or "there"
    sender_email = sender_tuple[1].strip() or sender_raw

    # === Build chat context ===
    chat_context = ""
    for idx, msg in enumerate(chat_history, 1):
        role = msg.get("role", "unknown")
        content = msg.get("content", "")
        chat_context += f"[{role.upper()}]: {content}\n\n"

    # === Prompt (same as before) ===
    prompt = f"""You are a HubSpot API assistant. Analyze this latest message to determine which entities (deals, contacts, companies) are mentioned or need to be processed, and whether the user is requesting a summary of a client or deal before their next meeting.
    You cannot create or update any records, your only job is to identify and validate the entities based on the conversation.

Variant Spelling Information:
{variants_section}

LATEST USER MESSAGE:
{latest_message}

IMPORTANT: 
    - You must respond with ONLY a valid JSON object. No HTML, no explanations, no markdown formatting.
    - You are not capable of calling any APIs or Tool. You should answer based on your knowledge.
Analyze the content and determine:
1. Is the user requesting a summary of a client or deal before their next meeting? Look for phrases like "summarize details for contact name", "summary for deal name", or explicit mentions of preparing for an upcoming meeting.
2. If a summary is requested, set ALL other flags (search_deals, search_contacts, search_companies, parse_notes, parse_tasks, parse_meetings) to false.
3. Determine if the user is requesting a 360° enhanced summary with external research. Look for phrases like:
"360 view", "full 360", "deal 360", "research the company", "background on", "web search", "perplexity", "company intel", "who are they", "what do we know about this company", "detailed analysis" etc.
→ If detected → set "request_summary_360": true. set ALL other flags (search_deals, search_contacts, search_companies, parse_notes, parse_tasks, parse_meetings) to false.
4. If no summary is requested, determine the following:
   - CONTACTS (search_contacts):
        - Set to TRUE if ANY person's name is mentioned (first name, last name, or full name) — even multiple people.
        - This includes ALL mentioned individuals regardless of role (e.g., "spoke with Neha", "cc'd Riya", "John from finance", "met Sarah and Priya").
        - Contact information like email or phone number also triggers this.
        - exclude the user name or hubspot owner names.
        - Exclude the contact name used for assigning a task or deal owner.
        - User mentions the contact name of a existing contact.
        - User mentions company name or deal name of a existing contact.
        - Always search every single person mentioned — never skip or filter out any individual.
        - **Use spelling variants**: If a name appears to be misspelled and variants are provided above, prefer the most likely correct spelling from the variants when deciding to trigger a search.
    - COMPANIES (search_companies):
        - Set to TRUE if a company/organization name is mentioned.
        - Contact person name is also a trigger for company search.
        - This includes formal company names, business names, or organizational references
        - User Mentions the contact name or deal name of a exiting company. 
        - Do not consider the company `lowtouch.ai`.
        - Strictly ignore the previous company of the contacts and consider the current company from contacts email id.
        - **Use spelling variants**: If a company name appears to be misspelled and variants are provided above, prefer the most likely correct spelling from the variants when deciding to trigger a search.
    - DEALS (search_deals):
        - Set to TRUE ONLY if ANY of these conditions are met:
            a) User explicitly mention the name of a existing deal name.The user refers to an existing deal in any way (e.g., “this deal”, “the ABC deal”, “update the opportunity”, “for this pipeline item”).
            b) User explicitly mention the name of contact or name of company and the context implies an action related to CRM entities(deal lookup, linking, creation, follow-up, updating, etc.).
            c) User is talking about creating entities for existing deals, contacts or company.
            d) User is creating followup tasks, notes, or meetings for existing deals.The user is creating or modifying follow-ups, tasks, notes, or meetings that relate to an existing deal.
            e) User explicitly mentions creating a deal, opportunity, or sale.The user clearly states they want to create a deal/opportunity,propose a deal,open a new opportunity,add something to the pipeline
            f) User states the client/contact is interested in moving forward with a purchase, contract, or agreement
            g) User mentions pricing discussions, proposals sent, quotes provided, or contract negotiations
            h) User indicates a clear buying intent from the client (e.g., "they want to proceed", "ready to sign", "committed to purchase")
        - **Use spelling variants**: If a deal name appears to be misspelled and variants are provided above, prefer the most likely correct spelling from the variants when deciding to trigger a search.
        - Set to FALSE for:
            - Initial conversations or introductions
            - Exploratory discussions without commitment
            - Interest without explicit forward movement (e.g., "interested in learning more", "exploring options", "could turn into something").Meeting summaries that only shows interest,good engagement,strong opportunity,potential alignment,exploratory discussion without any clear buying signal or creation request.
            - No Deal Movement Discussions about:platform walkthroughsdemosdiscovery conversationsuse-case alignmentwithout any explicit mention of purchase intent or deal creation.
            - Future potential without current action.Statements like:“This could turn into something”“They're interested in learning more”,“It's a strong opportunity”.do NOT count as a deal unless the user clearly states commercial next steps.
            - Administrative or Context-Only Inputs:General updates, notes, or summaries without CRM action or buying intent.
            - Client may be interested or impressed, but no explicit intent to buy or move forward is stated.
            - **DO NOT treat ANY topics, areas of interest, use cases, bullet points, capabilities, product features, or discussion points as deals.Even if the text contains Areas of interest(e.g., “Autonomous Workflows”),Use cases,Capabilities,Bullet points,Topics from a meeting,these should never be interpreted as deals.**

    - NOTES (parse_notes):
        - Set to TRUE ONLY if a conversation, call, or meeting with a client HAS ALREADY OCCURRED
        - The message must describe what was discussed, outcomes, or information exchanged
        - Set to FALSE for:
            - Future intentions (e.g., "I should call them", "planning to meet")
            - General information about a company or contact without a discussion
            - Thoughts or observations without an actual interaction

    - TASKS (parse_tasks):
        - Set parse_tasks to TRUE if the text contains any real, actionable next steps based on intent—regardless of specific words. A task exists when:
            1. A commitment is made.A meeting, follow-up, review, or discussion is scheduled or requested.
            2. A request is made.Someone asks for deeper exploration, a review, a demo, a document, or internal support.
            3. An implied obligation exists.A future event requires preparation, coordination, or internal actions to fulfill.Someone expresses interest in going deeper on a topic, implying preparation.
            4. The text describes an upcoming time-bound activity.Any future session, follow-up review, or jointly planned next step.
        - Set to FALSE for:
            1. The text is purely descriptive with no expectations of future action.
            2. The statements are vague (“could be”, “might be good”) and not tied to concrete events.
            3. Relationship or sentiment statements contain no specific next step.
            4. There is no implied preparation or commitment.

    - MEETINGS (parse_meetings):
        - Set to TRUE ONLY if ALL of these conditions are met:
            a) A meeting has already occurred (past tense)
            b) Specific meeting details are provided for already held meeting only.
        - Set to FALSE for:
            - Conversations or calls without formal meeting details
            - Future meeting intentions without confirmed details
            - Past meetings without time/date information


Return this exact JSON structure:
{{
    "search_deals": true/false,
    "search_contacts": true/false,
    "search_companies": true/false,
    "parse_notes": true/false,
    "parse_tasks": true/false,
    "parse_meetings": true/false,
    "request_summary": true/false,
    "request_summary_360": true/false,
    "deals_reason": "explanation why deals need processing or not",
    "contacts_reason": "explanation why contacts need processing or not",
    "companies_reason": "explanation why companies need processing or not",
    "notes_reason": "explanation why notes need processing or not",
    "tasks_reason": "explanation why tasks need processing or not",
    "meetings_reason": "explanation why meetings need processing or not",
    "summary_reason": "explanation why a summary is requested or not"
}}

RESPOND WITH ONLY THE JSON OBJECT - NO OTHER TEXT."""

    try:
        response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
        logging.debug(f"Prompt is : {prompt}")
        logging.debug(f"Conversation history to AI: {chat_history}")
        logging.debug(f"Raw AI response for entity analysis: {response[:1000]}...")
    except Exception as e:
        raise
    try:
        parsed_json = json.loads(response.strip())
        ti.xcom_push(key="entity_search_flags", value=parsed_json)

        logging.info(f"=== ENTITY ANALYSIS RESULTS ===")
        logging.info(f"  - Search deals: {parsed_json.get('search_deals')} - {parsed_json.get('deals_reason')}")
        logging.info(f"  - Search contacts: {parsed_json.get('search_contacts')} - {parsed_json.get('contacts_reason')}")
        logging.info(f"  - Search companies: {parsed_json.get('search_companies')} - {parsed_json.get('companies_reason')}")
        logging.info(f"  - Parse notes: {parsed_json.get('parse_notes')} - {parsed_json.get('notes_reason')}")
        logging.info(f"  - Parse tasks: {parsed_json.get('parse_tasks')} - {parsed_json.get('tasks_reason')}")
        logging.info(f"  - Parse meetings: {parsed_json.get('parse_meetings')} - {parsed_json.get('meetings_reason')}")
        logging.info(f"  - Request summary: {parsed_json.get('request_summary')} - {parsed_json.get('summary_reason')}")

    except Exception as e:
        logging.warning(f"AI failed in analyze_thread_entities for thread {thread_id}: {e} → Sending fallback email")


def summarize_engagement_details(ti, **context):
    """Retrieve and summarize engagement details based on conversation"""
    entity_flags = ti.xcom_pull(key="entity_search_flags", task_ids="analyze_thread_entities", default={})
    if not entity_flags.get("request_summary", False):
        logging.info("No summary requested, skipping engagement summary")
        ti.xcom_push(key="engagement_summary", value={})
        return
    
    chat_history = ti.xcom_pull(key="chat_history", task_ids = "load_context_from_dag_run", default=[])
    latest_message = ti.xcom_pull(key="latest_message",  task_ids = "load_context_from_dag_run", default="")
    email_data = ti.xcom_pull(key="email_data", task_ids = "load_context_from_dag_run", default={})

    conversation_context = ""
    for msg in chat_history:
        role = msg.get("role", "unknown")
        content = msg.get("content", "")
        conversation_context += f"[{role.upper()}]: {content}\n\n"
    
    conversation_context += f"[USER - LATEST]: {latest_message}\n"

    prompt = f"""You are a HubSpot API assistant. Summarize engagement details based on the provided email thread content. Parse the contact name, deal ID (if specified), company name, and other relevant details directly from the conversation context.

FULL CONVERSATION:
{conversation_context}

EMAIL SUBJECT:
{email_data.get("headers", {}).get("Subject", "")}

IMPORTANT: Respond with ONLY a valid JSON object. No HTML, no explanations, no markdown formatting.

Steps:
- **When the user requests "Summarize details for {{contact name to search}}", retrieve the `contactId` for the specified contact**.
  - Check if the user has provided a specific `dealId` or deal identifier in the request.
  - Invoke `get_engagements_by_object_id` tool to get engagement of a perticular `dealId`. 
  - List all the Associated Deals If no `dealId` is provided and , retrieve the engagements for the top 3 `dealId`s and summarize the details in the specified format.
  - If there are multiple deal invoke `get_engagements_by_object_id` for all the `dealIds` and summarize the details in the specified format.
  - Ensure the agent does not retrieve or process engagements for any deals other than the user-specified `dealId` or the single deal when applicable.
  - Summarize the details in the bellow format, ensuring clarity and relevance for the selected deal only.
  **Output format** :
    - **Contact**: {{contact_name}}, Email: {{email}}, Company: {{company_name}} in tabular format.
    - **Deal**: [{{Deal_name}}, Stage: {{Deal_stage}}, Amount: {{Deal_Amount}}, Close Date: {{Deal_close_date}}], Owner: {{Deal_owner_name}} in tabular format.
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
Important Instructions:
- use the first name and lastname both to search for contacts, if given.
- use the date format as MMM-DD-YYYY for close_date.
Return this exact JSON structure:
{{
    "contact_summary": {{
        "contact_name": "parsed_contact_name",
        "email": "inferred_email_from_thread",
        "company_name": "inferred_company_name"
    }},
    "deal_summary": [{{
        "deal_name": "inferred_deal_name",
        "stage": "inferred_deal_stage",
        "amount": "inferred_amount",
        "close_date": "inferred_close_date"
    }}],
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
    try:
        response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
        logging.info(f"Raw AI response for engagement summary: {response[:1000]}...")
    except Exception as e:
        raise
    try:
        parsed_json = json.loads(response.strip())
        ti.xcom_push(key="engagement_summary", value=parsed_json)
        logging.info(f"Engagement summary generated successfully")
    except Exception as e:
        logging.error(f"Error processing engagement summary AI response: {e}")
        ti.xcom_push(key="engagement_summary", value={"error": f"Error processing engagement summary: {str(e)}"})

def summarize_engagement_details_360(ti, **context):
    """Generate enhanced Deal 360 view using Perplexity-powered web research"""
    entity_flags = ti.xcom_pull(key="entity_search_flags", task_ids="analyze_thread_entities", default={})
    if not entity_flags.get("request_summary_360", False):
        logging.info("No 360 summary requested, skipping")
        return

    chat_history = ti.xcom_pull(key="chat_history", task_ids = "load_context_from_dag_run", default=[])
    latest_message = ti.xcom_pull(key="latest_message", task_ids = "load_context_from_dag_run", default="")
    email_data = ti.xcom_pull(key="email_data", task_ids = "load_context_from_dag_run", default={})

    conversation_context = ""
    for msg in chat_history:
        conversation_context += f"[{msg.get('role', 'unknown').upper()}]: {msg.get('content', '')}\n\n"
    conversation_context += f"[USER - LATEST]: {latest_message}\n"

    # First: Get structured HubSpot data (same as before, but minimal + 360 fields)
    prompt = f"""You are a HubSpot expert assistant. Extract and summarize key CRM data from this email thread for a Deal 360 view.

                FULL CONVERSATION:
                {conversation_context}

                EMAIL SUBJECT: {email_data.get("headers", {}).get("Subject", "")}

                IMPORTANT: Respond with ONLY a valid JSON object. No HTML, no explanations, no markdown formatting.

                Steps:
                - **When the user requests "Summarize details for {{contact name to search}}", retrieve the `contactId` for the specified contact**.
                - Check if the user has provided a specific `dealId` or deal identifier in the request.
                - Invoke `get_engagements_by_object_id` tool to get engagement of a perticular `dealId`. 
                - List all the Associated Deals If no `dealId` is provided and , retrieve the engagements for the top 3 `dealId`s and summarize the details in the specified format.
                - If there are multiple deal invoke `get_engagements_by_object_id` for all the `dealIds` and summarize the details in the specified format.
                - Ensure the agent does not retrieve or process engagements for any deals other than the user-specified `dealId` or the single deal when applicable.
                - Summarize the details in the bellow format, ensuring clarity and relevance for the selected deal only.
                - **Extract**:
                - Primary contact name and email
                - Company name
                - Deal name(s), ID(s), stage, amount, owner, create/close dates
                - Top 3 associated contacts
                - Last 5 activities (emails, calls, meetings, notes)

                - **Compute risk flags as of current date**:
                - past_close_date: true if close date is before today
                - no_activity_14_days: true if latest activity >14 days ago
                - stage_unchanged_21_days: true if stage hasn't moved in 21+ days
                **Output format** :
                    - **contact_summary**: {{contact_name}}, Email: {{email}}, Company: {{company_name}} in tabular format.
                    - **deal_summary**: [{{Deal_name}}, Stage: {{Deal_stage}}, Amount: {{Deal_Amount}}, Close Date: {{Deal_close_date}}] in tabular format.
                    - **company_summary**: {{Company_name}}, Domain: {{email}} in tabular format
                    - Never show Note ID.      
                    - **recent_5_activities**: ["...", "..."],  
                    - **risk_flags**: {{"past_close_date": false, "no_activity_14_days": false, "stage_unchanged_21_days": false}}
                Important Instructions:
                - use the first name and lastname both to search for contacts, if given.
                Return this exact JSON structure:
                {{
                    "contact_summary": {{
                        "contact_name": "parsed_contact_name",
                        "email": "inferred_email_from_thread",
                        "company_name": "inferred_company_name"
                    }},
                    "deal_summary": [{{
                        "deal_name": "inferred_deal_name",
                        "stage": "inferred_deal_stage",
                        "amount": "inferred_amount",
                        "close_date": "inferred_close_date"
                    }}],
                    "company_summary": {{
                        "company_name": "inferred_company_name",
                        "domain": "inferred_domain"
                    }},
                    "recent_5_activities": ["...", "..."],  
                    "risk_flags": {{"past_close_date": false, "no_activity_14_days": false, "stage_unchanged_21_days": false}}, 
                }}
                Guidelines:
                - Parse contact name, deal ID (if any), company name, and other details directly from thread content or email subject.
                - Infer all fields from thread content and email data; use empty string "" for missing values.
                - Do not use contact, company, or deal info from XCom; rely solely on thread content and email data.
                - If no contact name is found, return {{"error": "No contact name found in thread content"}}.
                - Ensure summaries and call strategy are tailored to the context in the thread.
                - use the date format as MMM-DD-YYYY for close_date.

                RESPOND WITH ONLY THE JSON OBJECT - NO OTHER TEXT."""

    try:
        response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
        logging.info(f"360 - Raw HubSpot AI response: {response[:1000]}...")
    except Exception as e:
        raise
    try:
        parsed_json = json.loads(response.strip())

        # Now: Perplexity-powered external research
        company_name = parsed_json.get("company_summary", {}).get("company_name", "").strip()
        if company_name and company_name.lower() not in [ "", "n/a"]:
            perplexity_prompt = f"""You are an elite B2B sales intelligence researcher using live web search (Perplexity). Your job is to deliver a sharp, actionable 360° external view of the company to help close the deal faster.

                                Company Name: {company_name}
                                Domain (if known): {parsed_json.get("company_summary", {}).get("domain", "not available")}

                                Deal Context (use this to tailor your research depth and focus):
                                - Deal Stage: {', '.join([d.get("stage", "unknown") for d in parsed_json.get("deal_summary", [])[:3]]) or "unknown"}
                                - Deal Amount: {', '.join([d.get("amount", "unknown") for d in parsed_json.get("deal_summary", [])[:3]]) or "unknown"}
                                - Expected Close Date: {', '.join([d.get("close_date", "unknown") for d in parsed_json.get("deal_summary", [])[:3]]) or "unknown"}
                                - Last Activity: {parsed_json.get("recent_5_activities", "unknown")}

                                Prioritize and include only the most decision-relevant insights:
                                • Recent funding rounds, revenue estimates, or growth metrics
                                • Key executives / decision-makers (especially if different from known contacts)
                                • Major product launches, partnerships, or tech stack signals
                                • Expansion, hiring surge, new office, or M&A activity
                                • Current challenges, layoffs, leadership changes, or competitive threats
                                • Any public intent signals around the problems our Pro plan solves (automation, workflows, CRM, etc.)

                                Formatting rules (strict):
                                - Use short, clear lines separated by a single newline
                                - No bullets required; each insight can be its own line
                                - No markdown, no headings, no emojis
                                - No blank lines between items
                                - No paragraphs; only single-line insights
                                - Keep to 5–8 lines
                                - Return ONLY valid JSON exactly in the structure below

                                Return ONLY this JSON (no extra text, no markdown):
                                `{{"deal_360": ["Insight 1", "Insight 2", "Insight 3"]}}`
                                """

            perp_response = get_ai_response(
                perplexity_prompt,
                conversation_history=[],
                expect_json=True,
                model="hubspot:v7-perplexity"
            )
            logging.info(f"360 - Raw Perplexity AI response: {perp_response[:1000]}...")
            perp_json = json.loads(perp_response.strip())
            parsed_json["deal_360"] = perp_json.get("deal_360", "No external insights available at this time.")
        else:
            parsed_json["deal_360"] = ["Company not identified for external research."]

        ti.xcom_push(key="engagement_summary", value=parsed_json)
        logging.info("360 engagement summary with Perplexity research generated successfully")

    except Exception as e:
        logging.error(f"Error in 360 summary: {e}")
        ti.xcom_push(key="engagement_summary", value={"error": f"Error processing engagement summary: {str(e)}"})

def determine_owner(ti, **context):
    """Determine deal owner and task owners from conversation"""
    chat_history = ti.xcom_pull(key="chat_history", task_ids = "load_context_from_dag_run", default=[])
    latest_message = ti.xcom_pull(key="latest_message",  task_ids = "load_context_from_dag_run", default="")
    email_data = ti.xcom_pull(key="email_data",  task_ids = "load_context_from_dag_run", default={})
    headers      = email_data.get("headers", {})
    sender_raw   = headers.get("From", "")                # e.g. "John Doe <john@acme.com>"
    # Parse a clean name and e-mail (fallback to raw string if parsing fails)
    import email.utils
    sender_tuple = email.utils.parseaddr(sender_raw)      # (realname, email)
    sender_name  = sender_tuple[0].strip() or sender_raw
    sender_email = sender_tuple[1].strip() or sender_raw
    

    prompt = f"""You are a HubSpot API assistant. Analyze this conversation to identify deal owner and task owners.
    **You CANNOT create deal owners or tasks owners in HubSpot.**  
**SENDER**  
Name : {sender_name}  
Email: {sender_email}
LATEST USER MESSAGE:
{latest_message}

IMPORTANT: You must respond with ONLY a valid JSON object. No HTML, no explanations, no markdown formatting.

DEFAULT OWNER: Kishore (ID: 71346067)

Steps:
1. Parse the Deal Owner and Task Owners and contact Owners from the conversation.
2. Invoke get_all_owners Tool to retrieve the list of available owners.
3. The contact owner is the same as the deal owner. If deal is not given take the contact owner as the default one.
4. Parse and validate the deal owner against the available owners list:
    - If deal owner is NOT specified at all:
        - Default to: "Kishore"
        - Message: "No deal owner specified, so assigning to default owner Kishore."
    - If deal owner IS specified but NOT found in available owners list:
        - Default to: "Kishore"
        - Message: "The specified deal owner '[parsed_owner]' is not valid, so assigning to default owner Kishore."
    - If deal owner IS specified and IS found in available owners list:
        - Use the matched owner (with correct casing from the available owners list)
        - Message: "Deal owner specified as [matched_owner_name]"
Important rule for deal owner - Never take the task owner as deal owner, if the owner is not specified.
5. Parse and validate each task owner against the available owners list:
    - Identify all tasks and their respective owners from the email content.
    - Identify if the task owner is the email sender himself by checking phrases like : I'll send, i will get back etc.
    - Identify multiple tasks from one email by checking for bullet points, numbered lists, or separate paragraphs indicating distinct action items. Also check for conjunctions like "and" or "also" that may link multiple tasks in a single sentence.
    - For each task owner:
        - If task owner is NOT specified for a task:
            - Default to: "Kishore"
            - Message: "No task owner specified for task [task_index], so assigning to default owner Kishore."
        - If task owner IS specified but NOT found in available owners list:
            - Default to: "Kishore"
            - Message: "The specified task owner '[parsed_owner]' for task [task_index] is not valid, so assigning to default owner Kishore."
        - If task owner IS specified and IS found in available owners list:
            - Use the matched owner (with correct casing from the available owners list)
            - Message: "Task owner for task [task_index] specified as [matched_owner_name]".
Important rule for task owner - Sender should be the task owner of the tasks where the phrase is I will do or I'll sned etc. For all others if the owner is not specified it should be kishore.
6. Parse and validate each contact owner against the available owners list:
    - If the deal details are not given and contact owner is also not specified:
        - Default to: "Kishore"
        - Message: "No contact owner specified, so assigning to default owner Kishore."
    - If the deal details are not given and also contact owner IS specified but NOT found in available owners list:
        - Default to: "Kishore"
        - Message: "The specified contact owner '[parsed_owner]' is not valid, so assigning to default owner Kishore."
    - If the deal details are given then contact owner is same as deal owner.

7. Important Rule for deciding owners for task deal and contact.
- Never use the task owner as deal owner or contact owner. 
- Parse the specified deal owner from latest_message. If not specified use default one rather than using task owners.
- COntact owner is always the deal owner.
8. Return a list of task owners with their validation details.

Return this exact JSON structure:
{{
    "contact_owner_id": "",
    "contact_owner_name": "",
    "contact_owner_message": "",
    "deal_owner_id": "",
    "deal_owner_name": "",
    "deal_owner_message": "",
    "task_owners": [
        {{
            "task_index": 1,
            "task_owner_id": "",
            "task_owner_name": "",
            "task_owner_message": ""
        }},
        {{
            "task_index": 2,
            "task_owner_id": "",
            "task_owner_name": "",
            "task_owner_message": ""
        }},
        ...
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
    try:
        response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
        logging.info(f"ai response is:{response}")
    except Exception as e:
        raise

    try:
        parsed_json = json.loads(response.strip())
        ti.xcom_push(key="owner_info", value=parsed_json)
        logging.info(f"Owner determined: {parsed_json.get('deal_owner_name')}")
        return parsed_json
    except Exception as e:
        logging.error(f"Error processing owner AI response: {e}")
        default_owner = {
            "deal_owner_id": "71346067",
            "deal_owner_name": "Kishore",
            "deal_owner_message": f"Error occurred: {str(e)}, so assigning to default owner Kishore.",
            "task_owners": [],
            "all_owners_table": []
        }
        ti.xcom_push(key="owner_info", value=default_owner)
        return default_owner

def validate_deal_stage(ti, **context):
    """Validate deal stage from conversation and assign default if invalid"""
    chat_history = ti.xcom_pull(key="chat_history", task_ids = "load_context_from_dag_run", default=[])
    latest_message = ti.xcom_pull(key="latest_message", task_ids = "load_context_from_dag_run", default="")
    
    # Valid deal stages
    VALID_DEAL_STAGES = [
        "Lead", "Qualified Lead", "Solution Discussion", "Proposal", 
        "Negotiations", "Contracting", "Closed Won", "Closed Lost", "Inactive"
    ]
    
    prompt = f"""You are a HubSpot Deal Stage Validation Assistant. Your role is to validate deal stages from the conversation.
    **You cannot create or update deals in HubSpot.**

LATEST USER MESSAGE:
{latest_message}

VALID DEAL STAGES:
{', '.join(VALID_DEAL_STAGES)}

DEFAULT STAGE: Lead

IMPORTANT: You must respond with ONLY a valid JSON object. No HTML, no explanations, no markdown formatting.

Steps:
1. Parse any deal stage mentioned in the conversation for new deals being created.
2. For each deal mentioned:
   - If NO deal stage is specified:
     - Default to: "Lead"
     - Message: "No deal stage specified for deal '[deal_name]', so assigning to default stage 'Lead'."
   
   - If deal stage IS specified but NOT found in the valid stages list:
     - Default to: "Lead"
     - Message: "The specified deal stage '[specified_stage]' for deal '[deal_name]' is not valid. Valid stages are: {', '.join(VALID_DEAL_STAGES)}. Assigning to default stage 'Lead'."
   
   - If deal stage IS specified and IS found in the valid stages list:
     - Use the matched stage (with correct casing from the valid stages list)
     - Message: "Deal stage for deal '[deal_name]' specified as '[matched_stage]'."

3. Return validation results for all deals.

Return this exact JSON structure:
{{
    "deal_stages": [
        {{
            "deal_index": 1,
            "deal_name": "parsed_deal_name",
            "original_stage": "user_specified_valid_stage_or_Lead",
            "validated_stage": "Lead",
            "stage_message": "explanation_message"
        }}
    ]
}}

RESPOND WITH ONLY THE JSON OBJECT - NO OTHER TEXT."""
    try:
        response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
        logging.info(f"Raw AI response for deal stage validation: {response[:1000]}...")
    except Exception as e:
        raise
    try:
        parsed_json = json.loads(response.strip())
        ti.xcom_push(key="deal_stage_info", value=parsed_json)
        logging.info(f"Deal stage validation completed for {len(parsed_json.get('deal_stages', []))} deal(s)")
    except Exception as e:
        logging.error(f"Error processing deal stage validation: {e}")
        default_result = {
            "deal_stages": []
        }
        ti.xcom_push(key="deal_stage_info", value=default_result)

    return parsed_json

def search_contacts_api(firstname=None, lastname=None, email=None):
    """
    Search contacts in HubSpot with fallback logic.

    Priority:
    1. Email (exact match)
    2. Firstname + Lastname (AND)
    3. Firstname only (fallback)

    Lastname-only search is intentionally not supported.
    """

    endpoint = f"{HUBSPOT_BASE_URL}/crm/v3/objects/contacts/search"
    headers = {
        "Authorization": f"Bearer {HUBSPOT_API_KEY}",
        "Content-Type": "application/json"
    }

    properties = [
        "hs_object_id", "firstname", "lastname", "email",
        "phone", "address", "jobtitle", "hubspot_owner_id"
    ]

    def _execute_search(filters):
        """Execute a single HubSpot search"""
        payload = {
            "filterGroups": [{"filters": filters}],
            "properties": properties,
            "sorts": [{"propertyName": "lastmodifieddate", "direction": "DESCENDING"}],
            "limit": 10
        }

        response = requests.post(endpoint, headers=headers, json=payload, timeout=30)
        response.raise_for_status()

        return response.json().get("results", [])

    try:
        search_attempts = []

        # Attempt 1: Email (exact match)
        if email:
            search_attempts.append([
                {
                    "propertyName": "email",
                    "operator": "EQ",
                    "value": email.strip().lower()
                }
            ])

        # Attempt 2: Firstname + Lastname
        elif firstname and lastname:
            search_attempts.append([
                {
                    "propertyName": "firstname",
                    "operator": "CONTAINS_TOKEN",
                    "value": firstname.strip()
                },
                {
                    "propertyName": "lastname",
                    "operator": "CONTAINS_TOKEN",
                    "value": lastname.strip()
                }
            ])

        # Attempt 3: Firstname only (fallback)
        elif firstname:
            search_attempts.append([
                {
                    "propertyName": "firstname",
                    "operator": "CONTAINS_TOKEN",
                    "value": firstname.strip()
                }
            ])

        # If nothing valid to search
        if not search_attempts:
            logging.info("No valid contact search parameters provided.")
            return {"total": 0, "results": []}

        # Execute searches in order, stop at first successful result
        for filters in search_attempts:
            results = _execute_search(filters)

            if results:
                formatted_results = []
                for contact in results:
                    props = contact.get("properties", {})
                    formatted_results.append({
                        "contactId": contact.get("id"),
                        "firstname": props.get("firstname", ""),
                        "lastname": props.get("lastname", ""),
                        "email": props.get("email", ""),
                        "phone": props.get("phone", ""),
                        "address": props.get("address", ""),
                        "jobtitle": props.get("jobtitle", ""),
                        "contactOwnerId": props.get("hubspot_owner_id", ""),
                        "contactOwnerName": ""
                    })

                search_desc = email or f"{firstname or ''} {lastname or ''}".strip()
                logging.info(
                    f"Found {len(formatted_results)} contacts for search: {search_desc}"
                )

                return {
                    "total": len(formatted_results),
                    "results": formatted_results
                }

        # No results from any attempt
        logging.info("No contacts found after applying all fallback strategies.")
        return {"total": 0, "results": []}

    except requests.exceptions.HTTPError as http_err:
        logging.error(
            f"HTTP error during contact search: {http_err.response.status_code} - "
            f"{http_err.response.text}"
        )
        return {"total": 0, "results": []}
    except Exception as e:
        logging.error(f"Failed to search contacts: {e}")
        return {"total": 0, "results": []}


def get_contact_associations(contact_id, association_type="companies"):
    """Get associated companies or deals for a contact"""
    try:
        endpoint = f"{HUBSPOT_BASE_URL}/crm/v4/objects/contacts/{contact_id}/associations/{association_type}"
        headers = {"Authorization": f"Bearer {HUBSPOT_API_KEY}"}
        
        response = requests.get(endpoint, headers=headers, timeout=30)
        response.raise_for_status()
        
        results = response.json().get("results", [])
        associated_ids = [result.get("toObjectId") for result in results if result.get("toObjectId")]
        
        logging.info(f"Contact {contact_id} has {len(associated_ids)} associated {association_type}")
        return associated_ids
        
    except Exception as e:
        logging.error(f"Failed to get associations for contact {contact_id}: {e}")
        return []


def get_company_details(company_id):
    """Get company details by ID"""
    try:
        endpoint = f"{HUBSPOT_BASE_URL}/crm/v3/objects/companies/{company_id}"
        headers = {"Authorization": f"Bearer {HUBSPOT_API_KEY}"}
        
        properties = [
            "hs_object_id", "name", "domain", "address", "city", 
            "state", "zip", "country", "phone", "description", "type"
        ]
        props_param = "&".join([f"properties={p}" for p in properties])
        
        response = requests.get(f"{endpoint}?{props_param}", headers=headers, timeout=30)
        response.raise_for_status()
        
        company = response.json()
        props = company.get("properties", {})
        
        return {
            "companyId": company.get("id"),
            "name": props.get("name", ""),
            "domain": props.get("domain", ""),
            "address": props.get("address", ""),
            "city": props.get("city", ""),
            "state": props.get("state", ""),
            "zip": props.get("zip", ""),
            "country": props.get("country", ""),
            "phone": props.get("phone", ""),
            "description": props.get("description", ""),
            "type": props.get("type", "")
        }
        
    except Exception as e:
        logging.error(f"Failed to get company {company_id}: {e}")
        return None


def get_deal_details(deal_id):
    """Get deal details by ID"""
    try:
        endpoint = f"{HUBSPOT_BASE_URL}/crm/v3/objects/deals/{deal_id}"
        headers = {"Authorization": f"Bearer {HUBSPOT_API_KEY}"}
        
        properties = [
            "hs_object_id", "dealname", "dealstage", "amount", 
            "closedate", "hubspot_owner_id"
        ]
        props_param = "&".join([f"properties={p}" for p in properties])
        
        response = requests.get(f"{endpoint}?{props_param}", headers=headers, timeout=30)
        response.raise_for_status()
        
        deal = response.json()
        props = deal.get("properties", {})
        
        # Get stage label
        stage_id = props.get("dealstage", "")
        stage_label = get_deal_stage_label(stage_id)
        
        return {
            "dealId": deal.get("id"),
            "dealName": props.get("dealname", ""),
            "dealLabelName": stage_label,
            "dealAmount": props.get("amount", ""),
            "closeDate": props.get("closedate", ""),
            "dealOwnerId": props.get("hubspot_owner_id", "")
        }
        
    except Exception as e:
        logging.error(f"Failed to get deal {deal_id}: {e}")
        return None


def get_deal_stage_label(stage_id):
    """Convert deal stage ID to label"""
    stage_mapping = {
        "appointmentscheduled": "Appointment Scheduled",
        "qualifiedtobuy": "Qualified to Buy",
        "presentationscheduled": "Presentation Scheduled",
        "decisionmakerboughtin": "Decision Maker Bought-In",
        "contractsent": "Contract Sent",
        "closedwon": "Closed Won",
        "closedlost": "Closed Lost"
    }
    return stage_mapping.get(stage_id.lower(), stage_id)


# ============================================================================
# UPDATED SEARCH FUNCTIONS WITH ASSOCIATION LOGIC
# ============================================================================

def search_contacts_with_associations(ti, **context):
    """
    Step 1: Search contacts from email
    Step 2: Get their associated companies and deals
    """
    entity_flags = ti.xcom_pull(key="entity_search_flags", task_ids="analyze_thread_entities", default={})
    if not entity_flags.get("search_contacts", True):
        logging.info(f"Skipping contacts search: {entity_flags.get('contacts_reason', 'Not mentioned')}")
        default_result = {
            "contact_results": {"total": 0, "results": []},
            "new_contacts": [],
            "associated_companies": [],
            "associated_deals": []
        }
        ti.xcom_push(key="contact_info_with_associations", value=default_result)
        return

    chat_history = ti.xcom_pull(key="chat_history", task_ids = "load_context_from_dag_run",default=[])
    latest_message = ti.xcom_pull(key="latest_message", task_ids = "load_context_from_dag_run", default="")
    
    # AI extracts contact names from email
    prompt = f"""Extract ALL contact names mentioned in this email. You cannot create any contact in HubSpot.
For each contact, parse:
- firstname
- lastname (empty string if not provided)
- email (if mentioned)

    YOU ARE A JSON-ONLY API. 
    DO NOT WRITE ANY TEXT, EXPLANATION, OR NARRATIVE.
    DO NOT USE <think> TAGS.
    DO NOT SAY "invoking" OR "successful".
    IMMEDIATELY OUTPUT THE RAW JSON AND NOTHING ELSE.

LATEST MESSAGE:
{latest_message}

RULES:
- Extract EVERY person mentioned
- Exclude any person who meets ANY of the following:
    A. INTERNAL TEAM MEMBER  
    A person is internal if their email domain or company matches any of:
    ["lowtouch.ai", "hybcloud technologies", "ccs", "cc", "cloud control", "cloudcontrol", "ecloudcontrol"]

    B. DEAL OWNER  
    Any user returned by the get_all_owners tool must be excluded.
    PROCESS:
    - First identify internal members  
    - Then identify deal owners  
    - Remove both groups from results  
    - Return only external, non-owner participants

- Exclude the people whose email is retrieved from `get_all_owners` tool matches the domain given in the latest message.
- Handle single names (e.g., "Neha") as firstname only
- Parse "Neha (Ops)" as firstname="Neha", ignore role

Return ONLY valid JSON:
{{
    "contacts": [
        {{"firstname": "...", "lastname": "...", "email": "...", "phone": "...", "Job Title": "...", "Contact Owner": "..."}}
    ]
    "associated_companies": [],
    "associated_deals":[]
}}
"""
    try:
        response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
    except Exception as e:
        raise
    try: 
        response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
        logging.info(f"AI response is: {response}")
    except Exception as e:
        raise
        
    try:
        parsed = json.loads(response.strip())
        extracted_contacts = parsed.get("contacts", [])
        
        if not extracted_contacts:
            logging.info("No contacts extracted from email")
            ti.xcom_push(key="contact_info_with_associations", value={
                "contact_results": {"total": 0, "results": []},
                "new_contacts": [],
                "associated_companies": [],
                "associated_deals": []
            })
            return
        
        # Search each contact in HubSpot
        found_contacts = []
        not_found_contacts = []
        all_associated_companies = []
        all_associated_deals = []
        
        for contact in extracted_contacts:
            firstname = contact.get("firstname", "").strip()
            lastname = contact.get("lastname", "").strip()
            email = contact.get("email", "").strip()
            logging.info(f"first_name:{firstname} and last_name is:{lastname}")
            # Search contact
            search_result = search_contacts_api(
                firstname=firstname if firstname else None,
                lastname=lastname if lastname else None,
                email=email if email else None
            )
            logging.info(f"search result is :{search_result}")
            if search_result["total"] > 0:
                # Contact found - get their associations
                for found_contact in search_result["results"]:
                    found_contacts.append(found_contact)
                    contact_id = found_contact["contactId"]
                    
                    # Get associated companies
                    company_ids = get_contact_associations(contact_id, "companies")
                    for company_id in company_ids:
                        company_details = get_company_details(company_id)
                        if company_details:
                            # 🔹 OPTION 1: normalize company name upstream
                                original_name = company_details.get("name", "")
                                company_details["name"] = (
                                    original_name
                                    .lower()
                                    .replace(" ", "")
                                    .replace("-", "")
                            )

                        if company_details and company_details not in all_associated_companies:
                            all_associated_companies.append(company_details)
                    
                    # Get associated deals
                    deal_ids = get_contact_associations(contact_id, "deals")
                    for deal_id in deal_ids:
                        deal_details = get_deal_details(deal_id)
                        if deal_details and deal_details not in all_associated_deals:
                            all_associated_deals.append(deal_details)
                    logging.info(f"Contact {contact_id} associations fetched: "
                                 f"{len(company_ids)} companies, {deal_ids} deals")
            else:
                # Get the correct contact owner from owner_info
                owner_info = ti.xcom_pull(key="owner_info", task_ids = "determine_owner",default={})
                contact_owner_name = owner_info.get("contact_owner_name", "Kishore")

                not_found_contacts.append({
                    "firstname": firstname,
                    "lastname": lastname,
                    "email": email,
                    "phone": "",
                    "address": "",
                    "jobtitle": "",
                    "contactOwnerName": contact_owner_name  
                })
        
        result = {
            "contact_results": {
                "total": len(found_contacts),
                "results": found_contacts
            },
            "new_contacts": not_found_contacts,
            "associated_companies": all_associated_companies,
            "associated_deals": all_associated_deals
        }
        
        ti.xcom_push(key="contact_info_with_associations", value=result)
        logging.info(f"Contacts: {len(found_contacts)} found, {len(not_found_contacts)} new | "
                    f"Associated: {len(all_associated_companies)} companies, {len(all_associated_deals)} deals")
        
    except Exception as e:
        logging.error(f"Error in contact search with associations: {e}")
        ti.xcom_push(key="contact_info_with_associations", value={
            "contact_results": {"total": 0, "results": []},
            "new_contacts": [],
            "associated_companies": [],
            "associated_deals": []
        })


def validate_companies_against_associations(ti, **context):
    """
    Compare companies mentioned in prompt vs associated companies.
    Always include ALL associated companies as existing, and add unmatched mentioned as new.
    """
    
    def normalize_text(text):
        """
        Normalize text for comparison by:
        1. Converting to lowercase
        2. Removing all types of dashes and special chars
        3. Removing spaces
        4. Removing punctuation
        """
        import unicodedata
        
        if not text:
            return ""
        text = text.lower()
        text = unicodedata.normalize('NFKD', text)
        text = re.sub(r'[-–—‐‑‒―]', '', text)
        text = re.sub(r'\s+', '', text)
        text = re.sub(r'[^\w]', '', text)
        return text
    
    entity_flags = ti.xcom_pull(key="entity_search_flags", task_ids= "analyze_thread_entities", default={})
    if not entity_flags.get("search_contacts", True):
        logging.info("Contact search was skipped, so skipping company association validation")
        # Push empty result
        ti.xcom_push(key="company_info", value={
            "company_results": {"total": 0, "results": []},
            "new_companies": [],
            "partner_status": None
        })
        return

    contact_data = ti.xcom_pull(key="contact_info_with_associations",task_ids = "search_contacts_with_associations", default={})
    logging.info(f"contact data:{contact_data}")
    associated_companies = contact_data.get("associated_companies", [])
    contact_results = contact_data.get("contact_results", {})
    logging.info(f"contact_results:{contact_results}")
    chat_history = ti.xcom_pull(key="chat_history", task_ids = "load_context_from_dag_run", default=[])
    latest_message = ti.xcom_pull(key="latest_message", task_ids = "load_context_from_dag_run", default = "")
    
    # AI extracts companies mentioned in email
    prompt = f"""Extract ALL company names explicitly mentioned in this email.
    **You CANNOT create companies in HubSpot.**

    YOU ARE A JSON-ONLY API. 
    DO NOT WRITE ANY TEXT, EXPLANATION, OR NARRATIVE.
    DO NOT USE <think> TAGS.
    DO NOT SAY "invoking" OR "successful".
    IMMEDIATELY OUTPUT THE RAW JSON AND NOTHING ELSE.

LATEST MESSAGE:
{latest_message}

RULES:
- Only extract formal company/organization names
- Exclude any company if its name or domain matches: lowtouch.ai, hybcloud technologies, ccs, cc, cloud control, cloudcontrol    ecloudcontrol.These are INTERNAL companies and must not be included in outputs.
- Return empty list if no companies mentioned
- **Never create any companies**
- Only search for companies if it is explicitly mentioned in the email by the user.**Do not** infer or assume any company names from deal name.

Return ONLY valid JSON:
{{
    "companies": [
        {{
            "name": "...",
            "domain": "...",
            "address": "...",
            "city": "...",
            "state": "...",
            "zip": "...",
            "country": "...",
            "phone": "...",
            "description": "...",
            "type": "..."
        }}
    ]
}}
"""
    try:
        response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
    except Exception as e:
        raise
    try:
        parsed = json.loads(response.strip())
        mentioned_companies = parsed.get("companies", [])
        
        # ✅ FIX 1: Normalize associated companies into a dictionary with improved normalization
        normalized_associated = {}
        for assoc in associated_companies:
            original_name = assoc.get("name", "").strip()
            if not original_name:
                continue
            
            # Use the new normalize_text function
            normalized_key = normalize_text(original_name)
            
            # If duplicate keys exist, keep the first one
            if normalized_key not in normalized_associated:
                normalized_associated[normalized_key] = assoc
                logging.info(f"Normalized existing company: '{original_name}' → '{normalized_key}'")
            else:
                logging.info(f"Duplicate existing company skipped: '{original_name}' (already have '{normalized_associated[normalized_key].get('name')}')")
        
        logging.info(f"Associated companies found: {len(associated_companies)}")
        logging.info(f"Unique normalized companies: {len(normalized_associated)}")
        logging.info(f"Normalized keys: {list(normalized_associated.keys())}")
        
        # ✅ FIX 2: Handle BOTH "Name" and "name" from AI response
        extracted_names = []
        for company in mentioned_companies:
            # Try both "Name" (capital) and "name" (lowercase)
            company_name = company.get("Name") or company.get("name") or ""
            company_name = company_name.strip()
            if company_name:
                extracted_names.append(company_name)
        
        logging.info(f"Mentioned companies from email: {extracted_names}")
        
        # Start with ALL unique associated companies as existing
        existing_companies = list(normalized_associated.values())
        
        # Now add unmatched mentioned as new
        new_companies = []
        
        for mentioned in mentioned_companies:
            # Handle both "Name" and "name" fields
            mentioned_name = (mentioned.get("Name") or mentioned.get("name") or "").strip()
            
            if not mentioned_name:
                logging.warning(f"Skipping company with no name: {mentioned}")
                continue
            
            # Use the new normalize_text function
            mentioned_normalized = normalize_text(mentioned_name)
            
            logging.info(f"Checking company: '{mentioned_name}' → normalized: '{mentioned_normalized}'")
            
            # Check if already exists
            if mentioned_normalized in normalized_associated:
                logging.info(f"✓ EXISTING company matched: '{mentioned_name}' (matches '{normalized_associated[mentioned_normalized].get('name')}')")
                continue  # Skip - already in existing_companies
            
            # ✅ FIX 4: Not found - add to new companies with proper field names
            new_company = {
                "name": mentioned_name,  # Use lowercase "name" for consistency
                "domain": mentioned.get("domain", ""),
                "address": mentioned.get("address", ""),
                "city": mentioned.get("city", ""),
                "state": mentioned.get("state", ""),
                "zip": mentioned.get("zip", ""),
                "country": mentioned.get("country", ""),
                "phone": mentioned.get("phone", ""),
                "description": mentioned.get("description", ""),
                "type": mentioned.get("type", "PROSPECT")
            }
            new_companies.append(new_company)
            logging.info(f"✓ NEW company to create: '{mentioned_name}' (normalized: '{mentioned_normalized}')")
        
        result = {
            "company_results": {
                "total": len(existing_companies),
                "results": existing_companies
            },
            "new_companies": new_companies,
            "partner_status": None
        }
        
        ti.xcom_push(key="company_info", value=result)
        logging.info(f"COMPANY VALIDATION SUMMARY:")
        logging.info(f"  - Existing companies: {len(existing_companies)}")
        logging.info(f"  - New companies to create: {len(new_companies)}")
        if existing_companies:
            logging.info(f"  - Existing: {[c.get('name') for c in existing_companies]}")
        if new_companies:
            logging.info(f"  - New: {[c.get('name') for c in new_companies]}")
        logging.info(f"=" * 60)
        
    except Exception as e:
        logging.error(f"Error validating companies: {e}", exc_info=True)
        ti.xcom_push(key="company_info", value={
            "company_results": {"total": 0, "results": []},
            "new_companies": [],
            "partner_status": None
        })


def validate_deals_against_associations(ti, **context):
    """
    Compare deals mentioned in prompt vs associated deals.
    Always include ALL associated deals as existing, and add unmatched mentioned as new.
    """
    entity_flags = ti.xcom_pull(key="entity_search_flags", task_ids="analyze_thread_entities", default={})
    if not entity_flags.get("search_contacts", True):
        logging.info("Contact search was skipped, so skipping deal association validation")
        # Push empty result
        ti.xcom_push(key="deal_info", value={
            "deal_results": {"total": 0, "results": []},
            "new_deals": []
        })
        return

    def normalize_text(text):
        """Normalize text for comparison"""
        import unicodedata
        
        if not text:
            return ""
        
        text = text.lower()
        text = unicodedata.normalize('NFKD', text)
        text = re.sub(r'[-–—‐‑‒―]', '', text)
        text = re.sub(r'\s+', '', text)
        text = re.sub(r'[^\w]', '', text)
        
        return text

    contact_data = ti.xcom_pull(key="contact_info_with_associations",task_ids = "search_contacts_with_associations", default={})
    logging.info(f"contact data : {contact_data}")
    associated_deals = contact_data.get("associated_deals", [])
    contact_results = contact_data.get("contact_results", {})
    logging.info(f"contact_results:{contact_results}")
    # if contact_results.get("total", 0) == 0 and len(associated_deals) == 0:
    #     logging.info("No contacts and no associated deals - skipping validation to preserve direct search results")
    #     return
    
    chat_history = ti.xcom_pull(key="chat_history", task_ids = "load_context_from_dag_run", default=[])
    latest_message = ti.xcom_pull(key="latest_message", task_ids = "load_context_from_dag_run", default="")
    owner_info = ti.xcom_pull(key="owner_info",task_ids = "determine_owner", default={})
    
    deal_owner_name = owner_info.get('deal_owner_name', 'Kishore')
    
    # AI extracts deals mentioned in email
    prompt = f"""Extract deals mentioned in this email. Only include if there's CLEAR buying intent.
    **You CANNOT create deals in HubSpot.**

    YOU ARE A JSON-ONLY API. 
    DO NOT WRITE ANY TEXT, EXPLANATION, OR NARRATIVE.
    DO NOT USE <think> TAGS.
    DO NOT SAY "invoking" OR "successful".
    IMMEDIATELY OUTPUT THE RAW JSON AND NOTHING ELSE. 

LATEST MESSAGE:
{latest_message}

Deal Owner: {deal_owner_name}

RULES:
- Strictly follow these rules, for new deals, if the deal name is not mentioned by the user :
   - Extract the Client Name (company or individual being sold to) from the email.
   - Check if it's a direct deal (no partner) or partner deal (partner or intermediary mentioned).
   - Direct deal: <Client Name>-<Deal Name>
   - Partner deal: <Partner Name>-<Client Name>-<Deal Name>
   - Use the Deal Name from the email if specified; otherwise, create a concise one based on the description (e.g., product or service discussed).
- If the deal name is mentioned by the user, use that as the deal name without fail.
- Only extract if explicit deal creation requested OR clear buying signals.Do not create any deals if there is no clear buying intent.
- NOT exploratory conversations
- Return deal name, stage (default: Lead), amount, close date and deal owner name
FOR EACH DEAL, YOU MUST PROVIDE:
    1. dealName: The name of the deal (required)
    2. dealLabelName: The stage (default to "Lead" if not specified or invalid)
    3. dealAmount: The deal value (default to "5000" if not specified)
    4. closeDate: Expected close date in YYYY-MM-DD format (default to three months from current date if not specified)
    5. dealOwnerName: {deal_owner_name}
- ALL fields are REQUIRED for every deal

Return ONLY valid JSON:
{{
    "deals": [
        {{
            "dealName": "...",
            "dealLabelName": "...",
            "dealAmount": "....",
            "closeDate": "YYYY-MM-DD",
            "dealOwnerName": "{deal_owner_name}"
        }}
    ]
}}
"""
    try:
        response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
    except Exception as e:
        raise

    response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
    logging.info(f"Raw AI response for deal validation: {response[:1000]}...")
    try:
        parsed = json.loads(response.strip())
        mentioned_deals = parsed.get("deals", [])
        
        # ✅ FIX: Use improved normalization
        normalized_associated = {}
        for assoc in associated_deals:
            original_name = assoc.get("dealName", "").strip()
            if not original_name:
                continue
            
            # Use the new normalize_text function
            normalized_key = normalize_text(original_name)
            
            # If duplicate keys exist, keep the first one
            if normalized_key not in normalized_associated:
                normalized_associated[normalized_key] = assoc
                logging.info(f"Normalized existing deal: '{original_name}' → '{normalized_key}'")
            else:
                logging.info(f"Duplicate existing deal skipped: '{original_name}' (already have '{normalized_associated[normalized_key].get('dealName')}')")
        
        logging.info(f"Associated deals found: {len(associated_deals)}")
        logging.info(f"Unique normalized deals: {len(normalized_associated)}")
        logging.info(f"Normalized keys: {list(normalized_associated.keys())}")
        
        # Extract deal names for logging
        extracted_names = [d.get("dealName", "").strip() for d in mentioned_deals if d.get("dealName", "").strip()]
        logging.info(f"Mentioned deals from email: {extracted_names}")
        
        # Start with ALL unique associated deals as existing
        existing_deals = list(normalized_associated.values())
        
        # Now add unmatched mentioned as new
        new_deals = []
        
        for mentioned in mentioned_deals:
            mentioned_name = mentioned.get("dealName", "").strip()
            
            if not mentioned_name:
                logging.warning(f"Skipping deal with no name: {mentioned}")
                continue
            
            # Use the new normalize_text function
            mentioned_normalized = normalize_text(mentioned_name)
            
            logging.info(f"Checking deal: '{mentioned_name}' → normalized: '{mentioned_normalized}'")
            
            # Check if already exists
            if mentioned_normalized in normalized_associated:
                logging.info(f"✓ EXISTING deal matched: '{mentioned_name}' (matches '{normalized_associated[mentioned_normalized].get('dealName')}')")
                continue  # Skip - already in existing_deals
            
            # Not found - add to new deals with validation
            if not mentioned.get("dealLabelName"):
                mentioned["dealLabelName"] = "Lead"
            if not mentioned.get("dealAmount"):
                mentioned["dealAmount"] = "5000"
            if not mentioned.get("dealOwnerName"):
                mentioned["dealOwnerName"] = deal_owner_name
            
            new_deals.append(mentioned)
            logging.info(f"✓ NEW deal to create: '{mentioned_name}' (normalized: '{mentioned_normalized}')")
        
        result = {
            "deal_results": {
                "total": len(existing_deals),
                "results": existing_deals
            },
            "new_deals": new_deals
        }
        
        ti.xcom_push(key="deal_info", value=result)
        logging.info(f"DEAL VALIDATION SUMMARY:")
        logging.info(f"  - Existing deals: {len(existing_deals)}")
        logging.info(f"  - New deals to create: {len(new_deals)}")
        if existing_deals:
            logging.info(f"  - Existing: {[d.get('dealName') for d in existing_deals]}")
        if new_deals:
            logging.info(f"  - New: {[d.get('dealName') for d in new_deals]}")
        logging.info(f"=" * 60)
        
    except Exception as e:
        logging.error(f"Error validating deals: {e}", exc_info=True)
        ti.xcom_push(key="deal_info", value={
            "deal_results": {"total": 0, "results": []},
            "new_deals": []
        })

def refine_contacts_by_associations(ti, **context):
    """
    Refine the list of found contacts to ONLY those associated with
    the validated (relevant) companies and/or deals from the email context.
    
    CRITICAL RULE:
    - If there are NO relevant companies AND NO relevant deals → discard ALL existing contacts.
      Only new_contacts should proceed downstream.
    - Otherwise, prioritize contacts linked to relevant companies/deals.
    """
    contact_info = ti.xcom_pull(key="contact_info_with_associations",task_ids = "search_contacts_with_associations", default={})
    logging.info(f"contact_info:{contact_info}")
    company_info = ti.xcom_pull(key="company_info",task_ids = "validate_companies_against_associations", default={})
    deal_info = ti.xcom_pull(key="deal_info",task_ids = "validate_deals_against_associations", default={})

    original_contacts = contact_info.get("contact_results", {}).get("results", [])
    if not original_contacts:
        logging.info("No contacts to refine")
        # Preserve new_contacts, refined = []
        refined_contact_info = {
            "contact_results": {"total": 0, "results": []},
            "new_contacts": contact_info.get("new_contacts", []),
            "associated_companies": contact_info.get("associated_companies", []),
            "associated_deals": contact_info.get("associated_deals", [])
        }
        ti.xcom_push(key="contact_info_with_associations", value=refined_contact_info)
        return

    # Extract relevant (validated) company and deal IDs
    relevant_company_ids = {c.get("companyId") for c in company_info.get("company_results", {}).get("results", []) if c.get("companyId")}
    relevant_deal_ids = {d.get("dealId") for d in deal_info.get("deal_results", {}).get("results", []) if d.get("dealId")}

    logging.info(f"Refining contacts using {len(relevant_company_ids)} relevant companies and {len(relevant_deal_ids)} relevant deals")

    new_contacts = contact_info.get("new_contacts", [])
    associated_companies = contact_info.get("associated_companies", [])
    associated_deals = contact_info.get("associated_deals", [])

    if not relevant_company_ids and not relevant_deal_ids:
        logging.info("No relevant companies or deals found → discarding all existing contacts (refined = 0)")
        refined_contacts = []
    else:
        high_priority_contacts = []
        fallback_contacts = []  # Only used if we have relevant entities

        for contact in original_contacts:
            contact_id = contact.get("contactId")
            if not contact_id:
                fallback_contacts.append(contact)
                continue

            # Get this specific contact's associations (from earlier pull)
            # Note: associated_companies/deals are per-contact in the original search
            contact_comp_ids = {c.get("companyId") for c in associated_companies if c.get("companyId")}
            contact_deal_ids = {d.get("dealId") for d in associated_deals if d.get("dealId")}

            if (contact_comp_ids & relevant_company_ids) or (contact_deal_ids & relevant_deal_ids):
                high_priority_contacts.append(contact)
            else:
                fallback_contacts.append(contact)

        refined_contacts = high_priority_contacts + fallback_contacts
        logging.info(f"Refined: {len(high_priority_contacts)} directly linked + {len(fallback_contacts)} fallback")

    refined_total = len(refined_contacts)
    logging.info(f"Contact refinement complete:")
    logging.info(f"   → Directly linked to relevant company/deal: {len(high_priority_contacts) if 'high_priority_contacts' in locals() else 0}")
    logging.info(f"   → Fallback (name-matched only): {refined_total - len(high_priority_contacts) if 'high_priority_contacts' in locals() else refined_total}")
    logging.info(f"   → Total refined contacts: {refined_total}")
    logging.info(f"   → New contacts preserved: {len(new_contacts)}")

    refined_contact_info = {
        "contact_results": {
            "total": refined_total,
            "results": refined_contacts
        },
        "new_contacts": new_contacts,  # Always preserve
        "associated_companies": associated_companies,
        "associated_deals": associated_deals
    }
    # === NEW: Add owner names to all contacts ===
    owner_info = ti.xcom_pull(key="owner_info",task_ids = "determine_owner", default={})
    all_owners_table = owner_info.get("all_owners_table", [])

    # Fix existing contacts
    for contact in refined_contacts:
        if not contact.get("contactOwnerName") and contact.get("contactOwnerId"):
            contact["contactOwnerName"] = get_owner_name_from_id(
                contact["contactOwnerId"], 
                all_owners_table
            )

    # Fix new contacts (they might already have names, but ensure they're correct)
    for contact in new_contacts:
        if not contact.get("contactOwnerName"):
            contact["contactOwnerName"] = owner_info.get("contact_owner_name", "Kishore")

    ti.xcom_push(key="contact_info_with_associations", value=refined_contact_info)

def search_deals_directly(ti, **context):
    """
    Search for deals directly by name, independent of contact associations.
    This ensures we find existing deals even when no contacts are specified.
    """
    contact_data = ti.xcom_pull(key="contact_info_with_associations", task_ids="search_contacts_with_associations", default={})
    contact_results = contact_data.get("contact_results", {})
    new_contacts = contact_data.get("new_contacts", [])
    logging.info(f"Contact results before direct deal search: {contact_results} new contacts: {len(new_contacts)}")
    if contact_results.get("total", 0) > 0:
        logging.info("No contacts - skipping direct search results")
        return
    
    entity_flags = ti.xcom_pull(key="entity_search_flags",task_ids = "analyze_thread_entities", default={})
    if not entity_flags.get("search_deals", True):
        logging.info(f"Skipping direct deal search: {entity_flags.get('deals_reason', 'Not mentioned')}")
        ti.xcom_push(key="direct_deal_results", value={"total": 0, "results": []})
        return
    
    chat_history = ti.xcom_pull(key="chat_history", task_ids = "load_context_from_dag_run", default=[])
    latest_message = ti.xcom_pull(key="latest_message", task_ids = "load_context_from_dag_run", default="")
    owner_info = ti.xcom_pull(key="owner_info", task_ids = "determine_owner", default={})
    
    # AI extracts deal names from email
    prompt = f"""Extract ALL deal names explicitly mentioned in this email.
    **You CANNOT create deals in HubSpot.**

    YOU ARE A JSON-ONLY API. 
    DO NOT WRITE ANY TEXT, EXPLANATION, OR NARRATIVE.
    DO NOT USE <think> TAGS.
    DO NOT SAY "invoking" OR "successful".
    IMMEDIATELY OUTPUT THE RAW JSON AND NOTHING ELSE.

LATEST MESSAGE:
{latest_message}

RULES:
- Only extract if there's a clear reference to an existing deal/opportunity
- Include phrases like "the ABC deal", "our project with XYZ", "opportunity for..."
- Exclude vague references without specific names
- search by the deal name extracted using `search_deals`.

Return ONLY valid JSON:
{{
    "deals": [
        {{
            "dealName": "...",
            "dealLabelName": "...",
            "dealAmount": "....",
            "closeDate": "YYYY-MM-DD",
            "dealOwnerName": "hubspot_owner_name"
        }}
    ]
}}
"""
    try:
        response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
    except Exception as e:
        raise
    try:
        parsed = json.loads(response.strip())
        mentioned_deals = parsed.get("deals", [])
        
        if not mentioned_deals:
            logging.info("No deal names extracted for direct search")
            ti.xcom_push(key="direct_deal_results", value={"total": 0, "results": []})
            return
        
        # Search each deal in HubSpot
        found_deals = []
        
        for deal_ref in mentioned_deals:
            deal_name = deal_ref.get("dealName", "").strip()
            logging.info(f"Direct deal search for extracted deal name: '{deal_name}'")
            if not deal_name:
                continue
            
            # Search HubSpot for deal by name
            try:
                endpoint = f"{HUBSPOT_BASE_URL}/crm/v3/objects/deals/search"
                headers = {
                    "Authorization": f"Bearer {HUBSPOT_API_KEY}",
                    "Content-Type": "application/json"
                }
                
                payload = {
                    "filterGroups": [{
                        "filters": [{
                            "propertyName": "dealname",
                            "operator": "CONTAINS_TOKEN",
                            "value": deal_name
                        }]
                    }],
                    "properties": [
                        "hs_object_id", "dealname", "dealstage", "amount",
                        "closedate", "hubspot_owner_id"
                    ],
                    "limit": 10
                }
                
                response = requests.post(endpoint, headers=headers, json=payload, timeout=30)
                logging.info(f"Searching for deal '{deal_name}' in HubSpot")
                response.raise_for_status()
                
                results = response.json().get("results", [])
                logging.info(f"HubSpot returned {results} results for deal search '{deal_name}'")
                
                for deal in results:
                    props = deal.get("properties", {})
                    stage_id = props.get("dealstage", "")
                    stage_label = get_deal_stage_label(stage_id)
                    
                    found_deals.append({
                        "dealId": deal.get("id"),
                        "dealName": props.get("dealname", ""),
                        "dealLabelName": stage_label,
                        "dealAmount": props.get("amount", ""),
                        "closeDate": props.get("closedate", ""),
                        "dealOwnerId": props.get("hubspot_owner_id", "")
                    })
                
                logging.info(f"Found {len(results)} deals matching '{deal_name}'")
                
            except Exception as e:
                logging.error(f"Error searching for deal '{deal_name}': {e}")
                continue
        
        result = {
            "total": len(found_deals),
            "results": found_deals
        }
        
        ti.xcom_push(key="direct_deal_results", value=result)
        logging.info(f"Direct deal search completed: {len(found_deals)} deals found")
        
    except Exception as e:
        logging.error(f"Error in direct deal search: {e}")
        ti.xcom_push(key="direct_deal_results", value={"total": 0, "results": []})


def search_companies_directly(ti, **context):
    """
    Search for companies directly by name, independent of contact associations.
    """
    contact_data = ti.xcom_pull(key="contact_info_with_associations", task_ids="search_contacts_with_associations", default={})
    contact_results = contact_data.get("contact_results", {})
    new_contacts = contact_data.get("new_contacts", [])
    logging.info(f"Contact results before direct deal search: {contact_results}")
    if contact_results.get("total", 0) > 0 or len(new_contacts) > 0:
        logging.info("No company - skipping direct search results")
        return
    entity_flags = ti.xcom_pull(key="entity_search_flags", task_ids="analyze_thread_entities", default={})
    if not entity_flags.get("search_companies", True):
        logging.info(f"Skipping direct company search: {entity_flags.get('companies_reason', 'Not mentioned')}")
        ti.xcom_push(key="direct_company_results", value={"total": 0, "results": []})
        return
    
    chat_history = ti.xcom_pull(key="chat_history", task_ids = "load_context_from_dag_run", default=[])
    latest_message = ti.xcom_pull(key="latest_message",  task_ids = "load_context_from_dag_run", default="")
    
    # AI extracts company names
    prompt = f"""Extract ALL company names explicitly mentioned in this email.

    YOU ARE A JSON-ONLY API. 
    DO NOT WRITE ANY TEXT, EXPLANATION, OR NARRATIVE.
    DO NOT USE <think> TAGS.
    DO NOT SAY "invoking" OR "successful".
    IMMEDIATELY OUTPUT THE RAW JSON AND NOTHING ELSE.

LATEST MESSAGE:
{latest_message}

RULES:
- Only extract formal company/organization names
- Exclude any company if its name or domain matches:lowtouch.ai, hybcloud technologies, ccs, cc, cloud control, cloudcontrol, ecloudcontrol.These are INTERNAL companies and must not be included in outputs.
- Return empty list if no companies mentioned
- Only search for companies if it is explicitly mentioned in the email by the user.**Do not** infer or assume any company names from deal name.

Return ONLY valid JSON:
{{
    "companies": [
        {{"name": "...", "domain": "..."}}
    ]
}}
"""
    try:
        response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
    except Exception as e:
        raise
    try:
        parsed = json.loads(response.strip())
        mentioned_companies = parsed.get("companies", [])
        
        if not mentioned_companies:
            logging.info("No company names extracted for direct search")
            ti.xcom_push(key="direct_company_results", value={"total": 0, "results": []})
            return
        
        # Search each company in HubSpot
        found_companies = []
        
        for company_ref in mentioned_companies:
            company_name = company_ref.get("name", "").strip()
            if not company_name:
                continue
            
            # Search HubSpot for company by name
            try:
                endpoint = f"{HUBSPOT_BASE_URL}/crm/v3/objects/companies/search"
                headers = {
                    "Authorization": f"Bearer {HUBSPOT_API_KEY}",
                    "Content-Type": "application/json"
                }
                
                payload = {
                    "filterGroups": [{
                        "filters": [{
                            "propertyName": "name",
                            "operator": "CONTAINS_TOKEN",
                            "value": company_name
                        }]
                    }],
                    "properties": [
                        "hs_object_id", "name", "domain", "address", "city",
                        "state", "zip", "country", "phone", "description", "type"
                    ],
                    "limit": 10
                }
                
                response = requests.post(endpoint, headers=headers, json=payload, timeout=30)
                response.raise_for_status()
                
                results = response.json().get("results", [])
                
                for company in results:
                    props = company.get("properties", {})
                    found_companies.append({
                        "companyId": company.get("id"),
                        "name": props.get("name", ""),
                        "domain": props.get("domain", ""),
                        "address": props.get("address", ""),
                        "city": props.get("city", ""),
                        "state": props.get("state", ""),
                        "zip": props.get("zip", ""),
                        "country": props.get("country", ""),
                        "phone": props.get("phone", ""),
                        "description": props.get("description", ""),
                        "type": props.get("type", "")
                    })
                
                logging.info(f"Found {len(results)} companies matching '{company_name}'")
                
            except Exception as e:
                logging.error(f"Error searching for company '{company_name}': {e}")
                continue
        
        result = {
            "total": len(found_companies),
            "results": found_companies
        }
        
        ti.xcom_push(key="direct_company_results", value=result)
        logging.info(f"Direct company search completed: {len(found_companies)} companies found")
        
    except Exception as e:
        logging.error(f"Error in direct company search: {e}")
        ti.xcom_push(key="direct_company_results", value={"total": 0, "results": []})
    
def merge_search_results(ti, **context):
    """
    Merge results from contact-association searches and direct searches.
    Prioritize direct searches to ensure standalone entities are found.
    """
    # Get association-based results
    contact_info = ti.xcom_pull(key="contact_info_with_associations",task_ids="refine_contacts_by_associations", default={})
    company_info = ti.xcom_pull(key="company_info", task_ids="validate_companies_against_associations", default={})
    deal_info = ti.xcom_pull(key="deal_info", task_ids="validate_deals_against_associations",default={})
    
    # Get direct search results
    direct_deals = ti.xcom_pull(key="direct_deal_results", task_ids="search_deals_directly", default={"total": 0, "results": []}) or {"total": 0, "results": []}
    direct_companies = ti.xcom_pull(key="direct_company_results", task_ids="search_companies_directly", default={"total": 0, "results": []}) or {"total": 0, "results": []}
    
    # Log what we received
    logging.info(f"=== MERGE INPUT ===")
    logging.info(f"Association-based company results: {company_info.get('company_results', {}).get('total', 0)} existing")
    logging.info(f"Association-based new companies: {len(company_info.get('new_companies', []))}")
    logging.info(f"Direct search companies: {direct_companies.get('total', 0)}")
    
    logging.info(f"Association-based deal results: {deal_info.get('deal_results', {}).get('total', 0)} existing")
    logging.info(f"Association-based new deals: {len(deal_info.get('new_deals', []))}")
    logging.info(f"Direct search deals: {direct_deals.get('total', 0)}")
    
    # ========== MERGE EXISTING DEALS ==========
    # Start with deals from association search
    all_deals = list(deal_info.get("deal_results", {}).get("results", []))
    existing_deal_ids = {d.get("dealId") for d in all_deals if d.get("dealId")}
    
    for direct_deal in direct_deals.get("results", []):
        if direct_deal.get("dealId") not in existing_deal_ids:
            all_deals.append(direct_deal)
            existing_deal_ids.add(direct_deal.get("dealId"))
    
    # Merge companies (remove duplicates by companyId)
    all_companies = list(company_info.get("company_results", {}).get("results", []))
    existing_company_ids = {c.get("companyId") for c in all_companies if c.get("companyId")}
    
    for direct_company in direct_companies.get("results", []):
        if direct_company.get("companyId") not in existing_company_ids:
            all_companies.append(direct_company)
            existing_company_ids.add(direct_company.get("companyId"))
    
    # ========== PRESERVE NEW ENTITIES FROM VALIDATION ONLY ==========
    # New entities come ONLY from validation tasks, not direct search
    new_deals = deal_info.get("new_deals", [])
    new_companies = company_info.get("new_companies", [])

    owner_info = ti.xcom_pull(key="owner_info", task_ids = "determine_owner",default={})
    all_owners_table = owner_info.get("all_owners_table", [])
    deal_owner_name = owner_info.get("deal_owner_name", "Kishore")
    
    for deal in all_deals:
        if not deal.get("dealOwnerName") and deal.get("dealOwnerId"):
            deal["dealOwnerName"] = get_owner_name_from_id(
                deal["dealOwnerId"],
                all_owners_table
            )
    
    for deal in new_deals:
        if not deal.get("dealOwnerName"):
            deal["dealOwnerName"] = deal_owner_name
    
    # ========== CREATE MERGED RESULTS ==========
    merged_deal_info = {
        "deal_results": {
            "total": len(all_deals),
            "results": all_deals
        },
        "new_deals": new_deals  # From validation only
    }
    
    merged_company_info = {
        "company_results": {
            "total": len(all_companies),
            "results": all_companies
        },
        "new_companies": new_companies,  # From validation only
        "partner_status": company_info.get("partner_status", None)
    }
    
    # Push to BOTH original and merged keys to ensure compatibility
    ti.xcom_push(key="deal_info", value=merged_deal_info)
    ti.xcom_push(key="company_info", value=merged_company_info)
    ti.xcom_push(key="merged_deal_info", value=merged_deal_info)  # Additional key
    ti.xcom_push(key="merged_company_info", value=merged_company_info)  # Additional key
    
    logging.info(f"=== MERGED SEARCH RESULTS ===")
    logging.info(f"Existing Deals: {len(all_deals)} total ({direct_deals.get('total', 0)} from direct search)")
    logging.info(f"New Deals (to create): {len(new_deals)} (from validation only)")
    logging.info(f"Existing Companies: {len(all_companies)} total ({direct_companies.get('total', 0)} from direct search)")
    logging.info(f"New Companies (to create): {len(new_companies)} (from validation only)")
    
    if new_companies:
        logging.info(f"New companies to create: {[c.get('name') for c in new_companies]}")
    if new_deals:
        logging.info(f"New deals to create: {[d.get('dealName') for d in new_deals]}")
    
    logging.info(f"Deal IDs in merged results: {[d.get('dealId') for d in all_deals]}")
    logging.info(f"Company IDs in merged results: {[c.get('companyId') for c in all_companies]}")

def validate_associations_against_context(ti, **context):
    """
    Validate that all associations (contacts, companies, deals) match the user's actual context.
    Create new entities when user context differs from existing associations.
    
    CRITICAL RULES:
    1. If user mentions "Contact X works at Company Y", create new contact even if Contact X exists elsewhere
    2. Only show existing deals/companies if they're explicitly mentioned in user's message
    3. Match contacts by FULL context (firstname + lastname + company), not just firstname
    4. Abandon irrelevant associations - don't show everything linked to a contact
    """
    
    chat_history = ti.xcom_pull(key="chat_history", default=[])
    latest_message = ti.xcom_pull(key="latest_message", default="")
    
    # Get current results
    contact_info = ti.xcom_pull(key="contact_info_with_associations", task_ids="search_contacts_with_associations", default={})
    company_info = ti.xcom_pull(key="merged_company_info",task_ids ="merge_search_results" ) or ti.xcom_pull(key="company_info",task_ids ="merge_search_results") or {}
    deal_info = ti.xcom_pull(key="merged_deal_info",task_ids ="merge_search_results") or ti.xcom_pull(key="deal_info",task_ids ="merge_search_results") or {}
    
    existing_contacts = contact_info.get("contact_results", {}).get("results", [])
    new_contacts = contact_info.get("new_contacts", [])
    existing_companies = company_info.get("company_results", {}).get("results", [])
    new_companies = company_info.get("new_companies", [])
    existing_deals = deal_info.get("deal_results", {}).get("results", [])
    new_deals = deal_info.get("new_deals", [])
    
    # Format existing data for AI prompt
    existing_contacts_summary = json.dumps([
        {
            "firstname": c.get("firstname"),
            "lastname": c.get("lastname"),
            "email": c.get("email"),
            "contactId": c.get("contactId")
        } for c in existing_contacts
    ], indent=2)
    
    new_contacts_summary = json.dumps([
        {
            "firstname": c.get("firstname"),
            "lastname": c.get("lastname"),
            "email": c.get("email")
        } for c in new_contacts
    ], indent=2)
    
    existing_companies_summary = json.dumps([
        {
            "name": c.get("name"),
            "domain": c.get("domain"),
            "companyId": c.get("companyId")
        } for c in existing_companies
    ], indent=2)
    
    new_companies_summary = json.dumps([
        {
            "name": c.get("name"),
            "domain": c.get("domain")
        } for c in new_companies
    ], indent=2)
    
    existing_deals_summary = json.dumps([
        {
            "dealName": d.get("dealName"),
            "dealId": d.get("dealId")
        } for d in existing_deals
    ], indent=2)
    
    new_deals_summary = json.dumps([
        {
            "dealName": d.get("dealName")
        } for d in new_deals
    ], indent=2)
    
    # AI analyzes user context to extract precise associations
    prompt = f"""You are a HubSpot context validator. Analyze this email to identify EXACTLY which contacts, companies, and deals the user is referring to, and whether existing associations are relevant.
    You cannot create or update any records, your only job is to identify and validate the contacts, companies, and deals based on the conversation.

YOU ARE A JSON-ONLY API. 
DO NOT WRITE ANY TEXT, EXPLANATION, OR NARRATIVE.
DO NOT USE <think> TAGS.
DO NOT SAY "invoking" OR "successful".
IMMEDIATELY OUTPUT THE RAW JSON AND NOTHING ELSE.

LATEST USER MESSAGE:
{latest_message}

CURRENT SEARCH RESULTS:

Existing Contacts Found:
{existing_contacts_summary}

New Contacts to Create:
{new_contacts_summary}

Existing Companies Found:
{existing_companies_summary}

New Companies to Create:
{new_companies_summary}

Existing Deals Found:
{existing_deals_summary}

New Deals to Create:
{new_deals_summary}

YOUR TASK:
For each contact mentioned in the user's message, determine:
1. **Full Identity Match**: Does the user provide firstname + lastname + company context?
2. **Context Match**: If an existing contact is found, does their CURRENT company match what the user stated?
3. **Association Relevance**: Are the existing deals/companies associated with this contact relevant to user's request?

CRITICAL RULES:
- If user says about a contact X from a company Y but if there is a same named contact from another company Z, CREATE NEW contact for X and associate with Y
- Only include existing deals/companies if they're EXPLICITLY mentioned in user's message or clearly relevant to context
- Match contacts by: (firstname + lastname + company context), NOT just firstname
- If company context differs, treat as DIFFERENT contact - create new one

EXAMPLES:

Example 1 - Create New Contact (Company Mismatch):
User: "Meeting with Rahul Mehta from MedAxis Hospitals"
Existing: Rahul Mehta (contactId: 123) at TechCorp Solutions
Action: Create NEW contact "Rahul Mehta" for MedAxis. Do NOT use contactId 123.
Reason: Company context doesn't match - existing Rahul is at TechCorp, user mentioned MedAxis.

Example 2 - Use Existing Contact (Perfect Match):
User: "Follow up with Sarah Collins at HealthBridge Consulting"
Existing: Sarah Collins (contactId: 456) at HealthBridge Consulting
Action: Keep existing Sarah (contactId: 456). Show only HealthBridge associations.
Reason: Name AND company context both match.

Example 3 - Use Existing (No Company Specified):
User: "Call Sarah Collins tomorrow"
Existing: Sarah Collins (contactId: 456) at HealthBridge Consulting
Action: Keep existing Sarah (contactId: 456).
Reason: User didn't specify company, so existing contact is valid.

Example 4 - Create New (Company Specified, Different):
User: "Sarah Collins from Acme Corp wants a demo"
Existing: Sarah Collins (contactId: 456) at HealthBridge Consulting
Action: Create NEW contact "Sarah Collins" for Acme Corp. Do NOT use contactId 456.
Reason: User specified Acme Corp, but existing Sarah works at HealthBridge.

Return this exact JSON structure:
{{{{
    "validated_contacts": [
        {{{{
            "mentioned_name": "Full Name from user message",
            "mentioned_company": "Company from user message",
            "use_existing": true/false,
            "existing_contact_id": "contact_id or null",
            "action": "use_existing" or "create_new",
            "reason": "Why this decision was made",
            "create_new_details": {{{{
                "firstname": "...",
                "lastname": "...",
                "company_name": "...",
                "email": "..."
            }}}} or null
        }}}}
    ],
    "validated_companies": [
        {{{{
            "mentioned_name": "Company from user message",
            "use_existing": true/false,
            "existing_company_id": "company_id or null",
            "action": "use_existing" or "create_new",
            "reason": "Why this decision was made"
        }}}}
    ],
    "validated_deals": [
        {{{{
            "mentioned_name": "Deal from user message",
            "use_existing": true/false,
            "existing_deal_id": "deal_id or null",
            "action": "use_existing" or "create_new",
            "reason": "Why this decision was made"
        }}}}
    ],
    "irrelevant_associations": {{{{
        "contact_ids_to_remove": ["contact_id_1", "contact_id_2"],
        "company_ids_to_remove": ["company_id_1"],
        "deal_ids_to_remove": ["deal_id_1"]
    }}}}
}}}}

RESPOND WITH ONLY THE JSON OBJECT - NO OTHER TEXT."""

    try:
        logging.info("prompt:{prompt}")
        response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
        logging.info(f"Raw AI response for context validation: {response[:1000]}...")
        
        validation_result = json.loads(response.strip())
        
        # Process validation results
        validated_contacts_list = validation_result.get("validated_contacts", [])
        validated_companies_list = validation_result.get("validated_companies", [])
        validated_deals_list = validation_result.get("validated_deals", [])
        irrelevant = validation_result.get("irrelevant_associations", {})
        
        # Build new contact list (keeping only relevant existing + adding new ones)
        final_existing_contacts = []
        final_new_contacts = list(new_contacts)  # Start with already identified new contacts
        
        irrelevant_contact_ids = set(irrelevant.get("contact_ids_to_remove", []))
        
        owner_info = ti.xcom_pull(key="owner_info", task_ids = "determine_owner",default={})
        contact_owner_name = owner_info.get("contact_owner_name", "Kishore")
        
        for val in validated_contacts_list:
            action = val.get("action")
            
            if action == "use_existing":
                contact_id = val.get("existing_contact_id")
                if not contact_id:
                    continue
                contact = next((c for c in existing_contacts if c.get("contactId") == contact_id), None)
                if contact and contact_id not in irrelevant_contact_ids:
                    final_existing_contacts.append(contact)
            
            elif action == "create_new":
                details = val.get("create_new_details") or {}
                firstname = (details.get("firstname") or "").strip()
                lastname = (details.get("lastname") or "").strip()
                email = (details.get("email") or "").strip()
                
                if not firstname or not lastname:
                    logging.warning(f"Skipping invalid new contact - missing name: {details}")
                    continue
                
                # Duplicate check using tuple for consistency
                key = (firstname.lower(), lastname.lower(), email.lower())
                if any(
                    (nc.get("firstname","").strip().lower(),
                     nc.get("lastname","").strip().lower(),
                     nc.get("email","").strip().lower()) == key
                    for nc in final_new_contacts
                ):
                    logging.info(f"⚠ Skipping duplicate new contact: {firstname} {lastname}")
                    continue
                
                final_new_contacts.append({
                    "firstname": firstname,
                    "lastname": lastname,
                    "email": email,
                    "phone": "",
                    "address": "",
                    "jobtitle": "",
                    "contactOwnerName": contact_owner_name,
                    "company_context": details.get("company_name", "")
                })
                logging.info(f"✓ Creating new contact: {firstname} {lastname} at {details.get('company_name','')}")
        
        # Process companies
        final_existing_companies = []
        final_new_companies = list(new_companies)
        
        irrelevant_company_ids = set(irrelevant.get("company_ids_to_remove", []))
        
        for val in validated_companies_list:
            action = val.get("action")
            name = (val.get("mentioned_name") or "").strip()
            
            if not name:
                continue
                
            if action == "use_existing":
                company_id = val.get("existing_company_id")
                if not company_id:
                    continue
                company = next((c for c in existing_companies if c.get("companyId") == company_id), None)
                if company and company_id not in irrelevant_company_ids:
                    final_existing_companies.append(company)
            
            elif action == "create_new":
                name_lower = name.lower()
                if any(c.get("name","").strip().lower() == name_lower for c in final_existing_companies):
                    logging.info(f"⚠ Company '{name}' already in existing - skipping")
                    continue
                if any(nc.get("name","").strip().lower() == name_lower for nc in final_new_companies):
                    logging.info(f"⚠ Company '{name}' already in new companies - skipping")
                    continue
                
                final_new_companies.append({
                    "name": name,
                    "domain": "",
                    "address": "",
                    "city": "",
                    "state": "",
                    "zip": "",
                    "country": "",
                    "phone": "",
                    "description": "",
                    "type": "PROSPECT"
                })
                logging.info(f"✓ Creating new company: {name}")
        
        # Process deals
        final_existing_deals = []
        final_new_deals = list(new_deals)
        
        irrelevant_deal_ids = set(irrelevant.get("deal_ids_to_remove", []))
        
        deal_owner_name = owner_info.get("deal_owner_name", "Kishore")
        
        for val in validated_deals_list:
            action = val.get("action")
            name = (val.get("mentioned_name") or "").strip()
            
            if not name:
                continue
                
            if action == "use_existing":
                deal_id = val.get("existing_deal_id")
                if not deal_id:
                    continue
                deal = next((d for d in existing_deals if d.get("dealId") == deal_id), None)
                if deal and deal_id not in irrelevant_deal_ids:
                    final_existing_deals.append(deal)
                    logging.info(f"✓ Keeping existing deal: {deal.get('dealName')}")
            
            elif action == "create_new":
                name_lower = name.lower()
                if any(nd.get("dealName","").strip().lower() == name_lower for nd in final_new_deals):
                    logging.info(f"⚠ Skipping duplicate new deal: {name}")
                    continue
                
                final_new_deals.append({
                    "dealName": name,
                    "dealLabelName": "",
                    "dealAmount": "",
                    "closeDate": "",
                    "dealOwnerName": deal_owner_name
                })
                logging.info(f"✓ Creating new deal: {name}")
        
        # Update XCom with validated results
        validated_contact_info = {
            "contact_results": {
                "total": len(final_existing_contacts),
                "results": final_existing_contacts
            },
            "new_contacts": final_new_contacts,
            "associated_companies": contact_info.get("associated_companies", []),
            "associated_deals": contact_info.get("associated_deals", [])
        }
        
        validated_company_info = {
            "company_results": {
                "total": len(final_existing_companies),
                "results": final_existing_companies
            },
            "new_companies": final_new_companies,
            "partner_status": company_info.get("partner_status", None)
        }
        
        validated_deal_info = {
            "deal_results": {
                "total": len(final_existing_deals),
                "results": final_existing_deals
            },
            "new_deals": final_new_deals
        }
        
        # Push validated results
        ti.xcom_push(key="contact_info_with_associations", value=validated_contact_info)
        ti.xcom_push(key="company_info", value=validated_company_info)
        ti.xcom_push(key="merged_company_info", value=validated_company_info)
        ti.xcom_push(key="deal_info", value=validated_deal_info)
        ti.xcom_push(key="merged_deal_info", value=validated_deal_info)
        
        logging.info(f"=== CONTEXT VALIDATION COMPLETE ===")
        logging.info(f"Existing contacts (relevant): {len(final_existing_contacts)}")
        logging.info(f"New contacts to create: {len(final_new_contacts)}")
        logging.info(f"Existing companies (relevant): {len(final_existing_companies)}")
        logging.info(f"New companies to create: {len(final_new_companies)}")
        logging.info(f"Existing deals (relevant): {len(final_existing_deals)}")
        logging.info(f"New deals to create: {len(final_new_deals)}")
        logging.info(f"Removed irrelevant: {len(irrelevant_contact_ids)} contacts, {len(irrelevant_company_ids)} companies, {len(irrelevant_deal_ids)} deals")
        
    except Exception as e:
        logging.error(f"Error in context validation: {e}", exc_info=True)
        logging.warning("Validation failed - preserving original search results")

def parse_notes_tasks_meeting(ti, **context):
    """Parse notes, tasks, and meetings from conversation"""
    entity_flags = ti.xcom_pull(key="entity_search_flags", task_ids="analyze_thread_entities", default={})
    email_data = ti.xcom_pull(key="email_data", task_ids = "load_context_from_dag_run", default={})
    headers      = email_data.get("headers", {})
    sender_raw   = headers.get("From", "")                # e.g. "John Doe <john@acme.com>"
    # Parse a clean name and e-mail (fallback to raw string if parsing fails)
    import email.utils
    sender_tuple = email.utils.parseaddr(sender_raw)      # (realname, email)
    sender_name  = sender_tuple[0].strip() or sender_raw
    sender_email = sender_tuple[1].strip() or sender_raw
    
    should_parse_notes = entity_flags.get("parse_notes", True)
    should_parse_tasks = entity_flags.get("parse_tasks", True)
    should_parse_meetings = entity_flags.get("parse_meetings", True)
    
    if not (should_parse_notes or should_parse_tasks or should_parse_meetings):
        logging.info(f"Skipping all parsing")
        ti.xcom_push(key="notes_tasks_meeting", value={
            "notes": [],
            "tasks": [],
            "meeting_details": {}
        })
        return
    
    chat_history = ti.xcom_pull(key="chat_history", task_ids = "load_context_from_dag_run", default=[])
    latest_message = ti.xcom_pull(key="latest_message", task_ids = "load_context_from_dag_run", default="")
    owner_info = ti.xcom_pull(key="owner_info", task_ids = "determine_owner", default={})
    
    task_owners = owner_info.get('task_owners', [])
    default_task_owner_id = "71346067"
    default_task_owner_name = "Kishore"
    
    parsing_instructions = []
    if should_parse_notes:
        parsing_instructions.append("1. Notes - All the email content exactly the same format except branding and signatures should be captured as notes.")
    if should_parse_tasks:
        parsing_instructions.append("2. Tasks - Action items, Next steps with owner and due dates. Adding entities to HubSpot is NOT a task. All the next steps should be logged as tasks.")
    if should_parse_meetings:
        parsing_instructions.append("3. Meeting Details - Title, start time, end time, location, outcome, attendees")

    prompt = f"""You are a HubSpot Conversation Parser. Your role is to **analyze** the email conversation and **extract** only the information explicitly requested.  
**You CANNOT create notes, tasks, or meetings in HubSpot.**  
You may only **parse and structure** data that is **clearly present** in the conversation.

    YOU ARE A JSON-ONLY API. 
    DO NOT WRITE ANY TEXT, EXPLANATION, OR NARRATIVE.
    DO NOT USE <think> TAGS.
    DO NOT SAY "invoking" OR "successful".
    IMMEDIATELY OUTPUT THE RAW JSON AND NOTHING ELSE.

---
**SENDER**  
Name : {sender_name}  
Email: {sender_email}
LATEST USER MESSAGE:
{latest_message}

Task Owners:
{json.dumps(task_owners, indent=2)}

PARSING INSTRUCTIONS (only parse these):
{chr(10).join(parsing_instructions)}

---

**STRICT PARSING RULES (execute in order):**

For notes (only if note parsing is enabled):
- Extract the whole email content except branding and signatures and capture it in the same format as the email came as notes. The should be captured in the email format, if there are new lines gaps headings , the same should be captured.

**SENDER CONTEXT**  
This email was sent by: {sender_name} ({sender_email})  
The latest message is written in first person from their perspective.

**NOTES PARSING RULE (MUST FOLLOW)**  
When extracting notes, you **MUST replace all first-person pronouns** referring to the sender with their actual name: "{sender_name}"
* Default Speaker will always be Sender  
   - If no speaker is mentioned in a sentence, automatically assume it is {sender_name}  
   - Example: "Had a good discussion with Nikhil about pricing" then "{sender_name} had a good discussion with Nikhil about pricing"
* Common Implicit Patterns (automatically attribute to sender):
   - If "Met with X" means "{sender_name} met with X"
   - If "Spoke to X" means "{sender_name} spoke to X"
   - If "Call with X" means "{sender_name} had a call with X"
   - If sentence starts with connected,mentioned and similar words, then it means "{sender_name} connected with X", "{sender_name} mentioned ..."
* Valid replacements:
- I means {sender_name}
- me means {sender_name} 
- my means {sender_name}'s
- I'll means {sender_name} will
- I've means {sender_name} has
- I'm means {sender_name} is
- This is mandatory. The final note stored in HubSpot must be in third person with the real name — never leave "I" in notes.
- Always check for the context of the sentence to identify if the pronoun is referring to the sender or someone else.If the pronoun is referring to someone else, do not replace it with the sender name.Instead keep the pronoun and add pronoun mention {sender_name} in the note.
- Before generating notes, check the Search DAG result.
A. If an existing note already contains any speaker name (speaker context exists):
    * DO NOT apply pronoun replacement
    * DO NOT rewrite or re-introduce speaker attribution
    * DO NOT prepend text like:
    “{sender_name} mentioned …”
- Store the new extracted text exactly as it appears in the email, maintaining original formatting
- The new note must NOT add speaker details again.

For meetings (only if meeting parsing is enabled):
- Extract meeting title, start time, end time, location, outcome, timestamp, attendees, meeting type, and meeting status.
- If "I" is mentioned for attendees that refers to the email sender name.
- Do not return empty result if no meeting details are found.

For tasks (only if task parsing is enabled):
- Identify all tasks and their respective owners from the email content.
- Always check for headings mext steps or followup steps. All the next steps, followup steps specified in email content are considered as tasks.
- If headings are not given check for following up phrases or action items in the email content. 
- Identify multiple tasks from one email by checking for bullet points, numbered lists, or separate paragraphs indicating distinct action items. Also check for conjunctions like "and" or "also" that may link multiple tasks in a single sentence.
- check the due date of the task from email content even if the tasks are given as conjunctions.
- If the user is mentioning task for himself for example: I'll send the documents, I'll review the proposal, I will get back to you, I will share the details, I will check and revert, I will look into it etc., assign the task to the email sender.
- For each task:
  - Match the task to the corresponding owner in the provided Task Owners list by task_index (1-based indexing).
  - If a specific task owner is mentioned in the email and matches an entry in the Task Owners list, use that owner's name and ID.
  - If a specific task owner is mentioned but does not match any entry in the Task Owners list, use the default task owner: {default_task_owner_name} (ID: {default_task_owner_id}).
  - If no task owner is specified, use the default task owner: {default_task_owner_name} (ID: {default_task_owner_id}).
  - If no due date is specified, use the date three business days from the current date.
  - For due date is today is mentioned, use the current date. Tomorrow is current date + 1 day, after 2 days is current date + 2 days and so on. day after tomorrow is current date + 2 days.
  - When a task due date is mentioned using a day of the week (for example: Monday, Tuesday, Wednesday), resolve the exact calendar date for that day relative to today, instead of applying any default offset.
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

    try:
        response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
        logging.info(f"Raw AI response for notes/tasks/meeting: {response[:1000]}...")
    except Exception as e:
        raise
    try:
        parsed_json = json.loads(response.strip())
        
        if not should_parse_notes:
            parsed_json["notes"] = []
        if not should_parse_tasks:
            parsed_json["tasks"] = []
        if not should_parse_meetings:
            parsed_json["meeting_details"] = {}
        
        if should_parse_tasks:
            for task in parsed_json.get("tasks", []):
                task_index = task.get("task_index", 0)
                matching_owner = next((owner for owner in task_owners if owner.get("task_index") == task_index), None)
                if matching_owner:
                    # Use the validated owner from determine_owner
                    task["task_owner_id"] = matching_owner.get("task_owner_id", default_task_owner_id)
                    task["task_owner_name"] = matching_owner.get("task_owner_name", default_task_owner_name)
                else:
                    # If no matching owner in the list, keep defaults
                    if "task_owner_id" not in task or not task["task_owner_id"]:
                        task["task_owner_id"] = default_task_owner_id
                    if "task_owner_name" not in task or not task["task_owner_name"]:
                        task["task_owner_name"] = default_task_owner_name
                
                # Ensure task_index is set
                if "task_index" not in task:
                    task["task_index"] = task_index or (parsed_json["tasks"].index(task) + 1)
        
        ti.xcom_push(key="notes_tasks_meeting", value=parsed_json)
        logging.info(f"Parsed {len(parsed_json.get('notes', []))} notes, {len(parsed_json.get('tasks', []))} tasks, meeting: {bool(parsed_json.get('meeting_details'))}")

    except Exception as e:
        logging.error(f"Error processing notes/tasks/meeting AI response: {e}")
        default = {
            "notes": [],
            "tasks": [],
            "meeting_details": {}
        }
        ti.xcom_push(key="notes_tasks_meeting", value=default)

def validate_entity_creation_rules(ti, **context):
    """
    Validate that entity creation follows HubSpot association rules.
    Returns validation result and detailed error messages.
    
    CRITICAL FIX: Check BOTH existing AND new entities when validating associations.
    """
    entity_flags = ti.xcom_pull(key="entity_search_flags", task_ids="analyze_thread_entities", default={})
    contact_info = ti.xcom_pull(key="contact_info_with_associations",task_ids = "validate_associations_against_context", default={})
    company_info = ti.xcom_pull(key="company_info", task_ids="validate_associations_against_context", default={})
    deal_info = ti.xcom_pull(key="deal_info",task_ids = "validate_associations_against_context", default={})
    notes_tasks_meeting = ti.xcom_pull(key="notes_tasks_meeting", task_ids = "parse_notes_tasks_meeting",default={})
    email_data = ti.xcom_pull(key="email_data", task_ids = "load_context_from_dag_run", default={})
    
    # Extract sender info
    headers = email_data.get("headers", {})
    sender_raw = headers.get("From", "")
    import email.utils
    sender_tuple = email.utils.parseaddr(sender_raw)
    sender_name = sender_tuple[0].strip() or "there"
    
    validation_errors = []
    
    # Get all existing and new entities
    # Note: contact_info_with_associations uses different structure
    contact_results = contact_info.get("contact_results", {})
    existing_contacts = contact_results.get("total", 0) if isinstance(contact_results, dict) else len(contact_results.get("results", []))
    new_contacts = len(contact_info.get("new_contacts", []))
    
    existing_companies = company_info.get("company_results", {}).get("total", 0)
    new_companies = len(company_info.get("new_companies", []))
    
    existing_deals = deal_info.get("deal_results", {}).get("total", 0)
    new_deals = len(deal_info.get("new_deals", []))
    
    # Get engagement entities
    notes = notes_tasks_meeting.get("notes", [])
    tasks = notes_tasks_meeting.get("tasks", [])
    meeting_details = notes_tasks_meeting.get("meeting_details", {})
    has_meeting = bool(meeting_details and any(str(v).strip() for v in meeting_details.values() if v is not None))
    
    # Calculate totals (existing + new)
    total_contacts = existing_contacts + new_contacts
    total_companies = existing_companies + new_companies
    total_deals = existing_deals + new_deals
    
    logging.info(f"=== VALIDATION COUNTS ===")
    logging.info(f"Contacts: {existing_contacts} existing + {new_contacts} new = {total_contacts} total")
    logging.info(f"Companies: {existing_companies} existing + {new_companies} new = {total_companies} total")
    logging.info(f"Deals: {existing_deals} existing + {new_deals} new = {total_deals} total")
    logging.info(f"Engagements: {len(notes)} notes, {len(tasks)} tasks, {1 if has_meeting else 0} meeting")
    
    # RULE 1: Meetings/Notes/Tasks must have at least one base entity (contact, deal, OR company)
    # This checks TOTAL entities (existing + new)
    has_base_entity = total_contacts > 0 or total_deals > 0 or total_companies > 0
    
    if notes and not has_base_entity:
        validation_errors.append({
            "entity_type": "Notes",
            "count": len(notes),
            "issue": "Notes cannot be created without at least one associated contact, deal, or company.",
            "suggestion": "Please specify a contact, company, or deal to associate with these notes. You can provide names, emails, or other identifying information."
        })
    
    if tasks and not has_base_entity:
        validation_errors.append({
            "entity_type": "Tasks",
            "count": len(tasks),
            "issue": "Tasks cannot be created without at least one associated contact, deal, or company.",
            "suggestion": "Please specify a contact, company, or deal to associate with these tasks. You can provide names, emails, or other identifying information."
        })
    
    if has_meeting and not has_base_entity:
        validation_errors.append({
            "entity_type": "Meeting",
            "count": 1,
            "issue": "Meetings cannot be created without at least one associated contact, deal, or company.",
            "suggestion": "Please specify a contact, company, or deal to associate with this meeting. You can provide names, emails, or other identifying information."
        })
    
    # RULE 2: NEW Deals should be associated with contacts (existing OR new)
    # Only validate if we're creating NEW deals AND have NO contacts at all
    if new_deals > 0 and total_contacts == 0:
        validation_errors.append({
            "entity_type": "Deals",
            "count": new_deals,
            "issue": f"Creating {new_deals} new deal(s) but no contacts are specified (existing or new).",
            "suggestion": "Please specify at least one contact to associate with the deal(s). You can:\n" +
                         "  • Mention an existing contact by name or email\n" +
                         "  • Include details for a new contact to be created\n" +
                         "  • The system found these contacts but they need to be mentioned in deal context: " +
                         (f"{existing_contacts} existing contact(s)" if existing_contacts > 0 else "none")
        })
    
    # RULE 3: NEW Contacts with company context should ideally have company association
    # This is informational only - we auto-link them, so it's not an error
    if new_contacts > 0 and (existing_companies > 0 or new_companies > 0):
        logging.info(f"✓ {new_contacts} new contact(s) will be associated with {existing_companies + new_companies} company/companies")
    
    # RULE 4: NEW Companies MUST be associated with contacts
    if new_companies > 0 and total_contacts == 0:
        validation_errors.append({
            "entity_type": "Companies",
            "count": new_companies,
            "issue": f"Creating {new_companies} new company/companies but no contacts are specified (existing or new).",
            "suggestion": "Please specify at least one contact to associate with the company/companies. You can:\n" +
                        "  • Mention a contact by name or email\n" +
                        "  • Include details for a new contact to be created\n" +
                        "  • Companies must be linked to contacts in HubSpot for proper tracking"
        })
    
    # Build validation result
    validation_result = {
        "is_valid": len(validation_errors) == 0,
        "errors": validation_errors,
        "entity_summary": {
            "contacts": {
                "existing": existing_contacts, 
                "new": new_contacts, 
                "total": total_contacts
            },
            "companies": {
                "existing": existing_companies, 
                "new": new_companies, 
                "total": total_companies
            },
            "deals": {
                "existing": existing_deals, 
                "new": new_deals, 
                "total": total_deals
            },
            "notes": len(notes),
            "tasks": len(tasks),
            "meetings": 1 if has_meeting else 0
        },
        "sender_name": sender_name
    }
    
    ti.xcom_push(key="validation_result", value=validation_result)
    
    if validation_result["is_valid"]:
        logging.info(f"✅ VALIDATION PASSED")
        logging.info(f"   - {total_contacts} contact(s) available for associations")
        logging.info(f"   - {total_companies} company/companies available for associations")
        logging.info(f"   - {total_deals} deal(s) will be processed")
        logging.info(f"   - {len(notes)} note(s), {len(tasks)} task(s), {1 if has_meeting else 0} meeting(s)")
    else:
        logging.warning(f"❌ VALIDATION FAILED with {len(validation_errors)} error(s):")
        for idx, error in enumerate(validation_errors, 1):
            logging.warning(f"   {idx}. {error['entity_type']}: {error['issue']}")
    
    return validation_result
    
def compose_validation_error_email(ti, **context):
    """
    Compose a polite email explaining why entities cannot be created.
    """
    validation_result = ti.xcom_pull(key="validation_result",task_ids = "validate_entity_creation_rules", default={})
    email_data = ti.xcom_pull(key="email_data", task_ids = "load_context_from_dag_run", default={})
    notes_tasks_meeting = ti.xcom_pull(key="notes_tasks_meeting", task_ids = "parse_notes_tasks_meeting", default={})
    
    # ✅ GET COMPANY AND DEAL DATA FROM CORRECT SOURCES
    company_info = ti.xcom_pull(key="company_info", task_ids="validate_companies_against_associations", default={})
    deal_info = ti.xcom_pull(key="deal_info",task_ids = "validate_deals_against_associations", default={})
    
    errors = validation_result.get("errors", [])
    entity_summary = validation_result.get("entity_summary", {})
    sender_name = validation_result.get("sender_name", "there")
    
    # Determine what the user is actually trying to create
    has_notes = len(notes_tasks_meeting.get("notes", [])) > 0
    has_tasks = len(notes_tasks_meeting.get("tasks", [])) > 0
    has_meeting = bool(notes_tasks_meeting.get("meeting_details", {}))

    has_company = len(company_info.get("new_companies", [])) > 0 
    has_deal = len(deal_info.get("new_deals", [])) > 0           
    # Collect all entity types the user wants to create
    requested_entities = []
    if has_notes:
        requested_entities.append("Note")
    if has_tasks:
        requested_entities.append("Task")
    if has_meeting:
        requested_entities.append("Meeting")
    if has_company:
        requested_entities.append("Company")
    if has_deal:
        requested_entities.append("Deal")
    
    # Determine primary entity from what was actually requested
    if len(requested_entities) == 1:
        primary_entity = requested_entities[0].capitalize()
    elif len(requested_entities) > 1:
        # ✅ IMPROVED: Prioritize based on validation errors
        primary_entity = None
        for error in errors:
            entity_type = error.get("entity_type", "")
            if entity_type == "Deals":
                primary_entity = "Deal"
                break
            elif entity_type == "Companies":
                primary_entity = "Company"
                break
            elif entity_type == "Meetings":
                primary_entity = "Meeting"
                break
            elif entity_type == "Notes":
                primary_entity = "Note"
                break
            elif entity_type == "Tasks":
                primary_entity = "Task"
                break
        
        # If still not found, use first requested entity
        if not primary_entity:
            primary_entity = requested_entities[0]
        else:
            # ✅ IMPROVED: Fallback based on errors
            primary_entity = None
            for error in errors:
                entity_type = error.get("entity_type", "")
                if entity_type in ["Tasks", "Meetings", "Notes", "Deals", "Companies"]:
                    primary_entity = entity_type.rstrip('s')
                    break
    
        primary_entity = "entities"
    else:
        # Fallback: check errors list
        primary_entity = None
        for error in errors:
            entity_type = error.get("entity_type", "")
            if entity_type in ["Tasks", "Meetings", "Notes", "Deals", "Companies"]:

                primary_entity = entity_type.rstrip('s')
                break
        
        # Default fallback if somehow not detected
        if not primary_entity:
            primary_entity = "Task"  # or raise/log an error
    
    # Build error details HTML (kept but not used in these specific templates)
    error_details_html = ""
    for idx, error in enumerate(errors, 1):
        entity_type = error.get("entity_type", "Entity")
        count = error.get("count", 0)
        issue = error.get("issue", "")
        suggestion = error.get("suggestion", "")
        
        error_details_html += f"""
        <div>
            <h4 style="margin-top: 0; color: #856404;">Issue {idx}: {entity_type} ({count} item{'s' if count > 1 else ''})</h4>
            <p style="margin: 10px 0;"><strong>Problem:</strong> {issue}</p>
            <p style="margin: 10px 0;"><strong>Solution:</strong> {suggestion}</p>
        </div>
        """

    # Compose full email
    if primary_entity == "Deal":
        email_html = f"""<!DOCTYPE html>
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
        .greeting {{ margin-bottom: 20px; }}
        .section-title {{
            color: #000;
            margin-top: 30px;
            margin-bottom: 15px;
            font-size: 18px;
        }}
        .closing {{margin-top: 30px; }}
        .signature {{
            margin-top: 20px;
            padding-top: 15px;
            border-top: 1px solid #ddd;
        }}
    </style>
</head>
<body>
    <p>Hello {sender_name},</p>
    <p>Thank you for your request to create a deal in HubSpot. I reviewed the request and noticed that the deal cannot be completed yet due to a missing required association.</p>
    
    <h4 class="section-title">What's needed</h4>
    <p><li>HubSpot requires every deal to be linked to a contact for proper follow-up and ownership.</li><br>
    Since no contact was specified, the deal could not be created.</p>
    
    <p><strong>Next steps</strong></p>
    <p>Please reply to this email with one of the following:</p>
    <ul>
        <li>The contact you would like this deal associated with (name, email, or other identifying details), or</li>
        <li>Confirmation to link the deal to an existing contact in your HubSpot account</li>
    </ul>
    
    <p>Once I have this information, I'll take care of the rest right away.</p>
    <p>If you have any questions, feel free to let me know.</p>
    
    <div class="signature">
        <p><strong>Best regards,</strong><br>
         The HubSpot Assistant Team<br>
         <a href="http://lowtouch.ai">lowtouch.ai</a></p>
    </div>
</body>
</html>"""
    elif primary_entity == "Company":
        email_html = f"""<!DOCTYPE html>
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
        .greeting {{ margin-bottom: 20px; }}
        .section-title {{
            color: #000;
            margin-top: 30px;
            margin-bottom: 15px;
            font-size: 18px;
        }}
        .closing {{margin-top: 30px; }}
        .signature {{
            margin-top: 20px;
            padding-top: 15px;
            border-top: 1px solid #ddd;
        }}
    </style>
</head>
<body>
    <p>Hello {sender_name},</p>
    <p>Thank you for your request to create a company in HubSpot. I reviewed the request and noticed that the company cannot be created due to a missing required association.</p>
    
    <h4 class="section-title">What's needed</h4>
    <p><li>HubSpot requires every company to be linked to a contact for proper follow-up and ownership.</li><br>
    Since no contact was specified, the company could not be created.</p>
    
    <p><strong>Next steps</strong></p>
    <p>Please reply to this email with one of the following:</p>
    <ul>
        <li>The contact you would like this company associated with (name, email, or other identifying details), or</li>
        <li>Confirmation to link the company to an existing contact in your HubSpot account</li>
    </ul>
    
    <p>Once I have this information, I'll take care of the rest right away.</p>
    <p>If you have any questions, feel free to let me know.</p>
    
    <div class="signature">
        <p><strong>Best regards,</strong><br>
         The HubSpot Assistant Team<br>
         <a href="http://lowtouch.ai">lowtouch.ai</a></p>
    </div>
</body>
</html>"""

    elif primary_entity == "Meeting":
        email_html = f"""<!DOCTYPE html>
<html>
<head>
    <style>
        body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; max-width: 700px; margin: 0 auto; padding: 20px; }}
        .section-title {{ color: #000; margin-top: 30px; margin-bottom: 15px; font-size: 18px; }}
        .signature {{ margin-top: 20px; padding-top: 15px; border-top: 1px solid #ddd; }}
    </style>
</head>
<body>
    <p>Hello {sender_name},</p>
    <p>Thank you for your request to create a meeting in HubSpot. I reviewed the request and noticed that the creation of meeting cannot be completed.</p>

    <h4 class="section-title">What's needed</h4>
    <p>HubSpot requires every meeting to be created only if it has the following details:
    <ul>
        <li>Attendees</li>
        <li>Mode of meeting(offline or online)</li>
        <li>Start time</li>
    </ul>
    Since none of the required details were specified, the meeting could not be created.</p>
    
    <p><strong>Next steps</strong></p>
    <p>Please reply to this email with all the details:</p>
    <ul>
        <li>Attendees,mode of meeting and start time of the meeting</li>
    </ul>

    <p>Once I have this information, I'll take care of the rest right away.</p>
    <p>If you have any questions, feel free to let me know.</p>
    
    <div class="signature">
        <p><strong>Best regards,</strong><br>
        The HubSpot Assistant Team<br>
        <a href="http://lowtouch.ai">lowtouch.ai</a></p>
    </div>
</body>
</html>"""

    elif primary_entity == "Note":
        email_html = f"""<!DOCTYPE html>
<html>
<head>
    <style>
        body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; max-width: 700px; margin: 0 auto; padding: 20px; }}
        .section-title {{ color: #000; margin-top: 30px; margin-bottom: 15px; font-size: 18px; }}
        .signature {{ margin-top: 20px; padding-top: 15px; border-top: 1px solid #ddd; }}
    </style>
</head>
<body>
    <p>Hello {sender_name},</p>
    <p>Thank you for your request to create a note in HubSpot. I reviewed the request and noticed that the note cannot be created due to a missing required association.</p>

    <h4 class="section-title">What's needed</h4>
    <p>HubSpot requires every note to be linked to at least one of the following:
    <ul>
        <li>A contact</li>
        <li>A company</li>
        <li>A deal</li>
    </ul>
    Since no association was specified, the note could not be created.</p>
    
    <p><strong>Next steps</strong></p>
    <p>Please reply to this email with one of the following:</p>
    <ul>
        <li>The contact, company, or deal you would like this note associated with, or</li>
        <li>Confirmation to link the note to an existing contact, company, or deal in your HubSpot account</li>
    </ul>

    <p>Once I have this information, I'll take care of the rest right away.</p>
    <p>If you have any questions or would like guidance on the best association to use, feel free to let me know.</p>
    
    <div class="signature">
        <p><strong>Best regards,</strong><br>
         The HubSpot Assistant Team<br>
         <a href="http://lowtouch.ai">lowtouch.ai</a></p>
    </div>
</body>
</html>"""
        
    else:
        # Default (Task)
        email_html = f"""<!DOCTYPE html>
<html>
<head>
    <style>
        body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; max-width: 700px; margin: 0 auto; padding: 20px; }}
        .section-title {{ color: #000; margin-top: 30px; margin-bottom: 15px; font-size: 18px; }}
        .signature {{ margin-top: 20px; padding-top: 15px; border-top: 1px solid #ddd; }}
    </style>
</head>
<body>
    <p>Hello {sender_name},</p>
    <p>Thank you for your request to create a task in HubSpot. I reviewed the request and noticed that the task cannot be completed due to a missing required association.</p>

    <h4 class="section-title">What's needed</h4>
    <p>HubSpot requires every task to be linked to at least one of the following:
    <ul>
        <li>A contact</li>
        <li>A company</li>
        <li>A deal</li>
    </ul>
    Since no association was specified, the task could not be created.</p>
    
    <p><strong>Next steps</strong></p>
    <p>Please reply to this email with one of the following:</p>
    <ul>
        <li>The contact, company, or deal you would like this task associated with, or</li>
        <li>Confirmation to link the task to an existing contact, company, or deal in your HubSpot account</li>
    </ul>

    <p>Once I have this information, I'll take care of the rest right away.</p>
    <p>If you have any questions or would like guidance on the best association to use, feel free to let me know.</p>
    
    <div class="signature">
        <p><strong>Best regards,</strong><br>
        The HubSpot Assistant Team<br>
        <a href="http://lowtouch.ai">lowtouch.ai</a></p>
    </div>
</body>
</html>"""
    
    ti.xcom_push(key="validation_error_email", value=email_html)
    logging.info(f"Composed validation error email for primary entity: {primary_entity}")
    
    return email_html


def send_validation_error_email(ti, **context):
    """
    Send the validation error email to the user with proper threading.
    """
    email_data = ti.xcom_pull(key="email_data",  task_ids = "load_context_from_dag_run", default={})
    validation_error_email = ti.xcom_pull(key="validation_error_email", task_ids = "compose_validation_error_email")
    
    service = authenticate_gmail()
    if not service:
        logging.error("Gmail authentication failed")
        return None
    
    # Extract all recipients from original email
    all_recipients = extract_all_recipients(email_data)
    
    headers = email_data.get("headers", {})
    sender_email = headers.get("From", "")
    original_message_id = headers.get("Message-ID", "")
    references = headers.get("References", "")
    
    if original_message_id and original_message_id not in references:
        references = f"{references} {original_message_id}".strip()
    
    subject = f"Re: {headers.get('Subject', 'HubSpot Request')}"
    if not subject.lower().startswith("re:"):
        subject = f"Re: {subject}"
    
    # Prepare recipients for reply-all
    primary_recipient = sender_email
    cc_recipients = []
    
    for to_addr in all_recipients["to"]:
        if (to_addr.lower() != sender_email.lower() and 
            HUBSPOT_FROM_ADDRESS.lower() not in to_addr.lower()):
            cc_recipients.append(to_addr)
    
    for cc_addr in all_recipients["cc"]:
        if (HUBSPOT_FROM_ADDRESS.lower() not in cc_addr.lower() and 
            cc_addr not in cc_recipients):
            cc_recipients.append(cc_addr)
    
    bcc_recipients = [addr for addr in all_recipients["bcc"] 
                      if HUBSPOT_FROM_ADDRESS.lower() not in addr.lower()]
    
    cc_string = ', '.join(cc_recipients) if cc_recipients else None
    bcc_string = ', '.join(bcc_recipients) if bcc_recipients else None
    
    logging.info(f"Sending validation error email:")
    logging.info(f"Primary recipient: {primary_recipient}")
    logging.info(f"Cc recipients: {cc_string}")
    logging.info(f"Bcc recipients: {bcc_string}")
    
    result = send_email(service, primary_recipient, subject, validation_error_email,
                       original_message_id, references, cc=cc_string, bcc=bcc_string)
    
    if result:
        logging.info(f"Validation error email sent successfully")
        
        # Mark original message as read
        try:
            original_msg_id = email_data.get("id")
            if original_msg_id:
                service.users().messages().modify(
                    userId="me",
                    id=original_msg_id,
                    body={"removeLabelIds": ["UNREAD"]}
                ).execute()
                logging.info(f"Marked message {original_msg_id} as read")
        except Exception as read_err:
            logging.warning(f"Failed to mark message as read: {read_err}")
        
        ti.xcom_push(key="validation_error_email_sent", value=True)
    else:
        logging.error("Failed to send validation error email")
    
    return result

def decide_validation_path(ti, **context):
    """
    Branch task to decide whether to proceed with entity creation or send validation error.
    """
    validation_result = ti.xcom_pull(key="validation_result", task_ids = "validate_entity_creation_rules", default={})
    
    is_valid = validation_result.get("is_valid", False)
    
    if is_valid:
        logging.info(" Validation passed - proceeding with entity creation workflow")
        return "compile_search_results"
    else:
        logging.warning(" Validation failed - sending error email to user")
        return "compose_validation_error_email"

def check_task_threshold(ti, **context):
    """Check if task volume exceeds threshold"""
    entity_flags = ti.xcom_pull(key="entity_search_flags", task_ids="analyze_thread_entities", default={})
    if not entity_flags.get("parse_tasks", True):
        logging.info(f"Skipping task threshold check")
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
    
    chat_history = ti.xcom_pull(key="chat_history", task_ids = "load_context_from_dag_run", default=[])
    latest_message = ti.xcom_pull(key="latest_message", task_ids = "load_context_from_dag_run", default="")
    
    # Get CORRECTED tasks from parse_notes_tasks_meeting
    notes_tasks_meeting = ti.xcom_pull(key="notes_tasks_meeting", task_ids = "parse_notes_tasks_meeting", default={})
    corrected_tasks = notes_tasks_meeting.get('tasks', [])
    
    # Build task owner mapping from CORRECTED tasks
    task_owner_mapping = []
    for task in corrected_tasks:
        task_owner_mapping.append({
            'task_details': task.get('task_details', ''),
            'due_date': task.get('due_date', ''),
            'task_owner_id': task.get('task_owner_id', '71346067'),
            'task_owner_name': task.get('task_owner_name', 'Kishore')
        })


    prompt = f"""You are a HubSpot API assistant. Check task volume thresholds.
    You cannot create or modify tasks.

    YOU ARE A JSON-ONLY API. 
    DO NOT WRITE ANY TEXT, EXPLANATION, OR NARRATIVE.
    DO NOT USE <think> TAGS.
    DO NOT SAY "invoking" OR "successful".
    IMMEDIATELY OUTPUT THE RAW JSON AND NOTHING ELSE.

LATEST USER MESSAGE:
{latest_message}

Task Owner Mapping:
{json.dumps(task_owner_mapping, indent=2)}

IMPORTANT: You must respond with ONLY a valid JSON object. No HTML, no explanations, no markdown formatting. You can only invoke search_tasks tool to check task threshold. You cannot invoke any other tools.

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
    try:
        response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
        logging.info(f"Raw AI response for task threshold: {response[:1000]}...")
    except Exception as e:
        raise
    try:
        parsed_json = json.loads(response.strip())
        warnings = parsed_json.get("warnings", [])
        
        ti.xcom_push(key="task_warnings", value=warnings)
        ti.xcom_push(key="task_threshold_info", value=parsed_json)
        
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

    return warnings

def compile_search_results(ti, **context):
    """Compile all search results for confirmation email"""
    owner_info = ti.xcom_pull(key="owner_info", task_ids = "determine_owner", default={})
    deal_info = ti.xcom_pull(key="merged_deal_info",task_ids ="validate_associations_against_context") or ti.xcom_pull(key="deal_info",task_ids = "validate_deals_against_associations") or {}
    contact_info = ti.xcom_pull(key="contact_info_with_associations",task_ids = "refine_contacts_by_associations", default={})
    company_info = ti.xcom_pull(key="merged_company_info",task_ids ="validate_associations_against_context") or ti.xcom_pull(key="company_info",task_ids = "validate_companies_against_associations") or {}
    notes_tasks_meeting = ti.xcom_pull(key="notes_tasks_meeting", task_ids="parse_notes_tasks_meeting", default={})
    task_threshold_info = ti.xcom_pull(key="task_threshold_info",task_ids ="check_task_threshold", default={})
    deal_stage_info = ti.xcom_pull(key="deal_stage_info", task_ids = "validate_deal_stage", default={"deal_stages": []})
    thread_id = ti.xcom_pull(key="thread_id", task_ids="load_context_from_dag_run")
    email_data = ti.xcom_pull(key="email_data", task_ids="load_context_from_dag_run", default={})
    
    logging.info(f"=== COMPILING SEARCH RESULTS ===")
    logging.info(f"Thread ID: {thread_id}")
    
    # ✅ FIX: Apply validated stages to new deals BEFORE adding to search_results
    new_deals = deal_info.get("new_deals", [])
    validated_stages = {stage['deal_name']: stage['validated_stage'] 
                       for stage in deal_stage_info.get('deal_stages', [])}
    
    for deal in new_deals:
        deal_name = deal.get("dealName", "")
        if deal_name in validated_stages:
            # Apply the validated stage (either user's valid input or default "Lead")
            deal["dealLabelName"] = validated_stages[deal_name]
            logging.info(f"Applied validated stage '{validated_stages[deal_name]}' to deal '{deal_name}'")
    
    search_results = {
        "thread_id": thread_id,
        "deal_results": deal_info.get("deal_results", {"total": 0, "results": []}),
        "contact_results": contact_info.get("contact_results", {"total": 0, "results": []}),
        "company_results": company_info.get("company_results", {"total": 0, "results": []}),
        "new_entity_details": {
            "deals": new_deals,  # Now contains corrected stages
            "contacts": contact_info.get("new_contacts", []),
            "companies": company_info.get("new_companies", []),
            "notes": notes_tasks_meeting.get("notes", []),
            "tasks": notes_tasks_meeting.get("tasks", []),
            "meeting_details": notes_tasks_meeting.get("meeting_details", {})
        },
        "contact_owner_id": owner_info.get("contact_owner_id", "71346067"),
        "contact_owner_name": owner_info.get("contact_owner_name", "Kishore"),
        "contact_owner_message": owner_info.get("contact_owner_message", ""),
        "deal_owner_id": owner_info.get("deal_owner_id", "71346067"),
        "deal_owner_name": owner_info.get("deal_owner_name", "Kishore"),
        "deal_owner_message": owner_info.get("deal_owner_message", ""),
        "task_owners": owner_info.get("task_owners", []),
        "all_owners_table": owner_info.get("all_owners_table", []),
        "partner_status": company_info.get("partner_status", None),
        "task_threshold_info": task_threshold_info,
        "search_timestamp": datetime.now().isoformat()
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

    ti.xcom_push(key="search_results", value=search_results)
    ti.xcom_push(key="confirmation_needed", value=confirmation_needed)

    logging.info(f"✅ Compiled search results for thread {thread_id}")
    logging.info(f"   - Confirmation needed: {confirmation_needed}")
    logging.info(f"   - Existing entities: {has_existing_entities}")
    logging.info(f"   - New entities: {has_new_entities}")
    
    return search_results

def compose_confirmation_email(ti, **context):
    """Compose confirmation email with search results"""
    search_results = ti.xcom_pull(key="search_results",task_ids = "compile_search_results")
    email_data = ti.xcom_pull(key="email_data",task_ids = "load_context_from_dag_run")
    confirmation_needed = ti.xcom_pull(key="confirmation_needed", task_ids="compile_search_results", default=False)
    engagement_summary = ti.xcom_pull(key="engagement_summary", task_ids = "summarize_engagement_details", default={})
    notes_tasks_meeting = ti.xcom_pull(key="notes_tasks_meeting",  task_ids = "parse_notes_tasks_meeting", default={})
    deal_stage_info = ti.xcom_pull(key="deal_stage_info", task_ids="validate_deal_stage", default={"deal_stages": []})  # This is already correct
    corrected_tasks = notes_tasks_meeting.get("tasks", [])
    
    if not confirmation_needed:
        logging.info("No confirmation needed")
        return "No confirmation needed"

    # Valid deal stages
    VALID_DEAL_STAGES = [
        "Lead", "Qualified Lead", "Solution Discussion", "Proposal",
        "Negotiations", "Contracting", "Closed Won", "Closed Lost", "Inactive"
    ]

    def has_meaningful_data(entity, required_fields):
        if not entity or not isinstance(entity, dict):
            return False
        return any(entity.get(field, "").strip() for field in required_fields)

    def filter_meaningful_entities(entities, required_fields):
        if not entities:
            return []
        return [entity for entity in entities if has_meaningful_data(entity, required_fields)]

    from_email = email_data["headers"].get("From", "")
    
    email_content = f"""<!DOCTYPE html>
<html>
<head>
    <style>
        table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
        th {{ background-color: #f2f2f2; font-weight: bold; }}
        h3 {{ color: #333; margin-top: 30px; margin-bottom: 15px; }}
        .greeting {{ margin-bottom: 20px; }}
        .closing {{ margin-top: 30px; }}
    </style>
</head>
<body>
    <div class="greeting">
        <p>Hello {from_email},</p>
        <p>I reviewed your request and prepared the following summary:</p>
    </div>
"""

    has_content_sections = False

    # Existing Contacts
    contact_results = search_results.get("contact_results", {})
    if contact_results.get("total", 0) > 0:
        has_content_sections = True
        email_content += """
        <h3>Existing Contact Details</h3>
        <table>
            <thead>
                <tr>
                    <th>ID</th>
                    <th>Firstname</th>
                    <th>Lastname</th>
                    <th>Email</th>
                    <th>Phone</th>
                    <th>Address</th>
                    <th>Job Title</th>
                    <th>Owner</th>
                </tr>
            </thead>
            <tbody>
        """
        for contact in contact_results.get("results", []):
            email_content += f"""
                <tr>
                    <td>{contact.get("contactId", "")}</td>
                    <td>{contact.get("firstname", "")}</td>
                    <td>{contact.get("lastname", "")}</td>
                    <td>{contact.get("email", "")}</td>
                    <td>{contact.get("phone", "")}</td>
                    <td>{contact.get("address", "")}</td>
                    <td>{contact.get("jobtitle", "")}</td>
                    <td>{contact.get("contactOwnerName", "")}</td>
                </tr>
            """
        email_content += "</tbody></table><hr>"

    # Existing Companies
    company_results = search_results.get("company_results", {})
    if company_results.get("total", 0) > 0:
        has_content_sections = True
        email_content += """
        <h3>Existing Company Details</h3>
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
        for company in company_results.get("results", []):
            email_content += f"""
                <tr>
                    <td>{company.get("companyId", "")}</td>
                    <td>{company.get("name", "")}</td>
                    <td>{company.get("domain", "")}</td>
                    <td>{company.get("address", "")}</td>
                    <td>{company.get("city", "")}</td>
                    <td>{company.get("state", "")}</td>
                    <td>{company.get("zip", "")}</td>
                    <td>{company.get("country", "")}</td>
                    <td>{company.get("phone", "")}</td>
                    <td>{company.get("description", "")}</td>
                    <td>{company.get("type", "")}</td>
                </tr>
            """
        email_content += "</tbody></table><hr>"

    # Existing Deals
    deal_results = search_results.get("deal_results", {})
    if deal_results.get("total", 0) > 0:
        has_content_sections = True
        email_content += """
        <h3>Existing Deal Details</h3>
        <table>
            <thead>
                <tr>
                    <th>ID</th>
                    <th>Deal Name</th>
                    <th>Stage</th>
                    <th>Amount</th>
                    <th>Close Date</th>
                    <th>Owner</th>
                </tr>
            </thead>
            <tbody>
        """
        for deal in deal_results.get("results", []):
            email_content += f"""
                <tr>
                    <td>{deal.get("dealId", "")}</td>
                    <td>{deal.get("dealName", "")}</td>
                    <td>{deal.get("dealLabelName", "")}</td>
                    <td>{deal.get("dealAmount", "")}</td>
                    <td>{deal.get("closeDate", "")}</td>
                    <td>{deal.get("dealOwnerName", "")}</td>
                </tr>
            """
        email_content += "</tbody></table><hr>"

    # New Entities Section
    raw_new_contacts = search_results.get("new_entity_details", {}).get("contacts", [])
    raw_new_companies = search_results.get("new_entity_details", {}).get("companies", [])
    raw_new_deals = search_results.get("new_entity_details", {}).get("deals", [])
    notes = search_results.get("new_entity_details", {}).get("notes", [])
    tasks = search_results.get("new_entity_details", {}).get("tasks", [])
    meeting_details = search_results.get("new_entity_details", {}).get("meeting_details", {})

    new_contacts = filter_meaningful_entities(raw_new_contacts, ["firstname", "lastname", "email"])
    new_companies = filter_meaningful_entities(raw_new_companies, ["name", "domain"])
    new_deals = filter_meaningful_entities(raw_new_deals, ["dealName", "dealAmount"])
    
    meaningful_notes = [note for note in notes if note.get("note_content", "").strip()]
    meaningful_tasks = [task for task in corrected_tasks if task.get("task_details", "").strip()]
    meaningful_meeting = bool(meeting_details and any(str(v).strip() for v in meeting_details.values() if v is not None))

    has_new_objects = bool(new_contacts or new_companies or new_deals or meaningful_notes or meaningful_tasks or meaningful_meeting)

    if has_new_objects:
        has_content_sections = True
        email_content += "<h3>Objects to be Created</h3>"

        if new_contacts:
            email_content += """
            <h4>New Contacts</h4>
            <table>
                <thead>
                    <tr>
                        <th>Firstname</th>
                        <th>Lastname</th>
                        <th>Email</th>
                        <th>Phone</th>
                        <th>Address</th>
                        <th>Job Title</th>
                        <th>Owner</th>
                    </tr>
                </thead>
                <tbody>
            """
            for contact in new_contacts:
                email_content += f"""
                    <tr>
                        <td>{contact.get("firstname", "")}</td>
                        <td>{contact.get("lastname", "")}</td>
                        <td>{contact.get("email", "")}</td>
                        <td>{contact.get("phone", "")}</td>
                        <td>{contact.get("address", "")}</td>
                        <td>{contact.get("jobtitle", "")}</td>
                        <td>{contact.get("contactOwnerName", "")}</td>
                    </tr>
                """
            email_content += "</tbody></table>"

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
                email_content += f"""
                    <tr>
                        <td>{company.get("name", "")}</td>
                        <td>{company.get("domain", "")}</td>
                        <td>{company.get("address", "")}</td>
                        <td>{company.get("city", "")}</td>
                        <td>{company.get("state", "")}</td>
                        <td>{company.get("zip", "")}</td>
                        <td>{company.get("country", "")}</td>
                        <td>{company.get("phone", "")}</td>
                        <td>{company.get("description", "")}</td>
                        <td>{company.get("type", "")}</td>
                    </tr>
                """
            email_content += "</tbody></table>"

        if new_deals:
            email_content += """
            <h4>New Deals</h4>
            <table>
                <thead>
                    <tr>
                        <th>Deal Name</th>
                        <th>Stage</th>
                        <th>Amount</th>
                        <th>Close Date</th>
                        <th>Owner</th>
                    </tr>
                </thead>
                <tbody>
            """
            for deal in new_deals:
                email_content += f"""
                    <tr>
                        <td>{deal.get("dealName", "")}</td>
                        <td>{deal.get("dealLabelName", "")}</td>
                        <td>{deal.get("dealAmount", "")}</td>
                        <td>{deal.get("closeDate", "")}</td>
                        <td>{deal.get("dealOwnerName", "")}</td>
                    </tr>
                """
            email_content += "</tbody></table>"

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
                email_content += f"""
                    <tr>
                        <td>{note.get("note_content", "")}</td>
                        <td>{note.get("timestamp", "")}</td>
                    </tr>
                """
            email_content += "</tbody></table>"

        if meaningful_tasks:
            email_content += """
            <h4>Tasks</h4>
            <table>
                <thead>
                    <tr>
                        <th>Task Details</th>
                        <th>Owner</th>
                        <th>Due Date</th>
                        <th>Priority</th>
                    </tr>
                </thead>
                <tbody>
            """
            for task in meaningful_tasks:
                email_content += f"""
                    <tr>
                        <td>{task.get("task_details", "")}</td>
                        <td>{task.get("task_owner_name", "")}</td>
                        <td>{task.get("due_date", "")}</td>
                        <td>{task.get("priority", "")}</td>
                    </tr>
                """
            email_content += "</tbody></table>"

        if meaningful_meeting:
            email_content += """
            <h4>Meeting Details</h4>
            <table>
                <thead>
                    <tr>
                        <th>Title</th>
                        <th>Start Time</th>
                        <th>End Time</th>
                        <th>Location</th>
                        <th>Outcome</th>
                        <th>Attendees</th>
                    </tr>
                </thead>
                <tbody>
            """
            attendees = ", ".join(meeting_details.get("attendees", []))
            email_content += f"""
                <tr>
                    <td>{meeting_details.get("meeting_title", "")}</td>
                    <td>{meeting_details.get("start_time", "")}</td>
                    <td>{meeting_details.get("end_time", "")}</td>
                    <td>{meeting_details.get("location", "")}</td>
                    <td>{meeting_details.get("outcome", "")}</td>
                    <td>{attendees}</td>
                </tr>
            """
            email_content += "</tbody></table>"

        email_content += "<hr>"

    # Task Volume Analysis
    task_threshold_info = search_results.get("task_threshold_info", {})
    dates_checked = task_threshold_info.get("task_threshold_results", {}).get("dates_checked", [])

    if meaningful_tasks and dates_checked:
        has_content_sections = True
        email_content += """
        <h3>Task Volume Analysis</h3>
        <table>
            <thead>
                <tr>
                    <th>Date</th>
                    <th>Owner</th>
                    <th>Existing Tasks</th>
                    <th>Status</th>
                    <th>Warning</th>
                </tr>
            </thead>
            <tbody>
        """
        for date_info in dates_checked:
            date_str = date_info.get("date", "")
            owner_name = date_info.get("owner_name", "Kishore")
            
            exceeds = "Exceeds" if date_info.get("exceeds_threshold") else "Within Limit"
            email_content += f"""
                <tr>
                    <td>{date_str}</td>
                    <td>{owner_name}</td>
                    <td>{date_info.get("existing_task_count", 0)}</td>
                    <td>{exceeds}</td>
                    <td>{date_info.get("warning", "None")}</td>
                </tr>
            """
        email_content += """
            </tbody>
        </table>
        <p><em>Note: High task volumes may impact workflow performance.</em></p>
        <hr>
        """

    # # Engagement Summary
    # if engagement_summary and "error" not in engagement_summary:
    #     has_content_sections = True
    #     contact_summary = engagement_summary.get('contact_summary', {})
    #     deal_summary = engagement_summary.get('deal_summary', {})
    #     company_summary = engagement_summary.get('company_summary', {})
        
    #     email_content += f"""
    #     <h3>Engagement Summary</h3>
    #     <h4>Contact Summary</h4>
    #     <table>
    #         <thead>
    #             <tr>
    #                 <th>Name</th>
    #                 <th>Email</th>
    #                 <th>Company</th>
    #             </tr>
    #         </thead>
    #         <tbody>
    #             <tr>
    #                 <td>{contact_summary.get('contact_name', '')}</td>
    #                 <td>{contact_summary.get('email', '')}</td>
    #                 <td>{contact_summary.get('company_name', '')}</td>
    #             </tr>
    #         </tbody>
    #     </table>
        
    #     <h4>Deal Summary</h4>
    #     <table>
    #         <thead>
    #             <tr>
    #                 <th>Name</th>
    #                 <th>Stage</th>
    #                 <th>Amount</th>
    #                 <th>Close Date</th>
    #             </tr>
    #         </thead>
    #         <tbody>
    #             <tr>
    #                 <td>{deal_summary.get('deal_name', '')}</td>
    #                 <td>{deal_summary.get('stage', '')}</td>
    #                 <td>{deal_summary.get('amount', '')}</td>
    #                 <td>{deal_summary.get('close_date', '')}</td>
    #             </tr>
    #         </tbody>
    #     </table>
        
    #     <h4>Company Summary</h4>
    #     <table>
    #         <thead>
    #             <tr>
    #                 <th>Name</th>
    #                 <th>Domain</th>
    #             </tr>
    #         </thead>
    #         <tbody>
    #             <tr>
    #                 <td>{company_summary.get('company_name', '')}</td>
    #                 <td>{company_summary.get('domain', '')}</td>
    #             </tr>
    #         </tbody>
    #     </table>
        
    #     <h4>Engagement Details</h4>
    #     <p>{engagement_summary.get('engagement_summary', '')}</p>
        
    #     <h4>Deal Analysis</h4>
    #     <p>{engagement_summary.get('detailed_deal_summary', '')}</p>
        
    #     <h4>Call Strategy</h4>
    #     <p>{engagement_summary.get('call_strategy', '')}</p>
    #     <hr>
    #     """

    # Owner Assignment Section
    task_owners = search_results.get("task_owners", [])
    all_owners = search_results.get("all_owners_table", [])
    chosen_deal_owner_name = search_results.get("deal_owner_name", "Kishore")
    chosen_deal_owner_id = search_results.get("deal_owner_id", "71346067")
    deal_owner_msg = search_results.get("deal_owner_message", "")
    contact_owner_id = search_results.get("contact_owner_id", "71346067")
    contact_owner_name = search_results.get("contact_owner_name", "Kishore")
    contact_owner_msg = search_results.get("contact_owner_message", "")

    has_new_deals_or_tasks_or_contacts = (
        len(new_deals) > 0 or
        len(meaningful_tasks) > 0 or
        len(new_contacts) > 0
    )
    
    assignment_html = []

    # === Contact Owner Warnings ===
    if len(new_contacts) > 0:
        contact_msg_lower = contact_owner_msg.lower()
        if "no contact owner specified" in contact_msg_lower:
            assignment_html.append(f"""
                <h4 style='color: #2c5aa0; margin-bottom: 5px;'>Contact Owner Assignment:</h4>
                <p style='background-color: #d1ecf1; padding: 10px; border-left: 4px solid #17a2b8; margin: 0 0 15px 0;'>
                    <strong>Reason:</strong> Contact owner not specified.<br>
                    <strong>Action:</strong> Assigning to default owner '{contact_owner_name}'.
                </p>
            """)
        elif "not valid" in contact_msg_lower:
            assignment_html.append(f"""
                <h4 style='color: #2c5aa0; margin-bottom: 5px;'>Contact Owner Assignment:</h4>
                <p style='background-color: #f8d7da; padding: 10px; border-left: 4px solid #dc3545; margin: 0 0 15px 0;'>
                    <strong>Reason:</strong> Contact owner not found in available owners.<br>
                    <strong>Action:</strong> Assigning to default owner '{contact_owner_name}'.
                </p>
            """)

    # === Deal Owner Warnings ===
    if len(new_deals) > 0:
        deal_msg_lower = deal_owner_msg.lower()
        if "no deal owner specified" in deal_msg_lower:
            assignment_html.append(f"""
                <h4 style='color: #2c5aa0; margin-bottom: 5px;'>Deal Owner Assignment:</h4>
                <p style='background-color: #d1ecf1; padding: 10px; border-left: 4px solid #17a2b8; margin: 0 0 15px 0;'>
                    <strong>Reason:</strong> Deal owner not specified.<br>
                    <strong>Action:</strong> Assigning to default owner '{chosen_deal_owner_name}'.
                </p>
            """)
        elif "not valid" in deal_msg_lower:
            assignment_html.append(f"""
                <h4 style='color: #2c5aa0; margin-bottom: 5px;'>Deal Owner Assignment:</h4>
                <p style='background-color: #f8d7da; padding: 10px; border-left: 4px solid #dc3545; margin: 0 0 15px 0;'>
                    <strong>Reason:</strong> Deal owner not found in available owners.<br>
                    <strong>Action:</strong> Assigning to default owner '{chosen_deal_owner_name}'.
                </p>
            """)

    # === Task Owner Warnings ===
    if len(meaningful_tasks) > 0:
        task_warning_blocks = []
        for task in corrected_tasks:
            task_index = task.get("task_index", 0)
            task_details = task.get("task_details", "Unknown")
            task_owner_name = task.get("task_owner_name", "Kishore")
            original_task_owner = next((to for to in task_owners if to.get("task_index") == task_index), None)
            task_owner_msg = original_task_owner.get("task_owner_message", "") if original_task_owner else ""
            task_msg_lower = task_owner_msg.lower()

            if "no task owner specified" in task_msg_lower:
                task_warning_blocks.append(f"""
                    <p style='background-color: #d1ecf1; padding: 10px; border-left: 4px solid #17a2b8; margin: 10px 0 20px 0;'>
                        <strong>Task {task_index}:</strong> {task_details}<br>
                        <strong>Reason:</strong> Task owner not specified.<br>
                        <strong>Action:</strong> Assigning to default owner '{task_owner_name}'.
                    </p>
                """)
            elif "not valid" in task_msg_lower:
                task_warning_blocks.append(f"""
                    <p style='background-color: #f8d7da; padding: 10px; border-left: 4px solid #dc3545; margin: 10px 0 20px 0;'>
                        <strong>Task {task_index}:</strong> {task_details}<br>
                        <strong>Reason:</strong> Task owner not found.<br>
                        <strong>Action:</strong> Assigning to default owner '{task_owner_name}'.
                    </p>
                """)

        if task_warning_blocks:  # Only add heading if there are actual warnings
            assignment_html.append("<h4 style='color: #2c5aa0; margin-bottom: 5px;'>Task Owner Assignment:</h4>")
            assignment_html.extend(task_warning_blocks)

    # === Render the entire section ONLY if there is at least one warning ===
    if assignment_html:
        has_content_sections = True
        email_content += "<div style='margin-bottom: 15px;'>"
        email_content += "<h3>Owner Assignment Details</h3>"
        email_content += "".join(assignment_html)
        email_content += "</div>"

    # Deal Stage Section
    stage_html = []
    has_stage_issues = False
    
    # FIXED: Only process stage validation for NEW deals
    if len(new_deals) > 0:
        for stage_info in deal_stage_info.get('deal_stages', []):
            deal_name = stage_info.get('deal_name', 'Unknown Deal')
            original_stage = stage_info.get('original_stage', '')
            validated_stage = stage_info.get('validated_stage', 'Lead')
            
            if not original_stage or original_stage.strip() == '':
                has_stage_issues = True
                stage_html.append(f"""
                    <h4 style='color: #2c5aa0; margin-bottom: 5px;'>Deal Stage for "{deal_name}":</h4>
                    <p style='background-color: #d1ecf1; padding: 10px; border-left: 4px solid #17a2b8; margin: 0;'>
                        <strong>Reason:</strong> Deal stage not specified.
                        <br><strong>Action:</strong> Assigning to default stage 'Lead'.
                    </p>
                """)
            elif validated_stage == 'Lead' and original_stage.lower() != 'lead':
                has_stage_issues = True
                stage_html.append(f"""
                    <h4 style='color: #2c5aa0; margin-bottom: 5px;'>Deal Stage for "{deal_name}":</h4>
                    <p style='background-color: #f8d7da; padding: 10px; border-left: 4px solid #dc3545; margin: 0;'>
                        <strong>Reason:</strong> Invalid deal stage '{original_stage}' specified.
                        <br><strong>Valid Stages:</strong> {', '.join(VALID_DEAL_STAGES)}
                        <br><strong>Action:</strong> Assigning to default stage 'Lead'.
                    </p>
                """)

    if has_stage_issues:
        email_content += "<div style='margin-bottom: 15px;'>"
        email_content += "<h3>Deal Stage Assignment Details</h3>"
        email_content += ''.join(stage_html)
        email_content += "</div>"

    email_content += """
        <div class="closing">
            <p>Please confirm whether this summary looks correct before I proceed.</p>
            <p><strong>Best regards,</strong><br>The HubSpot Assistant Team<br><a href="http://lowtouch.ai">lowtouch.ai</a></p>
        </div>
    </body>
    </html>
    """

    ti.xcom_push(key="confirmation_email", value=email_content)
    logging.info(f"Confirmation email composed")
    return email_content

def send_confirmation_email(ti, **context):
    """Send confirmation email to all recipients"""
    email_data = ti.xcom_pull(key="email_data", task_ids = "load_context_from_dag_run")
    confirmation_email = ti.xcom_pull(key="confirmation_email", task_ids = "compose_confirmation_email")
    confirmation_needed = ti.xcom_pull(key="confirmation_needed", task_ids="compile_search_results", default=False)
    
    if not confirmation_needed:
        return "No confirmation email needed"

    service = authenticate_gmail()
    if not service:
        logging.error("Gmail authentication failed")
        return "Gmail authentication failed"

    # Extract all recipients from original email
    all_recipients = extract_all_recipients(email_data)
    
    sender_email = email_data["headers"].get("From", "")
    original_message_id = email_data["headers"].get("Message-ID", "")
    references = email_data["headers"].get("References", "")

    if original_message_id and original_message_id not in references:
        references = f"{references} {original_message_id}".strip()

    subject = f"Re: {email_data['headers'].get('Subject', 'Meeting Minutes Request')}"

    # Prepare recipients for reply-all
    primary_recipient = sender_email
    cc_recipients = []
    
    for to_addr in all_recipients["to"]:
        if (to_addr.lower() != sender_email.lower() and 
            HUBSPOT_FROM_ADDRESS.lower() not in to_addr.lower()):
            cc_recipients.append(to_addr)
    
    for cc_addr in all_recipients["cc"]:
        if (HUBSPOT_FROM_ADDRESS.lower() not in cc_addr.lower() and 
            cc_addr not in cc_recipients):
            cc_recipients.append(cc_addr)
    
    bcc_recipients = [addr for addr in all_recipients["bcc"] 
                      if HUBSPOT_FROM_ADDRESS.lower() not in addr.lower()]

    cc_string = ', '.join(cc_recipients) if cc_recipients else None
    bcc_string = ', '.join(bcc_recipients) if bcc_recipients else None

    logging.info(f"Sending confirmation email:")
    logging.info(f"Primary recipient: {primary_recipient}")
    logging.info(f"Cc recipients: {cc_string}")
    logging.info(f"Bcc recipients: {bcc_string}")

    result = send_email(service, primary_recipient, subject, confirmation_email,
                       original_message_id, references, cc=cc_string, bcc=bcc_string)

    if result:
        logging.info(f"Confirmation email sent successfully")
        
        # ⭐ CRITICAL: Store recipients for the final email
        # This is necessary because BCC recipients are not retrievable from thread
        ti.xcom_push(key="original_all_recipients", value=all_recipients)
        ti.xcom_push(key="original_sender", value=sender_email)
        
        ti.xcom_push(key="confirmation_sent", value=True)
        ti.xcom_push(key="confirmation_message_id", value=result.get("id", ""))
    else:
        logging.error("Failed to send confirmation email")
        return result

def check_if_action_needed(ti, **context):
    """
    Check if any entities were found or proposed.
    If nothing to do, send a polite acknowledgment email.
    """
    search_results = ti.xcom_pull(key="search_results", task_ids = "compile_search_results", default={})
    
    # Check if confirmation is actually needed
    confirmation_needed = search_results.get("confirmation_needed", False)
    
    if confirmation_needed:
        # Normal flow - confirmation email will be sent
        logging.info("✓ Action needed - proceeding to send confirmation email")
        return "compose_confirmation_email"
    else:
        # No entities found or created - send acknowledgment
        logging.info("✗ No action needed - will send acknowledgment email")
        return "compose_no_action_email"


def compose_no_action_email(ti, **context):
    """
    Compose a polite email when no entities were found or created.
    """
    email_data = ti.xcom_pull(key="email_data", task_ids = "load_context_from_dag_run", default={})
    latest_message = ti.xcom_pull(key="latest_message", task_ids = "load_context_from_dag_run", default="")
    search_results = ti.xcom_pull(key="search_results", task_ids = "compile_search_results", default={})
    entity_flags = ti.xcom_pull(key="entity_search_flags", task_ids="analyze_thread_entities", default={})
    
    # Extract sender info
    headers = email_data.get("headers", {})
    sender_raw = headers.get("From", "")
    import email.utils
    sender_tuple = email.utils.parseaddr(sender_raw)
    sender_name = sender_tuple[0].strip() or "there"
    
    # Get what was searched for
    deal_results = search_results.get("deal_results", {})
    contact_results = search_results.get("contact_results", {})
    company_results = search_results.get("company_results", {})
    
    searched_deals = entity_flags.get("search_deals", False)
    searched_contacts = entity_flags.get("search_contacts", False)
    searched_companies = entity_flags.get("search_companies", False)
    
    # Determine what was attempted
    search_summary = []
    if searched_deals:
        search_summary.append("deals")
    if searched_contacts:
        search_summary.append("contacts")
    if searched_companies:
        search_summary.append("companies")
    
    search_text = ", ".join(search_summary) if search_summary else "entities"
    
    # Build email content
    email_html = f"""<!DOCTYPE html>
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
        .greeting {{
            margin-bottom: 20px;
        }}
        .closing {{ margin-top: 30px; }}
        .signature {{
            margin-top: 20px;
            padding-top: 15px;
            border-top: 1px solid #ddd;
        }}
        ul {{
            margin: 10px 0;
            padding-left: 20px;
        }}
        li {{
            margin: 5px 0;
        }}
    </style>
</head>
<body>
    <div class="greeting">
        <p>Hello {sender_name},</p>
        <p>Thank you for your message. I've reviewed your request and searched through HubSpot, but I didn't find anything matching your query, and there's nothing new to create based on the information provided.</p>
    </div>
    <div class="help-section">
        <h4 style="margin-top: 0;">How I Can Help You:</h4>
        <p>I can assist you with the following HubSpot operations:</p>
        <ul>
            <li><strong>Search & Retrieve:</strong> Find contacts, companies, or deals by name, email, or other details</li>
            <li><strong>Create Entities:</strong> Add new contacts, companies, deals, notes, tasks, or meetings</li>
            <li><strong>Update Records:</strong> Modify existing entity information</li>
            <li><strong>Log Activities:</strong> Record meeting minutes, notes, and follow-up tasks</li>
            <li><strong>Generate Reports:</strong> Create summaries and reports of your HubSpot data</li>
        </ul>
    </div>
    
    <div class="closing">
        <p>If you believe there should be matching records in HubSpot, please provide more details such as:</p>
        <ul>
            <li>Full names or exact email addresses</li>
            <li>Company names or domains</li>
            <li>Deal names or IDs</li>
            <li>Specific dates or time periods</li>
        </ul>
        <p style="margin-top: 15px;">I'm here to help! Simply reply with your request, and I'll assist you right away.</p>
    </div>
    
    <div class="signature">
        <p><strong>Best regards,</strong><br>
        The HubSpot Assistant Team<br>
        <a href="http://lowtouch.ai">lowtouch.ai</a></p>
    </div>
</body>
</html>"""
    
    ti.xcom_push(key="no_action_email", value=email_html)
    logging.info("Composed no-action acknowledgment email")
    
    return email_html

def send_no_action_email(ti, **context):
    """
    Send the no-action acknowledgment email.
    """
    email_data = ti.xcom_pull(key="email_data", task_ids = "load_context_from_dag_run", default={})
    no_action_email = ti.xcom_pull(key="no_action_email", task_ids = "compose_no_action_email")
    
    service = authenticate_gmail()
    if not service:
        logging.error("Gmail authentication failed")
        return None
    
    # Extract all recipients
    all_recipients = extract_all_recipients(email_data)
    
    headers = email_data.get("headers", {})
    sender_email = headers.get("From", "")
    original_message_id = headers.get("Message-ID", "")
    references = headers.get("References", "")
    
    if original_message_id and original_message_id not in references:
        references = f"{references} {original_message_id}".strip()
    
    subject = headers.get("Subject", "No Subject")
    if not subject.lower().startswith("re:"):
        subject = f"Re: {subject}"
    
    # Extract primary recipient
    sender_match = re.search(r'<([^>]+)>', sender_email)
    primary_recipient = sender_match.group(1) if sender_match else sender_email
    
    # Prepare CC recipients (reply-all)
    cc_recipients = []
    for to_addr in all_recipients["to"]:
        if (to_addr.lower() != sender_email.lower() and 
            HUBSPOT_FROM_ADDRESS.lower() not in to_addr.lower()):
            cc_recipients.append(to_addr)
    
    for cc_addr in all_recipients["cc"]:
        if (HUBSPOT_FROM_ADDRESS.lower() not in cc_addr.lower() and 
            cc_addr not in cc_recipients):
            cc_recipients.append(cc_addr)
    
    bcc_recipients = [addr for addr in all_recipients["bcc"] 
                      if HUBSPOT_FROM_ADDRESS.lower() not in addr.lower()]
    
    cc_string = ', '.join(cc_recipients) if cc_recipients else None
    bcc_string = ', '.join(bcc_recipients) if bcc_recipients else None
    
    logging.info(f"Sending no-action acknowledgment email:")
    logging.info(f"Primary recipient: {primary_recipient}")
    logging.info(f"Cc recipients: {cc_string}")
    
    result = send_email(service, primary_recipient, subject, no_action_email,
                       original_message_id, references, cc=cc_string, bcc=bcc_string)
    
    if result:
        logging.info("No-action acknowledgment email sent successfully")
        
        # Mark original message as read
        try:
            original_msg_id = email_data.get("id")
            if original_msg_id:
                service.users().messages().modify(
                    userId="me",
                    id=original_msg_id,
                    body={"removeLabelIds": ["UNREAD"]}
                ).execute()
                logging.info(f"Marked message {original_msg_id} as read")
        except Exception as read_err:
            logging.warning(f"Failed to mark message as read: {read_err}")
        
        ti.xcom_push(key="no_action_email_sent", value=True)
    else:
        logging.error("Failed to send no-action acknowledgment email")
    
    return result

def compose_engagement_summary_email(ti, **context):
    """Compose a dedicated email for engagement summary with conditional sections"""
    engagement_summary = ti.xcom_pull(key="engagement_summary", task_ids="summarize_engagement_details_360", default={})
    email_data = ti.xcom_pull(key="email_data", task_ids = "load_context_from_dag_run", default={})
    entity_flags = ti.xcom_pull(key="entity_search_flags", task_ids="analyze_thread_entities", default={})
    
    # Check if summary was requested
    if not entity_flags.get("request_summary",False) and not entity_flags.get("request_summary_360", False):
        logging.info("No engagement summary requested, skipping email composition")
        ti.xcom_push(key="summary_email_needed", value=False)
        return "No summary email needed"
    
    # Check if there's an error in the summary
    if "error" in engagement_summary:
        logging.error(f"Error in engagement summary: {engagement_summary.get('error')}")
        error_email = f"""<!DOCTYPE html>
<html>
<head>
    <style>
        body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #000; }}
        .error {{ background-color: #fff; padding: 15px; border: 2px solid #000; margin: 20px 0; }}
    </style>
</head>
<body>
    <h2>Engagement Summary Request</h2>
    <p>I apologize, but I encountered an issue retrieving the engagement summary. Please check if the contact/deal information is correct and try again.</p>
    <p><strong>Best regards,</strong><br>The HubSpot Assistant Team<br><a href="http://lowtouch.ai">lowtouch.ai</a></p>
</body>
</html>"""
        ti.xcom_push(key="engagement_summary_email", value=error_email)
        ti.xcom_push(key="summary_email_needed", value=True)
        return error_email
    
    from_email = email_data["headers"].get("From", "")
    
    contact_summary = engagement_summary.get('contact_summary', {})
    deal_summary_raw = engagement_summary.get('deal_summary', [])
    company_summary = engagement_summary.get('company_summary', {})
    engagement_details = engagement_summary.get('engagement_summary', '')
    detailed_deal = engagement_summary.get('detailed_deal_summary', '')
    call_strategy = engagement_summary.get('call_strategy', '')
    top_3_contacts_raw = engagement_summary.get('top_3_contacts', [])
    recent_5_activities = engagement_summary.get('recent_5_activities', [])
    risk_flags = engagement_summary.get('risk_flags', {})
    deal_360 = engagement_summary.get('deal_360', [])
    logging.info(f"deal_360 found: {deal_360}")
    # Helper: Check if a dict has meaningful data for given fields
    def has_meaningful_data(entity, required_fields):
        if not entity or not isinstance(entity, dict):
            return False
        return any(str(entity.get(field, "")).strip() for field in required_fields)

    # Helper: Filter list of entities
    def filter_meaningful_entities(entities, required_fields):
        if not entities:
            return []
        return [e for e in entities if has_meaningful_data(e, required_fields)]

    # Filter deals with meaningful data
    meaningful_deals = filter_meaningful_entities(
        deal_summary_raw if isinstance(deal_summary_raw, list) else [],
        ["deal_name", "stage", "amount", "close_date"]
    )

    meaningful_top_3_contacts = filter_meaningful_entities(
        top_3_contacts_raw if isinstance(top_3_contacts_raw, list) else [],
        ["name", "email"]
    )

    # Determine which sections have content
    has_contact = has_meaningful_data(contact_summary, ["contact_name", "email", "company_name"])
    has_company = has_meaningful_data(company_summary, ["company_name", "domain"])
    has_deals = len(meaningful_deals) > 0
    has_engagement = bool(engagement_details and engagement_details.strip())
    has_detailed_deal = bool(detailed_deal and detailed_deal.strip())
    has_call_strategy = bool(call_strategy and call_strategy.strip())
    has_top_3_contacts = len(meaningful_top_3_contacts) > 0
    has_recent_activities = len(recent_5_activities) > 0
    has_risk_flags = any(risk_flags.values())
    has_deal_360 = len(deal_360) > 0


    # If no meaningful sections at all, send a minimal email
    if not (has_contact or has_company or has_deals or has_engagement or has_detailed_deal or has_call_strategy or has_top_3_contacts or has_recent_activities or has_risk_flags or has_deal_360):
        minimal_email = f"""<!DOCTYPE html>
<html>
<head>
    <style>
        body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #000; }}
    </style>
</head>
<body>
    <p>Hello {from_email},</p>
    <p>I processed your request, but no meaningful engagement data was found to summarize.</p>
    <p>Please verify the contact or deal details and try again.</p>
    <p>Best regards,<br>HubSpot Agent</p>
</body>
</html>"""
        ti.xcom_push(key="engagement_summary_email", value=minimal_email)
        ti.xcom_push(key="summary_email_needed", value=True)
        return minimal_email

    # Start building email
    email_content = f"""<!DOCTYPE html>
<html>
<head>
    <style>
        body {{ 
            font-family: Arial, sans-serif; 
            line-height: 1.6; 
            color: #000; 
        }}
        table {{ 
            border-collapse: collapse; 
            width: 100%; 
            margin: 20px 0; 
        }}
        th, td {{ 
            border: 1px solid #ddd; 
            padding: 12px; 
            text-align: left; 
        }}
        th {{ 
            background-color: #f2f2f2; 
            color: #000; 
            font-weight: bold; 
        }}
        h3 {{ 
            color: #000; 
            margin-top: 30px; 
            margin-bottom: 15px;
            font-size: 16px;
        }}
        .section {{ 
            margin: 20px 0; 
        }}
        .section p {{
            margin: 10px 0;
        }}
        .greeting {{ 
            margin-bottom: 30px; 
        }}
        .closing {{ 
            margin-top: 40px;
        }}
    </style>
</head>
<body>
    <div class="greeting">
        <p>Hello {from_email},</p>
        <p>Here is the detailed engagement summary you requested:</p>
    </div>
"""

    # === Conditionally Add Sections ===

    if has_contact:
        email_content += """
        <h3>Contact Information</h3>
        <table>
            <thead>
                <tr>
                    <th>Name</th>
                    <th>Email</th>
                    <th>Company</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td>{}</td>
                    <td>{}</td>
                    <td>{}</td>
                </tr>
            </tbody>
        </table>
        """.format(
            contact_summary.get('contact_name', 'N/A'),
            contact_summary.get('email', 'N/A'),
            contact_summary.get('company_name', 'N/A')
        )

    if has_company:
        email_content += """
        <h3>Company Information</h3>
        <table>
            <thead>
                <tr>
                    <th>Company Name</th>
                    <th>Domain</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td>{}</td>
                    <td>{}</td>
                </tr>
            </tbody>
        </table>
        """.format(
            company_summary.get('company_name', 'N/A'),
            company_summary.get('domain', 'N/A')
        )

    if has_deals:
        email_content += """\
    <div>
        <h3>Deal Information</h3>
        <table border="1">
            <tr><th>Deal Name</th><th>Stage</th><th>Amount</th><th>Close Date</th></tr>"""
        for deal in meaningful_deals:
            email_content += f"""\
            <tr>
                <td>{deal.get("deal_name", "N/A")}</td>
                <td>{deal.get("stage", "N/A")}</td>
                <td>{deal.get("amount", "N/A")}</td>
                <td>{deal.get("close_date", "N/A")}</td>
            </tr>"""
        email_content += "</table></div>"
    
    if has_top_3_contacts:
        email_content += """\
    <div>
        <h3>Top 3 Associated Contacts</h3>
        <table border="1">
            <tr><th>Name</th><th>Email</th></tr>"""
        for contact in meaningful_top_3_contacts:
            email_content += f"""\
            <tr>
                <td>{contact.get("name", "N/A")}</td>
                <td>{contact.get("email", "N/A")}</td>
            </tr>"""
        email_content += "</table></div>"

    if has_recent_activities:
        email_content += f"""\
    <div>
        <h3>Recent 5 Activities</h3>
        <ul>"""
        for activity in recent_5_activities:
            email_content += f"<li>{activity}</li>"
        email_content += "</ul></div>"

    if has_risk_flags:
        email_content += f"""\
    <div>
        <h3>Risk/Opportunity Flags</h3>
        <ul>"""
        if risk_flags.get("past_close_date"):
            email_content += "<li>Deal is past the expected close date.</li>"
        if risk_flags.get("no_activity_14_days"):
            email_content += "<li>No activity in the last 14 days.</li>"
        if risk_flags.get("stage_unchanged_21_days"):
            email_content += "<li>Stage hasn't changed in the previous 21 days.</li>"
        email_content += "</ul></div>"

    if has_detailed_deal:
        email_content += f"""
        <h3>Detailed Deal Analysis</h3>
        <div class="section">
            <p>{detailed_deal}</p>
        </div>
        """

    if has_call_strategy:
        email_content += f"""
        <h3>Recommended Call Strategy</h3>
        <div class="section">
            <p>{call_strategy}</p>
        </div>
        """
    if has_deal_360:
        email_content += f"""\
    <div>
        <h3>Deal 360° Intelligence – External Insights</h3>
        <ul>"""
        for activity in deal_360:
            email_content += f"<li>{activity}</li>"
        email_content += "</ul></div>"

    # Closing
    email_content += """
    <div class="closing">
        <p>This summary provides a comprehensive overview to help you prepare for your upcoming engagement.</p>
        <p>If you need any clarifications or additional information, please let me know.</p>
        <br>
        <p><strong>Best regards,</strong><br>The HubSpot Assistant Team<br><a href="http://lowtouch.ai">lowtouch.ai</a></p>
    </div>
</body>
</html>"""

    ti.xcom_push(key="engagement_summary_email", value=email_content)
    ti.xcom_push(key="summary_email_needed", value=True)
    logging.info("Engagement summary email composed successfully with conditional sections")
    return email_content


def send_engagement_summary_email(ti, **context):
    """Send the engagement summary email with multi-recipient support"""
    summary_email_needed = ti.xcom_pull(key="summary_email_needed", task_ids="compose_engagement_summary_email", default=False)
    
    if not summary_email_needed:
        logging.info("No engagement summary email to send")
        return "No summary email to send"
    
    email_data = ti.xcom_pull(key="email_data", task_ids = "load_context_from_dag_run", default={})
    engagement_summary_email = ti.xcom_pull(key="engagement_summary_email",task_ids = "compose_engagement_summary_email")
    
    service = authenticate_gmail()
    if not service:
        logging.error("Gmail authentication failed")
        return "Gmail authentication failed"
    
    # Extract all recipients from original email
    all_recipients = extract_all_recipients(email_data)
    
    sender_email = email_data["headers"].get("From", "")
    original_message_id = email_data["headers"].get("Message-ID", "")
    references = email_data["headers"].get("References", "")
    
    if original_message_id and original_message_id not in references:
        references = f"{references} {original_message_id}".strip()
    
    subject = f"Re: {email_data['headers'].get('Subject', 'Engagement Summary')}"
    
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
    
    # For Bcc: Include all original Bcc recipients
    bcc_recipients = []
    for bcc_addr in all_recipients["bcc"]:
        if HUBSPOT_FROM_ADDRESS.lower() not in bcc_addr.lower():
            bcc_recipients.append(bcc_addr)
    
    # Convert lists to comma-separated strings
    cc_string = ', '.join(cc_recipients) if cc_recipients else None
    bcc_string = ', '.join(bcc_recipients) if bcc_recipients else None
    
    logging.info(f"Sending engagement summary email:")
    logging.info(f"Primary recipient: {primary_recipient}")
    logging.info(f"Cc recipients: {cc_string}")
    logging.info(f"Bcc recipients: {bcc_string}")
    
    result = send_email(service, primary_recipient, subject, engagement_summary_email,
                       original_message_id, references, cc=cc_string, bcc=bcc_string)
    
    if result:
        logging.info(f"Engagement summary email sent to all recipients")
        
        # ⭐ Store recipients for potential future use in the workflow
        # This is especially important if there are subsequent emails after this
        ti.xcom_push(key="summary_all_recipients", value=all_recipients)
        ti.xcom_push(key="summary_email_sent", value=True)
        ti.xcom_push(key="summary_message_id", value=result.get("id", ""))
        
        logging.info(f"Engagement summary email sent successfully with message ID: {result.get('id')}")
    else:
        logging.error("Failed to send engagement summary email")
    
    return result

def decide_workflow_path(ti, **context):
    """Decide whether to proceed with summary email or confirmation workflow"""
    entity_flags = ti.xcom_pull(key="entity_search_flags", task_ids="analyze_thread_entities",default={})
    if entity_flags.get("request_summary",False) or entity_flags.get("request_summary_360", False):
        logging.info("Taking summary path - will compose and send engagement summary")
        return "compose_engagement_summary_email"
    else:
        logging.info("Taking confirmation path - will process entities and send confirmation")
        return "determine_owner"
# ============================================================================
# DAG DEFINITION
# ============================================================================

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
    schedule=None,
    catchup=False,
    doc_md=readme_content,
    tags=["hubspot", "search", "entities"],
    on_success_callback=clear_retry_tracker_on_success,
    on_failure_callback=update_retry_tracker_on_failure
) as dag:

    load_context_task = PythonOperator(
        task_id="load_context_from_dag_run",
        python_callable=load_context_from_dag_run,
    )

    generate_variants_task = PythonOperator(
    task_id="generate_spelling_variants",
    python_callable=generate_and_inject_spelling_variants,
    )

    analyze_entities_task = PythonOperator(
        task_id="analyze_thread_entities",
        python_callable=analyze_thread_entities,
    )

    summarize_engagement_task = PythonOperator(
        task_id="summarize_engagement_details",
        python_callable=summarize_engagement_details,
    )

    summarize_engagement_360_task = PythonOperator(
    task_id="summarize_engagement_details_360",
    python_callable=summarize_engagement_details_360,
    )

    branch_task = BranchPythonOperator(
        task_id="decide_workflow_path",
        python_callable=decide_workflow_path,
    )

    determine_owner_task = PythonOperator(
        task_id="determine_owner",
        python_callable=determine_owner,
    )
    
    # ADD THIS NEW TASK
    validate_deal_stage_task = PythonOperator(
        task_id="validate_deal_stage",
        python_callable=validate_deal_stage,
    )

    search_contacts_task = PythonOperator(
        task_id="search_contacts_with_associations",
        python_callable=search_contacts_with_associations,
        retries=2
    )

    validate_companies_task = PythonOperator(
        task_id="validate_companies_against_associations",
        python_callable=validate_companies_against_associations,
    )

    validate_deals_task = PythonOperator(
        task_id="validate_deals_against_associations",
        python_callable=validate_deals_against_associations,
    )

    refine_contacts_task = PythonOperator(
        task_id="refine_contacts_by_associations",
        python_callable=refine_contacts_by_associations,
    )

    search_deals_directly_task = PythonOperator(
    task_id="search_deals_directly",
    python_callable=search_deals_directly,
    )

    search_companies_directly_task = PythonOperator(
        task_id="search_companies_directly",
        python_callable=search_companies_directly,
    )

    merge_results_task = PythonOperator(
        task_id="merge_search_results",
        python_callable=merge_search_results,
    )

    validate_context_task = PythonOperator(
        task_id="validate_associations_against_context",
        python_callable=validate_associations_against_context,
    )

    parse_notes_tasks_task = PythonOperator(
        task_id="parse_notes_tasks_meeting",
        python_callable=parse_notes_tasks_meeting,
    )

    check_threshold_task = PythonOperator(
        task_id="check_task_threshold",
        python_callable=check_task_threshold,
    )

    validate_rules_task = PythonOperator(
        task_id="validate_entity_creation_rules",
        python_callable=validate_entity_creation_rules,
    )

    validation_branch_task = BranchPythonOperator(
        task_id="decide_validation_path",
        python_callable=decide_validation_path,
    )

    compile_task = PythonOperator(
        task_id="compile_search_results",
        python_callable=compile_search_results,
    )

    # NEW: Branch to check if any action is needed
    check_action_branch_task = BranchPythonOperator(
        task_id="check_if_action_needed",
        python_callable=check_if_action_needed,
    )

    compose_email_task = PythonOperator(
        task_id="compose_confirmation_email",
        python_callable=compose_confirmation_email,
    )

    send_email_task = PythonOperator(
        task_id="send_confirmation_email",
        python_callable=send_confirmation_email,
    )

    # NEW: No-action acknowledgment tasks
    compose_no_action_task = PythonOperator(
        task_id="compose_no_action_email",
        python_callable=compose_no_action_email,
    )

    send_no_action_task = PythonOperator(
        task_id="send_no_action_email",
        python_callable=send_no_action_email,
    )

    compose_validation_error_task = PythonOperator(
        task_id="compose_validation_error_email",
        python_callable=compose_validation_error_email,
    )

    send_validation_error_task = PythonOperator(
        task_id="send_validation_error_email",
        python_callable=send_validation_error_email,
    )

    validation_end_task = EmptyOperator(
        task_id="validation_end"
    )

    compose_summary_email_task = PythonOperator(
        task_id="compose_engagement_summary_email",
        python_callable=compose_engagement_summary_email,
    )

    send_summary_email_task = PythonOperator(
        task_id="send_engagement_summary_email",
        python_callable=send_engagement_summary_email,
    )

    end_task = EmptyOperator(
        task_id="end_workflow"
    )

    load_context_task >> analyze_entities_task >> summarize_engagement_task >> summarize_engagement_360_task >> branch_task
    load_context_task >> generate_variants_task >> analyze_entities_task
    # === Summary Path ===
    branch_task >> compose_summary_email_task >> send_summary_email_task >> end_task

    # === Entity Processing Path ===
    branch_task >> determine_owner_task >> validate_deal_stage_task

    # Correct order: search contacts first (with associations), then validate companies and deals
    validate_deal_stage_task >> search_contacts_task
    search_contacts_task >> validate_companies_task
    search_contacts_task >> validate_deals_task

    validate_deal_stage_task >> search_contacts_task >> search_deals_directly_task
    validate_deal_stage_task >> search_contacts_task >> search_companies_directly_task
    
    validate_companies_task >> refine_contacts_task
    validate_deals_task >> refine_contacts_task
    [validate_companies_task, validate_deals_task, search_deals_directly_task, search_companies_directly_task] >> merge_results_task
    merge_results_task >> validate_context_task >> refine_contacts_task
    refine_contacts_task >> parse_notes_tasks_task

    # Also allow determine_owner_task to trigger contact search in parallel if needed
    determine_owner_task >> search_contacts_task

    # After all searches/validation, parse notes/tasks/meetings
    validate_deals_task >> refine_contacts_task >> parse_notes_tasks_task

    # Continue existing flow
    parse_notes_tasks_task >> check_threshold_task >> validate_rules_task >> validation_branch_task

    # Validation passes → compile results
    validation_branch_task >> compile_task >> check_action_branch_task

    # Action needed → confirmation flow
    check_action_branch_task >> compose_email_task >> send_email_task >> end_task

    # No action → polite acknowledgment
    check_action_branch_task >> compose_no_action_task >> send_no_action_task >> end_task

    # Validation fails → error email
    validation_branch_task >> compose_validation_error_task >> send_validation_error_task >> validation_end_task

    validation_end_task >> end_task