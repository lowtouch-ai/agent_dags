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
                if "role" in item and "content" in item:
                    messages.append({"role": item["role"], "content": item["content"]})
                else:
                    messages.append({"role": "user", "content": item.get("prompt", "")})
                    messages.append({"role": "assistant", "content": item.get("response", "")})
        
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

def analyze_thread_entities(ti, **context):
    """Analyze thread to determine which entities to search and actions to take"""
    chat_history = ti.xcom_pull(key="chat_history", default=[])
    latest_message = ti.xcom_pull(key="latest_message", default="")
    
    chat_context = ""
    for idx, msg in enumerate(chat_history, 1):
        role = msg.get("role", "unknown")
        content = msg.get("content", "")
        chat_context += f"[{role.upper()}]: {content}\n\n"
    
    prompt = f"""You are a HubSpot API assistant. Analyze this latest message to determine which entities (deals, contacts, companies) are mentioned or need to be processed, and whether the user is requesting a summary of a client or deal before their next meeting. 

LATEST USER MESSAGE:
{latest_message}

IMPORTANT: 
    - You must respond with ONLY a valid JSON object. No HTML, no explanations, no markdown formatting.
    - You are not capable of calling any APIs or Tool. You should answer based on your knowledge.
Analyze the content and determine:
1. Is the user requesting a summary of a client or deal before their next meeting? Look for phrases like "summarize details for contact name", "summary for deal name", or explicit mentions of preparing for an upcoming meeting.
2. If a summary is requested, set ALL other flags (search_deals, search_contacts, search_companies, parse_notes, parse_tasks, parse_meetings) to false.
3. If no summary is requested, determine the following:
   - CONTACTS (search_contacts):
        - Set to TRUE if a person's name is mentioned (first name, last name, or full name).
        - This includes contacts in conversational context like "I spoke with John" or "Sarah from ABC Corp".
        - Contact information like email or phone number also triggers this.
        - exclude the user name or hubspot owner names.
        - Exclude the contact name used for assigning a task or deal owner.
        - Set to FALSE if no person is mentioned by name.

    - COMPANIES (search_companies):
        - Set to TRUE if a company/organization name is mentioned
        - This includes formal company names, business names, or organizational references
        - Set to FALSE if no company is mentioned by name. 

    - DEALS (search_deals):
        - Set to TRUE ONLY if ANY of these conditions are met:
            a) User explicitly mentions creating a deal, opportunity, or sale
            b) User states the client/contact is interested in moving forward with a purchase, contract, or agreement
            c) User mentions pricing discussions, proposals sent, quotes provided, or contract negotiations
            d) User indicates a clear buying intent from the client (e.g., "they want to proceed", "ready to sign", "committed to purchase")
        - Set to FALSE for:
            - Initial conversations or introductions
            - Exploratory discussions without commitment
            - Interest without explicit forward movement (e.g., "interested in learning more", "exploring options", "could turn into something")
            - Future potential without current action
            - Client may be interested or impressed, but no explicit intent to buy or move forward is stated.

    - NOTES (parse_notes):
        - Set to TRUE ONLY if a conversation, call, or meeting with a client HAS ALREADY OCCURRED
        - The message must describe what was discussed, outcomes, or information exchanged
        - Set to FALSE for:
            - Future intentions (e.g., "I should call them", "planning to meet")
            - General information about a company or contact without a discussion
            - Thoughts or observations without an actual interaction

    - TASKS (parse_tasks):
        - Set to TRUE ONLY if there is an EXPLICIT action item or follow-up task mentioned. Check for headings next steps, followup steps, if found set tasks to true.
        - Look for phrases like: "need to...", "should...", "must...", "follow up on...", "send them...", "schedule...", "remind me to..."
        - Set to FALSE for:
            - Vague possibilities (e.g., "this could turn into something", "might be good to connect")
            - General hopes or thoughts without specific action items
            - Statements like "stay connected" or "follow up soon" without specific tasks

    - MEETINGS (parse_meetings):
        - Set to TRUE ONLY if ALL of these conditions are met:
            a) A meeting has already occurred (past tense)
            b) Specific meeting details are provided: date, time, duration, and/or timezone
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
    "deals_reason": "explanation why deals need processing or not",
    "contacts_reason": "explanation why contacts need processing or not",
    "companies_reason": "explanation why companies need processing or not",
    "notes_reason": "explanation why notes need processing or not",
    "tasks_reason": "explanation why tasks need processing or not",
    "meetings_reason": "explanation why meetings need processing or not",
    "summary_reason": "explanation why a summary is requested or not"
}}

RESPOND WITH ONLY THE JSON OBJECT - NO OTHER TEXT."""


    response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
    logging.info(f"Prompt is : {prompt}")
    logging.info(f"Conversation history to AI: {chat_history}")
    logging.info(f"Raw AI response for entity analysis: {response[:1000]}...")

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
        logging.error(f"Error processing entity analysis AI response: {e}")
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
    """Retrieve and summarize engagement details based on conversation"""
    entity_flags = ti.xcom_pull(key="entity_search_flags", default={})
    if not entity_flags.get("request_summary", False):
        logging.info("No summary requested, skipping engagement summary")
        ti.xcom_push(key="engagement_summary", value={})
        return
    
    chat_history = ti.xcom_pull(key="chat_history", default=[])
    latest_message = ti.xcom_pull(key="latest_message", default="")
    email_data = ti.xcom_pull(key="email_data", default={})

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
    - **Deal**: [{{Deal_name}}, Stage: {{Deal_stage}}, Amount: {{Deal_Amount}}, Close Date: {{Deal_close_date}}] in tabular format.
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

    response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
    logging.info(f"Raw AI response for engagement summary: {response[:1000]}...")

    try:
        parsed_json = json.loads(response.strip())
        ti.xcom_push(key="engagement_summary", value=parsed_json)
        logging.info(f"Engagement summary generated successfully")
    except Exception as e:
        logging.error(f"Error processing engagement summary AI response: {e}")
        ti.xcom_push(key="engagement_summary", value={"error": f"Error processing engagement summary: {str(e)}"})

def determine_owner(ti, **context):
    """Determine deal owner and task owners from conversation"""
    chat_history = ti.xcom_pull(key="chat_history", default=[])
    latest_message = ti.xcom_pull(key="latest_message", default="")
    

    prompt = f"""You are a HubSpot API assistant. Analyze this conversation to identify deal owner and task owners.

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
5. Parse and validate each task owner against the available owners list:
    - Identify all tasks and their respective owners from the email content.
    - For each task owner:
        - If task owner is NOT specified for a task:
            - Default to: "Kishore"
            - Message: "No task owner specified for task [task_index], so assigning to default owner Kishore."
        - If task owner IS specified but NOT found in available owners list:
            - Default to: "Kishore"
            - Message: "The specified task owner '[parsed_owner]' for task [task_index] is not valid, so assigning to default owner Kishore."
        - If task owner IS specified and IS found in available owners list:
            - Use the matched owner (with correct casing from the available owners list)
            - Message: "Task owner for task [task_index] specified as [matched_owner_name]"
6. Parse and validate each contact owner against the available owners list:
    - If the deal details are not given and contact owner is also not specified:
        - Default to: "Kishore"
        - Message: "No contact owner specified, so assigning to default owner Kishore."
    - If the deal details are not given and also contact owner IS specified but NOT found in available owners list:
        - Default to: "Kishore"
        - Message: "The specified contact owner '[parsed_owner]' is not valid, so assigning to default owner Kishore."
    - If the deal details are given then contact owner is same as deal owner.
7. Return a list of task owners with their validation details.

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

    response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
    logging.info(f"Prompt is : {prompt}")
    logging.info(f"Conversation history to AI: {chat_history}")
    logging.info(f"Raw AI response for owner: {response[:1000]}...")

    try:
        parsed_json = json.loads(response.strip())
        ti.xcom_push(key="owner_info", value=parsed_json)
        logging.info(f"Owner determined: {parsed_json.get('deal_owner_name')}")
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

def search_deals(ti, **context):
    """Search for deals based on conversation context"""
    entity_flags = ti.xcom_pull(key="entity_search_flags", default={})
    if not entity_flags.get("search_deals", True):
        logging.info(f"Skipping deals search: {entity_flags.get('deals_reason', 'Not mentioned')}")
        ti.xcom_push(key="deal_info", value={
            "deal_results": {"total": 0, "results": []},
            "new_deals": []
        })
        return
    
    chat_history = ti.xcom_pull(key="chat_history", default=[])
    latest_message = ti.xcom_pull(key="latest_message", default="")
    owner_info = ti.xcom_pull(key="owner_info")
    
    deal_owner_id = owner_info.get('deal_owner_id', '71346067')
    deal_owner_name = owner_info.get('deal_owner_name', 'Kishore')
    

    prompt = f"""You are a HubSpot Deal Intelligence Assistant. Your role is to analyze the email conversation and:

1. **Search** for existing deals by extracting and matching deal names.
2. **Suggest** new deal drafts **only when the email clearly expresses intent to move forward** (e.g., pricing, timeline, commitment).
3. **You cannot create deals in HubSpot** — you only return structured suggestions for human review.

---

LATEST USER MESSAGE:
{latest_message}

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
8. Always use default closeDate 90 days from today, if not specified in YYYY-MM-DD format.
9. Always use the default deal amount as 5000 if not specified.
10. Fill all fields in the JSON. Use empty string "" for any missing values.

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

    response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
    logging.info(f"Raw AI response for deals: {response[:1000]}...")

    try:
        parsed_json = json.loads(response.strip())
        ti.xcom_push(key="deal_info", value=parsed_json)
        logging.info(f"Deals search completed: {parsed_json['deal_results']['total']} existing, {len(parsed_json['new_deals'])} new")
    except Exception as e:
        logging.error(f"Error processing deals AI response: {e}")
        default = {
            "deal_results": {"total": 0, "results": []},
            "new_deals": []
        }
        ti.xcom_push(key="deal_info", value=default)

def search_contacts(ti, **context):
    """Search for contacts based on conversation context"""
    entity_flags = ti.xcom_pull(key="entity_search_flags", default={})
    if not entity_flags.get("search_contacts", True):
        logging.info(f"Skipping contacts search: {entity_flags.get('contacts_reason', 'Not mentioned')}")
        ti.xcom_push(key="contact_info", value={
            "contact_results": {"total": 0, "results": []},
            "new_contacts": []
        })
        return
    
    chat_history = ti.xcom_pull(key="chat_history", default=[])
    latest_message = ti.xcom_pull(key="latest_message", default="")
    owner_info = ti.xcom_pull(key="owner_info")
    
    contact_owner_id = owner_info.get('contact_owner_id', '71346067')
    contact_owner_name = owner_info.get('contact_owner_name', 'Kishore')

    prompt = f"""You are a HubSpot Contact Search Assistant. Your role is to **search** for existing contacts based on the email conversation.  
**You CANNOT create contacts in HubSpot.**  
You may only **suggest** new contact details **when no match is found and the email clearly identifies a new external person**.

---

LATEST USER MESSAGE:
{latest_message}
Validated Contact Owner ID: {contact_owner_id}
Validated Contact Owner Name: {contact_owner_name}
---

**STRICT INSTRUCTIONS (execute in order):**

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
7. For new contacts, use the validated contact owner name in contactOwnerName.

Return this exact JSON structure:
{{
    "reasoning_summary": {{
        "extracted_names": [
            {{"firstname": "example", "lastname": "example", "template_used": "both|firstname_only", "num_results": 1}}
        ],
        "excluded_names": [
            {{"name": "Amy Thomas", "reason": "deal_owner"}}
        ],
        "total_extracted": 2,
        "search_notes": "Brief notes on decisions"
    }},
    "contact_results": {{
        "total": 0,
        "results": [
            {{
                "contactId": "hs_object_id",
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
            "firstname": "proposed_first",
            "lastname": "proposed_last",
            "email": "proposed_email",
            "phone": "proposed_phone",
            "address": "proposed_address",
            "jobtitle": "proposed_job_title",
            "contactOwnerName": "{contact_owner_name}"
        }}
    ]
}}

Fill ALL fields, use "" for missing values.

RESPOND WITH ONLY THE JSON OBJECT - NO OTHER TEXT."""

    response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
    logging.info(f"Raw AI response for contacts: {response[:1000]}...")

    try:
        parsed_json = json.loads(response.strip())
        ti.xcom_push(key="contact_info", value=parsed_json)
        logging.info(f"Contacts search completed: {parsed_json['contact_results']['total']} existing, {len(parsed_json['new_contacts'])} new")
    except Exception as e:
        logging.error(f"Error processing contacts AI response: {e}")
        default = {
            "contact_results": {"total": 0, "results": []},
            "new_contacts": []
        }
        ti.xcom_push(key="contact_info", value=default)

def search_companies(ti, **context):
    """Search for companies based on conversation context"""
    entity_flags = ti.xcom_pull(key="entity_search_flags", default={})
    if not entity_flags.get("search_companies", True):
        logging.info(f"Skipping companies search: {entity_flags.get('companies_reason', 'Not mentioned')}")
        ti.xcom_push(key="company_info", value={
            "company_results": {"total": 0, "results": []},
            "new_companies": [],
            "partner_status": None
        })
        return
    
    chat_history = ti.xcom_pull(key="chat_history", default=[])
    latest_message = ti.xcom_pull(key="latest_message", default="")


    prompt = f"""You are a HubSpot Company Search Assistant. Your role is to **search** for existing companies based on the email conversation.  
**You CANNOT create companies in HubSpot.**  
You may only **suggest** new company details **when no match is found and the email clearly identifies a new external organization**.

---

LATEST USER MESSAGE:
{latest_message}

---

**STRICT INSTRUCTIONS (execute in order):**

1. **Extract company name(s) and email** from the conversation:
   - Look for formal company names (e.g., "Acme Corp", "TechFlow Inc.", "Neha's Startup").
   - **Exclude**:
     - Internal references to your own company.
     - Generic terms: "the client", "vendor", "partner" (unless part of a proper name).
     - Email domains alone (e.g., `@gmail.com`) unless tied to a clear company.
   - Extract **one company per distinct entity**.

2. **For each extracted company name**:
   - **Simulate a HubSpot `search_companies` API call** using 90 percent match on `name`.
   - Use **CONTAINS_TOKEN operator** on `name` property for precision. 
   - Assume API returns matching records or empty list. Display only the most matching results in the matching records 
   **Search payload template**:
   {{{{
       "filterGroups": [
           {{{{
               "filters": [
                   {{{{
                       "propertyName": "name",
                       "operator": "CONTAINS_TOKEN",
                       "value": "{{{{extracted_company_name}}}}",

                       "propertyName": "email",
                       "operator": "CONTAINS_TOKEN",
                       "value": "{{{{extracted_company_email}}}}"

                   }}}}
               ]
           }}}}
       ],
       "properties": [
           "hs_object_id", "name", "domain", "address", "city", "state", "zip", 
           "country", "phone", "description", "type"
       ],
       "sorts": [{{{{ "propertyName": "hs_lastmodifieddate", "direction": "DESCENDING" }}}}],
       "limit": 5
   }}}}

3. **Process search results**:
   - Deduplicate by `hs_object_id`.
   - Populate `company_results.results` with **exact API-returned values**.
   - Set `type` to `"PARTNER"` or `"PROSPECT"` if present; otherwise `"PROSPECT"`.
   - `total` = number of unique matches.

4. **Suggest new companies ONLY if**:
   - **No match found** for a clearly mentioned company, **AND**
   - Email provides **at least one identifying detail** (domain, address, phone, description, signature).
   - **Do NOT suggest** duplicates already in `company_results`.

5. **Determine `type` for new companies**:
   - `"PARTNER"` if words like "partner", "reseller", "agency", "referral", "integrator" appear in context.
   - Otherwise → `"PROSPECT"`.

6. **Extract additional fields from email**:
   - `domain`: From email signature, website, or mention (e.g., `acme.com`).
   - `address`, `city`, etc.: From signature, footer, or context.
   - `description`: Summarize business in 1 sentence if possible.
   - Use `""` if not found.

7. **Set `partner_status`**:
   - `true` → if **any** company (existing or new) is marked `"PARTNER"`.
   - `false` → if **all** are `"PROSPECT"` and no partner language.
   - `null` → if no companies extracted.

---

**RETURN EXACTLY THIS JSON STRUCTURE (NO CHANGES TO BRACKETS):**
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

**RULES**:
- Fill **ALL fields**; use `""` for missing values.
- `new_companies` = `[]` unless **no match + clear external company identity**.
- Never suggest a company already in `company_results`.
- **RESPOND WITH ONLY THE JSON OBJECT — NO OTHER TEXT.**
"""

    response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
    logging.info(f"Raw AI response for companies: {response[:1000]}...")

    try:
        parsed_json = json.loads(response.strip())
        ti.xcom_push(key="company_info", value=parsed_json)
        logging.info(f"Companies search completed: {parsed_json['company_results']['total']} existing, {len(parsed_json['new_companies'])} new")
    except Exception as e:
        logging.error(f"Error processing companies AI response: {e}")
        default = {
            "company_results": {"total": 0, "results": []},
            "new_companies": [],
            "partner_status": None
        }
        ti.xcom_push(key="company_info", value=default)

def parse_notes_tasks_meeting(ti, **context):
    """Parse notes, tasks, and meetings from conversation"""
    entity_flags = ti.xcom_pull(key="entity_search_flags", default={})
    email_data = ti.xcom_pull(key="email_data", default={})
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
    
    chat_history = ti.xcom_pull(key="chat_history", default=[])
    latest_message = ti.xcom_pull(key="latest_message", default="")
    owner_info = ti.xcom_pull(key="owner_info", default={})
    
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

For meetings (only if meeting parsing is enabled):
- Extract meeting title, start time, end time, location, outcome, timestamp, attendees, meeting type, and meeting status.
- If "I" is mentioned for attendees that refers to the email sender name.

For tasks (only if task parsing is enabled):
- Identify all tasks and their respective owners from the email content.
- Always check for headings mext steps or followup steps. All the next steps, followup steps specified in email content are considered as tasks.
- For each task:
  - Match the task to the corresponding owner in the provided Task Owners list by task_index (1-based indexing).
  - If a specific task owner is mentioned in the email and matches an entry in the Task Owners list, use that owner's name and ID.
  - If a specific task owner is mentioned but does not match any entry in the Task Owners list, use the default task owner: {default_task_owner_name} (ID: {default_task_owner_id}).
  - If no task owner is specified, use the default task owner: {default_task_owner_name} (ID: {default_task_owner_id}).
  - If no due date is specified, use the date three business days from the current date.
  - For due date is today is mentioned, use the current date. Tomorrow is current date + 1 day, after 2 days is current date + 2 days and so on. day after tomorrow is current date + 2 days.
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


    response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
    logging.info(f"Raw AI response for notes/tasks/meeting: {response[:1000]}...")

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

def check_task_threshold(ti, **context):
    """Check if task volume exceeds threshold"""
    entity_flags = ti.xcom_pull(key="entity_search_flags", default={})
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
    
    chat_history = ti.xcom_pull(key="chat_history", default=[])
    latest_message = ti.xcom_pull(key="latest_message", default="")
    
    # Get CORRECTED tasks from parse_notes_tasks_meeting
    notes_tasks_meeting = ti.xcom_pull(key="notes_tasks_meeting", default={})
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

    response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
    logging.info(f"Raw AI response for task threshold: {response[:1000]}...")

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
    owner_info = ti.xcom_pull(key="owner_info")
    deal_info = ti.xcom_pull(key="deal_info")
    contact_info = ti.xcom_pull(key="contact_info")
    company_info = ti.xcom_pull(key="company_info")
    notes_tasks_meeting = ti.xcom_pull(key="notes_tasks_meeting")
    task_threshold_info = ti.xcom_pull(key="task_threshold_info", default={})
    thread_id = ti.xcom_pull(key="thread_id")
    email_data = ti.xcom_pull(key="email_data")
    
    logging.info(f"=== COMPILING SEARCH RESULTS ===")
    logging.info(f"Thread ID: {thread_id}")
    
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
    search_results = ti.xcom_pull(key="search_results")
    email_data = ti.xcom_pull(key="email_data")
    confirmation_needed = ti.xcom_pull(key="confirmation_needed", default=False)
    engagement_summary = ti.xcom_pull(key="engagement_summary", default={})
    notes_tasks_meeting = ti.xcom_pull(key="notes_tasks_meeting", default={})
    corrected_tasks = notes_tasks_meeting.get("tasks", [])
    
    if not confirmation_needed:
        logging.info("No confirmation needed")
        return "No confirmation needed"

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

    has_deals_or_tasks_or_contacts = (
        deal_results.get("total", 0) > 0 or
        len(new_deals) > 0 or
        len(meaningful_tasks) > 0 or
        contact_results.get("total", 0) > 0 or
        len(new_contacts) > 0
    )

    if has_deals_or_tasks_or_contacts:
        has_content_sections = True
        email_content += "<h3>Owner Assignment Details</h3>"
        
        # Contact Owner
        if contact_results.get("total", 0) > 0 or len(new_contacts) > 0:
            email_content += "<div style='margin-bottom: 15px;'>"
            email_content += ""
            
            contact_msg_lower = contact_owner_msg.lower()
            
            if "no contact owner specified" in contact_msg_lower:
                email_content += f"""
                <h4 style='color: #2c5aa0;'>Contact Owner Assignment:</h4>
                <p style='background-color: #d1ecf1; padding: 10px; border-left: 4px solid #17a2b8;'>
                    <strong>Reason:</strong> Contact owner not specified.
                    <br><strong>Action:</strong> Assigning to default owner '{contact_owner_name}'.
                </p>
                """
            elif "not valid" in contact_msg_lower:
                email_content += f"""
                <h4 style='color: #2c5aa0;'>Contact Owner Assignment:</h4>
                <p style='background-color: #f8d7da; padding: 10px; border-left: 4px solid #dc3545;'>
                    <strong>Reason:</strong> Contact owner not found in available owners.
                    <br><strong>Action:</strong> Assigning to default owner '{contact_owner_name}'.
                </p>
                """
            email_content += "</div>"
        # Deal Owner
        if deal_results.get("total", 0) > 0 or len(new_deals) > 0:
            email_content += "<div style='margin-bottom: 15px;'>"
            email_content += ""
            
            deal_msg_lower = deal_owner_msg.lower()
            
            if "no deal owner specified" in deal_msg_lower:
                email_content += f"""
                <h4 style='color: #2c5aa0;'>Deal Owner Assignment:</h4>
                <p style='background-color: #d1ecf1; padding: 10px; border-left: 4px solid #17a2b8;'>
                    <strong>Reason:</strong> Deal owner not specified.
                    <br><strong>Action:</strong> Assigning to default owner '{chosen_deal_owner_name}'.
                </p>
                """
            elif "not valid" in deal_msg_lower:
                email_content += f"""
                <h4 style='color: #2c5aa0;'>Deal Owner Assignment:</h4>
                <p style='background-color: #f8d7da; padding: 10px; border-left: 4px solid #dc3545;'>
                    <strong>Reason:</strong> Deal owner not found in available owners.
                    <br><strong>Action:</strong> Assigning to default owner '{chosen_deal_owner_name}'.
                </p>
                """
            email_content += "</div>"
        
        # Task Owners
        if len(meaningful_tasks) > 0:
            email_content += "<div style='margin-bottom: 15px;'>"
            email_content += ""
            
            for task in corrected_tasks:  # Use corrected_tasks instead of task_owners
                task_index = task.get("task_index", 0)
                task_owner_name = task.get("task_owner_name", "Kishore")
                task_details = task.get("task_details", "Unknown")
                
                # Find the original message from determine_owner to show what happened
                original_task_owner = next((to for to in task_owners if to.get("task_index") == task_index), None)
                task_owner_msg = original_task_owner.get("task_owner_message", "") if original_task_owner else ""
                
                task_msg_lower = task_owner_msg.lower()
                
                if "no task owner specified" in task_msg_lower:
                    email_content += f"""
                    <h4 style='color: #2c5aa0;'>Task Owner Assignment:</h4>
                    <p style='background-color: #d1ecf1; padding: 10px; border-left: 4px solid #17a2b8;'>
                        <strong>Task {task_index}:</strong> {task_details}
                        <br><strong>Reason:</strong> Task owner not specified.
                        <br><strong>Action:</strong> Assigning to default owner '{task_owner_name}'.
                    </p>
                    """
                elif "not valid" in task_msg_lower:
                    email_content += f"""
                    <h4 style='color: #2c5aa0;'>Task Owner Assignment:</h4>
                    <p style='background-color: #f8d7da; padding: 10px; border-left: 4px solid #dc3545;'>
                        <strong>Task {task_index}:</strong> {task_details}
                        <br><strong>Reason:</strong> Task owner not found.
                        <br><strong>Action:</strong> Assigning to default owner '{task_owner_name}'.
                    </p>
                    """
            email_content += "</div>"
        
        # Available Owners Table
        if all_owners:
            email_content += """
            <h4 style='color: #2c5aa0;'>Available Owners:</h4>
            <table>
                <thead>
                    <tr>
                        <th>Owner ID</th>
                        <th>Owner Name</th>
                        <th>Owner Email</th>
                        <th>Assignment</th>
                    </tr>
                </thead>
                <tbody>
            """
            
            for owner in all_owners:
                owner_id = owner.get("id", "")
                owner_name = owner.get("name", "")
                owner_email = owner.get("email", "")
                
                assignments = []
                if owner_id == contact_owner_id and (contact_results.get("total", 0) > 0 or len(new_contacts) > 0):
                    assignments.append("Contact Owner")
                if owner_id == chosen_deal_owner_id and (deal_results.get("total", 0) > 0 or len(new_deals) > 0):
                    assignments.append("Deal Owner")
                
                if any(task.get("task_owner_id") == owner_id for task in corrected_tasks) and len(meaningful_tasks) > 0:
                    task_indices = [str(task.get("task_index")) for task in corrected_tasks if task.get("task_owner_id") == owner_id]
                    assignments.append(f"Task Owner (Tasks {', '.join(task_indices)})")
                
                assignment_text = ", ".join(assignments) if assignments else ""

                if "Contact Owner" in assignments and "Task Owner" in assignments:
                    row_style = ' style="background-color: #c3e6cb; border-left: 4px solid #155724;"'
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
                        <td>{owner_id}</td>
                        <td><strong>{owner_name}</strong></td>
                        <td>{owner_email}</td>
                        <td><strong>{assignment_text}</strong></td>
                    </tr>
                """
            
            email_content += "</tbody></table>"
        
        email_content += "<hr>"

    email_content += """
        <div class="closing">
            <p><strong>Instructions:</strong></p>
            <ul>
                <li>Reply with "PROCEED WITH EXISTING" to use existing entities and create new objects</li>
                <li>Reply with "CREATE NEW" along with corrections to modify proposed objects</li>
                <li>Specify any changes needed in your reply</li>
            </ul>
            <p>Please confirm whether this summary looks correct before I proceed.</p>
            <p>Best regards,<br>HubSpot Agent</p>
        </div>
    </body>
    </html>
    """

    ti.xcom_push(key="confirmation_email", value=email_content)
    logging.info(f"Confirmation email composed")
    return email_content

def send_confirmation_email(ti, **context):
    """Send confirmation email to all recipients"""
    email_data = ti.xcom_pull(key="email_data")
    confirmation_email = ti.xcom_pull(key="confirmation_email")
    confirmation_needed = ti.xcom_pull(key="confirmation_needed", default=False)
    
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

def compose_engagement_summary_email(ti, **context):
    """Compose a dedicated email for engagement summary with conditional sections"""
    engagement_summary = ti.xcom_pull(key="engagement_summary", default={})
    email_data = ti.xcom_pull(key="email_data")
    entity_flags = ti.xcom_pull(key="entity_search_flags", default={})
    
    # Check if summary was requested
    if not entity_flags.get("request_summary", False):
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
    <div class="error">
        <strong>Error:</strong> {engagement_summary.get('error')}
    </div>
    <p>I apologize, but I encountered an issue retrieving the engagement summary. Please check if the contact/deal information is correct and try again.</p>
    <p>Best regards,<br>HubSpot Agent</p>
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

    # Determine which sections have content
    has_contact = has_meaningful_data(contact_summary, ["contact_name", "email", "company_name"])
    has_company = has_meaningful_data(company_summary, ["company_name", "domain"])
    has_deals = len(meaningful_deals) > 0
    has_engagement = bool(engagement_details and engagement_details.strip())
    has_detailed_deal = bool(detailed_deal and detailed_deal.strip())
    has_call_strategy = bool(call_strategy and call_strategy.strip())

    # If no meaningful sections at all, send a minimal email
    if not (has_contact or has_company or has_deals or has_engagement or has_detailed_deal or has_call_strategy):
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
            border: 1px solid #000; 
            padding: 12px; 
            text-align: left; 
        }}
        th {{ 
            background-color: #fff; 
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
        <p>Here is the comprehensive engagement summary you requested:</p>
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
        email_content += """
        <h3>Deal Information</h3>
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
        """
        for deal in meaningful_deals:
            email_content += f"""
                <tr>
                    <td>{deal.get("deal_name", "N/A")}</td>
                    <td>{deal.get("stage", "N/A")}</td>
                    <td>{deal.get("amount", "N/A")}</td>
                    <td>{deal.get("close_date", "N/A")}</td>
                </tr>
            """
        email_content += """
            </tbody>
        </table>
        """

    if has_engagement:
        email_content += f"""
        <h3>Engagement Overview</h3>
        <div class="section">
            <p>{engagement_details}</p>
        </div>
        """

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

    # Closing
    email_content += """
    <div class="closing">
        <p>This summary provides a comprehensive overview to help you prepare for your upcoming engagement.</p>
        <p>If you need any clarifications or additional information, please don't hesitate to ask.</p>
        <br>
        <p><strong>Best regards,</strong><br>HubSpot Agent</p>
    </div>
</body>
</html>"""

    ti.xcom_push(key="engagement_summary_email", value=email_content)
    ti.xcom_push(key="summary_email_needed", value=True)
    logging.info("Engagement summary email composed successfully with conditional sections")
    return email_content


def send_engagement_summary_email(ti, **context):
    """Send the engagement summary email with multi-recipient support"""
    summary_email_needed = ti.xcom_pull(key="summary_email_needed", default=False)
    
    if not summary_email_needed:
        logging.info("No engagement summary email to send")
        return "No summary email to send"
    
    email_data = ti.xcom_pull(key="email_data")
    engagement_summary_email = ti.xcom_pull(key="engagement_summary_email")
    
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
    entity_flags = ti.xcom_pull(key="entity_search_flags", default={})
    
    if entity_flags.get("request_summary", False):
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
    schedule_interval=None,
    catchup=False,
    doc_md=readme_content,
    tags=["hubspot", "search", "entities"]
) as dag:

    load_context_task = PythonOperator(
        task_id="load_context_from_dag_run",
        python_callable=load_context_from_dag_run,
        provide_context=True
    )

    analyze_entities_task = PythonOperator(
        task_id="analyze_thread_entities",
        python_callable=analyze_thread_entities,
        provide_context=True
    )

    summarize_engagement_task = PythonOperator(
        task_id="summarize_engagement_details",
        python_callable=summarize_engagement_details,
        provide_context=True
    )

    branch_task = BranchPythonOperator(
        task_id="decide_workflow_path",
        python_callable=decide_workflow_path,
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

    parse_notes_tasks_task = PythonOperator(
        task_id="parse_notes_tasks_meeting",
        python_callable=parse_notes_tasks_meeting,
        provide_context=True
    )

    check_threshold_task = PythonOperator(
        task_id="check_task_threshold",
        python_callable=check_task_threshold,
        provide_context=True
    )

    compile_task = PythonOperator(
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

    compose_summary_email_task = PythonOperator(
        task_id="compose_engagement_summary_email",
        python_callable=compose_engagement_summary_email,
        provide_context=True
    )

    send_summary_email_task = PythonOperator(
        task_id="send_engagement_summary_email",
        python_callable=send_engagement_summary_email,
        provide_context=True
    )

    end_task = DummyOperator(
        task_id="end_workflow"
    )

    load_context_task >> analyze_entities_task >> summarize_engagement_task >> branch_task
    
    # Summary workflow path (when request_summary is True)
    branch_task >> compose_summary_email_task >> send_summary_email_task >> end_task
    # Define task dependencies
    load_context_task >> analyze_entities_task >> summarize_engagement_task >> determine_owner_task
    determine_owner_task >> [search_deals_task, search_contacts_task, search_companies_task]
    [search_deals_task, search_contacts_task, search_companies_task] >> parse_notes_tasks_task
    parse_notes_tasks_task >> check_threshold_task >> compile_task >> compose_email_task >> send_email_task
