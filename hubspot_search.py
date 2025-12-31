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
import requests


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
OLLAMA_HOST = Variable.get("ltai.v3.hubspot.ollama.host","http://agentomatic:8000")
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
        logging.error(f"Error in get_ai_response: {e}")
        return handle_json_error(expect_json)


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

def generate_and_inject_spelling_variants(ti, **context):
    latest_message = ti.xcom_pull(key="latest_message", default="")
    chat_history = ti.xcom_pull(key="chat_history", default=[])

    recent_context = ""
    for msg in chat_history[-4:]:  # Last few messages
        recent_context += f"{msg['role'].upper()}: {msg['content']}\n\n"
    recent_context += f"USER: {latest_message}"

    variant_prompt = f"""You are a helpful assistant that detects potential spelling mistakes in names mentioned in business emails.

Your task: Extract any contact names, company names, or deal names that might have typos.
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
        response = get_ai_response(variant_prompt, expect_json=True)
        logging.info(f"Raw AI response for spelling variants: {response}")
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
    chat_history = ti.xcom_pull(key="chat_history", default=[])
    latest_message = ti.xcom_pull(key="latest_message", default="")
    email_data = ti.xcom_pull(key="email_data", default={})
    thread_id = ti.xcom_pull(key="thread_id", default="unknown")
    spelling_variants = ti.xcom_pull(key="spelling_variants", default={})

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
            c) Only create meeting if all the details including Title,Start Time,End Time,Location(Offline or online),Outcome and Attendees are given.Do not create meetings if any of the details are missing.
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
        logging.info(f"Prompt is : {prompt}")
        logging.info(f"Conversation history to AI: {chat_history}")
        logging.info(f"Raw AI response for entity analysis: {response[:1000]}...")

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

    except Exception as ai_error:
        logging.warning(f"AI failed in analyze_thread_entities for thread {thread_id}: {ai_error} → Sending fallback email")

        # === FALLBACK EMAIL - FULLY INLINE ===
        # Replace the fallback_body section in analyze_user_response function (around line 300)

        fallback_body = f"""
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

        try:
            service = authenticate_gmail()
            if not service:
                logging.error("Gmail auth failed during fallback")
                ti.xcom_push(key="entity_search_flags", value={
                    "search_deals": True, "search_contacts": True, "search_companies": True,
                    "parse_notes": True, "parse_tasks": True, "parse_meetings": True,
                    "request_summary": False,
                    "deals_reason": "AI failed, defaulting to search",
                    "contacts_reason": "AI failed, defaulting to search",
                    "companies_reason": "AI failed, defaulting to search",
                    "notes_reason": "AI failed, defaulting to parse",
                    "tasks_reason": "AI failed, defaulting to parse",
                    "meetings_reason": "AI failed, defaulting to parse",
                    "summary_reason": "AI failed, no summary requested"
                })
                return

            # Build threading
            original_message_id = headers.get("Message-ID", "")
            references = headers.get("References", "")
            if original_message_id:
                references = f"{references} {original_message_id}".strip() if references else original_message_id

            subject = headers.get("Subject", "No Subject")
            if not subject.lower().startswith("re:"):
                subject = f"Re: {subject}"

            # Recipients (reply-all)
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

            # Compose message
            msg = MIMEMultipart()
            msg["From"] = f"HubSpot via lowtouch.ai <{HUBSPOT_FROM_ADDRESS}>"
            msg["To"] = primary_recipient
            if cc_string: msg["Cc"] = cc_string
            if bcc_string: msg["Bcc"] = bcc_string
            msg["Subject"] = subject
            if original_message_id: msg["In-Reply-To"] = original_message_id
            if references: msg["References"] = references
            msg.attach(MIMEText(fallback_body, "html"))

            # Send
            raw_msg = base64.urlsafe_b64encode(msg.as_string().encode("utf-8")).decode("utf-8")
            service.users().messages().send(userId="me", body={"raw": raw_msg}).execute()

            # === Mark original message as read (inline) ===
            try:
                if email_data.get("id"):
                    service.users().messages().modify(
                        userId="me",
                        id=email_data["id"],
                        body={"removeLabelIds": ["UNREAD"]}
                    ).execute()
                    logging.info(f"Marked message {email_data['id']} as read")
            except Exception as read_err:
                logging.warning(f"Failed to mark message as read: {read_err}")

            logging.info(f"Fallback technical issue email sent for thread {thread_id}")

        except Exception as send_error:
            logging.error(f"Failed to send fallback email: {send_error}", exc_info=True)

        # === Default to full search ===
        ti.xcom_push(key="entity_search_flags", value={
            "search_deals": True,
            "search_contacts": True,
            "search_companies": True,
            "parse_notes": True,
            "parse_tasks": True,
            "parse_meetings": True,
            "request_summary": False,
            "deals_reason": "AI failed, defaulting to search",
            "contacts_reason": "AI failed, defaulting to search",
            "companies_reason": "AI failed, defaulting to search",
            "notes_reason": "AI failed, defaulting to parse",
            "tasks_reason": "AI failed, defaulting to parse",
            "meetings_reason": "AI failed, defaulting to parse",
            "summary_reason": "AI failed, no summary requested"
        })

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

    response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
    logging.info(f"Raw AI response for engagement summary: {response[:1000]}...")

    try:
        parsed_json = json.loads(response.strip())
        ti.xcom_push(key="engagement_summary", value=parsed_json)
        logging.info(f"Engagement summary generated successfully")
    except Exception as e:
        logging.error(f"Error processing engagement summary AI response: {e}")
        ti.xcom_push(key="engagement_summary", value={"error": f"Error processing engagement summary: {str(e)}"})

def summarize_engagement_details_360(ti, **context):
    """Generate enhanced Deal 360 view using Perplexity-powered web research"""
    entity_flags = ti.xcom_pull(key="entity_search_flags", default={})
    if not entity_flags.get("request_summary_360", False):
        logging.info("No 360 summary requested, skipping")
        return

    chat_history = ti.xcom_pull(key="chat_history", default=[])
    latest_message = ti.xcom_pull(key="latest_message", default="")
    email_data = ti.xcom_pull(key="email_data", default={})

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
    chat_history = ti.xcom_pull(key="chat_history", default=[])
    latest_message = ti.xcom_pull(key="latest_message", default="")
    email_data = ti.xcom_pull(key="email_data", default={})
    headers      = email_data.get("headers", {})
    sender_raw   = headers.get("From", "")                # e.g. "John Doe <john@acme.com>"
    # Parse a clean name and e-mail (fallback to raw string if parsing fails)
    import email.utils
    sender_tuple = email.utils.parseaddr(sender_raw)      # (realname, email)
    sender_name  = sender_tuple[0].strip() or sender_raw
    sender_email = sender_tuple[1].strip() or sender_raw
    

    prompt = f"""You are a HubSpot API assistant. Analyze this conversation to identify deal owner and task owners.
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

def validate_deal_stage(ti, **context):
    """Validate deal stage from conversation and assign default if invalid"""
    chat_history = ti.xcom_pull(key="chat_history", default=[])
    latest_message = ti.xcom_pull(key="latest_message", default="")
    
    # Valid deal stages
    VALID_DEAL_STAGES = [
        "Lead", "Qualified Lead", "Solution Discussion", "Proposal", 
        "Negotiations", "Contracting", "Closed Won", "Closed Lost", "Inactive"
    ]
    
    prompt = f"""You are a HubSpot Deal Stage Validation Assistant. Your role is to validate deal stages from the conversation.

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
            "original_stage": "user_specified_stage_or_empty",
            "validated_stage": "Lead",
            "stage_message": "explanation_message"
        }}
    ]
}}

RESPOND WITH ONLY THE JSON OBJECT - NO OTHER TEXT."""

    response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
    logging.info(f"Raw AI response for deal stage validation: {response[:1000]}...")

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
    """Search contacts in HubSpot using API
    
    Search priority:
    - If email is provided → exact match on email
    - If both firstname and lastname → exact match on both (AND)
    - If only firstname → search by firstname using CONTAINS_TOKEN (fuzzy match)
    - If only lastname → not supported (to avoid irrelevant results)
    - EXCLUDE deal owners mentioned with "assign to", "owner", or similar assignment language
    - EXCLUDE internal team members, senders, or system users (e.g., skip "From: John Doe <john@company.com>")
    - EXCLUDE names that are clearly role/department indicators in parentheses like "(Ops)", "(Finance)", "(IT)"
 
    """
    try:
        endpoint = f"{HUBSPOT_BASE_URL}/crm/v3/objects/contacts/search"
        headers = {
            "Authorization": f"Bearer {HUBSPOT_API_KEY}",
            "Content-Type": "application/json"
        }
        
        filters = []
        
        # Priority 1: Email exact match (if provided)
        if email:
            filters.append({
                "propertyName": "email",
                "operator": "CONTAINS_TOKEN",
                "value": email.strip().lower()  # HubSpot emails are case-insensitive
            })
        else:
            # Priority 2: Both first and last name → require both (AND)
            if firstname and lastname:
                filters.extend([
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
            # Priority 3: Only firstname → use CONTAINS_TOKEN for broader match
            elif firstname:
                filters.append({
                    "propertyName": "firstname",
                    "operator": "CONTAINS_TOKEN",
                    "value": firstname.strip()
                })
            # If only lastname is provided → do nothing (avoid poor results)
            elif lastname:
                logging.info("Search by lastname only is not supported. Requires firstname or email.")
                return {"total": 0, "results": []}
            else:
                return {"total": 0, "results": []}
        
        payload = {
            "filterGroups": [{"filters": filters}],
            "properties": [
                "hs_object_id", "firstname", "lastname", "email", 
                "phone", "address", "jobtitle", "hubspot_owner_id"
            ],
            "sorts": [{"propertyName": "lastmodifieddate", "direction": "DESCENDING"}],
            "limit": 10
        }
        
        response = requests.post(endpoint, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        results = data.get("results", [])
        
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
                "contactOwnerId": props.get("hubspot_owner_id", "")
            })
        
        search_desc = email or f"{firstname or ''} {lastname or ''}".strip()
        logging.info(f"Found {len(formatted_results)} contacts for search: {search_desc}")
        return {"total": len(formatted_results), "results": formatted_results}
        
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error during contact search: {http_err} - {response.text}")
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
    entity_flags = ti.xcom_pull(key="entity_search_flags", default={})
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

    chat_history = ti.xcom_pull(key="chat_history", default=[])
    latest_message = ti.xcom_pull(key="latest_message", default="")
    
    # AI extracts contact names from email
    prompt = f"""Extract ALL contact names mentioned in this email. For each contact, provide:
- firstname
- lastname (empty string if not provided)
- email (if mentioned)

LATEST MESSAGE:
{latest_message}

RULES:
- Extract EVERY person mentioned
- Exclude internal team members and deal owners
- Handle single names (e.g., "Neha") as firstname only
- Parse "Neha (Ops)" as firstname="Neha", ignore role

Return ONLY valid JSON:
{{
    "contacts": [
        {{"firstname": "...", "lastname": "...", "email": "..."}}
    ]
}}
"""

    response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
    
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
            
            # Search contact
            search_result = search_contacts_api(
                firstname=firstname if firstname else None,
                lastname=lastname if lastname else None,
                email=email if email else None
            )
            
            if search_result["total"] > 0:
                # Contact found - get their associations
                for found_contact in search_result["results"]:
                    found_contacts.append(found_contact)
                    contact_id = found_contact["contactId"]
                    
                    # Get associated companies
                    company_ids = get_contact_associations(contact_id, "companies")
                    for company_id in company_ids:
                        company_details = get_company_details(company_id)
                        if company_details and company_details not in all_associated_companies:
                            all_associated_companies.append(company_details)
                    
                    # Get associated deals
                    deal_ids = get_contact_associations(contact_id, "deals")
                    for deal_id in deal_ids:
                        deal_details = get_deal_details(deal_id)
                        if deal_details and deal_details not in all_associated_deals:
                            all_associated_deals.append(deal_details)
            else:
                # Contact not found - propose for creation
                not_found_contacts.append({
                    "firstname": firstname,
                    "lastname": lastname,
                    "email": email,
                    "phone": "",
                    "address": "",
                    "jobtitle": "",
                    "contactOwnerName": "Kishore"  # Default
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
    contact_data = ti.xcom_pull(key="contact_info_with_associations", default={})
    associated_companies = contact_data.get("associated_companies", [])
    
    chat_history = ti.xcom_pull(key="chat_history", default=[])
    latest_message = ti.xcom_pull(key="latest_message", default="")
    
    # AI extracts companies mentioned in email
    prompt = f"""Extract ALL company names explicitly mentioned in this email.

LATEST MESSAGE:
{latest_message}

RULES:
- Only extract formal company/organization names
- Exclude "lowtouch.ai and ecloudcontrol" (internal)
- Return empty list if no companies mentioned

Return ONLY valid JSON:
{{
    "companies": [
        {{"name": "...", "domain": "..."}}
    ]
}}
"""

    response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
    
    try:
        parsed = json.loads(response.strip())
        mentioned_companies = parsed.get("companies", [])
        
        # Log for debugging
        logging.info(f"Associated companies from contacts: {[c.get('name') for c in associated_companies]}")
        logging.info(f"Mentioned companies from email: {[c.get('name') for c in mentioned_companies]}")
        
        # Start with ALL associated as existing
        existing_companies = associated_companies.copy()
        
        # Now add unmatched mentioned as new
        new_companies = []
        
        for mentioned in mentioned_companies:
            mentioned_name_lower = mentioned.get("name", "").strip().lower()
            if not mentioned_name_lower:
                continue
            
            matched = False
            for assoc in associated_companies:
                assoc_name_lower = assoc.get("name", "").strip().lower()
                if assoc_name_lower and (
                    mentioned_name_lower == assoc_name_lower or
                    mentioned_name_lower in assoc_name_lower or
                    assoc_name_lower in mentioned_name_lower
                ):
                    matched = True
                    break
            
            if not matched:
                new_companies.append({
                    "name": mentioned.get("name", ""),
                    "domain": mentioned.get("domain", ""),
                    "address": "",
                    "city": "",
                    "state": "",
                    "zip": "",
                    "country": "",
                    "phone": "",
                    "description": "",
                    "type": "PROSPECT"
                })
        
        result = {
            "company_results": {
                "total": len(existing_companies),
                "results": existing_companies
            },
            "new_companies": new_companies,
            "partner_status": None
        }
        
        ti.xcom_push(key="company_info", value=result)
        logging.info(f"Companies: {len(existing_companies)} existing (all associated), {len(new_companies)} new (unmatched mentioned)")
        
    except Exception as e:
        logging.error(f"Error validating companies: {e}")
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
    contact_data = ti.xcom_pull(key="contact_info_with_associations", default={})
    associated_deals = contact_data.get("associated_deals", [])
    
    chat_history = ti.xcom_pull(key="chat_history", default=[])
    latest_message = ti.xcom_pull(key="latest_message", default="")
    owner_info = ti.xcom_pull(key="owner_info", default={})
    
    deal_owner_name = owner_info.get('deal_owner_name', 'Kishore')
    
    # AI extracts deals mentioned in email
    prompt = f"""Extract deals mentioned in this email. Only include if there's CLEAR buying intent.

LATEST MESSAGE:
{latest_message}

Deal Owner: {deal_owner_name}

RULES:
- Only extract if explicit deal creation requested OR clear buying signals
- NOT exploratory conversations
- Return deal name, stage (default: Lead), amount, close date

Return ONLY valid JSON:
{{
    "deals": [
        {{
            "dealName": "...",
            "dealLabelName": "Lead",
            "dealAmount": "5000",
            "closeDate": "YYYY-MM-DD",
            "dealOwnerName": "{deal_owner_name}"
        }}
    ]
}}
"""

    response = get_ai_response(prompt, conversation_history=chat_history, expect_json=True)
    
    try:
        parsed = json.loads(response.strip())
        mentioned_deals = parsed.get("deals", [])
        
        # Log for debugging
        logging.info(f"Associated deals from contacts: {[d.get('dealName') for d in associated_deals]}")
        logging.info(f"Mentioned deals from email: {[d.get('dealName') for d in mentioned_deals]}")
        
        # Start with ALL associated as existing
        existing_deals = associated_deals.copy()
        
        # Now add unmatched mentioned as new
        new_deals = []
        
        for mentioned in mentioned_deals:
            mentioned_name_lower = mentioned.get("dealName", "").strip().lower()
            if not mentioned_name_lower:
                continue
            
            matched = False
            for assoc in associated_deals:
                assoc_name_lower = assoc.get("dealName", "").strip().lower()
                if assoc_name_lower and (
                    mentioned_name_lower == assoc_name_lower or
                    mentioned_name_lower in assoc_name_lower or
                    assoc_name_lower in mentioned_name_lower
                ):
                    matched = True
                    break
            
            if not matched:
                new_deals.append(mentioned)
        
        result = {
            "deal_results": {
                "total": len(existing_deals),
                "results": existing_deals
            },
            "new_deals": new_deals
        }
        
        ti.xcom_push(key="deal_info", value=result)
        logging.info(f"Deals: {len(existing_deals)} existing (all associated), {len(new_deals)} new (unmatched mentioned)")
        
    except Exception as e:
        logging.error(f"Error validating deals: {e}")
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
    contact_info = ti.xcom_pull(key="contact_info_with_associations", default={})
    company_info = ti.xcom_pull(key="company_info", default={})
    deal_info = ti.xcom_pull(key="deal_info", default={})

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

    ti.xcom_push(key="contact_info_with_associations", value=refined_contact_info)

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

def validate_entity_creation_rules(ti, **context):
    """
    Validate that entity creation follows HubSpot association rules.
    Returns validation result and detailed error messages.
    
    CRITICAL FIX: Check BOTH existing AND new entities when validating associations.
    """
    entity_flags = ti.xcom_pull(key="entity_search_flags", default={})
    contact_info = ti.xcom_pull(key="contact_info_with_associations", default={})
    company_info = ti.xcom_pull(key="company_info", default={})
    deal_info = ti.xcom_pull(key="deal_info", default={})
    notes_tasks_meeting = ti.xcom_pull(key="notes_tasks_meeting", default={})
    email_data = ti.xcom_pull(key="email_data", default={})
    
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
    
    # RULE 4: NEW Companies should ideally be associated with contacts
    # This is a soft validation - only log warning, don't block
    if new_companies > 0 and total_contacts == 0:
        logging.warning(f"⚠ {new_companies} new company/companies being created without any contact association")
        # Note: We don't add this to validation_errors because companies can exist standalone
    
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
    validation_result = ti.xcom_pull(key="validation_result", default={})
    email_data = ti.xcom_pull(key="email_data", default={})
    
    errors = validation_result.get("errors", [])
    entity_summary = validation_result.get("entity_summary", {})
    sender_name = validation_result.get("sender_name", "there")
    
    # Build error details HTML
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
    <div class="greeting">
        <p>Hello {sender_name},</p>
        <p>Thank you for your request to log information in HubSpot. I've reviewed your request and found that some entities cannot be created due to HubSpot's association requirements.</p>
    </div>
    
    <h3 class="section-title"> Issues Found</h3>
    {error_details_html}
    
    <h4 class="section-title"> How to fix this</h4>
        <p><strong>HubSpot Association Rules:</strong></p>
            <li><strong>Meetings, Notes, and Tasks</strong> must be linked to at least one: Contact, Deal, or Company</li>
            <li><strong>Deals</strong> should be associated with a Contact for proper follow-up</li>
            <li><strong>Contacts</strong> can be standalone, but work best when linked to a Company</li>
        <p style="margin-top: 15px;"><strong>To proceed, please reply with:</strong></p>
        <ul style="margin: 10px 0; padding-left: 20px;">
            <li>The missing contact/company/deal information, OR</li>
            <li>Confirmation to use an existing contact/company/deal (if applicable)</li>
        </ul>
    
    <div class="closing">
        <p>I'm ready to help once you provide the additional information. Simply reply to this email with the details, and I'll process your request right away.</p>
        <p>If you have any questions about these requirements, feel free to ask!</p>
    </div>
    
    <div class="signature">
        <p><strong>Best regards,</strong><br>
        The HubSpot Assistant Team<br>
        <a href="http://lowtouch.ai">lowtouch.ai</a></p>
    </div>
</body>
</html>"""
    
    ti.xcom_push(key="validation_error_email", value=email_html)
    logging.info(f"Composed validation error email with {len(errors)} error(s)")
    
    return email_html


def send_validation_error_email(ti, **context):
    """
    Send the validation error email to the user with proper threading.
    """
    email_data = ti.xcom_pull(key="email_data", default={})
    validation_error_email = ti.xcom_pull(key="validation_error_email")
    
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
    validation_result = ti.xcom_pull(key="validation_result", default={})
    
    is_valid = validation_result.get("is_valid", False)
    
    if is_valid:
        logging.info(" Validation passed - proceeding with entity creation workflow")
        return "compile_search_results"
    else:
        logging.warning(" Validation failed - sending error email to user")
        return "compose_validation_error_email"

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
    contact_info = ti.xcom_pull(key="contact_info_with_associations", default={})
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
    deal_stage_info = ti.xcom_pull(key="deal_stage_info", default={"deal_stages": []})  # This is already correct
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

    has_deals_or_tasks_or_contacts = (
        deal_results.get("total", 0) > 0 or
        len(new_deals) > 0 or
        len(meaningful_tasks) > 0 or
        contact_results.get("total", 0) > 0 or
        len(new_contacts) > 0
    )

    if has_deals_or_tasks_or_contacts:
        has_content_sections = True
        
        
        
        # Contact Owner
        if contact_results.get("total", 0) > 0 or len(new_contacts) > 0:
            email_content += "<div style='margin-bottom: 15px;'>"
            email_content += ""
            
            contact_msg_lower = contact_owner_msg.lower()
            
            if "no contact owner specified" in contact_msg_lower:
                email_content += "<h3>Owner Assignment Details</h3>"
                email_content += f"""
                <h4 style='color: #2c5aa0;'>Contact Owner Assignment:</h4>
                <p style='background-color: #d1ecf1; padding: 10px; border-left: 4px solid #17a2b8;'>
                    <strong>Reason:</strong> Contact owner not specified.
                    <br><strong>Action:</strong> Assigning to default owner '{contact_owner_name}'.
                </p>
                """
            elif "not valid" in contact_msg_lower:
                email_content += "<h3>Owner Assignment Details</h3>"
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
                email_content += "<h3>Owner Assignment Details</h3>"
                email_content += f"""
                <h4 style='color: #2c5aa0;'>Deal Owner Assignment:</h4>
                <p style='background-color: #d1ecf1; padding: 10px; border-left: 4px solid #17a2b8;'>
                    <strong>Reason:</strong> Deal owner not specified.
                    <br><strong>Action:</strong> Assigning to default owner '{chosen_deal_owner_name}'.
                </p>
                """
            elif "not valid" in deal_msg_lower:
                email_content += "<h3>Owner Assignment Details</h3>"
                email_content += f"""
                <h4 style='color: #2c5aa0;'>Deal Owner Assignment:</h4>
                <p style='background-color: #f8d7da; padding: 10px; border-left: 4px solid #dc3545;'>
                    <strong>Reason:</strong> Deal owner not found in available owners.
                    <br><strong>Action:</strong> Assigning to default owner '{chosen_deal_owner_name}'.
                </p>
                """
            email_content += "</div>"

        # ADD THIS: Deal Stage Validation Section
        if deal_stage_info.get('deal_stages'):
            email_content += "<div style='margin-bottom: 15px;'>"
        has_stage_issues = False
        for stage_info in deal_stage_info.get('deal_stages', []):
            original_stage = stage_info.get('original_stage', '')
            validated_stage = stage_info.get('validated_stage', 'Lead')
            
            # Check if there's an actual issue (no stage or invalid stage)
            if not original_stage or original_stage.strip() == '':
                has_stage_issues = True
                break
            elif validated_stage == 'Lead' and original_stage.lower() != 'lead':
                has_stage_issues = True
                break
        
        # Only show the section if there are issues
        if has_stage_issues:
            email_content += "<br>"
            email_content += "<h3>Deal Stage Assignment Details</h3>"
            
        for stage_info in deal_stage_info.get('deal_stages', []):
                deal_name = stage_info.get('deal_name', 'Unknown Deal')
                original_stage = stage_info.get('original_stage', '')
                validated_stage = stage_info.get('validated_stage', 'Lead')
                stage_message = stage_info.get('stage_message', '')
                
                if not original_stage or original_stage.strip() == '':
                    # No stage specified
                    email_content += f"""
                    <h4 style='color: #2c5aa0;'>Deal Stage for "{deal_name}":</h4>
                    <p style='background-color: #d1ecf1; padding: 10px; border-left: 4px solid #17a2b8;'>
                        <strong>Reason:</strong> Deal stage not specified.
                        <br><strong>Action:</strong> Assigning to default stage 'Lead'.
                    </p>
                    """
                elif validated_stage == 'Lead' and original_stage.lower() != 'lead':
                    # Invalid stage specified
                    email_content += f"""
                    <h4 style='color: #2c5aa0;'>Deal Stage for "{deal_name}":</h4>
                    <p style='background-color: #f8d7da; padding: 10px; border-left: 4px solid #dc3545;'>
                        <strong>Reason:</strong> Invalid deal stage '{original_stage}' specified.
                        <br><strong>Valid Stages:</strong> {', '.join(VALID_DEAL_STAGES)}
                        <br><strong>Action:</strong> Assigning to default stage 'Lead'.
                    </p>
                    """
            # Don't show anything for valid stages
        
        email_content += "<br>"

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
                    email_content += "<h3>Owner Assignment Details</h3>"
                    email_content += f"""
                    <h4 style='color: #2c5aa0;'>Task Owner Assignment:</h4>
                    <p style='background-color: #d1ecf1; padding: 10px; border-left: 4px solid #17a2b8;'>
                        <strong>Task {task_index}:</strong> {task_details}
                        <br><strong>Reason:</strong> Task owner not specified.
                        <br><strong>Action:</strong> Assigning to default owner '{task_owner_name}'.
                    </p>
                    """
                elif "not valid" in task_msg_lower:
                    email_content += "<h3>Owner Assignment Details</h3>"
                    email_content += f"""
                    <h4 style='color: #2c5aa0;'>Task Owner Assignment:</h4>
                    <p style='background-color: #f8d7da; padding: 10px; border-left: 4px solid #dc3545;'>
                        <strong>Task {task_index}:</strong> {task_details}
                        <br><strong>Reason:</strong> Task owner not found.
                        <br><strong>Action:</strong> Assigning to default owner '{task_owner_name}'.
                    </p>
                    """
            email_content += "</div>"

    email_content += """
        <div class="closing">
            <p>Please confirm whether this summary looks correct before I proceed.</p>
            <p><strong>Best regards,</strong><br>The HubSpot Assistant Team<br><a href="http://lowtouch.ai">Lowtouch.ai</a></p>
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

def check_if_action_needed(ti, **context):
    """
    Check if any entities were found or proposed.
    If nothing to do, send a polite acknowledgment email.
    """
    search_results = ti.xcom_pull(key="search_results", default={})
    
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
    email_data = ti.xcom_pull(key="email_data", default={})
    latest_message = ti.xcom_pull(key="latest_message", default="")
    search_results = ti.xcom_pull(key="search_results", default={})
    entity_flags = ti.xcom_pull(key="entity_search_flags", default={})
    
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
    email_data = ti.xcom_pull(key="email_data", default={})
    no_action_email = ti.xcom_pull(key="no_action_email")
    
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
    engagement_summary = ti.xcom_pull(key="engagement_summary", default={})
    email_data = ti.xcom_pull(key="email_data")
    entity_flags = ti.xcom_pull(key="entity_search_flags", default={})
    
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
    <p><strong>Best regards,</strong><br>The HubSpot Assistant Team<br><a href="http://lowtouch.ai">Lowtouch.ai</a></p>
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
        <p><strong>Best regards,</strong><br>The HubSpot Assistant Team<br><a href="http://lowtouch.ai">Lowtouch.ai</a></p>
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

    generate_variants_task = PythonOperator(
    task_id="generate_spelling_variants",
    python_callable=generate_and_inject_spelling_variants,
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

    summarize_engagement_360_task = PythonOperator(
    task_id="summarize_engagement_details_360",
    python_callable=summarize_engagement_details_360,
    provide_context=True,
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
    
    # ADD THIS NEW TASK
    validate_deal_stage_task = PythonOperator(
        task_id="validate_deal_stage",
        python_callable=validate_deal_stage,
        provide_context=True
    )

    search_contacts_task = PythonOperator(
        task_id="search_contacts_with_associations",
        python_callable=search_contacts_with_associations,
        provide_context=True,
        retries=2
    )

    validate_companies_task = PythonOperator(
        task_id="validate_companies_against_associations",
        python_callable=validate_companies_against_associations,
        provide_context=True
    )

    validate_deals_task = PythonOperator(
        task_id="validate_deals_against_associations",
        python_callable=validate_deals_against_associations,
        provide_context=True
    )

    refine_contacts_task = PythonOperator(
        task_id="refine_contacts_by_associations",
        python_callable=refine_contacts_by_associations,
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

    validate_rules_task = PythonOperator(
        task_id="validate_entity_creation_rules",
        python_callable=validate_entity_creation_rules,
        provide_context=True
    )

    validation_branch_task = BranchPythonOperator(
        task_id="decide_validation_path",
        python_callable=decide_validation_path,
        provide_context=True
    )

    compile_task = PythonOperator(
        task_id="compile_search_results",
        python_callable=compile_search_results,
        provide_context=True
    )

    # NEW: Branch to check if any action is needed
    check_action_branch_task = BranchPythonOperator(
        task_id="check_if_action_needed",
        python_callable=check_if_action_needed,
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

    # NEW: No-action acknowledgment tasks
    compose_no_action_task = PythonOperator(
        task_id="compose_no_action_email",
        python_callable=compose_no_action_email,
        provide_context=True
    )

    send_no_action_task = PythonOperator(
        task_id="send_no_action_email",
        python_callable=send_no_action_email,
        provide_context=True
    )

    compose_validation_error_task = PythonOperator(
        task_id="compose_validation_error_email",
        python_callable=compose_validation_error_email,
        provide_context=True
    )

    send_validation_error_task = PythonOperator(
        task_id="send_validation_error_email",
        python_callable=send_validation_error_email,
        provide_context=True
    )

    validation_end_task = DummyOperator(
        task_id="validation_end"
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

    load_context_task >> analyze_entities_task >> summarize_engagement_task >> summarize_engagement_360_task >> branch_task
    load_context_task >> generate_variants_task >> analyze_entities_task
    # === Summary Path ===
    branch_task >> compose_summary_email_task >> send_summary_email_task >> end_task

    # === Entity Processing Path ===
    branch_task >> determine_owner_task >> validate_deal_stage_task

    # Correct order: search contacts first (with associations), then validate companies and deals
    validate_deal_stage_task >> search_contacts_task >> validate_companies_task >> validate_deals_task

    validate_companies_task >> refine_contacts_task
    validate_deals_task >> refine_contacts_task
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