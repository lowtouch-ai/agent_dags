from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from datetime import datetime
import json
import requests
import difflib  # <--- Built-in Python library for fuzzy matching
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
import logging
from ollama import Client
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import re
import gspread

default_args = {
    'owner': 'devsecops',
    'retries': 1,
}

# SMTP Configuration from Airflow Variables
SMTP_USER = Variable.get("ltai.v1.imageguard.SMTP_USER")
SMTP_PASSWORD = Variable.get("ltai.v1.imageguard.SMTP_PASSWORD")
SMTP_HOST = Variable.get("ltai.v1.imageguard.SMTP_HOST", default_var="mail.authsmtp.com")
SMTP_PORT = int(Variable.get("ltai.v1.imageguard.SMTP_PORT", default_var="2525"))
SMTP_SUFFIX = Variable.get("ltai.v1.imageguard.SMTP_FROM_SUFFIX", default_var="via lowtouch.ai <webmaster@ecloudcontrol.com>")
SENDER_EMAIL = Variable.get("ltai.v1.imageguard.FROM_ADDRESS", default_var=SMTP_USER)
RECEIVER_EMAIL = Variable.get("ltai.v1.imageguard.TO_ADDRESS", default_var=SENDER_EMAIL)

OLLAMA_HOST = Variable.get("ltai.v1.imageguard.OLLAMA_HOST", "http://agentomatic:8000/")

def get_ai_response(prompt, conversation_history=None):
    try:
        logging.debug(f"Query received: {prompt}")
        if not prompt or not isinstance(prompt, str):
            raise ValueError("Invalid prompt provided.")

        client = Client(host=OLLAMA_HOST)
        logging.debug(f"Connecting to Ollama at {OLLAMA_HOST} with model 'imageguard:0.3'")

        messages = []
        if conversation_history:
            messages.extend(conversation_history)
        
        messages.append({"role": "user", "content": prompt})

        response = client.chat(
            model='imageguard:0.3',
            messages=messages,
            stream=False
        )
        logging.info(f"Raw response from agent: {str(response)[:500]}...")

        if 'message' not in response or 'content' not in response['message']:
            logging.error("Response lacks expected 'message.content' structure")
            raise ValueError("Invalid response format from AI.")
        
        ai_content = response['message']['content'].strip()
        if not ai_content:
            logging.warning("AI returned empty content")
            raise ValueError("No response generated.")

        return ai_content
    except Exception as e:
        logging.error(f"Error in get_ai_response: {str(e)}")
        raise

with DAG(
    dag_id='imageguard_eol_monitor',
    default_args=default_args,
    schedule_interval='30 05 1 * *',
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=['imageguard', 'eol', 'alert'],
) as dag:

    @task()
    def fetch_inventory_secure():

        # --- CONFIGURATION ---
        TOKEN_VAR_NAME = 'IMAGEGUARD_GMAIL_CREDENTIALS'
        SHEET_ID = Variable.get("IMAGEGUARD_EOL_SHEET_ID")

        # 2. Get Token
        token_json = Variable.get(TOKEN_VAR_NAME, default_var=None)
        if not token_json:
            logging.error(f"Variable '{TOKEN_VAR_NAME}' is missing!")
            raise ValueError(f"Variable '{TOKEN_VAR_NAME}' is missing!")

        # 3. Refresh Token if needed
        token_dict = json.loads(token_json)
        creds = Credentials.from_authorized_user_info(token_dict)
        if creds.expired and creds.refresh_token:
            logging.info("Refreshing expired token...")
            creds.refresh(Request())
            Variable.set(TOKEN_VAR_NAME, creds.to_json())

        # 4. Fetch Data using ID (The Fix)
        client = gspread.authorize(creds)
        try:
            # open_by_key is faster and requires fewer permissions than open()
            logging.info(f"Opening Google Sheet with ID: {SHEET_ID}")
            spreadsheet = client.open_by_key(SHEET_ID)
            worksheet = spreadsheet.worksheet("Sheet1")
            rows = worksheet.get_all_values()
            headers = rows[0]
            data = [dict(zip(headers, row)) for row in rows[1:] if any(row)]
            return data

        except Exception as e:
            logging.error(f"Error opening sheet: {e}")
            raise

    @task()
    def check_eol_status(inventory_list: list):
        compliance_report = []
        
        # Helper to clean versions
        def normalize_version(v):
            v = str(v).strip()
            m = re.search(r'\d+(\.\d+){0,2}', v)
            return m.group(0) if m else v

        # Prefetch Master List
        try:
            all_products = requests.get("https://endoflife.date/api/all.json", timeout=10).json()
        except:
            logging.warning("Failed to fetch master list")
            all_products = []

        for item in inventory_list:
            # 1. Get Clean Inputs
            raw_name = str(item.get('Base Image', '')).strip().lower() # <--- FIXED KEY FROM YOUR INPUT
            # Handle cases where image name might be "bitnami/alertmanager:0.28.1"
            # We want just "alertmanager" for the name lookup, usually found before the colon or slash
            #if '/' in raw_name: raw_name = raw_name.split('/')[-1]
            #if ':' in raw_name: raw_name = raw_name.split(':')[0]
            
            raw_version = str(item.get('Base Version', '')).strip()
            clean_ver = normalize_version(raw_version)

            if not raw_name or not raw_version: continue

            eol_date_str = None
            source = "Unknown"
            status = "Unknown"
            days_remaining = 9999
            api_slug = None

            # --- 2. TRY TO MATCH PRODUCT NAME ---
            if raw_name in all_products:
                api_slug = raw_name
            else:
                matches = difflib.get_close_matches(raw_name, all_products, n=1, cutoff=0.8)
                if matches:
                    api_slug = matches[0]

            # --- 3. EXECUTE CHECKS ---
            
            # PATH A: We found a valid API Product -> Check API
            if api_slug:
                version_parts = clean_ver.split(".")
                version_candidates = []
                if len(version_parts) >= 3: version_candidates.append(".".join(version_parts[:3]))
                if len(version_parts) >= 2: version_candidates.append(".".join(version_parts[:2]))
                version_candidates.append(version_parts[0])
                version_candidates = list(dict.fromkeys(version_candidates))

                for ver in version_candidates:
                    url = f"https://endoflife.date/api/{api_slug}/{ver}.json"
                    try:
                        response = requests.get(url, timeout=5)
                        if response.status_code == 200:
                            data = response.json()
                            eol_date_str = data.get('eol', 'Unknown')
                            source = "API"
                            break
                    except:
                        pass
            
            # PATH B: API Failed OR Product Name not found -> ASK AI
            if not eol_date_str or eol_date_str == 'Unknown':
                logging.info(f"⚡ API match failed/missing for '{raw_name} v{raw_version}'. Asking AI...")
                source = "AI_Success" # Optimistic default
                
                prompt = f"""
                Act as a Software Lifecycle Expert.
                Product: {raw_name}
                Version: {raw_version}
                
                Question: What is the End-of-Life (EOL) date for this specific version?
                
                Rules:
                1. If it is a rolling release (like MinIO, Arch Linux) or strictly actively supported with no defined EOL date, return: ALIVE
                2. If found, return date format: YYYY-MM-DD
                3. Return ONLY the string (Date or ALIVE). No explanation.
                """
                
                try:
                    # Using your existing global function
                    ai_result = get_ai_response(prompt)
                    
                    if ai_result:
                        # Clean up response (AI sometimes adds text)
                        ai_clean = str(ai_result).strip().upper()
                        
                        date_match = re.search(r'\d{4}-\d{2}-\d{2}', ai_clean)
                        if date_match:
                            eol_date_str = date_match.group(0)
                        elif "ALIVE" in ai_clean or "SUPPORTED" in ai_clean:
                            eol_date_str = "ALIVE"
                        else:
                            eol_date_str = "Unknown"
                            source = "AI_Unsure"
                except Exception as e:
                    logging.error(f"AI failed for {raw_name}: {e}")
                    source = "AI_Error"

            # --- 4. CALCULATE STATUS ---
            if eol_date_str and eol_date_str not in ['Unknown', 'ALIVE', 'False', 'None']:
                 try:
                      eol_dt = datetime.strptime(eol_date_str, '%Y-%m-%d').date()
                      days_remaining = (eol_dt - datetime.now().date()).days
                      
                      if days_remaining < 0: status = "EXPIRED"
                      elif days_remaining <= 45: status = "CRITICAL_WARN"
                      else: status = "OK (Supported)"
                 except ValueError:
                      status = "Date Error"
            elif eol_date_str == 'ALIVE':
                status = "OK (Active)"
            else:
                status = "MANUAL_CHECK"

            compliance_report.append({
                'image': item.get('Image Name '),
                'base_image': raw_name,
                'version': raw_version,
                'eol_date': eol_date_str, 
                'days_remaining': days_remaining,
                'status': status,
                'source': source
            })
        
        return compliance_report

    @task()
    def prepare_eol_alert(report: list):
        """
        Filters for Urgent items (< 45 days, Expired, OR Unknown/Failed Checks)
        """
        critical_statuses = [
            'EXPIRED', 
            'CRITICAL_WARN', 
            'MANUAL_CHECK', 
            'Date Error',  # <--- Added this
            'Unknown'      # <--- Added this to catch unforeseen fall-throughs
        ]
        # 1. Filter Logic
        # CHANGED: Added 'MANUAL_CHECK' and 'Product Not Found' to this list
        critical_images = [
            row for row in report 
            if row['status'] in critical_statuses
        ]

        if not critical_images:
            logging.info("No critical images found. Skipping email generation.")
            return None

        # 2. Construct the Table Data
        eol_table_data = []
        for img in critical_images:
            # Logic: If days are negative, it's URGENT. 
            # If we don't know the days (9999), treat it as a WARNING (Manual Review needed)
            urgency = "URGENT" if img['days_remaining'] <= 0 else "WARNING"
            
            eol_table_data.append({
                "Urgency": urgency,
                "Image": img['image'],
                "Base Image": img['base_image'],
                "Version": img['version'],
                "EOL Date": img['eol_date'],
                "Days Left": str(img['days_remaining']),
                "Source": img['source']
            })

        logging.info(f"Generated Alert Table for {len(eol_table_data)} images.")
        return eol_table_data

    @task()
    def send_slack_alert(report_data: list):
        """
        Sends a formatted Slack message for items with EOL < 45 days.
        Uses the filtered data from prepare_eol_alert.
        """
        if not report_data:
            logging.info("No critical items found. Skipping Slack alert.")
            return

        # 1. Get Webhook URL
        webhook_url = Variable.get("SLACK_WEBHOOK_URL")
        if not webhook_url:
            logging.error("❌ Slack webhook URL not found in Airflow Variables (SLACK_WEBHOOK_URL).")
            return

        # 2. Build the Message Payload (Using Slack Block Kit for better formatting)
        # We limit the message size to prevent Slack API errors if there are too many items
        header_text = f"🚨 *EOL Compliance Alert* ({len(report_data)} images at risk)"
        
        message_lines = []
        for item in report_data:
            # Determine Icon
            icon = "🔴" if item['Urgency'] == "URGENT" else "🟡"
            
            # Format: 🔴 nginx (v1.18) - EOL: 2024-01-01 ([-5] days )
            line = (
                f"{icon} *{item['Image']}* ({item['Base Image']} v`{item['Version']}`)\n"
                f"\t\t🗓 EOL: {item['EOL Date']} (*{item['Days Left']} days*)"
            )
            message_lines.append(line)

        # Join lines (Slack has a char limit, but assuming list isn't massive)
        body_text = "\n".join(message_lines)

        payload = {
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "⚠️ Docker Image EOL Warning",
                        "emoji": True
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": header_text
                    }
                },
                {
                    "type": "divider"
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": body_text
                    }
                },
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": "Check <https://docs.google.com/spreadsheets/d/12EiTmMl-8nI-cPXfjGgNIHwhqYCXZx3ue7ZhlX7aUbg|Inventory Sheet> for details."
                        }
                    ]
                }
            ]
        }

        # 3. Send Request
        try:
            response = requests.post(
                webhook_url,
                data=json.dumps(payload),
                headers={"Content-Type": "application/json"},
                timeout=10
            )
            if response.status_code != 200:
                logging.error(f"Slack API returned {response.status_code}: {response.text}")
            else:
                logging.info("✅ Slack alert sent successfully.")
        except Exception as e:
            logging.error(f"Failed to send Slack alert: {e}")

    # --- DAG Flow ---
    data = fetch_inventory_secure()
    analyzed_data = check_eol_status(data)
    
    # Get the filtered list (critical items only)
    # This list ALREADY filters for statuses defined in check_eol_status
    # where CRITICAL_WARN is <= 45 days.
    critical_data = prepare_eol_alert(analyzed_data)
    
    # Send the Slack Alert (using the same filtered critical data)
    send_slack_alert(critical_data)
