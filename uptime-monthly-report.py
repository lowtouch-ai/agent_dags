from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta, timezone
import base64
import json
import logging
import re
from ollama import Client
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
import os
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.dates import DayLocator, DateFormatter
import io
import requests
import smtplib

# Configure detailed logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

default_args = {
    "owner": "lowtouch.ai_developers",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 18),
    "retries": 0,
    "retry_delay": timedelta(seconds=15),
}

SMTP_USER = Variable.get("SMTP_USER")
SMTP_PASSWORD = Variable.get("SMTP_PASSWORD")
SMTP_HOST = Variable.get("SMTP_HOST", default_var="mail.authsmtp.com")
SMTP_PORT = int(Variable.get("SMTP_PORT", default_var="2525"))
SMTP_SUFFIX = Variable.get("SMTP_FROM_SUFFIX", default_var="via lowtouch.ai <webmaster@ecloudcontrol.com>")
OLLAMA_HOST = "http://agentomatic:8000/"
UPTIME_API_KEY = Variable.get("UPTIME_API_KEY")

MONITOR_ID = Variable.get("UPTIME_MONITOR_ID")
RECIPIENT_EMAIL = Variable.get("UPTIME_REPORT_RECIPIENT_EMAIL")
REPORT_PERIOD = "last 30 days"

def get_ai_response(prompt, conversation_history=None):
    """Get AI response with conversation history context"""
    try:
        logging.debug(f"Query received: {prompt}")
        
        if not prompt or not isinstance(prompt, str):
            return "Invalid input provided. Please enter a valid query."

        client = Client(host=OLLAMA_HOST, headers={'x-ltai-client': 'uptime-agent'})
        logging.debug(f"Connecting to Ollama at {OLLAMA_HOST} with model 'uptime_agent:0.3'")

        messages = []
        if conversation_history:
            for history_item in conversation_history:
                messages.append({"role": "user", "content": history_item["prompt"]})
                messages.append({"role": "assistant", "content": history_item["response"]})
        
        messages.append({"role": "user", "content": prompt})

        response = client.chat(
            model='uptime_agent:0.3',
            messages=messages,
            stream=False
        )
        logging.info(f"Raw response from agent: {str(response)[:500]}...")

        if not (hasattr(response, 'message') and hasattr(response.message, 'content')):
            logging.error("Response lacks expected 'message.content' structure")
            return "Invalid response format from AI. Please try again later."
        
        ai_content = response.message.content
        logging.info(f"Full message content from agent: {ai_content[:500]}...")
        return ai_content.strip()

    except Exception as e:
        logging.error(f"Error in get_ai_response: {str(e)}")
        return f"An error occurred while processing your request: {str(e)}"

def send_email(recipient, subject, body, in_reply_to="", references="", img_b64=None):
    try:
        # Initialize SMTP server
        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=10)
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
    
        # Create MIME message
        logging.debug(f"Preparing email to {recipient} with subject: {subject}")
        msg = MIMEMultipart('related')
        msg['Subject'] = subject
        msg['From'] = f"Uptime Reports {SMTP_SUFFIX}"
        msg['To'] = recipient
        if in_reply_to:
            msg["In-Reply-To"] = in_reply_to
        if references:
            msg["References"] = references
        
        # Attach the HTML content
        msg.attach(MIMEText(body, 'html'))

        if img_b64:
            try:
                # Decode the base64 string
                img_data = base64.b64decode(img_b64)
                
                # Create the image part
                img_part = MIMEImage(img_data, 'png')
                
                # Add Content-ID header to be referenced by <img src="cid:response_chart">
                img_part.add_header('Content-ID', '<response_chart>')
                img_part.add_header('Content-Disposition', 'inline', filename='response_chart.png')
                
                # Attach the image to the message
                msg.attach(img_part)
                logging.info("Successfully attached CID image to email.")
                
            except Exception as e:
                logging.error(f"Failed to attach image to email: {str(e)}")
                
        # Send the email
        server.sendmail("webmaster@ecloudcontrol.com", recipient, msg.as_string())
        logging.info(f"Email sent successfully: {recipient}")
        server.quit()
        return True
    except Exception as e:
        logging.error(f"Failed to send email: {str(e)}")
        return None

def fetch_main_monitor_data(start_ts, end_ts):
    """
    Fetches main monitor data (logs, uptime, ssl) WITHOUT response_times.
    """
    url = "https://api.uptimerobot.com/v2/getMonitors"
    payload = {
        'api_key': UPTIME_API_KEY,
        'format': 'json',
        'logs': '1',
        'logs_limit': '1000', # Increased limit for monthly
        'custom_uptime_ratios': '1-7-30-365',
        'ssl': '1',
        'alert_contacts': '1',
        'monitors': MONITOR_ID,
        'logs_start_date': str(int(start_ts)),
        'logs_end_date': str(int(end_ts)),
    }
    resp = requests.post(url, data=payload)
    if resp.status_code != 200:
        raise ValueError(f"API request failed: {resp.text}")
    data = resp.json()
    if data.get('stat') != 'ok':
        raise ValueError(f"API error: {data}")
    if not data.get('monitors'):
        raise ValueError("No monitor data found")
    return data['monitors'][0]

def fetch_chunked_response_times(start_dt, end_dt):
    """
    Fetches response_times in 7-day chunks to bypass API limits.
    """
    url = "https://api.uptimerobot.com/v2/getMonitors"
    all_response_times = []
    chunk_start_dt = start_dt
    
    logging.info(f"Starting chunked fetch for response times from {start_dt} to {end_dt}")
    
    # Use 6-day intervals to be safe with the <= 7-day limit
    chunk_interval_days = 6 
    
    while chunk_start_dt < end_dt:
        # Ensure the chunk end date doesn't exceed the final end_dt
        chunk_end_dt = min(chunk_start_dt + timedelta(days=chunk_interval_days), end_dt)
        
        start_ts = int(chunk_start_dt.timestamp())
        end_ts = int(chunk_end_dt.timestamp())

        # Avoid making a request for a zero-width time range
        if start_ts >= end_ts:
             break

        logging.info(f"Fetching response time chunk: {chunk_start_dt} to {chunk_end_dt}")

        payload = {
            'api_key': UPTIME_API_KEY,
            'format': 'json',
            'response_times': '1',
            'response_times_average': '30', # Use 30-min avg for granularity
            'monitors': MONITOR_ID,
            'response_times_start_date': str(start_ts),
            'response_times_end_date': str(end_ts),
        }
        resp = requests.post(url, data=payload)
        
        if resp.status_code != 200:
            logging.warning(f"API request failed for chunk {chunk_start_dt}-{chunk_end_dt}: {resp.text}")
            chunk_start_dt += timedelta(days=chunk_interval_days, seconds=1) # Move to next chunk
            continue
            
        data = resp.json()
        if data.get('stat') == 'ok' and data.get('monitors'):
            all_response_times.extend(data['monitors'][0].get('response_times', []))
        else:
            logging.warning(f"API error for chunk {chunk_start_dt}-{chunk_end_dt}: {data}")
            
        # Move start time to 1 second after the end of the last chunk
        chunk_start_dt = chunk_end_dt + timedelta(seconds=1) 
        
    logging.info(f"Chunked fetch completed. Got {len(all_response_times)} response time entries.")
    return all_response_times

def parse_monitor_data(monitor, rt_list):
    """
    Parses the main monitor data and the separately-fetched response time list.
    """
    custom_uptime = monitor.get('custom_uptime_ratio', '').split('-')
    custom_down = monitor.get('custom_down_durations', '').split('-')
    
    # Use index [2] for 30-day data (indices: [0]=1d, [1]=7d, [2]=30d, [3]=365d)
    uptime_30d = f"{float(custom_uptime[2]):.2f}%" if len(custom_uptime) > 2 else "N/A"
    down_sec_30d = int(custom_down[2]) if len(custom_down) > 2 else 0
    # Convert to hours for readability
    downtime_30d = f"{down_sec_30d / 3600:.1f} hours" if down_sec_30d > 0 else "0 hours"
    
    logs = monitor.get('logs', [])
    errors = sum(1 for log in logs if log.get('type') == 1)
    status_map = {0: 'Paused', 1: 'Down', 2: 'Up', 9: 'Pending'}
    status = status_map.get(monitor.get('status', 0), 'Unknown')
    ssl = monitor.get('ssl', {})
    brand = ssl.get('brand', 'N/A')
    expires = ssl.get('expires', 0)
    expiry_date = datetime.fromtimestamp(expires).strftime('%Y-%m-%d') if expires else 'N/A'
    
    # rt_list is now passed in from the chunked fetch
    if rt_list:
        values = [r['value'] for r in rt_list if 'value' in r and r['value'] is not None]
        min_rt = min(values) if values else 0
        max_rt = max(values) if values else 0
        avg_rt = sum(values) / len(values) if values else 0
    else:
        min_rt = max_rt = avg_rt = 0
        
    alert_contacts = [c.get('value', '') for c in monitor.get('alert_contacts', [])]
    to_be_notified = ', '.join(alert_contacts) if alert_contacts else 'N/A'
    
    structured = {
        "monitor_information": {
            "monitor_name": monitor.get('friendly_name', 'N/A'),
            "monitor_id": monitor.get('id', 'N/A'),
            "monitor_url": monitor.get('url', 'N/A')
        },
        "uptime_status": {
            "status": status,
            "uptime_last_30days": uptime_30d,
            "downtime_last_30days": downtime_30d,
            "errors_last_30days": str(errors)
        },
        "ssl_information": {"brand": brand, "expiry_date": expiry_date},
        "response_time": {"min": f"{min_rt:.2f}", "max": f"{max_rt:.2f}", "avg": f"{avg_rt:.2f}"},
        "notifications": {"to_be_notified": to_be_notified},
        "logs_summary": {"summary": f"Total logs: {len(logs)}. Errors: {errors}."}
    }
    return structured

def step_1_fetch_data(ti, **context):
    now = datetime.now(timezone.utc)
    today_start_dt = now.replace(hour=0, minute=0, second=0, microsecond=0)
    current_month_start_dt = today_start_dt.replace(day=1)

    # Current 30-day period
    current_end_dt = current_month_start_dt - timedelta(seconds=1)
    current_start_dt = current_end_dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    
    # Previous 30-day period
    prev_end_dt = current_start_dt - timedelta(seconds=1)
    prev_start_dt = prev_end_dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    logging.info(f"Fetching time periods: Current {current_start_dt} to {current_end_dt}, Previous {prev_start_dt} to {prev_end_dt}")
    
    # Fetch current period data
    monitor_current = fetch_main_monitor_data(int(current_start_dt.timestamp()), int(current_end_dt.timestamp()))
    rt_current = fetch_chunked_response_times(current_start_dt, current_end_dt)
    structured_current = parse_monitor_data(monitor_current, rt_current)
    logs = monitor_current.get('logs', [])
    df_current_list = [{'datetime': datetime.fromtimestamp(r['datetime']).isoformat(), 'value': r['value']} for r in rt_current]
    df_current = pd.DataFrame(df_current_list)
    
    # Fetch previous period data
    monitor_prev = fetch_main_monitor_data(int(prev_start_dt.timestamp()), int(prev_end_dt.timestamp()))
    rt_prev = fetch_chunked_response_times(prev_start_dt, prev_end_dt)
    structured_prev = parse_monitor_data(monitor_prev, rt_prev)
    df_prev_list = [{'datetime': datetime.fromtimestamp(r['datetime']).isoformat(), 'value': r['value']} for r in rt_prev]
    df_prev = pd.DataFrame(df_prev_list)
    
    # Conditional check for empty DataFrames to trigger retry
    if df_current.empty or df_prev.empty:
        # Log an error message with which DataFrame is empty, mention which one is empty
        logging.error("One or more DataFrames are empty: "
                      f"df_current empty: {df_current.empty}, "
                      f"df_prev_week empty: {df_prev.empty}. Triggering retry...")
        
        raise ValueError("One or more DataFrames (df_current or df_prev) are empty. Triggering retry...")

    # Push all data to XCom
    ti.xcom_push(key="structured_current", value=json.dumps(structured_current))
    ti.xcom_push(key="df_current", value=df_current.to_json(orient='records', date_format='iso'))
    ti.xcom_push(key="logs", value=json.dumps(logs))
    ti.xcom_push(key="structured_prev", value=json.dumps(structured_prev))
    ti.xcom_push(key="df_prev", value=df_prev.to_json(orient='records', date_format='iso'))
    ti.xcom_push(key="report_month_start_dt", value=current_start_dt.strftime('%Y-%m-%d'))
    ti.xcom_push(key="report_month_end_dt", value=current_end_dt.strftime('%Y-%m-%d'))

    logging.info("Monthly data fetch completed.")
    return structured_current

def step_2a_anomaly_detection(ti, **context):
    structured_current_str = ti.xcom_pull(key="structured_current")
    structured_current = json.loads(structured_current_str)
    df_current_json = ti.xcom_pull(key="df_current")
    df_current = pd.read_json(io.StringIO(df_current_json), orient='records')
    logs_json = ti.xcom_pull(key="logs")
    logs = json.loads(logs_json)
    
    structured_prev_str = ti.xcom_pull(key="structured_prev")
    structured_prev = json.loads(structured_prev_str)
    df_prev_json = ti.xcom_pull(key="df_prev")
    df_prev = pd.read_json(io.StringIO(df_prev_json), orient='records')
    
    current_data_str = json.dumps({
        "structured": structured_current,
        "response_times": (df_current.astype({'datetime': 'str'}) if 'datetime' in df_current.columns else df_current).to_dict('records'),
        "logs": logs
    })
    prev_data_str = json.dumps({
        "structured": structured_prev,
        "response_times": (df_prev.astype({'datetime': 'str'}) if 'datetime' in df_prev.columns else df_prev).to_dict('records')
    })
    
    prompt = f"""
Analyze the following uptime and response time data for the monitor over the current 30-day period and previous 30-day period to detect anomalies. Always provide a concise summary in 1-3 sentences, starting with key findings (e.g., spikes or patterns), including percentage changes where applicable, and ending with an overall assessment. Use exact phrasing for missing data (e.g., "No data available for [period]").

Current 30-day period data: {current_data_str}
Previous 30-day period data: {prev_data_str}

Logic for analysis (follow steps in order):
1. Edge cases (Check first): If the 'current 30-day period data' (specifically 'response_times' or 'structured' data) is empty or missing, stop analysis and return 'No current data available for anomaly detection.'. If 'previous 30-day period data' is missing, note this (e.g., "No baseline for comparison.") but proceed with the current period's analysis.
2. Identify spikes: (Only if current data exists) Scan current 30-day period's response_times for values >100ms; count them, describe top 3 by time (use datetime).
3. Detect unusual patterns: (Only if current data exists) Compute distributions—min/max/avg from response_times['value'] (or structured['response_time']['avg'] if available). If previous period data is available, compare current vs previous: Flag if current avg > previous avg by 10%+; include exact % change.
4. Scan logs: (Only if current data exists) For down events (type=1), check frequency (e.g., >2 in 1 hour = cluster) and reasons (e.g., code 500=server error, 408=timeout, 0=unknown); note unusual if not in previous 30-day period.
5. Overall: (Only if current data exists) If no spikes, <10% avg change, and no clusters/unusual logs, conclude 'No significant anomalies detected.'.

Return ONLY a single JSON object with the key "anomaly_detection" and its value as a concise summary string (1-3 sentences). Do not include any additional text, explanations, or markdown.
Example 1 (Normal): {{"anomaly_detection": "Detected 2 spikes >100ms and one down log cluster (2 events, code 408 timeout); however, the average response time improved, dropping 9.12% (from 82.2ms to 74.7ms) compared to the previous 30-day period."}}
Example 2 (Spikes and Patterns): {{"anomaly_detection": "Detected multiple spikes (>100ms) in the past month, with the highest at 2025-11-10 11:00:00 (844ms) and others at 2025-11-10 07:00:00 (244ms) and 2025-11-10 09:00:00 (198ms). Average response time increased significantly by 112% (157.34ms) compared to the previous month (74.17ms). Significant anomaly detected regarding response time."}}
Example 3 (Edge Case): {{"anomaly_detection": "No current data available for anomaly detection."}}
"""
    
    response = get_ai_response(prompt)
    cleaned_response = re.sub(r'```json\n?|```\n?', '', response, flags=re.DOTALL).strip()
    try:
        analysis_part = json.loads(cleaned_response)
        ti.xcom_push(key="anomaly_detection", value=analysis_part.get("anomaly_detection", "N/A"))
    except json.JSONDecodeError:
        ti.xcom_push(key="anomaly_detection", value="Error parsing AI response")
    logging.info("Anomaly detection completed.")
    return ti.xcom_pull(key="anomaly_detection")

def step_2b_rca(ti, **context):
    structured_current_str = ti.xcom_pull(key="structured_current")
    structured_current = json.loads(structured_current_str)
    df_current_json = ti.xcom_pull(key="df_current")
    df_current = pd.read_json(io.StringIO(df_current_json), orient='records')
    logs_json = ti.xcom_pull(key="logs")
    logs = json.loads(logs_json)
    
    structured_prev_str = ti.xcom_pull(key="structured_prev")
    structured_prev = json.loads(structured_prev_str)
    df_prev_json = ti.xcom_pull(key="df_prev")
    df_prev = pd.read_json(io.StringIO(df_prev_json), orient='records')

    current_data_str = json.dumps({
        "structured": structured_current,
        "response_times": (df_current.astype({'datetime': 'str'}) if 'datetime' in df_current.columns else df_current).to_dict('records'),
        "logs": logs
    })
    prev_data_str = json.dumps({
        "structured": structured_prev,
        "response_times": (df_prev.astype({'datetime': 'str'}) if 'datetime' in df_prev.columns else df_prev).to_dict('records')
    })
    
    prompt = f"""
Analyze the following uptime and response time data for the monitor over the current 30-day period and previous 30-day period for root cause analysis (RCA) of errors. Always provide a concise summary in bulleted format: Use bullets for each error (format: '- [Time]: [Reason] (duration [X]s) - [Root cause inference]; [Recommendation].'), or a single bullet '- No errors requiring RCA.' if none. Do not mention missing data unless it directly impacts analysis.

Current 30-day period data: {current_data_str}
Previous 30-day period data: {prev_data_str}

Logic for analysis (follow steps in order):
1. Extract down logs: Filter current 30-day period's logs where type=1; sort by datetime descending. If none, output single bullet '- No errors requiring RCA.' and stop.
2. For each: Analyze reason (code/detail: e.g., 500=server error → overload; 404=not found → config issue; 408=timeout → network; 0=unknown → investigate API).
3. Infer root cause: Correlate with preceding response_times (e.g., if avg >200ms in 30min before down → overload; check if similar in previous 30-day period for recurrence (count matching reasons >1)).
4. Recommendations: Tailor per cause (e.g., overload: 'Scale resources'; timeout: 'Check network latency'; config: 'Verify endpoints'). Limit to 1-2 actionable steps.
5. Edge cases: If logs empty, treat as no errors (single bullet); if previous missing, skip recurrence without noting.
6. Overall: If multiple, add final bullet summarizing common causes.

Return ONLY a single JSON object with the key "rca" and its value as a concise summary string (bulleted points). Do not include any additional text, explanations, or markdown. Example: {{"rca": "- 2025-10-17 10:00:00: Code 500 server error (duration 120s) - Root cause: Overload inferred from preceding high response times (avg 250ms); recurred from previous 30-day period (2 similar). Recommendation: Add autoscaling and monitor CPU usage.\\n- Common cause: Server overload - Implement load balancing."}} or {{"rca": "- No errors requiring RCA."}}
"""
    
    response = get_ai_response(prompt)
    cleaned_response = re.sub(r'```json\n?|```\n?', '', response, flags=re.DOTALL).strip()
    try:
        analysis_part = json.loads(cleaned_response)
        ti.xcom_push(key="rca", value=analysis_part.get("rca", "N/A"))
    except json.JSONDecodeError:
        ti.xcom_push(key="rca", value="Error parsing AI response")
    logging.info("RCA completed.")
    return ti.xcom_pull(key="rca")

def step_2c_comparative_analysis(ti, **context):
    structured_current_str = ti.xcom_pull(key="structured_current")
    structured_current = json.loads(structured_current_str)
    df_current_json = ti.xcom_pull(key="df_current")
    df_current = pd.read_json(io.StringIO(df_current_json), orient='records')
    logs_json = ti.xcom_pull(key="logs")
    logs = json.loads(logs_json)
    
    structured_prev_str = ti.xcom_pull(key="structured_prev")
    structured_prev = json.loads(structured_prev_str)
    df_prev_json = ti.xcom_pull(key="df_prev")
    df_prev = pd.read_json(io.StringIO(df_prev_json), orient='records')

    current_data_str = json.dumps({
        "structured": structured_current,
        "response_times": (df_current.astype({'datetime': 'str'}) if 'datetime' in df_current.columns else df_current).to_dict('records'),
        "logs": logs
    })
    prev_data_str = json.dumps({
        "structured": structured_prev,
        "response_times": (df_prev.astype({'datetime': 'str'}) if 'datetime' in df_prev.columns else df_prev).to_dict('records')
    })
    
    prompt = f"""
Analyze the following uptime and response time data for the monitor over the current 30-day period and previous 30-day period for comparative analysis. Always provide a concise summary in bullet points (one per metric, format: '- [Metric] month-over-month: [change value] ([direction: improvement/degradation/no change] from [prev] to [current]).'), using exact phrasing for missing data or baselines (e.g., "No data available for [period]" or "N/A - no baseline").

Current 30-day period data: {current_data_str}
Previous 30-day period data: {prev_data_str}

Logic for analysis (follow steps in order):
1. Edge cases (Check first): If the 'current 30-day period data' (specifically 'structured' data or 'response_times') is empty or missing, stop analysis and return 'No current data available for comparative analysis.'.
2. Extract metrics: (Only if current data exists) Uptime = structured['uptime_status']['uptime_last_30days']; Avg response = structured['response_time']['avg'] or mean(response_times['value']); Errors = structured['uptime_status']['errors_last_30days'].
3. Calculate changes: For each metric, calculate the change. Uptime % = ((current - prev) / prev * 100) if prev >0; Avg response delta = current - prev (ms); Errors delta = current - prev.
4. Highlight direction: Uptime >0 = 'improvement'; <0 = 'degradation'; =0 = 'no change'. For response/errors: <0 = 'improvement'; >0 = 'degradation'; =0 = 'no change'.
5. Handle Missing Baselines: If 'previous 30-day period data' is missing for a specific metric, output: '- [Metric] month-over-month: N/A - no baseline.' Round % to 2 decimals, deltas to 1 decimal.
6. Order bullets: Always return all 3 bullets in this order: 
- Uptime month-over-month
- Avg response month-over-month
- Errors month-over-month

Return ONLY a single JSON object with the key "comparative_analysis" and its value as a concise summary string (bullet points, one per metric, or the single edge case string). Do not include any additional text, explanations, or markdown.
Example 1 (Normal): {{"comparative_analysis": "- Uptime month-over-month: +0.0% (no change from 100.0% to 100.0%).\\n- Avg response month-over-month: -7.5ms (improvement from 82.2ms to 74.7ms).\\n- Errors month-over-month: +0 (no change from 0 to 0)."}}
Example 2 (Edge Case): {{"comparative_analysis": "No current data available for comparative analysis."}}
"""
    
    response = get_ai_response(prompt)
    cleaned_response = re.sub(r'```json\n?|```\n?', '', response, flags=re.DOTALL).strip()
    try:
        analysis_part = json.loads(cleaned_response)
        ti.xcom_push(key="comparative_analysis", value=analysis_part.get("comparative_analysis", "N/A"))
    except json.JSONDecodeError:
        ti.xcom_push(key="comparative_analysis", value="Error parsing AI response")
    logging.info("Comparative analysis completed.")
    return ti.xcom_pull(key="comparative_analysis")

def step_2f_combine_analysis(ti, **context):
    # Combine all analysis parts into one dict
    anomaly_detection = ti.xcom_pull(key="anomaly_detection")
    rca = ti.xcom_pull(key="rca")
    comparative_analysis = ti.xcom_pull(key="comparative_analysis")
    
    analysis_json = {
        "anomaly_detection": anomaly_detection,
        "rca": rca,
        "comparative_analysis": comparative_analysis
    }

    flag_prompt = f"""
Analyze the following AI-generated analysis contents to determine boolean flags for alerting:

Contents: {analysis_json}

Logic for flags (follow strictly):
- has_anomalies (for anomaly_detection): true if the content mentions spikes (>100ms), unusual patterns, >10% average response time increase, clusters, or unusual logs; false if 'No significant anomalies detected' or equivalent, or if no issues noted (e.g., minor/isolated changes <10%).
- has_errors (for rca): true if the content describes any errors (e.g., down events, timeouts, errors) with root causes or recommendations; false if 'No errors requiring RCA' or equivalent.
- has_degradation (for comparative_analysis): true if the content mentions any 'degradation' in metrics (e.g., lower uptime, higher response time, more errors) vs baselines; false if all 'improvement', 'no change', or 'N/A'.

Return ONLY a single JSON object with keys "has_anomalies" (boolean), "has_errors" (boolean), and "has_degradation" (boolean). No additional text.
Example: {{"has_anomalies": true, "has_errors": false, "has_degradation": true}}
    """

    flag_response = get_ai_response(flag_prompt)
    cleaned_flag_response = re.sub(r'```json\n?|```\n?', '', flag_response, flags=re.DOTALL).strip()
    try:
        flags = json.loads(cleaned_flag_response)
        ti.xcom_push(key="analysis_flags", value=json.dumps(flags))
    except json.JSONDecodeError:
        default_flags = {"has_anomalies": False, "has_errors": False, "has_degradation": False}
        ti.xcom_push(key="analysis_flags", value=json.dumps(default_flags))
        logging.error("Error parsing AI flag response; using defaults.")
    
    ti.xcom_push(key="analysis", value=json.dumps(analysis_json))
    logging.info("Analysis combination completed.")
    return analysis_json

def step_3_generate_plot(ti, **context):
    try:
        structured_str = ti.xcom_pull(key="structured_current")
        structured = json.loads(structured_str)
        monitor_name = structured.get("monitor_information", {}).get("monitor_name", "Default Monitor")
        report_month_start_dt = ti.xcom_pull(key="report_month_start_dt")
        report_month_end_dt = ti.xcom_pull(key="report_month_end_dt")
        
        df_current_json = ti.xcom_pull(key="df_current")
        df_current = pd.read_json(io.StringIO(df_current_json), orient='records')
        if 'datetime' in df_current.columns:
            df_current['datetime'] = pd.to_datetime(df_current['datetime'])
            df_current = df_current.sort_values('datetime')
        
        df_prev_json = ti.xcom_pull(key="df_prev")
        df_prev = pd.read_json(io.StringIO(df_prev_json), orient='records')
        if 'datetime' in df_prev.columns:
            df_prev['datetime'] = pd.to_datetime(df_prev['datetime'])
        
        # Compute previous month average safely
        prev_avg = df_prev['value'].mean() if 'value' in df_prev.columns and not df_prev.empty else 0
        
        fig, ax = plt.subplots(figsize=(15, 6), dpi=120)
        ax.set_facecolor('white')
        fig.set_facecolor('white')
        
        if not df_current.empty and 'datetime' in df_current.columns and 'value' in df_current.columns:
            ax.plot(df_current['datetime'], df_current['value'], color='#1e275d', linewidth=1.5, label='This Month\'s Response Time')
        
        ax.axhline(y=prev_avg, color='#ff52ff', linestyle='--', label=f'Previous Month Avg ({prev_avg:.2f}ms)')
        
        if 'value' in df_current.columns:
            high_current = df_current[df_current['value'] > 100] # Highlight threshold
            if not high_current.empty:
                ax.scatter(high_current['datetime'], high_current['value'], color='#fb47de', s=80, label='High Response (>100ms)', zorder=5)
        
        ax.grid(True, linestyle='--', alpha=0.2, color='black')
        ax.tick_params(axis='x', colors='black', which='major', labelsize=10)
        ax.tick_params(axis='y', colors='black')
        ax.xaxis.label.set_color('black')
        ax.yaxis.label.set_color('black')
        
        # Day labels for x-axis (e.g., every 3 days)
        ax.xaxis.set_major_locator(DayLocator(interval=3))
        ax.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d'))
        ax.xaxis.set_minor_locator(DayLocator(interval=1))
        
        fig.autofmt_xdate(rotation=45)
        
        if not df_current.empty and 'value' in df_current.columns:
            max_val = df_current['value'].max()
            ax.set_ylim(bottom=0, top=max(100, max_val * 1.2))
        else:
             ax.set_ylim(bottom=0, top=100)

        ax.set_title(
            f"Response Time: Month-over-Month Comparison for {monitor_name} for {report_month_start_dt} to {report_month_end_dt}",
            fontsize=18, fontweight='bold', color='black'
        )
        ax.set_xlabel("Datetime", fontsize=14)
        ax.set_ylabel("Response Time (ms)", fontsize=14)
        
        legend = ax.legend()
        plt.setp(legend.get_texts(), color='black')
        
        fig.tight_layout()
        
        buf = io.BytesIO()
        fig.savefig(buf, format='png', bbox_inches='tight', facecolor=fig.get_facecolor())
        buf.seek(0)
        img_b64 = base64.b64encode(buf.read()).decode('utf-8')
        plt.close(fig)
        img_html = (
            f'<img src="cid:response_chart" alt="Response Time Chart" '
            'style="max-width:100%;height:auto;border-radius:8px;" />'
        )
        ti.xcom_push(key="chart_b64", value=img_b64)
        ti.xcom_push(key="chart_html", value=img_html)
        return img_html
    
    except Exception as e:
        logging.error(f"Error generating plot: {str(e)}", exc_info=True)
        error_html = '<p style="color:#dc3545;font-weight:bold;">Could not generate chart.</p>'
        ti.xcom_push(key="chart_b64", value=None)
        ti.xcom_push(key="chart_html", value=error_html)
        return error_html

def step_4_compose_email(ti, **context):
    """
    Composes the hardcoded MONTHLY uptime report with professional styling.
    """
    
    # 1. Hardcode Report Type
    report_type = "Monthly"

    # 2. Pull data from XCom
    try:
        structured_str = ti.xcom_pull(key="structured_current")
        structured = json.loads(structured_str)
        report_month_start_dt = ti.xcom_pull(key="report_month_start_dt", default="N/A")
        report_month_end_dt = ti.xcom_pull(key="report_month_end_dt", default="N/A")
        
        analysis_str = ti.xcom_pull(key="analysis")
        analysis = json.loads(analysis_str)

        analysis_flags_str = ti.xcom_pull(key="analysis_flags")
        analysis_flags = json.loads(analysis_flags_str) if analysis_flags_str else {"has_anomalies": False, "has_errors": False, "has_degradation": False}
        
        logs_json = ti.xcom_pull(key="logs")
        logs = json.loads(logs_json)
        
        chart_html = ti.xcom_pull(key="chart_html", default='<p style="color: #888;">Chart data is unavailable for this period.</p>')
    
    except (TypeError, json.JSONDecodeError) as e:
        logging.error(f"Failed to parse XCom JSON data: {e}")
        # Set defaults to prevent downstream errors
        structured = {}
        analysis = {}
        logs = []
        report_month_start_dt = "N/A"
        report_month_end_dt = "N/A"
        chart_html = '<p style="color: #dc3545; font-weight: bold;">Failed to load report data.</p>'

    # 3. Define Embedded CSS Styles
    css_styles = """
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f4f7f6;
            color: #333;
        }
        .container {
            max-width: 800px;
            margin: 0 auto;
            background-color: #ffffff;
            border: 1px solid #e0e0e0;
            border-radius: 8px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.05);
            overflow: hidden;
        }
        .header {
            padding: 24px 30px;
            background-color: #1e275d;
            color: #ffffff;
        }
        .header h1 {
            margin: 0;
            font-size: 24px;
        }
        .content {
            padding: 30px;
        }
        .section {
            margin-bottom: 30px;
        }
        .section h2 {
            font-size: 20px;
            margin-top: 0;
            margin-bottom: 15px;
            color: #004a99;
            border-bottom: 2px solid #f0f0f0;
            padding-bottom: 5px;
        }
        .info-table {
            width: 100%;
            border-collapse: collapse;
        }
        .info-table tr td {
            padding: 12px 0;
            border-bottom: 1px solid #eee;
            font-size: 14px;
            vertical-align: top;
        }
        .info-table tr:last-child td {
            border-bottom: none;
        }
        .info-table tr td:first-child {
            font-weight: 600;
            color: #555;
            width: 30%;
        }
        .logs-table {
            width: 100%;
            border-collapse: collapse;
            font-size: 13px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        }
        .logs-table th, .logs-table td {
            padding: 10px 12px;
            border: 1px solid #ddd;
            text-align: left;
        }
        .logs-table th {
            background-color: #f9f9f9;
            font-weight: 600;
        }
        .logs-table tr:nth-child(even) {
            background-color: #fdfdfd;
        }
        .status-up {
            color: #28a745;
            font-weight: bold;
        }
        .status-down {
            color: #dc3545;
            font-weight: bold;
        }
        .ai-section {
            margin-top: 15px;
            padding: 20px;
            background-color: #fcfdff;
            border: 1px solid #e0eafc;
            border-radius: 5px;
        }
        .ai-section.alert-section {
            border-left: 4px solid #dc3545;
            background-color: #f8d7da33;
        }
        .ai-section h3 {
            margin-top: 0;
            margin-bottom: 10px;
            color: #004a99;
            font-size: 16px;
        }
        .ai-section p, .ai-section ul {
            font-size: 14px;
            line-height: 1.6;
            margin: 0;
            white-space: pre-wrap;
        }
        .ai-section ul {
            padding-left: 20px;
        }
        .chart-container {
            text-align: center;
            margin-top: 20px;
            background-color: #fcfcfc;
            border-radius: 5px;
        }
        .footer {
            padding: 30px;
            text-align: left;
            font-size: 14px;
            color: #fcfcfc;
            background-color: #1e275d;
            border-top: 1px solid #eee;
        }
    </style>
    """

    # 4. Build HTML Body
    monitor_info = structured.get('monitor_information', {})
    uptime = structured.get('uptime_status', {})
    ssl_info = structured.get('ssl_information', {})
    rt = structured.get('response_time', {})
    notifications = structured.get('notifications', {})
    
    monitor_name = monitor_info.get('monitor_name', 'N/A')
    monitor_id = monitor_info.get('monitor_id', 'N/A')

    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>{report_type} Uptime Report</title>
        {css_styles}
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>{report_type} Uptime Report - {report_month_start_dt} to {report_month_end_dt}</h1>
            </div>
            <div class="content">
                <p>Dear Team,</p>
                <p>Please find below the {report_type.lower()} uptime report for the date <strong>{report_month_start_dt}</strong> to <strong>{report_month_end_dt}</strong> for the monitor: <strong>{monitor_name}</strong>.</p>
                
                <div class="section">
                    <h2>Monitor Information</h2>
                    <table class="info-table">
                        <tr>
                            <td>Monitor Name</td>
                            <td>{monitor_name}</td>
                        </tr>
                        <tr>
                            <td>Monitor ID</td>
                            <td>{monitor_id}</td>
                        </tr>
                    </table>
                </div>
                
                <div class="section">
                    <h2>Uptime Status (Last 30 Days)</h2>
                    <table class="info-table">
                        <tr>
                            <td>Overall Status</td>
                            <td><span class="{ 'status-up' if uptime.get('status') == 'Up' else 'status-down' }">{uptime.get('status', 'N/A')}</span></td>
                        </tr>
                        <tr>
                            <td>Uptime (Last 30d)</td>
                            <td>{uptime.get('uptime_last_30days', 'N/A')}</td>
                        </tr>
                        <tr>
                            <td>Total Downtime (Last 30d)</td>
                            <td>{uptime.get('downtime_last_30days', 'N/A')}</td>
                        </tr>
                        <tr>
                            <td>Errors (Last 30d)</td>
                            <td>{uptime.get('errors_last_30days', 'N/A')}</td>
                        </tr>
                    </table>
                </div>
                
                <div class="section">
                    <h2>SSL Information</h2>
                    <table class="info-table">
                        <tr>
                            <td>Issuer/Brand</td>
                            <td>{ssl_info.get('brand', 'N/A')}</td>
                        </tr>
                        <tr>
                            <td>Expires On</td>
                            <td>{ssl_info.get('expiry_date', 'N/A')}</td>
                        </tr>
                    </table>
                </div>
                
                <div class="section">
                    <h2>Response Time (Last 30 Days)</h2>
                    <div class="chart-container">
                        {chart_html}
                    </div>
                    <table class="info-table" style="margin-top: 20px;">
                        <tr>
                            <td>Average (Last 30d)</td>
                            <td>{rt.get('avg', 'N/A')} ms</td>
                        </tr>
                        <tr>
                            <td>Min (Last 30d)</td>
                            <td>{rt.get('min', 'N/A')} ms</td>
                        </tr>
                        <tr>
                            <td>Max (Last 30d)</td>
                            <td>{rt.get('max', 'N/A')} ms</td>
                        </tr>
                    </table>
                </div>
                
                <div class="section">
                    <h2>Notifications</h2>
                    <table class="info-table">
                        <tr>
                            <td>Alert Contacts</td>
                            <td>{notifications.get('to_be_notified', 'N/A')}</td>
                        </tr>
                    </table>
                </div>
                
                <div class="section">
                    <h2>Logs Summary (Last 10)</h2>
    """

    if logs:
        log_table = """
                    <table class="logs-table">
                        <thead>
                            <tr>
                                <th>Timestamp</th>
                                <th>Type</th>
                                <th>Duration (s)</th>
                                <th>Reason</th>
                            </tr>
                        </thead>
                        <tbody>
        """
        for log in logs[-10:]: # Show only the last 10 logs for brevity
            try:
                dt = datetime.fromtimestamp(log.get('datetime', 0)).strftime('%Y-%m-%d %H:%M:%S')
            except:
                dt = "Invalid Date"
            typ_val = log.get('type')
            typ_class = 'status-down' if typ_val == 1 else 'status-up'
            typ_text = 'Down' if typ_val == 1 else 'Up'
            dur = log.get('duration', 0)
            reason_dict = log.get('reason', {})
            reason = f"{reason_dict.get('code', 'N/A')} - {reason_dict.get('detail', 'N/A')}"
            
            log_table += f"""
                            <tr>
                                <td>{dt}</td>
                                <td><span class="{typ_class}">{typ_text}</span></td>
                                <td>{dur}</td>
                                <td>{reason}</td>
                            </tr>
            """
        log_table += '</tbody></table>'
        html += log_table
    else:
        html += '<p>No logs available for this period.</p>'
    html += '</div>'  # close section

    # AI Analysis Sections
    html += """
                <div class="section">
                    <h2>AI Analysis</h2>
    """
    
    sections = [
    ("Anomaly Detection (Response Time)", analysis.get("anomaly_detection", "N/A"), "has_anomalies"),
    ("Root Cause Analysis", analysis.get("rca", "N/A"), "has_errors"),
    ("Comparative Analysis", analysis.get("comparative_analysis", "N/A"), "has_degradation")
    ]
    
    has_ai_content = False
    for title, content, flag_key in sections:
        content_str = str(content)
        if content and content_str != "N/A" and content_str != "Error parsing AI response":
            has_ai_content = True

            class_add = "alert-section" if analysis_flags.get(flag_key, False) else ""
            
            # Format as bullets if content contains newlines or hyphens
            if '\n' in content_str or content_str.strip().startswith('-'):
                 # Split by newline, strip, remove empty lines, and wrap in <li>
                bullets = [f"<li>{line.strip().lstrip('- ')}</li>" for line in content_str.split('\n') if line.strip()]
                content_html = f"<ul>{''.join(bullets)}</ul>"
            else:
                content_html = f"<p>{content_str}</p>"
                
            html += f"""
                    <div class="ai-section" {class_add}>
                        <h3>{title}</h3>
                        {content_html}
                    </div>
            """
    
    if not has_ai_content:
        html += "<p>No AI analysis is available for this period.</p>"
        
    html += '</div>'  # close section
        
    # Footer
    html += """
            </div>
            <div class="footer">
                Best regards,<br>
                The Monitoring Team
                <center><span style="font-size: 14px; opacity: 0.9;">Powered by lowtouch<span style="color: #fb47de;">.ai</span></span></center>
            </div>
        </div>
    </body>
    </html>
    """
    
    # 5. Push final HTML to XCom
    ti.xcom_push(key="final_html_content", value=html)
    logging.info("Email composition completed.")
    return html

def step_5_send_report_email(ti, **context):
    try:
        structured_str = ti.xcom_pull(key="structured_current")
        structured = json.loads(structured_str)
        monitor_name = structured.get("monitor_information", {}).get("monitor_name", "Default Monitor")
        
        final_html_content = ti.xcom_pull(key="final_html_content")
        if not final_html_content:
            logging.error("No final HTML content found from previous steps")
            return "Error: No content to send"
        
        chart_b64 = ti.xcom_pull(key="chart_b64")
        if not chart_b64:
            logging.warning("No chart data found, sending email without image.")
                
        subject = f"Monthly Uptime Report with Insights for {monitor_name}"
        
        result = send_email(
            RECIPIENT_EMAIL, subject, final_html_content, img_b64=chart_b64
        )
        
        if result:
            logging.info(f"Report email sent successfully to {RECIPIENT_EMAIL}")
            return f"Email sent successfully to {RECIPIENT_EMAIL}"
        else:
            logging.error("Failed to send report email")
            return "Failed to send email"
            
    except Exception as e:
        logging.error(f"Error in send_report_email: {str(e)}")
        return f"Error sending email: {str(e)}"

# Read README if available (adapt as needed)
readme_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'uptime_monthly_report.md')
readme_content = ""
try:
    with open(readme_path, 'r') as file:
        readme_content = file.read()
except FileNotFoundError:
    readme_content = "Monthly uptime report generation and email DAG with AI insights"

with DAG(
    "uptime_monthly_data_report", 
    default_args=default_args, 
    schedule_interval="@monthly", 
    catchup=False, 
    doc_md=readme_content, 
    tags=["uptime", "report", "monthly", "ai-insights"]
) as dag:
    
    fetch_data_task = PythonOperator(
        task_id="step_1_fetch_data",
        python_callable=step_1_fetch_data,
        provide_context=True,
        retries=3,
        retry_delay=timedelta(minutes=15)
    )
    
    anomaly_detection_task = PythonOperator(
        task_id="step_2a_anomaly_detection",
        python_callable=step_2a_anomaly_detection,
        provide_context=True
    )
    
    rca_task = PythonOperator(
        task_id="step_2b_rca",
        python_callable=step_2b_rca,
        provide_context=True
    )
    
    comparative_analysis_task = PythonOperator(
        task_id="step_2c_comparative_analysis",
        python_callable=step_2c_comparative_analysis,
        provide_context=True
    )
    
    combine_analysis_task = PythonOperator(
        task_id="step_2f_combine_analysis",
        python_callable=step_2f_combine_analysis,
        provide_context=True
    )
    
    generate_plot_task = PythonOperator(
        task_id="step_3_generate_plot",
        python_callable=step_3_generate_plot,
        provide_context=True
    )
    
    compose_email_task = PythonOperator(
        task_id="step_4_compose_email",
        python_callable=step_4_compose_email,
        provide_context=True
    )
    
    send_report_email_task = PythonOperator(
        task_id="step_5_send_report_email",
        python_callable=step_5_send_report_email,
        provide_context=True
    )
    
    fetch_data_task >> anomaly_detection_task >> rca_task >> comparative_analysis_task >> combine_analysis_task >> generate_plot_task >> compose_email_task >> send_report_email_task