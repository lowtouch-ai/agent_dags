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
from matplotlib.dates import HourLocator, DateFormatter, DayLocator
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
REPORT_PERIOD = "last 7 days"

def get_ai_response(prompt, conversation_history=None):
    """Get AI response with conversation history context"""
    try:
        logging.debug(f"Query received: {prompt}")
        
        # Validate input
        if not prompt or not isinstance(prompt, str):
            return "Invalid input provided. Please enter a valid query."

        client = Client(host=OLLAMA_HOST, headers={'x-ltai-client': 'uptime-agent'})
        logging.debug(f"Connecting to Ollama at {OLLAMA_HOST} with model 'uptime_agent:0.3'")

        # Build messages array with conversation history
        messages = []
        if conversation_history:
            for history_item in conversation_history:
                messages.append({"role": "user", "content": history_item["prompt"]})
                messages.append({"role": "assistant", "content": history_item["response"]})
        
        # Add current prompt
        messages.append({"role": "user", "content": prompt})

        response = client.chat(
            model='uptime_agent:0.3',
            messages=messages,
            stream=False
        )
        logging.info(f"Raw response from agent: {str(response)[:500]}...")

        # Extract content
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

def fetch_monitor_data(start_ts, end_ts):
    url = "https://api.uptimerobot.com/v2/getMonitors"
    payload = {
        'api_key': UPTIME_API_KEY,
        'format': 'json',
        'logs': '1',
        'logs_limit': '500',
        'response_times': '1',
        'custom_uptime_ratios': '1-7-30-365',
        'ssl': '1',
        'alert_contacts': '1',
        'mwindows': '1',
        'response_times_average': '30',
        'monitors': MONITOR_ID,
        'logs_start_date': str(int(start_ts)),
        'logs_end_date': str(int(end_ts)),
        'response_times_start_date': str(int(start_ts)),
        'response_times_end_date': str(int(end_ts)),
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

def parse_monitor_data(monitor):
    # Filter non-empty parts to avoid ValueError on float('')
    custom_uptime = [x.strip() for x in monitor.get('custom_uptime_ratio', '').split('-') if x.strip()]
    custom_down = [x.strip() for x in monitor.get('custom_down_durations', '').split('-') if x.strip()]
    # Use index [1] for 7-day data (indices: [0]=1d, [1]=7d, [2]=30d, [3]=365d)
    uptime_7d = f"{float(custom_uptime[1]):.2f}%" if len(custom_uptime) > 1 else "N/A"
    down_sec_7d = int(custom_down[1]) if len(custom_down) > 1 else 0
    downtime_7d = f"{down_sec_7d / 60:.1f} minutes" if down_sec_7d > 0 else "0 minutes"
    logs = monitor.get('logs', [])
    incidents = sum(1 for log in logs if log.get('type') == 1)
    status_map = {0: 'Paused', 1: 'Down', 2: 'Up', 9: 'Pending'}
    status = status_map.get(monitor.get('status', 0), 'Unknown')
    ssl = monitor.get('ssl', {})
    brand = ssl.get('brand', 'N/A')
    expires = ssl.get('expires', 0)
    expiry_date = datetime.fromtimestamp(expires).strftime('%Y-%m-%d') if expires else 'N/A'
    rt_list = monitor.get('response_times', [])
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
            "uptime_last_7days": uptime_7d,
            "downtime_last_7days": downtime_7d,
            "incidents_last_7days": str(incidents)
        },
        "ssl_information": {"brand": brand, "expiry_date": expiry_date},
        "response_time": {"min": f"{min_rt:.2f}", "max": f"{max_rt:.2f}", "avg": f"{avg_rt:.2f}"},
        "notifications": {"to_be_notified": to_be_notified},
        "logs_summary": {"summary": f"Total logs: {len(logs)}. Incidents: {incidents}."}
    }
    return structured, rt_list, logs

def step_1_fetch_data(ti, **context):
    now = datetime.now(timezone.utc)
    today_start_dt = now.replace(hour=0, minute=0, second=0, microsecond=0)

    # Previous Calendar Week (strict Sun 00:00:00 to Sat 23:59:59 UTC)
    days_since_sunday = (today_start_dt.weekday() + 1) % 7  # Days to current Sun 00:00
    current_week_start_dt = today_start_dt - timedelta(days=days_since_sunday)
    report_week_end_dt = current_week_start_dt - timedelta(microseconds=1)  # Prev Sat 23:59:999999
    report_week_start_dt = current_week_start_dt - timedelta(days=7)         # Prev Sun 00:00

    # Baseline: Week before
    prev_week_end_dt = report_week_start_dt - timedelta(microseconds=1)
    prev_week_start_dt = report_week_start_dt - timedelta(days=7)

    report_week_start_ts = int(report_week_start_dt.timestamp())
    report_week_end_ts = int(report_week_end_dt.timestamp())
    prev_week_start_ts = int(prev_week_start_dt.timestamp())
    prev_week_end_ts = int(prev_week_end_dt.timestamp())

    logging.info(f"Fetching: Main Week {report_week_start_ts} ({report_week_start_dt}) to {report_week_end_ts} ({report_week_end_dt}), "
                 f"Baseline {prev_week_start_ts} ({prev_week_start_dt}) to {prev_week_end_ts} ({prev_week_end_dt}), ")
    
    # Fetch + Filter Main
    monitor = fetch_monitor_data(report_week_start_ts, report_week_end_ts)
    logging.info(f"Fetched monitor data: {monitor}")
    filtered_rt = [r for r in monitor.get('response_times', []) if report_week_start_ts <= r.get('datetime', 0) <= report_week_end_ts]
    filtered_logs = [l for l in monitor.get('logs', []) if report_week_start_ts <= l.get('datetime', 0) <= report_week_end_ts]
    monitor['response_times'] = filtered_rt  # For parse min/max/avg
    monitor['logs'] = filtered_logs
    logging.info(f"Main: {len(filtered_rt)} RTs, {len(filtered_logs)} logs")
    structured_current, rt_current, logs = parse_monitor_data(monitor)
    df_current = pd.DataFrame([{'datetime': datetime.fromtimestamp(r['datetime']).isoformat(), 'value': r['value']} for r in rt_current])
    
    # Fetch + Filter Baseline
    prev_monitor = fetch_monitor_data(prev_week_start_ts, prev_week_end_ts)
    logging.info(f"Fetched baseline response times: {len(prev_monitor.get('response_times', []))}, logs: {len(prev_monitor.get('logs', []))}")
    filtered_rt_prev = [r for r in prev_monitor.get('response_times', []) if prev_week_start_ts <= r.get('datetime', 0) <= prev_week_end_ts]
    filtered_logs_prev = [l for l in prev_monitor.get('logs', []) if prev_week_start_ts <= l.get('datetime', 0) <= prev_week_end_ts]
    prev_monitor['response_times'] = filtered_rt_prev
    prev_monitor['logs'] = filtered_logs_prev
    logging.info(f"Baseline: {len(filtered_rt_prev)} RTs, {len(filtered_logs_prev)} logs")
    structured_prev, rt_prev, _ = parse_monitor_data(prev_monitor)
    df_prev = pd.DataFrame([{'datetime': datetime.fromtimestamp(r['datetime']).isoformat(), 'value': r['value']} for r in rt_prev])
    
    # XCom push
    ti.xcom_push(key="structured_current", value=json.dumps(structured_current))
    ti.xcom_push(key="df_current", value=df_current.to_json(orient='records', date_format='iso'))
    ti.xcom_push(key="logs", value=json.dumps(logs))
    ti.xcom_push(key="structured_prev", value=json.dumps(structured_prev))
    ti.xcom_push(key="df_prev", value=df_prev.to_json(orient='records', date_format='iso'))
    ti.xcom_push(key="report_week_start_dt", value=report_week_start_dt.strftime('%Y-%m-%d'))
    ti.xcom_push(key="report_week_end_dt", value=report_week_end_dt.strftime('%Y-%m-%d'))

    
    logging.info("Data fetch completed.")
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
    
    # Convert datetime to str to make JSON serializable, but only if column exists (handles empty DataFrames)
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
Analyze the following uptime and response time data for the monitor over the current week and previous week to detect anomalies. Always provide a concise summary in 1-3 sentences, starting with key findings (e.g., spikes or patterns), including percentage changes where applicable, and ending with an overall assessment. Use exact phrasing for missing data (e.g., "No data available for [period]").

Current week data: {current_data_str}
Previous week data: {prev_data_str}

Logic for analysis (follow steps in order):
1. Edge cases (Check first): If the 'current week data' (specifically 'response_times' or 'structured' data) is empty or missing, stop analysis and return 'No current data available for anomaly detection.'. If 'previous week data' is missing, note this (e.g., "No baseline for comparison.") but proceed with the current week's analysis.
2. Identify spikes: (Only if current data exists) Scan current week's response_times for values >100ms; count them, describe top 3 by time (use datetime).
3. Detect unusual patterns: (Only if current data exists) Compute distributions—min/max/avg from response_times['value'] (or structured['avg_response_time'] if available). If previous week data is available, compare current vs previous: Flag if current avg > previous avg by 10%+; include exact % change.
4. Scan logs: (Only if current data exists) For down events (type=1), check frequency (e.g., >2 in 1 hour = cluster) and reasons (e.g., code 500=server error, 408=timeout, 0=unknown); note unusual if not in previous week.
5. Overall: (Only if current data exists) If no spikes, <10% avg change, and no clusters/unusual logs, conclude 'No significant anomalies detected.'.

Return ONLY a single JSON object with the key "anomaly_detection" and its value as a concise summary string (1-3 sentences). Do not include any additional text, explanations, or markdown.
Example 1 (Normal): {{"anomaly_detection": "Detected one down log cluster (2 events, code 408 timeout) and a 9.12% lower average response time (74.7ms) compared to the previous week (82.2ms), but no significant anomalies were found."}}
Example 2 (Edge Case): {{"anomaly_detection": "No current data available for anomaly detection."}}
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
    
    # Convert datetime to str to make JSON serializable, but only if column exists (handles empty DataFrames)
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
Analyze the following uptime and response time data for the monitor over the current week and previous week for root cause analysis (RCA) of incidents. Always provide a concise summary in bulleted format: Use bullets for each incident (format: '- [Time]: [Reason] (duration [X]s) - [Root cause inference]; [Recommendation].'), or a single bullet '- No incidents requiring RCA.' if none. Do not mention missing data unless it directly impacts analysis.

Current week data: {current_data_str}
Previous week data: {prev_data_str}

Logic for analysis (follow steps in order):
1. Extract down logs: Filter current week's logs where type=1; sort by datetime descending. If none, output single bullet '- No incidents requiring RCA.' and stop.
2. For each: Analyze reason (code/detail: e.g., 500=server error → overload; 404=not found → config issue; 408=timeout → network; 0=unknown → investigate API).
3. Infer root cause: Correlate with preceding response_times (e.g., if avg >200ms in 30min before down → overload; check if similar in previous week for recurrence (count matching reasons >1)).
4. Recommendations: Tailor per cause (e.g., overload: 'Scale resources'; timeout: 'Check network latency'; config: 'Verify endpoints'). Limit to 1-2 actionable steps.
5. Edge cases: If logs empty, treat as no incidents (single bullet); if previous missing, skip recurrence without noting.
6. Overall: If multiple, add final bullet summarizing common causes.

Return ONLY a single JSON object with the key "rca" and its value as a concise summary string (bulleted points). Do not include any additional text, explanations, or markdown. Example: {{"rca": "- 2025-10-17 10:00:00: Code 500 server error (duration 120s) - Root cause: Overload inferred from preceding high response times (avg 250ms); recurred from previous week (2 similar). Recommendation: Add autoscaling and monitor CPU usage.\\n- Common cause: Server overload - Implement load balancing."}} or {{"rca": "- No incidents requiring RCA."}}
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
    
    # Convert datetime to str to make JSON serializable, but only if column exists (handles empty DataFrames)
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
Analyze the following uptime and response time data for the monitor over the current week and previous week for comparative analysis. Always provide a concise summary in bullet points (one per metric, format: '- [Metric] week-over-week: [change value] ([direction: improvement/degradation/no change] from [prev] to [current]).'), using exact phrasing for missing data or baselines (e.g., "No data available for [period]" or "N/A - no baseline").

Current week data: {current_data_str}
Previous week data: {prev_data_str}

Logic for analysis (follow steps in order):
1. Edge cases (Check first): If the 'current week data' (specifically 'structured' data or 'response_times') is empty or missing, stop analysis and return 'No current data available for comparative analysis.'.
2. Extract metrics: (Only if current data exists) Uptime = structured['7days']['uptime']; Avg response = structured['avg_response_time'] or mean(response_times['value']); Incidents = len([log for log in logs if log['type']==1]).
3. Calculate changes: For each metric, calculate the change. Uptime % = ((current - prev) / prev * 100) if prev >0; Avg response delta = current - prev (ms); Incidents delta = current - prev.
4. Highlight direction: Uptime >0 = 'improvement'; <0 = 'degradation'; =0 = 'no change'. For response/incidents: <0 = 'improvement'; >0 = 'degradation'; =0 = 'no change'.
5. Handle Missing Baselines: If 'previous week data' is missing for a specific metric, output: '- [Metric] week-over-week: N/A - no baseline.' Round % to 2 decimals, deltas to 1 decimal.
6. Order bullets: Always return all 3 bullets in this order: Uptime, Avg response, Incidents.

Return ONLY a single JSON object with the key "comparative_analysis" and its value as a concise summary string (bullet points, one per metric, or the single edge case string). Do not include any additional text, explanations, or markdown.
Example 1 (Normal): {{"comparative_analysis": "- Uptime week-over-week: +0.0% (no change from 100.0% to 100.0%).\\n- Avg response week-over-week: -7.5ms (improvement from 82.2ms to 74.7ms).\\n- Incidents week-over-week: +0 (no change from 0 to 0)."}}
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
    
    ti.xcom_push(key="analysis", value=json.dumps(analysis_json))
    logging.info("Analysis combination completed.")
    return analysis_json

def step_3_generate_plot(ti, **context):
    try:
        structured_str = ti.xcom_pull(key="structured_current")
        structured = json.loads(structured_str)
        monitor_name = structured.get("monitor_information", {}).get("monitor_name", "Default Monitor")
        report_week_start_dt = ti.xcom_pull(key="report_week_start_dt")
        report_week_end_dt = ti.xcom_pull(key="report_week_end_dt")
        
        df_current_json = ti.xcom_pull(key="df_current")
        df_current = pd.read_json(io.StringIO(df_current_json), orient='records')
        if 'datetime' in df_current.columns:
            df_current['datetime'] = pd.to_datetime(df_current['datetime'])
            df_current = df_current.sort_values('datetime')  # Sort for proper plotting
        
        df_prev_json = ti.xcom_pull(key="df_prev")
        df_prev = pd.read_json(io.StringIO(df_prev_json), orient='records')
        if 'datetime' in df_prev.columns:
            df_prev['datetime'] = pd.to_datetime(df_prev['datetime'])
        
        # Compute previous week average safely
        prev_avg = df_prev['value'].mean() if 'value' in df_prev.columns and not df_prev.empty else 0
        
        fig, ax = plt.subplots(figsize=(15, 6), dpi=120)
        ax.set_facecolor('#1a1a1a')
        fig.set_facecolor('#1a1a1a')
        
        # Plot current week if data available
        if not df_current.empty and 'datetime' in df_current.columns and 'value' in df_current.columns:
            ax.plot(df_current['datetime'], df_current['value'], color='#28a745', linewidth=1.5, label='This Week\'s Response Time')
        
        # Plot dashed previous week average
        ax.axhline(y=prev_avg, color='#6c757d', linestyle='--', label=f'Previous Week Avg ({prev_avg:.2f}ms)')
        
        # Highlight high responses if data available
        if 'value' in df_current.columns:
            high_current = df_current[df_current['value'] > 100]
            if not high_current.empty:
                ax.scatter(high_current['datetime'], high_current['value'], color='#dc3545', s=80, label='High Response (Current)', zorder=5)
        
        ax.grid(True, linestyle='--', alpha=0.2, color='gray')
        ax.tick_params(axis='x', colors='white', which='major', labelsize=10)
        ax.tick_params(axis='y', colors='white')
        ax.xaxis.label.set_color('white')
        ax.yaxis.label.set_color('white')
        
        # Day labels for x-axis
        ax.xaxis.set_major_locator(DayLocator())
        ax.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d'))
        ax.xaxis.set_minor_locator(HourLocator(interval=6))
        
        fig.autofmt_xdate(rotation=45)
        
        if not df_current.empty and 'value' in df_current.columns:
            ax.set_ylim(bottom=0, top=df_current['value'].max() * 1.2)
        
        ax.set_title(
            f"Response Time: Week-over-Week Comparison for {monitor_name} from {report_week_start_dt} to {report_week_end_dt}",
            fontsize=18, fontweight='bold', color='white'
        )
        ax.set_xlabel("Datetime", fontsize=14)
        ax.set_ylabel("Response Time (ms)", fontsize=14)
        
        legend = ax.legend()
        plt.setp(legend.get_texts(), color='white')
        
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
        error_html = '<p style="color:red;font-weight:bold;">Could not generate chart.</p>'
        ti.xcom_push(key="chart_b64", value=None)
        ti.xcom_push(key="chart_html", value=error_html)
        return error_html

def step_4_compose_email(ti, **context):
    """
    Composes the hardcoded WEEKLY uptime report with professional styling.
    
    This version is corrected to match the data structure provided by
    the 'parse_monitor_data' function in the weekly DAG file.
    """
    
    # 1. Hardcode Report Type
    report_type = "Weekly"

    # 2. Pull data from XCom
    try:
        structured_str = ti.xcom_pull(key="structured_current")
        structured = json.loads(structured_str)
        report_week_start_date = ti.xcom_pull(key="report_week_start_dt", default="N/A")
        report_week_end_date = ti.xcom_pull(key="report_week_end_dt", default="N/A")
        
        analysis_str = ti.xcom_pull(key="analysis")
        analysis = json.loads(analysis_str)
        
        logs_json = ti.xcom_pull(key="logs")
        logs = json.loads(logs_json)
        
        chart_html = ti.xcom_pull(key="chart_html", default='<p style="color: #888;">Chart data is unavailable for this period.</p>')
    
    except (TypeError, json.JSONDecodeError) as e:
        logging.error(f"Failed to parse XCom JSON data: {e}")
        # Set defaults to prevent downstream errors
        structured = {}
        analysis = {}
        logs = []
        report_week_start_date = "N/A"
        report_week_end_date = "N/A"
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
            background-color: #004a99; /* Professional blue */
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
        .ai-section p {
            font-size: 14px;
            line-height: 1.6;
            margin: 0;
            white-space: pre-wrap; /* Renders newline characters */
        }
        .ai-bullets {
            list-style-type: disc;
            padding-left: 20px;
            font-size: 14px;
            line-height: 1.6;
            margin: 0;
        }
        .ai-bullets li {
            margin-bottom: 5px;
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
            color: #888;
            background-color: #fcfcfc;
            border-top: 1px solid #eee;
        }
    </style>
    """

    # 4. Build HTML Body
    # Get nested dictionaries safely
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
                <h1>{report_type} Uptime Report - {report_week_start_date} to {report_week_end_date}</h1>
            </div>
            <div class="content">
                <p>Dear Team,</p>
                <p>Please find below the {report_type.lower()} uptime report for the date <strong>{report_week_start_date}</strong> to <strong>{report_week_end_date}</strong> for the monitor: <strong>{monitor_name}</strong>.</p>
                
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
                    <h2>Uptime Status (Last 7 Days)</h2>
                    <table class="info-table">
                        <tr>
                            <td>Overall Status</td>
                            <td><span class="{ 'status-up' if uptime.get('status') == 'Up' else 'status-down' }">{uptime.get('status', 'N/A')}</span></td>
                        </tr>
                        <tr>
                            <td>Uptime (Last 7d)</td>
                            <td>{uptime.get('uptime_last_7days', 'N/A')}</td>
                        </tr>
                        <tr>
                            <td>Total Downtime (Last 7d)</td>
                            <td>{uptime.get('downtime_last_7days', 'N/A')}</td>
                        </tr>
                        <tr>
                            <td>Incidents (Last 7d)</td>
                            <td>{uptime.get('incidents_last_7days', 'N/A')}</td>
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
                    <h2>Response Time (Last 7 Days)</h2>
                    <div class="chart-container">
                        {chart_html}
                    </div>
                    <table class="info-table" style="margin-top: 20px;">
                        <tr>
                            <td>Average (Last 7d)</td>
                            <td>{rt.get('avg', 'N/A')} ms</td>
                        </tr>
                        <tr>
                            <td>Min (Last 7d)</td>
                            <td>{rt.get('min', 'N/A')} ms</td>
                        </tr>
                        <tr>
                            <td>Max (Last 7d)</td>
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
        for log in logs[-10:]:
            try:
                # Use .get() for 'datetime' for safety
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
        ("Anomaly Detection (Response Time)", analysis.get("anomaly_detection", "N/A")),
        ("Root Cause Analysis", analysis.get("rca", "N/A")),
        ("Comparative Analysis", analysis.get("comparative_analysis", "N/A"))
    ]
    
    has_ai_content = False
    for title, content in sections:
        # Ensure content is a string before checking
        content_str = str(content)
        if content and content_str != "N/A" and "error" not in content_str.lower():
            has_ai_content = True

            content_lower = content_str.lower()
            class_add = ""
            if title == "Anomaly Detection (Response Time)":
                if any(word in content_lower for word in ["detected", "spike", "unusual"]):
                    class_add = "alert-section"
            elif title == "Root Cause Analysis":
                if "no incidents requiring rca" not in content_lower:
                    class_add = "alert-section"
            elif title == "Comparative Analysis":
                if "degradation" in content_lower:
                    class_add = "alert-section"
            
            # Detect and format bullets vs paragraphs
            if title in ["Root Cause Analysis", "Comparative Analysis"] or content_str.startswith('-') or '\n-' in content_str:
                # Parse bullets: split by \n, strip '-', wrap in <li>
                lines = [line.strip() for line in content_str.split('\n') if line.strip().startswith('-')]
                if lines:
                    bullet_html = '<ul class="ai-bullets">' + ''.join(f'<li>{line[1:].strip()}</li>' for line in lines) + '</ul>'
                else:
                    bullet_html = '<p>' + content_str.replace('\n', '<br>') + '</p>'
            else:
                # Paragraph/sentences for anomaly
                bullet_html = '<p>' + content_str.replace('\n', '<br>') + '</p>'

            html += f"""
                    <div class="ai-section" {class_add}>
                        <h3>{title}</h3>
                        {bullet_html}
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
               
        subject = f"Weekly Uptime Report with Insights for {monitor_name}"
        
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

# Read README if available
readme_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'uptime_weekly_report.md')
readme_content = ""
try:
    with open(readme_path, 'r') as file:
        readme_content = file.read()
except FileNotFoundError:
    readme_content = "Daily uptime report generation and email DAG with AI insights"

with DAG(
    "uptime_weekly_data_report", 
    default_args=default_args, 
    schedule_interval="0 0 * * 1",  # Run every Monday at midnight, 
    catchup=False, 
    doc_md=readme_content, 
    tags=["uptime", "report", "weekly", "ai-insights"]
) as dag:
    
    fetch_data_task = PythonOperator(
        task_id="step_1_fetch_data",
        python_callable=step_1_fetch_data,
        provide_context=True
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
    
    # Set up task dependencies serially for analysis steps
    fetch_data_task >> anomaly_detection_task >> rca_task >> comparative_analysis_task >> combine_analysis_task >> generate_plot_task >> compose_email_task >> send_report_email_task