from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task, task_group
from typing import List
from datetime import datetime, timedelta, timezone
import logging
from ollama import Client
from airflow.models import Variable
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import base64
import json
import re
import html
import smtplib
from email.mime.image import MIMEImage
import os
import requests
import pandas as pd
from requests.auth import HTTPBasicAuth

# Configure detailed logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

default_args = {
    "owner": "sre_team",
    "depends_on_past": False,
    "start_date": datetime(2025, 11, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# SMTP Configuration from Airflow Variables
SMTP_USER = Variable.get("ltai.v1.sreunityfi.SMTP_USER")
SMTP_PASSWORD = Variable.get("ltai.v1.sreunityfi.SMTP_PASSWORD")
SMTP_HOST = Variable.get("ltai.v1.sreunityfi.SMTP_HOST", default_var="mail.authsmtp.com")
SMTP_PORT = int(Variable.get("ltai.v1.sreunityfi.SMTP_PORT", default_var="2525"))
SMTP_SUFFIX = Variable.get("ltai.v1.sreunityfi.SMTP_FROM_SUFFIX", default_var="via lowtouch.ai <webmaster@ecloudcontrol.com>")
SENDER_EMAIL = Variable.get("ltai.v1.sreunityfi.UNITYFI_FROM_ADDRESS", default_var=SMTP_USER)
RECEIVER_EMAIL = Variable.get("ltai.v1.sreunityfi.UNITYFI_TO_ADDRESS", default_var=SENDER_EMAIL)

OLLAMA_HOST = Variable.get("ltai.v1.sreunityfi.UNITYFI_OLLAMA_HOST", "http://agentomatic:8000/")

PROMETHEUS_URL = Variable.get("UNITYFI_PROMETHEUS_URL","https://unityfi-prod-promethues.lowtouchcloud.io") 

# === Precise Date & Time Helpers (computed once per DAG run) ===
IST = timezone(timedelta(hours=5, minutes=30))

def get_thursday_ranges():
    now = datetime.now(IST)
    days_to_thursday = (now.weekday() - 3) % 7
    if now.weekday() == 3 and now.hour >= 11:
        days_to_thursday = 0

    this_thursday_11am = (now - timedelta(days=days_to_thursday)).replace(
        hour=11, minute=0, second=0, microsecond=0
    )
    last_thursday_11am = this_thursday_11am - timedelta(days=7)
    prev_thursday_11am = last_thursday_11am - timedelta(days=7)

    # CORRECT: Just use .isoformat() — it already includes +05:30
    current_week = (
        last_thursday_11am.isoformat(),      # → 2025-11-20T11:00:00+05:30
        this_thursday_11am.isoformat()
    )
    previous_week = (
        prev_thursday_11am.isoformat(),
        last_thursday_11am.isoformat()
    )

    period_current = f"{last_thursday_11am.strftime('%b %d')} – {this_thursday_11am.strftime('%b %d, %Y')}"
    period_previous = f"{prev_thursday_11am.strftime('%b %d')} – {last_thursday_11am.strftime('%b %d, %Y')}"

    return current_week, previous_week, period_current, period_previous

def query_prometheus_range(query: str, start: str, end: str, step: str = "1h"):
    url = f"{PROMETHEUS_URL}/api/v1/query_range"
    params = {
        "query": query,
        "start": start,
        "end": end,
        "step": step
    }
    
    # Add your Prometheus credentials here
    auth = HTTPBasicAuth(
        username=Variable.get("AGENT_PROMETHEUS_USER_UNITYFI"),
        password=Variable.get("AGENT_PROMETHEUS_PASSWORD_UNITYFI")
    )
    
    resp = requests.get(url, params=params, auth=auth, timeout=60, verify=True)
    resp.raise_for_status()
    data = resp.json()["data"]["result"]

    rows = []
    for result in data:
        metric = result["metric"]
        instance = metric.get("instance", "unknown")
        for timestamp, value in result["values"]:
            if value == "NaN":
                continue
            rows.append({
                "instance": instance,
                "timestamp": datetime.fromtimestamp(timestamp),
                "value": float(value)
            })
    return pd.DataFrame(rows)

def fetch_all_metrics_this_week(ti):
    (start_curr, end_curr), _, period_curr, _ = get_thursday_ranges()
    interval = "7d"

    queries = {
        "engine_cpu_avg": f'avg_over_time((1 - avg by(instance) (rate(node_cpu_seconds_total{{mode="idle"}}[30s])))[7d:]) * 100',
        "engine_mem_avg": f'avg_over_time((1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes))[7d:5m]) * 100',
        "engine_disk_avg": f'avg_over_time((1 - (node_filesystem_free_bytes{{device="/dev/sdb1",fstype!~"tmpfs|overlay",mountpoint="/var/lib/docker"}} / node_filesystem_size_bytes{{device="/dev/sdb1",fstype!~"tmpfs|overlay",mountpoint="/var/lib/docker"}}))[7d:]) * 100',

        "windows_cpu_avg": f'avg_over_time((100 - (avg by(instance) (rate(windows_cpu_time_total{{mode="idle"}}[30s])) * 100))[7d:])',
        "windows_cpu_peak": f'max_over_time((100 - (avg by(instance) (rate(windows_cpu_time_total{{mode="idle"}}[30s])) * 100))[7d:])',
        "windows_mem_avg": f'avg_over_time(((1 - (windows_os_physical_memory_free_bytes / windows_cs_physical_memory_bytes)) * 100)[7d:])',
        "windows_mem_peak": f'max_over_time(((1 - (windows_os_physical_memory_free_bytes / windows_cs_physical_memory_bytes)) * 100)[7d:])',
        "windows_cdisk_avg": f'avg_over_time(((1 - (windows_logical_disk_free_bytes{{volume="C:"}} / windows_logical_disk_size_bytes{{volume="C:"}})) * 100)[7d:])',
        "windows_ddisk_avg": f'avg_over_time(((1 - (windows_logical_disk_free_bytes{{volume="D:"}} / windows_logical_disk_size_bytes{{volume="D:"}})) * 100)[7d:])',
        "windows_edisk_avg": f'avg_over_time(((1 - (windows_logical_disk_free_bytes{{volume="E:"}} / windows_logical_disk_size_bytes{{volume="E:"}})) * 100)[7d:])',
        "windows_fdisk_avg": f'avg_over_time(((1 - (windows_logical_disk_free_bytes{{volume="F:"}} / windows_logical_disk_size_bytes{{volume="F:"}})) * 100)[7d:])',
    }

    results = {}
    for name, q in queries.items():
        logging.info(f"Fetching {name} for current week...")
        df = query_prometheus_range(q, start_curr, end_curr)
        if df.empty:
            results[name] = {}
            continue
        # Take the last (most recent) value per instance
        latest = df.sort_values("timestamp").groupby("instance").last()["value"]
        results[name] = {inst: round(float(val), 4) for inst, val in latest.items()}

    # Push everything cleanly
    ti.xcom_push(key="metrics_this_week", value=json.dumps(results))
    ti.xcom_push(key="period_this_week", value=period_curr)
    ti.xcom_push(key="range_this_week", value={"start": start_curr, "end": end_curr})

    logging.info("This week metrics fetched and pushed.")
    return results


def fetch_all_metrics_previous_week(ti):
    _, (start_prev, end_prev), _, period_prev = get_thursday_ranges()
    interval = "7d"

    queries = {  # same as above
        "engine_cpu_avg": f'avg_over_time((1 - avg by(instance) (rate(node_cpu_seconds_total{{mode="idle"}}[30s])))[7d:]) * 100',
        "engine_mem_avg": f'avg_over_time((1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes))[7d:5m]) * 100',
        "engine_disk_avg": f'avg_over_time((1 - (node_filesystem_free_bytes{{device="/dev/sdb1",fstype!~"tmpfs|overlay",mountpoint="/var/lib/docker"}} / node_filesystem_size_bytes{{device="/dev/sdb1",fstype!~"tmpfs|overlay",mountpoint="/var/lib/docker"}}))[7d:]) * 100',

        "windows_cpu_avg": f'avg_over_time((100 - (avg by(instance) (rate(windows_cpu_time_total{{mode="idle"}}[30s])) * 100))[7d:])',
        "windows_cpu_peak": f'max_over_time((100 - (avg by(instance) (rate(windows_cpu_time_total{{mode="idle"}}[30s])) * 100))[7d:])',
        "windows_mem_avg": f'avg_over_time(((1 - (windows_os_physical_memory_free_bytes / windows_cs_physical_memory_bytes)) * 100)[7d:])',
        "windows_mem_peak": f'max_over_time(((1 - (windows_os_physical_memory_free_bytes / windows_cs_physical_memory_bytes)) * 100)[7d:])',
    }

    results = {}
    for name, q in queries.items():
        df = query_prometheus_range(q.replace("7d", interval), start_prev, end_prev)
        if df.empty:
            results[name] = {}
            continue
        latest = df.sort_values("timestamp").groupby("instance").last()["value"]
        results[name] = {inst: round(float(val), 4) for inst, val in latest.items()}

    ti.xcom_push(key="metrics_previous_week", value=json.dumps(results))
    ti.xcom_push(key="period_previous_week", value=period_prev)

    return results

def generate_engine_master_comparison(ti, **context):
    """
    Pulls this_week and previous_week metrics and generates:
    Appz-Engine-Master Metrics
    Metric         | This week | Previous Week | Difference |
    ---------------|-----------|---------------|------------|
    CPU Usage      | 12.45%    | 10.81%        | +1.64%     |
    Memory Usage   | 68.32%    | 65.19%        | +3.13%     |
    Disk Usage     | 72.10%    | 70.05%        | +2.05%     |
    """

    # Pull fetched data
    this_week_data = json.loads(ti.xcom_pull(key="metrics_this_week"))
    prev_week_data = json.loads(ti.xcom_pull(key="metrics_previous_week"))

    # Extract engine master values (assuming only one engine-master instance)
    def get_engine_value(data, key):
        values = data.get(key, {})
        if not values:
            return None
        # Take first (and only) value
        return list(values.values())[0]

    # This week
    cpu_this = get_engine_value(this_week_data, "engine_cpu_avg")
    mem_this = get_engine_value(this_week_data, "engine_mem_avg")
    disk_this = get_engine_value(this_week_data, "engine_disk_avg")

    # Previous week
    cpu_prev = get_engine_value(prev_week_data, "engine_cpu_avg")
    mem_prev = get_engine_value(prev_week_data, "engine_mem_avg")
    disk_prev = get_engine_value(prev_week_data, "engine_disk_avg")

    # Helper to format with % and color diff
    def fmt(val):
        return f"{val:.2f}%" if val is not None else "N/A"

    def diff(a, b):
        if a is None or b is None:
            return "N/A"
        d = a - b
        sign = "+" if d > 0 else ""
        return f"{sign}{d:.2f}%"

    markdown = "### Appz-Engine-Master Metrics\n\n"
    markdown += "| Metric         | This Week     | Previous Week | Difference   |\n"
    markdown += "|----------------|---------------|---------------|--------------|\n"
    markdown += f"| CPU Usage      | {fmt(cpu_this)} | {fmt(cpu_prev)} | {diff(cpu_this, cpu_prev)} |\n"
    markdown += f"| Memory Usage   | {fmt(mem_this)} | {fmt(mem_prev)} | {diff(mem_this, mem_prev)} |\n"
    markdown += f"| Disk Usage     | {fmt(disk_this)} | {fmt(disk_prev)} | {diff(disk_this, disk_prev)} |\n"

    # Push for final report
    ti.xcom_push(key="section_engine_master", value=markdown)
    return markdown

def generate_windows_vm_wow_comparison(ti, **context):
    """
    Generates:
    ### Windows VM – Average Resource Summary

    #### CPU Utilization
    | VM Name       | CPU (%) - This Week | CPU (%) - Previous Week | Difference   |
    |---------------|---------------------|-------------------------|--------------|
    | 10.10.1.101   | 12.45               | 10.81                   | +1.64%       |

    #### Memory Utilization
    ...

    #### Disk Utilization 
    ...
    """

    # Pull pre-fetched data
    this_week = json.loads(ti.xcom_pull(key="metrics_this_week"))
    prev_week = json.loads(ti.xcom_pull(key="metrics_previous_week"))

    # Extract Windows VM data
    cpu_this = this_week.get("windows_cpu_avg", {})
    cpu_prev = prev_week.get("windows_cpu_avg", {})

    mem_this = this_week.get("windows_mem_avg", {})
    mem_prev = prev_week.get("windows_mem_avg", {})

    disk_this = this_week.get("windows_cdisk_avg", {})
    disk_prev = prev_week.get("windows_cdisk_avg", {})

    # Get all unique instances from both weeks
    all_instances = set(cpu_this.keys()) | set(cpu_prev.keys()) | \
                    set(mem_this.keys()) | set(mem_prev.keys()) | \
                    set(disk_this.keys()) | set(disk_prev.keys())

    def clean_name(inst):
        # Use IP:port → just IP, or keep full if you prefer
        return inst.split(":")[0]

    def get_val(d, inst):
        return d.get(inst)

    def fmt(val):
        return f"{val:.2f}" if val is not None else "N/A"

    def diff(a, b):
        if a is None or b is None:
            return "N/A"
        d = a - b
        sign = "+" if d > 0 else ""
        return f"{sign}{d:.2f}%"

    # Build markdown
    markdown = "### Windows VM – Average Resource Summary\n\n"

    # === CPU Table ===
    markdown += "#### CPU Utilization\n"
    markdown += "| VM Name       | CPU (%) - This Week | CPU (%) - Previous Week | Difference   |\n"
    markdown += "|---------------|---------------------|-------------------------|--------------|\n"
    for inst in sorted(all_instances):
        name = clean_name(inst)
        c_this = get_val(cpu_this, inst)
        c_prev = get_val(cpu_prev, inst)
        markdown += f"| {name:<13} | {fmt(c_this):>19} | {fmt(c_prev):>23} | {diff(c_this, c_prev):>12} |\n"
    markdown += "\n"

    # === Memory Table ===
    markdown += "#### Memory Utilization\n"
    markdown += "| VM Name       | Memory (%) - This Week | Memory (%) - Previous Week | Difference   |\n"
    markdown += "|---------------|------------------------|----------------------------|--------------|\n"
    for inst in sorted(all_instances):
        name = clean_name(inst)
        m_this = get_val(mem_this, inst)
        m_prev = get_val(mem_prev, inst)
        markdown += f"| {name:<13} | {fmt(m_this):>20} | {fmt(m_prev):>26} | {diff(m_this, m_prev):>12} |\n"
    markdown += "\n"

    # === Disk Table ===
    markdown += "#### Disk Utilization – This Week\n"
    markdown += "| VM Name       | C Disk (%) | D Disk (%) | E Disk (%) | F Disk (%) |\n"
    markdown += "|---------------|------------|------------|------------|------------|\n"

    # Pull all disk metrics for this week
    c_this = this_week.get("windows_cdisk_avg", {})
    d_this = this_week.get("windows_ddisk_avg", {})
    e_this = this_week.get("windows_edisk_avg", {})
    f_this = this_week.get("windows_fdisk_avg", {})

    for inst in sorted(all_instances):
        name = clean_name(inst)
        c_val = fmt(get_val(c_this, inst))
        d_val = fmt(get_val(d_this, inst))
        e_val = fmt(get_val(e_this, inst))
        f_val = fmt(get_val(f_this, inst))

        markdown += f"| {name:<13} | {c_val:>10} | {d_val:>10} | {e_val:>10} | {f_val:>10} |\n"
    markdown += "\n"

    # Push for final report
    ti.xcom_push(key="section_windows_wow", value=markdown.strip())
    return markdown

def generate_high_cpu_peaks(ti, **context):
    """
    Section: High CPU Peaks (≥90%)
    Output:
    High CPU Peaks (≥90%)
    | VM Name       | Peak CPU (%) |
    |---------------|--------------|
    | 10.10.1.101   |        98.4  |
    | 10.10.1.150   |        96.1  |
    """
    this_week = json.loads(ti.xcom_pull(key="metrics_this_week"))
    cpu_peak_data = this_week.get("windows_cpu_peak", {})

    high_peaks = []
    for instance, peak_val in cpu_peak_data.items():
        if peak_val >= 90:
            vm_name = instance.split(":")[0]  # or .split(":")[0] + ":" + instance.split(":")[1] if you want port
            high_peaks.append((vm_name, peak_val))

    # Sort by peak value descending
    high_peaks.sort(key=lambda x: x[1], reverse=True)

    markdown = "### High CPU Peaks (≥90%)\n\n"
    if not high_peaks:
        markdown += "No Windows VMs recorded CPU peak ≥90% this week.\n"
    else:
        markdown += "| VM Name       | Peak CPU (%) |\n"
        markdown += "|---------------|--------------|\n"
        for name, val in high_peaks:
            markdown += f"| {name:<13} | {val:>8.1f}     |\n"

    ti.xcom_push(key="section_high_cpu_peaks", value=markdown.strip())
    return markdown

def generate_high_memory_peaks(ti, **context):
    """
    Section: High Memory Peaks (≥90%)
    Output:
    High Memory Peaks (≥90%)
    | VM Name       | Peak Memory (%) |
    |---------------|-----------------|
    | 10.10.1.150   |            97.8 |
    """
    this_week = json.loads(ti.xcom_pull(key="metrics_this_week"))
    mem_peak_data = this_week.get("windows_mem_peak", {})

    high_mem = []
    for instance, peak_val in mem_peak_data.items():
        if peak_val >= 90:
            vm_name = instance.split(":")[0]
            high_mem.append((vm_name, peak_val))

    high_mem.sort(key=lambda x: x[1], reverse=True)

    markdown = "### High Memory Peaks (≥90%)\n\n"
    if not high_mem:
        markdown += "No Windows VMs recorded memory peak ≥90% this week.\n"
    else:
        markdown += "| VM Name       | Peak Memory (%) |\n"
        markdown += "|---------------|-----------------|\n"
        for name, val in high_mem:
            markdown += f"| {name:<13} | {val:>10.1f}       |\n"

    ti.xcom_push(key="section_high_memory_peaks", value=markdown.strip())
    return markdown



def get_ai_response(prompt, conversation_history=None):
    try:
        logging.debug(f"Query received: {prompt}")
        if not prompt or not isinstance(prompt, str):
            raise ValueError("Invalid prompt provided.")

        client = Client(host=OLLAMA_HOST)
        logging.debug(f"Connecting to Ollama at {OLLAMA_HOST} with model 'SRE/AILAB:0.4'")

        messages = []
        if conversation_history:
            messages.extend(conversation_history)
        
        messages.append({"role": "user", "content": prompt})

        response = client.chat(
            model='appz/sre/unityfi:0.3',
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

def overall_summary(ti, **context):
    period = ti.xcom_pull(key="period_this_week")

    # Pull all the beautiful Python-generated markdown sections
    engine_master       = ti.xcom_pull(key="section_engine_master") or "No data"
    windows_wow         = ti.xcom_pull(key="section_windows_wow") or "No data"
    high_cpu_peaks      = ti.xcom_pull(key="section_high_cpu_peaks") or "No data"
    high_memory_peaks   = ti.xcom_pull(key="section_high_memory_peaks") or "No data"

    prompt = f"""
You are the SRE Unityfi Agent — professional, concise, and proactive.

Generate a **high-quality Weekly SRE Summary & Recommendations** for the period: **{period}** (Thursday 11:00 AM IST to next Thursday 11:00 AM IST).

### Raw Data for Context (do NOT copy tables verbatim — summarize intelligently):

#### 1. Appz-Engine-Master (Week-over-Week)
{engine_master}

#### 2. Windows VMs — Average Utilization (This vs Previous Week)
{windows_wow}

#### 3. High CPU Peaks (≥90%)
{high_cpu_peaks}

#### 4. High Memory Peaks (≥90%)
{high_memory_peaks}

### Instructions:
Write in clear, executive-friendly Markdown with two main sections:

1. **Weekly SRE Summary (This Week)**  
   → Highlight overall stability of engine-master and Windows fleet  
   → Call out any concerning averages or peaks  
   → Mention number of VMs with high CPU/memory/disk

2. **Week-over-Week Insights & Recommendations**  
   → Highlight biggest regressions/improvements  
   → Flag any node or VM showing sustained increase in resource usage  
   → Suggest actions if needed (e.g. investigate, scale, cleanup)

Tone: Professional, calm, proactive. Use bullet points. No fluff.
"""

    response = get_ai_response(prompt)
    ti.xcom_push(key="ai_weekly_summary", value=response)
    return response


def compile_sre_report(ti, **context):
    period = ti.xcom_pull(key="period_this_week")

    report = f"""# Weekly SRE Report – Unityfi Platform
**Period**: {period} (Thursday 11:00 AM → Thursday 11:00 AM IST)  

---

{ti.xcom_pull(key="ai_weekly_summary") or "Summary generation in progress..."}

---

## 1. Appz-Engine-Master Metrics (This Week vs Previous Week)
{ti.xcom_pull(key="section_engine_master") or "No data"}

---

## 2. Windows VM – Average Resource Summary (Week-over-Week)
{ti.xcom_pull(key="section_windows_wow") or "No data"}

---

## 3. Alerting: High Resource Peaks This Week
{ti.xcom_pull(key="section_high_cpu_peaks") or "No data"}

{ti.xcom_pull(key="section_high_memory_peaks") or "No data"}

---

**End of Report**  
"""

    # Clean up extra blank lines
    report = re.sub(r'\n{3,}', '\n\n', report.strip())

    ti.xcom_push(key="sre_full_report", value=report)
    return report

# === t11: Convert SRE Markdown Report to HTML (Local Markdown Parser) ===
def preprocess_markdown(markdown_text):
    """
    Clean and standardize Markdown before conversion to HTML.
    Fixes spacing and table formatting issues.
    """
    markdown_text = markdown_text.lstrip('\ufeff\u200b\u200c\u200d')
    markdown_text = re.sub(r'^(#{1,6})\s*', r'\1 ', markdown_text, flags=re.MULTILINE)
    
    lines = markdown_text.split('\n')
    processed = []
    in_table = False

    for line in lines:
        stripped = line.strip()
        if '|' in stripped and stripped.count('|') >= 2:
            if not in_table and processed and processed[-1].strip():
                processed.append('')
            in_table = True
            processed.append(line)
        else:
            if in_table and stripped:
                processed.append('')
                in_table = False
            processed.append(line)
    return '\n'.join(processed)


def convert_to_html(ti, **context):
    """
    Airflow task: Convert the Markdown SRE report into a clean, responsive, Gmail-safe HTML.
    """
    # --- Pull Markdown from previous task ---
    markdown_report = ti.xcom_pull(key="sre_full_report") or "# No report generated."
    logging.info(f"Markdown length: {len(markdown_report)}")

    # --- Preprocess Markdown ---
    markdown_report = preprocess_markdown(markdown_report)

    html_body = None

    # --- Try python-markdown (best support for tables & lists) ---
    try:
        import markdown
        logging.info("✅ Using 'markdown' library for conversion")
        html_body = markdown.markdown(
            markdown_report,
            extensions=[
                'tables',
                'fenced_code',
                'nl2br',
                'sane_lists',
                'attr_list'
            ]
        )
    except ImportError:
        logging.warning("⚠️ 'markdown' library not found.")
    except Exception as e:
        logging.error(f"⚠️ markdown library error: {e}")

    # --- Fallback to markdown2 ---
    if not html_body or len(html_body) < 100:
        try:
            import markdown2
            logging.info("✅ Using 'markdown2' fallback parser")
            html_body = markdown2.markdown(
                markdown_report,
                extras=[
                    "fenced-code-blocks",
                    "tables",
                    "strike",
                    "task_list",
                    "cuddled-lists",
                    "header-ids",
                    "footnotes",
                    "break-on-newline"
                ]
            )
        except ImportError:
            logging.error("❌ markdown2 library not installed (pip install markdown2)")
        except Exception as e:
            logging.error(f"❌ markdown2 error: {e}")

    # --- Fallback to mistune (final safety) ---
    if not html_body or len(html_body) < 50:
        try:
            import mistune
            logging.info("✅ Using 'mistune' parser as final fallback")
            html_body = mistune.html(markdown_report)
        except Exception as e:
            logging.error(f"❌ Mistune conversion failed: {e}")
            html_body = f"<pre>{html.escape(markdown_report)}</pre>"

    # --- Wrap HTML with email-safe container ---
    full_html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>SRE Daily Report</title>
<style>
body {{
    font-family: Arial, sans-serif;
    background-color: #f4f4f4;
    margin: 0;
    padding: 20px;
}}
.container {{
    max-width: 1000px;
    background: #ffffff;
    margin: auto;
    padding: 30px;
    border-radius: 8px;
    box-shadow: 0 2px 8px rgba(0,0,0,0.1);
}}
table {{
    border-collapse: collapse;
    width: 100%;
    margin: 20px 0;
    font-size: 14px;
}}
table, th, td {{
    border: 1px solid #ddd;
}}
th, td {{
    padding: 10px;
    text-align: left;
}}
th {{
    background-color: #1a5fb4;
    color: #ffffff;
}}
tr:nth-child(even) {{
    background-color: #f8f9fa;
}}
pre {{
    background-color: #f6f8fa;
    padding: 12px;
    border-radius: 6px;
    overflow-x: auto;
    border: 1px solid #e1e4e8;
}}
h1 {{
    color: #1a5fb4;
    font-size: 24px;
    border-bottom: 3px solid #1a5fb4;
    padding-bottom: 6px;
}}
h2 {{
    color: #1a5fb4;
    border-bottom: 2px solid #ccc;
    padding-bottom: 4px;
}}
h3 {{
    color: #2d3748;
}}
strong {{
    color: #111;
}}
ul, ol {{
    padding-left: 25px;
}}
a {{
    color: #1a5fb4;
    text-decoration: none;
}}
a:hover {{
    text-decoration: underline;
}}
@media screen and (max-width: 600px) {{
    .container {{
        padding: 15px;
    }}
    table {{
        font-size: 12px;
    }}
}}
</style>
</head>
<body>
<div class="container">
{html_body}
</div>
</body>
</html>"""

    # --- Log preview ---
    logging.info(f"HTML generated, length={len(full_html)}")

    # --- Push to XCom for next task (email sender) ---
    ti.xcom_push(key="sre_html_report", value=full_html)

    logging.info("✅ SRE Markdown successfully converted to HTML.")
    return full_html

def send_sre_email(ti, **context):
    """Send SRE HTML report via SMTP with PDF attachment"""
    html_report = ti.xcom_pull(key="sre_html_report")

    if not html_report or "<html" not in html_report.lower():
        logging.error("No valid HTML report found in XCom.")
        raise ValueError("HTML report missing or invalid.")

    # === PDF Attachment (same logic as Gmail version) ===
    pdf_path = ti.xcom_pull(key="sre_pdf_path")
    pdf_attachment = None
    if pdf_path and os.path.exists(pdf_path):
        with open(pdf_path, "rb") as f:
            pdf_attachment = MIMEApplication(f.read(), _subtype="pdf")
            pdf_attachment.add_header(
                "Content-Disposition",
                "attachment",
                filename="Unityfi_SRE_Report.pdf"
            )
        logging.info(f"Attaching PDF: {pdf_path}")
    else:
        logging.warning("PDF not found or not generated, skipping attachment")

    # Clean up any code block wrappers
    html_body = re.sub(r'```html\s*|```', '', html_report).strip()

    subject = f"SRE Weekly Report – {datetime.utcnow().strftime('%Y-%m-%d')}"
    recipient = RECEIVER_EMAIL
    try:
        # Initialize SMTP server
        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30)
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        # Create MIME message
        logging.debug(f"Preparing email to {recipient} with subject: {subject}")
        msg = MIMEMultipart("mixed")
        msg["Subject"] = subject
        msg["From"] = f"Unityfi SRE Agent {SMTP_SUFFIX}"
        msg["To"] = recipient

        # --- HTML Part (with possible inline images) ---
        html_part = MIMEMultipart("related")
        html_part.attach(MIMEText(html_body, "html"))

        # Optional: Inline chart image (unchanged from your original)
        chart_b64 = ti.xcom_pull(key="chart_b64")
        if chart_b64:
            try:
                img_data = base64.b64decode(chart_b64)
                img_part = MIMEImage(img_data, 'png')
                img_part.add_header('Content-ID', '<chart_image>')
                img_part.add_header('Content-Disposition', 'inline', filename='chart.png')
                html_part.attach(img_part)
                logging.info("Attached chart image to email.")
            except Exception as e:
                logging.warning(f"Failed to attach inline image: {str(e)}")

        # Attach the HTML+inline part to the main message
        msg.attach(html_part)

        # --- PDF Attachment ---
        if pdf_attachment:
            msg.attach(pdf_attachment)
            logging.info("PDF successfully attached to email")
        else:
            logging.info("No PDF attachment added")

        # Send the email
        server.sendmail("webmaster@ecloudcontrol.com", recipient, msg.as_string())
        logging.info(f"Email sent successfully: {recipient}")
        server.quit()
        return True
    except Exception as e:
        logging.error(f"Failed to send email: {str(e)}")
        return None

        
def generate_pdf_report_callable(ti=None, **context):
    """
    Unityfi SRE PDF – ReportLab (ABSOLUTE FINAL VERSION)
    • No raw # or ## visible
    • Tables perfect with text wrap
    • No small squares
    • Professional layout
    """
    try:
        md = ti.xcom_pull(key="sre_full_report") or "# No SRE report generated."
        md = preprocess_markdown(md)

        out_path = f"/tmp/Unityfi_SRE_Report.pdf"

        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.enums import TA_LEFT, TA_CENTER
        from reportlab.lib import colors
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Preformatted, PageBreak

        doc = SimpleDocTemplate(
            out_path,
            pagesize=A4,
            leftMargin=28, rightMargin=28,
            topMargin=30, bottomMargin=30
        )

        styles = getSampleStyleSheet()
        title = ParagraphStyle('Title', parent=styles['Title'], fontSize=22, leading=28, alignment=TA_CENTER,
                               spaceAfter=20, textColor=colors.HexColor("#1a5fb4"), fontName="Helvetica-Bold")
        h2 = ParagraphStyle('H2', parent=styles['Heading2'], fontSize=15, leading=20, spaceBefore=18, spaceAfter=10,
                            textColor=colors.HexColor("#1a5fb4"), fontName="Helvetica-Bold")
        h3 = ParagraphStyle('H3', parent=styles['Heading3'], fontSize=12, leading=16, spaceBefore=12, spaceAfter=8)
        normal = ParagraphStyle('Normal', parent=styles['Normal'], fontSize=10, leading=14, spaceAfter=8)
        small = ParagraphStyle('Small', parent=styles['Normal'], fontSize=9, textColor=colors.gray, spaceAfter=12)
        code = ParagraphStyle('Code', fontName='Courier', fontSize=8, leading=10,
                              backColor=colors.HexColor("#f6f8fa"), borderPadding=10,
                              borderColor=colors.lightgrey, borderWidth=1, borderRadius=4)
        cell_style = ParagraphStyle('Cell', parent=styles['Normal'], fontSize=8.5, leading=10, alignment=TA_LEFT)

        flowables = []

        lines = md.splitlines()
        i = 0
        in_code = False
        code_lines = []

        while i < len(lines):
            raw_line = lines[i]
            stripped = raw_line.strip()

            # === CODE BLOCKS ===
            if stripped.startswith("```"):
                if in_code:
                    flowables.append(Preformatted("\n".join(code_lines), code))
                    flowables.append(Spacer(1, 10))
                    code_lines = []
                in_code = not in_code
                i += 1
                continue
            if in_code:
                code_lines.append(raw_line)
                i += 1
                continue

            # === SKIP HR LINES (no squares) ===
            if stripped.startswith(("---", "***", "___")):
                flowables.append(Spacer(1, 14))
                i += 1
                continue

            # === HEADINGS – NOW WORKS EVEN WITH LEADING SPACES ===
            if raw_line.lstrip().startswith("# "):
                text = raw_line.lstrip("# ").strip()
                flowables.append(Paragraph(text, title))
                flowables.append(Spacer(1, 14))
            elif raw_line.lstrip().startswith("## "):
                text = raw_line.lstrip("# ").strip()
                flowables.append(Paragraph(text, h2))
                flowables.append(Spacer(1, 10))
            elif raw_line.lstrip().startswith("### "):
                text = raw_line.lstrip("# ").strip()
                flowables.append(Paragraph(text, h3))
                flowables.append(Spacer(1, 8))
            elif raw_line.lstrip().startswith("#### "):
                text = raw_line.lstrip("# ").strip()
                flowables.append(Paragraph(f"<b>{text}</b>", normal))

            # === TABLES – PERFECT TEXT WRAP ===
            elif "|" in raw_line and i + 1 < len(lines) and re.match(r"^[\s\|:-]*$", lines[i+1].strip()):
                table_data = []
                header = [c.strip() for c in raw_line.split("|")[1:-1]]
                table_data.append(header)
                i += 2
                while i < len(lines) and "|" in lines[i]:
                    row = [c.strip() for c in lines[i].split("|")[1:-1]]
                    if row:
                        table_data.append(row)
                    i += 1

                if len(table_data) > 1:
                    num_cols = len(table_data[0])
                    col_width = doc.width / num_cols
                    wrapped_data = [[Paragraph(html.escape(cell), cell_style) for cell in row] for row in table_data]

                    table = Table(wrapped_data, colWidths=[col_width] * num_cols, repeatRows=1)
                    table.setStyle(TableStyle([
                        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#1a5fb4")),
                        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
                        ('ALIGN', (0, 1), (-1, -1), 'LEFT'),
                        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                        ('FONTSIZE', (0, 0), (-1, -1), 8.5),
                        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                        ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor("#f9f9f9")),
                        ('LEFTPADDING', (0, 0), (-1, -1), 6),
                        ('RIGHTPADDING', (0, 0), (-1, -1), 6),
                        ('TOPPADDING', (0, 0), (-1, -1), 8),
                        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
                    ]))
                    flowables.append(table)
                    flowables.append(Spacer(1, 14))
                continue

            # === NORMAL TEXT ===
            elif stripped:
                text = html.escape(raw_line)
                text = re.sub(r'\*\*(.*?)\*\*', r'<b>\1</b>', text)
                text = re.sub(r'__(.*?)__', r'<b>\1</b>', text)
                text = re.sub(r'\*(.*?)\*', r'<i>\1</i>', text)
                text = re.sub(r'`([^`]+)`', r'<font name=Courier>\1</font>', text)
                flowables.append(Paragraph(text, normal))
            else:
                flowables.append(Spacer(1, 6))

            i += 1

        # Footer
        flowables.append(PageBreak())
        flowables.append(Paragraph("End of Report", h2))
        flowables.append(Paragraph("Generated by Unityfi SRE Agent • Powered by Airflow + ReportLab", small))

        doc.build(flowables)
        logging.info(f"PDF generated – PERFECT (no # visible, tables wrapped): {out_path}")
        ti.xcom_push(key="sre_pdf_path", value=out_path)
        return out_path

    except Exception as e:
        logging.error("PDF generation failed", exc_info=True)
        raise


with DAG(
    dag_id="sre_weekly_report_unityfi",
    schedule_interval="0 11 * * 4",  # Every Thursday at 11:00 AM IST
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["sre", "weekly", "unityfi"],
) as dag:

    fetch_this = PythonOperator(task_id="fetch_this_week", python_callable=fetch_all_metrics_this_week)
    fetch_prev = PythonOperator(task_id="fetch_previous_week", python_callable=fetch_all_metrics_previous_week)

    gen_engine     = PythonOperator(task_id="gen_engine_master", python_callable=generate_engine_master_comparison)
    gen_windows    = PythonOperator(task_id="gen_windows_wow", python_callable=generate_windows_vm_wow_comparison)
    gen_cpu_peaks  = PythonOperator(task_id="gen_cpu_peaks", python_callable=generate_high_cpu_peaks)
    gen_mem_peaks  = PythonOperator(task_id="gen_mem_peaks", python_callable=generate_high_memory_peaks)

    ai_summary     = PythonOperator(task_id="ai_summary", python_callable=overall_summary)
    compile_report = PythonOperator(task_id="compile_report", python_callable=compile_sre_report)
    generate_pdf = PythonOperator(task_id="generate_pdf", python_callable=generate_pdf_report_callable, provide_context=True)
    convert_to_html = PythonOperator(task_id="convert_to_html", python_callable=convert_to_html, provide_context=True)
    send_sre_email = PythonOperator(task_id="send_sre_email", python_callable=send_sre_email, provide_context=True)
    
    # Execution order
    [fetch_this, fetch_prev] >> gen_engine >> gen_windows >> gen_cpu_peaks >> gen_mem_peaks
    [gen_engine, gen_windows, gen_cpu_peaks, gen_mem_peaks] >> ai_summary
    ai_summary >> compile_report >> generate_pdf >> convert_to_html >> send_sre_email


