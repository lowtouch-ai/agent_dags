from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
from ollama import Client
from airflow.models import Variable
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import base64
import json
import re
import html
import smtplib
from email.mime.image import MIMEImage

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
SMTP_USER = Variable.get("SMTP_USER")
SMTP_PASSWORD = Variable.get("SMTP_PASSWORD")
SMTP_HOST = Variable.get("SMTP_HOST", default_var="mail.authsmtp.com")
SMTP_PORT = int(Variable.get("SMTP_PORT", default_var="2525"))
SMTP_SUFFIX = Variable.get("SMTP_FROM_SUFFIX", default_var="via lowtouch.ai <webmaster@ecloudcontrol.com>")
SENDER_EMAIL = Variable.get("TRADEIDEAS_FROM_ADDRESS", default_var=SMTP_USER)
RECEIVER_EMAIL = Variable.get("TRADEIDEAS_TO_ADDRESS", default_var=SENDER_EMAIL)

OLLAMA_HOST = Variable.get("TRADEIDEAS_OLLAMA_HOST", "http://agentomatic:8000/")

YESTERDAY = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
TODAY = datetime.utcnow().strftime('%Y-%m-%d')

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
            model='appz/sre/tradeideas:0.3',
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

# === Generic helper to generate SRE reports ===
def generate_sre_report(ti, key, description, period=None):
    """
    Generic function to generate SRE TradeIdeas reports.

    Args:
        ti: Airflow TaskInstance for pushing XCom.
        key: The key under which the response is stored (used for XCom).
        description: A short description of what report to generate.
        period: 'today', 'yesterday', or None (default: None).
    """
    if period:
        prompt = f"""
        You are the SRE TradeIdeas agent.
        Generate a **complete {description} report for {period}**
        """
    else:
        prompt = f"""
        You are the SRE TradeIdeas agent.
        Generate a **complete {description} report**
        """

    response = get_ai_response(prompt)
    ti.xcom_push(key=key, value=response)
    return response


# === Node CPU ===
def node_cpu_today(ti, **context):
    return generate_sre_report(ti, "node_cpu_today", "node-level CPU utilization", "today")

def node_cpu_yesterday(ti, **context):
    return generate_sre_report(ti, "node_cpu_yesterday", "node-level CPU utilization", "yesterday")


# === Node Memory ===
def node_memory_today(ti, **context):
    return generate_sre_report(ti, "node_memory_today", "node-level memory utilization", "today")

def node_memory_yesterday(ti, **context):
    return generate_sre_report(ti, "node_memory_yesterday", "node-level memory utilization", "yesterday")


# === Node Disk ===
def node_disk_today(ti, **context):
    return generate_sre_report(ti, "node_disk_today", "node-level disk utilization", "today")

def node_disk_yesterday(ti, **context):
    return generate_sre_report(ti, "node_disk_yesterday", "node-level disk utilization", "yesterday")


# === Node Readiness ===
def node_readiness_check(ti, **context):
    return generate_sre_report(ti, "node_readiness_check", "node readiness check")


# === Pod CPU ===
def pod_cpu_today(ti, **context):
    return generate_sre_report(ti, "pod_cpu_today", "pod-level CPU utilization", "today")

def pod_cpu_yesterday(ti, **context):
    return generate_sre_report(ti, "pod_cpu_yesterday", "pod-level CPU utilization", "yesterday")


# === Pod Memory ===
def pod_memory_today(ti, **context):
    return generate_sre_report(ti, "pod_memory_today", "pod-level memory utilization", "today")

def pod_memory_yesterday(ti, **context):
    return generate_sre_report(ti, "pod_memory_yesterday", "pod-level memory utilization", "yesterday")


# === Pod Restart ===
def pod_restart_today(ti, **context):
    return generate_sre_report(ti, "pod_restart_today", "pod restart count", "today")


# === MySQL Health ===
def mysql_health_today(ti, **context):
    return generate_sre_report(ti, "mysql_health_today", "MySQL health status", "today")

def mysql_health_yesterday(ti, **context):
    return generate_sre_report(ti, "mysql_health_yesterday", "MySQL health status", "yesterday")


# === MicroK8s Checks ===
def microk8s_version_check(ti, **context):
    return generate_sre_report(ti, "microk8s_version_check", "MicroK8s version check")

def microk8s_expiry_check(ti, **context):
    return generate_sre_report(ti, "microk8s_expiry_check", "MicroK8s master node certificate expiry check")


# === LKE PVC Storage Details ===
def lke_pvc_storage_details(ti, **context):
    return generate_sre_report(ti, "lke_pvc_storage_details", "LKE PVC storage details with disk sizes")


def generate_sre_comparison(
    ti,
    key,
    report_name,
    data_today_key,
    data_yesterday_key,
    instructions
):
    """
    Generic function to compare 'today' vs 'yesterday' SRE reports.

    Args:
        ti: Airflow TaskInstance.
        key: XCom key to push the result under.
        report_name: Name/title of the report (e.g., "Node CPU Utilization").
        data_today_key: XCom key for today's data.
        data_yesterday_key: XCom key for yesterday's data.
        instructions: String containing the AI comparison instructions (specific to each metric).
    """
    data_today = ti.xcom_pull(key=data_today_key)
    data_yesterday = ti.xcom_pull(key=data_yesterday_key)

    prompt = f"""
You are the SRE TradeIdeas agent.

**Today's {report_name} Data:**
{data_today}

**Yesterday's {report_name} Data:**
{data_yesterday}

---

{instructions}
"""

    response = get_ai_response(prompt)
    ti.xcom_push(key=key, value=response)
    return response


def node_cpu_today_vs_yesterday(ti, **context):
    instructions = """
Each record contains: `instance_ip`, `node_name`, `avg_cpu`, `max_cpu`.

**Compute**:
- `avg_cpu_diff = today.avg_cpu - yesterday.avg_cpu`
- `max_cpu_diff = today.max_cpu - yesterday.max_cpu` (round 4 decimals)

**Output exactly this:**

### CPU Utilization Comparison - Node Level (Period1: 2025-11-07, Period2: 2025-11-06)
| Instance IP | Node Name | Avg CPU (%) - P1 | Avg CPU (%) - P2 | Avg Diff (%) | Max CPU (%) - P1 | Max CPU (%) - P2 | Max Diff (%) |
|-------------|-----------|------------------|------------------|---------------|------------------|------------------|---------------|
| [ip]        | [node]    | [p1_avg]         | [p2_avg]         | [avg_diff]    | [p1_max]         | [p2_max]         | [max_diff]    |

### Summary
List nodes with |max_diff| > 20%, else “No significant CPU changes.”
"""
    return generate_sre_comparison(
        ti,
        key="node_cpu_today_vs_yesterday",
        report_name="Node-Level CPU Utilization",
        data_today_key="node_cpu_today",
        data_yesterday_key="node_cpu_yesterday",
        instructions=instructions
    )


def node_memory_today_vs_yesterday(ti, **context):
    instructions = """
Each record contains: `instance_ip`, `node_name`, `total_memory_gb`, `avg_available_gb`, `max_usage_percent`.

**Compute**:
- `avg_avail_diff = today.avg_available_gb - yesterday.avg_available_gb`
- `max_usage_diff = today.max_usage_percent - yesterday.max_usage_percent` (round 2 decimals)

**Output exactly this:**

### Memory Utilization Comparison - Node Level (Period1: 2025-11-07, Period2: 2025-11-06)
| Instance IP | Node Name | Total Mem (GB) - P1 | Total Mem (GB) - P2 | Avg Avail (GB) - P1 | Avg Avail (GB) - P2 | Avg Diff (GB) | Max Usage (%) - P1 | Max Usage (%) - P2 | Max Diff (%) |
|-------------|-----------|---------------------|---------------------|--------------------|--------------------|----------------|--------------------|--------------------|---------------|

### Summary
List instances with |max_diff| > 20%, else “No significant memory issues.”
"""
    return generate_sre_comparison(
        ti,
        key="node_memory_today_vs_yesterday",
        report_name="Node-Level Memory Utilization",
        data_today_key="node_memory_today",
        data_yesterday_key="node_memory_yesterday",
        instructions=instructions
    )


def node_disk_today_vs_yesterday(ti, **context):
    instructions = """
Each record contains: `instance_ip`, `node_name`, `mountpoint`, `used_percent`.

**Compute**:
- `used_diff = today.used_percent - yesterday.used_percent` (round 2 decimals)

**Output exactly this:**

### Disk Utilization Comparison - Node Level (Period1: 2025-11-07, Period2: 2025-11-06)
| Instance IP | Node Name | Mountpoint | Used (%) - P1 | Used (%) - P2 | Diff (%) |
|-------------|-----------|------------|---------------|---------------|-----------|

### Summary
Show entries with |used_diff| > 20%, else “No significant disk issues.”
"""
    return generate_sre_comparison(
        ti,
        key="node_disk_today_vs_yesterday",
        report_name="Node-Level Disk Utilization",
        data_today_key="node_disk_today",
        data_yesterday_key="node_disk_yesterday",
        instructions=instructions
    )


def pod_cpu_today_vs_yesterday(ti, **context):
    instructions = """
Each record contains: `namespace`, `pod`, `avg_cpu_cores`, `max_cpu_cores`.

Compute:
- `avg_cpu_diff` and `max_cpu_diff` (round 4 decimals).

Output: one table per namespace.

**Output exactly this:**

### [namespace] Pod CPU Comparison (P1: 2025-11-07, P2: 2025-11-06)
| Pod Name | Avg CPU (cores) - P1 | Avg CPU (cores) - P2 | Avg Diff | Max CPU (cores) - P1 | Max CPU (cores) - P2 | Max Diff |
|-----------|---------------------|---------------------|----------|----------------------|----------------------|----------|

### Summary
List pods with |max_diff| > 0.8 cores, else “No CPU spikes.”
"""
    return generate_sre_comparison(
        ti,
        key="pod_cpu_today_vs_yesterday",
        report_name="Pod-Level CPU Utilization",
        data_today_key="pod_cpu_today",
        data_yesterday_key="pod_cpu_yesterday",
        instructions=instructions
    )


def pod_memory_today_vs_yesterday(ti, **context):
    instructions = """
Each record contains: `namespace`, `pod`, `avg_memory_gb`, `max_memory_gb`.

Compute:
- `avg_memory_diff` and `max_memory_diff` (round 4 decimals).

Output: one table per namespace.

**Output exactly this:**

### [namespace] Pod Memory Comparison (P1: 2025-11-07, P2: 2025-11-06)
| Pod Name | Avg Memory (GB) - P1 | Avg Memory (GB) - P2 | Avg Diff (GB) | Max Memory (GB) - P1 | Max Memory (GB) - P2 | Max Diff (GB) |
|-----------|---------------------|---------------------|----------------|----------------------|----------------------|----------------|

### Summary
List pods with |max_diff| > 1.0 GB, else “No memory pressure.”
"""
    return generate_sre_comparison(
        ti,
        key="pod_memory_today_vs_yesterday",
        report_name="Pod-Level Memory Utilization",
        data_today_key="pod_memory_today",
        data_yesterday_key="pod_memory_yesterday",
        instructions=instructions
    )


def mysql_health_today_vs_yesterday(ti, **context):
    instructions = """
Each record contains: `status`, `downtime_count`, `total_downtime_seconds`, `avg_probe_duration_seconds`.

Compute:
- `count_diff` and `duration_diff` (round 1 decimal).

**Output exactly this:**

### MySQL Health Comparison (Period1: 2025-11-07, Period2: 2025-11-06)
| Endpoint | Status P1 | Status P2 | Count P1 | Count P2 | Diff | Duration P1 | Duration P2 | Duration Diff (s) | Probe P1 | Probe P2 |
|-----------|------------|------------|----------|----------|------|--------------|--------------|-------------------|----------|----------|

### Summary
Describe downtime and final status comparison.
"""
    return generate_sre_comparison(
        ti,
        key="mysql_health_today_vs_yesterday",
        report_name="MySQL Health Status",
        data_today_key="mysql_health_today",
        data_yesterday_key="mysql_health_yesterday",
        instructions=instructions
    )


# === tX: Overall Summary (Today + Comparison) ===
def overall_summary(ti, **context):
    """
    Generate an overall summary combining today's metrics and comparisons
    across all monitored components.
    """

    # --- Today's Data ---
    node_cpu       = ti.xcom_pull(key="node_cpu_today") or "No CPU data"
    node_memory    = ti.xcom_pull(key="node_memory_today") or "No memory data"
    node_disk      = ti.xcom_pull(key="node_disk_today") or "No disk data"
    node_readiness = ti.xcom_pull(key="node_readiness_check") or "No readiness data"
    pod_cpu        = ti.xcom_pull(key="pod_cpu_today") or "No pod CPU data"
    pod_memory     = ti.xcom_pull(key="pod_memory_today") or "No pod memory data"
    pod_restart    = ti.xcom_pull(key="pod_restart_today") or "No restart data"
    mysql_health   = ti.xcom_pull(key="mysql_health_today") or "No MySQL data"
    microk8s_ver   = ti.xcom_pull(key="microk8s_version_check") or "No MicroK8s version data"
    microk8s_exp   = ti.xcom_pull(key="microk8s_expiry_check") or "No certificate data"
    lke_pvc        = ti.xcom_pull(key="lke_pvc_storage_details") or "No PVC data"

    # --- Comparison Data ---
    node_cpu_cmp   = ti.xcom_pull(key="node_cpu_today_vs_yesterday") or "No comparison data"
    node_mem_cmp   = ti.xcom_pull(key="node_memory_today_vs_yesterday") or "No comparison data"
    node_disk_cmp  = ti.xcom_pull(key="node_disk_today_vs_yesterday") or "No comparison data"
    pod_cpu_cmp    = ti.xcom_pull(key="pod_cpu_today_vs_yesterday") or "No comparison data"
    pod_mem_cmp    = ti.xcom_pull(key="pod_memory_today_vs_yesterday") or "No comparison data"
    mysql_cmp      = ti.xcom_pull(key="mysql_health_today_vs_yesterday") or "No comparison data"

    # --- Build Prompt for AI Summary ---
    prompt = f"""
    You are the SRE TradeIdeas agent.

    Your task: Generate a **complete overall summary** for today's SRE report,
    followed by a **comparative summary** analyzing performance trends versus yesterday.

    ### Part 1: Today's Summary
    Use the data below to provide a high-level assessment of current cluster health.

    #### Today's Section Data:
    - Node CPU Report: {node_cpu}
    - Node Memory Report: {node_memory}
    - Node Disk Report: {node_disk}
    - Node Readiness: {node_readiness}
    - Pod CPU Report: {pod_cpu}
    - Pod Memory Report: {pod_memory}
    - Pod Restart Summary: {pod_restart}
    - MySQL Health: {mysql_health}
    - MicroK8s Version: {microk8s_ver}
    - MicroK8s Certificate Expiry: {microk8s_exp}
    - LKE PVC Storage Details: {lke_pvc}

    ### Part 2: Comparison Summary (Today vs Yesterday)
    Use the comparison metrics to highlight improvements, degradations, or stability across systems.

    #### Comparison Data:
    - Node CPU Comparison: {node_cpu_cmp}
    - Node Memory Comparison: {node_mem_cmp}
    - Node Disk Comparison: {node_disk_cmp}
    - Pod CPU Comparison: {pod_cpu_cmp}
    - Pod Memory Comparison: {pod_mem_cmp}
    - MySQL Health Comparison: {mysql_cmp}

    ### Instructions:
    1. Write two clearly separated sections:
       - **Overall Summary (Today)**
       - **Comparison Summary (Today vs Yesterday)**
    2. Use professional tone, concise sentences, and structured paragraphs.
    3. Highlight critical alerts, anomalies, and areas of improvement.
    4. Summarize key positives and potential issues.
    5. Avoid repeating detailed metrics — focus on interpretation and trends.
    """

    # --- Generate AI Response ---
    response = get_ai_response(prompt)

    # --- Push to XCom ---
    ti.xcom_push(key="overall_summary", value=response)

    return response

# === t10: Compile SRE Report ===
def compile_sre_report(ti, **context):
    # Pull all XComs for today
    node_cpu       = ti.xcom_pull(key="node_cpu_today") or "No CPU data"
    node_memory    = ti.xcom_pull(key="node_memory_today") or "No memory data"
    node_disk      = ti.xcom_pull(key="node_disk_today") or "No disk data"
    node_readiness = ti.xcom_pull(key="node_readiness_check") or "No readiness data"
    pod_cpu        = ti.xcom_pull(key="pod_cpu_today") or "No pod CPU data"
    pod_memory     = ti.xcom_pull(key="pod_memory_today") or "No pod memory data"
    pod_restart    = ti.xcom_pull(key="pod_restart_today") or "No restart data"
    mysql_health   = ti.xcom_pull(key="mysql_health_today") or "No MySQL data"
    microk8s_ver   = ti.xcom_pull(key="microk8s_version_check") or "No MicroK8s version data"
    microk8s_exp   = ti.xcom_pull(key="microk8s_expiry_check") or "No cert data"
    lke_pvc        = ti.xcom_pull(key="lke_pvc_storage_details") or "No PVC data"

    # --- Generate Overall Summary ---
    overall_summary = ti.xcom_pull(key="overall_summary") or "No overall summary available."

    # XComs for yesterday comparisons
    node_cpu_cmp     = ti.xcom_pull(key="node_cpu_today_vs_yesterday") or "No comparison"
    node_mem_cmp     = ti.xcom_pull(key="node_memory_today_vs_yesterday") or "No comparison"
    node_disk_cmp    = ti.xcom_pull(key="node_disk_today_vs_yesterday") or "No comparison"
    pod_cpu_cmp      = ti.xcom_pull(key="pod_cpu_today_vs_yesterday") or "No comparison"
    pod_mem_cmp      = ti.xcom_pull(key="pod_memory_today_vs_yesterday") or "No comparison"
    mysql_cmp        = ti.xcom_pull(key="mysql_health_today_vs_yesterday") or "No comparison"

    # Compile final markdown
    report = f"""
# SRE Daily Report – TradeIdeas Platform
**Generated**: **11:00 AM IST**

---

## 1. Node-Level Metrics (Last 24h)
{node_cpu}

---

{node_memory}

---

{node_disk}

---

{node_readiness}

---

## 2. Pod-Level Metrics (Last 24h)
{pod_cpu}

---

{pod_memory}

---

{pod_restart}

---

## 3. Database Health
{mysql_health}

---

## 4. Certificate Status

{microk8s_ver}

---

{microk8s_exp}

---

## 5. Storage (LKE PVCs)
{lke_pvc}

---

## 6. Node-Level Metrics (Today vs Yesterday)
{node_cpu_cmp}

---

{node_mem_cmp}

---

{node_disk_cmp}

---

## 7. Pod-Level Metrics (Today vs Yesterday)
{pod_cpu_cmp}

---

{pod_mem_cmp}

---

## 8. Database Health (Today vs Yesterday)
{mysql_cmp}

---

## 9. Overall Summary
{overall_summary}

---

**End of Report**  
*Generated by SRE TradeIdeas Agent @ 11:00 AM IST*
"""

    # Clean up extra newlines
    report = re.sub(r'\n{3,}', '\n\n', report.strip())

    ti.xcom_push(key="sre_full_report", value=report)
    logging.info("SRE report compiled successfully.")
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
    """Send SRE HTML report via SMTP instead of Gmail API"""
    html_report = ti.xcom_pull(key="sre_html_report")

    if not html_report or "<html" not in html_report.lower():
        logging.error("No valid HTML report found in XCom.")
        raise ValueError("HTML report missing or invalid.")

    # Clean up any code block wrappers
    html_body = re.sub(r'```html\s*|```', '', html_report).strip()

    subject = f"SRE Daily Report – {datetime.utcnow().strftime('%Y-%m-%d')}"
    recipient = RECEIVER_EMAIL

    try:
        # Initialize SMTP connection
        logging.info(f"Connecting to SMTP server {SMTP_HOST}:{SMTP_PORT} as {SMTP_USER}")
        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15)
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)

        # Prepare email
        msg = MIMEMultipart("related")
        msg["Subject"] = subject
        msg["From"] = f"TradeIdeas SRE Agent {SMTP_SUFFIX}"
        msg["To"] = recipient

        # Attach the HTML body
        msg.attach(MIMEText(html_body, "html"))

        # Optional: Inline image support (if your DAG attaches graphs later)
        chart_b64 = ti.xcom_pull(key="chart_b64")
        if chart_b64:
            try:
                img_data = base64.b64decode(chart_b64)
                img_part = MIMEImage(img_data, 'png')
                img_part.add_header('Content-ID', '<chart_image>')
                img_part.add_header('Content-Disposition', 'inline', filename='chart.png')
                msg.attach(img_part)
                logging.info("Attached chart image to email.")
            except Exception as e:
                logging.warning(f"Failed to attach inline image: {str(e)}")

        # Send the email
        server.sendmail(SENDER_EMAIL, recipient, msg.as_string())
        server.quit()

        logging.info(f"Email sent successfully to {recipient}")
        return f"Email sent successfully to {recipient}"

    except Exception as e:
        logging.error(f"Failed to send email via SMTP: {str(e)}")
        raise


# === DAG ===
with DAG(
    dag_id="sre-tradeideas",
    default_args=default_args,
    schedule_interval="30 5 * * *",  # 05:30 UTC = 11:00 AM IST
    catchup=False,
    tags=["sre", "tradeideas", "daily", "11am-ist"],
    max_active_runs=1,
) as dag:

   # TODAY TASKS
    t1 = PythonOperator(
        task_id="node_cpu_today",
        python_callable=node_cpu_today,
        provide_context=True,
    )

    t2 = PythonOperator(
        task_id="node_memory_today",
        python_callable=node_memory_today,
        provide_context=True,
    )

    t3 = PythonOperator(
        task_id="node_disk_today",
        python_callable=node_disk_today,
        provide_context=True,
    )

    t4= PythonOperator(
        task_id="node_readiness_check",
        python_callable=node_readiness_check,
        provide_context=True,
    )

    t5 = PythonOperator(
        task_id="pod_cpu_today",
        python_callable=pod_cpu_today,
        provide_context=True,
    )

    t6 = PythonOperator(
        task_id="pod_memory_today",
        python_callable=pod_memory_today,
        provide_context=True,
    )

    t7 = PythonOperator(
        task_id="pod_restart_today",
        python_callable=pod_restart_today,
        provide_context=True,
    )

    t8 = PythonOperator(
        task_id="mysql_health_today",
        python_callable=mysql_health_today,
        provide_context=True,
    )

    t9 = PythonOperator(
        task_id="microk8s_version_check",
        python_callable=microk8s_version_check,
        provide_context=True,
    )

    t10 = PythonOperator(
        task_id="microk8s_expiry_check",
        python_callable=microk8s_expiry_check,
        provide_context=True,
    )

    t11 = PythonOperator(
        task_id="lke_pvc_storage_details",
        python_callable=lke_pvc_storage_details,
        provide_context=True,
    )


    # YESTERDAY TASKS
    t12 = PythonOperator(
        task_id="node_cpu_yesterday",
        python_callable=node_cpu_yesterday,
        provide_context=True,
    )

    t13 = PythonOperator(
        task_id="node_memory_yesterday",
        python_callable=node_memory_yesterday,
        provide_context=True,
    )

    t14 = PythonOperator(
        task_id="node_disk_yesterday",
        python_callable=node_disk_yesterday,
        provide_context=True,
    )

    t15 = PythonOperator(
        task_id="pod_cpu_yesterday",
        python_callable=pod_cpu_yesterday,
        provide_context=True,
    )

    t16 = PythonOperator(
        task_id="pod_memory_yesterday",
        python_callable=pod_memory_yesterday,
        provide_context=True,
    )

    t17 = PythonOperator(
        task_id="mysql_health_yesterday",
        python_callable=mysql_health_yesterday,
        provide_context=True,
    )


    # COMPARISON TASKS
    t18 = PythonOperator(
        task_id="node_cpu_today_vs_yesterday",
        python_callable=node_cpu_today_vs_yesterday,
        provide_context=True,
    )

    t19 = PythonOperator(
        task_id="node_memory_today_vs_yesterday",
        python_callable=node_memory_today_vs_yesterday,
        provide_context=True,
    )

    t20 = PythonOperator(
        task_id="node_disk_today_vs_yesterday",
        python_callable=node_disk_today_vs_yesterday,
        provide_context=True,
    )

    t21 = PythonOperator(
        task_id="pod_cpu_today_vs_yesterday",
        python_callable=pod_cpu_today_vs_yesterday,
        provide_context=True,
    )

    t22 = PythonOperator(
        task_id="pod_memory_today_vs_yesterday",
        python_callable=pod_memory_today_vs_yesterday,
        provide_context=True,
    )

    t23 = PythonOperator(
        task_id="mysql_health_today_vs_yesterday",
        python_callable=mysql_health_today_vs_yesterday,
        provide_context=True,
    )


    t24 = PythonOperator(
        task_id="overall_summary",
        python_callable=overall_summary,
        provide_context=True,
    )

    # FINAL REPORTING
    t25 = PythonOperator(
        task_id="compile_sre_report",
        python_callable=compile_sre_report,
        provide_context=True,
    )

    t26 = PythonOperator(
        task_id="convert_to_html",
        python_callable=convert_to_html,
        provide_context=True,
    )

    t27 = PythonOperator(
        task_id="send_sre_email",
        python_callable=send_sre_email,
        provide_context=True,
    )


    # FINAL DAG CHAIN
    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9 >> t10 >> t11 >> t12 >> t13 >> t14 >> t15 >> t16 >> t17 >> t18 >> t19 >> t20 >> t21 >> t22 >> t23 >> t24 >> t25 >> t26 >> t27
