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
    prompt = f"You are the SRE TradeIdeas agent. Generate a **complete {description} report for {period or 'last 24 hours'}**"
    response = get_ai_response(prompt)
    ti.xcom_push(key=key, value=response)
    return response

# === Node CPU ===
def node_cpu_today(ti, **context): return generate_sre_report(ti, "node_cpu_today", "node-level CPU utilization", "last 24 hours")
def node_cpu_yesterday(ti, **context): return generate_sre_report(ti, "node_cpu_yesterday", "node-level CPU utilization", "yesterday")

# === Node Memory ===
def node_memory_today(ti, **context): return generate_sre_report(ti, "node_memory_today", "node-level memory utilization", "last 24 hours")
def node_memory_yesterday(ti, **context): return generate_sre_report(ti, "node_memory_yesterday", "node-level memory utilization", "yesterday")

# === Node Disk ===
def node_disk_today(ti, **context): return generate_sre_report(ti, "node_disk_today", "node-level disk utilization", "last 24 hours")
def node_disk_yesterday(ti, **context): return generate_sre_report(ti, "node_disk_yesterday", "node-level disk utilization", "yesterday")

# === Node Readiness ===
def node_readiness_check(ti, **context): return generate_sre_report(ti, "node_readiness_check", "node readiness check")

# === Pod Restart (today only) ===
def pod_restart_today(ti, **context): return generate_sre_report(ti, "pod_restart_today", "pod restart count", "last 24 hours")

# === MySQL Health ===
def mysql_health_today(ti, **context): return generate_sre_report(ti, "mysql_health_today", "MySQL health status", "last 24 hours")
def mysql_health_yesterday(ti, **context): return generate_sre_report(ti, "mysql_health_yesterday", "MySQL health status", "yesterday")

# === MicroK8s Checks ===
def kubernetes_version_check(ti, **context): return generate_sre_report(ti, "kubernetes_version_check", "Kubernetes version check")
def microk8s_expiry_check(ti, **context): return generate_sre_report(ti, "microk8s_expiry_check", "MicroK8s master node certificate expiry check")

# === LKE PVC Storage Details ===
def lke_pvc_storage_details(ti, **context): return generate_sre_report(ti, "lke_pvc_storage_details", "LKE PVC storage details with disk sizes")
def lke_pvc_storage_details_yesterday(ti, **context): return generate_sre_report(ti, "lke_pvc_storage_details_yesterday", "LKE PVC storage details with disk sizes", "yesterday")


# === Comparison Functions ===
def generate_sre_comparison(ti, key, report_name, data_today_key, data_yesterday_key, instructions):
    data_today = ti.xcom_pull(key=data_today_key)
    data_yesterday = ti.xcom_pull(key=data_yesterday_key)
    prompt = f"""
You are the SRE TradeIdeas agent.
**Today's {report_name} Data:** {data_today}
**Yesterday's {report_name} Data:** {data_yesterday}
---
{instructions}
"""
    response = get_ai_response(prompt)
    ti.xcom_push(key=key, value=response)
    return response

# Node Comparisons
def node_cpu_today_vs_yesterday(ti, **context):
    instructions = """
Each record contains: `instance_ip`, `node_name`, `avg_cpu`, `max_cpu`.
**Compute**: `avg_cpu_diff`, `max_cpu_diff` (round 4 decimals)
**Output exactly this:**
### CPU Utilization Comparison - Node Level (Period1: [period1_date], Period2: [period2_date])
| Node Name | Instance IP | Avg CPU (%) - Period1 | Avg CPU (%) - Period2 | Avg CPU Diff (%) | Max CPU (%) - Period1 | Max CPU (%) - Period2 | Max CPU Diff (%) |
### Summary
List nodes with |max_diff| > 20%, else “No significant CPU changes.”
"""
    return generate_sre_comparison(ti, "node_cpu_today_vs_yesterday", "Node-Level CPU Utilization", "node_cpu_today", "node_cpu_yesterday", instructions)

def node_memory_today_vs_yesterday(ti, **context):
    instructions = """
Each record contains: `instance_ip`, `node_name`, `total_memory_gb`, `avg_available_gb`, `max_usage_percent`.
**Compute**: `avg_avail_diff`, `max_usage_diff` (round 2 decimals)
**Output exactly this:**
### Memory Utilization Comparison - Node Level (Period1: [period1_date], Period2: [period2_date])
| Instance IP | Node Name | Total Mem (GB) - P1 | Total Mem (GB) - P2 | Avg Avail (GB) - P1 | Avg Avail (GB) - P2 | Avg Diff (GB) | Max Usage (%) - P1 | Max Usage (%) - P2 | Max Diff (%) |
### Summary
List instances with |max_diff| > 20%, else “No significant memory issues.”
"""
    return generate_sre_comparison(ti, "node_memory_today_vs_yesterday", "Node-Level Memory Utilization", "node_memory_today", "node_memory_yesterday", instructions)

def node_disk_today_vs_yesterday(ti, **context):
    instructions = """
Each record contains: `instance_ip`, `node_name`, `mountpoint`, `used_percent`.
**Compute**: `used_diff` (round 2 decimals)
**Output exactly this:**
### Disk Utilization Comparison - Node Level (Period1: [period1_date], Period2: [period2_date])
| Instance IP | Node Name | Mountpoint | Used (%) - P1 | Used (%) - P2 | Diff (%) |
### Summary
Show entries with |used_diff| > 20%, else “No significant disk issues.”
"""
    return generate_sre_comparison(ti, "node_disk_today_vs_yesterday", "Node-Level Disk Utilization", "node_disk_today", "node_disk_yesterday", instructions)

def mysql_health_today_vs_yesterday(ti, **context):
    instructions = """
Each record contains: `status`, `downtime_count`, `total_downtime_seconds`, `avg_probe_duration_seconds`.
**Compute**: `count_diff`, `duration_diff` (round 1 decimal)
**Output exactly this:**
### MySQL Health Comparison (Period1: [period1_date], Period2: [period2_date])
| Endpoint | Status P1 | Status P2 | Count P1 | Count P2 | Diff | Duration P1 | Duration P2 | Duration Diff (s) | Probe P1 | Probe P2 |
### Summary
Describe downtime and final status comparison.
"""
    return generate_sre_comparison(ti, "mysql_health_today_vs_yesterday", "MySQL Health Status", "mysql_health_today", "mysql_health_yesterday", instructions)

def lke_pvc_today_vs_yesterday(ti, **context):
    instructions = """
Each record contains: `pvc_name`, `namespace`, `storageclass`, `capacity_gib`, `used_current_gib`, `used_prev_gib`, `available_current_gib`, `available_prev_gib`.
**Compute** (round to 2 decimals): `used_diff_gib` = used_current_gib - used_prev_gib, `available_diff_gib` = available_current_gib - available_prev_gib, `used_percent_current` = round((used_current_gib / capacity_gib) * 100, 2).
**Output exactly this:**
### LKE PVC Storage Comparison (Period1: [period1_date], Period2: [period2_date])
| Persistent Volume Claim | Namespace   | Storage Class      | Capacity (GiB) | Used Current (GiB) | Used Previous (GiB) | Used Diff (GiB) | Available Current (GiB) | Available Previous (GiB) | Available Diff (GiB) |
|-------------------------|-------------|--------------------|----------------|--------------------|---------------------|-----------------|-------------------------|--------------------------|----------------------|
| [pvc_name]              | [namespace] | [storageclass]     | [capacity_gib] | [used_current_gib] | [used_prev_gib]     | [used_diff_gib] | [available_current_gib] | [available_prev_gib]     | [available_diff_gib] |

### Summary
[Generate a concise paragraph summarizing the comparison of storage details]
"""
    return generate_sre_comparison(ti, "lke_pvc_today_vs_yesterday", "LKE PVC Storage Comparison", "lke_pvc_storage_details", "lke_pvc_storage_details_yesterday", instructions)

# === POD-LEVEL DYNAMIC TASKS (Today + Yesterday + Comparison) ===
@task
def pod_metrics_for_namespace(ns: str, period: str):
    """Fetch CPU and Memory + Running Pods count + Problematic Pods (Failed/Pending/Unknown)"""
    
    # 1. Number of running pods
    count_prompt = f"""
    Execute this exact Prometheus query and return only the number:
    count( kube_pod_status_phase{{namespace="{ns}", phase="Running"}} == 1 )
    Give only the plain number, nothing else.
    """
    total_running_pods = get_ai_response(count_prompt).strip()

    # 2. Problematic pods (Failed, Pending, Unknown) – list only bad ones
    problem_prompt = f"""
    Execute this exact query:
    sum by (pod, namespace, phase) (kube_pod_status_phase{{namespace="{ns}", phase=~"Failed|Pending|Unknown"}} == 1)

    If result is empty → respond exactly: No problematic pods
    If result has values → return only the problematic pods in this format (one per line):
    - <pod-name> (<phase>)
    Example:
    - auth-service-abc123 (Failed)
    - payment-worker-xyz (Pending)
    Do not include namespace, timestamps, or any extra text.
    """
    problematic_pods = get_ai_response(problem_prompt).strip()

    # 3. CPU table (unchanged)
    cpu_prompt = f"""
    Generate the **pod-level CPU utilization** for namespace `{ns}` for {period}.
    Return a markdown table with columns: pod, avg_cpu_cores, max_cpu_cores.
    """
    cpu = get_ai_response(cpu_prompt)

    # 4. Memory table (unchanged)
    mem_prompt = f"""
    Generate the **pod-level memory utilization** for namespace `{ns}` for {period}.
    Return a markdown table with columns: pod, avg_memory_gb, max_memory_gb.
    """
    mem = get_ai_response(mem_prompt)

    return {
        "namespace": ns,
        "period": period,
        "total_running_pods": total_running_pods,
        "problematic_pods": problematic_pods,
        "cpu": cpu.strip(),
        "memory": mem.strip()
    }

@task
def compile_pod_sections_today(namespace_results: list, **context):
    ti = context['ti']
    sections = []
    first_ns = "alphaprod"
    ns_map = {r["namespace"]: r for r in namespace_results if r and r["period"] == "last 24 hours"}
    
    if first_ns in ns_map:
        data = ns_map.pop(first_ns)
        sections.append(f"### Namespace: `{first_ns}`\n")
        sections.append(f"**Running Pods**: {data['total_running_pods']} | **Problematic Pods**: {data['problematic_pods']}\n")
        sections.append("#### CPU\n" + data["cpu"] + "\n")
        sections.append("#### Memory\n" + data["memory"] + "\n")
        sections.append("---\n")
    
    for ns in sorted(ns_map.keys()):
        data = ns_map[ns]
        sections.append(f"### Namespace: `{ns}`\n")
        sections.append(f"**Running Pods**: {data['total_running_pods']} | **Problematic Pods**: {data['problematic_pods']}\n")
        sections.append("#### CPU\n" + data["cpu"] + "\n")
        sections.append("#### Memory\n" + data["memory"] + "\n")
        sections.append("---\n")
    
    result = "\n".join(sections)
    
    # CRITICAL: Push with expected key
    ti.xcom_push(key="pod_today_markdown", value=result)
    
    return result
@task
def compile_pod_comparison(namespace_results: list, **context):
    ti = context['ti']
    sections = []
    first_ns = "alphaprod"
    today_map = {r["namespace"]: r for r in namespace_results if r and r["period"] == "last 24 hours"}
    yesterday_map = {r["namespace"]: r for r in namespace_results if r and r["period"] == "yesterday"}

    def compare_ns(ns):
        today = today_map.get(ns, {})
        yesterday = yesterday_map.get(ns, {})
        if not today or not yesterday:
            return None
        prompt = f"""
You are the SRE TradeIdeas agent.
**Today's Data (P1):** {today["cpu"]} {today["memory"]}
**Yesterday's Data (P2):** {yesterday["cpu"]} {yesterday["memory"]}
**Output exactly this:**
### {ns} Pod CPU & Memory Comparison (Period1: [period1_date], Period2: [period2_date])
#### CPU
| Pod Name | Avg CPU (cores) - P1 | Avg CPU (cores) - P2 | Avg Diff | Max CPU (cores) - P1 | Max CPU (cores) - P2 | Max Diff |
|----------|----------------------|----------------------|----------|----------------------|----------------------|----------|
#### Memory
| Pod Name | Avg Memory (GB) - P1 | Avg Memory (GB) - P2 | Avg Diff (GB) | Max Memory (GB) - P1 | Max Memory (GB) - P2 | Max Diff (GB) |
|----------|----------------------|----------------------|---------------|----------------------|----------------------|----------------|
### Summary
List pods with |max_cpu_diff| > 0.8 cores or |max_mem_diff| > 1.0 GB, else “No significant changes.”
"""
        return get_ai_response(prompt)

    if first_ns in today_map and first_ns in yesterday_map:
        comp = compare_ns(first_ns)
        if comp:
            sections.append(comp)
    
    for ns in sorted(set(today_map.keys()) & set(yesterday_map.keys())):
        if ns != first_ns:
            comp = compare_ns(ns)
            if comp:
                sections.append(comp)
    
    result = "\n\n---\n\n".join(sections)
    
    # CRITICAL: Push with expected key
    ti.xcom_push(key="pod_comparison_markdown", value=result)
    
    return result


# === Overall Summary ===
def overall_summary(ti, **context):
    def xp(key, default="No data"): return ti.xcom_pull(key=key) or default
    node_cpu = xp("node_cpu_today"); node_memory = xp("node_memory_today"); node_disk = xp("node_disk_today")
    node_readiness = xp("node_readiness_check"); pod_today = xp("pod_today_markdown", "No pod data")
    pod_restart = xp("pod_restart_today"); mysql_health = xp("mysql_health_today")
    kubernetes_ver = xp("kubernetes_version_check"); microk8s_exp = xp("microk8s_expiry_check"); lke_pvc = xp("lke_pvc_storage_details")
    node_cpu_cmp = xp("node_cpu_today_vs_yesterday"); node_mem_cmp = xp("node_memory_today_vs_yesterday")
    node_disk_cmp = xp("node_disk_today_vs_yesterday"); pod_cmp = xp("pod_comparison_markdown", "No comparison")
    mysql_cmp = xp("mysql_health_today_vs_yesterday"); pvc_cmp = xp("lke_pvc_today_vs_yesterday")

    prompt = f"""
You are the SRE TradeIdeas agent.
Generate a **complete overall summary** for today's SRE report, followed by a **comparative summary**.

### Part 1: Today's Summary
- Node CPU: {node_cpu}
- Node Memory: {node_memory}
- Node Disk: {node_disk}
- Node Readiness: {node_readiness}
- Pod Metrics (by Namespace): {pod_today}
- Pod Restarts: {pod_restart}
- MySQL Health: {mysql_health}
- LKE PVC: {lke_pvc}
- MicroK8s Version: {kubernetes_ver}
- MicroK8s Expiry: {microk8s_exp}

### Part 2: Comparison Summary
- Node CPU: {node_cpu_cmp}
- Node Memory: {node_mem_cmp}
- Node Disk: {node_disk_cmp}
- Pod CPU & Memory: {pod_cmp}
- LKE PVC: {pvc_cmp}
- MySQL Health: {mysql_cmp}

Write two sections: **Overall Summary (Today)** and **Comparison Summary (Today vs Yesterday)**.
Highlight critical alerts and anomalies.
"""
    response = get_ai_response(prompt)
    ti.xcom_push(key="overall_summary", value=response)
    return response


# === Compile SRE Report ===
def compile_sre_report(ti, **context):
    def xp(key, default="No data"): return ti.xcom_pull(key=key) or default
    node_cpu = xp("node_cpu_today"); node_memory = xp("node_memory_today"); node_disk = xp("node_disk_today")
    node_readiness = xp("node_readiness_check"); pod_today = xp("pod_today_markdown", "No pod data")
    pod_restart = xp("pod_restart_today"); mysql_health = xp("mysql_health_today")
    kubernetes_ver = xp("kubernetes_version_check"); microk8s_exp = xp("microk8s_expiry_check"); lke_pvc = xp("lke_pvc_storage_details")
    node_cpu_cmp = xp("node_cpu_today_vs_yesterday"); node_mem_cmp = xp("node_memory_today_vs_yesterday")
    node_disk_cmp = xp("node_disk_today_vs_yesterday"); pod_cmp = xp("pod_comparison_markdown", "No comparison")
    mysql_cmp = xp("mysql_health_today_vs_yesterday"); pvc_cmp = xp("lke_pvc_today_vs_yesterday")
    overall_summary = xp("overall_summary", "No summary")

    report = f"""
# SRE Daily Report – TradeIdeas Platform
**Generated**: **11:00 AM IST**

---

## 1. Node-Level Metrics (Last 24h)
{node_cpu}
{node_memory}
{node_disk}
{node_readiness}

---

## 2. Pod-Level Metrics (Last 24h)
{pod_today}

---

## 3. Pod Restart Count (Last 24h)
{pod_restart}

---

## 4. Storage (LKE PVCs)
{lke_pvc}

---
    
## 5. Database Health
{mysql_health}

---

## 6. Certificate Status
{kubernetes_ver}
{microk8s_exp}



---

## 7. Node-Level Metrics (Today vs Yesterday)
{node_cpu_cmp}
{node_mem_cmp}
{node_disk_cmp}

---

## 8. Pod-Level Metrics (Today vs Yesterday)
{pod_cmp}

---

## 9. LKE PVC Storage Details (Today vs Yesterday)
{pvc_cmp}

---

## 10. Database Health (Today vs Yesterday)
{mysql_cmp}

---

## 11. Overall Summary
{overall_summary}

---

**End of Report**  
*Generated by SRE TradeIdeas Agent @ 11:00 AM IST*
""".strip()

    report = re.sub(r'\n{3,}', '\n\n', report)
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

   # Static Tasks - renumbered sequentially
    t1 = PythonOperator(task_id="node_cpu_today", python_callable=node_cpu_today, provide_context=True)
    t2 = PythonOperator(task_id="node_memory_today", python_callable=node_memory_today, provide_context=True)
    t3 = PythonOperator(task_id="node_disk_today", python_callable=node_disk_today, provide_context=True)
    t4 = PythonOperator(task_id="node_readiness_check", python_callable=node_readiness_check, provide_context=True)
    t5 = PythonOperator(task_id="pod_restart_today", python_callable=pod_restart_today, provide_context=True)
    t6 = PythonOperator(task_id="mysql_health_today", python_callable=mysql_health_today, provide_context=True)
    t7 = PythonOperator(task_id="kubernetes_version_check", python_callable=kubernetes_version_check, provide_context=True)
    t8 = PythonOperator(task_id="microk8s_expiry_check", python_callable=microk8s_expiry_check, provide_context=True)
    t9 = PythonOperator(task_id="lke_pvc_storage_details", python_callable=lke_pvc_storage_details, provide_context=True)
    
    t10 = PythonOperator(task_id="node_cpu_yesterday", python_callable=node_cpu_yesterday, provide_context=True)
    t11 = PythonOperator(task_id="node_memory_yesterday", python_callable=node_memory_yesterday, provide_context=True)
    t12 = PythonOperator(task_id="node_disk_yesterday", python_callable=node_disk_yesterday, provide_context=True)
    t13 = PythonOperator(task_id="lke_pvc_storage_details_yesterday", python_callable=lke_pvc_storage_details_yesterday, provide_context=True)
    t14 = PythonOperator(task_id="mysql_health_yesterday", python_callable=mysql_health_yesterday, provide_context=True)
    
    t15 = PythonOperator(task_id="node_cpu_today_vs_yesterday", python_callable=node_cpu_today_vs_yesterday, provide_context=True)
    t16 = PythonOperator(task_id="node_memory_today_vs_yesterday", python_callable=node_memory_today_vs_yesterday, provide_context=True)
    t17 = PythonOperator(task_id="node_disk_today_vs_yesterday", python_callable=node_disk_today_vs_yesterday, provide_context=True)
    t18 = PythonOperator(task_id="lke_pvc_today_vs_yesterday", python_callable=lke_pvc_today_vs_yesterday, provide_context=True)
    t19 = PythonOperator(task_id="mysql_health_today_vs_yesterday", python_callable=mysql_health_today_vs_yesterday, provide_context=True)
    
    t20 = PythonOperator(task_id="overall_summary", python_callable=overall_summary, provide_context=True)
    t21 = PythonOperator(task_id="compile_sre_report", python_callable=compile_sre_report, provide_context=True)
    t22 = PythonOperator(task_id="convert_to_html", python_callable=convert_to_html, provide_context=True)
    t23 = PythonOperator(task_id="send_sre_email", python_callable=send_sre_email, provide_context=True)
    
    # === POD DYNAMIC FLOW (fixed) =================================================
    pod_namespaces_var = Variable.get(
        "ltai.v1.sretradeideas.pod.namespaces",
        default_var='["alpha-prod","tipreprod-prod"]'
    )
    try:
        namespaces_list = json.loads(pod_namespaces_var)
    except Exception:
        namespaces_list = ["alpha-prod", "tipreprod-prod"]
    
    # 1. TODAY
    today_results = (
        pod_metrics_for_namespace.partial(period="last 24 hours")
        .expand(ns=namespaces_list)
    )
    
    # 2. YESTERDAY
    yesterday_results = (
        pod_metrics_for_namespace.partial(period="yesterday")
        .expand(ns=namespaces_list)
    )
    
    # 3. TODAY MARKDOWN
    pod_today_markdown = compile_pod_sections_today(today_results)
    
    # 4. COMPARISON MARKDOWN
    @task
    def collect_pod_results(today_res, yesterday_res):
        today_list = list(today_res) if not isinstance(today_res, list) else today_res
        yesterday_list = list(yesterday_res) if not isinstance(yesterday_res, list) else yesterday_res
        return today_list + yesterday_list
    
    all_pod_results = collect_pod_results(today_results, yesterday_results)
    pod_comparison_markdown = compile_pod_comparison(all_pod_results)
    
    # === DEPENDENCIES =====================================
    # Node + static tasks (all previous static tasks feed into comparisons)
    [t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14] >> t15 >> t16 >> t17 >> t18 >> t19
    
    # Pod flow
    t4 >> today_results
    t4 >> yesterday_results
    today_results >> pod_today_markdown
    [yesterday_results, pod_today_markdown] >> all_pod_results
    all_pod_results >> pod_comparison_markdown
    
    # Final chain
    pod_comparison_markdown >> t20
    t19 >> t20 >> t21 >> t22 >> t23
