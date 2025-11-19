from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task  
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
SMTP_USER = Variable.get("ltai.v1.sretradeideas.SMTP_USER")
SMTP_PASSWORD = Variable.get("ltai.v1.sretradeideas.SMTP_PASSWORD")
SMTP_HOST = Variable.get("ltai.v1.sretradeideas.SMTP_HOST", default_var="mail.authsmtp.com")
SMTP_PORT = int(Variable.get("ltai.v1.sretradeideas.SMTP_PORT", default_var="2525"))
SMTP_SUFFIX = Variable.get("ltai.v1.sretradeideas.SMTP_FROM_SUFFIX", default_var="via lowtouch.ai <webmaster@ecloudcontrol.com>")
SENDER_EMAIL = Variable.get("ltai.v1.sretradeideas.TRADEIDEAS_FROM_ADDRESS", default_var=SMTP_USER)
RECEIVER_EMAIL = Variable.get("ltai.v1.sretradeideas.TRADEIDEAS_TO_ADDRESS", default_var=SENDER_EMAIL)

OLLAMA_HOST = Variable.get("ltai.v1.sretradeideas.TRADEIDEAS_OLLAMA_HOST", "http://agentomatic:8000/")

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

# === Generic Report Generator ===
def generate_sre_report(ti, key, description, period="last 7 days"):
    prompt = f"You are the SRE TradeIdeas agent. Generate a **complete {description} report for {period}**"
    response = get_ai_response(prompt)
    ti.xcom_push(key=key, value=response)
    return response

# === This Week Reports ===
def node_cpu_this_week(ti, **context): return generate_sre_report(ti, "node_cpu_this_week", "node-level CPU utilization", "last 7 days")
def node_memory_this_week(ti, **context): return generate_sre_report(ti, "node_memory_this_week", "node-level memory utilization", "last 7 days")
def node_disk_this_week(ti, **context): return generate_sre_report(ti, "node_disk_this_week", "node-level disk utilization", "last 7 days")
def node_readiness_check(ti, **context): return generate_sre_report(ti, "node_readiness_check", "node readiness check")
def pod_restart_this_week(ti, **context): return generate_sre_report(ti, "pod_restart_this_week", "pod restart count", "last 7 days")
def mysql_health_this_week(ti, **context): return generate_sre_report(ti, "mysql_health_this_week", "MySQL health status", "last 7 days")
def kubernetes_version_check(ti, **context): return generate_sre_report(ti, "kubernetes_version_check", "Kubernetes version check")
def kubernetes_eol_and_next_version(ti, **context): return generate_sre_report(ti, "kubernetes_eol_and_next_version", "Kubernetes Current Version EOL Details and Next Supported Kubernetes Version")
def microk8s_expiry_check(ti, **context): return generate_sre_report(ti, "microk8s_expiry_check", "MicroK8s master node certificate expiry check")
def lke_pvc_storage_details(ti, **context): return generate_sre_report(ti, "lke_pvc_storage_details", "LKE PVC storage details with disk sizes", "last 7 days")

# === Previous Week Reports ===
def node_cpu_last_week(ti, **context): return generate_sre_report(ti, "node_cpu_last_week", "node-level CPU utilization", "previous week")
def node_memory_last_week(ti, **context): return generate_sre_report(ti, "node_memory_last_week", "node-level memory utilization", "previous week")
def node_disk_last_week(ti, **context): return generate_sre_report(ti, "node_disk_last_week", "node-level disk utilization", "previous week")
def mysql_health_last_week(ti, **context): return generate_sre_report(ti, "mysql_health_last_week", "MySQL health status", "previous week")
def lke_pvc_storage_details_last_week(ti, **context): return generate_sre_report(ti, "lke_pvc_storage_details_last_week", "LKE PVC storage details with disk sizes", "previous week")

# === Comparison Functions (This Week vs Previous Week) ===
def generate_sre_comparison(ti, key, report_name, data_this_key, data_last_key, instructions):
    data_this = ti.xcom_pull(key=data_this_key)
    data_last = ti.xcom_pull(key=data_last_key)
    prompt = f"""
You are the SRE TradeIdeas agent.
**This Week's {report_name} Data:** {data_this}
**Previous Week's {report_name} Data:** {data_last}
---
{instructions}
"""
    response = get_ai_response(prompt)
    ti.xcom_push(key=key, value=response)
    return response

# Reuse same comparison logic with updated period labels
def node_cpu_this_vs_last_week(ti, **context):
    instructions = """
Each record contains: `instance_ip`, `node_name`, `avg_cpu`, `max_cpu`.
**Compute**: `avg_cpu_diff`, `max_cpu_diff` (round 4 decimals)
**Output exactly this:**
### CPU Utilization Comparison - Node Level (This Week: [period1] , Previous Week: [period2])
| Node Name | Instance IP | Avg CPU (%) - This Week | Avg CPU (%) - Previous Week | Avg CPU Diff (%) | Max CPU (%) - This Week | Max CPU (%) - Previous Week | Max CPU Diff (%) |
### Summary
List nodes with |max_diff| > 20%, else “No significant CPU changes.”
"""
    return generate_sre_comparison(ti, "node_cpu_this_vs_last_week", "Node-Level CPU Utilization", "node_cpu_this_week", "node_cpu_last_week", instructions)

def node_memory_this_vs_last_week(ti, **context):
    instructions = """
Each record contains: `instance_ip`, `node_name`, `total_memory_gb`, `avg_available_gb`, `max_usage_percent`.
**Compute**: `avg_avail_diff`, `max_usage_diff` (round 2 decimals)
**Output exactly this:**
### Memory Utilization Comparison - Node Level (This Week: [period1] , Previous Week: [period2])
| Instance IP | Node Name | Total Mem (GB) | Avg Avail This Week | Avg Avail Previous Week | Avg Diff (GB) | Max Usage This Week (%) | Max Usage Previous Week (%) | Max Diff (%) |
### Summary
List instances with |max_diff| > 20%, else “No significant memory issues.”
"""
    return generate_sre_comparison(ti, "node_memory_this_vs_last_week", "Node-Level Memory Utilization", "node_memory_this_week", "node_memory_last_week", instructions)

def node_disk_this_vs_last_week(ti, **context):
    instructions = """
Each record contains: `instance_ip`, `node_name`, `mountpoint`, `used_percent`.
**Compute**: `used_diff` (round 2 decimals)
**Output exactly this:**
### Disk Utilization Comparison - Node Level (This Week: [period1] , Previous Week: [period2])
| Instance IP | Node Name | Mountpoint | Used (%) - This Week | Used (%) - Previous Week | Diff (%) |
### Summary
Show entries with |used_diff| > 20%, else “No significant disk issues.”
"""
    return generate_sre_comparison(ti, "node_disk_this_vs_last_week", "Node-Level Disk Utilization", "node_disk_this_week", "node_disk_last_week", instructions)

def mysql_health_this_vs_last_week(ti, **context):
    instructions = """
Each record contains: `status`, `downtime_count`, `total_downtime_seconds`, `avg_probe_duration_seconds`.
**Compute**: `count_diff`, `duration_diff` (round 1 decimal)
**Output exactly this:**
### MySQL Health Comparison (This Week: [period1] , Previous Week: [period2])
| Endpoint | Status This Week | Status Previous Week | Count This Week | Count Previous Week | Diff | Duration This Week | Duration Previous Week | Duration Diff (s) |
### Summary
Describe downtime trends and current status.
"""
    return generate_sre_comparison(ti, "mysql_health_this_vs_last_week", "MySQL Health Status", "mysql_health_this_week", "mysql_health_last_week", instructions)

def lke_pvc_this_vs_last_week(ti, **context):
    instructions = """
Each record contains: `pvc_name`, `namespace`, `storageclass`, `capacity_gib`, `used_current_gib`, `used_prev_gib`, etc.
**Output exactly this:**
### LKE PVC Storage Comparison (This Week: [period1] , Previous Week: [period2])
| Persistent Volume Claim | Namespace   | Storage Class      | Capacity (GiB) | Used This Week (GiB) | Used Previous Week (GiB) | Used Diff (GiB) | Available This Week | Available Previous Week | Available Diff |
### Summary
[Concise summary of storage growth or anomalies]
"""
    return generate_sre_comparison(ti, "lke_pvc_this_vs_last_week", "LKE PVC Storage Comparison", "lke_pvc_storage_details", "lke_pvc_storage_details_last_week", instructions)

# === Pod Metrics (Dynamic per Namespace) ===
@task
def pod_metrics_for_namespace(ns: str, period: str):
    prompts = {
        "running_pods": f"Return only the count of running pods in namespace `{ns}` for {period}. Plain number only.",
        "problematic_pods": f"List only Failed/Pending/Unknown pods in `{ns}` for {period}. Format: - pod-name (phase)",
        "cpu": f"Generate pod-level CPU utilization table for namespace `{ns}` for {period}. Columns: pod, avg_cpu_cores, max_cpu_cores.",
        "memory": f"Generate pod-level memory utilization table for namespace `{ns}` for {period}. Columns: pod, avg_memory_gb, max_memory_gb."
    }
    results = {"namespace": ns, "period": period}
    for key, prompt in prompts.items():
        if key == "running_pods":
            raw = get_ai_response(prompt)
            results["total_running_pods"] = int(re.search(r'\d+', raw).group()) if re.search(r'\d+', raw) else 0
        elif key == "problematic_pods":
            results["problematic_pods"] = get_ai_response(prompt).strip() or "No problematic pods"
        else:
            results[key] = get_ai_response(prompt).strip()
    return results

@task
def compile_pod_sections_this_week(results: list, **context):
    ti = context['ti']
    sections = []
    namespaces = json.loads(Variable.get("ltai.v1.sretradeideas.pod.namespaces", '["alpha-prod","tipreprod-prod"]'))
    data_map = {r["namespace"]: r for r in results if r["period"] == "last 7 days"}
    for ns in namespaces:
        if ns not in data_map: continue
        d = data_map[ns]
        sections.append(f"### Namespace: `{ns}`\n**Running Pods**: {d['total_running_pods']} | **Problematic Pods**: {d['problematic_pods']}\n#### CPU\n{d['cpu']}\n#### Memory\n{d['memory']}\n---\n")
    result = "\n".join(sections)
    ti.xcom_push(key="pod_this_week_markdown", value=result)
    return result

@task
def compile_pod_comparison_weekly(all_results: list, **context):
    ti = context['ti']
    this_week = {r["namespace"]: r for r in all_results if r["period"] == "last 7 days"}
    last_week = {r["namespace"]: r for r in all_results if r["period"] == "previous week"}
    namespaces = json.loads(Variable.get("ltai.v1.sretradeideas.pod.namespaces", '["alpha-prod","tipreprod-prod"]'))
    sections = []
    for ns in namespaces:
        if ns not in this_week or ns not in last_week: continue
        prompt = f"""
This Week CPU/Mem: {this_week[ns]['cpu']} {this_week[ns]['memory']}
Previous Week CPU/Mem: {last_week[ns]['cpu']} {last_week[ns]['memory']}
Generate comparison tables for {ns} pod CPU and memory (This Week vs Previous Week).
Highlight pods with significant changes (>0.8 cores or >1 GB).
"""
        sections.append(get_ai_response(prompt))
    result = "\n\n---\n\n".join(sections)
    ti.xcom_push(key="pod_comparison_weekly", value=result)
    return result

# === FULLY DETAILED OVERALL SUMMARY (like daily) ===
def overall_summary(ti, **context):
    node_cpu_this       = ti.xcom_pull(key="node_cpu_this_week") or "No data"
    node_memory_this    = ti.xcom_pull(key="node_memory_this_week") or "No data"
    node_disk_this      = ti.xcom_pull(key="node_disk_this_week") or "No data"
    node_readiness      = ti.xcom_pull(key="node_readiness_check") or "No data"
    pod_this_week       = ti.xcom_pull(key="pod_this_week_markdown") or "No data"
    pod_restarts        = ti.xcom_pull(key="pod_restart_this_week") or "No data"
    mysql_this_week     = ti.xcom_pull(key="mysql_health_this_week") or "No data"
    k8s_version         = ti.xcom_pull(key="kubernetes_version_check") or "No data"
    k8s_eol             = ti.xcom_pull(key="kubernetes_eol_and_next_version") or "No data"
    cert_expiry         = ti.xcom_pull(key="microk8s_expiry_check") or "No data"
    pvc_this_week       = ti.xcom_pull(key="lke_pvc_storage_details") or "No data"

    node_cpu_cmp        = ti.xcom_pull(key="node_cpu_comparison") or "No comparison"
    node_mem_cmp        = ti.xcom_pull(key="node_memory_comparison") or "No comparison"
    node_disk_cmp       = ti.xcom_pull(key="node_disk_comparison") or "No comparison"
    pod_cmp             = ti.xcom_pull(key="pod_comparison_weekly") or "No comparison"
    mysql_cmp           = ti.xcom_pull(key="mysql_health_comparison") or "No comparison"
    pvc_cmp             = ti.xcom_pull(key="lke_pvc_comparison") or "No comparison"

    prompt = f"""
You are the SRE TradeIdeas agent.
Generate a **complete Weekly SRE Summary** and **Week-over-Week Insights**.

### This Week Summary 
- Node CPU: {node_cpu_this}
- Node Memory: {node_memory_this}
- Node Disk: {node_disk_this}
- Node Readiness: {node_readiness}
- Pod Metrics: {pod_this_week}
- Pod Restarts: {pod_restarts}
- MySQL Health: {mysql_this_week}
- PVC Storage: {pvc_this_week}
- Kubernetes Version: {k8s_version}
- Kubernetes EOL: {k8s_eol}
- Certificate Expiry: {cert_expiry}

### Week-over-Week Comparison
- Node CPU Changes: {node_cpu_cmp}
- Node Memory Changes: {node_mem_cmp}
- Node Disk Changes: {node_disk_cmp}
- Pod CPU/Memory Changes: {pod_cmp}
- PVC Storage Growth: {pvc_cmp}
- MySQL Downtime Changes: {mysql_cmp}

Write two clear sections:
1. **Weekly Summary (This Week)**
2. **Week-over-Week Comparison & Alerts**
Highlight any critical regressions or anomalies.
"""
    response = get_ai_response(prompt)
    ti.xcom_push(key="overall_summary", value=response)
    return response


# === FULLY DETAILED REPORT COMPILATION (exact same style as daily) ===
def compile_sre_report(ti, **context):
    report = f"""
# SRE Weekly Report – TradeIdeas Platform
 **Generated**: Monday 11:00 AM IST

---

## 1. Node-Level Metrics (Last 7 Days)
{ti.xcom_pull(key="node_cpu_this_week") or "No data"}
{ti.xcom_pull(key="node_memory_this_week") or "No data"}
{ti.xcom_pull(key="node_disk_this_week") or "No data"}
{ti.xcom_pull(key="node_readiness_check") or "No data"}

---

## 2. Pod-Level Metrics (Last 7 Days)
{ti.xcom_pull(key="pod_this_week_markdown") or "No data"}

---

## 3. Pod Restart Count (Last 7 Days)
{ti.xcom_pull(key="pod_restart_this_week") or "No data"}

---

## 4. Storage (LKE PVCs)
{ti.xcom_pull(key="lke_pvc_storage_details") or "No data"}

---

## 5. Database Health
{ti.xcom_pull(key="mysql_health_this_week") or "No data"}

---

## 6. System Status
{ti.xcom_pull(key="kubernetes_version_check") or "No data"}
{ti.xcom_pull(key="kubernetes_eol_and_next_version") or "No data"}
{ti.xcom_pull(key="microk8s_expiry_check") or "No data"}

---

## 7. Week-over-Week Comparison
{ti.xcom_pull(key="node_cpu_comparison") or "No comparison"}
{ti.xcom_pull(key="node_memory_comparison") or "No comparison"}
{ti.xcom_pull(key="node_disk_comparison") or "No comparison"}
{ti.xcom_pull(key="pod_comparison_weekly") or "No comparison"}
{ti.xcom_pull(key="lke_pvc_comparison") or "No comparison"}
{ti.xcom_pull(key="mysql_health_comparison") or "No comparison"}

---

## 8. Overall Weekly Summary & Recommendations
{ti.xcom_pull(key="overall_summary") or "No summary"}

---

**End of Weekly Report**  
*Generated by SRE TradeIdeas Agent – Every Monday @ 11:00 AM IST*
"""
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
    """Send SRE HTML report via SMTP """
    html_report = ti.xcom_pull(key="sre_html_report")

    if not html_report or "<html" not in html_report.lower():
        logging.error("No valid HTML report found in XCom.")
        raise ValueError("HTML report missing or invalid.")

    # Clean up any code block wrappers
    html_body = re.sub(r'```html\s*|```', '', html_report).strip()

    subject = f"SRE Weekly Report – {datetime.utcnow().strftime('%Y-%m-%d')}"
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
    dag_id="sre-tradeideas-weekly",
    default_args=default_args,
    schedule_interval="30 5 * * 1",  # 05:30 UTC on Monday = 11:00 AM IST
    catchup=False,
    tags=["sre", "tradeideas", "weekly", "11am-ist"],
    max_active_runs=1,
) as dag:

   # Static tasks
    t1  = PythonOperator(task_id="node_cpu_this_week", python_callable=node_cpu_this_week)
    t2  = PythonOperator(task_id="node_memory_this_week", python_callable=node_memory_this_week)
    t3  = PythonOperator(task_id="node_disk_this_week", python_callable=node_disk_this_week)
    t4  = PythonOperator(task_id="node_readiness_check", python_callable=node_readiness_check)
    t5  = PythonOperator(task_id="pod_restart_this_week", python_callable=pod_restart_this_week)
    t6  = PythonOperator(task_id="mysql_health_this_week", python_callable=mysql_health_this_week)
    t7  = PythonOperator(task_id="kubernetes_version_check", python_callable=kubernetes_version_check)
    t7b = PythonOperator(task_id="kubernetes_eol_and_next_version", python_callable=kubernetes_eol_and_next_version)
    t8  = PythonOperator(task_id="microk8s_expiry_check", python_callable=microk8s_expiry_check)
    t9  = PythonOperator(task_id="lke_pvc_this_week", python_callable=lke_pvc_storage_details)

    t10 = PythonOperator(task_id="node_cpu_last_week", python_callable=node_cpu_last_week)
    t11 = PythonOperator(task_id="node_memory_last_week", python_callable=node_memory_last_week)
    t12 = PythonOperator(task_id="node_disk_last_week", python_callable=node_disk_last_week)
    t13 = PythonOperator(task_id="mysql_health_last_week", python_callable=mysql_health_last_week)
    t14 = PythonOperator(task_id="lke_pvc_last_week", python_callable=lke_pvc_storage_details_last_week)

    c1 = PythonOperator(task_id="compare_cpu", python_callable=node_cpu_this_vs_last_week)
    c2 = PythonOperator(task_id="compare_memory", python_callable=node_memory_this_vs_last_week)
    c3 = PythonOperator(task_id="compare_disk", python_callable=node_disk_this_vs_last_week)
    c4 = PythonOperator(task_id="compare_mysql", python_callable=mysql_health_this_vs_last_week)
    c5 = PythonOperator(task_id="compare_pvc", python_callable=lke_pvc_this_vs_last_week)

    summary = PythonOperator(task_id="overall_summary", python_callable=overall_summary)
    compile = PythonOperator(task_id="compile_report", python_callable=compile_sre_report)
    html    = PythonOperator(task_id="convert_to_html", python_callable=convert_to_html)
    email   = PythonOperator(task_id="send_email", python_callable=send_sre_email)

    # === POD DYNAMIC FLOW (fixed) =================================================
    pod_namespaces_var = Variable.get(
        "ltai.v1.sretradeideas.pod.namespaces",
        default_var='["alpha-prod","tipreprod-prod"]'
    )
    try:
        namespaces_list = json.loads(pod_namespaces_var)
        if not isinstance(namespaces_list, list):
            raise ValueError("Parsed value is not a list")
                  
        logging.info(
            "Successfully loaded pod namespaces from Airflow Variable 'ltai.v1.sretradeideas.pod.namespaces': %s", namespaces_list)
    
    except Exception as e:
        logging.warning(
            "Failed to parse Airflow Variable 'ltai.v1.sretradeideas.pod.namespaces' (value was: %s). "
            "Reason: %s. Falling back to hard-coded default namespaces.", pod_namespaces_var, str(e))
        namespaces_list = ["alpha-prod", "tipreprod-prod"]

    # === 1. THIS WEEK ===
    this_week_results = (
        pod_metrics_for_namespace.partial(period="last 7 days")
        .expand(ns=namespaces_list)
    )

    # === 2. Previous Week ===
    previous_week_results = (
        pod_metrics_for_namespace.partial(period="previous week")
        .expand(ns=namespaces_list)
    )

    # === 3. THIS WEEK MARKDOWN ===
    pod_this_week_markdown = compile_pod_sections_this_week(this_week_results)

    # === 4. COMPARISON MARKDOWN ===
    @task
    def collect_pod_results(this_week_res, previous_week_res):
        this_list = list(this_week_res) if not isinstance(this_week_res, list) else this_week_res
        previous_list = list(previous_week_res) if not isinstance(previous_week_res, list) else previous_week_res
        return this_list + previous_list

    all_pod_results = collect_pod_results(this_week_results, previous_week_results)
    pod_comparison_weekly = compile_pod_comparison_weekly(all_pod_results)


    # Dependencies
    [t1, t2, t3, t4, t5, t6, t7, t7b, t8, t9, t10, t11, t12, t13, t14] >> c1 >> c2 >> c3 >> c4 >> c5
    this_week_results >> pod_this_week_markdown
    [this_week_results, previous_week_results] >> pod_comparison_weekly
    [c1, c2, c3, c4, c5, pod_comparison_weekly, pod_this_week_markdown] >> summary >> compile >> html >> email
