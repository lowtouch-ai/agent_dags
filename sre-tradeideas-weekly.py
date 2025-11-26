from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task, task_group
from typing import List
from datetime import datetime, timedelta
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
POD_BATCH_SIZE = 6

# === Precise Date & Time Helpers (computed once per DAG run) ===
def _get_last_7_days_range():
    """Returns (start_iso, end_iso) for last 7 full days (UTC)"""
    end_date = (datetime.utcnow() - timedelta(days=7)).date()
    start_date = end_date - timedelta(days=6)
    start = f"{start_date}T00:00:00Z"
    end = f"{end_date}T23:59:59Z"
    return start, end

# For use in prompts
PREVIOUS_WEEK_START, PREVIOUS_WEEK_END = _get_last_7_days_range()
PREVIOUS_WEEK_FULL_RANGE = f"previous week ({PREVIOUS_WEEK_START} to {PREVIOUS_WEEK_END})"

# For display
PREVIOUS_WEEK_DATE_STR = (datetime.utcnow() - timedelta(days=7)).strftime('%Y-%m-%d')

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

# === Enhanced Generic helper to generate SRE reports ===
def generate_sre_report(ti, key, description, period=None):
    if period == "previous week":
        period = PREVIOUS_WEEK_FULL_RANGE
    elif period is None:
        period = "last 7 days"

    prompt = f"""You are the SRE TradeIdeas agent.
Generate a **complete {description} report** for the period: **{period}**.
Be precise about the time range in your response.
"""
    response = get_ai_response(prompt)
    ti.xcom_push(key=key, value=response)
    return response

# === Node CPU ===
def node_cpu_this_week(ti, **context): return generate_sre_report(ti, "node_cpu_this_week", "node-level CPU utilization", "last 7 days")
def node_cpu_last_week(ti, **context): return generate_sre_report(ti, "node_cpu_last_week", "node-level CPU utilization", "previous week")

# === Node Memory ===
def node_memory_this_week(ti, **context): return generate_sre_report(ti, "node_memory_this_week", "node-level memory utilization", "last 7 days")
def node_memory_last_week(ti, **context): return generate_sre_report(ti, "node_memory_last_week", "node-level memory utilization", "previous week")

# === Node Disk ===
def node_disk_this_week(ti, **context): return generate_sre_report(ti, "node_disk_this_week", "node-level disk utilization", "last 7 days")
def node_disk_last_week(ti, **context): return generate_sre_report(ti, "node_disk_last_week", "node-level disk utilization", "previous week")

# === Node Readiness ===
def node_readiness_check(ti, **context): return generate_sre_report(ti, "node_readiness_check", "node readiness check")

# === Pod Restart ===
def pod_restart_this_week(ti, **context): return generate_sre_report(ti, "pod_restart_this_week", "pod restart count", "last 7 days")

# === MySQL Health ===
def mysql_health_this_week(ti, **context): return generate_sre_report(ti, "mysql_health_this_week", "MySQL health status", "last 7 days")
def mysql_health_last_week(ti, **context): return generate_sre_report(ti, "mysql_health_last_week", "MySQL health status", "previous week")

# === Certificate Checks ===
def kubernetes_version_check(ti, **context): return generate_sre_report(ti, "kubernetes_version_check", "Kubernetes version check")
def kubernetes_eol_and_next_version(ti, **context): return generate_sre_report(ti, "kubernetes_eol_and_next_version", "Kubernetes Current Version EOL Details and Next Supported Kubernetes Version")
def microk8s_expiry_check(ti, **context): return generate_sre_report(ti, "microk8s_expiry_check", "MicroK8s master node certificate expiry check")

# === LKE PVC Storage Details ===
def lke_pvc_storage_details(ti, **context): return generate_sre_report(ti, "lke_pvc_storage_details", "LKE PVC storage details with disk sizes", "last 7 days")
def lke_pvc_storage_details_last_week(ti, **context): return generate_sre_report(ti, "lke_pvc_storage_details_last_week", "LKE PVC storage details with disk sizes", "previous week")

# === Comparison Functions ===
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

def node_cpu_this_vs_last_week(ti, **context):
    instructions = """
Each record contains: `instance_ip`, `node_name`, `avg_cpu`, `max_cpu`.
**Compute**: `avg_cpu_diff`, `max_cpu_diff` (round 4 decimals)
**Output exactly this:**
### CPU Utilization Comparison - Node Level (This Week vs Previous Week)
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
### Memory Utilization Comparison - Node Level (This Week vs Previous Week)
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
### Disk Utilization Comparison - Node Level (This Week vs Previous Week)
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
### MySQL Health Comparison (This Week vs Previous Week)
| Endpoint | Status This Week | Status Previous Week | Count This Week | Count Previous Week | Diff | Duration This Week | Duration Previous Week | Duration Diff (s) |
### Summary
Describe downtime trends and current status.
"""
    return generate_sre_comparison(ti, "mysql_health_this_vs_last_week", "MySQL Health Status", "mysql_health_this_week", "mysql_health_last_week", instructions)

def lke_pvc_this_vs_last_week(ti, **context):
    instructions = """
Each record contains: `pvc_name`, `namespace`, `storageclass`, `capacity_gib`, `used_current_gib`, `used_prev_gib`, etc.
**Output exactly this:**
### LKE PVC Storage Comparison (This Week vs Previous Week)
| Persistent Volume Claim | Namespace   | Storage Class      | Capacity (GiB) | Used This Week (GiB) | Used Previous Week (GiB) | Used Diff (GiB) | Available This Week | Available Previous Week | Available Diff |
### Summary
[Concise summary of storage growth or anomalies]
"""
    return generate_sre_comparison(ti, "lke_pvc_this_vs_last_week", "LKE PVC Storage Comparison", "lke_pvc_storage_details", "lke_pvc_storage_details_last_week", instructions)

# === POD-LEVEL DYNAMIC TASKS (This Week + Previous Week + Comparison) ===
@task
def pod_metrics_for_namespace(ns: str, period: str):
    """Fetch CPU and Memory + Running Pods count + Problematic Pods (Failed/Pending/Unknown)"""

    # 1. Number of running pods
    count_prompt = f"""
    Execute this exact Prometheus query and return only the number:
    count( kube_pod_status_phase{{namespace="{ns}", phase="Running"}} == 1 )
    Give only the plain number, nothing else.
    """
    total_running_pods = int(re.search(r'\d+', get_ai_response(count_prompt) or '').group() or 0)

    # 2. Problematic pods (Failed, Pending, Unknown) – list only bad ones
    problem_prompt = f"""
    Execute this exact query:
    sum by (pod, namespace, phase) (kube_pod_status_phase{{namespace="{ns}", phase=~"Failed|Pending|Unknown"}} == 1)
    If result is empty → respond exactly: No problematic pods
    If result has values → return only the problematic pods in this format:
    - <pod-name> (<phase>)
    """
    problematic_pods = get_ai_response(problem_prompt).strip()

    cpu_prompt = f"""
    Generate the **pod-level CPU utilization** for namespace `{ns}` for {period}.
    Return a markdown table with columns: pod, avg_cpu_cores, max_cpu_cores.
    """
    cpu = get_ai_response(cpu_prompt)

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
def get_real_pod_counts(namespaces: list) -> dict:
    result = {}
    for ns in namespaces:
        prompt = f"""
        Execute this exact Prometheus query and return ONLY the number:
        count(kube_pod_status_phase{{namespace="{ns}", phase="Running"}} == 1)
        """
        response = get_ai_response(prompt).strip()
        num = re.search(r'\d+', response)
        result[ns] = int(num.group()) if num else 0
    return result

@task
def compile_pod_sections_this_week(namespace_results: list, real_counts: dict, **context):
    ti = context['ti']
    logging.info("compile_pod_sections_today: merging %d batches", len(namespace_results))

    # Load ordered namespaces
    raw = Variable.get("ltai.v1.sretradeideas.pod.namespaces", default_var='["alpha-prod","tipreprod-prod"]')
    try:
        namespaces = json.loads(raw)
    except:
        namespaces = [ns.strip() for ns in raw.split(",") if ns.strip()]

    # Merge all batches
    merged = {}

    for batch in namespace_results:
        if not batch or not isinstance(batch, dict): continue
        key = (batch["namespace"], batch["period"])
        if key not in merged:
            merged[key] = {"cpu": [], "memory": []}

        def is_valid_row(line):
            line = line.strip()
            if not line.startswith("|") or line.count("|") < 4: return False
            parts = [p.strip() for p in line.split("|")[1:-1]]
            if len(parts) < 3 or parts[0] in ["pod", "Pod", "", "Namespace"]: return False
            return any(c.isdigit() or c == "." for c in "".join(parts[1:]))

        for line in batch["cpu"].splitlines():
            if is_valid_row(line):
                merged[key]["cpu"].append(line.strip())
        for line in batch["memory"].splitlines():
            if is_valid_row(line):
                merged[key]["memory"].append(line.strip())

    sections = []
    for ns in namespaces:
        key_this = (ns, "last 7 days")
        if key_this not in merged: continue

        data = merged[key_this]
        actual_count = real_counts.get(ns, "unknown")
        problematic = context['ti'].xcom_pull(task_ids='fetch_real_problematic_pods').get(ns, "No problematic pods")

        sections.append(f"### Namespace: `{ns}`")
        sections.append(f"**Running Pods**: {actual_count} | **Problematic Pods**: {problematic}\n")

        def safe_float(s): 
                try: return float(s) 
                except: return 0.0
                
        if data["cpu"]:
            sections.append("#### CPU Utilization (Last 7 Days)")
            sections.append("| Pod | Avg (cores) | Max (cores) | Current (cores) |")
            sections.append("|-----|-------------|-------------|-----------------|")
            sorted_cpu = sorted(data["cpu"], key=lambda x: safe_float(x.split("|")[2].strip()), reverse=True)
            sections.extend(sorted_cpu)
            sections.append("")

        if data["memory"]:
            sections.append("#### Memory Utilization (Last 7 Days)")
            sections.append("| Pod | Avg (GB) | Max (GB) | Current (GB) |")
            sections.append("|-----|----------|----------|-------------|")
            sorted_mem = sorted(data["memory"], key=lambda x: safe_float(x.split("|")[2].strip()), reverse=True)
            sections.extend(sorted_mem)
            sections.append("")

        sections.append("---")

    result = "\n".join(sections).strip()
    ti.xcom_push(key="pod_this_week_markdown", value=result)
    return result

@task
def compile_pod_comparison(namespace_results: list, **context):
    ti = context['ti']
    
    merged = {}
    for batch in namespace_results:
        if not batch: continue
        key = (batch["namespace"], batch["period"])
        if key not in merged:
            merged[key] = {**batch, "cpu_lines": [], "mem_lines": []}
        for line in batch["cpu"].split('\n'):
            if line.strip().startswith('|') and '|' in line and not line.startswith('| pod |') and not line.startswith('|---'):
                merged[key]["cpu_lines"].append(line.strip())
        for line in batch["memory"].split('\n'):
            if line.strip().startswith('|') and '|' in line and not line.startswith('| pod |') and not line.startswith('|---'):
                merged[key]["mem_lines"].append(line.strip())

    raw = Variable.get("ltai.v1.sretradeideas.pod.namespaces", default_var="[]")
    try:
        namespaces = json.loads(raw)
    except:
        namespaces = [ns.strip() for ns in raw.split(",") if ns.strip()]

    sections = []
    for ns in namespaces:
        this = merged.get((ns, "last 7 days"))
        last = merged.get((ns, "previous week"))
        if not this or not last: continue

        prompt = f"""
You are the SRE TradeIdeas agent.
Generate a clean comparison between this week and previous week for namespace `{ns}`.

This Week CPU rows:
{"\n".join(this["cpu_lines"])}

This Week Memory rows:
{"\n".join(this["mem_lines"])}

Previous Week CPU rows:
{"\n".join(last["cpu_lines"])}

Previous Week Memory rows:
{"\n".join(last["mem_lines"])}

Return exactly this format:
### {ns} — Pod CPU & Memory Comparison (This Week vs Previous Week)
#### CPU Changes
| Pod | This Week Avg | Prev Week Avg | Diff | This Week Max | Prev Week Max | Max Diff |
...
#### Memory Changes
| Pod | This Week Avg (GB) | Prev Week Avg | Diff | ...
### Summary
Highlight pods with >0.8 core or >2GB change.
"""
        comparison = get_ai_response(prompt)
        sections.append(comparison)

    result = "\n\n---\n\n".join(sections)
    ti.xcom_push(key="pod_comparison_markdown", value=result)
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

    node_cpu_cmp        = ti.xcom_pull(key="node_cpu_this_vs_last_week") or "No comparison"
    node_mem_cmp        = ti.xcom_pull(key="node_memory_this_vs_last_week") or "No comparison"
    node_disk_cmp       = ti.xcom_pull(key="node_disk_this_vs_last_week") or "No comparison"
    pod_cmp             = ti.xcom_pull(key="pod_comparison_markdown") or "No comparison"
    mysql_cmp           = ti.xcom_pull(key="mysql_health_this_vs_last_week") or "No comparison"
    pvc_cmp             = ti.xcom_pull(key="lke_pvc_this_vs_last_week") or "No comparison"

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
{ti.xcom_pull(key="node_cpu_this_vs_last_week") or "No comparison"}
{ti.xcom_pull(key="node_memory_this_vs_last_week") or "No comparison"}
{ti.xcom_pull(key="node_disk_this_vs_last_week") or "No comparison"}
{ti.xcom_pull(key="pod_comparison_markdown") or "No comparison"}
{ti.xcom_pull(key="lke_pvc_this_vs_last_week") or "No comparison"}
{ti.xcom_pull(key="mysql_health_this_vs_last_week") or "No comparison"}

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
                filename="TradeIdeas_SRE_Report.pdf"
            )
        logging.info(f"Attaching PDF: {pdf_path}")
    else:
        logging.warning("PDF not found or not generated, skipping attachment")

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

        # Prepare email - use "mixed" to support both HTML + attachments (including inline images + file attachments)
        msg = MIMEMultipart("mixed")
        msg["Subject"] = subject
        msg["From"] = f"TradeIdeas SRE Agent {SMTP_SUFFIX}"
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
        server.sendmail(SENDER_EMAIL, recipient, msg.as_string())
        server.quit()

        logging.info(f"Email sent successfully to {recipient}")
        return f"Email sent successfully to {recipient}"

    except Exception as e:
        logging.error(f"Failed to send email via SMTP: {str(e)}")
        raise
        
def generate_pdf_report_callable(ti=None, **context):
    """
    TradeIdeas SRE PDF – ReportLab (ABSOLUTE FINAL VERSION)
    • No raw # or ## visible
    • Tables perfect with text wrap
    • No small squares
    • Professional layout
    """
    try:
        md = ti.xcom_pull(key="sre_full_report") or "# No SRE report generated."
        md = preprocess_markdown(md)

        date_str = YESTERDAY_DATE_STR
        out_path = f"/tmp/TradeIdeas_SRE_Report_{date_str}.pdf"

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
        flowables.append(Paragraph("Generated by TradeIdeas SRE Agent • Powered by Airflow + ReportLab", small))

        doc.build(flowables)
        logging.info(f"PDF generated – PERFECT (no # visible, tables wrapped): {out_path}")
        ti.xcom_push(key="sre_pdf_path", value=out_path)
        return out_path

    except Exception as e:
        logging.error("PDF generation failed", exc_info=True)
        raise


# === DAG ===
with DAG(
    dag_id="sre-tradeideas_weekly",
    default_args=default_args,
    schedule_interval="30 5 * * 1",  # Every Monday 05:30 UTC → 11:00 AM IST
    catchup=False,
    tags=["sre", "tradeideas", "weekly", "11am-ist"],
    max_active_runs=1,
) as dag:

    # === Static Tasks - This Week (Last 7 Days) ===
    t1  = PythonOperator(task_id="node_cpu_this_week", python_callable=node_cpu_this_week, provide_context=True)
    t2  = PythonOperator(task_id="node_memory_this_week", python_callable=node_memory_this_week, provide_context=True)
    t3  = PythonOperator(task_id="node_disk_this_week", python_callable=node_disk_this_week, provide_context=True)
    t4  = PythonOperator(task_id="node_readiness_check", python_callable=node_readiness_check, provide_context=True)
    t5  = PythonOperator(task_id="pod_restart_this_week", python_callable=pod_restart_this_week, provide_context=True)
    t6  = PythonOperator(task_id="mysql_health_this_week", python_callable=mysql_health_this_week, provide_context=True)
    t7  = PythonOperator(task_id="kubernetes_version_check", python_callable=kubernetes_version_check, provide_context=True)
    t7_1 = PythonOperator(task_id="kubernetes_eol_and_next_version", python_callable=kubernetes_eol_and_next_version, provide_context=True)
    t8  = PythonOperator(task_id="microk8s_expiry_check", python_callable=microk8s_expiry_check, provide_context=True)
    t9  = PythonOperator(task_id="lke_pvc_storage_details", python_callable=lke_pvc_storage_details, provide_context=True)

    # === Static Tasks - Previous Week (Absolute Range) ===
    t10 = PythonOperator(task_id="node_cpu_last_week", python_callable=node_cpu_last_week, provide_context=True)
    t11 = PythonOperator(task_id="node_memory_last_week", python_callable=node_memory_last_week, provide_context=True)
    t12 = PythonOperator(task_id="node_disk_last_week", python_callable=node_disk_last_week, provide_context=True)
    t13 = PythonOperator(task_id="lke_pvc_storage_details_last_week", python_callable=lke_pvc_storage_details_last_week, provide_context=True)
    t14 = PythonOperator(task_id="mysql_health_last_week", python_callable=mysql_health_last_week, provide_context=True)

    # === Comparison Tasks ===
    t15 = PythonOperator(task_id="node_cpu_this_vs_last_week", python_callable=node_cpu_this_vs_last_week, provide_context=True)
    t16 = PythonOperator(task_id="node_memory_this_vs_last_week", python_callable=node_memory_this_vs_last_week, provide_context=True)
    t17 = PythonOperator(task_id="node_disk_this_vs_last_week", python_callable=node_disk_this_vs_last_week, provide_context=True)
    t18 = PythonOperator(task_id="lke_pvc_this_vs_last_week", python_callable=lke_pvc_this_vs_last_week, provide_context=True)
    t19 = PythonOperator(task_id="mysql_health_this_vs_last_week", python_callable=mysql_health_this_vs_last_week, provide_context=True)

    t20 = PythonOperator(task_id="overall_summary", python_callable=overall_summary, provide_context=True)
    t21 = PythonOperator(task_id="compile_sre_report", python_callable=compile_sre_report, provide_context=True)
    t21_1 = PythonOperator(task_id="generate_pdf", python_callable=generate_pdf_report_callable, provide_context=True)
    t22 = PythonOperator(task_id="convert_to_html", python_callable=convert_to_html, provide_context=True)
    t23 = PythonOperator(task_id="send_sre_email", python_callable=send_sre_email, provide_context=True)

    # === POD DYNAMIC FLOW - BATCHED (6 pods max per AI call) ===
    try:
        namespaces_list = json.loads(
            Variable.get("ltai.v1.sretradeideas.pod.namespaces", default_var='["alpha-prod","tipreprod-prod"]')
        )
    except Exception:
        logging.warning(
            "Failed to parse Airflow Variable 'ltai.v1.sretradeideas.pod.namespaces' (value was: %s). "
            "Reason: %s. Falling back to hard-coded default namespaces.", pod_namespaces_var, str(e))
        namespaces_list = ["alpha-prod", "tipreprod-prod"]

    @task(task_id="fetch_real_problematic_pods")
    def fetch_real_problematic_pods(namespaces: List[str]) -> dict:
        result = {}
        for ns in namespaces:
            prompt = f"""
            Namespace: `{ns}`
            Execute this exact Prometheus query:
            sum by (pod, phase) (kube_pod_status_phase{{namespace="{ns}", phase=~"Failed|Pending|Unknown"}} == 1)
            Return ONLY one of these two exact responses:
            • If no problematic pods → respond exactly: No problematic pods
            • If any → respond with one line per pod in this format (no extra text):
            - my-pod-abc123 (Failed)
            - another-pod-xyz (Pending)

            Do not include headers, counts, timestamps, or markdown.
            """
            resp = get_ai_response(prompt).strip()
            result[ns] = resp if "no problematic" not in resp.lower() else "No problematic pods"
        return result

    real_problematic_pods_task = fetch_real_problematic_pods(namespaces_list)

    @task
    def get_pods_in_namespace(ns: str) -> List[str]:
        prompt = f"""
        Execute this query and return only pod names (one per line):
        sum by(pod) (kube_pod_info{{namespace='{ns}'}})
        """
        resp = get_ai_response(prompt)
        return [line.strip() for line in resp.splitlines() if line.strip()]

    @task
    def process_pod_batch(pods_batch: List[str], ns: str, period: str):
        if not pods_batch:
            return []

        pod_regex = "|".join(pods_batch)
        is_previous_week = "previous week" in period.lower()

        if is_previous_week:
            start = PREVIOUS_WEEK_START
            end = PREVIOUS_WEEK_END
            time_range = f"[{start}:{end}]"
            cpu_prompt = f"""
            Namespace: {ns} — Previous Week ({start} to {end})
            
            Run this exact query and process results:
            sum by (pod) (rate(container_cpu_usage_seconds_total{{namespace=~"{ns}", pod=~"{pod_regex}", image!="", container!="POD"}}[5m])){time_range}
            
            For each pod:
            - avg_cpu_cores  = MeanTool(values)   → round to 4 decimals
            - max_cpu_cores  = MaxTool(values)    → round to 4 decimals
        

            Return ONLY this markdown table, sorted by avg_cpu_cores DESC:
            | pod                  | avg_cpu_cores | max_cpu_cores |
            |----------------------|---------------|---------------|
            Use `N/A` if no data for a pod.
            """
            
            mem_prompt = f"""
            Same absolute range — memory:

            Query:
            sum by (pod) (container_memory_working_set_bytes{{namespace=~"{ns}", pod=~"{pod_regex}", image!=""}}){time_range}


            For each pod:
            - avg_memory_gb = MeanTool(values) / 1024 / 1024 / 1024 → round to 2 decimals
            - max_memory_gb = MaxTool(values) / 1024 / 1024 / 1024 → round to 2 decimals


            Return ONLY this markdown table, sorted by avg_memory_gb DESC:
            | pod                  | avg_memory_gb | max_memory_gb |
            |----------------------|---------------|---------------|
            Use `N/A` if no data.
            """
        else:
            cpu_prompt = f"""
    Namespace: {ns} — Last 24 hours (relative)

    Use these exact queries:

    Average CPU:
    avg_over_time(avg by(pod)(rate(container_cpu_usage_seconds_total{{image!="", container!="POD", namespace=~"{ns}", pod=~"{pod_regex}"}}[5m]))[7d:5m])

    Max CPU:
    max_over_time(avg by(pod)(rate(container_cpu_usage_seconds_total{{image!="", container!="POD", namespace=~"{ns}", pod=~"{pod_regex}"}}[5m]))[7d:5m])

    Current CPU:
    avg by(pod)(rate(container_cpu_usage_seconds_total{{image!="", container!="POD", namespace=~"{ns}", pod=~"{pod_regex}"}}[5m]))

    Return ONLY this table, sorted by avg_cpu_cores DESC:
    | pod                  | avg_cpu_cores | max_cpu_cores | current_cpu_cores |
    |----------------------|---------------|---------------|-------------------|
    Round to 4 decimals. Use N/A if missing.
    """

            mem_prompt = f"""
    Same namespace, last 24 hours — memory in GB.

    Average:
    avg_over_time(sum by(pod)(container_memory_working_set_bytes{{image!="", namespace=~"{ns}", pod=~"{pod_regex}"}})[7d:5m]) / 1024 / 1024 / 1024

    Max:
    max_over_time(sum by(pod)(container_memory_working_set_bytes{{image!="", namespace=~"{ns}", pod=~"{pod_regex}"}})[7d:5m]) / 1024 / 1024 / 1024

    Current:
    sum by(pod)(container_memory_working_set_bytes{{image!="", namespace=~"{ns}", pod=~"{pod_regex}"}}) / 1024 / 1024 / 1024

    Return ONLY this table, sorted by avg_memory_gb DESC:
    | pod                  | avg_memory_gb | max_memory_gb | current_memory_gb |
    |----------------------|---------------|---------------|-------------------|
    Round to 2 decimals. Use N/A if missing.
            """

        return [{
            "namespace": ns,
            "period": period,
            "cpu": get_ai_response(cpu_prompt).strip(),
            "memory": get_ai_response(mem_prompt).strip()
        }]

    @task
    def merge_batch_results(batches: List) -> List:
        return [item for batch in batches for item in batch]

    @task_group(group_id="pod_metrics_batched")
    def pod_metrics_batched():
        namespace_results = []
      
        for ns in namespaces_list:
            pods = get_pods_in_namespace.override(task_id=f"list_pods_{ns}")(ns=ns)

            @task(task_id=f"split_{ns}")
            def split_pods(pod_list: List[str]) -> List[List[str]]:
                return [pod_list[i:i + POD_BATCH_SIZE] for i in range(0, len(pod_list), POD_BATCH_SIZE)]

            batches = split_pods(pods)

            # This Week
            this_week_batches = process_pod_batch.partial(ns=ns, period="last 7 days").expand(pods_batch=batches)
            this_week_merged = merge_batch_results.override(task_id=f"merged_this_week_{ns}")(this_week_batches)

            # Previous Week
            last_week_batches = process_pod_batch.partial(ns=ns, period="previous week").expand(pods_batch=batches)
            last_week_merged = merge_batch_results.override(task_id=f"merged_last_week_{ns}")(last_week_batches)

            namespace_results.extend([this_week_merged, last_week_merged])

        all_data = merge_batch_results(namespace_results)
        return all_data

    all_pod_data = pod_metrics_batched()
    real_pod_counts = get_real_pod_counts(namespaces_list)

    all_pod_data >> real_problematic_pods_task

    pod_this_week_markdown = compile_pod_sections_this_week(
        namespace_results=all_pod_data,
        real_counts=real_pod_counts
    )

    pod_comparison_markdown = compile_pod_comparison(all_pod_data)

    # === DEPENDENCIES (exact mirror of daily) ===
    [t1, t2, t3, t4, t5, t6, t7, t7_1, t8, t9, t10, t11, t12, t13, t14] >> t15 >> t16 >> t17 >> t18 >> t19

    t4 >> all_pod_data
    all_pod_data >> pod_this_week_markdown >> t20
    all_pod_data >> pod_comparison_markdown >> t20
    t19 >> t20 >> t21 >> t21_1 >> t22 >> t23
