# sre-mediamelon-weekly-updated.py (WEEKLY VERSION)
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from datetime import datetime, timedelta
import logging
from ollama import Client
from airflow.models import Variable
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
import base64
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import json
import re
import html
import requests
import markdown
import smtplib
import traceback

# Configure detailed logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

default_args = {
    "owner": "sre_team",
    "depends_on_past": False,
    "start_date": datetime(2025, 11, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# SMTP + Mail configuration from Airflow Variables (defensive defaults)
SMTP_HOST = Variable.get("ltai.v3.mediamelon.smtp.host","mail.authsmtp.com")
SMTP_PORT = int(Variable.get("ltai.v3.mediamelon.smtp.port"))
SMTP_USER = Variable.get("ltai.v3.mediamelon.smtp.user")
SMTP_PASSWORD = Variable.get("ltai.v3.mediamelon.smtp.password")
SMTP_SUFFIX = Variable.get("ltai.v3.mediamelon.smtp.suffix","<noreply@mediamelon.com>")

# From/To - MediaMelon
MEDIAMELON_FROM_ADDRESS = Variable.get("ltai.v3.mediamelon.mediamelon_from_address","SMTP_USER"or "noreply@mediamelon.com")
MEDIAMELON_TO_ADDRESS = Variable.get("ltai.v3.mediamelon.mediamelon_to_address", "MEDIAMELON_FROM_ADDRESS")

# Ollama Host for MediaMelon agent
OLLAMA_HOST = Variable.get("MEDIAMELON_OLLAMA_HOST", default_var="http://agentomatic:8000/")

# Gmail credentials (optional) - store JSON string in Variable MEDIAMELON_GMAIL_CREDENTIALS
GMAIL_CREDENTIALS_VAR = "ltai.v3.mediamelon.mediamelon_gmail_credentials"

# WEEK WINDOWS
THIS_WEEK = f"{(datetime.utcnow() - timedelta(days=7)).strftime('%Y-%m-%d')} to {datetime.utcnow().strftime('%Y-%m-%d')}"
PREVIOUS_WEEK = f"{(datetime.utcnow() - timedelta(days=14)).strftime('%Y-%m-%d')} to {(datetime.utcnow() - timedelta(days=7)).strftime('%Y-%m-%d')}"

# ======================================================================
#  Gmail Authentication Helper
# ======================================================================
def authenticate_gmail():
    try:
        creds_json = Variable.get("ltai.v3.mediamelon.mediamelon_gmail_credentials", default_var=None)
        if not creds_json:
            logging.info("Gmail credentials not found in Airflow variables.")
            return None

        creds_info = json.loads(creds_json)
        creds = Credentials.from_authorized_user_info(creds_info)
        service = build("gmail", "v1", credentials=creds)
        profile = service.users().getProfile(userId="me").execute()
        logged_in_email = profile.get("emailAddress", "")
        sender_email = Variable.get("ltai.v3.mediamelon.mediamelon_from_address", default_var=logged_in_email)
        if logged_in_email and sender_email and logged_in_email.lower() != sender_email.lower():
            logging.warning(
                "Gmail credentials email %s doesn't match MEDIAMELON_FROM_ADDRESS %s; proceeding but check config.",
                logged_in_email,
                sender_email,
            )
        logging.info("Authenticated Gmail account: %s", logged_in_email)
        return service
    except Exception:
        logging.error("Failed to authenticate Gmail: %s", traceback.format_exc())
        return None

# ======================================================================
#  AI Response Wrapper
# ======================================================================
def get_ai_response(prompt, conversation_history=None):
    try:
        logging.debug("Query received: %s", (prompt[:200] + "...") if len(prompt) > 200 else prompt)

        if not prompt or not isinstance(prompt, str):
            return "Invalid input provided. Please enter a valid query."

        client = Client(host=OLLAMA_HOST, headers={"x-ltai-client": "media_melon-agent"})
        logging.debug("Connecting to Ollama at %s with model 'appz/sre/media_melon:0.4'", OLLAMA_HOST)

        messages = []
        if conversation_history:
            for history_item in conversation_history:
                if isinstance(history_item, dict) and "prompt" in history_item and "response" in history_item:
                    messages.append({"role": "user", "content": history_item["prompt"]})
                    messages.append({"role": "assistant", "content": history_item["response"]})
                else:
                    messages.append({"role": "user", "content": str(history_item)})

        messages.append({"role": "user", "content": prompt})
        response = client.chat(model="appz/sre/media_melon:0.4", messages=messages, stream=False)

        logging.info("Raw response: %s", str(response)[:500] + "..." if response else "None")

        ai_content = None
        if isinstance(response, dict):
            ai_content = response.get("message", {}).get("content")
        else:
            msg = getattr(response, "message", None)
            if msg:
                ai_content = getattr(msg, "content", None)

        if not ai_content:
            return "Invalid response format from AI."

        return ai_content.strip()

    except Exception:
        logging.error("Error in get_ai_response: %s", traceback.format_exc())
        return f"An error occurred: {traceback.format_exc()}"

# ======================================================================
# Helper utilities for JSON → Markdown (same as daily)
# ======================================================================
def safe_parse_json(json_text):
    if not json_text or not isinstance(json_text, str):
        return None
    try:
        return json.loads(json_text)
    except Exception:
        try:
            match = re.search(r"(\[.*\]|\{.*\})", json_text, re.DOTALL)
            if match:
                return json.loads(match.group(0))
        except Exception:
            pass
    return None


def json_list_to_markdown_table(json_list, column_defs):
    if not isinstance(json_list, list) or not json_list:
        return None
    headers = [h for (_, h, _) in column_defs]
    header_row = "| " + " | ".join(headers) + " |"
    sep_row = "| " + " | ".join(["---"] * len(headers)) + " |"
    rows = [header_row, sep_row]
    for item in json_list:
        row_cells = []
        for key, _, fmt in column_defs:
            val = item.get(key, "")
            if fmt:
                try:
                    val = fmt(val)
                except Exception:
                    pass
            row_cells.append(str(val))
        rows.append("| " + " | ".join(row_cells) + " |")
    return "\n".join(rows)


def try_convert_peak_cpu_to_markdown(text):
    parsed = safe_parse_json(text)
    if isinstance(parsed, list):
        cols = [("pod_name", "Pod Name", None), ("peak_cpu_cores", "Peak CPU (cores)", None), ("timestamp", "Timestamp", None)]
        md = json_list_to_markdown_table(parsed, cols)
        if md:
            return md
    return text


def try_convert_peak_memory_to_markdown(text):
    parsed = safe_parse_json(text)
    if isinstance(parsed, list):
        cols = [("pod_name", "Pod Name", None), ("peak_memory_gb", "Peak Memory (GB)", None), ("timestamp", "Timestamp", None)]
        md = json_list_to_markdown_table(parsed, cols)
        if md:
            return md
    return text

# ======================================================================
# WEEKLY NODE + POD TASKS (same structure, weekly prompts)
# ======================================================================

# === t1: Node CPU – Weekly ===
def node_cpu_this_week(ti, **context):
    prompt = "Generate the **node level CPU utilisation for this week (last 7 days)**."
    response = get_ai_response(prompt)
    ti.xcom_push(key="node_cpu_this_week", value=response)
    return response


def node_cpu_last_week(ti, **context):
    prompt = "Generate the **node level CPU utilisation for last week (8–14 days ago)**."
    response = get_ai_response(prompt)
    ti.xcom_push(key="node_cpu_last_week", value=response)
    return response

# === t2: Node Memory – Weekly ===
def node_memory_this_week(ti, **context):
    prompt = "Generate the **node level memory utilisation for this week (last 7 days)**."
    response = get_ai_response(prompt)
    ti.xcom_push(key="node_memory_this_week", value=response)
    return response


def node_memory_last_week(ti, **context):
    prompt = "Generate the **node level memory utilisation for last week (8–14 days ago)**."
    response = get_ai_response(prompt)
    ti.xcom_push(key="node_memory_last_week", value=response)
    return response

# === t3: Node Disk – Weekly ===
def node_disk_this_week(ti, **context):
    prompt = "Generate the **node level disk utilisation for this week (last 7 days)**."
    response = get_ai_response(prompt)
    ti.xcom_push(key="node_disk_this_week", value=response)
    return response


def node_disk_last_week(ti, **context):
    prompt = "Generate the **node level disk utilisation for last week (8–14 days ago)**."
    response = get_ai_response(prompt)
    ti.xcom_push(key="node_disk_last_week", value=response)
    return response

# === t4: Pod CPU – Weekly ===
def pod_cpu_this_week(ti, **context):
    prompt = "Generate the **pod level CPU utilisation for this week (last 7 days)**."
    response = get_ai_response(prompt)
    ti.xcom_push(key="pod_cpu_this_week", value=response)
    return response


def pod_cpu_last_week(ti, **context):
    prompt = "Generate the **pod level CPU utilisation for last week (8–14 days ago)**."
    response = get_ai_response(prompt)
    ti.xcom_push(key="pod_cpu_last_week", value=response)
    return response

# === t5: Pod Memory – Weekly ===
def pod_memory_this_week(ti, **context):
    prompt = "Generate the **pod level memory utilisation for this week (last 7 days)**."
    response = get_ai_response(prompt)
    ti.xcom_push(key="pod_memory_this_week", value=response)
    return response


def pod_memory_last_week(ti, **context):
    prompt = "Generate the **pod level memory utilisation for last week (8–14 days ago)**."
    response = get_ai_response(prompt)
    ti.xcom_push(key="pod_memory_last_week", value=response)
    return response

# ======================================================================
# Weekly Comparison Functions
# ======================================================================
def node_cpu_week_compare(ti, **context):
    this_w = ti.xcom_pull(key="node_cpu_this_week")
    last_w = ti.xcom_pull(key="node_cpu_last_week")
    prompt = (
        "Compare **node CPU utilisation** for this week vs last week.\n\n"
        "This Week:\n" + (this_w or "No data") + "\n\n"
        "Last Week:\n" + (last_w or "No data") + "\n\n"
        "Produce a table comparing avg and max per node, and highlight significant changes."
    )
    resp = get_ai_response(prompt)
    ti.xcom_push(key="node_cpu_week_compare", value=resp)
    return resp


def node_memory_week_compare(ti, **context):
    this_w = ti.xcom_pull(key="node_memory_this_week")
    last_w = ti.xcom_pull(key="node_memory_last_week")
    prompt = (
        "Compare **node memory utilisation** for this week vs last week.\n\n"
        "This Week:\n" + (this_w or "No data") + "\n\n"
        "Last Week:\n" + (last_w or "No data")
    )
    resp = get_ai_response(prompt)
    ti.xcom_push(key="node_memory_week_compare", value=resp)
    return resp


def node_disk_week_compare(ti, **context):
    this_w = ti.xcom_pull(key="node_disk_this_week")
    last_w = ti.xcom_pull(key="node_disk_last_week")
    prompt = (
        "Compare **node disk utilisation** for this week vs last week.\n\n"
        "This Week:\n" + (this_w or "No data") + "\n\n"
        "Last Week:\n" + (last_w or "No data")
    )
    resp = get_ai_response(prompt)
    ti.xcom_push(key="node_disk_week_compare", value=resp)
    return resp


def pod_cpu_week_compare(ti, **context):
    this_w = ti.xcom_pull(key="pod_cpu_this_week")
    last_w = ti.xcom_pull(key="pod_cpu_last_week")
    prompt = (
        "Compare **pod CPU utilisation** for this week vs last week.\n\n"
        "This Week:\n" + (this_w or "No data") + "\n\n"
        "Last Week:\n" + (last_w or "No data")
    )
    resp = get_ai_response(prompt)
    ti.xcom_push(key="pod_cpu_week_compare", value=resp)
    return resp


def pod_memory_week_compare(ti, **context):
    this_w = ti.xcom_pull(key="pod_memory_this_week")
    last_w = ti.xcom_pull(key="pod_memory_last_week")
    prompt = (
        "Compare **pod memory utilisation** for this week vs last week.\n\n"
        "This Week:\n" + (this_w or "No data") + "\n\n"
        "Last Week:\n" + (last_w or "No data")
    )
    resp = get_ai_response(prompt)
    ti.xcom_push(key="pod_memory_week_compare", value=resp)
    return resp

# -------------------------------
# Namespace-level dynamic tasks (weekly)
# -------------------------------
@task
def list_namespaces_weekly():
    """Get list of namespaces from Prometheus via AI prompt (weekly)"""
    namespace_prompt = (
        "Run this Prometheus query:\n"
        "count by (namespace) (kube_pod_info)\n\n"
        "Return as a clean JSON array of namespace strings, e.g.:\n"
        '["namespace1", "namespace2"]\n\n'
        "Only output the JSON array."
    )
    response = get_ai_response(namespace_prompt)
    logging.info("Agent returned namespace list: %s", (response[:500] + "...") if response else "None")
    try:
        match = re.search(r"\[.*?\]", response or "", re.DOTALL)
        namespaces = json.loads(match.group(0)) if match else []
        logging.info("Parsed namespaces: %s", namespaces)
        return namespaces
    except Exception:
        logging.error("Failed to parse namespace list: %s", traceback.format_exc())
        return []


@task
def process_namespace_weekly(ns: str):
    """Process namespace for weekly peaks, limits, disk usage"""
    logging.info("Processing weekly namespace: %s", ns)

    cpu_peak_prompt = (
        f"Get the peak CPU usage for every pod in namespace '{ns}' over the last 7 days.\n\n"
        "Return a JSON array of objects with keys: pod_name, peak_cpu_cores, timestamp.\n"
        "Example output:\n"
        '[{"pod_name":"pod-a","peak_cpu_cores":0.125,"timestamp":"2025-11-10T12:34:56Z"}, '
        '{"pod_name":"pod-b","peak_cpu_cores":0.456,"timestamp":"2025-11-12T13:22:11Z"}]'
    )

    mem_peak_prompt = (
        f"Get the peak memory usage for every pod in namespace '{ns}' over the last 7 days.\n\n"
        "Return a JSON array of objects with keys: pod_name, peak_memory_gb, timestamp.\n"
        "Example output:\n"
        '[{"pod_name":"pod-a","peak_memory_gb":0.12,"timestamp":"2025-11-10T12:34:56Z"}, '
        '{"pod_name":"pod-b","peak_memory_gb":0.45,"timestamp":"2025-11-12T13:22:11Z"}]'
    )

    try:
        cpu_response = get_ai_response(cpu_peak_prompt)
        memory_response = get_ai_response(mem_peak_prompt)
        return {"namespace": ns, "cpu_peak": cpu_response, "memory_peak": memory_response, "status": "success"}
    except Exception:
        logging.error("Error processing namespace %s: %s", ns, traceback.format_exc())
        return {"namespace": ns, "error": "processing failed", "status": "failed"}


@task
def collect_namespace_results_weekly(namespace_results: list):
    """Aggregate results from namespace processing tasks (weekly)"""
    logging.info("Collecting weekly namespace results count=%d", len(namespace_results or []))
    aggregated = {}
    failed = []
    for res in (namespace_results or []):
        if not res:
            continue
        ns = res.get("namespace")
        if res.get("status") == "failed":
            failed.append(ns)
            logging.warning("Namespace %s failed: %s", ns, res.get("error"))
        else:
            aggregated[ns] = res
    return {
        "total_namespaces": len(namespace_results or []),
        "successful": len(aggregated),
        "failed": len(failed),
        "results": aggregated,
        "failed_namespaces": failed,
    }


@task
def compile_namespace_report_weekly(namespace_data):
    """Convert namespace data into markdown report and compute overall week peak summary"""
    logging.info("Compiling namespace weekly report")
    if not namespace_data or not namespace_data.get("results"):
        return "## Pod-Level Weekly Metrics by Namespace\n\nNo namespace data available."

    sections = []
    sections.append("## Pod-Level Weekly Metrics by Namespace\n")
    sections.append(
        f"**Total Namespaces Analyzed**: {namespace_data.get('successful', 0)}/{namespace_data.get('total_namespaces', 0)}\n"
    )
    if namespace_data.get("failed_namespaces"):
        sections.append("**Failed Namespaces**: " + ", ".join(namespace_data["failed_namespaces"]) + "\n")
    sections.append("---\n")

    overall_cpu_peaks = []
    overall_mem_peaks = []

    for ns in sorted(namespace_data.get("results", {})):
        data = namespace_data["results"][ns]
        sections.append(f"\n### Namespace: `{ns}`\n")

        cpu_raw = data.get("cpu_peak")
        if cpu_raw:
            sections.append("\n#### Peak CPU Usage (Per Pod)\n")
            try:
                cpu_md = try_convert_peak_cpu_to_markdown(cpu_raw)
                sections.append(cpu_md + "\n")
                parsed_cpu = safe_parse_json(cpu_raw)
                if isinstance(parsed_cpu, list):
                    for item in parsed_cpu:
                        try:
                            pod = item.get("pod_name")
                            peak = item.get("peak_cpu_cores")
                            ts = item.get("timestamp")
                            if pod and peak is not None:
                                overall_cpu_peaks.append((ns, pod, float(peak), ts))
                        except Exception:
                            logging.debug("Error parsing cpu item: %s", traceback.format_exc())
            except Exception:
                logging.warning("Failed to convert cpu_peak for namespace %s: %s", ns, traceback.format_exc())
                sections.append(cpu_raw + "\n")
        else:
            sections.append("\n#### Peak CPU Usage (Per Pod)\n")
            sections.append("No CPU peak data available.\n")

        mem_raw = data.get("memory_peak")
        if mem_raw:
            sections.append("\n#### Peak Memory Usage (Per Pod)\n")
            try:
                mem_md = try_convert_peak_memory_to_markdown(mem_raw)
                sections.append(mem_md + "\n")
                parsed_mem = safe_parse_json(mem_raw)
                if isinstance(parsed_mem, list):
                    for item in parsed_mem:
                        try:
                            pod = item.get("pod_name")
                            peak = item.get("peak_memory_gb")
                            ts = item.get("timestamp")
                            if pod and peak is not None:
                                overall_mem_peaks.append((ns, pod, float(peak), ts))
                        except Exception:
                            logging.debug("Error parsing mem item: %s", traceback.format_exc())
            except Exception:
                logging.warning("Failed to convert memory_peak for namespace %s: %s", ns, traceback.format_exc())
                sections.append(mem_raw + "\n")
        else:
            sections.append("\n#### Peak Memory Usage (Per Pod)\n")
            sections.append("No memory peak data available.\n")

        sections.append("---\n")

    top_cpu = None
    top_mem = None
    try:
        if overall_cpu_peaks:
            overall_cpu_peaks_sorted = sorted(overall_cpu_peaks, key=lambda x: x[2], reverse=True)
            top_cpu = overall_cpu_peaks_sorted[0]
        if overall_mem_peaks:
            overall_mem_peaks_sorted = sorted(overall_mem_peaks, key=lambda x: x[2], reverse=True)
            top_mem = overall_mem_peaks_sorted[0]
    except Exception:
        logging.debug("Error computing top peaks: %s", traceback.format_exc())

    sections.append("\n---\n")
    sections.append("## Namespace Weekly Peak Metrics Summary\n")
    ai_lines = []
    ai_lines.append(f"Total namespaces analyzed: {namespace_data.get('successful', 0)}")
    if top_cpu:
        ai_lines.append(f"Highest CPU peak: pod `{top_cpu[1]}` in namespace `{top_cpu[0]}` with {top_cpu[2]} cores at {top_cpu[3]}")
    else:
        ai_lines.append("Highest CPU peak: None")
    if top_mem:
        ai_lines.append(f"Highest Memory peak: pod `{top_mem[1]}` in namespace `{top_mem[0]}` with {top_mem[2]} GB at {top_mem[3]}")
    else:
        ai_lines.append("Highest Memory peak: None")
    try:
        cpu_over = len([1 for (_, _, p, _) in overall_cpu_peaks if p > 0.5])
        mem_over = len([1 for (_, _, m, _) in overall_mem_peaks if m > 1.0])
        ai_lines.append(f"Namespaces with pod CPU peak > 0.5 cores: {cpu_over}")
        ai_lines.append(f"Namespaces with pod Memory peak > 1.0 GB: {mem_over}")
    except Exception:
        pass

    sections.append("\n".join(ai_lines) + "\n")
    sections.append("---\n")

    report = "\n".join(sections)
    logging.info("Compiled namespace weekly markdown")
    return report

# -------------------------------
# Compile and overall summary functions (weekly)
# -------------------------------
def compile_sre_report_weekly(ti, **context):
    """
    Combine all XComs into a single weekly markdown report.
    """
    try:
        logging.info("Compiling final SRE weekly markdown report for Mediamelon")

        node_cpu = ti.xcom_pull(key="node_cpu_this_week") or "No CPU data"
        node_memory = ti.xcom_pull(key="node_memory_this_week") or "No memory data"
        node_disk = ti.xcom_pull(key="node_disk_this_week") or "No disk data"
        pod_cpu = ti.xcom_pull(key="pod_cpu_this_week") or "No pod CPU data"
        pod_memory = ti.xcom_pull(key="pod_memory_this_week") or "No pod memory data"

        node_cpu_cmp = ti.xcom_pull(key="node_cpu_week_compare") or "No comparison"
        node_mem_cmp = ti.xcom_pull(key="node_memory_week_compare") or "No comparison"
        node_disk_cmp = ti.xcom_pull(key="node_disk_week_compare") or "No comparison"
        pod_cpu_cmp = ti.xcom_pull(key="pod_cpu_week_compare") or "No comparison"
        pod_mem_cmp = ti.xcom_pull(key="pod_memory_week_compare") or "No comparison"

        namespace_md = ti.xcom_pull(task_ids="compile_namespace_report_weekly") or "No namespace report generated."

        report = (
            "# Mediamelon SRE Weekly Report\n"
            f"**Reporting Window**: **{THIS_WEEK}**\n\n"
            f"**Generated**: **{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}**\n\n"
            "---\n\n"
            "## 1. Node-Level Metrics (This Week)\n"
            f"{node_cpu}\n\n"
            "---\n\n"
            f"{node_memory}\n\n"
            "---\n\n"
            f"{node_disk}\n\n"
            "---\n\n"
            "## 2. Pod-Level Metrics (This Week)\n"
            f"{pod_cpu}\n\n"
            "---\n\n"
            f"{pod_memory}\n\n"
            "---\n\n"
            "## 6. Namespace-Level Pod Peaks (This Week)\n"
            f"{namespace_md}\n\n"
            "---\n\n"
            "## 7. Node-Level Metrics (This Week vs Previous Week)\n"
            f"{node_cpu_cmp}\n\n"
            "---\n\n"
            f"{node_mem_cmp}\n\n"
            "---\n\n"
            f"{node_disk_cmp}\n\n"
            "---\n\n"
            "## 8. Pod-Level Metrics (This Week vs Previous Week)\n"
            f"{pod_cpu_cmp}\n\n"
            "---\n\n"
            f"{pod_mem_cmp}\n\n"
            "---\n\n"
            "## 10. Overall Summary\n"
            "(See consolidated summary)\n\n"
            "---\n"
        )

        report = re.sub(r"\n{3,}", "\n\n", report.strip())
        ti.xcom_push(key="sre_weekly_full_report", value=report)
        logging.info("Weekly SRE report compiled and pushed to XCom.")
        return report
    except Exception:
        logging.error("Failed to compile weekly SRE report: %s", traceback.format_exc())
        raise


def overall_weekly_summary(ti, **context):
    """
    Generate overall summary for weekly report using AI.
    """
    try:
        node_cpu = ti.xcom_pull(key="node_cpu_this_week") or "No CPU data"
        node_memory = ti.xcom_pull(key="node_memory_this_week") or "No memory data"
        node_disk = ti.xcom_pull(key="node_disk_this_week") or "No disk data"
        pod_cpu = ti.xcom_pull(key="pod_cpu_this_week") or "No pod CPU data"
        pod_memory = ti.xcom_pull(key="pod_memory_this_week") or "No pod memory data"

        node_cpu_cmp = ti.xcom_pull(key="node_cpu_week_compare") or "No comparison data"
        node_mem_cmp = ti.xcom_pull(key="node_memory_week_compare") or "No comparison data"
        node_disk_cmp = ti.xcom_pull(key="node_disk_week_compare") or "No comparison data"
        pod_cpu_cmp = ti.xcom_pull(key="pod_cpu_week_compare") or "No comparison data"
        pod_mem_cmp = ti.xcom_pull(key="pod_memory_week_compare") or "No comparison data"

        namespace_md = ti.xcom_pull(task_ids="compile_namespace_report_weekly") or "No namespace report generated."

        prompt = (
            "You are the SRE Mediamelon agent.\n\n"
            "Your task: Generate a complete overall summary for this week's SRE report, "
            "followed by a comparative summary analyzing performance trends versus the previous week.\n\n"
            "### Part 1: This Week's Summary\n"
            "Use the data below to provide a high-level assessment of current cluster health.\n\n"
            "- Node CPU Report: " + node_cpu + "\n"
            "- Node Memory Report: " + node_memory + "\n"
            "- Node Disk Report: " + node_disk + "\n"
            "- Pod CPU Report: " + pod_cpu + "\n"
            "- Pod Memory Report: " + pod_memory + "\n\n"
            "### Namespace Peak Metrics (Pod-level peaks - Tables included below)\n\n"
            + namespace_md + "\n\n"
            "### Part 2: Comparison Summary (This Week vs Previous Week)\n"
            "- Node CPU Comparison: " + node_cpu_cmp + "\n"
            "- Node Memory Comparison: " + node_mem_cmp + "\n"
            "- Node Disk Comparison: " + node_disk_cmp + "\n"
            "- Pod CPU Comparison: " + pod_cpu_cmp + "\n"
            "- Pod Memory Comparison: " + pod_mem_cmp + "\n\n"
            "### Instructions:\n"
            "1. Write two clearly separated sections:\n"
            "   - Overall Summary (This Week)\n"
            "   - Comparison Summary (This Week vs Previous Week)\n"
            "2. Use professional tone, concise sentences, and structured paragraphs.\n"
            "3. Highlight critical alerts, anomalies, and areas of improvement.\n"
            "4. Summarize key positives and potential issues.\n"
            "5. Include top 3 namespaces/pods by peak CPU and top 3 by peak Memory (use the tables above).\n"
            "6. Keep each section to roughly 6-12 sentences.\n"
            "7. Avoid repeating raw metrics from tables; focus on interpretation and action items.\n"
        )
        response = get_ai_response(prompt)
        ti.xcom_push(key="overall_weekly_summary", value=response)
        logging.info("Overall weekly summary generated and pushed to XCom.")
        return response
    except Exception:
        logging.error("Failed to generate overall weekly summary: %s", traceback.format_exc())
        raise

# -------------------------------
# Markdown -> HTML conversion (TradeIdeas template) - reuse functions from daily with weekly keys
# -------------------------------
def preprocess_markdown_weekly(markdown_text):
    """Clean and standardize Markdown before conversion to HTML."""
    markdown_text = markdown_text.lstrip("\ufeff\u200b\u200c\u200d")
    markdown_text = re.sub(r"^(#{1,6})\s*", r"\1 ", markdown_text, flags=re.MULTILINE)

    lines = markdown_text.split("\n")
    processed = []
    in_table = False

    for line in lines:
        stripped = line.strip()
        if "|" in stripped and stripped.count("|") >= 2:
            if not in_table and processed and processed[-1].strip():
                processed.append("")
            in_table = True
            processed.append(line)
        else:
            if in_table and stripped:
                processed.append("")
                in_table = False
            processed.append(line)
    return "\n".join(processed)


def convert_to_html_weekly(ti, **context):
    """
    Convert the combined weekly markdown SRE report into clean, responsive, Gmail-safe HTML using TradeIdeas template.
    """
    try:
        markdown_report = ti.xcom_pull(key="sre_weekly_full_report") or "# No report generated."
        logging.info("Converting weekly markdown report, length=%d", len(markdown_report))
        markdown_report = preprocess_markdown_weekly(markdown_report)

        html_body = None
        try:
            html_body = markdown.markdown(
                markdown_report,
                extensions=["tables", "fenced_code", "nl2br", "sane_lists", "attr_list"],
            )
            logging.info("Used 'markdown' library for conversion (weekly)")
        except Exception:
            logging.warning("markdown lib error, trying fallback converters (weekly)")

        if not html_body or len(html_body) < 50:
            try:
                import markdown2
                html_body = markdown2.markdown(markdown_report, extras=["tables", "fenced-code-blocks", "break-on-newline"])
                logging.info("Used markdown2 fallback for conversion (weekly)")
            except Exception:
                logging.debug("markdown2 not available or failed (weekly)")

        if not html_body or len(html_body) < 50:
            try:
                import mistune
                html_body = mistune.html(markdown_report)
                logging.info("Used mistune fallback for conversion (weekly)")
            except Exception:
                logging.error("All markdown conversions failed; using preformatted fallback (weekly)")
                html_body = "<pre>{}</pre>".format(html.escape(markdown_report))

        full_html = (
            "<!DOCTYPE html>\n"
            "<html>\n"
            "<head>\n"
            '<meta charset="UTF-8">\n'
            '<meta name="viewport" content="width=device-width, initial-scale=1.0">\n'
            "<title>Mediamelon SRE Weekly Report</title>\n"
            "<style>\n"
            "body { font-family: Arial, sans-serif; background-color: #f4f4f4; margin: 0; padding: 20px; }\n"
            ".container { max-width: 1000px; background: #ffffff; margin: auto; padding: 30px; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }\n"
            "table { border-collapse: collapse; width: 100%; margin: 20px 0; font-size: 14px; }\n"
            "table, th, td { border: 1px solid #ddd; }\n"
            "th, td { padding: 10px; text-align: left; }\n"
            "th { background-color: #1a5fb4; color: #ffffff; }\n"
            "tr:nth-child(even) { background-color: #f8f9fa; }\n"
            "pre { background-color: #f6f8fa; padding: 12px; border-radius: 6px; overflow-x: auto; border: 1px solid #e1e4e8; }\n"
            "h1 { color: #1a5fb4; font-size: 24px; border-bottom: 3px solid #1a5fb4; padding-bottom: 6px; }\n"
            "h2 { color: #1a5fb4; border-bottom: 2px solid #ccc; padding-bottom: 4px; }\n"
            "h3 { color: #2d3748; }\n"
            "strong { color: #111; }\n"
            "ul, ol { padding-left: 25px; }\n"
            "a { color: #1a5fb4; text-decoration: none; }\n"
            "@media screen and (max-width: 600px) { .container { padding: 15px; } table { font-size: 12px; } }\n"
            "</style>\n"
            "</head>\n            "
            "<body>\n"
            "<div class=\"container\">\n"
            + html_body
            + "\n</div>\n</body>\n</html>"
        )

        ti.xcom_push(key="sre_weekly_html_report", value=full_html)
        logging.info("Weekly HTML generated, length=%d and pushed to XCom.", len(full_html))
        return full_html
    except Exception:
        logging.error("Failed to convert weekly markdown to HTML: %s", traceback.format_exc())
        raise

# -------------------------------
# Email building + sending helpers (weekly) - reuse patterns from daily
# -------------------------------
def build_gmail_message_weekly(sender: str, to: list, subject: str, html_body: str, inline_images: dict = None):
    """
    Build a base64url encoded Gmail message (RFC 2822) ready to send via Gmail API.
    inline_images: {cid: bytes}
    """
    try:
        msg = MIMEMultipart("related")
        msg["Subject"] = subject
        msg["From"] = sender
        msg["To"] = ", ".join(to)
        alternative = MIMEMultipart("alternative")
        alternative.attach(MIMEText(html_body, "html"))
        msg.attach(alternative)

        if inline_images:
            for cid, img_bytes in inline_images.items():
                img_part = MIMEImage(img_bytes)
                img_part.add_header("Content-ID", "<{}>".format(cid))
                img_part.add_header("Content-Disposition", "inline", filename=f"{cid}.png")
                msg.attach(img_part)

        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
        return {"raw": raw}
    except Exception:
        logging.error("Failed to build Gmail message (weekly): %s", traceback.format_exc())
        raise


def send_via_gmail_api_weekly(service, sender: str, to: list, subject: str, html_body: str, inline_images: dict = None):
    """
    Send email via Gmail API service object (weekly).
    """
    try:
        message = build_gmail_message_weekly(sender, to, subject, html_body, inline_images)
        send_result = service.users().messages().send(userId="me", body=message).execute()
        logging.info("Sent weekly email via Gmail API: %s", send_result.get("id"))
        return send_result
    except HttpError:
        logging.error("Gmail API HttpError (weekly): %s", traceback.format_exc())
        raise
    except Exception:
        logging.error("Failed to send weekly via Gmail API: %s", traceback.format_exc())
        raise


def send_via_smtp_weekly(sender: str, to: list, subject: str, html_body: str, inline_images_b64: list = None):
    """
    Send email via SMTP server, using STARTTLS and login credentials (weekly).
    inline_images_b64: list of tuples (cid, base64_string)
    """
    try:
        msg = MIMEMultipart("related")
        msg["Subject"] = subject
        msg["From"] = f"Mediamelon SRE Reports {SMTP_SUFFIX}"
        msg["To"] = ", ".join(to)
        alternative = MIMEMultipart("alternative")
        alternative.attach(MIMEText(html_body, "html"))
        msg.attach(alternative)

        if inline_images_b64:
            for cid, b64 in inline_images_b64:
                try:
                    img_data = base64.b64decode(b64)
                    img_part = MIMEImage(img_data)
                    img_part.add_header("Content-ID", "<{}>".format(cid))
                    img_part.add_header("Content-Disposition", "inline", filename=f"{cid}.png")
                    msg.attach(img_part)
                except Exception:
                    logging.warning("Failed to attach inline image %s (weekly): %s", cid, traceback.format_exc())

        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20)
        server.starttls()
        if SMTP_USER and SMTP_PASSWORD:
            server.login(SMTP_USER, SMTP_PASSWORD)
        server.sendmail(sender, to, msg.as_string())
        server.quit()
        logging.info("Sent weekly email via SMTP to: %s", to)
        return True
    except Exception:
        logging.error("Failed to send weekly via SMTP: %s", traceback.format_exc())
        raise


def send_email_report_weekly(ti, **context):
    """
    Attempt to send weekly report via Gmail API if available, otherwise fallback to SMTP.
    Pulls HTML from XCom (sre_weekly_html_report).
    """
    try:
        html_report = ti.xcom_pull(key="sre_weekly_html_report")
        if not html_report or "<html" not in html_report.lower():
            logging.error("No valid weekly HTML report found in XCom.")
            raise ValueError("HTML report missing or invalid.")

        chart_b64 = ti.xcom_pull(key="chart_b64_weekly") or ti.xcom_pull(key="chart_b64")  # accept either key
        inline_images_b64 = []
        inline_images_bytes = {}

        if chart_b64:
            try:
                if isinstance(chart_b64, str):
                    inline_images_b64.append(("chart_image", chart_b64))
                    inline_images_bytes["chart_image"] = base64.b64decode(chart_b64)
                elif isinstance(chart_b64, dict):
                    for k, v in chart_b64.items():
                        inline_images_b64.append((k, v))
                        inline_images_bytes[k] = base64.b64decode(v)
                logging.info("Prepared inline images for weekly email (if any).")
            except Exception:
                logging.warning("Failed to prepare inline chart image (weekly): %s", traceback.format_exc())

        sender = Variable.get("ltai.v3.mediamelon.mediamelon_from_address")
        recipients = Variable.get("ltai.v3.mediamelon.mediamelon_to_address"," ").split(",")
        subject = f"Mediamelon SRE Weekly Report - {datetime.utcnow().strftime('%Y-%m-%d')} (Window: {THIS_WEEK})"

        service = authenticate_gmail()
        if service:
            try:
                send_result = send_via_gmail_api_weekly(service, sender, recipients, subject, html_report, inline_images_bytes or None)
                ti.xcom_push(key="email_send_status_weekly", value={"method": "gmail_api", "result": send_result})
                return {"status": "sent", "method": "gmail_api", "result": send_result}
            except Exception:
                logging.warning("Gmail API send failed for weekly report, falling back to SMTP: %s", traceback.format_exc())

        try:
            send_via_smtp_weekly(sender, recipients, subject, html_report, inline_images_b64 or None)
            ti.xcom_push(key="email_send_status_weekly", value={"method": "smtp", "result": "ok"})
            return {"status": "sent", "method": "smtp", "result": "ok"}
        except Exception:
            logging.error("SMTP send also failed for weekly report: %s", traceback.format_exc())
            raise

    except Exception:
        logging.error("Failed to send weekly email report: %s", traceback.format_exc())
        raise

# -------------------------------
# DAG definition and wiring (weekly)
# -------------------------------
with DAG(
    dag_id="sre_mediamelon_sre_report_weekly_v1",
    default_args=default_args,
    schedule_interval="30 5 * * 0",  # 05:30 UTC on Sunday = 11:00 AM IST on Sunday (weekly)
    catchup=False,
    tags=["sre", "mediamelon", "weekly", "11am-ist"],
    max_active_runs=1,
) as dag:

    # Node-level and Pod-level tasks (this week)
    t_node_cpu_this_week = PythonOperator(task_id="node_cpu_this_week", python_callable=node_cpu_this_week, provide_context=True)
    t_node_memory_this_week = PythonOperator(task_id="node_memory_this_week", python_callable=node_memory_this_week, provide_context=True)
    t_node_disk_this_week = PythonOperator(task_id="node_disk_this_week", python_callable=node_disk_this_week, provide_context=True)

    t_pod_cpu_this_week = PythonOperator(task_id="pod_cpu_this_week", python_callable=pod_cpu_this_week, provide_context=True)
    t_pod_memory_this_week = PythonOperator(task_id="pod_memory_this_week", python_callable=pod_memory_this_week, provide_context=True)

    # Last week tasks
    t_node_cpu_last_week = PythonOperator(task_id="node_cpu_last_week", python_callable=node_cpu_last_week, provide_context=True)
    t_node_memory_last_week = PythonOperator(task_id="node_memory_last_week", python_callable=node_memory_last_week, provide_context=True)
    t_node_disk_last_week = PythonOperator(task_id="node_disk_last_week", python_callable=node_disk_last_week, provide_context=True)
    t_pod_cpu_last_week = PythonOperator(task_id="pod_cpu_last_week", python_callable=pod_cpu_last_week, provide_context=True)
    t_pod_memory_last_week = PythonOperator(task_id="pod_memory_last_week", python_callable=pod_memory_last_week, provide_context=True)

    # Comparison tasks (week vs previous week)
    t_node_cpu_compare_week = PythonOperator(task_id="node_cpu_week_compare", python_callable=node_cpu_week_compare, provide_context=True)
    t_node_memory_compare_week = PythonOperator(task_id="node_memory_week_compare", python_callable=node_memory_week_compare, provide_context=True)
    t_node_disk_compare_week = PythonOperator(task_id="node_disk_week_compare", python_callable=node_disk_week_compare, provide_context=True)
    t_pod_cpu_compare_week = PythonOperator(task_id="pod_cpu_week_compare", python_callable=pod_cpu_week_compare, provide_context=True)
    t_pod_memory_compare_week = PythonOperator(task_id="pod_memory_week_compare", python_callable=pod_memory_week_compare, provide_context=True)

    # Namespace dynamic mapping flow (weekly)
    ns_list_weekly = list_namespaces_weekly()
    ns_results_weekly = process_namespace_weekly.expand(ns=ns_list_weekly)
    ns_aggregated_weekly = collect_namespace_results_weekly(ns_results_weekly)
    ns_markdown_weekly = compile_namespace_report_weekly(ns_aggregated_weekly)

    # Compile node report (uses comparison outputs) - weekly equivalent
    def compile_node_report_weekly(ti, **context):
        node_cpu_cmp_val = ti.xcom_pull(key="node_cpu_week_compare")
        node_mem_cmp_val = ti.xcom_pull(key="node_memory_week_compare")
        node_disk_cmp_val = ti.xcom_pull(key="node_disk_week_compare")

        sections = []
        sections.append("## Node-Level Weekly Metrics Summary\n")
        if node_cpu_cmp_val:
            sections.append("### Node CPU Utilisation (This Week vs Previous Week)\n")
            sections.append(node_cpu_cmp_val + "\n")
        if node_mem_cmp_val:
            sections.append("### Node Memory Utilisation (This Week vs Previous Week)\n")
            sections.append(node_mem_cmp_val + "\n")
        if node_disk_cmp_val:
            sections.append("### Node Disk Utilisation (This Week vs Previous Week)\n")
            sections.append(node_disk_cmp_val + "\n")
        report = "\n".join(sections)
        ti.xcom_push(key="node_weekly_markdown", value=report)
        return report

    t_compile_node_report_weekly = PythonOperator(task_id="compile_node_report_weekly", python_callable=compile_node_report_weekly, provide_context=True)

    # Combine node_markdown + namespace_markdown into final combined markdown (weekly)
    def combine_reports_weekly(ti, **context):
        node_md = ti.xcom_pull(key="node_weekly_markdown") or ""
        ns_md = ti.xcom_pull(task_ids="compile_namespace_report_weekly") or ""
        combined = f"# Mediamelon SRE Weekly Report\n\n{node_md}\n\n---\n\n{ns_md}\n\n"
        ti.xcom_push(key="combined_markdown_weekly", value=combined)
        return combined

    t_combine_reports_weekly = PythonOperator(task_id="combine_reports_weekly", python_callable=combine_reports_weekly, provide_context=True)

    # Summary extraction (weekly)
    def extract_and_combine_summary_weekly(ti, **context):
        node_md = ti.xcom_pull(key="node_weekly_markdown") or ""
        ns_md = ti.xcom_pull(task_ids="compile_namespace_report_weekly") or ""

        def summarize(text):
            try:
                prompt = (
                    "From the following weekly report, extract ONLY a clean 5-bullet summary "
                    "highlighting: CPU trends, Memory trends, anomalies, highest usage namespaces, and actions needed. "
                    "Do NOT include code, raw metrics, or long pod names.\n"
                    + (text or "")[:5000]
                )
                summary = get_ai_response(prompt)
                return summary.strip()
            except Exception:
                logging.error("Weekly summary extraction failed: %s", traceback.format_exc())
                return "Summary unavailable."

        node_summary = summarize(node_md)
        ns_summary = summarize(ns_md)

        combined_summary = (
            "## Weekly Summary Highlights\n\n"
            "### Node Level Overview\n"
            + node_summary
            + "\n\n### Namespace/Pod Overview\n"
            + ns_summary
            + "\n"
        )
        ti.xcom_push(key="combined_summary_md_weekly", value=combined_summary)
        return combined_summary

    t_extract_summary_weekly = PythonOperator(task_id="extract_and_combine_summary_weekly", python_callable=extract_and_combine_summary_weekly, provide_context=True)

    # Final compile (this uses compile_sre_report_weekly to push sre_weekly_full_report)
    t_compile_sre_report_weekly = PythonOperator(task_id="compile_sre_report_weekly", python_callable=compile_sre_report_weekly, provide_context=True)

    # Convert to HTML -> send email
    t_convert_to_html_weekly = PythonOperator(task_id="convert_to_html_weekly", python_callable=convert_to_html_weekly, provide_context=True)
    t_send_email_weekly = PythonOperator(task_id="send_email_report_weekly", python_callable=send_email_report_weekly, provide_context=True)

    # -----------------------
    # DAG wiring (weekly)
    # -----------------------

    # Start node/pod tasks in parallel
    [t_node_cpu_this_week, t_node_memory_this_week, t_node_disk_this_week, t_pod_cpu_this_week, t_pod_memory_this_week]
    [t_node_cpu_last_week, t_node_memory_last_week, t_node_disk_last_week, t_pod_cpu_last_week, t_pod_memory_last_week]

    # Wire comparisons (this week + last week -> comparison)
    [t_node_cpu_this_week, t_node_cpu_last_week] >> t_node_cpu_compare_week
    [t_node_memory_this_week, t_node_memory_last_week] >> t_node_memory_compare_week
    [t_node_disk_this_week, t_node_disk_last_week] >> t_node_disk_compare_week
    [t_pod_cpu_this_week, t_pod_cpu_last_week] >> t_pod_cpu_compare_week
    [t_pod_memory_this_week, t_pod_memory_last_week] >> t_pod_memory_compare_week

    # After comparisons -> compile node report
    [t_node_cpu_compare_week, t_node_memory_compare_week, t_node_disk_compare_week] >> t_compile_node_report_weekly

    # Namespace compile -> combine reports
    ns_markdown_weekly >> t_combine_reports_weekly
    t_compile_node_report_weekly >> t_combine_reports_weekly

    # Combine -> extract summary -> compile final -> html -> email
    t_combine_reports_weekly >> t_extract_summary_weekly >> t_compile_sre_report_weekly >> t_convert_to_html_weekly >> t_send_email_weekly