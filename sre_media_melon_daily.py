# sre-mediamelon-daily-updated.py
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
SMTP_HOST = Variable.get("ltai.v3.mediamelon.smtp.host",default_var="mail.authsmtp.com")
SMTP_PORT = int(Variable.get("ltai.v3.mediamelon.smtp.port"))
SMTP_USER = Variable.get("ltai.v3.mediamelon.smtp.user")
SMTP_PASSWORD = Variable.get("ltai.v3.mediamelon.smtp.password")
SMTP_SUFFIX = Variable.get("ltai.v3.mediamelon.smtp.suffix",default_var="<noreply@mediamelon.com>")

# From/To - MediaMelon
MEDIAMELON_FROM_ADDRESS = Variable.get("ltai.v3.mediamelon.mediamelon_from_address",default_var=SMTP_USER or "noreply@mediamelon.com")
MEDIAMELON_TO_ADDRESS = Variable.get("ltai.v3.mediamelon.mediamelon_to_address", default_var=MEDIAMELON_FROM_ADDRESS)

# Ollama Host for MediaMelon agent
OLLAMA_HOST = Variable.get("MEDIAMELON_OLLAMA_HOST", default_var="http://agentomatic:8000/")

# Gmail credentials (optional) - store JSON string in Variable MEDIAMELON_GMAIL_CREDENTIALS
GMAIL_CREDENTIALS_VAR = "ltai.v3.mediamelon.mediamelon_gmail_credentials"
YESTERDAY = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
TODAY = datetime.utcnow().strftime("%Y-%m-%d")


# ------------------------------
# Preserved MediaMelon helpers (clean quoting)
# ------------------------------
def authenticate_gmail():
    """
    Use stored credentials in Airflow Variable MEDIAMELON_GMAIL_CREDENTIALS to return a Gmail API service.
    Returns None on any failure.
    """
    try:
        creds_json = Variable.get(GMAIL_CREDENTIALS_VAR, default_var=None)
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


def get_ai_response(prompt, conversation_history=None):
    """
    Get AI response with conversation history context (preserve your implementation).
    Robust to different response shapes from ollama client.
    """
    try:
        logging.debug("Query received: %s", (prompt[:200] + "...") if len(prompt) > 200 else prompt)
        if not prompt or not isinstance(prompt, str):
            return "Invalid input provided. Please enter a valid query."

        client = Client(host=OLLAMA_HOST, headers={"x-ltai-client": "media_melon-agent"})
        logging.debug("Connecting to Ollama at %s with model 'appz/sre/media_melon:0.4'", OLLAMA_HOST)

        messages = []
        if conversation_history:
            for history_item in conversation_history:
                # expected: dict with keys "prompt" and "response"
                if isinstance(history_item, dict) and "prompt" in history_item and "response" in history_item:
                    messages.append({"role": "user", "content": history_item["prompt"]})
                    messages.append({"role": "assistant", "content": history_item["response"]})
                else:
                    messages.append({"role": "user", "content": str(history_item)})

        messages.append({"role": "user", "content": prompt})

        response = client.chat(model="appz/sre/media_melon:0.4", messages=messages, stream=False)
        logging.info("Raw response from agent: %s", str(response)[:500] + "..." if response else "None")

        ai_content = None
        # support dict style
        if isinstance(response, dict):
            ai_content = response.get("message", {}).get("content")
        else:
            # object style with attributes
            msg = getattr(response, "message", None)
            if msg:
                ai_content = getattr(msg, "content", None)

        if not ai_content:
            logging.error("Response lacks expected 'message.content' structure")
            return "Invalid response format from AI. Please try again later."

        ai_content = ai_content.strip()
        logging.info("Full message content from agent: %s", (ai_content[:500] + "...") if len(ai_content) > 500 else ai_content)
        return ai_content
    except Exception:
        logging.error("Error in get_ai_response: %s", traceback.format_exc())
        return f"An error occurred while processing your request: {traceback.format_exc()}"


# ------------------------------
# New helpers for JSON -> Markdown table conversion and peak aggregation
# ------------------------------
def safe_parse_json(json_text):
    """Safely parse JSON text that may contain surrounding text; return Python object or None."""
    if not json_text or not isinstance(json_text, str):
        return None
    try:
        # Try direct load first
        return json.loads(json_text)
    except Exception:
        # Try to extract first JSON array/object in the string
        try:
            match = re.search(r"(\[.*\]|\{.*\})", json_text, re.DOTALL)
            if match:
                return json.loads(match.group(0))
        except Exception:
            logging.debug("safe_parse_json failed to extract JSON: %s", traceback.format_exc())
    return None


def json_list_to_markdown_table(json_list, column_defs):
    """
    Convert a list of dicts to a markdown table.
    column_defs: list of tuples (key_in_dict, header_name, format_fn_or_None)
    """
    if not isinstance(json_list, list) or not json_list:
        return None
    headers = [h for (_, h, _) in column_defs]
    header_row = "| " + " | ".join(headers) + " |"
    sep_row = "| " + " | ".join(["---"] * len(headers)) + " |"
    rows = [header_row, sep_row]
    for item in json_list:
        if not isinstance(item, dict):
            continue
        row_cells = []
        for key, _, fmt in column_defs:
            val = item.get(key, "")
            try:
                if fmt and callable(fmt):
                    val = fmt(val)
            except Exception:
                val = item.get(key, "")
            # ensure strings don't break markdown
            row_cells.append(str(val))
        rows.append("| " + " | ".join(row_cells) + " |")
    return "\n".join(rows)


def try_convert_peak_cpu_to_markdown(text):
    """
    Given AI text for cpu peak (likely JSON array), attempt to parse and convert to markdown table.
    Returns markdown string or original text if conversion fails.
    """
    parsed = safe_parse_json(text)
    if isinstance(parsed, list):
        column_defs = [
            ("pod_name", "Pod Name", None),
            ("peak_cpu_cores", "Peak CPU (cores)", lambda v: f"{v}" if v is not None else ""),
            ("timestamp", "Timestamp", None),
        ]
        md = json_list_to_markdown_table(parsed, column_defs)
        if md:
            return md
    return text


def try_convert_peak_memory_to_markdown(text):
    """
    Given AI text for memory peak (likely JSON array), attempt to parse and convert to markdown table.
    Returns markdown string or original text if conversion fails.
    """
    parsed = safe_parse_json(text)
    if isinstance(parsed, list):
        column_defs = [
            ("pod_name", "Pod Name", None),
            ("peak_memory_gb", "Peak Memory (GB)", lambda v: f"{v}" if v is not None else ""),
            ("timestamp", "Timestamp", None),
        ]
        md = json_list_to_markdown_table(parsed, column_defs)
        if md:
            return md
    return text


# === t1: Node CPU – Last 24h ===
def node_cpu_today(ti, **context):
    prompt = (
        "Generate the **node level cpu utilisation for the last 24 hours**"
    )
    response = get_ai_response(prompt)
    ti.xcom_push(key="node_cpu_today", value=response)
    return response


def node_cpu_yesterday(ti, **context):
    prompt = "Generate the **node level cpu utilisation for yesterday**"
    response = get_ai_response(prompt)
    ti.xcom_push(key="node_cpu_yesterday", value=response)
    return response


# === t2: Node Memory – Last 24h ===
def node_memory_today(ti, **context):
    prompt = "Generate the **node level memory utilisation for last 24 hours**"
    response = get_ai_response(prompt)
    ti.xcom_push(key="node_memory_today", value=response)
    return response


def node_memory_yesterday(ti, **context):
    prompt = "Generate the **node level memory utilisation for yesterday**"
    response = get_ai_response(prompt)
    ti.xcom_push(key="node_memory_yesterday", value=response)
    return response


# === t3: Node Disk – Last 24h ===
def node_disk_today(ti, **context):
    prompt = "Generate the **node level disk utilisation for last 24 hours**"
    response = get_ai_response(prompt)
    ti.xcom_push(key="node_disk_today", value=response)
    return response


def node_disk_yesterday(ti, **context):
    prompt = "Generate the **node level disk utilisation for yesterday**"
    response = get_ai_response(prompt)
    ti.xcom_push(key="node_disk_yesterday", value=response)
    return response


# === t4: Pod CPU – Last 24h ===
def pod_cpu_today(ti, **context):
    prompt = "Generate the **pod level cpu utilisation for last 24 hours**"
    response = get_ai_response(prompt)
    ti.xcom_push(key="pod_cpu_today", value=response)
    return response


def pod_cpu_yesterday(ti, **context):
    prompt = "Generate the **pod level cpu utilisation for yesterday**"
    response = get_ai_response(prompt)
    ti.xcom_push(key="pod_cpu_yesterday", value=response)
    return response


# === t5: Pod Memory – Last 24h ===
def pod_memory_today(ti, **context):
    prompt = "Generate the **pod level memory utilisation for last 24 hours**"
    response = get_ai_response(prompt)
    ti.xcom_push(key="pod_memory_today", value=response)
    return response


def pod_memory_yesterday(ti, **context):
    prompt = "Generate the **pod level memory utilisation for yesterday**"
    response = get_ai_response(prompt)
    ti.xcom_push(key="pod_memory_yesterday", value=response)
    return response

# -------------------------------
# Comparison functions (wrappers)
# -------------------------------
def node_cpu_today_vs_yesterday(ti, **context):
    node_cpu_today_val = ti.xcom_pull(key="node_cpu_today")
    node_cpu_yesterday_val = ti.xcom_pull(key="node_cpu_yesterday")
    if not node_cpu_today_val or not node_cpu_yesterday_val:
        logging.warning("Missing node CPU XComs for comparison")
    prompt = (
        "Compare today's and yesterday's node CPU.\n\n"
        "Today's:\n" + (node_cpu_today_val or "No data") + "\n\n"
        "Yesterday's:\n" + (node_cpu_yesterday_val or "No data") + "\n\n"
        "Produce a table comparing avg and max per node and list significant changes."
    )
    response = get_ai_response(prompt)
    ti.xcom_push(key="node_cpu_today_vs_yesterday", value=response)
    return response


def node_memory_today_vs_yesterday(ti, **context):
    node_mem_today_val = ti.xcom_pull(key="node_memory_today")
    node_mem_yesterday_val = ti.xcom_pull(key="node_memory_yesterday")
    prompt = (
        "Compare today's and yesterday's node memory.\n\n"
        "Today's:\n" + (node_mem_today_val or "No data") + "\n\n"
        "Yesterday's:\n" + (node_mem_yesterday_val or "No data")
    )
    response = get_ai_response(prompt)
    ti.xcom_push(key="node_memory_today_vs_yesterday", value=response)
    return response


def node_disk_today_vs_yesterday(ti, **context):
    node_disk_today_val = ti.xcom_pull(key="node_disk_today")
    node_disk_yesterday_val = ti.xcom_pull(key="node_disk_yesterday")
    prompt = (
        "Compare today's and yesterday's node disk usage.\n\n"
        "Today's:\n" + (node_disk_today_val or "No data") + "\n\n"
        "Yesterday's:\n" + (node_disk_yesterday_val or "No data")
    )
    response = get_ai_response(prompt)
    ti.xcom_push(key="node_disk_today_vs_yesterday", value=response)
    return response


def pod_cpu_today_vs_yesterday(ti, **context):
    pod_cpu_today_val = ti.xcom_pull(key="pod_cpu_today")
    pod_cpu_yesterday_val = ti.xcom_pull(key="pod_cpu_yesterday")
    prompt = (
        "Compare today's and yesterday's pod CPU.\n\n"
        "Today's:\n" + (pod_cpu_today_val or "No data") + "\n\n"
        "Yesterday's:\n" + (pod_cpu_yesterday_val or "No data")
    )
    response = get_ai_response(prompt)
    ti.xcom_push(key="pod_cpu_today_vs_yesterday", value=response)
    return response


def pod_memory_today_vs_yesterday(ti, **context):
    pod_mem_today_val = ti.xcom_pull(key="pod_memory_today")
    pod_mem_yesterday_val = ti.xcom_pull(key="pod_memory_yesterday")
    prompt = (
        "Compare today's and yesterday's pod memory.\n\n"
        "Today's:\n" + (pod_mem_today_val or "No data") + "\n\n"
        "Yesterday's:\n" + (pod_mem_yesterday_val or "No data")
    )
    response = get_ai_response(prompt)
    ti.xcom_push(key="pod_memory_today_vs_yesterday", value=response)
    return response


# -------------------------------
# Namespace-level dynamic tasks
# -------------------------------
@task
def list_namespaces():
    """Get list of namespaces from Prometheus via AI prompt"""
    namespace_prompt = (
        "Run this Prometheus query:\n"
        "count by (namespace) (kube_pod_info)\n\n"
        "Return as a clean JSON array of namespace strings, e.g.:\n"
        '["namespace1", "namespace2"]\n\n'
        "Only output the JSON array."
    )
    response = get_ai_response(namespace_prompt)
    logging.info("Full message content from agent (namespaces): %s", (response[:500] + "...") if response else "None")
    try:
        match = re.search(r"\[.*?\]", response or "", re.DOTALL)
        namespaces = json.loads(match.group(0)) if match else []
        logging.info("Parsed namespaces: %s", namespaces)
        return namespaces
    except Exception:
        logging.error("Failed to parse namespace list: %s", traceback.format_exc())
        return []


@task
def process_namespace(ns: str):
    """Process namespace with peak metrics + limits + disk"""
    logging.info("Processing namespace: %s", ns)

    cpu_peak_prompt = (
        f"Get the peak CPU usage for every pod in namespace '{ns}' over the last 24 hours.\n\n"
        "Return a JSON array of objects with keys: pod_name, peak_cpu_cores, timestamp.\n"
        "Example output:\n"
        '[{"pod_name":"pod-a","peak_cpu_cores":0.125,"timestamp":"2025-11-14T12:34:56Z"}, '
        '{"pod_name":"pod-b","peak_cpu_cores":0.456,"timestamp":"2025-11-14T13:22:11Z"}]'
    )

    mem_peak_prompt = (
        f"Get the peak memory usage for every pod in namespace '{ns}' over the last 24 hours.\n\n"
        "Return a JSON array of objects with keys: pod_name, peak_memory_gb, timestamp.\n"
        "Example output:\n"
        '[{"pod_name":"pod-a","peak_memory_gb":0.12,"timestamp":"2025-11-14T12:34:56Z"}, '
        '{"pod_name":"pod-b","peak_memory_gb":0.45,"timestamp":"2025-11-14T13:22:11Z"}]'
    )

    try:
        cpu_response = get_ai_response(cpu_peak_prompt)
        memory_response = get_ai_response(mem_peak_prompt)
        return {"namespace": ns, "cpu_peak": cpu_response, "memory_peak": memory_response, "status": "success"}
    except Exception:
        logging.error("Error processing namespace %s: %s", ns, traceback.format_exc())
        return {"namespace": ns, "error": "processing failed", "status": "failed"}


@task
def collect_namespace_results(namespace_results: list):
    """Aggregate results from all namespace processing tasks"""
    logging.info("Collecting results from %d namespaces", len(namespace_results or []))
    aggregated = {}
    failed_namespaces = []
    for result in (namespace_results or []):
        if not result:
            logging.warning("Skipping empty result")
            continue
        ns = result.get("namespace")
        if result.get("status") == "failed":
            failed_namespaces.append(ns)
            logging.warning("Namespace %s processing failed: %s", ns, result.get("error"))
        else:
            aggregated[ns] = result
    logging.info("Successfully processed %d namespaces", len(aggregated))
    if failed_namespaces:
        logging.warning("Failed namespaces: %s", failed_namespaces)
    return {
        "total_namespaces": len(namespace_results or []),
        "successful": len(aggregated),
        "failed": len(failed_namespaces),
        "results": aggregated,
        "failed_namespaces": failed_namespaces,
    }


@task
def compile_namespace_report(namespace_data):
    """Convert namespace data into markdown report and compute overall peak summary"""
    logging.info("Compiling namespace metrics into markdown report")
    if not namespace_data or not namespace_data.get("results"):
        return "## Pod-Level Metrics by Namespace\n\nNo namespace data available."

    markdown_sections = []
    markdown_sections.append("## Pod-Level Metrics by Namespace\n")
    markdown_sections.append(
        f"**Total Namespaces Analyzed**: {namespace_data.get('successful', 0)}/{namespace_data.get('total_namespaces', 0)}\n"
    )

    if namespace_data.get("failed_namespaces"):
        markdown_sections.append("**Failed Namespaces**: " + ", ".join(namespace_data["failed_namespaces"]) + "\n")

    markdown_sections.append("---\n")

    # For overall peaks computation
    overall_cpu_peaks = []  # list of tuples (namespace, pod_name, peak_cpu, timestamp)
    overall_mem_peaks = []  # list of tuples (namespace, pod_name, peak_mem_gb, timestamp)

    for ns in sorted(namespace_data.get("results", {})):
        data = namespace_data["results"][ns]
        markdown_sections.append(f"\n### Namespace: `{ns}`\n")

        # CPU peaks: attempt conversion to markdown table
        cpu_raw = data.get("cpu_peak")
        if cpu_raw:
            markdown_sections.append("\n#### Peak CPU Usage (Per Pod)\n")
            try:
                cpu_md = try_convert_peak_cpu_to_markdown(cpu_raw)
                markdown_sections.append(cpu_md + "\n")
                # Try to parse for overall peaks
                parsed_cpu = safe_parse_json(cpu_raw)
                if isinstance(parsed_cpu, list):
                    for item in parsed_cpu:
                        try:
                            pod_name = item.get("pod_name")
                            peak = item.get("peak_cpu_cores")
                            ts = item.get("timestamp")
                            if pod_name and peak is not None:
                                overall_cpu_peaks.append((ns, pod_name, float(peak), ts))
                        except Exception:
                            logging.debug("Error parsing individual cpu item: %s", traceback.format_exc())
            except Exception:
                logging.warning("Failed to convert cpu_peak for namespace %s: %s", ns, traceback.format_exc())
                markdown_sections.append(cpu_raw + "\n")
        else:
            markdown_sections.append("\n#### Peak CPU Usage (Per Pod)\n")
            markdown_sections.append("No CPU peak data available.\n")

        # Memory peaks: attempt conversion to markdown table
        mem_raw = data.get("memory_peak")
        if mem_raw:
            markdown_sections.append("\n#### Peak Memory Usage (Per Pod)\n")
            try:
                mem_md = try_convert_peak_memory_to_markdown(mem_raw)
                markdown_sections.append(mem_md + "\n")
                # Try to parse for overall peaks
                parsed_mem = safe_parse_json(mem_raw)
                if isinstance(parsed_mem, list):
                    for item in parsed_mem:
                        try:
                            pod_name = item.get("pod_name")
                            peak = item.get("peak_memory_gb")
                            ts = item.get("timestamp")
                            if pod_name and peak is not None:
                                overall_mem_peaks.append((ns, pod_name, float(peak), ts))
                        except Exception:
                            logging.debug("Error parsing individual mem item: %s", traceback.format_exc())
            except Exception:
                logging.warning("Failed to convert memory_peak for namespace %s: %s", ns, traceback.format_exc())
                markdown_sections.append(mem_raw + "\n")
        else:
            markdown_sections.append("\n#### Peak Memory Usage (Per Pod)\n")
            markdown_sections.append("No memory peak data available.\n")

        markdown_sections.append("---\n")

    # Compute simple programmatic highlights (to give the AI a concise input)
    top_cpu = None
    top_mem = None
    try:
        if overall_cpu_peaks:
            overall_cpu_peaks_sorted = sorted(overall_cpu_peaks, key=lambda x: x[2], reverse=True)
            top_cpu = overall_cpu_peaks_sorted[0]  # (ns, pod_name, peak, ts)
        if overall_mem_peaks:
            overall_mem_peaks_sorted = sorted(overall_mem_peaks, key=lambda x: x[2], reverse=True)
            top_mem = overall_mem_peaks_sorted[0]  # (ns, pod_name, peak, ts)
    except Exception:
        logging.debug("Error computing top peaks: %s", traceback.format_exc())

    # Append an AI-friendly data summary that will be included in overall summary prompt
    markdown_sections.append("\n---\n")
    markdown_sections.append("## Namespace Peak Metrics Summary\n")
    ai_input_lines = []
    ai_input_lines.append(f"Total namespaces analyzed: {namespace_data.get('successful', 0)}")
    if top_cpu:
        ai_input_lines.append(f"Highest CPU peak: pod `{top_cpu[1]}` in namespace `{top_cpu[0]}` with {top_cpu[2]} cores at {top_cpu[3]}")
    else:
        ai_input_lines.append("Highest CPU peak: None")
    if top_mem:
        ai_input_lines.append(f"Highest Memory peak: pod `{top_mem[1]}` in namespace `{top_mem[0]}` with {top_mem[2]} GB at {top_mem[3]}")
    else:
        ai_input_lines.append("Highest Memory peak: None")
    # Add counts of namespaces with any peaks above simple thresholds (optional quick insight)
    try:
        cpu_over_thresh = len([1 for (_, _, p, _) in overall_cpu_peaks if p > 0.5])  # example threshold 0.5 cores
        mem_over_thresh = len([1 for (_, _, m, _) in overall_mem_peaks if m > 1.0])   # example threshold 1.0 GB
        ai_input_lines.append(f"Namespaces with pod CPU peak > 0.5 cores: {cpu_over_thresh}")
        ai_input_lines.append(f"Namespaces with pod Memory peak > 1.0 GB: {mem_over_thresh}")
    except Exception:
        pass

    markdown_sections.append("\n".join(ai_input_lines) + "\n")
    markdown_sections.append("---\n")

    report = "\n".join(markdown_sections)
    logging.info("Generated namespace markdown with %d sections", len(markdown_sections))
    return report


# -------------------------------
# Compile and overall summary functions (combine everything)
# -------------------------------
def compile_sre_report(ti, **context):
    """
    Combine all XComs (node, pod, namespace, comparisons) into a single markdown report.
    """
    try:
        logging.info("Compiling final SRE markdown report for Mediamelon")

        node_cpu = ti.xcom_pull(key="node_cpu_today") or "No CPU data"
        node_memory = ti.xcom_pull(key="node_memory_today") or "No memory data"
        node_disk = ti.xcom_pull(key="node_disk_today") or "No disk data"
        pod_cpu = ti.xcom_pull(key="pod_cpu_today") or "No pod CPU data"
        pod_memory = ti.xcom_pull(key="pod_memory_today") or "No pod memory data"
       

        node_cpu_cmp = ti.xcom_pull(key="node_cpu_today_vs_yesterday") or "No comparison"
        node_mem_cmp = ti.xcom_pull(key="node_memory_today_vs_yesterday") or "No comparison"
        node_disk_cmp = ti.xcom_pull(key="node_disk_today_vs_yesterday") or "No comparison"
        pod_cpu_cmp = ti.xcom_pull(key="pod_cpu_today_vs_yesterday") or "No comparison"
        pod_mem_cmp = ti.xcom_pull(key="pod_memory_today_vs_yesterday") or "No comparison"
    
        namespace_md = ti.xcom_pull(task_ids="compile_namespace_report") or "No namespace report generated."

        report = (
            "# Mediamelon SRE Daily Report\n"
            f"**Generated**: **{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}**\n\n"
            "---\n\n"
            "## 1. Node-Level Metrics (Last 24h)\n"
            f"{node_cpu}\n\n"
            "---\n\n"
            f"{node_memory}\n\n"
            "---\n\n"
            f"{node_disk}\n\n"
            "---\n\n"
            "## 2. Pod-Level Metrics (Last 24h)\n"
            f"{pod_cpu}\n\n"
            "---\n\n"
            f"{pod_memory}\n\n"
            "---\n\n"
            "## 6. Namespace-Level Pod Peaks\n"
            f"{namespace_md}\n\n"
            "---\n\n"
            "## 7. Node-Level Metrics (Today vs Yesterday)\n"
            f"{node_cpu_cmp}\n\n"
            "---\n\n"
            f"{node_mem_cmp}\n\n"
            "---\n\n"
            f"{node_disk_cmp}\n\n"
            "---\n\n"
            "## 8. Pod-Level Metrics (Today vs Yesterday)\n"
            f"{pod_cpu_cmp}\n\n"
            "---\n\n"
            f"{pod_mem_cmp}\n\n"
            "---\n\n"
            "## 10. Overall Summary\n"
            "(See consolidated summary)\n\n"
            "---\n"
        )

        report = re.sub(r"\n{3,}", "\n\n", report.strip())
        ti.xcom_push(key="sre_full_report", value=report)
        logging.info("SRE report compiled successfully and pushed to XCom (sre_full_report).")
        return report
    except Exception:
        logging.error("Failed to compile SRE report: %s", traceback.format_exc())
        raise


def overall_summary(ti, **context):
    """
    Generate an overall summary combining today's metrics and comparisons across all monitored components.
    Uses get_ai_response to produce the final textual summary. Includes namespace peak section (markdown) to the AI prompt.
    """
    try:
        node_cpu = ti.xcom_pull(key="node_cpu_today") or "No CPU data"
        node_memory = ti.xcom_pull(key="node_memory_today") or "No memory data"
        node_disk = ti.xcom_pull(key="node_disk_today") or "No disk data"
        pod_cpu = ti.xcom_pull(key="pod_cpu_today") or "No pod CPU data"
        pod_memory = ti.xcom_pull(key="pod_memory_today") or "No pod memory data"
        

        node_cpu_cmp = ti.xcom_pull(key="node_cpu_today_vs_yesterday") or "No comparison data"
        node_mem_cmp = ti.xcom_pull(key="node_memory_today_vs_yesterday") or "No comparison data"
        node_disk_cmp = ti.xcom_pull(key="node_disk_today_vs_yesterday") or "No comparison data"
        pod_cpu_cmp = ti.xcom_pull(key="pod_cpu_today_vs_yesterday") or "No comparison data"
        pod_mem_cmp = ti.xcom_pull(key="pod_memory_today_vs_yesterday") or "No comparison data"

        # Pull the namespace-level markdown report (contains the converted tables and AI input summary)
        namespace_md = ti.xcom_pull(task_ids="compile_namespace_report") or "No namespace report generated."

        # Build prompt for AI summarization that includes namespace peak summary
        prompt = (
            "You are the SRE Mediamelon agent.\n\n"
            "Your task: Generate a complete overall summary for today's SRE report, "
            "followed by a comparative summary analyzing performance trends versus yesterday.\n\n"
            "### Part 1: Today's Summary\n"
            "Use the data below to provide a high-level assessment of current cluster health.\n\n"
            "- Node CPU Report: " + node_cpu + "\n"
            "- Node Memory Report: " + node_memory + "\n"
            "- Node Disk Report: " + node_disk + "\n"
            "- Pod CPU Report: " + pod_cpu + "\n"
            "- Pod Memory Report: " + pod_memory + "\n\n"
            "### Namespace Peak Metrics (Pod-level peaks - Tables included below)\n\n"
            + namespace_md + "\n\n"
            "### Part 2: Comparison Summary (Today vs Yesterday)\n"
            "- Node CPU Comparison: " + node_cpu_cmp + "\n"
            "- Node Memory Comparison: " + node_mem_cmp + "\n"
            "- Node Disk Comparison: " + node_disk_cmp + "\n"
            "- Pod CPU Comparison: " + pod_cpu_cmp + "\n"
            "- Pod Memory Comparison: " + pod_mem_cmp + "\n\n"
            "### Instructions:\n"
            "1. Write two clearly separated sections:\n"
            "   - Overall Summary (Today)\n"
            "   - Comparison Summary (Today vs Yesterday)\n"
            "2. Use professional tone, concise sentences, and structured paragraphs.\n"
            "3. Highlight critical alerts, anomalies, and areas of improvement.\n"
            "4. Summarize key positives and potential issues.\n"
            "5. Include top 3 namespaces/pods by peak CPU and top 3 by peak Memory (use the tables above).\n"
            "6. Keep each section to roughly 6-12 sentences.\n"
            "7. Avoid repeating raw metrics from tables; focus on interpretation and action items.\n"
        )
        response = get_ai_response(prompt)
        ti.xcom_push(key="overall_summary", value=response)
        logging.info("Overall summary generated and pushed to XCom.")
        return response
    except Exception:
        logging.error("Failed to generate overall summary: %s", traceback.format_exc())
        raise
# -------------------------------
# Markdown -> HTML conversion (TradeIdeas template)
# -------------------------------
def preprocess_markdown(markdown_text):
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


def convert_to_html(ti, **context):
    """
    Convert the combined markdown SRE report into clean, responsive, Gmail-safe HTML using TradeIdeas template.
    """
    try:
        markdown_report = ti.xcom_pull(key="sre_full_report") or "# No report generated."
        logging.info("Converting markdown report, length=%d", len(markdown_report))
        markdown_report = preprocess_markdown(markdown_report)

        html_body = None
        try:
            html_body = markdown.markdown(
                markdown_report,
                extensions=["tables", "fenced_code", "nl2br", "sane_lists", "attr_list"],
            )
            logging.info("Used 'markdown' library for conversion")
        except Exception:
            logging.warning("markdown lib error, trying fallback converters")

        if not html_body or len(html_body) < 50:
            try:
                import markdown2

                html_body = markdown2.markdown(markdown_report, extras=["tables", "fenced-code-blocks", "break-on-newline"])
                logging.info("Used markdown2 fallback for conversion")
            except Exception:
                logging.debug("markdown2 not available or failed")

        if not html_body or len(html_body) < 50:
            try:
                import mistune

                html_body = mistune.html(markdown_report)
                logging.info("Used mistune fallback for conversion")
            except Exception:
                logging.error("All markdown conversions failed; using preformatted fallback")
                html_body = "<pre>{}</pre>".format(html.escape(markdown_report))

        # TradeIdeas-style HTML wrapper/template (clean and responsive)
        full_html = (
            "<!DOCTYPE html>\n"
            "<html>\n"
            "<head>\n"
            '<meta charset="UTF-8">\n'
            '<meta name="viewport" content="width=device-width, initial-scale=1.0">\n'
            "<title>Mediamelon SRE Daily Report</title>\n"
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

        ti.xcom_push(key="sre_html_report", value=full_html)
        logging.info("HTML generated, length=%d and pushed to XCom (sre_html_report).", len(full_html))
        return full_html
    except Exception:
        logging.error("Failed to convert markdown to HTML: %s", traceback.format_exc())
        raise


# -------------------------------
# Email sending - tries Gmail API first, falls back to SMTP
# -------------------------------
def build_gmail_message(sender: str, to: list, subject: str, html_body: str, inline_images: dict = None):
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
        logging.error("Failed to build Gmail message: %s", traceback.format_exc())
        raise


def send_via_gmail_api(service, sender: str, to: list, subject: str, html_body: str, inline_images: dict = None):
    """
    Send email via Gmail API service object.
    """
    try:
        message = build_gmail_message(sender, to, subject, html_body, inline_images)
        send_result = service.users().messages().send(userId="me", body=message).execute()
        logging.info("Sent email via Gmail API: %s", send_result.get("id"))
        return send_result
    except HttpError:
        logging.error("Gmail API HttpError: %s", traceback.format_exc())
        raise
    except Exception:
        logging.error("Failed to send via Gmail API: %s", traceback.format_exc())
        raise


def send_via_smtp(sender: str, to: list, subject: str, html_body: str, inline_images_b64: list = None):
    """
    Send email via SMTP server, using STARTTLS and login credentials.
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
                    logging.warning("Failed to attach inline image %s: %s", cid, traceback.format_exc())

        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20)
        server.starttls()
        if SMTP_USER and SMTP_PASSWORD:
            server.login(SMTP_USER, SMTP_PASSWORD)
        server.sendmail(sender, to, msg.as_string())
        server.quit()
        logging.info("Sent email via SMTP to: %s", to)
        return True
    except Exception:
        logging.error("Failed to send via SMTP: %s", traceback.format_exc())
        raise


def send_email_report(ti, **context):
    """
    Attempt to send via Gmail API if available, otherwise fallback to SMTP.
    Pulls HTML from XCom (sre_html_report).
    """
    try:
        html_report = ti.xcom_pull(key="sre_html_report")
        if not html_report or "<html" not in html_report.lower():
            logging.error("No valid HTML report found in XCom.")
            raise ValueError("HTML report missing or invalid.")

        chart_b64 = ti.xcom_pull(key="chart_b64")
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
                logging.info("Prepared inline images for email (if any).")
            except Exception:
                logging.warning("Failed to prepare inline chart image: %s", traceback.format_exc())

        sender = Variable.get("ltai.v3.mediamelon.mediamelon_to_address")
        recipients = Variable.get("ltai.v3.mediamelon.mediamelon_from_address").split(",")
        subject = f"Mediamelon SRE Daily Report - {datetime.utcnow().strftime('%Y-%m-%d')}"

        service = authenticate_gmail()
        if service:
            try:
                send_result = send_via_gmail_api(service, sender, recipients, subject, html_report, inline_images_bytes or None)
                ti.xcom_push(key="email_send_status", value={"method": "gmail_api", "result": send_result})
                return {"status": "sent", "method": "gmail_api", "result": send_result}
            except Exception:
                logging.warning("Gmail API send failed, falling back to SMTP: %s", traceback.format_exc())

        try:
            send_via_smtp(sender, recipients, subject, html_report, inline_images_b64 or None)
            ti.xcom_push(key="email_send_status", value={"method": "smtp", "result": "ok"})
            return {"status": "sent", "method": "smtp", "result": "ok"}
        except Exception:
            logging.error("SMTP send also failed: %s", traceback.format_exc())
            raise

    except Exception:
        logging.error("Failed to send email report: %s", traceback.format_exc())
        raise


# -------------------------------
# DAG definition and wiring (parallel groups, dynamic mapping)
# -------------------------------
with DAG(
    dag_id="sre_mediamelon_sre_report_daily_v1",
    default_args=default_args,
    schedule_interval="30 5 * * *",  # 05:30 UTC = 11:00 AM IST
    catchup=False,
    tags=["sre", "mediamelon", "daily", "11am-ist"],
    max_active_runs=1,
    max_active_tasks=64,
) as dag:

    # Node-level and Pod-level tasks (today)
    t_node_cpu_today = PythonOperator(task_id="node_cpu_today", python_callable=node_cpu_today, provide_context=True)
    t_node_memory_today = PythonOperator(task_id="node_memory_today", python_callable=node_memory_today, provide_context=True)
    t_node_disk_today = PythonOperator(task_id="node_disk_today", python_callable=node_disk_today, provide_context=True)

    t_pod_cpu_today = PythonOperator(task_id="pod_cpu_today", python_callable=pod_cpu_today, provide_context=True)
    t_pod_memory_today = PythonOperator(task_id="pod_memory_today", python_callable=pod_memory_today, provide_context=True)
   
    # Yesterday tasks
    t_node_cpu_yesterday = PythonOperator(task_id="node_cpu_yesterday", python_callable=node_cpu_yesterday, provide_context=True)
    t_node_memory_yesterday = PythonOperator(task_id="node_memory_yesterday", python_callable=node_memory_yesterday, provide_context=True)
    t_node_disk_yesterday = PythonOperator(task_id="node_disk_yesterday", python_callable=node_disk_yesterday, provide_context=True)
    t_pod_cpu_yesterday = PythonOperator(task_id="pod_cpu_yesterday", python_callable=pod_cpu_yesterday, provide_context=True)
    t_pod_memory_yesterday = PythonOperator(task_id="pod_memory_yesterday", python_callable=pod_memory_yesterday, provide_context=True)
    
    # Comparison tasks
    t_node_cpu_compare = PythonOperator(task_id="node_cpu_compare", python_callable=node_cpu_today_vs_yesterday, provide_context=True)
    t_node_memory_compare = PythonOperator(task_id="node_memory_compare", python_callable=node_memory_today_vs_yesterday, provide_context=True)
    t_node_disk_compare = PythonOperator(task_id="node_disk_compare", python_callable=node_disk_today_vs_yesterday, provide_context=True)
    t_pod_cpu_compare = PythonOperator(task_id="pod_cpu_compare",python_callable=pod_cpu_today_vs_yesterday,provide_context=True)
    t_pod_memory_compare = PythonOperator(task_id="pod_memory_compare",python_callable=pod_memory_today_vs_yesterday,provide_context=True)

    # Namespace dynamic mapping flow
    ns_list = list_namespaces()
    ns_results = process_namespace.expand(ns=ns_list)
    ns_aggregated = collect_namespace_results(ns_results)
    ns_markdown = compile_namespace_report(ns_aggregated)

    # Compile node report (uses comparison outputs)
    def compile_node_report(ti, **context):
        node_cpu_cmp_val = ti.xcom_pull(key="node_cpu_today_vs_yesterday")
        node_mem_cmp_val = ti.xcom_pull(key="node_memory_today_vs_yesterday")
        node_disk_cmp_val = ti.xcom_pull(key="node_disk_today_vs_yesterday")

        sections = []
        sections.append("## Node-Level Metrics Summary\n")
        if node_cpu_cmp_val:
            sections.append("### Node CPU Utilization (Today vs Yesterday)\n")
            sections.append(node_cpu_cmp_val + "\n")
        if node_mem_cmp_val:
            sections.append("### Node Memory Utilization (Today vs Yesterday)\n")
            sections.append(node_mem_cmp_val + "\n")
        if node_disk_cmp_val:
            sections.append("### Node Disk Utilization (Today vs Yesterday)\n")
            sections.append(node_disk_cmp_val + "\n")
        report = "\n".join(sections)
        ti.xcom_push(key="node_markdown", value=report)
        return report

    t_compile_node_report = PythonOperator(task_id="compile_node_report", python_callable=compile_node_report, provide_context=True)

    # Combine node_markdown + namespace_markdown into final combined markdown
    def combine_reports(ti, **context):
        node_md = ti.xcom_pull(key="node_markdown") or ""
        pod_md = ti.xcom_pull(task_ids="compile_namespace_report") or ""
        combined = f"# Mediamelon SRE Daily Report\n\n{node_md}\n\n---\n\n{pod_md}\n\n"
        ti.xcom_push(key="combined_markdown", value=combined)
        return combined

    t_combine_reports = PythonOperator(task_id="combine_reports", python_callable=combine_reports, provide_context=True)

    # Summary extraction
    def extract_and_combine_summary(ti, **context):
        node_md = ti.xcom_pull(key="node_markdown") or ""
        pod_md = ti.xcom_pull(task_ids="compile_namespace_report") or ""

        def summarize(text):
            try:
                prompt = (
                    "From the following report, extract ONLY a clean 5-bullet summary "
                    "highlighting: CPU trends, Memory trends, anomalies, highest usage namespaces, and actions needed. "
                    "Do NOT include code, raw metrics, or long pod names.\n"
                    + (text or "")[:5000]
                )
                summary = get_ai_response(prompt)
                return summary.strip()
            except Exception:
                logging.error("Summary extraction failed: %s", traceback.format_exc())
                return "Summary unavailable."

        node_summary = summarize(node_md)
        pod_summary = summarize(pod_md)

        combined_summary = (
            "## Summary Highlights\n\n"
            "### Node Level Overview\n"
            + node_summary
            + "\n\n### Namespace/Pod Overview\n"
            + pod_summary
            + "\n"
        )
        ti.xcom_push(key="combined_summary_md", value=combined_summary)
        return combined_summary

    t_extract_summary = PythonOperator(task_id="extract_and_combine_summary", python_callable=extract_and_combine_summary, provide_context=True)

    # Final compile (this uses compile_sre_report to push sre_full_report)
    t_compile_sre_report = PythonOperator(task_id="compile_sre_report", python_callable=compile_sre_report, provide_context=True)

    # Convert to HTML -> send email
    t_convert_to_html = PythonOperator(task_id="convert_to_html", python_callable=convert_to_html, provide_context=True)
    t_send_email = PythonOperator(task_id="send_email_report", python_callable=send_email_report, provide_context=True)

    # -----------------------
    # DAG wiring
    # -----------------------
    [t_node_cpu_today, t_node_memory_today, t_node_disk_today, t_pod_cpu_today, t_pod_memory_today]
    # yesterday tasks are independent and will run in parallel

    # Wire comparisons
    [t_node_cpu_today, t_node_cpu_yesterday] >> t_node_cpu_compare
    [t_node_memory_today, t_node_memory_yesterday] >> t_node_memory_compare
    [t_node_disk_today, t_node_disk_yesterday] >> t_node_disk_compare
    [t_pod_cpu_today, t_pod_cpu_yesterday] >> t_pod_cpu_compare
    [t_pod_memory_today, t_pod_memory_yesterday] >> t_pod_memory_compare

    # After comparisons -> compile node report
    [t_node_cpu_compare, t_node_memory_compare, t_node_disk_compare] >> t_compile_node_report

    # Namespace compile -> combine reports
    ns_markdown >> t_combine_reports
    t_compile_node_report >> t_combine_reports

    # Combine -> extract summary -> compile final -> html -> email
    t_combine_reports >> t_extract_summary >> t_compile_sre_report >> t_convert_to_html >> t_send_email
