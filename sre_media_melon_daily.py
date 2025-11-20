import logging
import json
import re
import html
import traceback
import base64
from ollama import Client
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.application import MIMEApplication
import smtplib
import markdown
import os

# Configure detailed logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("sre_mediamelon_dag")

# -------------------------
# Default DAG arguments
# -------------------------
default_args = {
    "owner": "sre_team",
    "depends_on_past": False,
    "start_date": datetime(2025, 11, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# -------------------------
# SMTP + Mail configuration from Airflow Variables (defensive defaults)
# -------------------------
SMTP_HOST = Variable.get("ltai.v3.mediamelon.smtp.host", "mail.authsmtp.com")
SMTP_PORT = int(Variable.get("ltai.v3.mediamelon.smtp.port", "587"))
SMTP_USER = Variable.get("ltai.v3.mediamelon.smtp.user", "")
SMTP_PASSWORD = Variable.get("ltai.v3.mediamelon.smtp.password", "")
SMTP_SUFFIX = Variable.get("ltai.v3.mediamelon.smtp.suffix", "<noreply@mediamelon.com>")

# From/To - MediaMelon
MEDIAMELON_FROM_ADDRESS = Variable.get("ltai.v3.mediamelon.mediamelon_from_address", SMTP_USER or "noreply@mediamelon.com")
MEDIAMELON_TO_ADDRESS = Variable.get("ltai.v3.mediamelon.mediamelon_to_address", MEDIAMELON_FROM_ADDRESS)

# Ollama Host for MediaMelon agent (fallback to local address)
OLLAMA_HOST = Variable.get("MEDIAMELON_OLLAMA_HOST", default_var="http://agentomatic:8000/")

# Gmail credentials variable name (optional)
GMAIL_CREDENTIALS_VAR = "ltai.v3.mediamelon.mediamelon_gmail_credentials"

# Date strings used in prompts
YESTERDAY = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
TODAY = datetime.utcnow().strftime("%Y-%m-%d")


# ------------------------------
# Helper functions (pure python)
# ------------------------------
def safe_parse_json(json_text):
    """Safely parse JSON text that may contain surrounding text; return Python object or None."""
    if not json_text or not isinstance(json_text, str):
        return None
    try:
        return json.loads(json_text)
    except Exception:
        try:
            # attempt to extract first JSON blob
            match = re.search(r"(\[.*?\]|\{.*?\})", json_text, re.DOTALL)
            if match:
                return json.loads(match.group(0))
        except Exception:
            logger.debug("safe_parse_json extraction failed: %s", traceback.format_exc())
    return None


def json_list_to_markdown_table(json_list, column_defs):
    """
    Convert a list of dicts to a markdown table.
    column_defs: list of tuples (key_in_dict, header_name, format_fn_or_None)
    """
    if not isinstance(json_list, list) or not json_list:
        return None
    headers = [h for (_, h, _) in column_defs]
    # create header and separator with a single space around pipes for consistent markdown
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
                if fmt and callable(fmt) and val not in (None, ""):
                    val = fmt(val)
            except Exception:
                val = item.get(key, "")
            # ensure no newlines in cells
            cell = str(val).replace("\n", " ").strip()
            row_cells.append(cell)
        rows.append("| " + " | ".join(row_cells) + " |")
    # Ensure there is a blank line before and after the table for most markdown parsers
    return "\n" + "\n".join(rows) + "\n"


def try_convert_peak_cpu_to_markdown(text):
    parsed = safe_parse_json(text)
    if isinstance(parsed, list):
        column_defs = [
            ("pod_name", "Pod Name", None),
            ("peak_cpu_cores", "Peak CPU (cores)", lambda v: f"{float(v):.3f}" if v is not None and v != "" else ""),
            ("timestamp", "Timestamp", None),
        ]
        md = json_list_to_markdown_table(parsed, column_defs)
        if md:
            return md
    return text


def try_convert_peak_memory_to_markdown(text):
    parsed = safe_parse_json(text)
    if isinstance(parsed, list):
        column_defs = [
            ("pod_name", "Pod Name", None),
            ("peak_memory_gb", "Peak Memory (GB)", lambda v: f"{float(v):.3f}" if v is not None and v != "" else ""),
            ("timestamp", "Timestamp", None),
        ]
        md = json_list_to_markdown_table(parsed, column_defs)
        if md:
            return md
    return text


def convert_cpu_usage_to_md(text):
    parsed = safe_parse_json(text)
    if isinstance(parsed, list):
        column_defs = [
            ("pod", "Pod", None),
            ("cpu_usage", "CPU Used (cores)", lambda v: f"{float(v):.3f}" if v not in (None, "") else ""),
            ("cpu_limit", "CPU Limit (cores)", lambda v: f"{float(v):.3f}" if v not in (None, "") else ""),
        ]
        md = json_list_to_markdown_table(parsed, column_defs)
        if md:
            return md
    return text


def convert_memory_usage_to_md(text):
    parsed = safe_parse_json(text)
    if isinstance(parsed, list):
        column_defs = [
            ("pod", "Pod", None),
            ("memory_gb", "Memory Used (GB)", lambda v: f"{float(v):.3f}" if v not in (None, "") else ""),
            ("memory_limit_gb", "Memory Limit (GB)", lambda v: f"{float(v):.3f}" if v not in (None, "") else ""),
        ]
        md = json_list_to_markdown_table(parsed, column_defs)
        if md:
            return md
    return text


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


def authenticate_gmail():
    """
    Use stored credentials in Airflow Variable MEDIAMELON_GMAIL_CREDENTIALS to return a Gmail API service.
    Returns None on any failure or if google libs are missing.
    """
    try:
        creds_json = Variable.get(GMAIL_CREDENTIALS_VAR, default_var=None)
        if not creds_json:
            logger.info("Gmail credentials not found in Airflow variables.")
            return None
        creds_info = json.loads(creds_json)
        if Credentials is None or build is None:
            logger.warning("google oauth libraries not installed in environment.")
            return None
        creds = Credentials.from_authorized_user_info(creds_info)
        service = build("gmail", "v1", credentials=creds)
        profile = service.users().getProfile(userId="me").execute()
        logged_in_email = profile.get("emailAddress", "")
        return service
    except Exception:
        logger.error("Failed to authenticate Gmail: %s", traceback.format_exc())
        return None


# -------------------------------
# TaskFlow tasks for dynamic mapping
# -------------------------------
@task
def list_namespaces():
    """Get list of namespaces from Prometheus via AI prompt - returns JSON array of namespace strings"""
    namespace_prompt = (
        "Run this Prometheus query:\n"
        "count by (namespace) (kube_pod_info)\n\n"
        "Return as a clean JSON array of namespace strings, e.g.:\n"
        '["namespace1", "namespace2"]\n\n'
        "Only output the JSON array."
    )
    response = get_ai_response(namespace_prompt)
    logger.info("Full message content from agent (namespaces): %s", (response[:500] + "...") if response else "None")
    try:
        match = re.search(r"\[.*?\]", response or "", re.DOTALL)
        namespaces = json.loads(match.group(0)) if match else []
        if not isinstance(namespaces, list):
            namespaces = []
        logger.info("Parsed namespaces: %s", namespaces)
        return namespaces
    except Exception:
        logger.error("Failed to parse namespace list: %s", traceback.format_exc())
        return []


@task
def process_namespace(ns: str):
    """
    Process a single namespace - get pod CPU/memory metrics and peaks using AI prompts.
    Returns a dict summarizing the namespace.
    """
    logger.info("Processing namespace: %s", ns)
    cpu_prompt = f"""
Get CPU usage for namespace {ns} for last 24 hours
"""
    memory_prompt = f"""
Get memory usage for namespace {ns}:
"""
    cpu_peak_prompt = f""" 
Get the peak CPU usage for every pod in namespace '{ns}' over the last 24 hours.
"""
    mem_peak_prompt = f"""
Get the peak memory usage for every pod in namespace '{ns}' over the last 24 hours.
"""
    try:
        cpu_response = get_ai_response(cpu_prompt)
        memory_response = get_ai_response(memory_prompt)
        cpu_peak_response = get_ai_response(cpu_peak_prompt)
        mem_peak_response = get_ai_response(mem_peak_prompt)

        # Convert responses to markdown tables when possible
        cpu_metrics_md = convert_cpu_usage_to_md(cpu_response) if cpu_response else ""
        memory_metrics_md = convert_memory_usage_to_md(memory_response) if memory_response else ""
        cpu_peak_md = try_convert_peak_cpu_to_markdown(cpu_peak_response) if cpu_peak_response else ""
        mem_peak_md = try_convert_peak_memory_to_markdown(mem_peak_response) if mem_peak_response else ""

        result = {
            "namespace": ns,
            # Keep both raw and converted outputs (converted preferred by report)
            "cpu_metrics": cpu_metrics_md or (cpu_response or ""),
            "cpu_raw": cpu_response or "",
            "cpu_peak_metrics": cpu_peak_md or (cpu_peak_response or ""),
            "cpu_peak_raw": cpu_peak_response or "",
            "memory_metrics": memory_metrics_md or (memory_response or ""),
            "memory_raw": memory_response or "",
            "memory_peak_metrics": mem_peak_md or (mem_peak_response or ""),
            "memory_peak_raw": mem_peak_response or "",
            "status": "success",
        }
        logger.info("Successfully processed namespace %s", ns)
        logger.info("namespace : %s result keys: %s", ns, list(result.keys()))
        return result
    except Exception:
        logger.exception("Error processing namespace %s", ns)
        return {
            "namespace": ns,
            "error": traceback.format_exc(),
            "status": "failed",
        }


@task
def collect_namespace_results(namespace_results: list):
    """Aggregate results from all namespace processing tasks"""
    namespace_results = namespace_results or []
    logger.info("Collecting results from %d namespaces", len(namespace_results))
    aggregated = {}
    failed_namespaces = []
    for result in namespace_results:
        if not result:
            logger.warning("Skipping empty result")
            continue
        ns = result.get("namespace")
        if result.get("status") == "failed":
            failed_namespaces.append(ns)
            logger.warning("Namespace %s processing failed: %s", ns, result.get("error"))
        else:
            aggregated[ns] = result
    logger.info("Successfully processed %d namespaces", len(aggregated))
    return {
        "total_namespaces": len(namespace_results),
        "successful": len(aggregated),
        "failed": len(failed_namespaces),
        "results": aggregated,
        "failed_namespaces": failed_namespaces,
    }


@task
def compile_namespace_report(namespace_data: dict):
    """Convert namespace data into markdown report and compute overall peak summary"""
    logger.info("Compiling namespace metrics into markdown report")
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

    results = namespace_data.get("results", {}) or {}
    # Sorted keys for consistent order
    for ns in sorted(results.keys()):
        data = results.get(ns, {})
        markdown_sections.append(f"\n### Namespace: `{ns}`\n")

        # CPU Metrics
        cpu_metrics_text = data.get("cpu_metrics")
        if cpu_metrics_text:
            markdown_sections.append(f"\n#### CPU Usage (24h)\n")
            markdown_sections.append(cpu_metrics_text)
            markdown_sections.append("\n")

        # Memory Metrics
        memory_metrics_text = data.get("memory_metrics")
        if memory_metrics_text:
            markdown_sections.append(f"\n#### Memory Usage (24h)\n")
            markdown_sections.append(memory_metrics_text)
            markdown_sections.append("\n")

        # CPU Peak
        cpu_peak_text = data.get("cpu_peak_metrics")
        if cpu_peak_text:
            markdown_sections.append(f"\n#### Peak CPU (per Pod, 24h)\n")
            markdown_sections.append(cpu_peak_text)
            markdown_sections.append("\n")

        # Memory Peak
        mem_peak_text = data.get("memory_peak_metrics")
        if mem_peak_text:
            markdown_sections.append(f"\n#### Peak Memory (per Pod, 24h)\n")
            markdown_sections.append(mem_peak_text)
            markdown_sections.append("\n")

    markdown_sections.append("---\n")
    report = "\n".join(markdown_sections)
    logger.info("Generated namespace markdown with %d sections", len(markdown_sections))
    return report


# -------------------------------
# Non-mapped helper functions used by PythonOperator (these are plain callables)
# -------------------------------
def node_cpu_today_callable(ti=None, **context):
    prompt = "Generate the **node level cpu utilisation for the last 24 hours**"
    response = get_ai_response(prompt)
    try:
        ti.xcom_push(key="node_cpu_today", value=response)
    except Exception:
        logger.debug("ti.xcom_push not available (maybe not provided).")
    return response


def node_cpu_yesterday_callable(ti=None, **context):
    prompt = "Generate the **node level cpu utilisation for yesterday**"
    response = get_ai_response(prompt)
    try:
        ti.xcom_push(key="node_cpu_yesterday", value=response)
    except Exception:
        logger.debug("ti.xcom_push not available (maybe not provided).")
    return response


def node_memory_today_callable(ti=None, **context):
    prompt = "Generate the **node level memory utilisation for last 24 hours**"
    response = get_ai_response(prompt)
    try:
        ti.xcom_push(key="node_memory_today", value=response)
    except Exception:
        logger.debug("ti.xcom_push not available.")
    return response


def node_memory_yesterday_callable(ti=None, **context):
    prompt = "Generate the **node level memory utilisation for yesterday**"
    response = get_ai_response(prompt)
    try:
        ti.xcom_push(key="node_memory_yesterday", value=response)
    except Exception:
        logger.debug("ti.xcom_push not available.")
    return response


def node_disk_today_callable(ti=None, **context):
    prompt = "Generate the **node level disk utilisation for last 24 hours**"
    response = get_ai_response(prompt)
    try:
        ti.xcom_push(key="node_disk_today", value=response)
    except Exception:
        logger.debug("ti.xcom_push not available.")
    return response


def node_disk_yesterday_callable(ti=None, **context):
    prompt = "Generate the **node level disk utilisation for yesterday**"
    response = get_ai_response(prompt)
    try:
        ti.xcom_push(key="node_disk_yesterday", value=response)
    except Exception:
        logger.debug("ti.xcom_push not available.")
    return response


def pod_cpu_today_callable(ti=None, **context):
    prompt = "Generate the **pod level cpu utilisation for last 24 hours**"
    response = get_ai_response(prompt)
    try:
        ti.xcom_push(key="pod_cpu_today", value=response)
    except Exception:
        logger.debug("ti.xcom_push not available.")
    return response


def pod_cpu_yesterday_callable(ti=None, **context):
    prompt = "Generate the **pod level cpu utilisation for yesterday**"
    response = get_ai_response(prompt)
    try:
        ti.xcom_push(key="pod_cpu_yesterday", value=response)
    except Exception:
        logger.debug("ti.xcom_push not available.")
    return response


def pod_memory_today_callable(ti=None, **context):
    prompt = "Generate the **pod level memory utilisation for last 24 hours**"
    response = get_ai_response(prompt)
    try:
        ti.xcom_push(key="pod_memory_today", value=response)
    except Exception:
        logger.debug("ti.xcom_push not available.")
    return response


def pod_memory_yesterday_callable(ti=None, **context):
    prompt = "Generate the **pod level memory utilisation for yesterday**"
    response = get_ai_response(prompt)
    try:
        ti.xcom_push(key="pod_memory_yesterday", value=response)
    except Exception:
        logger.debug("ti.xcom_push not available.")
    return response


# Comparison callables
def node_cpu_compare_callable(ti=None, **context):
    node_cpu_today = ti.xcom_pull(key="node_cpu_today") if ti else None
    node_cpu_yesterday = ti.xcom_pull(key="node_cpu_yesterday") if ti else None
    if not node_cpu_today or not node_cpu_yesterday:
        logger.warning("Missing node CPU XComs for comparison")
    prompt = (
        "Compare today's and yesterday's node CPU.\n\n"
        "Today's:\n" + (node_cpu_today or "No data") + "\n\n"
        "Yesterday's:\n" + (node_cpu_yesterday or "No data") + "\n\n"
        "Produce a table comparing avg and max per node and list significant changes."
    )
    response = get_ai_response(prompt)
    try:
        ti.xcom_push(key="node_cpu_today_vs_yesterday", value=response)
    except Exception:
        logger.debug("ti.xcom_push not available.")
    return response


def node_memory_compare_callable(ti=None, **context):
    node_mem_today = ti.xcom_pull(key="node_memory_today") if ti else None
    node_mem_yesterday = ti.xcom_pull(key="node_memory_yesterday") if ti else None
    prompt = (
        "Compare today's and yesterday's node memory.\n\n"
        "Today's:\n" + (node_mem_today or "No data") + "\n\n"
        "Yesterday's:\n" + (node_mem_yesterday or "No data")
    )
    response = get_ai_response(prompt)
    try:
        ti.xcom_push(key="node_memory_today_vs_yesterday", value=response)
    except Exception:
        logger.debug("ti.xcom_push not available.")
    return response


def node_disk_compare_callable(ti=None, **context):
    node_disk_today = ti.xcom_pull(key="node_disk_today") if ti else None
    node_disk_yesterday = ti.xcom_pull(key="node_disk_yesterday") if ti else None
    prompt = (
        "Compare today's and yesterday's node disk usage.\n\n"
        "Today's:\n" + (node_disk_today or "No data") + "\n\n"
        "Yesterday's:\n" + (node_disk_yesterday or "No data")
    )
    response = get_ai_response(prompt)
    try:
        ti.xcom_push(key="node_disk_today_vs_yesterday", value=response)
    except Exception:
        logger.debug("ti.xcom_push not available.")
    return response


def pod_cpu_compare_callable(ti=None, **context):
    pod_cpu_today_val = ti.xcom_pull(key="pod_cpu_today") if ti else None
    pod_cpu_yesterday_val = ti.xcom_pull(key="pod_cpu_yesterday") if ti else None
    prompt = (
        "Compare today's and yesterday's pod CPU.\n\n"
        "Today's:\n" + (pod_cpu_today_val or "No data") + "\n\n"
        "Yesterday's:\n" + (pod_cpu_yesterday_val or "No data")
    )
    response = get_ai_response(prompt)
    try:
        ti.xcom_push(key="pod_cpu_today_vs_yesterday", value=response)
    except Exception:
        logger.debug("ti.xcom_push not available.")
    return response


def pod_memory_compare_callable(ti=None, **context):
    pod_mem_today_val = ti.xcom_pull(key="pod_memory_today") if ti else None
    pod_mem_yesterday_val = ti.xcom_pull(key="pod_memory_yesterday") if ti else None
    prompt = (
        "Compare today's and yesterday's pod memory.\n\n"
        "Today's:\n" + (pod_mem_today_val or "No data") + "\n\n"
        "Yesterday's:\n" + (pod_mem_yesterday_val or "No data")
    )
    response = get_ai_response(prompt)
    try:
        ti.xcom_push(key="pod_memory_today_vs_yesterday", value=response)
    except Exception:
        logger.debug("ti.xcom_push not available.")
    return response


# Final compile SRE report -> pushes sre_full_report
def compile_sre_report_callable(ti=None, **context):
    try:
        logger.info("Compiling final SRE markdown report for Mediamelon")
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
            "## 1. Node-Level Metrics (Last 24h)\n\n"
            f"{node_cpu}\n\n"
            "---\n\n"
            f"{node_memory}\n\n"
            "---\n\n"
            f"{node_disk}\n\n"
            "---\n\n"
            "## 2. Pod-Level Metrics (Last 24h)\n\n"
            f"{pod_cpu}\n\n"
            "---\n\n"
            f"{pod_memory}\n\n"
            "---\n\n"
            "## 6. Namespace-Level Pod Peaks\n\n"
            f"{namespace_md}\n\n"
            "---\n\n"
            "## 7. Node-Level Metrics (Today vs Yesterday)\n\n"
            f"{node_cpu_cmp}\n\n"
            "---\n\n"
            f"{node_mem_cmp}\n\n"
            "---\n\n"
            f"{node_disk_cmp}\n\n"
            "---\n\n"
            "## 8. Pod-Level Metrics (Today vs Yesterday)\n\n"
            f"{pod_cpu_cmp}\n\n"
            "---\n\n"
            f"{pod_mem_cmp}\n\n"
            "---\n\n"
            "## 10. Overall Summary\n\n"
            "(See consolidated summary)\n\n"
            "---\n"
        )

        report = re.sub(r"\n{3,}", "\n\n", report.strip())
        try:
            ti.xcom_push(key="sre_full_report", value=report)
        except Exception:
            logger.debug("ti.xcom_push not available (maybe not provided).")
        logger.info("SRE report compiled successfully and pushed to XCom (sre_full_report).")
        return report
    except Exception:
        logger.exception("Failed to compile SRE report")
        raise


def overall_summary_callable(ti=None, **context):
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

        namespace_md = ti.xcom_pull(task_ids="compile_namespace_report") or "No namespace report generated."

        prompt = (
    "Write a brief HTML-formatted email summary for the Mediamelon SRE daily report.\n\n"
    "Begin with:\n"
    "\"Hi Mediamelon Team,<br><br>Here is the overall summary for today's SRE report.\"<br><br>"

    "### INSTRUCTIONS\n"
    "1. The summary MUST be short (8-12 sentences total).\n"
    "2. DO NOT repeat long tables, raw metrics, pod names, or namespace lists.\n"
    "3. Summarize only the key trends and critical issues.\n"
    "4. Use the following HTML highlighting:\n"
    "   - <span style='color:red;font-weight:bold'>CRITICAL</span> for high-risk issues\n"
    "   - <span style='color:#d17d00;font-weight:bold'>IMPORTANT</span> for notable issues\n\n"

    "### EMAIL STRUCTURE\n"
    "<h2>Node-Level Summary</h2>\n"
    "- Summarize CPU, Memory, and Disk trends.\n"
    "- Highlight risks using the HTML tags above.\n\n"

    "<h2>Pod-Level Summary</h2>\n"
    "- Summarize cross-namespace pod behavior.\n"
    "- Identify high-risk pods using:\n"
    "  <span style='color:red;font-weight:bold'>HIGH-RISK POD</span>\n"
    "- Identify warning pods using:\n"
    "  <span style='#d17d00;font-weight:bold'>WARNING</span>\n"
    "- Keep this section short and general.\n\n"

    "### DATA CONTEXT (DO NOT REPEAT RAW DATA IN OUTPUT — ONLY USE FOR ANALYSIS)\n"
    f"Node CPU (short excerpt): { (node_cpu or '')[:600] }\n\n"
    f"Node Memory (short excerpt): { (node_memory or '')[:600] }\n\n"
    f"Node Disk (short excerpt): { (node_disk or '')[:600] }\n\n"
    f"Pod CPU (short excerpt): { (pod_cpu or '')[:600] }\n\n"
    f"Pod Memory (short excerpt): { (pod_memory or '')[:600] }\n\n"
    f"Namespace Peak Metrics (short excerpt): { (namespace_md or '')[:600] }\n\n"

    "### FINAL REQUIREMENTS\n"
    "- Output MUST be valid HTML.\n"
    "- No markdown.\n"
    "- No code blocks.\n"
    "- Do not include long data or tables.\n"

)
        response = get_ai_response(prompt)
        try:
            ti.xcom_push(key="overall_summary", value=response)
        except Exception:
            logger.debug("ti.xcom_push not available.")
        logger.info("Overall summary generated and pushed to XCom.")
        return response
    except Exception:
        logger.exception("Failed to generate overall summary")
        raise


# -------------------------------
# Markdown -> HTML conversion
# -------------------------------
def preprocess_markdown(markdown_text):
    """Clean and standardize Markdown before conversion to HTML."""
    if not markdown_text:
        return ""
    markdown_text = markdown_text.lstrip("\ufeff\u200b\u200c\u200d")
    markdown_text = re.sub(r"^(#{1,6})\s*", r"\1 ", markdown_text, flags=re.MULTILINE)

    # Ensure blank lines before and after tables so markdown libs render them as proper tables
    lines = markdown_text.split("\n")
    processed = []
    in_table = False

    for line in lines:
        stripped = line.rstrip()
        if "|" in stripped and stripped.count("|") >= 2 and not stripped.startswith("```"):
            if not in_table and processed and processed[-1].strip():
                processed.append("")
            in_table = True
            processed.append(stripped)
        else:
            if in_table and stripped:
                processed.append("")
                in_table = False
            processed.append(stripped)
    out = "\n".join(processed)
    # collapse excessive blank lines
    out = re.sub(r"\n{3,}", "\n\n", out)
    return out


def convert_to_html_callable(ti=None, **context):
    """
    Convert the combined markdown SRE report into clean, responsive, Gmail-safe HTML.
    Returns HTML string.
    """
    try:
        markdown_report = ti.xcom_pull(key="sre_full_report") or "# No report generated."
        logger.info("Converting markdown report, length=%d", len(markdown_report))
        markdown_report = preprocess_markdown(markdown_report)

        html_body = None
        if markdown is not None:
            try:
                html_body = markdown.markdown(
                    markdown_report,
                    extensions=[
                        "tables",
                        "fenced_code",
                        "nl2br",
                        "sane_lists",
                        "attr_list",
                    ],
                )
                logger.info("Used 'markdown' library for conversion")
            except Exception:
                logger.warning("markdown lib error, trying fallback converters")

        if not html_body or len(html_body) < 50:
            try:
                import markdown2

                html_body = markdown2.markdown(markdown_report, extras=["tables", "fenced-code-blocks", "break-on-newline"])
                logger.info("Used markdown2 fallback for conversion")
            except Exception:
                logger.debug("markdown2 not available or failed")

        if not html_body or len(html_body) < 50:
            try:
                import mistune

                html_body = mistune.html(markdown_report)
                logger.info("Used mistune fallback for conversion")
            except Exception:
                logger.error("All markdown conversions failed; using preformatted fallback")
                html_body = "<pre>{}</pre>".format(html.escape(markdown_report))

        # Wrap in email-safe HTML
        full_html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Mediamelon SRE Daily Report</title>
<style>
body {{
    font-family: Arial, sans-serif;
    background-color: #f4f4f4;
    margin: 0;
    padding: 20px;
}}
.container {{
    max-width: 1200px;
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
    font-size: 28px;
    border-bottom: 3px solid #1a5fb4;
    padding-bottom: 8px;
    margin-bottom: 20px;
}}
h2 {{
    color: #1a5fb4;
    font-size: 22px;
    border-bottom: 2px solid #ccc;
    padding-bottom: 6px;
    margin-top: 30px;
}}
h3 {{
    color: #2d3748;
    font-size: 18px;
    margin-top: 20px;
}}
h4 {{
    color: #4a5568;
    font-size: 16px;
}}
strong {{
    color: #111;
}}
code {{
    background-color: #f6f8fa;
    padding: 2px 6px;
    border-radius: 3px;
    font-family: monospace;
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
.summary-box {{
    background-color: #e7f3ff;
    border-left: 4px solid #1a5fb4;
    padding: 15px;
    margin: 20px 0;
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
<h1>Mediamelon SRE Daily Report</h1>
<p><strong>Generated:</strong> {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
{html_body}
<hr>
<p style="text-align: center; color: #666; font-size: 12px;">
<em>Generated by Mediamelon SRE Agent</em>
</p>
</div>
</body>
</html>"""

        logger.info("HTML generated, length: %d", len(full_html))
        return full_html
    except Exception:
        logger.exception("convert_to_html failed")
        fallback = "<html><body><h1>Mediamelon SRE Daily Report</h1><pre>Conversion to HTML failed. Check scheduler logs for details.</pre></body></html>"
        return fallback


def send_email_report_callable(ti=None, **context):
    """Send the HTML report via SMTP, but with overall summary in body and full PDF attached"""
    try:
        # Pull overall summary from XCom
        overall_md = ti.xcom_pull(key="overall_summary") or "No overall summary generated."
        # Convert summary markdown to HTML for email body
        try:
            overall_html = markdown.markdown(preprocess_markdown(overall_md), extensions=["nl2br", "sane_lists"])
        except Exception:
            overall_html = "<pre>{}</pre>".format(html.escape(overall_md))

        # Pull PDF path from XCom pushed by generate_pdf task
        pdf_path = ti.xcom_pull(key="sre_pdf_path") or "/tmp/mediamelon_sre_report.pdf"

        # Build email
        sender = MEDIAMELON_FROM_ADDRESS or SMTP_USER or "noreply@mediamelon.com"
        recipients = [r.strip() for r in MEDIAMELON_TO_ADDRESS.split(",") if r.strip()]
        subject = f"Mediamelon SRE Daily Report - Summary - {datetime.utcnow().strftime('%Y-%m-%d')}"

        msg = MIMEMultipart("mixed")
        msg["Subject"] = subject
        msg["From"] = f"Mediamelon SRE Reports {SMTP_SUFFIX}"
        msg["To"] = ", ".join(recipients)

        # Attach the HTML summary as the email body (alternative for mail clients)
        alternative = MIMEMultipart("alternative")
        alternative.attach(MIMEText(overall_html, "html", "utf-8"))
        msg.attach(alternative)

        # Attach the PDF file
        if pdf_path and os.path.exists(pdf_path):
            with open(pdf_path, "rb") as f:
                part = MIMEApplication(f.read(), _subtype="pdf")
                part.add_header("Content-Disposition", "attachment", filename=os.path.basename(pdf_path))
                msg.attach(part)
            logger.info("Attached PDF: %s", pdf_path)
        else:
            logger.warning("PDF not found at path: %s. Email will be sent without PDF attachment.", pdf_path)

        # Send email via SMTP
        logger.info("Connecting to SMTP server %s:%d", SMTP_HOST, SMTP_PORT)
        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30)
        try:
            server.starttls()
        except Exception:
            logger.debug("starttls not supported or failed")

        if SMTP_USER and SMTP_PASSWORD:
            server.login(SMTP_USER, SMTP_PASSWORD)

        server.sendmail(sender, recipients, msg.as_string())
        logger.info("Email (summary + PDF) sent successfully to: %s", recipients)
        server.quit()
        return True

    except Exception:
        logger.exception("Failed to send email report (summary + PDF)")
        raise


# -------------------------------
# PDF generation (ReportLab) - styled with headings, paragraphs and real tables
# -------------------------------
def generate_pdf_report_callable(ti=None, **context):
    """
    Convert the sre_full_report (markdown) into a styled PDF using ReportLab.
    Produces /tmp/mediamelon_sre_report.pdf and pushes its path to XCom key 'sre_pdf_path'.
    """
    try:
        md = ti.xcom_pull(key="sre_full_report") or "# No report generated."
        md = preprocess_markdown(md)

        try:
            # Import reportlab inside function to avoid module import errors at file parse time
            from reportlab.lib.pagesizes import A4
            from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
            from reportlab.lib.enums import TA_LEFT, TA_CENTER
            from reportlab.lib import colors
            from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Preformatted
            logger.info("ReportLab imported successfully for PDF generation.")
        except ImportError:
            logger.exception("reportlab is not installed in the environment. Install reportlab to enable PDF generation.")
            raise

        # Output path
        out_path = "/tmp/mediamelon_sre_report.pdf"

        # Basic styles
        styles = getSampleStyleSheet()
        h1 = ParagraphStyle("Heading1", parent=styles["Heading1"], fontSize=20, leading=24, alignment=TA_CENTER, spaceAfter=12, textColor=colors.HexColor("#1a5fb4"))
        h2 = ParagraphStyle("Heading2", parent=styles["Heading2"], fontSize=14, leading=18, alignment=TA_LEFT, spaceAfter=8, textColor=colors.HexColor("#1a5fb4"))
        h3 = ParagraphStyle("Heading3", parent=styles["Heading3"], fontSize=12, leading=16, alignment=TA_LEFT, spaceAfter=6)
        body = ParagraphStyle("Body", parent=styles["BodyText"], fontSize=10, leading=14, alignment=TA_LEFT)
        pre_style = ParagraphStyle("Pre", parent=styles["Code"], fontName="Courier", fontSize=8, leading=12)

        # Convert markdown to flowables (rudimentary parser for headings, paragraphs, tables, fenced code)
        lines = md.splitlines()
        flowables = []

        # Title
        flowables.append(Paragraph("Mediamelon SRE Daily Report", h1))
        flowables.append(Paragraph(f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}", body))
        flowables.append(Spacer(1, 12))

        i = 0
        in_code_block = False
        code_block_lines = []
        while i < len(lines):
            line = lines[i].rstrip()

            # Fenced code block handling
            if line.strip().startswith("```"):
                if not in_code_block:
                    in_code_block = True
                    code_block_lines = []
                else:
                    # end code block
                    in_code_block = False
                    code_text = "\n".join(code_block_lines)
                    flowables.append(Preformatted(code_text, pre_style))
                    flowables.append(Spacer(1, 8))
                i += 1
                continue

            if in_code_block:
                code_block_lines.append(line)
                i += 1
                continue

            # Heading detection
            if line.startswith("# "):
                flowables.append(Paragraph(line.lstrip("# ").strip(), h2))
                i += 1
                continue
            if line.startswith("## "):
                flowables.append(Paragraph(line.lstrip("# ").strip(), h2))
                i += 1
                continue
            if line.startswith("### "):
                flowables.append(Paragraph(line.lstrip("# ").strip(), h3))
                i += 1
                continue

            # Table detection: current line has '|' and next line has --- (markdown table)
            if "|" in line and i + 1 < len(lines) and re.match(r"^\s*\|?\s*[-:]+\s*\|", lines[i+1]):
                # collect table lines starting at i while they contain '|'
                table_lines = []
                # header
                header_line = line
                separator_line = lines[i+1]
                i += 2
                while i <= len(lines) - 1 and "|" in lines[i]:
                    table_lines.append(lines[i])
                    i += 1
                # parse header and rows
                def split_row(r):
                    parts = [c.strip() for c in re.split(r"\|", r)]
                    # remove leading/trailing empty cells due to leading/trailing pipes
                    if parts and parts[0] == "":
                        parts = parts[1:]
                    if parts and parts[-1] == "":
                        parts = parts[:-1]
                    return parts

                header_cells = split_row(header_line)
                data_rows = [split_row(l) for l in table_lines]
                # Build table data with header as first row
                table_data = [header_cells] + data_rows

                # Create ReportLab Table
                t = Table(table_data, repeatRows=1)
                tbl_style = TableStyle([
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1a5fb4")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                    ("ALIGN", (0, 0), (-1, -1), "LEFT"),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    ("INNERGRID", (0, 0), (-1, -1), 0.25, colors.grey),
                    ("BOX", (0, 0), (-1, -1), 0.25, colors.grey),
                    ("FONTSIZE", (0, 0), (-1, -1), 8),
                    ("BOTTOMPADDING", (0, 0), (-1, 0), 6),
                    ("TOPPADDING", (0, 0), (-1, 0), 6),
                ])
                t.setStyle(tbl_style)
                flowables.append(t)
                flowables.append(Spacer(1, 8))
                continue

            # Horizontal rule
            if line.strip().startswith("---"):
                flowables.append(Spacer(1, 6))
                i += 1
                continue

            # Blank line -> spacer
            if not line.strip():
                flowables.append(Spacer(1, 6))
                i += 1
                continue

            # Normal paragraph
            flowables.append(Paragraph(line, body))
            i += 1

        # Build PDF
        doc = SimpleDocTemplate(out_path, pagesize=A4, rightMargin=30, leftMargin=30, topMargin=30, bottomMargin=30)
        doc.build(flowables)
        logger.info("PDF generated at: %s", out_path)

        # Push path to XCom for email task
        try:
            ti.xcom_push(key="sre_pdf_path", value=out_path)
        except Exception:
            logger.debug("ti.xcom_push not available (maybe not provided).")

        return out_path

    except Exception:
        logger.exception("Failed to generate PDF from markdown")
        raise


# -------------------------------
# DAG definition and wiring
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
    t_node_cpu_today = PythonOperator(task_id="node_cpu_today", python_callable=node_cpu_today_callable, provide_context=True)
    t_node_memory_today = PythonOperator(task_id="node_memory_today", python_callable=node_memory_today_callable, provide_context=True)
    t_node_disk_today = PythonOperator(task_id="node_disk_today", python_callable=node_disk_today_callable, provide_context=True)

    t_pod_cpu_today = PythonOperator(task_id="pod_cpu_today", python_callable=pod_cpu_today_callable, provide_context=True)
    t_pod_memory_today = PythonOperator(task_id="pod_memory_today", python_callable=pod_memory_today_callable, provide_context=True)

    # Yesterday tasks
    t_node_cpu_yesterday = PythonOperator(task_id="node_cpu_yesterday", python_callable=node_cpu_yesterday_callable, provide_context=True)
    t_node_memory_yesterday = PythonOperator(task_id="node_memory_yesterday", python_callable=node_memory_yesterday_callable, provide_context=True)
    t_node_disk_yesterday = PythonOperator(task_id="node_disk_yesterday", python_callable=node_disk_yesterday_callable, provide_context=True)

    t_pod_cpu_yesterday = PythonOperator(task_id="pod_cpu_yesterday", python_callable=pod_cpu_yesterday_callable, provide_context=True)
    t_pod_memory_yesterday = PythonOperator(task_id="pod_memory_yesterday", python_callable=pod_memory_yesterday_callable, provide_context=True)

    # Comparison tasks (depend on corresponding today/yesterday)
    t_node_cpu_compare = PythonOperator(task_id="node_cpu_compare", python_callable=node_cpu_compare_callable, provide_context=True)
    t_node_memory_compare = PythonOperator(task_id="node_memory_compare", python_callable=node_memory_compare_callable, provide_context=True)
    t_node_disk_compare = PythonOperator(task_id="node_disk_compare", python_callable=node_disk_compare_callable, provide_context=True)

    t_pod_cpu_compare = PythonOperator(task_id="pod_cpu_compare", python_callable=pod_cpu_compare_callable, provide_context=True)
    t_pod_memory_compare = PythonOperator(task_id="pod_memory_compare", python_callable=pod_memory_compare_callable, provide_context=True)

    # ---- Namespace dynamic mapping flow (TaskFlow decorated tasks) ----
    ns_list = list_namespaces()  # returns XComArg (list)
    ns_results = process_namespace.expand(ns=ns_list)  # mapped over namespaces (list)
    ns_aggregated = collect_namespace_results(ns_results)  # aggregate
    ns_markdown = compile_namespace_report(ns_aggregated)  # returns markdown string

    # Compile node report (uses comparison outputs)
    def compile_node_report_callable(ti=None, **context):
        node_cpu_cmp_val = ti.xcom_pull(key="node_cpu_today_vs_yesterday") or ""
        node_mem_cmp_val = ti.xcom_pull(key="node_memory_today_vs_yesterday") or ""
        node_disk_cmp_val = ti.xcom_pull(key="node_disk_today_vs_yesterday") or ""

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
        try:
            ti.xcom_push(key="node_markdown", value=report)
        except Exception:
            logger.debug("ti.xcom_push not available.")
        return report

    t_compile_node_report = PythonOperator(task_id="compile_node_report", python_callable=compile_node_report_callable, provide_context=True)

    # Combine node_markdown + namespace_markdown into final combined markdown
    def combine_reports_callable(ti=None, **context):
        node_md = ti.xcom_pull(key="node_markdown") or ""
        pod_md = ti.xcom_pull(task_ids="compile_namespace_report") or ""
        combined = f"# Mediamelon SRE Daily Report\n\n{node_md}\n\n---\n\n{pod_md}\n\n"
        try:
            ti.xcom_push(key="combined_markdown", value=combined)
        except Exception:
            logger.debug("ti.xcom_push not available.")
        return combined

    t_combine_reports = PythonOperator(task_id="combine_reports", python_callable=combine_reports_callable, provide_context=True)

    # Summary extraction
    def extract_and_combine_summary_callable(ti=None, **context):
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
                logger.error("Summary extraction failed: %s", traceback.format_exc())
                return "Summary unavailable."

        node_summary = summarize(node_md)
        pod_summary = summarize(pod_md)

        combined_summary = (
            "## Summary Highlights\n\n"
            "### Node Level Overview\n\n"
            + node_summary
            + "\n\n### Namespace/Pod Overview\n\n"
            + pod_summary
            + "\n"
        )
        try:
            ti.xcom_push(key="combined_summary_md", value=combined_summary)
        except Exception:
            logger.debug("ti.xcom_push not available.")
        return combined_summary

    t_extract_summary = PythonOperator(task_id="extract_and_combine_summary", python_callable=extract_and_combine_summary_callable, provide_context=True)

    # Final compile (uses compile_sre_report to push sre_full_report)
    t_compile_sre_report = PythonOperator(task_id="compile_sre_report", python_callable=compile_sre_report_callable, provide_context=True)

    # Convert to HTML -> send email
    t_convert_to_html = PythonOperator(task_id="convert_to_html", python_callable=convert_to_html_callable, provide_context=True)

    # PDF generation task (new)
    t_generate_pdf = PythonOperator(task_id="generate_pdf", python_callable=generate_pdf_report_callable, provide_context=True)

    # Overall summary task (new) - produces overall_summary XCom used for email body
    t_overall_summary = PythonOperator(task_id="overall_summary", python_callable=overall_summary_callable, provide_context=True)

    # Updated send email (attaches PDF, uses overall_summary in body)
    t_send_email = PythonOperator(task_id="send_email_report", python_callable=send_email_report_callable, provide_context=True)

    # -----------------------
    # DAG wiring (clean and inside DAG context)
    # -----------------------
    ns_list >> ns_results

    # Today tasks run in parallel
    [t_node_cpu_today, t_node_memory_today, t_node_disk_today, t_pod_cpu_today, t_pod_memory_today]

    # Yesterday tasks run in parallel
    [t_node_cpu_yesterday, t_node_memory_yesterday, t_node_disk_yesterday, t_pod_cpu_yesterday, t_pod_memory_yesterday]

    # Pair today's and yesterday's nodes/pods into comparison tasks
    [t_node_cpu_today, t_node_cpu_yesterday] >> t_node_cpu_compare
    [t_node_memory_today, t_node_memory_yesterday] >> t_node_memory_compare
    [t_node_disk_today, t_node_disk_yesterday] >> t_node_disk_compare

    [t_pod_cpu_today, t_pod_cpu_yesterday] >> t_pod_cpu_compare
    [t_pod_memory_today, t_pod_memory_yesterday] >> t_pod_memory_compare

    # Node compile depends on comparisons
    [t_node_cpu_compare, t_node_memory_compare, t_node_disk_compare] >> t_compile_node_report

    # Namespace mapping -> compile -> combine
    ns_markdown >> t_combine_reports
    t_compile_node_report >> t_combine_reports

    # Combine -> extract summary -> compile final -> overall summary -> generate PDF -> convert to HTML -> send email
    t_combine_reports >> t_extract_summary >> t_compile_sre_report >> t_overall_summary >> t_generate_pdf >> t_convert_to_html >> t_send_email
