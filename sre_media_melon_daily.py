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
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import json
import re
import html
from jinja2 import Template
import requests
import markdown
import smtplib


# Configure detailed logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

default_args = {
    "owner": "sre_team",
    "depends_on_past": False,
    "start_date": datetime(2025, 11, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

SMTP_HOST = Variable.get("ltai.v3.mediamelon.smtp.host")
SMTP_PORT = int(Variable.get("ltai.v3.mediamelon.smtp.port"))
SMTP_USER = Variable.get("ltai.v3.mediamelon.smtp.user")
SMTP_PASSWORD = Variable.get("ltai.v3.mediamelon.smtp.password")
SMTP_SUFFIX = Variable.get("ltai.v3.mediamelon.smtp.suffix")
MEDIAMELON_FROM_ADDRESS = Variable.get("MEDIAMELON_FROM_ADDRESS")
MEDIAMELON_TO_ADDRESS = Variable.get("MEDIAMELON_TO_ADDRESS")

OLLAMA_HOST = Variable.get("MEDIAMELON_OLLAMA_HOST", "http://agentomatic:8000/")

YESTERDAY = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
TODAY = datetime.utcnow().strftime('%Y-%m-%d')

def authenticate_gmail():
    try:
        creds = Credentials.from_authorized_user_info(json.loads(Variable.get("MEDIAMELON_GMAIL_CREDENTIALS", {})))
        service = build("gmail", "v1", credentials=creds)
        profile = service.users().getProfile(userId="me").execute()
        logged_in_email = profile.get("emailAddress", "")
        sender_email = Variable.get("MEDIAMELON_FROM_ADDRESS", "")
        if logged_in_email.lower() != sender_email.lower():
            raise ValueError(f"Wrong Gmail account! Expected {sender_email}, but got {logged_in_email}")
        logging.info(f"Authenticated Gmail account: {logged_in_email}")
        return service
    except Exception as e:
        logging.error(f"Failed to authenticate Gmail: {str(e)}")
        return None

def get_ai_response(prompt, conversation_history=None):
    """Get AI response with conversation history context"""
    try:
        logging.debug(f"Query received: {prompt}")
        if not prompt or not isinstance(prompt, str):
            return "Invalid input provided. Please enter a valid query."
        client = Client(host=OLLAMA_HOST, headers={'x-ltai-client': 'media_melon-agent'})
        logging.debug(f"Connecting to Ollama at {OLLAMA_HOST} with model 'media_melon:0.4'")
        messages = []
        if conversation_history:
            for history_item in conversation_history:
                messages.append({"role": "user", "content": history_item["prompt"]})
                messages.append({"role": "assistant", "content": history_item["response"]})
        messages.append({"role": "user", "content": prompt})
        response = client.chat(
            model='appz/sre/media_melon:0.4',
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


# === t1: Node CPU – Last 24h ===
def node_cpu_today(ti, **context):
    prompt = """
    Generate the **node level cpu utilisation for the last 24 hours**
    """
    response = get_ai_response(prompt)
    ti.xcom_push(key="node_cpu_today", value=response)
    return response

def node_cpu_yesterday(ti, **context):
    prompt = """
    Generate the **node level cpu utilisation for yesterday**
    """
    response = get_ai_response(prompt)
    ti.xcom_push(key="node_cpu_yesterday", value=response)
    return response


# === t2: Node Memory – Last 24h ===
def node_memory_today(ti, **context):
    prompt = """
    Generate the **node level memory utilisation for last 24 hours**
    """
    response = get_ai_response(prompt)
    ti.xcom_push(key="node_memory_today", value=response)
    return response

def node_memory_yesterday(ti, **context):
    prompt = """
    Generate the **node level memory utilisation for last 24 hours**
    """
    response = get_ai_response(prompt)
    ti.xcom_push(key="node_memory_yesterday", value=response)
    return response


# === t3: Node Disk – Last 24h ===
def node_disk_today(ti, **context):
    prompt = """
    Generate the **node level disk utilisation for last 24 hours**
    """
    response = get_ai_response(prompt)
    ti.xcom_push(key="node_disk_today", value=response)
    return response

def node_disk_yesterday(ti, **context):
    prompt = """
    Generate the **node level disk utilisation for last 24 hours**
    """
    response = get_ai_response(prompt)
    ti.xcom_push(key="node_disk_yesterday", value=response)
    return response

# === t4: Pod CPU – Last 24h ===
def pod_cpu_today(ti, **context):
    prompt = """
    Generate the **pod level cpu utilisation for last 24 hours**
    """
    response = get_ai_response(prompt)
    ti.xcom_push(key="pod_cpu_today", value=response)
    return response

def pod_cpu_yesterday(ti, **context):
    prompt = """
    Generate the **pod level cpu utilisation for last 24 hours**
    """
    response = get_ai_response(prompt)
    ti.xcom_push(key="pod_cpu_yesterday", value=response)
    return response


# === t5: Pod Memory – Last 24h ===
def pod_memory_today(ti, **context):
    prompt = """
    Generate the **pod level memory utilisation for last 24 hours**
    """
    response = get_ai_response(prompt)
    ti.xcom_push(key="pod_memory_today", value=response)
    return response

def pod_memory_yesterday(ti, **context):
    prompt = """
    Generate the **pod level memory utilisation for last 24 hours**
    """
    response = get_ai_response(prompt)
    ti.xcom_push(key="pod_memory_yesterday", value=response)
    return response


# === Comparisons ===
def node_cpu_today_vs_yesterday(ti, **kwargs):
    node_cpu_today = ti.xcom_pull(task_ids="node_cpu_today")
    node_cpu_yesterday = ti.xcom_pull(task_ids="node_cpu_yesterday")
    if not node_cpu_today or not node_cpu_yesterday:
        logging.error("Missing XCom data from previous tasks.")
        return "Comparison failed: Missing data."
    prompt = f"""
Compare today's and yesterday's node CPU.
"""
    response = get_ai_response(prompt)
    ti.xcom_push(key="node_cpu_today_vs_yesterday", value=response)
    return response

def node_memory_today_vs_yesterday(ti, **kwargs):
    node_memory_today = ti.xcom_pull(task_ids="node_memory_today")
    node_memory_yesterday = ti.xcom_pull(task_ids="node_memory_yesterday")
    if not node_memory_today or not node_memory_yesterday:
        logging.error("Missing XCom data from previous tasks.")
        return "Comparison failed: Missing data."
    prompt = f"""
Compare today's and yesterday's node memory.
"""
    response = get_ai_response(prompt)
    ti.xcom_push(key="node_memory_today_vs_yesterday", value=response)
    return response

def node_disk_today_vs_yesterday(ti, **kwargs):
    node_disk_today = ti.xcom_pull(task_ids="node_disk_today")
    node_disk_yesterday = ti.xcom_pull(task_ids="node_disk_yesterday")
    if not node_disk_today or not node_disk_yesterday:
        logging.error("Missing XCom data from previous tasks.")
        return "Comparison failed: Missing data."
    prompt = f"""
Compare today's and yesterday's node disk usage.
"""
    response = get_ai_response(prompt)
    ti.xcom_push(key="node_disk_today_vs_yesterday", value=response)
    return response


# === DAG ===
with DAG(
    dag_id="sre_mediamelon_sre_report_daily_v1",
    default_args=default_args,
    schedule_interval="30 5 * * *",
    catchup=False,
    tags=["sre", "mediamelon", "daily", "11am-ist"],
    max_active_runs=1,
) as dag:

    # === Namespace Section with Dynamic Task Mapping ===
    @task
    def list_namespaces():
        """Get list of namespaces from Prometheus"""
        namespace_prompt = """
Run this Prometheus query:
count by (namespace) (kube_pod_info)
Return as a clean JSON array.
output_format:["namespace1", "namespace2", .....].
"""
        response = get_ai_response(namespace_prompt)
        logging.info(f"Full message content from agent: {response}")
        
        try:
            match = re.search(r'\[.*?\]', response, re.DOTALL)
            namespaces = json.loads(match.group(0)) if match else []
            logging.info(f"Parsed namespaces: {namespaces}")
            return namespaces
        except Exception as e:
            logging.error(f"Failed to parse namespace list: {e}")
            return []

    @task
    def process_namespace(ns: str):
        """Process namespace with peak metrics + limits + disk"""
        logging.info(f"Processing namespace: {ns}")

        cpu_peak_prompt = f"""
Get the **peak CPU usage for every pod** in namespace `{ns}` over the last 24 hours.
Return each entry as:
pod_name, peak_cpu_cores, timestamp.
Format clean JSON.
"""

        mem_peak_prompt = f"""
Get the **peak memory usage for every pod** in namespace `{ns}` over the last 24 hours.
Return:
pod_name, peak_memory_gb, timestamp.
Format clean JSON.
"""

        try:
            cpu_response = get_ai_response(cpu_peak_prompt)
            memory_response = get_ai_response(mem_peak_prompt)

            return {
                "namespace": ns,
                "cpu_peak": cpu_response,
                "memory_peak": memory_response,
                "status": "success"
            }

        except Exception as e:
            logging.error(f"Error processing namespace {ns}: {e}")
            return {
                "namespace": ns,
                "error": str(e),
                "status": "failed"
            }

    @task
    def collect_namespace_results(namespace_results: list):
        """Aggregate results from all namespace processing tasks"""
        logging.info(f"Collecting results from {len(namespace_results)} namespaces")
        
        aggregated = {}
        failed_namespaces = []
        
        for result in namespace_results:
            if not result:
                logging.warning("Skipping empty result")
                continue
                
            ns = result.get("namespace")
            if result.get("status") == "failed":
                failed_namespaces.append(ns)
                logging.warning(f"Namespace {ns} processing failed: {result.get('error')}")
            else:
                aggregated[ns] = result
        
        logging.info(f"Successfully processed {len(aggregated)} namespaces")
        if failed_namespaces:
            logging.warning(f"Failed namespaces: {failed_namespaces}")
        
        return {
            "total_namespaces": len(namespace_results),
            "successful": len(aggregated),
            "failed": len(failed_namespaces),
            "results": aggregated,
            "failed_namespaces": failed_namespaces
        }

    @task
    def compile_namespace_report(namespace_data):
        """Convert namespace data into markdown report"""
        logging.info("Compiling namespace metrics into markdown report")
        
        if not namespace_data or not namespace_data.get("results"):
            return "## Pod-Level Metrics by Namespace\n\nNo namespace data."

        md = []
        md.append("## Pod-Level Metrics by Namespace\n")
        md.append(f"**Total Namespaces Analyzed**: {namespace_data['successful']}/{namespace_data['total_namespaces']}\n")

        if namespace_data.get("failed_namespaces"):
            md.append(f"**Failed Namespaces**: {', '.join(namespace_data['failed_namespaces'])}\n")

        md.append("---\n")

        for ns, data in sorted(namespace_data["results"].items()):
            md.append(f"\n### Namespace: `{ns}`\n")
            md.append("\n#### Peak CPU Usage (Per Pod)\n")
            md.append(data["cpu_peak"])
            md.append("\n\n#### Peak Memory Usage (Per Pod)\n")
            md.append(data["memory_peak"])
            md.append("\n---\n")

        return "\n".join(md)


    # ---------------- NODE REPORT SECTION ----------------

    @task
    def compile_node_report(ti=None):
        cpu = ti.xcom_pull(task_ids="node_cpu_compare")
        mem = ti.xcom_pull(task_ids="node_memory_compare")
        disk = ti.xcom_pull(task_ids="node_disk_compare")

        md = []
        md.append("## Node-Level Metrics Summary\n")

        if cpu:
            md.append("### Node CPU Today vs Yesterday\n")
            md.append(cpu + "\n")
        if mem:
            md.append("### Node Memory Today vs Yesterday\n")
            md.append(mem + "\n")
        if disk:
            md.append("### Node Disk Today vs Yesterday\n")
            md.append(disk + "\n")

        return "\n".join(md)

    node_markdown = compile_node_report()

    # === UPDATED: Combine node + pod markdown before converting to HTML ===
    @task
    def combine_reports(node_md: str, pod_md: str):
        logging.info("Combining node-level and pod-level markdown reports")
        return f"# Mediamelon SRE Daily Report\n\n{node_md}\n\n---\n\n{pod_md}\n\n"

    combined_markdown = combine_reports(node_markdown, namespace_markdown)

    # === NEW SUMMARY SECTION ===
    @task
    def extract_and_combine_summary(node_md: str, pod_md: str):
        """Extract summary highlights from node and pod markdown reports"""
        logging.info("Extracting combined summary from all reports")

        def summarize(text):
            try:
                prompt = (
                    "From the following pod-level report, extract ONLY a clean 5-bullet summary "
                    "highlighting: CPU trends, Memory trends, anomalies, highest usage namespaces, and actions needed. "
                    "Do NOT include code, raw metrics, or long pod names.\n"
                    + text[:5000]
                )
                summary = get_ai_response(prompt)
                return summary.strip()
            except Exception as e:
                logging.error(f"Summary extraction failed: {e}")
                return "Summary unavailable."

        node_summary = summarize(node_md)
        pod_summary = summarize(pod_md)

        combined_summary = f""" 
        Node Level Overview
        {node_summary}
        """

        f"""
        Pod Namespace-Level Overview
        {pod_summary}
        """

        logging.info("Summary successfully generated")
        return combined_summary

    summary_section = extract_and_combine_summary(node_markdown, namespace_markdown)
    # === END SUMMARY SECTION ===

    # === Markdown to HTML Conversion ===
    def preprocess_markdown(markdown_text):
        """Clean and standardize Markdown before conversion to HTML"""
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

    @task
    def convert_to_html(markdown_report: str, summary_md: str):
        """Convert Markdown report to HTML with summary included"""
        logging.info(f"Converting markdown + summary to HTML, length: {len(markdown_report)}")
        full_md = f"{summary_md}\n\n---\n\n{markdown_report}"
        # Preprocess markdown
        markdown_text = preprocess_markdown(full_md)
        html_body = None

        # Try python-markdown (best support for tables)
        try:
            html_body = markdown.markdown(
                markdown_text,
                extensions=[
                    'tables',
                    'fenced_code',
                    'nl2br',
                    'sane_lists',
                    'attr_list'
                ]
            )
            logging.info("Used 'markdown' library for conversion")
        except Exception as e:
            logging.error(f"Markdown conversion error: {e}")
            # Fallback to escaped text
            html_body = f"<pre>{html.escape(markdown_report)}</pre>"

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
<p><strong>Generated:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
{html_body}
<hr>
<p style="text-align: center; color: #666; font-size: 12px;">
<em>Generated by Mediamelon SRE Agent</em>
</p>
</div>
</body>
</html>"""

        logging.info(f"HTML generated, length: {len(full_html)}")
        return full_html


    # === Email ===
    @task
    def send_email_report(html_report: str):
        """Send the HTML report via email"""
        try:
            if not html_report or "<html" not in html_report.lower():
                logging.error("No valid HTML report found")
                raise ValueError("HTML report missing or invalid")

            # Clean up any code block wrappers
            html_body = re.sub(r'```html\s*|```', '', html_report).strip()

            # Initialize SMTP
            logging.info(f"Connecting to SMTP server {SMTP_HOST}:{SMTP_PORT}")
            server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=10)
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)

            # Prepare email message
            sender = Variable.get("MEDIAMELON_FROM_ADDRESS", "")
            recipients = Variable.get("MEDIAMELON_TO_ADDRESS", "apnair@ecloudcontrol.com").split(",")
            subject = f"Mediamelon SRE Daily Report - {datetime.now().strftime('%Y-%m-%d')}"

            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = f"Mediamelon SRE Reports {SMTP_SUFFIX}"
            msg["To"] = ", ".join(recipients)

            # Attach HTML body
            msg.attach(MIMEText(html_body, "html"))

            # Send email
            server.sendmail(sender, recipients, msg.as_string())
            logging.info(f"Email sent successfully to: {recipients}")
            server.quit()
            return True

        except Exception as e:
            raise e


    # ---------------- DAG FLOW ----------------

    namespaces = list_namespaces()
    namespace_results = process_namespace.expand(ns=namespaces)
    namespace_data = collect_namespace_results(namespace_results)
    namespace_markdown = compile_namespace_report(namespace_data)

    # Node level tasks
    t1_today = PythonOperator(task_id="node_cpu_today", python_callable=node_cpu_today, provide_context=True)
    t1_yesterday = PythonOperator(task_id="node_cpu_yesterday", python_callable=node_cpu_yesterday, provide_context=True)
    t1_compare = PythonOperator(task_id="node_cpu_compare", python_callable=node_cpu_today_vs_yesterday, provide_context=True)
    [t1_today, t1_yesterday] >> t1_compare >> node_markdown

    t1_today_mem = PythonOperator(task_id="node_memory_today", python_callable=node_memory_today, provide_context=True)
    t1_yesterday_mem = PythonOperator(task_id="node_memory_yesterday", python_callable=node_memory_yesterday, provide_context=True)
    t1_compare_mem = PythonOperator(task_id="node_memory_compare", python_callable=node_memory_today_vs_yesterday, provide_context=True)
    [t1_today_mem, t1_yesterday_mem] >> t1_compare_mem >> node_markdown

    t1_today_disk = PythonOperator(task_id="node_disk_today", python_callable=node_disk_today, provide_context=True)
    t1_yesterday_disk = PythonOperator(task_id="node_disk_yesterday", python_callable=node_disk_yesterday, provide_context=True)
    t1_compare_disk = PythonOperator(task_id="node_disk_compare", python_callable=node_disk_today_vs_yesterday, provide_context=True)
    [t1_today_disk, t1_yesterday_disk] >> t1_compare_disk >> node_markdown

    combined_markdown = combine_reports(node_markdown, namespace_markdown)

    summary_section = extract_and_combine_summary(node_markdown, namespace_markdown)

    html_report = convert_to_html(combined_markdown, summary_section)

    send_email_report(html_report)