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

# We'll treat weekly as rolling last 7 days from now
WEEK_FROM = (datetime.utcnow() - timedelta(days=7)).strftime('%Y-%m-%d')
WEEK_TO = datetime.utcnow().strftime('%Y-%m-%d')

def get_ai_response(prompt, conversation_history=None):
    """Get AI response with conversation history context"""
    try:
        logging.debug(f"Query received: {prompt[:500]}...")
        if not prompt or not isinstance(prompt, str):
            return "Invalid input provided. Please enter a valid query."
        client = Client(host=OLLAMA_HOST, headers={'x-ltai-client': 'media_melon-agent'})
        logging.debug(f"Connecting to Ollama at {OLLAMA_HOST} with model 'appz/sre/media_melon:0.4'")
        messages = []
        if conversation_history:
            for history_item in conversation_history:
                messages.append({"role": "user", "content": history_item.get("prompt", "")})
                messages.append({"role": "assistant", "content": history_item.get("response", "")})
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


# ------------------- Weekly Node & Pod compute functions -------------------

# Node-level weekly metrics
def node_cpu_week(ti, **context):
    prompt = f"""
Generate the **node level CPU utilisation for the last 7 days (rolling)**.
Return per-node daily averages and weekly peak in JSON format:
[{{"node":"node1","daily_avg":[...],"weekly_peak":val,"timestamp":"YYYY-MM-DDTHH:MM:SSZ"}}, ...]
Time window: {WEEK_FROM} to {WEEK_TO}
"""
    response = get_ai_response(prompt)
    ti.xcom_push(key="node_cpu_week", value=response)
    return response

def node_memory_week(ti, **context):
    prompt = f"""
Generate the **node level Memory utilisation for the last 7 days (rolling)**.
Return per-node daily averages and weekly peak in JSON format.
Time window: {WEEK_FROM} to {WEEK_TO}
"""
    response = get_ai_response(prompt)
    ti.xcom_push(key="node_memory_week", value=response)
    return response

def node_disk_week(ti, **context):
    prompt = f"""
Generate the **node level Disk utilisation for the last 7 days (rolling)**.
Return per-node daily averages and weekly peak in JSON format.
Time window: {WEEK_FROM} to {WEEK_TO}
"""
    response = get_ai_response(prompt)
    ti.xcom_push(key="node_disk_week", value=response)
    return response

# Pod-level weekly metrics
def pod_cpu_week(ti, **context):
    prompt = f"""
Generate the **pod level CPU utilisation for the last 7 days (rolling)**.
Return per-pod daily averages and weekly peak in JSON format (pod_name, daily_avg[], weekly_peak, timestamp).
Time window: {WEEK_FROM} to {WEEK_TO}
"""
    response = get_ai_response(prompt)
    ti.xcom_push(key="pod_cpu_week", value=response)
    return response

def pod_memory_week(ti, **context):
    prompt = f"""
Generate the pod level Memory utilisation for the last 7 days (rolling).
Return per-pod daily averages and weekly peak in JSON format.
Time window: {WEEK_FROM} to {WEEK_TO}
"""
    response = get_ai_response(prompt)
    ti.xcom_push(key="pod_memory_week", value=response)
    return response

# Weekly comparisons: this week vs last week
def node_cpu_week_vs_prev(ti, **context):
    this_week = ti.xcom_pull(task_ids="node_cpu_week")
    prompt = f"""
Compare Node CPU: THIS WEEK (rolling 7 days ending {WEEK_TO}) vs LAST WEEK (previous 7 days).
Provide:
- Top 5 nodes with highest week-over-week increase (with %)
- Top 5 nodes with highest decrease
- Short action bullets (3) for addressing high CPU nodes
Return concise JSON or bullet text.
"""
    response = get_ai_response(prompt)
    ti.xcom_push(key="node_cpu_week_vs_prev", value=response)
    return response

def node_memory_week_vs_prev(ti, **context):
    this_week = ti.xcom_pull(task_ids="node_memory_week")
    prompt = f"""
Compare Node Memory: THIS WEEK (ending {WEEK_TO}) vs LAST WEEK.
Provide: top increases, top decreases, anomalies, suggested actions in bullets.
"""
    response = get_ai_response(prompt)
    ti.xcom_push(key="node_memory_week_vs_prev", value=response)
    return response

def node_disk_week_vs_prev(ti, **context):
    this_week = ti.xcom_pull(task_ids="node_disk_week")
    prompt = f"""
Compare Node Disk: THIS WEEK (ending {WEEK_TO}) vs LAST WEEK.
Provide top changes and brief remediation recommendations.
"""
    response = get_ai_response(prompt)
    ti.xcom_push(key="node_disk_week_vs_prev", value=response)
    return response

# Pod comparisons
def pod_cpu_week_vs_prev(ti, **context):
    prompt = f"""
Compare Pod CPU usage: THIS WEEK (ending {WEEK_TO}) vs LAST WEEK.
Return top 10 pods with largest week-over-week changes and short notes.
"""
    response = get_ai_response(prompt)
    ti.xcom_push(key="pod_cpu_week_vs_prev", value=response)
    return response

def pod_memory_week_vs_prev(ti, **context):
    prompt = f"""
Compare Pod Memory usage: THIS WEEK (ending {WEEK_TO}) vs LAST WEEK.
Return top 10 pods with largest changes and suggested actions.
"""
    response = get_ai_response(prompt)
    ti.xcom_push(key="pod_memory_week_vs_prev", value=response)
    return response

# ------------------- Namespace dynamic mapping & processing -------------------

@task
def list_namespaces_weekly():
    """Get list of namespaces from Prometheus via AI extraction (clean JSON array)"""
    namespace_prompt = f"""
Run this Prometheus query:
count by (namespace) (kube_pod_info)
Return as a clean JSON array.
Time window: {WEEK_FROM} to {WEEK_TO}
Output format: ["namespace1","namespace2",...]
"""
    response = get_ai_response(namespace_prompt)
    logging.info(f"Full message content from agent (namespaces): {response}")
    try:
        match = re.search(r'\[.*?\]', response, re.DOTALL)
        namespaces = json.loads(match.group(0)) if match else []
        logging.info(f"Parsed namespaces: {namespaces}")
        return namespaces
    except Exception as e:
        logging.error(f"Failed to parse namespace list: {e}")
        return []

@task
def process_namespace_week(ns: str):
    """Process namespace with weekly peak metrics + limits + disk (rolling 7 days)"""
    logging.info(f"Processing weekly namespace: {ns}")
    cpu_peak_prompt = f"""
Get the **peak CPU usage for every pod** in namespace `{ns}` over the last 7 days (rolling).
Return each entry as JSON objects:
{{"pod_name":"...","peak_cpu_cores":0.12,"timestamp":"YYYY-MM-DDTHH:MM:SSZ"}}
Format clean JSON.
Time window: {WEEK_FROM} to {WEEK_TO}
"""
    mem_peak_prompt = f"""
Get the **peak memory usage for every pod** in namespace `{ns}` over the last 7 days (rolling).
Return JSON objects: {{"pod_name":"...","peak_memory_gb":1.2,"timestamp":"..."}}
Time window: {WEEK_FROM} to {WEEK_TO}
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
def collect_namespace_results_week(namespace_results: list):
    """Aggregate results from all weekly namespace processing tasks"""
    logging.info(f"Collecting weekly results from {len(namespace_results)} namespaces")
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
def compile_namespace_weekly_report(namespace_data):
    """Convert namespace weekly data into markdown report"""
    logging.info("Compiling weekly namespace metrics into markdown report")
    if not namespace_data or not namespace_data.get("results"):
        return "## Pod-Level Weekly Metrics by Namespace\n\nNo namespace data available for this week."
    markdown_sections = []
    markdown_sections.append("## Pod-Level Weekly Metrics by Namespace\n")
    markdown_sections.append(f"**Total Namespaces Analyzed (weekly)**: {namespace_data.get('successful', 0)}/{namespace_data.get('total_namespaces', 0)}\n")
    if namespace_data.get('failed_namespaces'):
        markdown_sections.append(f"**Failed Namespaces**: {', '.join(namespace_data['failed_namespaces'])}\n")
    markdown_sections.append("---\n")
    for ns, data in sorted(namespace_data.get("results", {}).items()):
        markdown_sections.append(f"\n### Namespace: `{ns}`\n")
        if data.get("cpu_peak"):
            markdown_sections.append("\n#### Peak CPU Usage (Per Pod - Weekly)\n")
            markdown_sections.append(data["cpu_peak"])
            markdown_sections.append("\n")
        if data.get("memory_peak"):
            markdown_sections.append("\n#### Peak Memory Usage (Per Pod - Weekly)\n")
            markdown_sections.append(data["memory_peak"])
            markdown_sections.append("\n")
        markdown_sections.append("---\n")
    report = "\n".join(markdown_sections)
    logging.info(f"Generated weekly namespace markdown report with {len(markdown_sections)} sections")
    return report

# ------------------- Weekly Node & Pod Markdown Compilation -------------------

@task
def compile_node_weekly_report(ti=None):
    """Compile node-level weekly CPU, Memory, and Disk markdown"""
    node_cpu = ti.xcom_pull(task_ids="node_cpu_week")
    node_mem = ti.xcom_pull(task_ids="node_memory_week")
    node_disk = ti.xcom_pull(task_ids="node_disk_week")
    cpu_cmp = ti.xcom_pull(task_ids="node_cpu_week_vs_prev")
    mem_cmp = ti.xcom_pull(task_ids="node_memory_week_vs_prev")
    disk_cmp = ti.xcom_pull(task_ids="node_disk_week_vs_prev")

    markdown_sections = []
    markdown_sections.append("## Node-Level Weekly Metrics Summary\n")
    if node_cpu:
        markdown_sections.append("### Node CPU (Weekly)\n")
        markdown_sections.append(node_cpu + "\n")
    if cpu_cmp:
        markdown_sections.append("### Node CPU - Week vs Last Week (Summary)\n")
        markdown_sections.append(cpu_cmp + "\n")
    if node_mem:
        markdown_sections.append("### Node Memory (Weekly)\n")
        markdown_sections.append(node_mem + "\n")
    if mem_cmp:
        markdown_sections.append("### Node Memory - Week vs Last Week (Summary)\n")
        markdown_sections.append(mem_cmp + "\n")
    if node_disk:
        markdown_sections.append("### Node Disk (Weekly)\n")
        markdown_sections.append(node_disk + "\n")
    if disk_cmp:
        markdown_sections.append("### Node Disk - Week vs Last Week (Summary)\n")
        markdown_sections.append(disk_cmp + "\n")
    return "\n".join(markdown_sections)

@task
def compile_pod_weekly_report(ti=None):
    """Compile pod-level weekly CPU & Memory markdown"""
    pod_cpu = ti.xcom_pull(task_ids="pod_cpu_week")
    pod_mem = ti.xcom_pull(task_ids="pod_memory_week")
    pod_cpu_cmp = ti.xcom_pull(task_ids="pod_cpu_week_vs_prev")
    pod_mem_cmp = ti.xcom_pull(task_ids="pod_memory_week_vs_prev")

    markdown_sections = []
    markdown_sections.append("## Pod-Level Weekly Metrics Summary\n")
    if pod_cpu:
        markdown_sections.append("### Pod CPU (Weekly)\n")
        markdown_sections.append(pod_cpu + "\n")
    if pod_cpu_cmp:
        markdown_sections.append("### Pod CPU - Week vs Last Week (Summary)\n")
        markdown_sections.append(pod_cpu_cmp + "\n")
    if pod_mem:
        markdown_sections.append("### Pod Memory (Weekly)\n")
        markdown_sections.append(pod_mem + "\n")
    if pod_mem_cmp:
        markdown_sections.append("### Pod Memory - Week vs Last Week (Summary)\n")
        markdown_sections.append(pod_mem_cmp + "\n")
    return "\n".join(markdown_sections)

# ------------------- Weekly Summary Extraction -------------------

@task
def extract_weekly_summary(node_md: str, pod_md: str, namespace_md: str):
    """Extract a concise weekly summary (5 bullets) from node, pod and namespace markdown"""
    logging.info("Generating weekly summary highlights")
    def summarize(text, label):
        try:
            prompt = (
                f"From the following {label} weekly report, extract ONLY a clean 5-bullet summary "
                "highlighting: CPU trends, Memory trends, anomalies, highest usage namespaces/pods, and actions needed. "
                "Do NOT include code, raw metrics, or long pod names.\n"
                + text[:5000]
            )
            return get_ai_response(prompt)
        except Exception as e:
            logging.error(f"Summary extraction failed for {label}: {e}")
            return f"{label} Summary unavailable."
    node_summary = summarize(node_md, "Node Level")
    pod_summary = summarize(pod_md, "Pod Level")
    ns_summary = summarize(namespace_md, "Namespace Level")
    combined = "## Weekly Summary\n\n"
    combined += "### Node Level Overview\n" + node_summary + "\n\n"
    combined += "### Pod Level Overview\n" + pod_summary + "\n\n"
    combined += "### Namespace Level Overview\n" + ns_summary + "\n\n"
    logging.info("Weekly summary generated")
    return combined

# ------------------- Markdown to HTML Conversion (reuse of daily template) -------------------

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
def convert_weekly_to_html(markdown_report: str, summary_md: str):
    """Convert weekly Markdown report to HTML with summary included"""
    logging.info(f"Converting weekly markdown + summary to HTML, length: {len(markdown_report)}")
    full_md = f"{summary_md}\n\n---\n\n{markdown_report}"
    markdown_text = preprocess_markdown(full_md)
    html_body = None
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
        logging.info("Used 'markdown' library for conversion (weekly)")
    except Exception as e:
        logging.error(f"Markdown conversion error (weekly): {e}")
        html_body = f"<pre>{html.escape(markdown_report)}</pre>"

    full_html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Mediamelon SRE Weekly Report</title>
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
<h1>Mediamelon SRE Weekly Report</h1>
<p><strong>Reporting window (rolling 7 days):</strong> {WEEK_FROM} to {WEEK_TO}</p>
<p><strong>Generated:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
{html_body}
<hr>
<p style="text-align: center; color: #666; font-size: 12px;">
<em>Generated by Mediamelon SRE Agent</em>
</p>
</div>
</body>
</html>"""
    logging.info(f"Weekly HTML generated, length: {len(full_html)}")
    return full_html

# ------------------- Email Send Task -------------------

@task
def send_weekly_email_report(html_report: str):
    """Send the weekly HTML report via email"""
    try:
        if not html_report or "<html" not in html_report.lower():
            logging.error("No valid HTML report found (weekly)")
            raise ValueError("HTML report missing or invalid")

        html_body = re.sub(r'```html\s*|```', '', html_report).strip()

        logging.info(f"Connecting to SMTP server {SMTP_HOST}:{SMTP_PORT} (weekly)")
        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=10)
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)

        sender = Variable.get("MEDIAMELON_FROM_ADDRESS", "")
        recipients = Variable.get("MEDIAMELON_TO_ADDRESS", "apnair@ecloudcontrol.com").split(",")
        subject = f"Mediamelon SRE Weekly Report - {datetime.now().strftime('%Y-%m-%d')}"

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = f"Mediamelon SRE Reports {SMTP_SUFFIX}"
        msg["To"] = ", ".join(recipients)
        msg.attach(MIMEText(html_body, "html"))

        server.sendmail(sender, recipients, msg.as_string())
        logging.info(f"Weekly email sent successfully to: {recipients}")
        server.quit()
        return True
    except Exception as e:
        logging.error(f"Failed to send weekly email report: {str(e)}")
        raise

# ------------------- DAG Definition -------------------

with DAG(
    dag_id="sre_mediamelon_sre_report_weekly_v1",
    default_args=default_args,
    # Run weekly on Fridays at 05:30 UTC (rolling 7-day window)
    schedule_interval="30 5 * * FRI",
    catchup=False,
    tags=["sre", "mediamelon", "weekly"],
    max_active_runs=1,
) as dag:

    # Node weekly tasks
    t_node_cpu_week = PythonOperator(task_id="node_cpu_week", python_callable=node_cpu_week, provide_context=True)
    t_node_mem_week = PythonOperator(task_id="node_memory_week", python_callable=node_memory_week, provide_context=True)
    t_node_disk_week = PythonOperator(task_id="node_disk_week", python_callable=node_disk_week, provide_context=True)

    # Node comparisons
    t_node_cpu_cmp = PythonOperator(task_id="node_cpu_week_vs_prev", python_callable=node_cpu_week_vs_prev, provide_context=True)
    t_node_mem_cmp = PythonOperator(task_id="node_memory_week_vs_prev", python_callable=node_memory_week_vs_prev, provide_context=True)
    t_node_disk_cmp = PythonOperator(task_id="node_disk_week_vs_prev", python_callable=node_disk_week_vs_prev, provide_context=True)

    # Wire node compute -> compare -> compile
    [t_node_cpu_week] >> t_node_cpu_cmp
    [t_node_mem_week] >> t_node_mem_cmp
    [t_node_disk_week] >> t_node_disk_cmp

    node_markdown = compile_node_weekly_report()

    # Ensure compare tasks feed into node_markdown
    [t_node_cpu_cmp, t_node_mem_cmp, t_node_disk_cmp] >> node_markdown

    # 4) Pod weekly compute tasks (after namespace and node comparisons, same order as daily)
    t_pod_cpu_week = PythonOperator(task_id="pod_cpu_week", python_callable=pod_cpu_week, provide_context=True)
    t_pod_mem_week = PythonOperator(task_id="pod_memory_week", python_callable=pod_memory_week, provide_context=True)
    t_pod_cpu_cmp = PythonOperator(task_id="pod_cpu_week_vs_prev", python_callable=pod_cpu_week_vs_prev, provide_context=True)
    t_pod_mem_cmp = PythonOperator(task_id="pod_memory_week_vs_prev", python_callable=pod_memory_week_vs_prev, provide_context=True)

    # Namespace dynamic mapping
    namespaces = list_namespaces_weekly()
    namespace_results = process_namespace_week.expand(ns=namespaces)
    collected_namespaces = collect_namespace_results_week(namespace_results)
    namespace_markdown = compile_namespace_weekly_report(collected_namespaces)

    # Compile node & pod markdown
    node_markdown = compile_node_weekly_report()
    pod_markdown = compile_pod_weekly_report()
    [t_pod_cpu_cmp, t_pod_mem_cmp] >> pod_markdown

    # Combine weekly markdowns
    @task
    def combine_weekly_reports(node_md: str, pod_md: str, ns_md: str):
        logging.info("Combining node, pod and namespace weekly markdown reports")
        header = f"# Mediamelon SRE Weekly Report\n\n**Reporting window (rolling 7 days):** {WEEK_FROM} to {WEEK_TO}\n\n"
        return f"{header}{node_md}\n\n---\n\n{pod_md}\n\n---\n\n{ns_md}\n\n"

    combined_markdown = combine_weekly_reports(node_markdown, pod_markdown, namespace_markdown)

    # Extract weekly summary
    summary_section = extract_weekly_summary(node_markdown, pod_markdown, namespace_markdown)

    # Convert to HTML and send
    html_report = convert_weekly_to_html(combined_markdown, summary_section)
    email_task = send_weekly_email_report(html_report)

    # Task dependencies
    # Node: compute -> compare -> compile
    [t_node_cpu_week, t_node_mem_week, t_node_disk_week] >> [t_node_cpu_cmp, t_node_mem_cmp, t_node_disk_cmp] >> node_markdown

    # Ensure node compute starts after namespace_markdown is available (as requested: namespace first)
    namespace_markdown >> [t_node_cpu_week, t_node_mem_week, t_node_disk_week]

    # Ensure node comparisons complete before compiling node markdown (already wired)
    # Ensure pod compute runs after node_markdown compiled (to mimic daily ordering where node runs after namespace then pod)
    node_markdown >> [t_pod_cpu_week, t_pod_mem_week]

    # Ensure compile -> combine -> summary -> html -> send
    [namespace_markdown, node_markdown, pod_markdown] >> combined_markdown >> summary_section >> html_report >> email_task