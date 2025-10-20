from airflow import DAG
from airflow.operators.python import PythonOperator
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
# from markdownify import markdownify as md

# Configure detailed logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

default_args = {
    "owner": "sre_team",
    "depends_on_past": False,
    "start_date": datetime(2025, 10, 16),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

OLLAMA_HOST = "http://agentomatic:8000/"

def authenticate_gmail():
    try:
        creds = Credentials.from_authorized_user_info(json.loads(Variable.get("BIGQUERY_SRE_GMAIL_CREDENTIALS", {})))
        service = build("gmail", "v1", credentials=creds)
        profile = service.users().getProfile(userId="me").execute()
        logged_in_email = profile.get("emailAddress", "")
        sender_email = Variable.get("BIGQUERY_SRE_FROM_ADDRESS", "")
        if logged_in_email.lower() != sender_email.lower():
            raise ValueError(f"Wrong Gmail account! Expected {sender_email}, but got {logged_in_email}")
        logging.info(f"Authenticated Gmail account: {logged_in_email}")
        return service
    except Exception as e:
        logging.error(f"Failed to authenticate Gmail: {str(e)}")
        return None

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
            model='bigquery/sre/ailab:0.3',
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

def check_execution_count(ti, **context):
    try:
        threshold = Variable.get("BQ_EXECUTION_COUNT_THRESHOLD", 1000)
        query="floor(sum (increase(bigquery_query_total{}[1h])))"
        prompt = f"""
        Use the queryInstantMetric Tool to retrieve the number of BigQuery query executions in the last 1 hour for project 'sre-agent-9809', dataset 'webshop'.
        Query to use: `{query}`
        Give me the total number of executions.
        """
        response = get_ai_response(prompt)
        ti.xcom_push(key="execution_count", value=response)
        logging.info(f"Execution count check completed: {response[:200]}...")
        return response
    except Exception as e:
        logging.error(f"Error in check_execution_count: {str(e)}")
        raise

def check_slot_usage(ti, **context):
    try:
        threshold = Variable.get("BQ_SLOT_USAGE_THRESHOLD", 50)
        prompt = f"""
        Use the queryInstantMetric Tool to retrieve the highest slot usage in the last 1 hour for project 'sre-agent-9809', dataset 'webshop'.
        Query to use: `max_over_time(bigquery_query_slot_time_seconds{{project_id=\"sre-agent-9809\", dataset_id=\"webshop\"}}[1h]) > {threshold}`
        Analyze the result to:
        1. Report the peak slot usage.
        2. List the problematic queries causing the issue.
        3. Identify potential causes (e.g., complex queries, large joins).
        Present the response in markdown format with:
        - **Peak Slot Usage**: Maximum slots used.
        - **Problematic Queries**: List of queries (with exact SQL text ) that contributed to high usage.
        - **Analysis**: Brief explanation of findings and potential issues, referencing schema if relevant  (e.g., joins on `articles.productid` or `order.customerid`) , provide the percentage of cost saving based on the findings.
        """
        response = get_ai_response(prompt)
        ti.xcom_push(key="slot_usage", value=response)
        logging.info(f"Slot usage check completed: {response[:200]}...")
        return response
    except Exception as e:
        logging.error(f"Error in check_slot_usage: {str(e)}")
        raise

def check_execution_time(ti, **context):
    try:
        threshold = Variable.get("BQ_EXECUTION_TIME_THRESHOLD", 10.0)
        prompt = f"""
        Use the queryInstantMetric Tool to retrieve the maximum query execution time in the last 1 hour for project 'sre-agent-9809', dataset 'webshop'.
        Query to use: `max_over_time(bigquery_query_execution_time_seconds{{project_id=\"sre-agent-9809\", dataset_id=\"webshop\"}}[1h]) > {threshold}`
        Analyze the result to:
        1. Report the highest execution time.
        2. List the problematic queries causing the issue.
        3. Identify potential causes (e.g., unoptimized queries, lack of partitioning on `order.ordertimestamp`).
        Present the response in markdown format with:
        - **Max Execution Time**: Highest execution time observed.
        - **Problematic Queries**: List of queries (with exact SQL text) that contributed to high execution times.
        - **Analysis**: Brief explanation of findings and potential issues, referencing schema if relevant , provide the percentage of cost saving based on the findings.
        """
        response = get_ai_response(prompt)
        ti.xcom_push(key="execution_time", value=response)
        logging.info(f"Execution time check completed: {response[:200]}...")
        return response
    except Exception as e:
        logging.error(f"Error in check_execution_time: {str(e)}")
        raise

def check_memory_usage(ti, **context):
    try:
        threshold = Variable.get("BQ_MEMORY_USAGE_THRESHOLD", 19)  # 19MB
        prompt = f"""
        Use the queryInstantMetric Tool to retrieve the maximum memory usage (bytes scanned) in the last 1 hour for project 'sre-agent-9809', dataset 'webshop'.
        Query to use: `max_over_time(bigquery_data_scanned_bytes{{project_id=\"sre-agent-9809\", dataset_id=\"webshop\"}}[1h]) / 1024 / 1024 > {threshold}`
        Analyze the result to:
        1. Report the peak memory usage in MB.
        2. List the problematic queries causing the issue.
        3. If the threshold is exceeded, identify potential causes (e.g., full table scans on `articles` or `order`).
        Present the response in markdown format with:
        - **Peak Memory Usage**: Maximum bytes scanned (in MB).
        - **Problematic Queries**: List of queries (with exact SQL text) that contributed to high memory usage.
        - **Analysis**: Brief explanation of findings and potential issues, referencing schema if relevant , provide the percentage of cost saving based on the findings.
        """
        response = get_ai_response(prompt)
        ti.xcom_push(key="memory_usage", value=response)
        logging.info(f"Memory usage check completed: {response[:200]}...")
        return response
    except Exception as e:
        logging.error(f"Error in check_memory_usage: {str(e)}")
        raise

def compile_analysis_report(ti, **context):
    try:
        execution_count = ti.xcom_pull(key="execution_count") or "No execution count data available."
        slot_usage = ti.xcom_pull(key="slot_usage") or "No slot usage data available."
        execution_time = ti.xcom_pull(key="execution_time") or "No execution time data available."
        memory_usage = ti.xcom_pull(key="memory_usage") or "No memory usage data available."

        prompt = f"""
            
        You are a professional SRE report compiler specializing in BigQuery performance analytics.
        Your task is to generate a **comprehensive and well-formatted email report** in **markdown format** 
        based on the following metrics for project **'sre-agent-9809'**, dataset **'webshop'** over the **last 1 hour**.

        Use the following metric-specific analytical responses as-is (retain their original content and formatting):
        - Execution Count: {execution_count}
        - Slot Usage: {slot_usage}
        - Execution Time: {execution_time}
        - Memory Usage: {memory_usage}

        Your goal is **not** to rewrite or summarize these sections — instead:
        - Keep each section’s content **exactly as provided**.
        - Only format them cohesively as part of a single professional email.
        - Include technical and analytical summaries before and after the main body to provide complete context.

        ---

        ### Structure the Email as follows:

        **1. Greeting**
        Start with:  
        `Dear Team,`


        **2. Summary of Findings**
        Provide a concise but technically detailed overview including:
        - Total number of queries executed (from Execution Count section).
        - Count of total *unique* problematic queries across all sections (deduplicate any repeated ones).
        - Key threshold exceedances (slot usage, execution time, memory usage) and their operational or cost implications.
        - Brief summary of the main causes (e.g., large joins, lack of partitioning, self-joins, unoptimized filters).
        - Overall impact and high-level conclusion (performance degradation, slot inefficiency, cost increase).

        ---

        **3. Detailed Findings**
        For each metric, include the following subsections:


        ### **Slot Usage**
        - Include the **exact content** from the provided Slot Usage section ({slot_usage}).
        - Then, add a short **section summary** explaining:
        - How slot usage behavior impacts query concurrency or resource saturation.
        - Whether queries exceeded expected thresholds.
        - Include 2–3 technical recommendations (partitioning, optimized joins, or clustering).

        

        ### **Execution Time**
        - Include the **exact content** from the provided Execution Time section ({execution_time}).
        - Then, add a **section summary** detailing:
        - Which queries caused delays and why (e.g., timestamp filters, unoptimized joins).
        - Impacts on pipeline latency or throughput.
        - Suggested optimizations or query rewrites.

        

        ### **Memory Usage**
        - Include the **exact content** from the provided Memory Usage section ({memory_usage}).
        - Then, add a **section summary** detailing:
        - How memory usage correlates with scan size and complex computations.
        - Impact on overall query efficiency and cost.
        - Technical recommendations (filter refinement, window function optimization, clustering).

        


        **4. Recommendations**
        Provide a consolidated list of all optimization actions combining findings from all metric sections.
        Organize recommendations under three categories:

        * **Schema Optimizations**

        * Partition large tables (e.g., `order` by `ordertimestamp`).
        * Cluster frequently joined columns (e.g., `articles.productid`).

        * **Query Optimizations**

        * Refine filter conditions (reduce time ranges, use selective predicates).
        * Optimize join order and conditions.
        * Avoid full-table scans by limiting timestamps.

        * **Monitoring and Alerts**

        * Build dashboards to monitor query slot usage, execution time, and scan bytes.
        * Define alerts for thresholds (e.g., >10s execution time, >100 slots).
        * Continuously track cost metrics and performance degradation.

        Make sure this section reads like a **technical recommendation report** suitable for an SRE post-analysis meeting.

        ---

        **5. Final Consolidated Summary**
        Write a closing section that:

        * Summarizes the overall findings and optimization plan.
        * Reiterates key performance issues (slot saturation, query inefficiency, memory overuse).
        * Emphasizes the next steps toward improvement.
        * Includes follow-up actions like:

        * Implementing schema changes.
        * Scheduling a review meeting.
        * Monitoring impact post-optimization.

        ---

        End the email with a professional sign-off:

        Thanks,
        BigQuery SRE Team
        lowtouch.ai

        """
        response = get_ai_response(prompt)
        ti.xcom_push(key="analysis_report", value=response)
        logging.info(f"Analysis report compiled: {response[:200]}...")
        return response
    except Exception as e:
        logging.error(f"Error in compile_analysis_report: {str(e)}")
        raise

def convert_to_html(ti, **context):
    try:
        report = ti.xcom_pull(key="analysis_report") or "No report available."
        prompt = f"""
            Convert the following markdown-formatted BigQuery report into a **well-structured and standards-compliant HTML document** for email rendering.

            ### Requirements:
            1. Output must be **pure valid HTML** (no markdown traces like `**`, `#`, or backticks).
            2. Begin with:
            ```html
            <html><head><title>BigQuery SRE Report</title></head><body>
            ````

            and end with:

            ```html
            </body></html>
            ```

            3. Preserve **all structure and content exactly** as provided — do not modify or shorten any section.
            4. Use **semantic HTML tags**:

            * Markdown headings → `<h1>`, `<h2>`, `<h3>`, `<h4>`
            * Paragraphs → `<p>`
            * Bullet/numbered lists → `<ul>` / `<ol>` with `<li>`
            * Code blocks → `<pre><code>` (preserve indentation and SQL syntax)
            * Bold text → `<strong>`
            * Italic text → `<em>`
            * Tables → `<table border="1" style="border-collapse: collapse; width:100%; border: 1px solid #333;">`
                Include `<tr>`, `<th>`, `<td>` with visible borders.
            5. Maintain **consistent indentation and spacing** for readability.
            6. Preserve **inline technical text** like dataset names, metrics, and SQL identifiers with `<code>` tags.
            7. Ensure **SQL code** remains readable and syntax-preserved inside `<pre><code>` blocks — do not escape quotes or alter indentation.
            8. Ensure **markdown characters (like `**`, `_`, `#`, ```) are fully removed** and replaced with proper HTML formatting.

            ### Example formatting rules:

            * `**Executive Summary**` → `<h2>Executive Summary</h2>`
            * `### Slot Usage` → `<h3>Slot Usage</h3>`
            * Inline code like `project_id` → `<code>project_id</code>`
            * Code block:

            ```sql
            SELECT * FROM table;
            ```

            →

            ```html
            <pre><code class="language-sql">SELECT * FROM table;</code></pre>
            ```

            Now, convert the following markdown report to clean, valid, and visually structured HTML:

            {report}

            Output **only** the final HTML code (no markdown, no commentary).
            """

        html_response = get_ai_response(prompt)
        if not html_response.startswith('<html>'):
            html_response = f"<html><head><title>BigQuery SRE Report</title></head><body>{html_response}</body></html>"
        ti.xcom_push(key="html_report", value=html_response)
        logging.info(f"HTML report generated: {html_response[:200]}...")
        return html_response
    except Exception as e:
        logging.error(f"Error in convert_to_html: {str(e)}")
        raise

def send_email(ti, **context):
    try:
        html_report = ti.xcom_pull(key="html_report") or ""
        if not html_report:
            logging.error("No HTML report found in XCom")
            raise ValueError("No HTML report available")

        cleaned_response = re.sub(r'```html\n|```', '', html_report).strip()
        if not cleaned_response.strip().startswith('<!DOCTYPE') and not cleaned_response.strip().startswith('<html'):
            cleaned_response = f"<html><head><title>BigQuery SRE Report</title></head><body>{cleaned_response}</body></html>"

        service = authenticate_gmail()
        if not service:
            logging.error("Gmail authentication failed")
            raise ValueError("Gmail authentication failed")

        sender_email = Variable.get("BIGQUERY_SRE_FROM_ADDRESS", "")
        receiver_email = Variable.get("BIGQUERY_SRE_TO_ADDRESS", "")
        if not sender_email or not receiver_email:
            logging.error("Sender or receiver email not configured")
            raise ValueError("Sender or receiver email not configured")

        msg = MIMEMultipart()
        msg["From"] = sender_email
        msg["To"] = receiver_email
        msg["Subject"] = f"BigQuery SRE Report - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        msg.attach(MIMEText(cleaned_response, "html"))
        raw_msg = base64.urlsafe_b64encode(msg.as_string().encode("utf-8")).decode("utf-8")

        result = service.users().messages().send(userId="me", body={"raw": raw_msg}).execute()
        logging.info(f"Email sent successfully to {receiver_email}")
        return f"Email sent successfully to {receiver_email}"
    except Exception as e:
        logging.error(f"Error in send_email: {str(e)}")
        raise

with DAG(
    "bigquery_monitor",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["sre", "bigquery", "monitoring", "lowtouch.ai"]
) as dag:
    
    t1 = PythonOperator(
        task_id="check_execution_count",
        python_callable=check_execution_count,
        provide_context=True
    )
    
    t2 = PythonOperator(
        task_id="check_slot_usage",
        python_callable=check_slot_usage,
        provide_context=True
    )
    
    t3 = PythonOperator(
        task_id="check_execution_time",
        python_callable=check_execution_time,
        provide_context=True
    )
    
    t4 = PythonOperator(
        task_id="check_memory_usage",
        python_callable=check_memory_usage,
        provide_context=True
    )
    
    t5 = PythonOperator(
        task_id="compile_analysis_report",
        python_callable=compile_analysis_report,
        provide_context=True
    )
    
    t6 = PythonOperator(
        task_id="convert_to_html",
        python_callable=convert_to_html,
        provide_context=True
    )
    
    t7 = PythonOperator(
        task_id="send_email",
        python_callable=send_email,
        provide_context=True
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7
