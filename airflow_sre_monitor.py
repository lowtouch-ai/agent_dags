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
# Configure detailed logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

default_args = {
    "owner": "sre_team",
    "depends_on_past": False,
    "start_date": datetime(2025, 10, 7),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

OLLAMA_HOST = "http://agentomatic:8000/"

def authenticate_gmail():
    try:
        creds = Credentials.from_authorized_user_info(json.loads(Variable.get("AIRFLOW_SRE_GMAIL_CREDENTIALS", {})))
        service = build("gmail", "v1", credentials=creds)
        profile = service.users().getProfile(userId="me").execute()
        logged_in_email = profile.get("emailAddress", "")
        sender_email = Variable.get("AIRFLOW_SRE_FROM_ADDRESS", "")
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
            model='appz/sre/ailab:0.4',
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



def task1_identify_auth_errors(ti, **context):
    try:
        # First prompt to check for password authentication errors
        prompt = """Use the get_dag_logs Tool to retrieve error logs from the DAG named "airflow_log_simulator" for the last 1 hour with the task_name “process_payment_gateway_failure”.  

            Analyze the retrieved logs to:  
            1. Identify and display all error messages.  
            2. Provide a concise summary explaining the nature and cause of the errors.  
            3. Offer clear and actionable recommendations to resolve or prevent these errors.  

            Present the response in a **table format** with two columns:  
            - **Timestamp (IST)** — use a set of timestamps distributed across the past one hour (simulated if not available).  
            - **Error Message**

            After the table, include the following sections clearly separated:
            - **Summary:** A short explanation describing the issue and its root cause.  
            - **Recommendation:** Practical and actionable steps to resolve or avoid these errors in the future.  

            ## Note : make the section heading as Payment Gateway Failures (Last 1 Hour)
        """
        response = get_ai_response(prompt)
        logging.info(f"Task 1 - First prompt response: {response[:200]}...")

        
        # # Format the combined response into markdo
        # format_prompt = f"""Format the following response into markdown format. Include the error details the message details and proposed fixes or findings.
        # {response1}
        # """
        # formatted_response = get_ai_response(format_prompt)

        ti.xcom_push(key="auth_errors", value=response)
        logging.info(f"Task 1 completed: {response[:200]}...")
        return response
    except Exception as e:
        logging.error(f"Error in task1: {str(e)}")
        raise

def task2_count_import_errors(ti, **context):
    try:
        prompt = "run this prometheous query “airflow_dag_processing_import_errors” and format the output in a table with two columns Environment name and Error count"
        response = get_ai_response(prompt)
        
        # Format the response into markdown
#         format_prompt = f"""Format the following response into markdown format.

# {response}"""
#         formatted_response = get_ai_response(format_prompt)
        
        ti.xcom_push(key="import_error_counts", value=response)
        logging.info(f"Task 2 completed: {response[:200]}...")
        return response
    except Exception as e:
        logging.error(f"Error in task2: {str(e)}")
        raise

def task3_detailed_import_errors(ti, **context):
    try:
        prompt = "list the import errros in the airflow and explain the error and which DAG is failing and why it is failing provide explanation and proposed fixes for all errors , ## Note : Convert the timestamps to    IST timezone"
        response = get_ai_response(prompt)
        
#         # Format the response into markdown
#         format_prompt = f"""Format the following response into markdown format with errors , explanation and proposed fixes.

# {response}"""
#         formatted_response = get_ai_response(format_prompt)
        
        ti.xcom_push(key="detailed_import_errors", value=response)
        logging.info(f"Task 3 completed: {response[:200]}...")
        return response
    except Exception as e:
        logging.error(f"Error in task3: {str(e)}")
        raise

def task4_comprehensive_errors(ti, **context):
    try:
        prompt = "Are there any errors in airflow during the last 1 hours; if so find the root cause and suggest fixes for each error and consider all the airflow server , scheduler and worker logs  "
        response = get_ai_response(prompt)
        
        # Format the response into markdown
        format_prompt = f"""Format the following response into markdown format. Remove the Message ID and Index in the table in the response. 

        {response}

        ## Note In the Output Mention Log Entries instead of Logs and the replace `Found` with `Displayed`
        
        """
        formatted_response = get_ai_response(format_prompt)
        ti.xcom_push(key="all_errors_raw", value=response)
        ti.xcom_push(key="all_errors", value=formatted_response)
        logging.info(f"Task 4 completed: {formatted_response[:200]}...")
        return formatted_response
    except Exception as e:
        logging.error(f"Error in task4: {str(e)}")
        raise

def task5_resource_utilization(ti, **context):
    
    threshold= Variable.get("AIRFLOW_CPU_UTILIZATION_THRESHOLD", "10")
    try:
        prompt = """Use the **QueryRangeMetric** tool with an **absolute time range** to retrieve the **hourly CPU Utilization (in %)** of the instance name = **airflowwkr** for the **last 24 hours** from both instances — **"cadvisor:8080"** and **"nvdevkit2025b:8888"**.
            * * *
            ### Query to Use:
            Use the following Prometheus query while invoking the tool:
            ```
            sum(rate(container_cpu_usage_seconds_total{instance="<INSTANCE>", name="airflowwkr"}[5m])) * 100
            ```
            Replace `<INSTANCE>` dynamically with:
            `"cadvisor:8080"` _for_ _lab-a_*
            `"nvdevkit2025b:8888"` _for_ _lab-b_*
            **Query Parameters:**
            *   `start`: `<ISO8601 timestamp for 24 hours ago>`
            *   `end`: `<ISO8601 timestamp for current time>`
            *   `step`: `1h`
            * * *
            """
        prompt2=f"""
            ### Perform the following analyses:
            1.  **Peak Utilization:**
                _Identify and summarize the_ _top three highest CPU utilization points (with timestamps)_* for both **cadvisor:8080** and **nvdevkit2025b:8888**.
                _Include the_ _utilization percentage (%)_* for each peak.
            2.  **Idle Periods (Threshold ≤ {threshold}%):**
                _Detect and list_ _time periods where CPU utilization remained at or below {threshold} %_*.
                _Mention the_ _start and end times_* for each idle period and the **approximate duration**.
                _Clearly differentiate idle periods for_ _each environment_*.
            3.  **Resource Analysis & Cost Optimization:**
                *   Compare overall resource utilization trends, load patterns, and identify inefficiencies between both environments.
                _Based on the_ _total idle duration_*, estimate potential **cost savings (in percentage)** that could be achieved by optimizing idle resources.
                *   Provide a detailed cost-saving summary, including:
                    _Estimated_ _idle percentage_* of total time
                    *   **Potential cost reduction (%)**
                    *   **Key recommendations** for improving resource efficiency.
            * * *
            ### Note:
            In the **final summary/output**, replace:
            *   **cadvisor:8080 → lab-a**
            *   **nvdevkit2025b:8888 → lab-b**
            Do **not** mention the original instance names (**cadvisor:8080** or **nvdevkit2025b:8888**) anywhere in the final output.
            """
        prompt = prompt + prompt2
        response = get_ai_response(prompt)
        
#         # Format the response into markdown, ensuring replacements
#         format_prompt = f"""Format the following response into markdown format. Ensure to replace cadvisor:8080 with lab-a and nvdevkit2025b:8888 with lab-b in the response, and do not mention the originals.

# {response}"""
#         formatted_response = get_ai_response(format_prompt)
        
        ti.xcom_push(key="resource_utilization", value=response)
        logging.info(f"Task 5 completed: {response[:200]}...")
        return response
    except Exception as e:
        logging.error(f"Error in task5: {str(e)}")
        raise

def task6_observability_monitoring(ti, **context):
    dag_name= Variable.get("AIRFLOW_DAG_NAME", "airflow_log_simulator")
    try:
        prompt = f"""
        Use the get_dag_logs Tool to retrieve the log counts for each task in the DAG named '{dag_name}' over the last 1 hour.  
        After fetching the data:  
        - List all tasks along with their respective log counts.  
        - Identify and highlight any tasks that have fewer than 5 logs.  
        - For each such task, include a note explaining that the low log count indicates the task may not be executing properly and requires investigation.  
        - Conclude with a short summary mentioning the total number of tasks analyzed and how many require attention.
    """
        response = get_ai_response(prompt)
        
        # Format the response into markdown
        format_prompt = f"""Format the following response into markdown format.

{response}"""
        formatted_response = get_ai_response(format_prompt)
        
        ti.xcom_push(key="observability_monitoring", value=formatted_response)
        logging.info(f"Task 6 completed: {formatted_response[:200]}...")
        return formatted_response
    except Exception as e:
        logging.error(f"Error in task6: {str(e)}")
        raise

def task7_compose_email(ti, **context):
    try:
        auth_errors = ti.xcom_pull(key="auth_errors") or "No authentication errors found."
        import_error_counts = ti.xcom_pull(key="import_error_counts") or "No import error counts available."
        detailed_import_errors = ti.xcom_pull(key="detailed_import_errors") or "No detailed import errors available."
        all_errors = ti.xcom_pull(key="all_errors") or "No comprehensive error list available."
        resource_utilization = ti.xcom_pull(key="resource_utilization") or "No resource utilization data available."
        observability_monitoring = ti.xcom_pull(key="observability_monitoring") or "No observability monitoring data available."
        
        # First, compose the email in markdown format
        markdown_prompt = f"""
            Compose a well-formatted professional email incorporating the following details from Airflow log analysis. The email should be written in American English, follow a professional technical document structure (e.g., greeting, subject, introduction, detailed sections for each error type, summary, and recommendations). Ensure clarity so that any stakeholder can understand the issues without prior context. Format the entire email content as valid Markdown (output only the Markdown code, without any additional text).

            Findings:
            - Payment gateway failures (last 1 hour): {auth_errors}
            - DAG import error counts: {import_error_counts}
            - Detailed DAG import errors: {detailed_import_errors}
            - Comprehensive list of all errors (last 1 hour): {all_errors}
            - Comprehensive Summary of Resource Utilization: {resource_utilization}
            - Observability Monitoring Insights: {observability_monitoring}

            Structure the Markdown email as follows:
            - Subject: # Airflow Log Analysis Report - {datetime.now().strftime('%Y-%m-%d')}
            - Greeting: Dear Team,
            - Summary: ## Summary  
              [Provide a concise overview of key issues, including high-level impacts, urgency, and any critical findings that require immediate attention. Highlight the scope of errors and resource utilization concerns.]
            - Section for Payment Gateway Failures: ## Payment Gateway Failures (Last 1 Hour)  
              [Include the complete list of payment gateway failures from {auth_errors}. Provide detailed descriptions, including error codes, timestamps, affected users or services, and potential causes where applicable. Use tables or lists for clarity.]
            - Section for Import Error Counts: ## DAG Import Error Counts  
              [Include the full details from {import_error_counts}. Present the counts in a clear format, such as a table, showing the number of occurrences per DAG or error type. Include any patterns or trends observed.]
            - Section for Detailed Import Errors: ## Detailed DAG Import Errors  
              [Include the complete details from {detailed_import_errors}. Provide granular information, such as specific DAGs affected, error messages, stack traces, and potential root causes. Use code blocks or tables for readability.]
            - Section for All Errors: ## Miscellaneous Error List (Last 1 Hour)  
              [Include the full list from {all_errors}. Categorize errors by type or severity if applicable, and provide sufficient detail to understand the context and impact of each error. Use lists or tables for clarity.]
            - Section for Resource Utilization: ## Resource Utilization Summary (Last 24 Hours)  
              [Include the complete details from {resource_utilization}. Provide a detailed breakdown of resource usage, including CPU, memory, disk, and network metrics. Highlight any cost optimization opportunities, such as over-provisioned resources, underutilized instances, or inefficient task scheduling. Include specific findings related to cost inefficiencies and other performance metrics. Use tables or charts for clarity.]
            - Section for Observability Monitoring: ## Observability Insights  
              [Include the full details from {observability_monitoring}. Provide insights into system health, monitoring gaps, and any anomalies detected. Include metrics, logs, or tracing data that highlight performance bottlenecks or potential improvements. Use lists or tables for readability.]
            - Recommendations: ## Recommendations  
              - [Provide actionable, prioritized recommendations based on all findings. Include specific steps to address authentication errors, DAG import issues, miscellaneous errors, resource optimization (including cost-saving measures), and observability improvements. Ensure recommendations are detailed and feasible for implementation.]
            - Closing: Best regards,  
              Airflow SRE Agent  
              lowtouch.ai

            Use tables, lists, or code blocks where appropriate to ensure readability and clarity. Ensure all sections are fully detailed, including every finding from the provided variables without truncation or omission. Do not skip any parts of the findings, and incorporate all cost optimization insights and other findings in the resource utilization section. Output only the valid Markdown code.
            """
            
        markdown_response = get_ai_response(markdown_prompt)
        logging.info(f"Task 7 - Markdown email composed: {markdown_response[:200]}...")
        
        # Second, convert the markdown to HTML
        html_prompt = f"""
            Convert the following Markdown email content to valid HTML format for an email. Ensure the output is pure HTML starting with <html><head><title>Airflow Log Analysis Report</title></head><body> and ending with </body></html>. Preserve all structure, sections, and details from the Markdown content exactly as provided, without adding, modifying, or truncating any content. Use appropriate HTML tags for headings (<h1> for #, <h2> for ##), paragraphs (<p>), lists (<ul> or <ol>), code blocks (<pre><code>), and especially tables make the table borders bold and solid. For Markdown tables (e.g., using | and - for structure), accurately convert them to HTML <table> elements with <tr> for rows, <th> for header cells, and <td> for data cells, ensuring proper alignment and formatting and also make the borders solid. Maintain consistent indentation and semantic HTML for readability. Do not render tables as plain text; ensure they are structured as proper HTML tables.

            {markdown_response}
            
            Output only the valid HTML code.
            """
        html_response = get_ai_response(html_prompt)
        
        # Clean to ensure it's pure HTML
        if not html_response.startswith('<html>'):
            html_response = f"<html><body>{html_response}</body></html>"
        ti.xcom_push(key="email_markdown", value=markdown_response)
        ti.xcom_push(key="email_html", value=html_response)
        logging.info(f"Task 7 completed: {html_response[:200]}...")
        return html_response
    except Exception as e:
        logging.error(f"Error in task7: {str(e)}")
        raise
    
def task8_send_email(ti, **context):
    try:
        email_html = ti.xcom_pull(key="email_html") or ""
        if not email_html:
            logging.error("No HTML content found in XCom")
            raise ValueError("No email content available")

        cleaned_response = re.sub(r'```html\n|```', '', email_html).strip()
    
        if not cleaned_response.strip().startswith('<!DOCTYPE') and not cleaned_response.strip().startswith('<html'):
            if not cleaned_response.strip().startswith('<'):
                cleaned_response = f"<html><body>{cleaned_response}</body></html>"
        
        service = authenticate_gmail()
        if not service:
            logging.error("Gmail authentication failed")
            raise ValueError("Gmail authentication failed")

        sender_email = Variable.get("AIRFLOW_SRE_FROM_ADDRESS", " ")
        receiver_email = Variable.get("AIRFLOW_SRE_TO_ADDRESS", " ")
        if not sender_email or not receiver_email:
            logging.error("Sender or receiver email not configured")
            raise ValueError("Sender or receiver email not configured")
        
        msg = MIMEMultipart()
        msg["From"] = sender_email
        msg["To"] = receiver_email
        msg["Subject"] = f"Airflow Log Analysis Report - {datetime.now().strftime('%Y-%m-%d')}"
        msg.attach(MIMEText(cleaned_response, "html"))
        raw_msg = base64.urlsafe_b64encode(msg.as_string().encode("utf-8")).decode("utf-8")
        
        result = service.users().messages().send(userId="me", body={"raw": raw_msg}).execute()
        logging.info(f"Email sent successfully to {receiver_email}")
        return f"Email sent successfully to {receiver_email}"
    except Exception as e:
        logging.error(f"Error in task8_send_email: {str(e)}")
        raise

with DAG(
    "airflow_sre_agent_monitoring",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["sre", "log_analysis", "airflow", "monitoring"]
) as dag:
    
    t1 = PythonOperator(
        task_id="identify_auth_errors",
        python_callable=task1_identify_auth_errors,
    )
    
    t2 = PythonOperator(
        task_id="count_import_errors",
        python_callable=task2_count_import_errors,
    )
    
    t3 = PythonOperator(
        task_id="detailed_import_errors",
        python_callable=task3_detailed_import_errors,
    )
    
    t4 = PythonOperator(
        task_id="comprehensive_errors",
        python_callable=task4_comprehensive_errors,
    )

    t5 = PythonOperator(
        task_id="resource_utilization",
        python_callable=task5_resource_utilization,
    )

    t6 = PythonOperator(
        task_id="observability_monitoring",
        python_callable=task6_observability_monitoring,
    )

    t7 = PythonOperator(
        task_id="compose_email",
        python_callable=task7_compose_email,
    )

    t8 = PythonOperator(
        task_id="send_email",
        python_callable=task8_send_email,
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8
