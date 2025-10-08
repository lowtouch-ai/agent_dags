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
    time_interval = Variable.get("AIRFLOW_LOG_TIME_INTERVAL", "2 hour")
    try:
        # First prompt to check for password authentication errors
        prompt1 = f"""check if there are any occurrences of \"password authentication\" errors in the 'airflowsvr-2.0' environment for the last {time_interval}."""
        response1 = get_ai_response(prompt1)
        logging.info(f"Task 1 - First prompt response: {response1[:200]}...")

        # Second prompt to get details of errors and identify affected DAGs and tasks
        prompt2 = """From the following password authentication error details, extract and display the detailed message for each error. Identify and list the corresponding DAG names and Task names associated with each error. Provide a recommendation and a summary of the findings."""
        response2 = get_ai_response(prompt2, conversation_history=[{"role": "assistant", "content": response1}])
        logging.info(f"Task 1 - Second prompt response: {response2[:200]}...")

        # Combine responses
        combined_response = f"Authentication Errors Check:\n{response1}\n\nDetailed Analysis and Recommendations:\n{response2}"
        
        # Format the combined response into markdown
        format_prompt = f"""Format the following response into markdown format. Include the error details the message details and proposed fixes or findings. Do not show the Message ID and Index in the table in the response.
        {combined_response}
        """
        formatted_response = get_ai_response(format_prompt)
        
        ti.xcom_push(key="auth_errors", value=formatted_response)
        logging.info(f"Task 1 completed: {formatted_response[:200]}...")
        return formatted_response
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
        prompt = "list the import errros in the airflow and explain the error and which DAG is failing and why it is failing provide explanation and proposed fixes for all errors"
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
        prompt = "Are there any errors in airflow during the last 1 hours; if so find the root cause and suggest fixes for each error and consider all the airflow server , scheduler and worker logs"
        response = get_ai_response(prompt)
        
        # Format the response into markdown
        format_prompt = f"""Format the following response into markdown format. Remove the Message ID and Index in the table in the response.

{response}"""
        formatted_response = get_ai_response(format_prompt)
        ti.xcom_push(key="all_errors_raw", value=response)
        ti.xcom_push(key="all_errors", value=formatted_response)
        logging.info(f"Task 4 completed: {formatted_response[:200]}...")
        return formatted_response
    except Exception as e:
        logging.error(f"Error in task4: {str(e)}")
        raise

def task5_resource_utilization(ti, **context):
    try:
        prompt = """Use the QueryRangeMetric tool to retrieve the **hourly CPU Utilization** of the  name =**airflowwkr** for the **last 24 hours** from both instances— "cadvisor:8080"and "nvdevkit2025b:8888".  
            Perform the following analyses:
            1. **Peak Utilization:** Identify and summarize the top three highest CPU utilization points (with timestamps) for both cadvisor:8080 and ndevkit2025b:8888.  
            2. **Idle Periods:** Check for any periods of low or idle CPU activity and specify their approximate durations and times.  
            3. **Resource Analysis:** Provide a brief resource utilization assessment comparing both environments, noting trends, load patterns, and any inefficiencies observed.
            ## Note: In the Output Replace the **cadvisor:8080** with **lab-a** and **nvdevkit2025b:8888** with **lab-b** in the final summary , Do not Mention the **cadvisor:8080** and **nvdevkit2025b:8888** in the final summary .
            """
        response = get_ai_response(prompt)
        
        # Format the response into markdown, ensuring replacements
        format_prompt = f"""Format the following response into markdown format. Ensure to replace cadvisor:8080 with lab-a and nvdevkit2025b:8888 with lab-b in the response, and do not mention the originals.

{response}"""
        formatted_response = get_ai_response(format_prompt)
        
        ti.xcom_push(key="resource_utilization", value=formatted_response)
        logging.info(f"Task 5 completed: {formatted_response[:200]}...")
        return formatted_response
    except Exception as e:
        logging.error(f"Error in task5: {str(e)}")
        raise

def task6_observability_monitoring(ti, **context):
    dag_name= Variable.get("AIRFLOW_DAG_NAME", "airflow_log_simulate")
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
            - Authentication errors (last 1 hour): {auth_errors}
            - DAG import error counts : {import_error_counts}
            - Detailed DAG import errors : {detailed_import_errors}
            - Comprehensive list of all errors (last 1 hour): {all_errors}
            - Comprehensive Summary of Resource Utilization: {resource_utilization}
            - Observability Monitoring Insights: {observability_monitoring}

            Structure the Markdown email as follows:
            - Subject: # Airflow Log Analysis Report - {datetime.now().strftime('%Y-%m-%d')}
            - Greeting: Dear Team,
            - Summary: ## Summary  
              [brief overview of key issues, including high-level impacts and urgency]
            - Section for Authentication Errors: ## Authentication Errors (Last 1 Hour)  
              [details]
            - Section for Import Error Counts: ## DAG Import Error Counts   
              [details]
            - Section for Detailed Import Errors: ## Detailed DAG Import Errors  
              [details]
            - Section for All Errors: ## Comprehensive Error List (Last 1 Hour)  
              [details]
            - Section for Resource Utilization: ## Resource Utilization Summary  of last 24 hours
              [details]
            - Section for Observability Monitoring: ## Observability Monitoring Insights  
              [details]
            - Recommendations: ## Recommendations  
              - [actionable items]
            - Closing: Best regards,  
              Airflow SRE Agent  
              lowtouch.ai

            Use tables or lists where appropriate for readability. Ensure the content is clear and technical, and include all details without avoiding any section.
            Output only the valid Markdown code.
            """
        markdown_response = get_ai_response(markdown_prompt)
        logging.info(f"Task 7 - Markdown email composed: {markdown_response[:200]}...")
        
        # Second, convert the markdown to HTML
        html_prompt = f"""
            Convert the following Markdown email content to valid HTML format for an email. Ensure the output is pure HTML starting with <html><head><title>Airflow Log Analysis Report</title></head><body> and ending with </body></html>. Preserve all structure, sections, and details. Use appropriate HTML tags for headings, paragraphs, lists, and tables.

            {markdown_response}
            
            Output only the valid HTML code.
            """
        html_response = get_ai_response(html_prompt)
        
        # Clean to ensure it's pure HTML
        if not html_response.startswith('<html>'):
            html_response = f"<html><body>{html_response}</body></html>"
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
    schedule_interval=None,
    catchup=False,
    tags=["sre", "log_analysis", "airflow", "monitoring"]
) as dag:
    
    t1 = PythonOperator(
        task_id="identify_auth_errors",
        python_callable=task1_identify_auth_errors,
        provide_context=True
    )
    
    t2 = PythonOperator(
        task_id="count_import_errors",
        python_callable=task2_count_import_errors,
        provide_context=True
    )
    
    t3 = PythonOperator(
        task_id="detailed_import_errors",
        python_callable=task3_detailed_import_errors,
        provide_context=True
    )
    
    t4 = PythonOperator(
        task_id="comprehensive_errors",
        python_callable=task4_comprehensive_errors,
        provide_context=True
    )

    t5 = PythonOperator(
        task_id="resource_utilization",
        python_callable=task5_resource_utilization,
        provide_context=True
    )

    t6 = PythonOperator(
        task_id="observability_monitoring",
        python_callable=task6_observability_monitoring,
        provide_context=True
    )

    t7 = PythonOperator(
        task_id="compose_email",
        python_callable=task7_compose_email,
        provide_context=True
    )

    t8 = PythonOperator(
        task_id="send_email",
        python_callable=task8_send_email,
        provide_context=True
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8
