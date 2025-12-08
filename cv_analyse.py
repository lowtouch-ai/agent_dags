"""
Airflow DAG for analyzing CVs from email attachments.
Processes PDF resumes and extracts candidate information.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
import json
import sys
import os

try:
    sys.path.append(os.path.dirname(os.path.realpath(__file__)))
    logging.info(f"Appended to sys.path: {os.path.dirname(os.path.realpath(__file__))}")
except Exception as e:
    logging.error(f"Error appending to sys.path: {e}")

# Import your email utilities if needed
from agent_dags.utils.email import authenticate_gmail
from agent_dags.utils.agent_util import get_ai_response, extract_json_from_text

# Configuration constants
GMAIL_CREDENTIALS = Variable.get("ltai.v3.lowtouch.recruitment.email_credentials", default_var=None)
RECRUITMENT_FROM_ADDRESS = Variable.get("ltai.v3.lowtouch.recruitment.from_address", default_var=None)
# Default DAG arguments
default_args = {
    "owner": "lowtouch.ai_developers",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 24),
    "retries": 1,
    "retry_delay": timedelta(seconds=15),
}


def extract_cv_content(**kwargs):
    """
    Extract CV content from email attachments.
    Retrieves the email data passed from the mailbox monitor DAG.
    """
    # Get the email data from the triggered DAG configuration
    email_data = kwargs['dag_run'].conf.get('email_data', {})
    
    if not email_data:
        logging.warning("No email data received from trigger")
        return None
    
    email_id = email_data.get('id', 'unknown')
    headers = email_data.get('headers', {})
    sender = headers.get('From', 'Unknown')
    subject = headers.get('Subject', 'No Subject')
    attachments = email_data.get('attachments', [])
    
    logging.info(f"Processing email {email_id} from {sender}")
    logging.info(f"Subject: {subject}")
    logging.info(f"Found {len(attachments)} attachment(s)")
    
    cv_data = {
        'email_id': email_id,
        'sender': sender,
        'subject': subject,
        'attachments': []
    }
    
    # Process each attachment
    for attachment in attachments:
        filename = attachment.get('filename', '')
        mime_type = attachment.get('mime_type', '')
        
        logging.info(f"Processing attachment: {filename} (type: {mime_type})")
        
        if mime_type == 'application/pdf':
            extracted_content = attachment.get('extracted_content', {})
            pdf_content = extracted_content.get('content', '')
            pdf_metadata = extracted_content.get('metadata', [])
            
            if pdf_content:
                logging.info(f"Successfully extracted content from PDF: {filename}")
                logging.info(f"Content preview: {pdf_content[:200]}...")
                
                cv_data['attachments'].append({
                    'filename': filename,
                    'type': 'pdf',
                    'content': pdf_content,
                    'metadata': pdf_metadata,
                    'path': attachment.get('path', '')
                })
            else:
                logging.warning(f"No content extracted from PDF: {filename}")
        
        elif mime_type in ['image/png', 'image/jpeg', 'image/jpg']:
            base64_content = attachment.get('base64_content', '')
            
            if base64_content:
                logging.info(f"Image attachment found: {filename}")
                cv_data['attachments'].append({
                    'filename': filename,
                    'type': 'image',
                    'base64_content': base64_content,
                    'path': attachment.get('path', '')
                })
    logging.debug(f"CV Data: {cv_data}")
    
    # Push CV data to XCom for downstream tasks
    kwargs['ti'].xcom_push(key='cv_data', value=cv_data)
    
    return cv_data
def retrive_jd_from_web(**kwargs):
    """
    Retrieve Job Description (JD) from a web source.
    This is a placeholder function and should be implemented to fetch actual JD data.
    """
    cv_data = kwargs['ti'].xcom_pull(task_ids='extract_cv_content', key='cv_data')
    logging.info("Retrieving Job Description from web source...")
    prompt = f"""Identify the jobs matching for this candidate using VectorSearchByUUID tool; give the output in the bellow json format
    ## Candidate Info
    {cv_data}
    ## Output format
    ```json
    {{
    [{{
        "job_name": "<job_title from vector database>",
        "summary": "<short job summary>"
    }}, {{...}}, {{...}}]
    }}
    ```
    """
    MODEL_NAME = Variable.get("ltai.v3.lowtouch.recruitment.model_name", default_var="recruitment:0.3af")
    jd_response = get_ai_response(prompt, stream=False, model=MODEL_NAME)
    logging.info(f"Retrieved Job Description: {jd_response[:500]}...")
    jd_data = extract_json_from_text(jd_response)
    logging.debug(f"Extracted JD Data: {jd_data}")
    # Push JD data to XCom for downstream tasks
    kwargs['ti'].xcom_push(key='jd_data', value=jd_data)
    return jd_data

def get_the_jd_for_cv_analysis(**kwargs):
    """
    Get the Job Description (JD) for CV analysis.
    This function retrieves the JD data from XCom or fetches it if not available.
    """
    ti = kwargs['ti']
    jd_data = ti.xcom_pull(task_ids='retrive_jd_from_web', key='jd_data')
    cv_data = ti.xcom_pull(task_ids='extract_cv_content', key='cv_data')
    prompt = f""" Step 1: Idenify the one best job profile matches for this candidate form the list of job descriptions (Do not call the vector search tool)
    list of job descriptions : {jd_data}
    Candidate CV: {cv_data}
    
    Step 2: Once you have identified the best job profile, Search using VectorSearchByUUID tool with job summary as the query to get the full job description and give the output in the following JSON format.
    
    ## Output format:
    {{  
        "job_title": "Title of the job",
        "job_description": "Full job description text",
        "Must have skills": ["list", "of", "skills"],
        "Nice to have skills": ["list", "of", "skills"],
        "experience_level": "e.g., 1 YEAR, 2 Year, 4 year",
        "location": "Job location",
        "Remote": "Yes or No or Flexible"
    }}
    """
    MODEL_NAME = Variable.get("ltai.v3.lowtouch.recruitment.model_name", default_var="recruitment:0.3af")
    jd_response = get_ai_response(prompt, stream=False, model=MODEL_NAME)
    logging.info(f"Refined Job Description for CV Analysis: {jd_response[:500]}...")
    jd_data = extract_json_from_text(jd_response)
    ti.xcom_push(key='jd_data', value=jd_data)
    logging.debug(f"JD Data for CV Analysis: {jd_data}")
    return jd_data

def get_the_score_for_cv_analysis(**kwargs):
    """
    Get the match score for the CV against the Job Description (JD).
    """
    ti = kwargs['ti']
    cv_data = ti.xcom_pull(task_ids='extract_cv_content', key='cv_data')
    jd_data = ti.xcom_pull(task_ids='get_the_jd_for_cv_analysis', key='jd_data')
    
    if not cv_data or not jd_data:
        logging.warning("Missing CV or JD data for scoring")
        return None
    
    prompt = f"""Match the CV with the Job Description (JD)
    CV: {cv_data}
    JD: {jd_data}
    
    ## Output format
    **Job Title listed in the website**
    <A summary about this job with the years of experence and key skills requiered>
    
    | Must have skills | Pass/Fail |
    | <skill name>  | Candidate has or not (✅ or ❌) |
    
    ---
    
    | Requiered Experence in years | candidate Experence | Pass/Fail |
    | <Requiered Experence in years> | <candidate Experence> | ✅ or ❌ |
    
    
    ```json
    {{
        "job_title": "<job title>",
        "selected": <true or false>,
        "reason": <reason for selection or rejection>,
        
    }}
    ```
    """
    MODEL_NAME = Variable.get("ltai.v3.lowtouch.recruitment.model_name", default_var="recruitment:0.3af")
    score_response = get_ai_response(prompt, stream=False, model=MODEL_NAME)
    logging.info(f"Match Score Response: {score_response}")
    score_data = extract_json_from_text(score_response)
    ti.xcom_push(key='score_data', value=score_data)
    logging.debug(f"Score Data: {score_data}")
    return score_data


def send_response_email(**kwargs):
    """
    Send a response email to the candidate (optional).
    """
    ti = kwargs['ti']
    # if the candate is selcted copose a initial assessment email else send a rejection email
    score_data = ti.xcom_pull(task_ids='get_the_score_for_cv_analysis', key='score_data')
    if not score_data or not score_data.get("selected", False):
        email_response = "Thank you for your application. Unfortunately, we are unable to proceed with your application at this time."
        logging.info("Candidate not selected, sending rejection email.")
        return email_response
    else:
        logging.info("Candidate selected, preparing response email.")
        agent_response_prompt = f"""Compose a response email to by asking the following questions:
        
        """
    email_data = ti.xcom_pull(task_ids='extract_cv_content', key='cv_data')
    analysis_results = ti.xcom_pull(task_ids='analyze_cv', key='analysis_results')
    
    if not email_data or not analysis_results:
        logging.warning("Missing data for sending response email")
        return
    score_data = ti.xcom_pull(task_ids='get_the_score_for_cv_analysis', key='score_data')
    response = f"""
    Dear Candidate,
    Thank you for your application. We have reviewed your CV and would like to share the analysis results with you.
    Here are the key points from our analysis:
    
    We appreciate your interest in joining our team and encourage you to apply for suitable positions in the future.
    Score: {score_data.get("match_score", "N/A")}"""
        
    
    try:
        service = authenticate_gmail(GMAIL_CREDENTIALS, RECRUITMENT_FROM_ADDRESS)
        if not service:
            logging.error("Gmail authentication failed, aborting email response.")
            return "Gmail authentication failed"
        thread_id = email_data.get("threadId", None)
        sender_email = email_data["headers"].get("From", "")
        subject = f"Re: {email_data['headers'].get('Subject', 'Recruitment Agent Report')}"
        in_reply_to = email_data["headers"].get("Message-ID", "")
        references = email_data["headers"].get("References", "")
        result = send_email(service, sender_email, subject, response,
                       original_message_id, references, thread_id=thread_id)

        if result:
            logging.info(f"Email sent successfully to {sender_email}")
            return f"Email sent successfully to {sender_email}"
        else:
            logging.error("Failed to send email")
            return "Failed to send email"
    except Exception as e:
        logging.error(f"Error sending response email: {str(e)}")
        raise f"Error sending response email: {str(e)}"
    logging.info(f"Response email would be sent to: {sender}")
    logging.info(f"Original subject: {subject}")




# Define the DAG
with DAG(
    "cv_analyse",
    default_args=default_args,
    schedule_interval=None,  # Triggered by cv_monitor_mailbox DAG
    catchup=False,
    doc_md="""
    # CV Analysis DAG
    
    This DAG is triggered by the `cv_monitor_mailbox` DAG when new emails with CV attachments are received.
    
    ## Workflow:
    1. Extract CV content from PDF attachments
    2. Analyze CV content to extract candidate information
    3. Store analysis results
    4. (Optional) Send response email to candidate
    
    ## Configuration:
    - Triggered DAG (no schedule)
    - Receives email data via conf parameter
    - Processes PDF attachments with extracted text content
    """,
    tags=["cv", "analysis", "recruitment"]
) as dag:
    extract_cv_task = PythonOperator(
        task_id="extract_cv_content",
        python_callable=extract_cv_content,
        provide_context=True
    )

    retrieve_jd_task = PythonOperator(
        task_id="retrive_jd_from_web",
        python_callable=retrive_jd_from_web,
        provide_context=True
    )

    get_jd_task = PythonOperator(
        task_id="get_the_jd_for_cv_analysis",
        python_callable=get_the_jd_for_cv_analysis,
        provide_context=True
    )

    score_cv_task = PythonOperator(
        task_id="get_the_score_for_cv_analysis",
        python_callable=get_the_score_for_cv_analysis,
        provide_context=True
    )


    send_response_task = PythonOperator(
        task_id="send_response_email",
        python_callable=send_response_email,
        provide_context=True
    )

    # Set task dependencies
    extract_cv_task >> retrieve_jd_task >> get_jd_task >> score_cv_task >> send_response_task