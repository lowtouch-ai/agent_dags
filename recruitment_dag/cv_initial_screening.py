"""
Airflow DAG for analyzing candidate responses to screening questions.
Processes email responses and determines acceptance/rejection for next round.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
import json
import sys
import os
from email.utils import parseaddr
from pathlib import Path
import re

try:
    sys.path.append(os.path.dirname(os.path.realpath(__file__)))
    logging.info(f"Appended to sys.path: {os.path.dirname(os.path.realpath(__file__))}")
except Exception as e:
    logging.error(f"Error appending to sys.path: {e}")

from agent_dags.utils.email_utils import authenticate_gmail, send_email, mark_email_as_read
from agent_dags.utils.agent_utils import get_ai_response, extract_json_from_text

# Configuration constants
GMAIL_CREDENTIALS = Variable.get("ltai.v3.lowtouch.recruitment.email_credentials", default_var=None)
RECRUITMENT_FROM_ADDRESS = Variable.get("ltai.v3.lowtouch.recruitment.from_address", default_var=None)
RECRUITER_EMAIL = Variable.get("ltai.v3.lowtouch.recruitment.recruiter_email", default_var="athira@lowtouch.ai")
RECRUITER_CC_EMAILS = Variable.get("ltai.v3.lowtouch.recruitment.recruiter_cc_emails", default_var=None)

# Default DAG arguments
default_args = {
    "owner": "lowtouch.ai_developers",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 24),
    "retries": 1,
    "retry_delay": timedelta(seconds=15),
}


def extract_candidate_response(**kwargs):
    """
    Extract candidate response from email.
    """
    email_data = kwargs['dag_run'].conf.get('email_data', {})
    
    if not email_data:
        logging.warning("No email data received from trigger")
        return None
    
    email_id = email_data.get('id', 'unknown')
    headers = email_data.get('headers', {})
    sender = headers.get('From', 'Unknown')
    subject = headers.get('Subject', 'No Subject')
    body = email_data.get('body', '')
    
    _, sender_email = parseaddr(sender)
    
    logging.info(f"Processing screening response from {sender_email}")
    logging.info(f"Subject: {subject}")
    
    response_data = {
        'email_id': email_id,
        'sender': sender,
        'sender_email': sender_email,
        'subject': subject,
        'body': body,
        'received_date': email_data.get('date', datetime.now().isoformat())
    }
    
    kwargs['ti'].xcom_push(key='response_data', value=response_data)
    logging.debug(f"Response Data: {response_data}")
    
    return response_data


def load_candidate_profile(**kwargs):
    """
    Load candidate profile from stored JSON file based on email.
    """
    ti = kwargs['ti']
    response_data = ti.xcom_pull(task_ids='extract_candidate_response', key='response_data')
    
    if not response_data:
        logging.warning("No response data available")
        return None
    
    sender_email = response_data.get('sender_email', '')
    
    # Sanitize email for filename
    safe_email = sender_email.replace('@', '_at_').replace('/', '_').replace('\\', '_')
    
    # Load candidate profile from file
    try:
        profile_path = Path(f"/appz/data/recruitment/{safe_email}.json")
        
        if not profile_path.exists():
            logging.error(f"Candidate profile not found: {profile_path}")
            return None
        
        with open(profile_path, 'r', encoding='utf-8') as f:
            candidate_profile = json.load(f)
        
        logging.info(f"Loaded candidate profile for {sender_email}")
        ti.xcom_push(key='candidate_profile', value=candidate_profile)
        
        return candidate_profile
        
    except Exception as e:
        logging.error(f"Failed to load candidate profile: {str(e)}")
        return None


def analyze_screening_responses(**kwargs):
    """
    Analyze candidate responses to screening questions using AI.
    """
    ti = kwargs['ti']
    response_data = ti.xcom_pull(task_ids='extract_candidate_response', key='response_data')
    candidate_profile = ti.xcom_pull(task_ids='load_candidate_profile', key='candidate_profile')
    
    if not response_data or not candidate_profile:
        logging.warning("Missing response or candidate profile data")
        return None
    
    candidate_responses = response_data.get('body', '')
    
    # Get original CV content: Prioritize from thread_history in conf, fallback to profile
    email_data = kwargs['dag_run'].conf.get('email_data', {})
    thread_history = email_data.get('thread_history', {})
    original_cv_content = thread_history.get('original_cv_content', '')
    
    if not original_cv_content:
        original_cv_content = candidate_profile.get('original_cv_content', '')
        if original_cv_content:
            logging.info("Using CV content from saved profile")
        else:
            logging.warning("No original CV content found in history or profile")

    prompt = f"""Analyze the candidate's responses to the screening questions and determine if they should proceed to the next round.

## Original Resume (CV) Content:
{original_cv_content}    

## Candidate Profile:
{json.dumps(candidate_profile, indent=2)}

## Candidate's Responses:
{candidate_responses}

## Evaluation Criteria:
1. Work arrangement compatibility (full-time/part-time/remote/hybrid)
2. Notice period or availability to start
3. Salary expectations alignment
4. Location/relocation willingness
5. Interest and motivation for the role
6. Technical skills and experience relevance
7. Education and certifications

## Output format:
```json
{{
    "decision": "<ACCEPT or REJECT>",
    "overall_score": <0-100>,
    "evaluation": {{
        "work_arrangement": {{
            "score": <0-100>,
            "comment": "<brief comment>"
        }},
        "availability": {{
            "score": <0-100>,
            "comment": "<brief comment>"
        }},
        "salary_expectations": {{
            "score": <0-100>,
            "comment": "<brief comment>"
        }},
        "location": {{
            "score": <0-100>,
            "comment": "<brief comment>"
        }},
        "motivation": {{
            "score": <0-100>,
            "comment": "<brief comment>"
        }},
        "technical_fit": {{
            "score": <0-100>,
            "comment": "<brief comment>"
        }},
        "qualifications": {{
            "score": <0-100>,
            "comment": "<brief comment>"
        }}
    }},
    "strengths": ["<strength 1>", "<strength 2>", "..."],
    "concerns": ["<concern 1>", "<concern 2>", "..."],
    "detailed_reason": "<detailed explanation for accept/reject decision>",
    "next_steps": "<recommended next steps if accepted>"
}}
```
"""
    
    MODEL_NAME = Variable.get("ltai.v3.lowtouch.recruitment.model_name", default_var="recruitment:0.3af")
    analysis_response = get_ai_response(prompt, stream=False, model=MODEL_NAME)
    
    logging.info(f"Screening Analysis Response: {analysis_response}")
    
    analysis_data = extract_json_from_text(analysis_response)
    
    ti.xcom_push(key='analysis_data', value=analysis_data)
    logging.debug(f"Analysis Data: {analysis_data}")
    
    return analysis_data


def update_candidate_profile(**kwargs):
    """
    Update candidate profile with screening analysis results.
    """
    ti = kwargs['ti']
    response_data = ti.xcom_pull(task_ids='extract_candidate_response', key='response_data')
    analysis_data = ti.xcom_pull(task_ids='analyze_screening_responses', key='analysis_data')
    candidate_profile = ti.xcom_pull(task_ids='load_candidate_profile', key='candidate_profile')
    
    if not all([response_data, analysis_data, candidate_profile]):
        logging.warning("Missing data for profile update")
        return None
    
    sender_email = response_data.get('sender_email', '')
    safe_email = sender_email.replace('@', '_at_').replace('/', '_').replace('\\', '_')
    
    # Update profile with screening results
    updated_profile = {
        **candidate_profile,
        'screening_stage': {
            'completed': True,
            'completion_date': datetime.now().isoformat(),
            'responses': response_data.get('body', ''),
            'analysis': analysis_data,
            'decision': analysis_data.get('decision', 'PENDING'),
            'overall_score': analysis_data.get('overall_score', 0)
        }
    }
    
    try:
        profile_path = Path(f"/appz/data/recruitment/{safe_email}.json")
        
        with open(profile_path, 'w', encoding='utf-8') as f:
            json.dump(updated_profile, f, indent=2, ensure_ascii=False)
        
        logging.info(f"Updated candidate profile for {sender_email}")
        ti.xcom_push(key='updated_profile', value=updated_profile)
        
        return updated_profile
        
    except Exception as e:
        logging.error(f"Failed to update candidate profile: {str(e)}")
        return None


def send_screening_result_email(**kwargs):
    """
    Send email to candidate with screening results (acceptance or rejection).
    """
    ti = kwargs['ti']
    email_data = kwargs['dag_run'].conf.get('email_data', {})
    analysis_data = ti.xcom_pull(task_ids='analyze_screening_responses', key='analysis_data')
    response_data = ti.xcom_pull(task_ids='extract_candidate_response', key='response_data')
    
    if not all([email_data, analysis_data, response_data]):
        logging.warning("Missing data for sending result email")
        return "Missing data for email"
    
    headers = email_data.get('headers', {})
    sender = headers.get('From', 'Unknown')
    subject = headers.get('Subject', 'No Subject')
    thread_id = email_data.get('threadId')
    original_message_id = headers.get('Message-ID', '')
    references = headers.get('References', '')
    
    if original_message_id and original_message_id not in references:
        references = f"{references} {original_message_id}".strip()
    
    _, sender_email = parseaddr(sender)
    
    decision = analysis_data.get('decision', 'PENDING')
    
    MODEL_NAME = Variable.get("ltai.v3.lowtouch.recruitment.model_name", default_var="recruitment:0.3af")
    
    if decision == 'REJECT':
        # Rejection email
        email_prompt = f"""Compose a professional rejection email for a candidate who didn't pass the screening stage.

## Analysis Results:
{json.dumps(analysis_data, indent=2)}

## Email Requirements:
- Thank them for their detailed responses
- Politely inform them they won't be moving forward
- Be respectful and encouraging
- Keep it professional and brief
- Avoid specific reasons (keep it general)
- Wish them success in their job search

Output clean HTML for the email body using proper tags (<p>, <h2>, etc.). Make it professional and compassionate. Use 'Dear Candidate' as greeting.
"""
    else:
        # Acceptance email - invite to next round (interview)
        email_prompt = f"""Compose a professional acceptance email for a candidate who passed the screening stage.

## Analysis Results:
{json.dumps(analysis_data, indent=2)}

## Email Requirements:
- Congratulate them on passing the initial screening
- Mention they'll be moving to the interview stage
- Provide next steps: HR will contact them within 3-5 business days to schedule an interview
- Express enthusiasm about their profile
- Keep it professional yet welcoming
- Mention they can reach out with any questions

Output clean HTML for the email body using proper tags (<p>, <h2>, etc.). Make it professional and encouraging. Use 'Dear Candidate' as greeting.
"""
    
    response = get_ai_response(email_prompt, stream=False, model=MODEL_NAME)
    
    # Clean HTML from response
    match = re.search(r'```html.*?\n(.*?)```', response, re.DOTALL)
    body = match.group(1).strip() if match else response.strip()
    
    if not body.strip().startswith('<!DOCTYPE') and not body.strip().startswith('<html'):
        body = body.strip()
    
    # Authenticate
    service = authenticate_gmail(GMAIL_CREDENTIALS, RECRUITMENT_FROM_ADDRESS)
    if not service:
        logging.error("Gmail authentication failed")
        return "Gmail authentication failed"
    
    subject = f"Re: {subject}"
    
    result = send_email(
        service, 
        sender_email, 
        subject, 
        body, 
        original_message_id, 
        references,
        RECRUITMENT_FROM_ADDRESS, 
        cc=None, 
        bcc=None, 
        thread_id=thread_id
    )
    
    if result:
        logging.info(f"Screening result email sent to {sender_email} (Decision: {decision})")
        return f"Email sent successfully to {sender_email}"
    else:
        logging.error("Failed to send screening result email")
        return "Failed to send email"


def notify_recruiter_for_interview(**kwargs):
    """
    Send email to recruiter (Athira) to schedule an interview call
    with the candidate if the screening decision is ACCEPT.
    """
    ti = kwargs['ti']
    analysis_data = ti.xcom_pull(task_ids='analyze_screening_responses', key='analysis_data')
    response_data = ti.xcom_pull(task_ids='extract_candidate_response', key='response_data')
    candidate_profile = ti.xcom_pull(task_ids='load_candidate_profile', key='candidate_profile')

    if not all([analysis_data, response_data]):
        logging.warning("Missing data for recruiter notification")
        return "Missing data - skipped"

    decision = analysis_data.get('decision', 'PENDING')

    if decision != 'ACCEPT':
        logging.info(f"Decision is {decision} - skipping recruiter notification")
        return f"Skipped - decision is {decision}"

    sender_email = response_data.get('sender_email', 'Unknown')
    candidate_name = candidate_profile.get('candidate_name', 'Unknown Candidate') if candidate_profile else 'Unknown Candidate'
    overall_score = candidate_profile.get('total_score', 'N/A')
    position = candidate_profile.get('job_title','N/A') if candidate_profile else 'N/A'

    body = f"""
    <h2>Interview Scheduling Request</h2>
    <p>Hi Athira,</p>
    <p>A candidate has passed the initial screening and is ready for an interview. Please schedule an interview call at your earliest convenience.</p>

    <h3>Candidate Details:</h3>
    <ul>
        <li><strong>Name:</strong> {candidate_name}</li>
        <li><strong>Email:</strong> {sender_email}</li>
        <li><strong>Position:</strong> {position}</li>
        <li><strong>Screening Score:</strong> {overall_score}</li>
    </ul>

    <p>Please reach out to the candidate to set up an interview call.</p>
    <p>Best regards,<br>Recruitment Automation System</p>
    """

    service = authenticate_gmail(GMAIL_CREDENTIALS, RECRUITMENT_FROM_ADDRESS)
    if not service:
        logging.error("Gmail authentication failed for recruiter notification")
        return "Gmail authentication failed"

    subject = f"Interview Scheduling Request - {candidate_name} ({position})"

    result = send_email(
        service,
        RECRUITER_EMAIL,
        subject,
        body,
        None,
        None,
        RECRUITMENT_FROM_ADDRESS,
        cc=RECRUITER_CC_EMAILS,
        bcc=None,
        thread_id=None
    )

    if result:
        logging.info(f"Recruiter notification sent to {RECRUITER_EMAIL} (CC: {RECRUITER_CC_EMAILS}) for candidate {sender_email}")
        return f"Recruiter notified for interview with {sender_email}"
    else:
        logging.error("Failed to send recruiter notification email")
        return "Failed to send recruiter notification"


# Define the DAG
with DAG( 
    "screening_response_analysis",
    default_args=default_args,
    schedule=None,  # Triggered by mailbox monitor
    catchup=False,
    doc_md="""
    # Screening Response Analysis DAG
    
    This DAG processes candidate responses to initial screening questions.
    
    ## Workflow:
    1. Extract candidate response from email
    2. Load existing candidate profile from storage
    3. Analyze responses against job requirements
    4. Update candidate profile with screening results
    5. Send acceptance/rejection email based on analysis
    
    ## Configuration:
    - Triggered DAG (no schedule)
    - Receives email data via conf parameter
    - Loads candidate profile from /appz/data/recruitment/
    - Updates profile with screening stage results
    
    ## Decision Criteria:
    - Work arrangement compatibility
    - Availability and notice period
    - Salary expectations
    - Location/relocation willingness
    - Technical skills alignment
    - Motivation and interest
    - Educational qualifications
    """,
    tags=["screening", "recruitment", "analysis","cv"]
) as dag:
    
    extract_response_task = PythonOperator(
        task_id="extract_candidate_response",
        python_callable=extract_candidate_response,
        
    )
    
    load_profile_task = PythonOperator(
        task_id="load_candidate_profile",
        python_callable=load_candidate_profile,
        
    )
    
    analyze_responses_task = PythonOperator(
        task_id="analyze_screening_responses",
        python_callable=analyze_screening_responses,
        
    )
    
    update_profile_task = PythonOperator(
        task_id="update_candidate_profile",
        python_callable=update_candidate_profile,
        
    )
    
    send_result_task = PythonOperator(
        task_id="send_screening_result_email",
        python_callable=send_screening_result_email,

    )

    notify_recruiter_task = PythonOperator(
        task_id="notify_recruiter_for_interview",
        python_callable=notify_recruiter_for_interview,

    )

    # Set task dependencies
    extract_response_task >> load_profile_task >> analyze_responses_task >> update_profile_task >> send_result_task >> notify_recruiter_task