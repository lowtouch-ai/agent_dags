"""
Airflow DAG for monitoring mailbox for CV submissions and screening responses.
Intelligently routes emails to appropriate DAGs based on content analysis.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
from email.utils import parseaddr
from pathlib import Path
import json

# Import utility functions
from agent_dags.utils.email_utils import (
    authenticate_gmail,
    fetch_unread_emails_with_attachments,
    get_last_checked_timestamp,
    update_last_checked_timestamp
)
from agent_dags.utils.agent_utils import get_ai_response, extract_json_from_text

# Configuration constants
GMAIL_CREDENTIALS = Variable.get("ltai.v3.lowtouch.recruitment.email_credentials", default_var=None)
RECRUITMENT_FROM_ADDRESS = Variable.get("ltai.v3.lowtouch.recruitment.from_address", default_var=None)
LAST_PROCESSED_FILE = "/appz/cache/cv_last_processed_email.json"
ATTACHMENT_DIR = "/appz/data/cv_attachments/"
CANDIDATE_DATA_DIR = "/appz/data/recruitment/"

# Default DAG arguments
default_args = {
    "owner": "lowtouch.ai_developers",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 24),
    "retries": 1,
    "retry_delay": timedelta(seconds=15),
}


def fetch_cv_emails(**kwargs):
    """
    Fetch unread emails with CV attachments from Gmail.
    Processes emails received after the last processed timestamp.
    """
    try:
        # Authenticate Gmail service
        service = authenticate_gmail(GMAIL_CREDENTIALS, RECRUITMENT_FROM_ADDRESS)
        if not service:
            logging.error("Gmail authentication failed")
            return []
        
        # Get last processed timestamp
        last_checked = get_last_checked_timestamp(LAST_PROCESSED_FILE)
        logging.info(f"Last checked timestamp: {last_checked}")
        
        # Build custom query with timestamp and unread filter
        query = f"is:unread after:{last_checked // 1000}"
        logging.info(f"Using query: {query}")
        
        # Fetch unread emails with attachments
        unread_emails = fetch_unread_emails_with_attachments(
            service=service,
            attachment_dir=ATTACHMENT_DIR,
            query=query,
            last_checked_timestamp=last_checked
        )
        
        logging.info(f"Found {len(unread_emails)} unread emails with attachments")
        
        # Update last processed timestamp if emails were found
        if unread_emails:
            max_timestamp = max(email['timestamp'] for email in unread_emails)
            update_last_checked_timestamp(LAST_PROCESSED_FILE, max_timestamp)
            logging.info(f"Updated last processed timestamp to: {max_timestamp}")
        
        # Push to XCom for downstream tasks
        kwargs['ti'].xcom_push(key='unread_emails', value=unread_emails)
        
        return unread_emails
        
    except Exception as e:
        logging.error(f"Error fetching CV emails: {str(e)}")
        raise


def classify_email_type(**kwargs):
    """
    Classify each email using AI to determine if it's:
    1. New CV submission (trigger cv_analyse)
    2. Screening response (trigger screening_response_analysis)
    3. Other (log and skip)
    """
    ti = kwargs['ti']
    unread_emails = ti.xcom_pull(task_ids='fetch_cv_emails', key='unread_emails')
    
    if not unread_emails:
        logging.info("No emails to classify")
        ti.xcom_push(key='classified_emails', value=[])
        return []
    
    MODEL_NAME = Variable.get("ltai.v3.lowtouch.recruitment.model_name", default_var="recruitment:0.3af")
    classified_emails = []
    
    for email in unread_emails:
        try:
            email_id = email.get('id', 'unknown')
            headers = email.get('headers', {})
            sender = headers.get('From', 'Unknown')
            _, sender_email = parseaddr(sender)
            subject = headers.get('Subject', '')
            body = email.get('body', '')
            attachments = email.get('attachments', [])
            has_cv_attachment = any(
                att.get('mime_type') == 'application/pdf' or 
                att.get('mime_type', '').startswith('image/') 
                for att in attachments
            )
            
            # Check if candidate profile exists
            safe_email = sender_email.replace('@', '_at_').replace('/', '_').replace('\\', '_')
            profile_path = Path(f"{CANDIDATE_DATA_DIR}{safe_email}.json")
            candidate_exists = profile_path.exists()
            
            # Load profile if exists
            candidate_profile = None
            if candidate_exists:
                try:
                    with open(profile_path, 'r', encoding='utf-8') as f:
                        candidate_profile = json.load(f)
                except Exception as e:
                    logging.warning(f"Failed to load candidate profile: {str(e)}")
            
            logging.info(f"Classifying email from {sender_email}")
            logging.info(f"Has CV attachment: {has_cv_attachment}, Candidate exists: {candidate_exists}")
            
            # Build classification prompt
            prompt = f"""Analyze this email and determine its type for recruitment workflow routing.

## Email Details:
- From: {sender_email}
- Subject: {subject}
- Has PDF/Image Attachments: {has_cv_attachment}
- Body: {body}

## Candidate Status:
- Existing Candidate: {candidate_exists}
"""
            
            if candidate_profile:
                screening_completed = candidate_profile.get('screening_stage', {}).get('completed', False)
                prompt += f"""- Screening Stage Completed: {screening_completed}
- Previous Job Applied: {candidate_profile.get('job_title', 'Unknown')}
"""
            
            prompt += """
## Classification Rules:
1. **NEW_CV_APPLICATION**: 
   - Email contains CV/resume attachment (PDF or image)
   - Candidate is applying for the first time OR
   - Candidate is reapplying with a new CV attachment
   
2. **SCREENING_RESPONSE**: 
   - Email is a reply/response from an existing candidate
   - No CV attachment (or CV is same as before)
   - Email body contains answers to questions
   - Candidate profile exists in system
   
3. **OTHER**: 
   - Doesn't fit above categories
   - Follow-up questions
   - General inquiries

## Output Format:
```json
{
    "email_type": "<NEW_CV_APPLICATION or SCREENING_RESPONSE or OTHER>",
    "confidence": <0-100>,
    "reasoning": "<brief explanation for classification>",
    "target_dag": "<cv_analyse or screening_response_analysis or none>"
}
```
"""
            
            # Get AI classification
            classification_response = get_ai_response(prompt, stream=False, model=MODEL_NAME)
            classification = extract_json_from_text(classification_response)
            
            email_type = classification.get('email_type', 'OTHER')
            confidence = classification.get('confidence', 0)
            reasoning = classification.get('reasoning', 'No reasoning provided')
            target_dag = classification.get('target_dag', 'none')
            
            logging.info(f"Email {email_id} classified as: {email_type} (confidence: {confidence}%)")
            logging.info(f"Reasoning: {reasoning}")
            logging.info(f"Target DAG: {target_dag}")
            
            # Add classification to email data
            email['classification'] = {
                'type': email_type,
                'confidence': confidence,
                'reasoning': reasoning,
                'target_dag': target_dag,
                'candidate_exists': candidate_exists
            }
            
            classified_emails.append(email)
            
        except Exception as e:
            logging.error(f"Error classifying email {email.get('id')}: {str(e)}")
            # Default to NEW_CV_APPLICATION if classification fails and has attachment
            email['classification'] = {
                'type': 'NEW_CV_APPLICATION' if has_cv_attachment else 'OTHER',
                'confidence': 50,
                'reasoning': f'Classification failed: {str(e)}',
                'target_dag': 'cv_analyse' if has_cv_attachment else 'none',
                'candidate_exists': candidate_exists
            }
            classified_emails.append(email)
    
    # Push classified emails to XCom
    ti.xcom_push(key='classified_emails', value=classified_emails)
    logging.info(f"Classified {len(classified_emails)} emails")
    
    return classified_emails


def check_for_emails(**kwargs):
    """
    Branch function to determine next task based on email presence.
    Returns task_id to execute next.
    """
    ti = kwargs['ti']
    classified_emails = ti.xcom_pull(task_ids='classify_email_type', key='classified_emails')
    
    if classified_emails and len(classified_emails) > 0:
        logging.info(f"Found {len(classified_emails)} emails, proceeding to route them")
        return 'route_emails_to_dags'
    else:
        logging.info("No new emails found")
        return 'no_emails_found'


def route_emails_to_dags(**kwargs):
    """
    Route emails to appropriate DAGs based on classification.
    Triggers cv_analyse for new applications and screening_response_analysis for responses.
    """
    ti = kwargs['ti']
    classified_emails = ti.xcom_pull(task_ids='classify_email_type', key='classified_emails')
    
    if not classified_emails:
        logging.info("No emails to route")
        return
    
    routing_summary = {
        'cv_analyse': 0,
        'screening_response_analysis': 0,
        'skipped': 0
    }
    
    for email in classified_emails:
        try:
            email_id = email.get('id', 'unknown')
            classification = email.get('classification', {})
            target_dag = classification.get('target_dag', 'none')
            email_type = classification.get('type', 'OTHER')
            sender = email.get('headers', {}).get('From', 'Unknown')
            
            if target_dag == 'none' or email_type == 'OTHER':
                logging.info(f"Skipping email {email_id} (type: {email_type})")
                routing_summary['skipped'] += 1
                continue
            
            # Create unique task_id for each trigger
            task_id = f"trigger_{target_dag}_{email_id.replace('-', '_')}"
            
            logging.info(f"Routing email {email_id} from {sender} to DAG: {target_dag}")
            logging.info(f"Classification: {email_type} (confidence: {classification.get('confidence')}%)")
            
            # Create and execute trigger operator
            trigger_task = TriggerDagRunOperator(
                task_id=task_id,
                trigger_dag_id=target_dag,
                conf={'email_data': email},
                wait_for_completion=False
            )
            
            trigger_task.execute(context=kwargs)
            routing_summary[target_dag] += 1
            
            logging.info(f"Successfully triggered {target_dag} for email {email_id}")
            
        except Exception as e:
            logging.error(f"Error routing email {email.get('id')}: {str(e)}")
            routing_summary['skipped'] += 1
            continue
    
    # Log routing summary
    logging.info("=" * 60)
    logging.info("Email Routing Summary:")
    logging.info(f"  - New CV Applications (cv_analyse): {routing_summary['cv_analyse']}")
    logging.info(f"  - Screening Responses (screening_response_analysis): {routing_summary['screening_response_analysis']}")
    logging.info(f"  - Skipped/Other: {routing_summary['skipped']}")
    logging.info("=" * 60)
    
    ti.xcom_push(key='routing_summary', value=routing_summary)
    
    return routing_summary


def log_no_emails(**kwargs):
    """
    Log message when no new emails are found.
    """
    logging.info("No new recruitment emails found in this polling cycle")
    return "No emails to process"


# Define the DAG
with DAG(
    "cv_monitor_mailbox",
    default_args=default_args,
    schedule_interval=timedelta(minutes=2),  # Check every 2 minutes
    catchup=False,
    doc_md="""
    # Smart CV Mailbox Monitor DAG
    
    This DAG monitors a Gmail mailbox for incoming recruitment emails and intelligently 
    routes them to the appropriate processing DAG.
    
    ## Workflow:
    1. Fetch unread emails with attachments from Gmail
    2. Classify each email using AI:
       - NEW_CV_APPLICATION: First-time CV submission or reapplication
       - SCREENING_RESPONSE: Response to screening questions
       - OTHER: General inquiries or follow-ups
    3. Route emails to appropriate DAGs:
       - `cv_analyse`: For new CV applications
       - `screening_response_analysis`: For screening responses
       - Skip: For other types
    
    ## Classification Logic:
    - Uses AI to analyze email content, attachments, and candidate history
    - Checks if candidate profile exists in system
    - Considers attachment types and email body content
    - Provides confidence scores and reasoning for each classification
    
    ## Configuration:
    - Polls mailbox every 2 minutes
    - Processes only emails received after last check
    - Extracts PDF and image attachments
    - Maintains candidate profile database at /appz/data/recruitment/
    
    ## Required Variables:
    - ltai.v3.lowtouch.recruitment.email_credentials
    - ltai.v3.lowtouch.recruitment.from_address
    - ltai.v3.lowtouch.recruitment.model_name
    
    ## Triggered DAGs:
    - cv_analyse: Processes new CV submissions
    - screening_response_analysis: Processes screening question responses
    """,
    tags=["cv", "monitor", "mailbox", "recruitment", "intelligent-routing"]
) as dag:
    
    # Task 1: Fetch emails with attachments
    fetch_emails_task = PythonOperator(
        task_id='fetch_cv_emails',
        python_callable=fetch_cv_emails,
        provide_context=True
    )
    
    # Task 2: Classify emails using AI
    classify_emails_task = PythonOperator(
        task_id='classify_email_type',
        python_callable=classify_email_type,
        provide_context=True
    )
    
    # Task 3: Branch based on email presence
    branch_task = BranchPythonOperator(
        task_id='check_for_emails',
        python_callable=check_for_emails,
        provide_context=True
    )
    
    # Task 4a: Route emails to appropriate DAGs
    route_emails_task = PythonOperator(
        task_id='route_emails_to_dags',
        python_callable=route_emails_to_dags,
        provide_context=True
    )
    
    # Task 4b: Log no emails found
    no_emails_task = PythonOperator(
        task_id='no_emails_found',
        python_callable=log_no_emails,
        provide_context=True
    )
    
    # Set task dependencies
    fetch_emails_task >> classify_emails_task >> branch_task
    branch_task >> [route_emails_task, no_emails_task]