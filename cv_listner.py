"""
Airflow DAG for monitoring mailbox for CV submissions.
Checks for new emails with CV attachments and triggers the CV analysis DAG.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
from email.utils import parseaddr

# Import utility functions
from agent_dags.utils.email_utils import (
    authenticate_gmail,
    fetch_unread_emails_with_attachments,
    get_last_checked_timestamp,
    update_last_checked_timestamp
)

# Configuration constants
GMAIL_CREDENTIALS = Variable.get("ltai.v3.lowtouch.recruitment.email_credentials", default_var=None)
RECRUITMENT_FROM_ADDRESS = Variable.get("ltai.v3.lowtouch.recruitment.from_address", default_var=None)
LAST_PROCESSED_FILE = "/appz/cache/cv_last_processed_email.json"
ATTACHMENT_DIR = "/appz/data/cv_attachments/"

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
        
        # Fetch unread emails with attachments
        unread_emails = fetch_unread_emails_with_attachments(
            service=service,
            last_checked_timestamp=last_checked,
            attachment_dir=ATTACHMENT_DIR
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


def check_for_emails(**kwargs):
    """
    Branch function to determine next task based on email presence.
    Returns task_id to execute next.
    """
    ti = kwargs['ti']
    unread_emails = ti.xcom_pull(task_ids='fetch_cv_emails', key='unread_emails')
    
    if unread_emails and len(unread_emails) > 0:
        logging.info(f"Found {len(unread_emails)} emails, proceeding to trigger CV analysis")
        return 'trigger_cv_analysis'
    else:
        logging.info("No new emails found")
        return 'no_emails_found'


def trigger_cv_analysis_tasks(**kwargs):
    """
    Trigger the CV analysis DAG for each email with CV attachments.
    Creates separate DAG runs for parallel processing.
    """
    ti = kwargs['ti']
    unread_emails = ti.xcom_pull(task_ids='fetch_cv_emails', key='unread_emails')
    
    if not unread_emails:
        logging.info("No emails to process")
        return
    
    triggered_count = 0
    
    for email in unread_emails:
        try:
            # Create unique task_id for each trigger
            task_id = f"trigger_cv_analysis_{email['id'].replace('-', '_')}"
            
            logging.info(f"Triggering CV analysis for email {email['id']} from {email['headers'].get('From', 'Unknown')}")
            logging.info(f"Email has {len(email['attachments'])} attachment(s)")
            
            # Create and execute trigger operator
            trigger_task = TriggerDagRunOperator(
                task_id=task_id,
                trigger_dag_id='cv_analyse',
                conf={'email_data': email},
                wait_for_completion=False
            )
            
            trigger_task.execute(context=kwargs)
            triggered_count += 1
            
        except Exception as e:
            logging.error(f"Error triggering CV analysis for email {email['id']}: {str(e)}")
            continue
    
    logging.info(f"Successfully triggered CV analysis for {triggered_count} emails")
    return triggered_count


def log_no_emails(**kwargs):
    """
    Log message when no new emails are found.
    """
    logging.info("No new CV emails found in this polling cycle")
    return "No emails to process"


# Define the DAG
with DAG(
    "cv_monitor_mailbox",
    default_args=default_args,
    schedule_interval=timedelta(minutes=2),  # Check every 2 minutes
    catchup=False,
    doc_md="""
    # CV Mailbox Monitor DAG
    
    This DAG monitors a Gmail mailbox for incoming CV submissions and triggers 
    the CV analysis DAG for processing.
    
    ## Workflow:
    1. Fetch unread emails with attachments from Gmail
    2. Check if any emails were found
    3. If emails found: Trigger CV analysis DAG for each email
    4. If no emails: Log and end
    
    ## Configuration:
    - Polls mailbox every 2 minutes
    - Processes only emails received after last check
    - Extracts PDF and image attachments
    - Triggers separate CV analysis DAG run for each email
    
    ## Required Variables:
    - ltai.v3.lowtouch.recruitment.email_credentials
    - ltai.v3.lowtouch.recruitment.from_address
    """,
    tags=["cv", "monitor", "mailbox", "recruitment"]
) as dag:
    
    # Task 1: Fetch emails with CV attachments
    fetch_emails_task = PythonOperator(
        task_id='fetch_cv_emails',
        python_callable=fetch_cv_emails,
        provide_context=True
    )
    
    # Task 2: Branch based on email presence
    branch_task = BranchPythonOperator(
        task_id='check_for_emails',
        python_callable=check_for_emails,
        provide_context=True
    )
    
    # Task 3a: Trigger CV analysis for each email
    trigger_analysis_task = PythonOperator(
        task_id='trigger_cv_analysis',
        python_callable=trigger_cv_analysis_tasks,
        provide_context=True
    )
    
    # Task 3b: Log no emails found
    no_emails_task = PythonOperator(
        task_id='no_emails_found',
        python_callable=log_no_emails,
        provide_context=True
    )
    
    # Set task dependencies
    fetch_emails_task >> branch_task
    branch_task >> [trigger_analysis_task, no_emails_task]