"""
Failure alerting for recruitment pipeline DAGs.
Sends email alerts via Gmail API when tasks fail after all retries are exhausted.
"""

import logging
from email.utils import parseaddr
from airflow.sdk import Variable

from agent_dags.utils.email_utils import authenticate_gmail, send_email


def _extract_candidate_info(context):
    """
    Best-effort extraction of candidate info from Airflow context.
    Returns a dict with whatever candidate data is available.
    """
    info = {
        'candidate_email': None,
        'candidate_name': None,
        'sender': None,
        'subject': None,
    }

    try:
        dag_run = context.get('dag_run')
        if dag_run and dag_run.conf:
            email_data = dag_run.conf.get('email_data', {})
            if email_data:
                headers = email_data.get('headers', {})
                info['sender'] = headers.get('From', None)
                info['subject'] = headers.get('Subject', None)
                info['candidate_email'] = (
                    email_data.get('extracted_candidate_email')
                    or email_data.get('search_email')
                )
                if info['sender'] and not info['candidate_email']:
                    _, parsed = parseaddr(info['sender'])
                    if parsed:
                        info['candidate_email'] = parsed
    except Exception as e:
        logging.warning(f"Could not extract candidate info from dag_run conf: {e}")

    try:
        ti = context.get('task_instance')
        if ti:
            score_data = ti.xcom_pull(task_ids='get_the_score_for_cv_analysis', key='score_data')
            if score_data:
                info['candidate_name'] = score_data.get('candidate_name')
                info['candidate_email'] = info['candidate_email'] or score_data.get('candidate_email')

            response_data = ti.xcom_pull(task_ids='extract_candidate_response', key='response_data')
            if response_data:
                info['candidate_email'] = info['candidate_email'] or response_data.get('sender_email')
                info['subject'] = info['subject'] or response_data.get('subject')

            classified = ti.xcom_pull(task_ids='classify_email_type', key='classified_emails')
            if classified and isinstance(classified, list):
                emails_summary = []
                for em in classified[:5]:
                    e_email = em.get('search_email') or em.get('headers', {}).get('From', '?')
                    emails_summary.append(e_email)
                if emails_summary:
                    info['candidate_email'] = info['candidate_email'] or ', '.join(emails_summary)
    except Exception as e:
        logging.warning(f"Could not extract candidate info from XCom: {e}")

    return info


def recruitment_failure_callback(context):
    """
    on_failure_callback for all recruitment DAG tasks.
    Sends an alert email via Gmail API to the recruiter.
    """
    try:
        ti = context.get('task_instance')
        dag_id = ti.dag_id if ti else 'unknown'
        task_id = ti.task_id if ti else 'unknown'
        execution_date = context.get('execution_date', 'unknown')
        exception = context.get('exception')
        exception_str = str(exception) if exception else 'No exception details'
        log_url = ti.log_url if ti else 'N/A'
        try_number = ti.try_number if ti else 'N/A'

        candidate_info = _extract_candidate_info(context)

        candidate_section = ""
        if any(v for v in candidate_info.values()):
            candidate_section = "<h3>Candidate Context:</h3><ul>"
            if candidate_info['candidate_name']:
                candidate_section += f"<li><strong>Name:</strong> {candidate_info['candidate_name']}</li>"
            if candidate_info['candidate_email']:
                candidate_section += f"<li><strong>Email:</strong> {candidate_info['candidate_email']}</li>"
            if candidate_info['sender']:
                candidate_section += f"<li><strong>Original Sender:</strong> {candidate_info['sender']}</li>"
            if candidate_info['subject']:
                candidate_section += f"<li><strong>Email Subject:</strong> {candidate_info['subject']}</li>"
            candidate_section += "</ul>"

        subject = f"[Recruitment Pipeline ALERT] {dag_id} / {task_id} failed"

        body = f"""
        <h2 style="color: #cc0000;">Recruitment Pipeline Task Failure</h2>
        <h3>Task Details:</h3>
        <ul>
            <li><strong>DAG:</strong> {dag_id}</li>
            <li><strong>Task:</strong> {task_id}</li>
            <li><strong>Execution Date:</strong> {execution_date}</li>
            <li><strong>Try Number:</strong> {try_number}</li>
        </ul>
        <h3>Error:</h3>
        <pre style="background: #f4f4f4; padding: 10px; border-radius: 4px;">{exception_str[:1000]}</pre>
        {candidate_section}
        <p><a href="{log_url}">View Full Logs in Airflow</a></p>
        <hr>
        <p style="color: #666; font-size: 12px;">This is an automated alert from the Recruitment Pipeline.</p>
        """

        gmail_creds = Variable.get("ltai.v3.lowtouch.recruitment.email_credentials", default=None)
        from_address = Variable.get("ltai.v3.lowtouch.recruitment.from_address", default=None)
        recruiter_email = Variable.get("ltai.v3.lowtouch.recruitment.recruiter_email", default="athira@lowtouch.ai")

        if not gmail_creds or not from_address:
            logging.error("Cannot send failure alert: missing Gmail credentials or from_address")
            return

        service = authenticate_gmail(gmail_creds, from_address)
        if not service:
            logging.error("Cannot send failure alert: Gmail authentication failed")
            return

        result = send_email(
            service=service,
            recipient=recruiter_email,
            subject=subject,
            body=body,
            in_reply_to=None,
            references=None,
            from_address=from_address,
            cc=None,
            bcc=None,
            thread_id=None,
        )

        if result:
            logging.info(f"Failure alert sent to {recruiter_email} for {dag_id}/{task_id}")
        else:
            logging.error(f"Failed to send failure alert email for {dag_id}/{task_id}")

    except Exception as e:
        logging.error(f"recruitment_failure_callback itself failed: {e}", exc_info=True)
