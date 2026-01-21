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
from email.utils import parseaddr
import statistics
import re

try:
    sys.path.append(os.path.dirname(os.path.realpath(__file__)))
    logging.info(f"Appended to sys.path: {os.path.dirname(os.path.realpath(__file__))}")
except Exception as e:
    logging.error(f"Error appending to sys.path: {e}")

# Import your email utilities if needed
from agent_dags.utils.email_utils import authenticate_gmail, send_email, mark_email_as_read
from agent_dags.utils.agent_utils import get_ai_response, extract_json_from_text

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

def calculate_candidate_score(analysis_json):
    """
    Calculate the candidate score based on the provided analysis JSON.
    Args:
    analysis_json (dict): The JSON object from the recruitment AI response.
    Returns:
    dict: A dictionary containing the scores and eligibility status.
    """
    # Extract data from JSON
    must_have_skills = analysis_json.get('must_have_skills', [])
    nice_to_have_skills = analysis_json.get('nice_to_have_skills', [])
    experience_match = analysis_json.get('experience_match', {})
    education_match = analysis_json.get('education_match', {})
    # Score arrays
    must_have_scores = []
    nice_to_have_scores = []
    other_scores = []
    # Track ineligibility reasons
    reasons = []
    # Score Must-Have Skills
    for skill in must_have_skills:
        match = skill.get('match', False)
        if match:
            must_have_scores.append(100)
        else:
            must_have_scores.append(0)
            reasons.append("missing Must-Have skills")
    # Score Nice-to-Have Skills
    for skill in nice_to_have_skills:
        match = skill.get('match', False)
        nice_to_have_scores.append(100 if match else 0)
    # Score Other Criteria (Experience and Education)
    # Now: full match=100, no match=0 (ineligible if no match)
    exp_match = experience_match.get('match', False)
    experience_score = 100 if exp_match else 0
    if not exp_match:
        reasons.append("experience mismatch")
    edu_match = education_match.get('match', False)
    education_score = 100 if edu_match else 0
    if not edu_match:
        reasons.append("education mismatch")
    other_scores.extend([experience_score, education_score])
    # Aggregate Scores
    must_have_avg = round(statistics.mean(must_have_scores)) if must_have_scores else 0
    nice_to_have_avg = round(statistics.mean(nice_to_have_scores)) if nice_to_have_scores else 0
    other_avg = round(statistics.mean(other_scores)) if other_scores else 0
    # Eligibility Check
    ineligible = bool(reasons)
    if ineligible:
        total_score = 0
        remarks = "Ineligible due to: " + ", ".join(reasons) + "."
    else:
        remarks = "Eligible."
        # If eligible, calculate weighted total
        must_have_weighted = (must_have_avg / 100) * 60
        nice_to_have_weighted = (nice_to_have_avg / 100) * 30
        other_weighted = (other_avg / 100) * 10
        total_score = round(must_have_weighted + nice_to_have_weighted + other_weighted)
    return {
        'must_have_score': must_have_avg,
        'nice_to_have_score': nice_to_have_avg,
        'other_criteria_score': other_avg,
        'total_score': total_score,
        'eligible': not ineligible,
        'remarks': remarks
    }
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
    ```json
    {{
        "job_title": "<job title>",
        "selected": <true or false>,
        "must_have_skills": [
            {{
                "skill_name": "<skill name>",
                "match": <true or false>
            }},
            {{...}},
            {{...}}
        ],
        "nice_to_have_skills": [
            {{
                "skill_name": "<skill name>",
                "match": <true or false>
            }},
            {{...}},
            {{...}}
        ],
        "experience_match": {{
            "required_experience_years": "<years>",
            "candidate_experience_years": "<years>",
            "match": <true or false>
        }},
        "education_match": {{
            "required_education": "<education details>",
            "candidate_education": "<education details>",
            "match": <true or false>
        }},
        "reason": <reason for selection or rejection>,
        "candidate_email": "<candidate email>",
    }}
    ```
    """
    MODEL_NAME = Variable.get("ltai.v3.lowtouch.recruitment.model_name", default_var="recruitment:0.3af")
    score_response = get_ai_response(prompt, stream=False, model=MODEL_NAME)
    logging.info(f"Match Score Response: {score_response}")
    score_data = extract_json_from_text(score_response)
    calculated_scores = calculate_candidate_score(score_data)
    ti.xcom_push(key='score_data', value=calculated_scores)
    logging.debug(f"Score Data: {calculated_scores}")
    return score_data

def send_response_email(**kwargs):
    """
    Send a response email to the candidate based on eligibility.
    Uses the email_utils for authentication and sending.
    """
    ti = kwargs['ti']
    email_data = kwargs['dag_run'].conf.get('email_data', {})
    score_data = ti.xcom_pull(task_ids='get_the_score_for_cv_analysis', key='score_data')
    cv_data = ti.xcom_pull(task_ids='extract_cv_content', key='cv_data')
    if not email_data or not score_data or not cv_data:
        logging.warning("Missing data for sending response email")
        return "Missing data for email"
    headers = email_data.get('headers', {})
    sender = headers.get('From', 'Unknown')
    subject = headers.get('Subject', 'No Subject')
    thread_id = email_data.get('threadId')
    original_message_id = headers.get('Message-ID', '')
    references = headers.get('References', '')
    if original_message_id and original_message_id not in references:
        references = f"{references} {original_message_id}".strip()
    # Parse sender email
    _, sender_email = parseaddr(sender)
    if not sender_email:
        logging.warning(f"Could not parse sender email from {sender}")
        return "Invalid sender email"
    # Compose email body based on eligibility
    if not score_data.get("eligible", False):
        # Rejection email
        body = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Application Update</title>
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; max-width: 600px; margin: 0 auto; padding: 20px; background-color: #f4f4f4; }}
                .email-container {{ background-color: #ffffff; padding: 30px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0, 0, 0, 0.1); }}
                .greeting {{ font-size: 18px; margin-bottom: 20px; }}
                .content {{ margin-bottom: 20px; }}
                .closing {{ margin-top: 30px; font-style: italic; }}
                .signature {{ margin-top: 20px; text-align: center; font-weight: bold; }}
                .company {{ color: #007bff; }}
            </style>
        </head>
        <body>
            <div class="email-container">
                <p class="greeting">Dear Candidate,</p>
                <div class="content">
                    <p>Thank you for your application and for sharing your CV with us. We appreciate the time and effort you invested.</p>
                    <p>After careful review, unfortunately, we are unable to proceed with your application for this position at this time. Our decision is based on the specific requirements of the role, and we encourage you to apply for other suitable positions in the future.</p>
                    <p>If you have any questions, feel free to reach out.</p>
                </div>
                <p class="closing">Best regards,<br>The Recruitment Team</p>
                <div class="signature">
                    <strong class="company">Lowtouch.ai</strong>
                </div>
            </div>
        </body>
        </html>
        """
        logging.info("Candidate not selected, sending rejection email.")
    else:
        # Selection email (initial assessment)
        MODEL_NAME = Variable.get("ltai.v3.lowtouch.recruitment.model_name", default_var="recruitment:0.3af")
        agent_response_prompt = f"""Compose a personalized response email for a selected candidate after initial screening. Include the following structure in the email body:

- Greeting: Use a professional greeting with the candidate's name if available from the CV data: {cv_data}. If no name is available, use 'Dear Candidate'.

- Next Steps: Present initial assessment questions:

    - Are you comfortable with a [full-time / part-time / contract / hybrid / remote / on-site] work arrangement?
    - What is your notice period with your current employer (or how soon can you start if not employed)?
    - What are your base salary expectations for this role? (Provide a range if possible.)
    - Are you open to relocating to [City/Region] if the role requires it, or do you already live within commuting distance? (Specify location if applicable)
    - Why are you interested in this role and our company?
  - Role-Specific Questions: Tailor these to the job role (e.g., for Data Scientist/ML Engineer):
    - How many years of experience do you have building and deploying machine learning models in production?
    - Are you proficient in Python and libraries such as TensorFlow, PyTorch, scikit-learn, or Pandas?(You can ask different qustion for different role)
    - Do you have experience with [specific domain/tools, e.g., NLP, computer vision, big data tools like Spark]? (Customize based on job)
    - Education/Certification (if required): Do you hold a [Bachelor's/Master's/PhD] degree in [specific field] or a related discipline? (Specify degree/field)
    - Do you hold any relevant certifications (e.g., PMP, AWS Certified, Scrum Master, etc.)? (List relevant ones)

- Call to Action: Encourage them to reply with their answers to these questions at their earliest convenience. Mention that responses will help advance them to the next stage, such as an interview.
- IMPORTANT: make the email completely professional, concise, and clear. Use a friendly yet formal tone. And avoid using subheaders which are unprofessional.

Use a professional, encouraging tone throughout. Output only clean, valid HTML for the email body (e.g., use <p>, <ul>, <li> for lists, <h2> for section headers). No technical placeholders—replace any dynamic elements with actual values if provided, or use sensible defaults. Ensure the email is concise yet comprehensive.
"""
        jd_data = ti.xcom_pull(task_ids='get_the_jd_for_cv_analysis', key='jd_data')
        agent_response_prompt += f"\nJob details: {jd_data}"
        response = get_ai_response(agent_response_prompt, stream=False, model=MODEL_NAME)
    
        # Clean HTML from response (similar to step_6)
        match = re.search(r'```html.*?\n(.*?)```', response, re.DOTALL)
        body = match.group(1).strip() if match else response.strip()
        # Fallback if not valid HTML
        if not body.strip().startswith('<!DOCTYPE') and not body.strip().startswith('<html'):
            body = body.strip()
        logging.info("Candidate selected, sending initial assessment email.")
    # Authenticate
    service = authenticate_gmail(GMAIL_CREDENTIALS, RECRUITMENT_FROM_ADDRESS)
    if not service:
        logging.error("Gmail authentication failed, aborting email response.")
        return "Gmail authentication failed"
    # For CV DAG, simple reply to sender (no CC/BCC unless specified)
    cc = None  # Add logic if needed
    bcc = None  # Add logic if needed
    subject = f"Re: {subject}"
    result = send_email(service, sender_email, subject, body, original_message_id, references, 
                   RECRUITMENT_FROM_ADDRESS, cc=cc, bcc=bcc, thread_id=thread_id)
    if result:
        logging.info(f"Email sent successfully to {sender_email}")
        # Optionally mark original as read
        # mark_email_as_read(service, email_data.get('id'))
        return f"Email sent successfully to {sender_email}"
    else:
        logging.error("Failed to send email")
        return "Failed to send email"




# Define the DAG
with DAG(
    "cv_analyse",
    default_args=default_args,
    schedule=None,  # Triggered by cv_monitor_mailbox DAG
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