from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import os
import json
import logging
from airflow.exceptions import AirflowSkipException

# Import utility functions
from agent_dags.utils.email_utils import (
    authenticate_gmail,
    send_email,
    mark_email_as_read,
    extract_all_recipients
)
from agent_dags.utils.agent_utils import (
    get_ai_response,
    extract_json_from_text
)

# Get configuration from Airflow Variables
GMAIL_FROM_ADDRESS = Variable.get("ltai.api.test.from_address", default_var="")
GMAIL_CREDENTIALS = Variable.get("ltai.api.test.gmail_credentials", default_var="")
MODEL_NAME = "APITestAgent:3.0"
# ═══════════════════════════════════════════════════════════════
# STEP 1: Extract and Parse Inputs from Email
# ═══════════════════════════════════════════════════════════════
def extract_inputs_from_email(*args, **kwargs):
    """
    Extracts API documentation from JSON attachment and email metadata.
    Also extracts config.yaml if present in attachments.
    Receives email_data from the mailbox monitor DAG.
    """
    ti = kwargs["ti"]
    dag_run = kwargs.get('dag_run')
    
    try:
        # Get email data from DAG configuration (passed by mailbox monitor)
        email_data = dag_run.conf.get('email_data')
        main_json_path = dag_run.conf.get('main_json_path')
        
        if not email_data:
            raise ValueError("No email_data provided in DAG configuration")
        
        logging.info(f"Processing email ID: {email_data.get('id')}")
        
        # Extract email metadata
        headers = email_data.get("headers", {})
        sender = headers.get("From", "")
        subject = headers.get("Subject", "")
        message_id = headers.get("Message-ID", "")
        references = headers.get("References", "")
        thread_id = email_data.get("threadId", "")
        email_content = email_data.get("content", "")
        
        # Extract all recipients (To, Cc, Bcc)
        all_recipient = extract_all_recipients(email_data)
        
        logging.info(f"Email from: {sender}")
        logging.info(f"Subject: {subject}")
        logging.info(f"Thread ID: {thread_id}")
        logging.info(f"Recipients - To: {all_recipient['to']}, Cc: {all_recipient['cc']}")
        
        # Get JSON attachments
        attachments = email_data.get("attachments", [])
        
        if not attachments:
            raise ValueError("No JSON attachments found in email")
        
        # Check for config.yaml attachment
        config_attachment = email_data.get("config")
        config_path = None
        
        if config_attachment:
            config_path = config_attachment.get("path")
            logging.info(f"Config file found: {config_path}")
        else:
            logging.info("No config.yaml file found in email attachments")
        
        # Load the first JSON attachment (API documentation)
        json_attachment = attachments[0]
        json_path = json_attachment.get("path") or main_json_path
        
        if not json_path or not os.path.exists(json_path):
            raise ValueError(f"JSON file not found: {json_path}")
        
        # Read and parse JSON content
        with open(json_path, "r", encoding="utf-8") as f:
            api_documentation = json.load(f)
        
        logging.info(f"Loaded API documentation from: {json_path}")
        logging.info(f"API doc keys: {list(api_documentation.keys())}")
        
        # Parse email body for additional requirements using AI
        parse_prompt = f"""
        Extract test requirements and special instructions from this email:
        
        Subject: {subject}
        Body: {email_content}
        
        Return strict JSON:
        {{
            "test_requirements": "specific scenarios or cases to test",
            "special_instructions": "any special notes or constraints",
            "priority_level": "high/medium/low"
        }}
        
        If no specific requirements are mentioned, use general best practices.
        """
        
        parsed_response = get_ai_response(parse_prompt, model=MODEL_NAME)
        parsed_requirements = extract_json_from_text(parsed_response)
        
        if not parsed_requirements:
            parsed_requirements = {
                "test_requirements": "Standard API testing coverage",
                "special_instructions": "None",
                "priority_level": "medium"
            }
        
        # Store all data in XCom
        ti.xcom_push(key="sender_email", value=sender)
        ti.xcom_push(key="email_subject", value=subject)
        ti.xcom_push(key="message_id", value=message_id)
        ti.xcom_push(key="references", value=references)
        ti.xcom_push(key="thread_id", value=thread_id)
        ti.xcom_push(key="original_email_id", value=email_data.get("id"))
        ti.xcom_push(key="all_recipients", value=json.dumps(all_recipient))
        ti.xcom_push(key="api_documentation", value=json.dumps(api_documentation))
        ti.xcom_push(key="test_requirements", value=parsed_requirements.get("test_requirements"))
        ti.xcom_push(key="special_instructions", value=parsed_requirements.get("special_instructions"))
        ti.xcom_push(key="priority_level", value=parsed_requirements.get("priority_level"))
        ti.xcom_push(key="config_path", value=config_path)  # Store config path
        
        logging.info(f"Successfully extracted inputs from email: {email_data.get('id')}")
        
    except Exception as e:
        logging.error(f"Error extracting inputs: {str(e)}", exc_info=True)
        raise


# ═══════════════════════════════════════════════════════════════
# MODIFIED: api_test_executor.py - create_and_validate_test_cases function
# ═══════════════════════════════════════════════════════════════

def create_and_validate_test_cases(*args, **kwargs):
    """
    Creates comprehensive test cases from API documentation and validates them.
    Uses thread_id as the test session folder instead of UUID.
    Supports retry logic with conversation history.
    """
    ti = kwargs["ti"]
    
    # Get inputs from previous task
    api_docs_json = ti.xcom_pull(key="api_documentation", task_ids="extract_inputs")
    test_reqs = ti.xcom_pull(key="test_requirements", task_ids="extract_inputs")
    special_instructions = ti.xcom_pull(key="special_instructions", task_ids="extract_inputs")
    priority = ti.xcom_pull(key="priority_level", task_ids="extract_inputs")
    thread_id = ti.xcom_pull(key="thread_id", task_ids="extract_inputs")
    config_path = ti.xcom_pull(key="config_path", task_ids="extract_inputs")
    
    # Parse API documentation
    api_docs = json.loads(api_docs_json) if api_docs_json else {}

    # Use thread_id as the test session folder
    test_session_id = thread_id
    if not test_session_id:
        raise ValueError("Thread ID is missing - cannot create test session folder")
    
    # Create the postman directory structure
    test_dir = f"/appz/postman/{test_session_id}"
    os.makedirs(test_dir, exist_ok=True)
    logging.info(f"Using test session folder: {test_dir}")
    
    # Store thread_id for use in other tasks
    ti.xcom_push(key="test_session_id", value=test_session_id)
    
    # Detect retry
    is_retry = ti.try_number > 1
    
    history = []
    if is_retry:
        logging.info("Retry detected. Loading history from XCom.")
        history_json = ti.xcom_pull(key="task_history", task_ids=ti.task_id) or []
        if history_json:
            history = json.loads(history_json)
        
        test_cases_response = ti.xcom_pull(
            key="generated_test_cases_raw",
            task_ids=ti.task_id
        )
    
    # Build comprehensive prompt
    config_info = ""
    if config_path and os.path.exists(config_path):
        config_info = f"""
    
    **Configuration File**: A config.yaml file is available at {config_path}
    - This file contains base_url and authentication credentials
    - Use this config file when generating test cases
    """
    
    generate_prompt = f"""
    Create API test cases based on the provided documentation. 
    Save the test cases in folder: {test_session_id}
    File name: all // Give only the test file name which will be saved in the folder mentioned above. Do not give full path.

    
    API Documentation:
    {json.dumps(api_docs, indent=2)}
    
    Test Requirements:
    {test_reqs}
    
    Special Instructions:
    {special_instructions}
    
    Priority Level: {priority}
    
    Generate test cases covering:
    1. **Valid Request Scenarios**:
        - Happy path with valid data
        - All required and optional parameters
        - Different data types and formats
    
    2. **Invalid Request Scenarios**:
        - Missing required parameters
        - Invalid data types
        - Malformed requests
        - Boundary value testing
    
    3. **Error Handling**:
        - 400 Bad Request scenarios
        - 404 Not Found
        - 500 Internal Server Error
    
    4. **Edge Cases**:
        - Empty values
        - Null values
        - Very long strings
        - Special characters
        - Concurrent requests
    
    RULES:
    - For not found cases (e.g., if the address you are searching does not exist), the API will return a 404 error
    - Example: for GET /users/{{user_id}}, if user_id does not exist, return 404
    - Strictly ensure one assertion per test case, not multiple assertions
    - Use exact values from the documentation for expected results
    - Save files to: test.yaml and output directory: {test_session_id}
    """
    
    test_cases_response = get_ai_response(generate_prompt, model=MODEL_NAME, conversation_history=history)
    logging.info("Generated test cases response. Response: " + test_cases_response)
    
    history_val = [
        {"prompt": generate_prompt, "response": test_cases_response},
    ]
    history.append(history_val[0])
    ti.xcom_push(key="generated_test_cases_raw", value=test_cases_response)
    ti.xcom_push(key="task_history", value=json.dumps(history))
    
    return "execute_test_cases"


# ═══════════════════════════════════════════════════════════════
# MODIFIED: api_test_executor.py - execute_test_cases function
# ═══════════════════════════════════════════════════════════════

def execute_test_cases(*args, **kwargs):
    """
    Executes the approved test cases and collects detailed results.
    Uses thread_id folder for test execution.
    Supports retry logic with conversation history.
    """
    ti = kwargs["ti"]
    
    test_cases_json = ti.xcom_pull(
        key="test_cases_approved",
        task_ids="create_and_validate_test_cases"
    )
    
    test_session_id = ti.xcom_pull(key="test_session_id", task_ids="create_and_validate_test_cases")
    config_path = ti.xcom_pull(key="config_path", task_ids="extract_inputs")
    
    if not test_session_id:
        raise ValueError("Test session ID (thread_id) not found")
    
    test_folder = test_session_id
    logging.info(f"Executing tests from folder: /appz/postman/{test_folder}")
    
    # Detect retry
    is_retry = ti.try_number > 1
    
    history = []
    if is_retry:
        logging.info("Retry detected in execute_test_cases. Loading history from XCom.")
        history_json = ti.xcom_pull(key="execution_history", task_ids=ti.task_id) or '[]'
        history = json.loads(history_json)
    
    # Build execution prompt with config file support
    config_instruction = ""
    if config_path and os.path.exists(config_path):
        config_instruction = f"""
    **Important**: Use the config file at {config_path} for authentication and base URL.
    Config file parameter: config_file="config.yaml"
    """
    
    # Execute test cases using AI agent
    execution_prompt = f"""
    Execute the following API test cases:
    - Folder: {test_folder}
    - Output directory: {test_folder} (the test cases are in {test_folder}/)
    - Test file: test.yaml
    - Base URL: http://connector:8000 (or use config file if available)
    {config_instruction}
    
    Use the api_test_runner tool with these parameters:
    - file_name: "test.yaml" or give all to run all test files in the folder
    - output_dir: "{test_folder}"
    - config_file: "config.yaml" (if config file exists)
    - verbose: True
    
    For each test case, validate:
    1. Request format and parameters
    2. Expected status code
    3. Response structure
    4. All assertions
    5. Error handling
    
    Return results as JSON:
    {{
        "execution_summary": {{
            "total_tests": 0,
            "executed": 0,
            "passed": 0,
            "failed": 0,
            "errors": 0,
            "skipped": 0,
            "pass_rate": 0.0,
            "execution_time_ms": 0
        }},
        "test_results": [
            {{
                "test_id": "TC001",
                "test_name": "Test Name",
                "status": "PASS/FAIL/ERROR/SKIP",
                "execution_time_ms": 0,
                "request": {{}},
                "actual_response": {{}},
                "expected_response": {{}},
                "assertions_checked": [
                    {{
                        "assertion": "description",
                        "result": "pass/fail",
                        "details": "additional info"
                    }}
                ],
                "error_message": "if status is FAIL or ERROR",
                "logs": ["execution logs"]
            }}
        ],
        "failed_tests": [
            {{
                "test_id": "TC001",
                "test_name": "Name",
                "failure_reason": "why it failed",
                "recommendation": "how to fix"
            }}
        ]
    }}
    """
    
    execution_response = get_ai_response(execution_prompt, model=MODEL_NAME, conversation_history=history)
    logging.info("Generated execution response. Response: " + execution_response)
    
    # Append to history
    history.append({"prompt": execution_prompt, "response": execution_response})
    ti.xcom_push(key="execution_response_raw", value=execution_response)
    ti.xcom_push(key="execution_history", value=json.dumps(history))
    
    # ───────────────────────────────────────────────
    # Evaluation of execution results
    # ───────────────────────────────────────────────
    evaluate_prompt = f"""
    Evaluate the test execution results you just generated:
    
    {execution_response}
    
    Based on:
    1. **Validity**: Is the output valid JSON?
    2. **Completeness**: Are all test cases executed with proper status, requests, responses, and assertions?
    3. **Consistency**: Do the summary metrics match the detailed results (e.g., passed/failed counts)?
    4. **Quality**: Are failure reasons and recommendations provided for failed tests? Are simulations realistic?
    
    Provide a score (0-100) for each category and overall.
    
    Output strict JSON:
    {{
        "proceed_to_reporting": true | false,
        "reason": "detailed explanation of decision",
        "analysis": {{
            "validity": 0-100,
            "completeness": 0-100,
            "consistency": 0-100,
            "quality": 0-100,
            "overall_score": 0-100
        }},
        "improvements_needed": ["list of specific improvements if not proceeding"]
    }}
    """
    
    logging.info("Evaluating generated execution results for validity and quality. Prompt: " + evaluate_prompt)
    
    evaluation_response = get_ai_response(evaluate_prompt, model=MODEL_NAME)
    decision = extract_json_from_text(evaluation_response)
    
    if not decision or "proceed_to_reporting" not in decision:
        raise ValueError(
            f"Evaluation did not return valid JSON.\nRaw response:\n{evaluation_response}"
        )
    
    # Log evaluation results
    analysis = decision.get("analysis", {})
    logging.info(f"Execution results evaluation - Overall score: {analysis.get('overall_score', 'N/A')}")
    logging.info(f"Decision: {'PROCEED' if decision['proceed_to_reporting'] else 'RETRY'}")
    logging.info(f"Reason: {decision.get('reason', 'No reason provided')}")
    
    if decision["proceed_to_reporting"] is True:
        # Parse the execution response
        test_results = extract_json_from_text(execution_response)
        logging.info("Parsed test execution results JSON. test_results: " + str(test_results))
        
        # Extract summary
        summary = test_results.get("execution_summary", {})
        
        logging.info(f"Test execution complete:")
        logging.info(f"  Total: {summary.get('total_tests', 0)}")
        logging.info(f"  Passed: {summary.get('passed', 0)}")
        logging.info(f"  Failed: {summary.get('failed', 0)}")
        logging.info(f"  Errors: {summary.get('errors', 0)}")
        logging.info(f"  Pass Rate: {summary.get('pass_rate', 0):.2f}%")
        
        # Store results
        ti.xcom_push(key="test_results", value=json.dumps(test_results))
        ti.xcom_push(key="test_summary", value=json.dumps(summary))
        ti.xcom_push(key="failed_tests", value=json.dumps(test_results.get("failed_tests", [])))
        
        ti.xcom_push(key="approval_decision", value=json.dumps(decision))
    else:
        # Update history for next retry
        history.append({"prompt": evaluate_prompt, "response": evaluation_response})
        ti.xcom_push(key="execution_history", value=json.dumps(history))
        
        improvements = decision.get("improvements_needed", [])
        raise ValueError(
            f"Execution results insufficient (Score: {analysis.get('overall_score', 0)}/100).\n"
            f"Reason: {decision.get('reason')}\n"
            f"Improvements needed: {', '.join(improvements)}"
        )

# ═══════════════════════════════════════════════════════════════
# STEP 4: Generate Email Content
# ═══════════════════════════════════════════════════════════════
def generate_email_content(*args, **kwargs):
    """
    Uses AI to generate a professional HTML email response with test results.
    """
    ti = kwargs["ti"]
    
    # Collect all necessary information
    sender = ti.xcom_pull(key="sender_email", task_ids="extract_inputs")
    subject = ti.xcom_pull(key="email_subject", task_ids="extract_inputs")
    test_results = ti.xcom_pull(key="test_results", task_ids="execute_test_cases")
    test_summary = ti.xcom_pull(key="test_summary", task_ids="execute_test_cases")
    failed_tests = ti.xcom_pull(key="failed_tests", task_ids="execute_test_cases")
    api_docs = ti.xcom_pull(key="api_documentation", task_ids="extract_inputs")
    
    if not all([test_results, test_summary]):
        raise ValueError("Missing test results or summary")
    
    # Parse JSON strings
    test_results_obj = json.loads(test_results)
    summary_obj = json.loads(test_summary)
    failed_tests_obj = json.loads(failed_tests) if failed_tests else []
    
    # Generate professional email content using AI
    email_generation_prompt = f"""
    Generate a professional HTML email response for API test execution results.
    
    Original Email:
    - From: {sender}
    - Subject: {subject}
    
    Test Execution Summary:
    {json.dumps(summary_obj, indent=2)}
    
    Failed Tests (if any):
    {json.dumps(failed_tests_obj, indent=2)}
    
    Full Test Results:
    {test_results}
    
    Create an HTML email with:
    
    1. **Subject Line**: Start with "Re: " + original subject
    
    2. **Email Structure**:
       - Professional greeting
       - Executive summary paragraph with key metrics
       - Visual summary section with color-coded statistics
       - Detailed results section (expandable/collapsible if many tests)
       - Failed tests highlighted (if any) with recommendations
       - Next steps or action items
       - Closing with offer for questions
    
    3. **Styling Requirements**:
       - Use professional color scheme (green for pass, red for fail, yellow for warnings)
       - Responsive design (mobile-friendly)
       - Clear typography and spacing
       - Tables for test results
       - Icons or visual indicators for status
    
    4. **Tone**: Professional, clear, actionable, positive (even for failures)
    
    Return JSON:
    {{
        "subject": "Re: [original subject]",
        "html_body": "complete HTML email content with inline CSS for reason for faliure for each failed test if any",
        "plain_text_summary": "brief plain text version for preview"
    }}
    """
    history_json = ti.xcom_pull(key="task_history", task_ids=ti.task_id) or []
    if history_json:
        history = json.loads(history_json) 
    email_response = get_ai_response(email_generation_prompt,model=MODEL_NAME, conversation_history=history_json)
    email_content = extract_json_from_text(email_response)
    
    if not email_content or "subject" not in email_content or "html_body" not in email_content:
        raise ValueError("Failed to generate valid email content")
    
    # Store email content
    ti.xcom_push(key="response_subject", value=email_content["subject"])
    ti.xcom_push(key="response_html_body", value=email_content["html_body"])
    ti.xcom_push(key="response_plain_text", value=email_content.get("plain_text_summary", ""))
    
    logging.info("Email content generated successfully")
    logging.info(f"Subject: {email_content['subject']}")


# ═══════════════════════════════════════════════════════════════
# STEP 5: Send Email Response
# ═══════════════════════════════════════════════════════════════
def send_response_email(*args, **kwargs):
    """
    Sends the generated email response maintaining thread continuity.
    Includes all original recipients (To, Cc).
    """
    ti = kwargs["ti"]
    
    # Get email details
    recipient = ti.xcom_pull(key="sender_email", task_ids="extract_inputs")
    subject = ti.xcom_pull(key="response_subject", task_ids="generate_email_content")
    html_body = ti.xcom_pull(key="response_html_body", task_ids="generate_email_content")
    message_id = ti.xcom_pull(key="message_id", task_ids="extract_inputs")
    references = ti.xcom_pull(key="references", task_ids="extract_inputs")
    thread_id = ti.xcom_pull(key="thread_id", task_ids="extract_inputs")
    original_email_id = ti.xcom_pull(key="original_email_id", task_ids="extract_inputs")
    all_recipient_json = ti.xcom_pull(key="all_recipient", task_ids="extract_inputs")
    
    if not all([recipient, subject, html_body]):
        raise ValueError("Missing required email information")
    
    # Parse all recipients
    all_recipient = json.loads(all_recipient_json) if all_recipient_json else {}
    cc_list = all_recipient.get('cc', [])
    
    try:
        # Authenticate Gmail
        service = authenticate_gmail(GMAIL_CREDENTIALS, GMAIL_FROM_ADDRESS)
        
        if not service:
            raise ValueError("Failed to authenticate Gmail service")
        
        # Send email with proper threading
        result = send_email(
            service=service,
            recipient=recipient,
            subject=subject,
            body=html_body,
            in_reply_to=message_id,
            references=references,
            from_address=GMAIL_FROM_ADDRESS,
            cc=cc_list if cc_list else None,
            thread_id=thread_id
        )
        
        if not result:
            raise ValueError("Failed to send email - no result returned")
        
        # Mark original email as read
        if original_email_id:
            mark_email_as_read(service, original_email_id)
        
        logging.info(f"Email sent successfully to {recipient}")
        logging.info(f"Thread ID: {thread_id}")
        logging.info(f"Message ID: {result.get('id')}")
        
        # Store confirmation
        ti.xcom_push(key="email_sent", value=True)
        ti.xcom_push(key="email_sent_timestamp", value=datetime.now().isoformat())
        ti.xcom_push(key="sent_message_id", value=result.get('id'))
        
    except Exception as e:
        logging.error(f"Failed to send email: {str(e)}", exc_info=True)
        raise


# ═══════════════════════════════════════════════════════════════
# STEP 6: Cleanup Task
# ═══════════════════════════════════════════════════════════════
def cleanup_attachments_task(*args, **kwargs):
    """
    Cleanup old attachment files to save disk space.
    """
    from agent_dags.utils.agent_utils import cleanup_attachments
    
    attachment_dir = "/appz/data/attachments/"
    older_than_days = 7
    
    try:
        deleted_count = cleanup_attachments(attachment_dir, older_than_days)
        logging.info(f"Cleanup completed: {deleted_count} files removed")
        
        kwargs['ti'].xcom_push(key="cleanup_count", value=deleted_count)
        
    except Exception as e:
        logging.error(f"Cleanup failed: {str(e)}")
        # Don't fail the DAG for cleanup errors
        pass


# ═══════════════════════════════════════════════════════════════
# DAG Definition
# ═══════════════════════════════════════════════════════════════
default_args = {
    'owner': 'lowtouch.ai_developers',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=15),
}

readme_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'api_test_executor.md')
try:
    with open(readme_path, 'r') as file:
        readme_content = file.read()
except:
    readme_content = "API Test Case Executor - Automated testing workflow"

with DAG(
    'api_test_case_executor',
    default_args=default_args,
    description='Automated API test case generation, execution, and reporting via email',
    schedule_interval=None,  # Triggered by mailbox monitor
    start_date=datetime(2024, 2, 24),
    catchup=False,
    doc_md=readme_content,
    tags=['api', 'testing', 'automation', 'agent', 'email'],
) as dag:
    
    # Task 1: Extract inputs from email and JSON attachment
    extract_inputs = PythonOperator(
        task_id='extract_inputs',
        python_callable=extract_inputs_from_email,
        provide_context=True,
        doc_md="Extracts API documentation from JSON attachment and parses email requirements"
    )
    
    # Task 2: Create and validate test cases (with branching)
    validate_test_cases = BranchPythonOperator(
        task_id='create_and_validate_test_cases',
        python_callable=create_and_validate_test_cases,
        provide_context=True,
        doc_md="Generates comprehensive test cases and validates coverage before execution"
    )
    
    # Task 3: Execute test cases
    execute_tests = PythonOperator(
        task_id='execute_test_cases',
        python_callable=execute_test_cases,
        provide_context=True,
        doc_md="Executes all approved test cases and collects detailed results"
    )
    
    # Task 4: Generate email content
    generate_email = PythonOperator(
        task_id='generate_email_content',
        python_callable=generate_email_content,
        provide_context=True,
        doc_md="Generates professional HTML email with test results and recommendations"
    )
    
    # Task 5: Send email response
    send_email_task = PythonOperator(
        task_id='send_response_email',
        python_callable=send_response_email,
        provide_context=True,
        doc_md="Sends email response to original sender maintaining thread continuity"
    )
    
    # Task 6: Cleanup old attachments
    cleanup_task = PythonOperator(
        task_id='cleanup_attachments',
        python_callable=cleanup_attachments_task,
        provide_context=True,
        trigger_rule='all_done',  # Run even if previous tasks fail
        doc_md="Removes old attachment files to free up disk space"
    )
    
    # Task 7: Success marker
    workflow_complete = DummyOperator(
        task_id='workflow_complete',
        trigger_rule='all_success'
    )
    
    # Define task dependencies
    extract_inputs >> validate_test_cases >> execute_tests >> generate_email >> send_email_task >> cleanup_task >> workflow_complete