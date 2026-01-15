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
        all_recipients = extract_all_recipients(email_data)
        
        logging.info(f"Email from: {sender}")
        logging.info(f"Subject: {subject}")
        logging.info(f"Recipients - To: {all_recipients['to']}, Cc: {all_recipients['cc']}")
        
        # Get JSON attachments
        attachments = email_data.get("attachments", [])
        
        if not attachments:
            raise ValueError("No JSON attachments found in email")
        
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
        
        parsed_response = get_ai_response(parse_prompt,model=MODEL_NAME)
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
        ti.xcom_push(key="all_recipients", value=json.dumps(all_recipients))
        ti.xcom_push(key="api_documentation", value=json.dumps(api_documentation))
        ti.xcom_push(key="test_requirements", value=parsed_requirements.get("test_requirements"))
        ti.xcom_push(key="special_instructions", value=parsed_requirements.get("special_instructions"))
        ti.xcom_push(key="priority_level", value=parsed_requirements.get("priority_level"))
        
        logging.info(f"Successfully extracted inputs from email: {email_data.get('id')}")
        
    except Exception as e:
        logging.error(f"Error extracting inputs: {str(e)}", exc_info=True)
        raise

import uuid

# ═══════════════════════════════════════════════════════════════
# STEP 2: Create and Validate Test Cases
# ═══════════════════════════════════════════════════════════════
def create_and_validate_test_cases(*args, **kwargs):
    """
    Creates comprehensive test cases from API documentation and validates them.
    Supports retry logic with conversation history.
    """
    ti = kwargs["ti"]
    
    # Get inputs from previous task
    api_docs_json = ti.xcom_pull(key="api_documentation", task_ids="extract_inputs")
    test_reqs = ti.xcom_pull(key="test_requirements", task_ids="extract_inputs")
    special_instructions = ti.xcom_pull(key="special_instructions", task_ids="extract_inputs")
    priority = ti.xcom_pull(key="priority_level", task_ids="extract_inputs")
    
    # Parse API documentation
    api_docs = json.loads(api_docs_json) if api_docs_json else {}

    # Generate a unique identifier for creating a test session unique per dag run
    test_session_id = ti.xcom_pull(key="test_session_uuid", task_ids=ti.task_id)
    if not test_session_id:
        test_session_id = str(uuid.uuid4())
        ti.xcom_push(key="test_session_uuid", value=test_session_id)
    
    # Detect retry
    is_retry = ti.try_number > 1
    
    history = []
    if is_retry:
        logging.info("Retry detected. Loading history from XCom.")
        history_json = ti.xcom_pull(key="task_history", task_ids=ti.task_id) or []
        # if not history_json:
        #     raise ValueError("Retry detected but no history found in XCom")
        if history_json:
            history = json.loads(history_json)
        
        test_cases_response = ti.xcom_pull(
            key="generated_test_cases_raw",
            task_ids=ti.task_id
        )
        # if not test_cases_response:
        #     raise ValueError("Retry detected but no test cases found in XCom")
    else:
        logging.info("First attempt. Generating new test cases.")
        
        # Build comprehensive prompt
        generate_prompt = f"""
        Create comprehensive API test cases based on the provided documentation. In the folder {test_session_id} in the test.yaml
        
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
        
        
        4. **Error Handling**:
           - 400 Bad Request scenarios
           - 404 Not Found
           - 500 Internal Server Error
        
        5. **Edge Cases**:
           - Empty values
           - Null values
           - Very long strings
           - Special characters
           - Concurrent requests
        
        Return test cases in this JSON format:
        {{
            "test_suite_name": "API Test Suite",
            "total_test_cases": 0,
            "test_cases": [
                {{
                    "test_id": "TC001",
                    "test_name": "Test Name",
                    "category": "valid/invalid/auth/error/edge",
                    "description": "What this test validates",
                    "endpoint": "/api/endpoint",
                    "method": "GET/POST/PUT/DELETE",
                    "headers": {{}},
                    "request_body": {{}},
                    "expected_status": 200,
                    "expected_response": {{}},
                    "assertions": ["list of validations to perform"]
                }}
            ]
        }}
        """
        
        test_cases_response = get_ai_response(generate_prompt,model=MODEL_NAME,history=history)
        history_val = [
            {"role": "user", "content": generate_prompt},
            {"role": "assistant", "content": test_cases_response},
        ]
        history.append(history_val[0])
        history.append(history_val[1])
        ti.xcom_push(key="generated_test_cases_raw", value=test_cases_response)
        ti.xcom_push(key="task_history", value=json.dumps(history))
    
    # ───────────────────────────────────────────────
    # Evaluation
    # ───────────────────────────────────────────────
    evaluate_prompt = """
    Evaluate the test cases you just created in the output folder {test_session_id} in the test.yaml based on:
    
    1. **Coverage**: Do they cover all major scenarios (valid, invalid, auth, errors, edge cases)?
    2. **Completeness**: Are all fields properly filled out?
    3. **Clarity**: Are test names and descriptions clear?
    4. **Assertions**: Are expected results well-defined?
    5. **Quality**: Are the test cases actionable and executable?
    
    Provide a coverage score (0-100) for each category and overall.
    
    Output strict JSON:
    {
        "proceed_to_execution": true | false,
        "reason": "detailed explanation of decision",
        "coverage_analysis": {
            "valid_scenarios": 0-100,
            "invalid_scenarios": 0-100,
            "auth_scenarios": 0-100,
            "error_handling": 0-100,
            "edge_cases": 0-100,
            "overall_score": 0-100
        },
        "improvements_needed": ["list of specific improvements if not proceeding"]
    }
    """
    
    evaluation_response = get_ai_response(evaluate_prompt, conversation_history=history, model=MODEL_NAME)
    decision = extract_json_from_text(evaluation_response)
    
    if not decision or "proceed_to_execution" not in decision:
        raise ValueError(
            f"Evaluation did not return valid JSON.\nRaw response:\n{evaluation_response}"
        )
    
    # Log evaluation results
    coverage = decision.get("coverage_analysis", {})
    logging.info(f"Test case evaluation - Overall score: {coverage.get('overall_score', 'N/A')}")
    logging.info(f"Decision: {'PROCEED' if decision['proceed_to_execution'] else 'RETRY'}")
    logging.info(f"Reason: {decision.get('reason', 'No reason provided')}")
    
    if decision["proceed_to_execution"] is True:
        ti.xcom_push(key="test_cases_approved", value=test_cases_response)
        ti.xcom_push(key="approval_decision", value=json.dumps(decision))
        return "execute_test_cases"
    else:
        # Update history for next retry
        history.append({"role": "assistant", "content": evaluation_response})
        ti.xcom_push(key="task_history", value=json.dumps(history))
        
        improvements = decision.get("improvements_needed", [])
        raise ValueError(
            f"Test cases insufficient (Score: {coverage.get('overall_score', 0)}/100).\n"
            f"Reason: {decision.get('reason')}\n"
            f"Improvements needed: {', '.join(improvements)}"
        )


# ═══════════════════════════════════════════════════════════════
# STEP 3: Execute Test Cases
# ═══════════════════════════════════════════════════════════════
def execute_test_cases(*args, **kwargs):
    """
    Executes the approved test cases and collects detailed results.
    """
    ti = kwargs["ti"]
    
    test_cases_json = ti.xcom_pull(
        key="test_cases_approved",
        task_ids="create_and_validate_test_cases"
    )
    
    if not test_cases_json:
        raise ValueError("No approved test cases found")
    
    # Parse test suite
    test_suite = extract_json_from_text(test_cases_json)
    
    if not test_suite or "test_cases" not in test_suite:
        raise ValueError("Invalid test suite format")
    
    test_cases = test_suite.get("test_cases", [])
    total_tests = len(test_cases)
    
    logging.info(f"Executing {total_tests} test cases from suite: {test_suite.get('test_suite_name', 'Unknown')}")
    
    # Execute test cases using AI agent
    execution_prompt = f"""
    Execute the following API test cases and provide detailed results.
    
    Test Suite:
    {json.dumps(test_suite, indent=2)}
    
    For each test case, simulate the API call and validate:
    1. Request format and parameters
    2. Expected status code
    3. Response structure
    4. All assertions
    5. Error handling
    
    Return results as JSON:
    {{
        "execution_summary": {{
            "total_tests": {total_tests},
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
    
    execution_response = get_ai_response(execution_prompt,model=MODEL_NAME)
    test_results = extract_json_from_text(execution_response)
    
    if not test_results or "test_results" not in test_results:
        raise ValueError("Failed to parse test execution results")
    
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
        "html_body": "complete HTML email content with inline CSS",
        "plain_text_summary": "brief plain text version for preview"
    }}
    """
    
    email_response = get_ai_response(email_generation_prompt,model=MODEL_NAME)
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
    all_recipients_json = ti.xcom_pull(key="all_recipients", task_ids="extract_inputs")
    
    if not all([recipient, subject, html_body]):
        raise ValueError("Missing required email information")
    
    # Parse all recipients
    all_recipients = json.loads(all_recipients_json) if all_recipients_json else {}
    cc_list = all_recipients.get('cc', [])
    
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
    'retry_delay': timedelta(minutes=5),
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