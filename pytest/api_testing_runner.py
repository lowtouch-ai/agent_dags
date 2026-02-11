from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import os
import json
import logging
import re

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
MODEL_NAME = Variable.get("ltai.api.test.model.name", default_var="PostmanAPITestAgent:3.0")
server_host = Variable.get("ltai.server.host", default_var="http://localhost:8080")

# ═══════════════════════════════════════════════════════════════
# STEP 1: Extract and Parse Inputs from Email
# ═══════════════════════════════════════════════════════════════
def extract_inputs_from_email(*args, **kwargs):
    """
    Extracts API documentation from JSON attachments, PDF files, and email metadata.
    Receives comprehensive email_data from the mailbox monitor DAG.
    """
    ti = kwargs["ti"]
    dag_run = kwargs.get('dag_run')
    
    try:
        # Get comprehensive data from DAG configuration (passed by enhanced mailbox monitor)
        conf = dag_run.conf
        email_id = conf.get('email_id')
        thread_id = conf.get('thread_id')
        json_files = conf.get('json_files', [])
        pdf_files = conf.get('pdf_files', [])
        config_file = conf.get('config_file')
        has_pdf = conf.get('has_pdf', False)
        email_headers = conf.get('email_headers', {})
        email_content = conf.get('email_content', '')
        
        if not email_id:
            raise ValueError("No email_id provided in DAG configuration")
        
        logging.info(f"Processing email ID: {email_id}")
        logging.info(f"Thread ID: {thread_id}")
        logging.info(f"JSON files: {len(json_files)}")
        logging.info(f"PDF files: {len(pdf_files)}")
        logging.info(f"Has config: {config_file is not None}")
        
        # Extract email metadata
        sender = email_headers.get("From", "")
        subject = email_headers.get("Subject", "")
        message_id = email_headers.get("Message-ID", "")
        references = email_headers.get("References", "")
        
        # Create a simplified email_data structure for extract_all_recipients
        email_data_for_recipients = {
            "headers": email_headers
        }
        all_recipient = extract_all_recipients(email_data_for_recipients)
        
        logging.info(f"Email from: {sender}")
        logging.info(f"Subject: {subject}")
        logging.info(f"Recipients - To: {all_recipient['to']}, Cc: {all_recipient['cc']}")
        
        # Validate JSON files
        if not json_files:
            raise ValueError("No JSON attachments found - Postman collection required")
        
        # Load the primary JSON file (Postman collection)
        primary_json = json_files[0]
        json_path = primary_json.get("path")
        
        if not json_path or not os.path.exists(json_path):
            raise ValueError(f"Primary JSON file not found: {json_path}")
        
        # Read and parse JSON content
        with open(json_path, "r", encoding="utf-8") as f:
            api_documentation = json.load(f)
        
        logging.info(f"Loaded API documentation from: {json_path}")
        logging.info(f"API doc keys: {list(api_documentation.keys())}")
        
        # Process PDF files if present
        pdf_context = ""
        pdf_paths = []
        
        if has_pdf and pdf_files:
            logging.info(f"Processing {len(pdf_files)} PDF file(s)")
            for pdf_file in pdf_files:
                pdf_path = pdf_file.get("path")
                pdf_content = pdf_file.get("extracted_content", "")
                pdf_filename = pdf_file.get("filename", "")
                
                if pdf_path:
                    pdf_paths.append(pdf_path)
                
                if pdf_content:
                    pdf_context += f"\n\n--- PDF: {pdf_filename} ---\n{pdf_content}\n"
            
            logging.info(f"Extracted content from {len(pdf_files)} PDF(s)")
        
        # Extract config path
        config_path = None
        if config_file:
            config_path = config_file.get("path")
            logging.info(f"Config file found: {config_path}")
        else:
            logging.info("No config.yaml file found in email attachments")
        
        # Build comprehensive prompt with PDF context
        parse_prompt = f"""
        Extract test requirements and special instructions from this email and attached documents:
        
        Subject: {subject}
        Email Body: {email_content}
        
        API Documentation (JSON): 
        {json.dumps(api_documentation, indent=2)[:2000]}...
        
        {"Additional Context from PDF Documents:" + pdf_context if pdf_context else "No PDF documents provided."}
        
        Analyze all the information and return strict JSON:
        {{
            "test_requirements": "specific scenarios or cases to test based on email and PDF content",
            "special_instructions": "any special notes, constraints, or requirements from PDFs",
            "priority_level": "high/medium/low",
            "base_url": "if specified in email, PDF, or api documentation",
            "pdf_insights": "key information extracted from PDF that affects testing"
        }}
        
        If PDF contains API specifications, test scenarios, or requirements, incorporate them into test_requirements.
        If no specific requirements are mentioned, use general best practices.
        """
        
        parsed_response = get_ai_response(parse_prompt, model=MODEL_NAME)
        parsed_requirements = extract_json_from_text(parsed_response)
        
        if not parsed_requirements:
            parsed_requirements = {
                "test_requirements": "Standard API testing coverage",
                "special_instructions": "None",
                "priority_level": "medium",
                "pdf_insights": "No PDF provided"
            }
        
        base_url = parsed_requirements.get("base_url", None)
        pdf_insights = parsed_requirements.get("pdf_insights", "")
        
        # Store all data in XCom
        ti.xcom_push(key="sender_email", value=sender)
        ti.xcom_push(key="email_subject", value=subject)
        ti.xcom_push(key="message_id", value=message_id)
        ti.xcom_push(key="references", value=references)
        ti.xcom_push(key="thread_id", value=thread_id)
        ti.xcom_push(key="original_email_id", value=email_id)
        ti.xcom_push(key="all_recipients", value=json.dumps(all_recipient))
        ti.xcom_push(key="api_documentation", value=json.dumps(api_documentation))
        ti.xcom_push(key="test_requirements", value=parsed_requirements.get("test_requirements"))
        ti.xcom_push(key="special_instructions", value=parsed_requirements.get("special_instructions"))
        ti.xcom_push(key="priority_level", value=parsed_requirements.get("priority_level"))
        ti.xcom_push(key="config_path", value=config_path)
        ti.xcom_push(key="base_url", value=base_url)
        
        # Store PDF-related data
        ti.xcom_push(key="has_pdf", value=has_pdf)
        ti.xcom_push(key="pdf_context", value=pdf_context)
        ti.xcom_push(key="pdf_insights", value=pdf_insights)
        ti.xcom_push(key="pdf_paths", value=json.dumps(pdf_paths))
        ti.xcom_push(key="pdf_count", value=len(pdf_files))
        
        logging.info(f"Successfully extracted inputs from email: {email_id}")
        logging.info(f"PDF files processed: {len(pdf_files)}")
        
    except Exception as e:
        logging.error(f"Error extracting inputs: {str(e)}", exc_info=True)
        raise


# ═══════════════════════════════════════════════════════════════
# STEP 2: Create and Validate Test Cases (Enhanced with PDF)
# ═══════════════════════════════════════════════════════════════

def create_and_validate_test_cases(*args, **kwargs):
    """
    Creates comprehensive test cases from API documentation and PDF context.
    Uses thread_id as the test session folder.
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
    
    # Get PDF-related data
    has_pdf = ti.xcom_pull(key="has_pdf", task_ids="extract_inputs")
    pdf_context = ti.xcom_pull(key="pdf_context", task_ids="extract_inputs")
    pdf_insights = ti.xcom_pull(key="pdf_insights", task_ids="extract_inputs")
    pdf_count = ti.xcom_pull(key="pdf_count", task_ids="extract_inputs")
    
    # Parse API documentation
    api_docs = json.loads(api_docs_json) if api_docs_json else {}

    # Use thread_id as the test session folder
    test_session_id = thread_id
    if not test_session_id:
        raise ValueError("Thread ID is missing - cannot create test session folder")
    
    # Create the test directory structure
    test_dir = Variable.get("ltai.test.base_dir", default_var="/appz/pyunit_testing") + f"/{test_session_id}"
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
    
    # Build comprehensive prompt with PDF context
    config_info = ""
    if config_path and os.path.exists(config_path):
        config_info = f"""
    
    **Configuration File**: A config.yaml file is available at {config_path}
    - This file contains base_url and authentication credentials
    - Use this config file when generating test cases
    """
    
    pdf_info = ""
    if has_pdf and pdf_context:
        pdf_info = f"""
    
    **PDF Documentation Provided** ({pdf_count} file(s)):
    {pdf_context[:3000]}...
    
    **Key Insights from PDF**:
    {pdf_insights}
    
    - Incorporate specifications, requirements, and test scenarios from the PDF
    - Use PDF content to enhance test coverage and validation
    - Pay special attention to edge cases mentioned in the PDF
    """
    
    generate_prompt = f"""
    Create API test cases based on the provided documentation and additional context.
    Save the test cases in folder: {test_session_id}
    File name: all // Give only the test file name which will be saved in the folder mentioned above. Do not give full path.
    {config_info}
    {pdf_info}
    
    API Documentation (JSON):
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
        - Scenarios from PDF documentation (if provided)
    
    2. **Invalid Request Scenarios**:
        - Missing required parameters
        - Invalid data types
        - Malformed requests
        - Boundary value testing
        - Error cases mentioned in PDF (if provided)
    
    3. **Error Handling**:
        - 400 Bad Request scenarios
        - 404 Not Found
        - 500 Internal Server Error
        - Custom error codes from PDF (if specified)
    
    4. **Edge Cases**:
        - Empty values
        - Null values
        - Very long strings
        - Special characters
        - Concurrent requests
        - Edge cases from PDF specifications (if provided)
    
    RULES:
    - For not found cases (e.g., if the address you are searching does not exist), the API will return a 404 error
    - Example: for GET /users/{{user_id}}, if user_id does not exist, return 404
    - Strictly ensure one assertion per test case, not multiple assertions
    - Use exact values from the documentation for expected results
    - **If PDF contains specific test data, use that data in test cases**
    - **If PDF specifies validation rules, incorporate them into assertions**
    - Save files to: test.yaml and output directory: {test_session_id}
    
    IMPORTANT:
    - **Do not create any test case for DELETE endpoints to avoid accidental data loss.**
    - **Always give preference to Special Instructions over general Test Requirements.**
    - **Only create test cases for the methods and endpoints mentioned in the API documentation.** For example, if the API description contains only GET methods, do not create test cases for POST, PUT or DELETE methods.
    - **If PDF documentation provides additional endpoints or methods, include them in testing.**
    - **Do not create test cases for authentication scenatios** skip authentication scenarios for now.
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
# STEP 3: Execute Test Cases
# ═══════════════════════════════════════════════════════════════

def execute_test_cases(**kwargs):
    """
    Executes the approved test cases and collects detailed results.
    Uses thread_id folder for test execution.
    Supports retry logic with conversation history.
    """
    ti = kwargs["ti"]

    test_session_id = ti.xcom_pull(key="test_session_id", task_ids="create_and_validate_test_cases")
    config_path = ti.xcom_pull(key="config_path", task_ids="extract_inputs")
    base_url = ti.xcom_pull(key="base_url", task_ids="extract_inputs")
    has_pdf = ti.xcom_pull(key="has_pdf", task_ids="extract_inputs")
    pdf_count = ti.xcom_pull(key="pdf_count", task_ids="extract_inputs")
    
    logging.info(f"Starting test execution for session: {test_session_id}")
    logging.info(f"Config path: {config_path}")
    logging.info(f"Base URL: {base_url}")
    logging.info(f"PDF documentation provided: {has_pdf} ({pdf_count} file(s))")
    
    if base_url is None:
        base_url = "http://connector:8000"
    
    if not test_session_id:
        raise ValueError("Test session ID (thread_id) not found")
    
    test_folder = test_session_id
    logging.info(f"Executing tests from folder: /appz/pyunit_test/{test_folder}")
    
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
    
    pdf_note = ""
    if has_pdf:
        pdf_note = f"""
    **Note**: Test cases were generated considering {pdf_count} PDF document(s) with additional specifications.
    Validation should align with requirements from both JSON and PDF sources.
    """
    
    # Execute test cases using AI agent
    execution_prompt = f"""
    Execute the following API test cases:
    - Folder: {test_folder}
    - Output directory: {test_folder} (the test cases are in {test_folder}/)
    - Test file: test.yaml
    - Base URL: {base_url}
    {config_instruction}
    {pdf_note}
    
    Use the api_test_runner tool with these parameters:
    - file_name: "test.yaml" or give all to run all test files in the folder
    - output_dir: "{test_folder}"
    - config_file: "config.yaml" (if config file exists)
    - verbose: True
    
    For each test case, validate:
    1. Request format and parameters
    2. Expected status code
    3. Response structure
    4. All assertions (including those derived from PDF specifications)
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
            "execution_time_ms": 0,
            "html_report_path": "{server_host}/static/postman_reports/xxxx.html" //replace xxxx with actual file name
        }}
    }}
    """
    
    execution_response = get_ai_response(execution_prompt, model=MODEL_NAME, conversation_history=history)
    logging.info("Generated execution response. Response: " + execution_response)
    
    # Append to history
    history.append({"prompt": execution_prompt, "response": execution_response})
    ti.xcom_push(key="execution_response_raw", value=execution_response)
    ti.xcom_push(key="execution_history", value=json.dumps(history))
    
    # Evaluation of execution results
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
    
    logging.info("Evaluating generated execution results for validity and quality.")
    
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
        logging.info("Parsed test execution results JSON.")
        
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
# STEP 4: Generate Email Content (Enhanced with PDF info)
# ═══════════════════════════════════════════════════════════════
def generate_email_content(*args, **kwargs):
    """
    Uses AI to generate professional HTML email body with test results.
    Includes PDF context information if PDFs were processed.
    """
    ti = kwargs["ti"]
    output_dir = ti.xcom_pull(key="test_session_id", task_ids="create_and_validate_test_cases")
    
    # Generate Postman collection
    postman_collection_prompt = f"Generate the postman collection for the output directory {output_dir} and provide in the json format {{postman_collection_url: {server_host}/static/postman_reports/xxx.json (postman_url from the output directory)}}"
    postman_collection_response = get_ai_response(postman_collection_prompt, model=MODEL_NAME)
    logging.info("Postman collection response: " + postman_collection_response)
    postman_collection_json = extract_json_from_text(postman_collection_response) or {}
    postman_collection_url = postman_collection_json.get("postman_collection_url", "")
    ti.xcom_push(key="postman_collection_url", value=postman_collection_url)
    
    # Pull required data
    sender = ti.xcom_pull(key="sender_email", task_ids="extract_inputs")
    subject = ti.xcom_pull(key="email_subject", task_ids="extract_inputs")
    test_results = ti.xcom_pull(key="test_results", task_ids="execute_test_cases")
    test_summary = ti.xcom_pull(key="test_summary", task_ids="execute_test_cases")
    failed_tests = ti.xcom_pull(key="failed_tests", task_ids="execute_test_cases")
    response_from_execute_test_cases = ti.xcom_pull(key="execution_response_raw", task_ids="execute_test_cases")
    
    # Get PDF-related data
    has_pdf = ti.xcom_pull(key="has_pdf", task_ids="extract_inputs")
    pdf_count = ti.xcom_pull(key="pdf_count", task_ids="extract_inputs")
    pdf_insights = ti.xcom_pull(key="pdf_insights", task_ids="extract_inputs")
    
    test_results_json = extract_json_from_text(response_from_execute_test_cases) or {}
    html_report_link = test_results_json.get("html_report_path", "")
    
    if not all([test_results, test_summary]):
        raise ValueError("Missing test results or summary")
    
    # Parse JSON strings
    summary_obj = json.loads(test_summary)
    failed_tests_obj = json.loads(failed_tests) if failed_tests else []
    
    # Build PDF context for email
    pdf_section = ""
    if has_pdf and pdf_count > 0:
        pdf_section = f"""
    
    **PDF Documentation Context**:
    - {pdf_count} PDF document(s) were analyzed
    - Test cases incorporated specifications from PDF files
    - Key insights: {pdf_insights[:200]}...
    
    Include a section in the email mentioning that PDF documentation was analyzed and incorporated into test scenarios.
    """
    
    email_generation_prompt = f"""You are an expert at creating clean, professional HTML emails.

Generate **ONLY** the complete HTML email body (including <!DOCTYPE html> ... </html>).
Do NOT include any JSON, plain text summary, explanations, markdown, or anything outside the HTML.
Do NOT wrap the output in ```html or any code block.

Requirements:

• Subject line suggestion (as HTML comment at the top): <!-- Subject: Re: {subject} -->

• From: reply to {sender}

• Professional, modern, responsive design
• Inline CSS only (no external stylesheets)
• Color scheme: 
  - Pass: #28a745 (green)
  - Fail: #dc3545 (red)
  - Warning/Skip: #ffc107 (yellow)
• Use simple status icons via emoji or unicode (✓ ✗ ⚠)
• Executive summary with big numbers at the top
• Table or cards for detailed results
{pdf_section}
• Failed test cases MUST show:
  - Test name
  - Failure reason / error message
  - Link to detailed report: {html_report_link}
  - Link to Postman collection: {postman_collection_url}
• Collapsible <details><summary> for long lists of tests (optional but recommended)
• Professional greeting and closing
• Mobile-friendly (max-width: 600px container, fluid images/tables)

Current data:

Test Summary:
{json.dumps(summary_obj, indent=2)}

Failed Tests:
{json.dumps(failed_tests_obj, indent=2)}

Full Results (raw):
{test_results}

PDF Context:
- PDFs provided: {has_pdf}
- PDF count: {pdf_count}
- Insights: {pdf_insights}

Always include the following links in the email body:   
- Full HTML Report: {html_report_link}
- Postman Collection: {postman_collection_url}

Output **only** the full HTML document.
"""

    # Optional: keep minimal history if needed for context/style consistency
    history_json = ti.xcom_pull(key="task_history", task_ids=ti.task_id) or "[]"
    history = json.loads(history_json)

    # Get raw AI response
    raw_response = get_ai_response(
        email_generation_prompt,
        model=MODEL_NAME,
        conversation_history=history
    )
    
    # Remove <think>…</think> block
    cleaned = raw_response.strip()
    cleaned = re.sub(
        r'^\s*<think>.*?</think>\s*',
        '',
        raw_response,
        flags=re.DOTALL | re.IGNORECASE
    )
    
    # Basic cleaning
    cleaned = cleaned.removeprefix("```html").removesuffix("```").strip()
    cleaned = cleaned.removeprefix("```").removesuffix("```").strip()

    # Basic validation
    if not cleaned.startswith(("<!DOCTYPE", "<html")):
        raise ValueError("AI did not return valid HTML — output starts with: " + cleaned[:60])

    html_body = cleaned

    # Push data to XCom
    ti.xcom_push(key="response_subject", value=f"Re: {subject}")
    ti.xcom_push(key="response_html_body", value=html_body)

    logging.info("Pure HTML email body generated successfully")
    logging.info(f"Subject will be: Re: {subject}")
    logging.info(f"HTML length: {len(html_body):,} characters")
    logging.info(f"PDF context included: {has_pdf}")

    return html_body


# ═══════════════════════════════════════════════════════════════
# STEP 5: Send Email Response
# ═══════════════════════════════════════════════════════════════
def send_response_email(**kwargs):
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
    all_recipient_json = ti.xcom_pull(key="all_recipients", task_ids="extract_inputs")
    
    if not all([recipient, subject, html_body]):
        raise ValueError("Missing required email information")
    
    # Parse all recipients
    all_recipient = json.loads(all_recipient_json) if all_recipient_json else {}
    cc_list = all_recipient.get('cc', [])
    
    # Log threading information for debugging
    logging.info(f"Threading details:")
    logging.info(f"  Thread ID: {thread_id}")
    logging.info(f"  In-Reply-To: {message_id}")
    logging.info(f"  References: {references}")
    logging.info(f"  CC List: {cc_list}")
    
    try:
        # Authenticate Gmail
        service = authenticate_gmail(GMAIL_CREDENTIALS, GMAIL_FROM_ADDRESS)
        
        if not service:
            raise ValueError("Failed to authenticate Gmail service")
        
        # Build proper References header for threading
        references_header = references
        if references and message_id:
            if message_id not in references:
                references_header = f"{references} {message_id}"
        elif message_id:
            references_header = message_id
        
        logging.info(f"Final References header: {references_header}")
        
        # Send email with proper threading
        result = send_email(
            service=service,
            recipient=recipient,
            subject=subject,
            body=html_body,
            in_reply_to=message_id,
            references=references_header,
            from_address=GMAIL_FROM_ADDRESS,
            cc=cc_list if cc_list else None,
            thread_id=thread_id,
            agent_name="API Test Agent"
        )
        
        if not result:
            raise ValueError("Failed to send email - no result returned")
        
        # Mark original email as read
        if original_email_id:
            mark_email_as_read(service, original_email_id)
        
        logging.info(f"Email sent successfully to {recipient}")
        logging.info(f"Thread ID: {thread_id}")
        logging.info(f"Sent Message ID: {result.get('id')}")
        if cc_list:
            logging.info(f"CC'd: {', '.join(cc_list)}")
        
        # Store confirmation
        ti.xcom_push(key="email_sent", value=True)
        ti.xcom_push(key="email_sent_timestamp", value=datetime.now().isoformat())
        ti.xcom_push(key="sent_message_id", value=result.get('id'))
        
    except Exception as e:
        logging.error(f"Failed to send email: {str(e)}", exc_info=True)
        raise


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
    readme_content = "API Test Case Executor - Automated testing workflow with PDF support"

with DAG(
    'api_test_case_executor',
    default_args=default_args,
    description='Automated API test case generation, execution, and reporting via email with PDF documentation support',
    schedule=None,  # Triggered by mailbox monitor
    start_date=datetime(2024, 2, 24),
    catchup=False,
    doc_md=readme_content,
    tags=['api', 'testing', 'automation', 'agent', 'email', 'pdf'],
) as dag:
    
    # Task 1: Extract inputs from email with JSON and PDF attachments
    extract_inputs = PythonOperator(
        task_id='extract_inputs',
        python_callable=extract_inputs_from_email,
        doc_md="Extracts API documentation from JSON attachments, PDF files, and parses email requirements"
    )
    
    # Task 2: Create and validate test cases with PDF context
    validate_test_cases = BranchPythonOperator(
        task_id='create_and_validate_test_cases',
        python_callable=create_and_validate_test_cases,
        doc_md="Generates comprehensive test cases incorporating PDF specifications and validates coverage"
    )
    
    # Task 3: Execute test cases
    execute_tests = PythonOperator(
        task_id='execute_test_cases',
        python_callable=execute_test_cases,
        doc_md="Executes all approved test cases and collects detailed results"
    )
    
    # Task 4: Generate email content with PDF insights
    generate_email = PythonOperator(
        task_id='generate_email_content',
        python_callable=generate_email_content,
        doc_md="Generates professional HTML email with test results, PDF context, and recommendations"
    )
    
    # Task 5: Send email response
    send_email_task = PythonOperator(
        task_id='send_response_email',
        python_callable=send_response_email,
        doc_md="Sends email response to original sender maintaining thread continuity"
    )
    
    # Task 6: Success marker
    workflow_complete = EmptyOperator(
        task_id='workflow_complete',
        trigger_rule='all_success'
    )
    
    # Define task dependencies
    extract_inputs >> validate_test_cases >> execute_tests >> generate_email >> send_email_task >> workflow_complete