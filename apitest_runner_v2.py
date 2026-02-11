from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.decorators import task
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
MODEL_NAME = Variable.get("ltai.model.name", default_var="APITestAgent:5.0")
server_host = Variable.get("ltai.server.host", default_var="http://localhost:8080")

MAX_FIX_ITERATIONS = 3


# ═══════════════════════════════════════════════════════════════
# Helpers: .env credential management
# ═══════════════════════════════════════════════════════════════
def _parse_config_yaml(config_path: str) -> dict:
    """
    Parses config.yaml and returns the full config dict.
    Returns empty dict if file is missing or unparseable.

    Expected config.yaml format:
    ─────────────────────────────────────────────────
    credentials:
      API_KEY: ltai.credentials.api_key             # Airflow variable name
      USERNAME: ltai.credentials.username
      PASSWORD: ltai.credentials.password
      BEARER_TOKEN: ltai.credentials.bearer_token

    auth:
      type: header            # header | basic | bearer | custom
      headers:                # used when type=header or type=custom
        X-API-Key: API_KEY    # header name → .env variable name
        X-Client-ID: CLIENT_ID
      username_var: USERNAME   # used when type=basic
      password_var: PASSWORD
      token_var: BEARER_TOKEN  # used when type=bearer
    ─────────────────────────────────────────────────
    """
    import yaml

    if not config_path or not os.path.exists(config_path):
        return {}

    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _create_env_file(test_session_id: str, config_path: str) -> str:
    """
    Reads the credentials section from config.yaml, resolves each value
    from Airflow Variables, and writes a .env file into the test session dir.
    Returns the .env file path (or empty string if nothing to write).
    """
    config_data = _parse_config_yaml(config_path)
    credentials_map = config_data.get("credentials", {})
    if not credentials_map:
        logging.info("No credentials section in config.yaml — skipping .env creation")
        return ""

    base_dir = Variable.get("ltai.test.base_dir", default_var="/appz/pyunit_testing")
    env_path = os.path.join(base_dir, test_session_id, ".env")

    lines = []
    for env_var_name, airflow_var_name in credentials_map.items():
        value = Variable.get(str(airflow_var_name), default_var="")
        if value:
            lines.append(f"{env_var_name}={value}")
            logging.info(f"Resolved credential {env_var_name} from Airflow variable {airflow_var_name}")
        else:
            logging.warning(f"Airflow variable '{airflow_var_name}' for {env_var_name} is empty or missing")

    if not lines:
        logging.info("No credentials resolved — skipping .env creation")
        return ""

    with open(env_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    logging.info(f"Created .env at {env_path} with {len(lines)} credential(s)")
    return env_path


def _build_auth_instructions(config_path: str) -> str:
    """
    Reads the auth section from config.yaml and builds a clear text block
    that tells the AI agent exactly how to set up authentication in test files.
    Returns empty string if no auth config is found.
    """
    config_data = _parse_config_yaml(config_path)
    credentials_map = config_data.get("credentials", {})
    auth_config = config_data.get("auth", {})

    if not credentials_map:
        # No credentials in config but auth is still required — agent must
        # derive the auth method entirely from the Postman collection / docs
        return (
            "AUTHENTICATION SETUP:\n"
            "─────────────────────\n"
            "No credentials were provided in config.yaml.\n"
            "The API requires authentication — determine the correct auth method\n"
            "(API key, Bearer token, Basic Auth, etc.) by inspecting the Postman\n"
            "collection, email body, and API documentation.\n"
            "If you find hardcoded tokens or keys in the Postman collection, use\n"
            "them via a .env file: load with `load_dotenv(Path(__file__).parent / '.env')`\n"
            "and read with `os.getenv('VARIABLE_NAME')`. NEVER hardcode secrets.\n"
        )

    # List of available env vars for the agent to reference
    env_vars = list(credentials_map.keys())

    lines = [
        "AUTHENTICATION SETUP:",
        "─────────────────────",
        "A .env file is placed in the SAME directory as the test files at runtime.",
        "Load it at the top of every test file:",
        "",
        "    import os",
        "    from pathlib import Path",
        "    from dotenv import load_dotenv",
        "    load_dotenv(Path(__file__).parent / '.env')",
        "",
        f"Available .env variables: {', '.join(env_vars)}",
        "",
    ]

    auth_type = auth_config.get("type", "").lower()

    if auth_type == "basic":
        u_var = auth_config.get("username_var", "USERNAME")
        p_var = auth_config.get("password_var", "PASSWORD")
        lines += [
            "Auth type: HTTP Basic Auth",
            "Use in requests like:",
            f"    auth = (os.getenv('{u_var}'), os.getenv('{p_var}'))",
            "    response = requests.get(url, auth=auth)",
        ]
    elif auth_type == "bearer":
        t_var = auth_config.get("token_var", "BEARER_TOKEN")
        lines += [
            "Auth type: Bearer Token",
            "Use in requests like:",
            f'    headers = {{"Authorization": f"Bearer {{os.getenv(\'{t_var}\')}}"}}',
            "    response = requests.get(url, headers=headers)",
        ]
    elif auth_type in ("header", "custom"):
        header_map = auth_config.get("headers", {})
        if header_map:
            lines.append("Auth type: Custom Headers")
            lines.append("Build headers dict like:")
            lines.append("    headers = {")
            for header_name, env_var_name in header_map.items():
                lines.append(f'        "{header_name}": os.getenv("{env_var_name}"),')
            lines.append("    }")
            lines.append("    response = requests.get(url, headers=headers)")
        else:
            lines.append("Auth type: Header-based (no specific headers configured)")
            lines.append("Put credentials in request headers as appropriate.")
    elif auth_type == "api_key":
        # api_key can be in header or query param
        key_var = auth_config.get("key_var", env_vars[0] if env_vars else "API_KEY")
        key_header = auth_config.get("header_name", "X-API-Key")
        lines += [
            "Auth type: API Key",
            "Use in requests like:",
            f'    headers = {{"{key_header}": os.getenv("{key_var}")}}',
            "    response = requests.get(url, headers=headers)",
        ]
    else:
        # No explicit type — agent must figure it out from the API docs / Postman collection
        lines += [
            "Auth type: NOT specified in config.",
            "Determine the correct authentication method by inspecting the API",
            "documentation (Postman collection), email instructions, and the",
            "variable names provided above.",
            "It could be Basic Auth, Bearer token, API key header, query param,",
            "or any other scheme — use whatever the API docs indicate.",
            "Read each credential with os.getenv('VARIABLE_NAME').",
        ]

    lines += [
        "",
        "CRITICAL: NEVER hardcode any credential values. Always use os.getenv().",
    ]

    return "\n".join(lines)


def _remove_env_file(test_session_id: str):
    """Removes the .env file from the test session directory."""
    base_dir = Variable.get("ltai.test.base_dir", default_var="/appz/pyunit_testing")
    env_path = os.path.join(base_dir, test_session_id, ".env")
    if os.path.exists(env_path):
        os.remove(env_path)
        logging.info(f"Removed .env at {env_path}")


# ═══════════════════════════════════════════════════════════════
# STEP 1: Extract and Parse Inputs from Email (NO CHANGE)
# ═══════════════════════════════════════════════════════════════
@task
def extract_inputs_from_email(**kwargs):
    """
    Extracts API documentation from JSON attachments, PDF files, and email metadata.
    """
    dag_run = kwargs.get('dag_run')

    try:
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

        # Extract email metadata
        sender = email_headers.get("From", "")
        subject = email_headers.get("Subject", "")
        message_id = email_headers.get("Message-ID", "")
        references = email_headers.get("References", "")

        email_data_for_recipients = {"headers": email_headers}
        all_recipient = extract_all_recipients(email_data_for_recipients)

        # Validate and load JSON
        if not json_files:
            raise ValueError("No JSON attachments found")

        json_path = json_files[0].get("path")
        with open(json_path, "r", encoding="utf-8") as f:
            api_documentation = json.load(f)

        # Process PDFs
        pdf_context = ""
        pdf_paths = []
        if has_pdf and pdf_files:
            for pdf_file in pdf_files:
                pdf_path = pdf_file.get("path")
                pdf_content = pdf_file.get("extracted_content", "")
                pdf_filename = pdf_file.get("filename", "")

                if pdf_path:
                    pdf_paths.append(pdf_path)
                if pdf_content:
                    pdf_context += f"\n\n--- PDF: {pdf_filename} ---\n{pdf_content}\n"

        # Get config path
        config_path = config_file.get("path") if config_file else None

        # Parse requirements
        parse_prompt = f"""
        Extract test requirements from email and documents:

        Subject: {subject}
        Email: {email_content}
        API Docs: {json.dumps(api_documentation, indent=2)[:2000]}...
        PDF Context: {"Available" if pdf_context else "None"}

        Return strict JSON:
        {{
            "test_requirements": "...",
            "special_instructions": "...",
            "priority_level": "high/medium/low",
            "base_url": "...",
            "pdf_insights": "...",
            "requires_authentication": true/false
        }}
        """

        parsed_response = get_ai_response(parse_prompt, model=MODEL_NAME)
        parsed_requirements = extract_json_from_text(parsed_response) or {
            "test_requirements": "Standard API testing",
            "special_instructions": "None",
            "priority_level": "medium",
            "requires_authentication": False
        }

        # Create test directory
        test_session_id = thread_id
        test_dir = Variable.get("ltai.test.base_dir", default_var="/appz/pyunit_testing") + f"/{test_session_id}"
        os.makedirs(test_dir, exist_ok=True)

        # Return all data as dict
        return {
            "sender_email": sender,
            "email_subject": subject,
            "message_id": message_id,
            "references": references,
            "thread_id": thread_id,
            "test_session_id": test_session_id,
            "original_email_id": email_id,
            "all_recipients": all_recipient,
            "api_documentation": api_documentation,
            "test_requirements": parsed_requirements.get("test_requirements"),
            "special_instructions": parsed_requirements.get("special_instructions"),
            "priority_level": parsed_requirements.get("priority_level"),
            "config_path": config_path,
            "base_url": parsed_requirements.get("base_url"),
            "requires_authentication": parsed_requirements.get("requires_authentication", False),
            "has_pdf": has_pdf,
            "pdf_context": pdf_context,
            "pdf_insights": parsed_requirements.get("pdf_insights", ""),
            "pdf_count": len(pdf_files)
        }

    except Exception as e:
        logging.error(f"Error extracting inputs: {str(e)}", exc_info=True)
        raise


# ═══════════════════════════════════════════════════════════════
# STEP 2: Generate Granular Sub-Test Scenarios
# ═══════════════════════════════════════════════════════════════
@task
def generate_sub_test_scenarios(email_data: dict):
    """
    Generates granular per-file sub-scenarios instead of broad categories.
    Each sub-scenario maps to one test file.
    """
    api_docs = email_data["api_documentation"]
    test_reqs = email_data["test_requirements"]
    special_instructions = email_data["special_instructions"]
    requires_auth = email_data["requires_authentication"]
    pdf_context = email_data["pdf_context"]
    pdf_insights = email_data["pdf_insights"]
    has_pdf = email_data["has_pdf"]

    pdf_info = ""
    if has_pdf and pdf_context:
        pdf_info = f"""
    **PDF Documentation**: {pdf_context[:2000]}...
    **Insights**: {pdf_insights}
    """

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    scenario_prompt = f"""
    Analyze the API documentation and generate a GRANULAR list of test sub-scenarios.
    Each sub-scenario will become ONE separate pytest test file.

    API Docs: {json.dumps(api_docs, indent=2)[:3000]}
    Requirements: {test_reqs}
    Special Instructions: {special_instructions}
    Requires Auth: {requires_auth}
    {pdf_info}

    Generate 5-15 sub-scenarios. Each should be specific enough for a single test file.
    Examples: "test_user_crud_positive", "test_input_validation", "test_auth_flows",
    "test_error_responses", "test_pagination", "test_rate_limiting", etc.

    Consider: Functional (positive/negative), Security, Error Handling, Data Validation,
    Boundary Testing, Integration, Performance edge cases.
    - User-specified categories from email/PDF take priority
    - Include authentication tests ONLY if requires_auth=true
    - NO DELETE endpoint tests

    **FILE NAMING**: Use format test_<descriptive_name>_{timestamp}.py
    Timestamp: {timestamp}

    Return STRICT JSON:
    {{
        "sub_scenarios": [
            {{
                "file_name": "test_user_crud_positive_{timestamp}.py",
                "description": "Positive CRUD operations for user endpoints",
                "endpoints": ["/api/users", "/api/users/{{id}}"],
                "test_type": "functional_positive",
                "priority": "high"
            }}
        ],
        "total_scenarios": 0,
        "estimated_test_count": 0,
        "timestamp": "{timestamp}"
    }}
    """

    response = get_ai_response(scenario_prompt, model=MODEL_NAME)
    scenarios_data = extract_json_from_text(response)
    logging.info(f"Sub-scenario generation response: {scenarios_data}")

    if not scenarios_data:
        raise ValueError(f"Invalid scenario JSON: {response[:500]}")

    sub_scenarios = scenarios_data.get("sub_scenarios", [])

    logging.info(f"Generated {len(sub_scenarios)} sub-scenarios:")
    for idx, s in enumerate(sub_scenarios):
        logging.info(f"  {idx+1}. {s['file_name']}: {s['description']}")

    return {
        "sub_scenarios": sub_scenarios,
        "total_scenarios": len(sub_scenarios),
        "estimated_test_count": scenarios_data.get("estimated_test_count", 0),
        "timestamp": timestamp,
        "email_data": email_data
    }


# ═══════════════════════════════════════════════════════════════
# STEP 3: Generate ALL Test Files (No Execution)
# ═══════════════════════════════════════════════════════════════
@task
def generate_all_test_files(scenario_data: dict):
    """
    Loops through all sub_scenarios and generates test files via the AI agent.
    Builds conversation_history incrementally so AI avoids duplicating tests.
    No execution happens here -- only file creation.
    """
    sub_scenarios = scenario_data["sub_scenarios"]
    email_data = scenario_data["email_data"]

    test_session_id = email_data["test_session_id"]
    api_docs = email_data["api_documentation"]
    test_reqs = email_data["test_requirements"]
    special_instructions = email_data["special_instructions"]
    config_path = email_data["config_path"]
    base_url = email_data.get("base_url") or "http://connector:8000"
    pdf_insights = email_data["pdf_insights"]
    requires_auth = email_data["requires_authentication"]

    config_info = ""
    if config_path and os.path.exists(config_path):
        config_info = f"**Config**: {config_path} (use for auth and base_url)"

    # Build auth instructions from config.yaml so the agent knows
    # exactly which env vars exist and how to use them
    auth_instructions = ""
    if requires_auth and config_path:
        auth_instructions = _build_auth_instructions(config_path)

    conversation_history = []
    generated_files = []

    for idx, scenario in enumerate(sub_scenarios):
        file_name = scenario["file_name"]
        description = scenario["description"]
        endpoints = scenario.get("endpoints", [])
        test_type = scenario.get("test_type", "functional")
        priority = scenario.get("priority", "medium")

        logging.info(f"═══════════════════════════════════════════════════")
        logging.info(f"Generating file {idx+1}/{len(sub_scenarios)}: {file_name}")
        logging.info(f"Description: {description}")
        logging.info(f"═══════════════════════════════════════════════════")

        generation_prompt = f"""
        Generate a COMPLETE Python pytest test file for the following sub-scenario.
        Save it in subdirectory "{test_session_id}" with filename "{file_name}".

        Sub-scenario: {description}
        File: {file_name}
        Test type: {test_type}
        Priority: {priority}
        Endpoints: {', '.join(endpoints) if endpoints else 'All relevant endpoints from the API docs'}
        Requires Auth: {requires_auth}
        {config_info}

        API Docs: {json.dumps(api_docs, indent=2)[:3000]}
        Requirements: {test_reqs}
        Special Instructions: {special_instructions}
        Base URL: {base_url}
        PDF Insights: {pdf_insights}

        {auth_instructions if auth_instructions else ""}

        Generate pytest file with:
        - All imports (pytest, requests, json, etc.)
        - Fixtures if needed (auth, test data)
        - MAXIMUM 10 test functions per file
        - Each test: descriptive name, docstring, ONE assertion, proper error handling
        - Use @pytest.mark.{test_type} markers
        - Parametrize for similar tests
        - Both positive and negative cases as appropriate for this test type

        RULES:
        - NO DELETE endpoint tests
        - ONE assertion per test
        - MAXIMUM 10 test cases per file - focus on the most important ones
        - Prefer Special Instructions
        - Only test documented endpoints
        - {"All credentials MUST come from .env via os.getenv() — NEVER hardcode secrets" if requires_auth else "Skip auth"}
        - Do NOT duplicate tests that were already generated in previous files

        Save to: {test_session_id}/{file_name}
        """

        gen_response = get_ai_response(
            generation_prompt,
            model=MODEL_NAME,
            conversation_history=conversation_history[-5:] if conversation_history else None
        )
        logging.info(f"Generated {file_name} ({len(gen_response)} chars)")

        # Add to conversation history for context
        conversation_history.append({
            "prompt": f"Generate tests for sub-scenario: {description} -> file: {file_name}",
            "response": gen_response[:1000]  # Trim to manage size
        })

        generated_files.append({
            "file_name": file_name,
            "description": description,
            "test_type": test_type,
            "priority": priority
        })

    logging.info(f"═══════════════════════════════════════════════════")
    logging.info(f"All {len(generated_files)} test files generated successfully")
    logging.info(f"═══════════════════════════════════════════════════")

    return {
        "generated_files": generated_files,
        "total_files": len(generated_files),
        "test_session_id": test_session_id,
        "email_data": email_data
    }


# ═══════════════════════════════════════════════════════════════
# STEP 4: Run ALL Tests Together
# ═══════════════════════════════════════════════════════════════
@task
def run_all_tests(generation_data: dict):
    """
    Runs a single consolidated pytest execution on the entire test session directory.
    Returns structured test results.
    """
    test_session_id = generation_data["test_session_id"]
    email_data = generation_data["email_data"]
    generated_files = generation_data["generated_files"]
    base_url = email_data.get("base_url") or "http://connector:8000"
    config_path = email_data.get("config_path")

    logging.info(f"Running ALL tests in session: {test_session_id}")
    logging.info(f"Total test files: {len(generated_files)}")

    # Create .env with credentials resolved from Airflow Variables
    env_path = _create_env_file(test_session_id, config_path)
    if env_path:
        logging.info(f"Credentials .env ready at {env_path}")

    config_info = ""
    if config_path:
        config_info = f"Config file: {config_path}"

    execution_prompt = f"""
    Run pytest on ALL test files in the "{test_session_id}" directory.

    Use the run_pytest tool with:
    - target_path: "{test_session_id}"
    - verbose: True
    - generate_html_report: True

    Base URL: {base_url}
    {config_info}

    After execution, return STRICT JSON with the results:
    {{
        "status": "success" or "error",
        "summary": {{
            "total": 0,
            "passed": 0,
            "failed": 0,
            "errors": 0,
            "skipped": 0,
            "pass_rate": 0.0,
            "execution_time_seconds": 0.0
        }},
        "failed_tests": [
            {{
                "file_name": "test_xxx.py",
                "test_name": "test_function_name",
                "error_type": "AssertionError|ImportError|etc",
                "error_message": "brief error description",
                "traceback": "relevant traceback snippet"
            }}
        ],
        "report_url": "URL to the generated HTML report",
        "exit_code": 0
    }}

    status="success": Tests ran (even if some failed)
    status="error": Execution itself failed (infrastructure error)
    """

    exec_response = get_ai_response(execution_prompt, model=MODEL_NAME)
    logging.info(f"Test execution response: {exec_response[:500]}...")

    result = extract_json_from_text(exec_response)

    if not result:
        result = {
            "status": "error",
            "summary": {"total": 0, "passed": 0, "failed": 0, "errors": 0, "skipped": 0, "pass_rate": 0.0},
            "failed_tests": [],
            "report_url": "",
            "exit_code": -1,
            "raw_response": exec_response[:1000]
        }

    summary = result.get("summary", {})
    logging.info(f"=== TEST RESULTS ===")
    logging.info(f"Total: {summary.get('total', 0)}")
    logging.info(f"Passed: {summary.get('passed', 0)}")
    logging.info(f"Failed: {summary.get('failed', 0)}")
    logging.info(f"Errors: {summary.get('errors', 0)}")
    logging.info(f"Pass Rate: {summary.get('pass_rate', 0)}%")

    return {
        "test_results": result,
        "test_session_id": test_session_id,
        "generated_files": generated_files,
        "email_data": email_data
    }


# ═══════════════════════════════════════════════════════════════
# STEP 5: Fix and Retry Loop
# ═══════════════════════════════════════════════════════════════
@task(retries=0)
def fix_and_retry_loop(run_data: dict):
    """
    Iteratively fixes failing test files and re-runs until passing or max retries.
    This is a Python-level loop (NOT Airflow retries) to maintain state across iterations.
    """
    test_results = run_data["test_results"]
    test_session_id = run_data["test_session_id"]
    generated_files = run_data["generated_files"]
    email_data = run_data["email_data"]
    base_url = email_data.get("base_url") or "http://connector:8000"
    config_path = email_data.get("config_path")

    config_info = ""
    if config_path:
        config_info = f"Config file: {config_path}"

    auth_instructions = ""
    if email_data.get("requires_authentication") and config_path:
        auth_instructions = _build_auth_instructions(config_path)

    current_results = test_results
    iteration = 0
    final_report_url = current_results.get("report_url", "")

    try:
        # Check if tests already all pass or if there was an infrastructure error
        if current_results.get("status") == "error" and current_results.get("exit_code", 0) >= 2:
            logging.info("Infrastructure error detected (exit_code >= 2). Skipping fix loop.")
            return {
                "final_results": current_results,
                "iterations": 0,
                "outcome": "infrastructure_error",
                "test_session_id": test_session_id,
                "email_data": email_data,
                "report_url": final_report_url
            }

        failed_tests = current_results.get("failed_tests", [])
        if not failed_tests:
            summary = current_results.get("summary", {})
            if summary.get("failed", 0) == 0 and summary.get("errors", 0) == 0:
                logging.info("All tests passed on first run! No fixes needed.")
                return {
                    "final_results": current_results,
                    "iterations": 0,
                    "outcome": "all_passed",
                    "test_session_id": test_session_id,
                    "email_data": email_data,
                    "report_url": final_report_url
                }

        previous_failure_key = None

        for iteration in range(1, MAX_FIX_ITERATIONS + 1):
            logging.info(f"═══════════════════════════════════════════════════")
            logging.info(f"FIX ITERATION {iteration}/{MAX_FIX_ITERATIONS}")
            logging.info(f"═══════════════════════════════════════════════════")

            failed_tests = current_results.get("failed_tests", [])
            if not failed_tests:
                logging.info("No failed tests. Exiting fix loop.")
                break

            # Build a key representing current failures for progress detection
            current_failure_key = "|".join(
                sorted(f"{ft.get('file_name', '')}::{ft.get('test_name', '')}" for ft in failed_tests)
            )

            if current_failure_key == previous_failure_key:
                logging.info("No progress detected (same failures as last iteration). Stopping.")
                break

            previous_failure_key = current_failure_key

            # Group failures by file
            failures_by_file = {}
            for ft in failed_tests:
                fname = ft.get("file_name", "unknown")
                if fname not in failures_by_file:
                    failures_by_file[fname] = []
                failures_by_file[fname].append(ft)

            logging.info(f"Fixing {len(failures_by_file)} file(s) with failures:")
            for fname, failures in failures_by_file.items():
                logging.info(f"  {fname}: {len(failures)} failing test(s)")

            # Fix each failing file
            for fname, failures in failures_by_file.items():
                error_details = "\n".join(
                    f"- {ft.get('test_name', 'unknown')}: [{ft.get('error_type', 'Error')}] "
                    f"{ft.get('error_message', 'No message')}\n"
                    f"  Traceback: {ft.get('traceback', 'N/A')[:300]}"
                    for ft in failures
                )

                fix_prompt = f"""
                The following test file has failing tests that need to be FIXED.

                File: {test_session_id}/{fname}

                FAILING TESTS:
                {error_details}

                Instructions:
                1. Read the existing test file {test_session_id}/{fname}
                2. Fix ONLY the failing tests listed above
                3. PRESERVE all passing tests unchanged
                4. Save the corrected file back using save_pytest_file to {test_session_id}/{fname}

                Base URL: {base_url}
                {config_info}

                {auth_instructions if auth_instructions else ""}

                RULES:
                - Do NOT delete or modify passing tests
                - Fix the root cause of each failure (wrong assertions, bad URLs, missing imports, etc.)
                - Keep the same file name and test structure where possible
                - {"All credentials MUST come from .env via os.getenv() — NEVER hardcode secrets" if auth_instructions else ""}
                - NO DELETE endpoint tests
                - ONE assertion per test
                """

                logging.info(f"Sending fix prompt for {fname}...")
                fix_response = get_ai_response(fix_prompt, model=MODEL_NAME)
                logging.info(f"Fix response for {fname}: {fix_response[:300]}...")

            # Re-run ALL tests after fixes
            logging.info(f"Re-running all tests after iteration {iteration} fixes...")
            rerun_prompt = f"""
            Run pytest on ALL test files in the "{test_session_id}" directory.

            Use the run_pytest tool with:
            - target_path: "{test_session_id}"
            - verbose: True
            - generate_html_report: True

            Base URL: {base_url}
            {config_info}

            After execution, return STRICT JSON with the results:
            {{
                "status": "success" or "error",
                "summary": {{
                    "total": 0,
                    "passed": 0,
                    "failed": 0,
                    "errors": 0,
                    "skipped": 0,
                    "pass_rate": 0.0,
                    "execution_time_seconds": 0.0
                }},
                "failed_tests": [
                    {{
                        "file_name": "test_xxx.py",
                        "test_name": "test_function_name",
                        "error_type": "AssertionError|ImportError|etc",
                        "error_message": "brief error description",
                        "traceback": "relevant traceback snippet"
                    }}
                ],
                "report_url": "URL to the generated HTML report",
                "exit_code": 0
            }}
            """

            rerun_response = get_ai_response(rerun_prompt, model=MODEL_NAME)
            rerun_result = extract_json_from_text(rerun_response)

            if not rerun_result:
                logging.error(f"Could not parse re-run results at iteration {iteration}")
                rerun_result = {
                    "status": "error",
                    "summary": {"total": 0, "passed": 0, "failed": 0, "errors": 0, "skipped": 0, "pass_rate": 0.0},
                    "failed_tests": [],
                    "report_url": "",
                    "exit_code": -1
                }

            current_results = rerun_result
            final_report_url = rerun_result.get("report_url", final_report_url)

            summary = rerun_result.get("summary", {})
            logging.info(f"Iteration {iteration} results: "
                         f"{summary.get('passed', 0)}/{summary.get('total', 0)} passed, "
                         f"{summary.get('failed', 0)} failed, {summary.get('errors', 0)} errors")

            # Exit conditions
            if rerun_result.get("exit_code", 0) >= 2:
                logging.info(f"Infrastructure error at iteration {iteration}. Stopping.")
                break

            new_failed = rerun_result.get("failed_tests", [])
            if not new_failed and summary.get("failed", 0) == 0 and summary.get("errors", 0) == 0:
                logging.info(f"All tests passed after iteration {iteration}!")
                break

        # Determine outcome
        final_failed = current_results.get("failed_tests", [])
        final_summary = current_results.get("summary", {})
        if not final_failed and final_summary.get("failed", 0) == 0 and final_summary.get("errors", 0) == 0:
            outcome = "all_passed"
        elif current_results.get("exit_code", 0) >= 2:
            outcome = "infrastructure_error"
        elif iteration >= MAX_FIX_ITERATIONS:
            outcome = "max_iterations_reached"
        else:
            outcome = "partial_pass"

        logging.info(f"═══════════════════════════════════════════════════")
        logging.info(f"Fix loop complete. Outcome: {outcome}, Iterations: {iteration}")
        logging.info(f"═══════════════════════════════════════════════════")

        return {
            "final_results": current_results,
            "iterations": iteration,
            "outcome": outcome,
            "test_session_id": test_session_id,
            "email_data": email_data,
            "report_url": final_report_url
        }

    finally:
        # Always clean up .env to avoid leaking credentials on disk
        _remove_env_file(test_session_id)
        logging.info("Credential .env cleanup complete")


# ═══════════════════════════════════════════════════════════════
# STEP 6: Generate Email Content
# ═══════════════════════════════════════════════════════════════
@task
def generate_email_content(fix_loop_data: dict):
    """
    Generates simple HTML email with overall metrics and link to detailed report.
    Input comes from fix_and_retry_loop output.
    """
    final_results = fix_loop_data["final_results"]
    iterations = fix_loop_data["iterations"]
    outcome = fix_loop_data["outcome"]
    email_data = fix_loop_data["email_data"]
    test_session_id = fix_loop_data["test_session_id"]
    report_url = fix_loop_data.get("report_url", "")

    sender = email_data["sender_email"]
    subject = email_data["email_subject"]
    has_pdf = email_data["has_pdf"]
    pdf_count = email_data["pdf_count"]

    summary = final_results.get("summary", {})
    total_tests = summary.get("total", 0)
    passed = summary.get("passed", 0)
    failed = summary.get("failed", 0)
    errors = summary.get("errors", 0)
    skipped = summary.get("skipped", 0)
    pass_rate = summary.get("pass_rate", 0.0)
    if total_tests > 0 and pass_rate == 0.0:
        pass_rate = round(passed / total_tests * 100, 2)

    # Generate report link (fallback if not from results)
    if not report_url:
        report_url = f"{server_host}/static/pytest_reports/{test_session_id}/index.html"

    # Try to get Postman collection
    postman_url = ""
    try:
        postman_prompt = (
            f"Generate postman collection URL for {test_session_id}: "
            f"{{postman_collection_url: {server_host}/static/postman_reports/xxx.json}}"
        )
        postman_resp = get_ai_response(postman_prompt, model=MODEL_NAME)
        postman_json = extract_json_from_text(postman_resp) or {}
        postman_url = postman_json.get("postman_collection_url", "")
    except Exception:
        pass

    # Determine overall status
    if pass_rate >= 95:
        status_icon = "✅"
        status_text = "All Tests Passed"
        status_color = "#28a745"
    elif pass_rate >= 80:
        status_icon = "✓"
        status_text = "Most Tests Passed"
        status_color = "#28a745"
    elif pass_rate >= 50:
        status_icon = "⚠"
        status_text = "Some Tests Failed"
        status_color = "#ffc107"
    else:
        status_icon = "✗"
        status_text = "Critical Failures"
        status_color = "#dc3545"

    pdf_note = ""
    if has_pdf:
        pdf_note = f"\n\n**PDF Documentation**: {pdf_count} document(s) analyzed and incorporated into test scenarios."

    iteration_note = ""
    if iterations > 0:
        iteration_note = f"\n- Tests refined over {iterations} iteration(s)"

    email_prompt = f"""Generate ONLY complete HTML email (<!DOCTYPE html>...</html>).
NO markdown, NO code blocks, NO explanations.

Subject: <!-- Re: {subject} -->
To: {sender}

Create a CLEAN, SIMPLE email with:

**DESIGN**: Professional, minimal, mobile-friendly (max-width: 600px)
**STYLE**: Inline CSS only, modern card-based layout

**CONTENT STRUCTURE**:

1. **Header Section**:
   - Brief greeting
   - "Your API test execution is complete"

2. **Overall Metrics Card** (large, prominent):
   ┌──────────────────────────────────┐
   │  {status_icon} {status_text}     │
   │                                   │
   │  Total Tests:    {total_tests}
   │  Passed:         {passed} ({pass_rate}%)
   │  Failed:         {failed}
   │  Errors:         {errors}
   │  Skipped:        {skipped}
   │                                   │
   │  Pass Rate: {pass_rate}%          │
   │  [Progress bar visualization]     │
   └──────────────────────────────────┘

3. **Quick Summary** (1-2 sentences):
   - Brief interpretation of results
   - Example: "Your API passed {passed} out of {total_tests} tests."
   {pdf_note}
   {iteration_note}

4. **Links Section** (prominent buttons):
   - 📊 View Detailed Report: {report_url}
   - 📦 Download Postman Collection: {postman_url}

5. **Footer**:
   - Professional closing
   - "For detailed breakdown by scenario, please view the full report"

**COLOR SCHEME**:
- Primary: {status_color}
- Pass: #28a745
- Fail: #dc3545
- Background: #f8f9fa
- Cards: white with subtle shadow

**CRITICAL REQUIREMENTS**:
- Keep it SIMPLE - only overall metrics, NO scenario breakdown tables
- Make the detailed report link VERY prominent
- Use large, readable fonts for metrics
- Include a visual progress bar for pass rate
- Mobile responsive
- Professional but not overwhelming

**DO NOT INCLUDE**:
- Detailed scenario breakdowns
- Individual test results
- Long lists or tables
- Multiple sections

Output ONLY the HTML document."""

    response = get_ai_response(email_prompt, model=MODEL_NAME)

    # Clean response
    html = response.strip()
    html = re.sub(r'^\s*<think>.*?</think>\s*', '', html, flags=re.DOTALL | re.IGNORECASE)
    html = html.removeprefix("```html").removesuffix("```").strip()
    html = html.removeprefix("```").removesuffix("```").strip()

    if not html.startswith(("<!DOCTYPE", "<html")):
        raise ValueError("Invalid HTML response")

    return {
        "subject": f"Re: {subject}",
        "html_body": html,
        "email_data": email_data,
        "postman_url": postman_url
    }


# ═══════════════════════════════════════════════════════════════
# STEP 7: Send Email
# ═══════════════════════════════════════════════════════════════
@task
def send_response_email(email_content: dict):
    """
    Sends the email response.
    """
    email_data = email_content["email_data"]

    recipient = email_data["sender_email"]
    subject = email_content["subject"]
    html_body = email_content["html_body"]
    message_id = email_data["message_id"]
    references = email_data["references"]
    thread_id = email_data["thread_id"]
    original_email_id = email_data["original_email_id"]
    all_recipients = email_data["all_recipients"]

    cc_list = all_recipients.get('cc', [])

    # Authenticate
    service = authenticate_gmail(GMAIL_CREDENTIALS, GMAIL_FROM_ADDRESS)

    # Build references
    references_header = references
    if references and message_id:
        if message_id not in references:
            references_header = f"{references} {message_id}"
    elif message_id:
        references_header = message_id

    # Send email
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
        agent_name="API Test Agent - Scenario Based"
    )

    # Mark as read
    if original_email_id:
        mark_email_as_read(service, original_email_id)

    logging.info(f"Email sent to {recipient}")
    logging.info(f"  Thread: {thread_id}")
    if cc_list:
        logging.info(f"  CC: {', '.join(cc_list)}")

    return {"sent": True, "timestamp": datetime.now().isoformat()}


# ═══════════════════════════════════════════════════════════════
# DAG Definition
# ═══════════════════════════════════════════════════════════════
default_args = {
    'owner': 'lowtouch.ai_developers',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 3,
    'retry_delay': timedelta(seconds=15),
}

with DAG(
    'api_test_executor_scenario_based',
    default_args=default_args,
    description='Generate-all, run-all, fix-and-retry API testing workflow',
    schedule=None,
    start_date=datetime(2024, 2, 24),
    catchup=False,
    doc_md="""
# API Test Executor - Scenario Based (v2)

Generate-all, run-all, fix-and-retry API testing workflow:

1. **Extract**: Parse email, JSON API docs, PDF specs
2. **Sub-Scenarios**: AI generates 5-15 granular per-file test scenarios
3. **Generate All**: Create all test files first (no execution)
   - Conversation history passed between generations to avoid duplication
4. **Run All**: Single consolidated pytest run on session directory
5. **Fix & Retry**: Parse errors, fix specific failing files, re-run (up to 3x)
   - Groups failures by file
   - Sends targeted fix prompts
   - Detects no-progress and infrastructure errors
6. **Report**: Simple HTML email with overall metrics + report link
7. **Send**: Email with thread continuity

## Key Improvements over v1
- Test files generated first, then run together (not generate-then-run per file)
- Fix loop targets specific failures instead of regenerating entire files
- Python-level retry loop maintains state across iterations
- Single consolidated pytest run for accurate cross-file metrics
    """,
    tags=['api', 'testing', 'scenario-based', 'pytest', 'ai-agent'],
) as dag:

    # Step 1: Extract email data
    email_data = extract_inputs_from_email()

    # Step 2: Generate granular sub-scenarios
    scenario_data = generate_sub_test_scenarios(email_data)

    # Step 3: Generate all test files
    generation_data = generate_all_test_files(scenario_data)

    # Step 4: Run all tests together
    run_data = run_all_tests(generation_data)

    # Step 5: Fix and retry loop
    fix_loop_data = fix_and_retry_loop(run_data)

    # Step 6: Generate email content
    email_content = generate_email_content(fix_loop_data)

    # Step 7: Send email
    send_result = send_response_email(email_content)

    # Done
    workflow_complete = EmptyOperator(
        task_id='workflow_complete',
        trigger_rule='all_success'
    )

    send_result >> workflow_complete
