from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.decorators import task
from datetime import datetime, timedelta
import os
import json
import re
import glob
import logging

from agent_dags.utils.email_utils import (
    authenticate_gmail,
    send_email,
    send_email_with_attachment,
    mark_email_as_read,
    extract_all_recipients,
)
from agent_dags.utils.agent_utils import get_ai_response, extract_json_from_text

GMAIL_FROM_ADDRESS = Variable.get("ltai.api.test.from_address", default_var="")
GMAIL_CREDENTIALS = Variable.get("ltai.api.test.gmail_credentials", default_var="")
MODEL_NAME = Variable.get("ltai.api.test.model.name", default_var="APITestAgent:5.0")


# ═══════════════════════════════════════════════════════════════
# Helper: replace ${filename.json} placeholders with actual data
# ═══════════════════════════════════════════════════════════════
def _replace_testdata_placeholders(collection_json_str: str, testdata_files: dict) -> str:
    """
    Replace ${filename.json} placeholders in the serialized collection JSON
    with the actual file contents.

    The placeholder sits inside a JSON string value, e.g. "raw": "${users.json}".
    We must produce valid JSON after replacement:
      1. json.dumps(content)  → e.g. '{"name": "John"}'
      2. json.dumps(step1)    → '"{\\"name\\": \\"John\\"}"'
      3. Strip outer quotes   → '{\\"name\\": \\"John\\"}'
      4. Replace placeholder with the escaped string
    """
    def _replacer(match):
        filename = match.group(1)
        if filename not in testdata_files:
            logging.warning(f"Testdata placeholder ${{{filename}}} not found in testdata_files")
            return match.group(0)
        content = testdata_files[filename]
        serialized = json.dumps(content)        # step 1: JSON string of the content
        escaped = json.dumps(serialized)         # step 2: double-encoded
        inner = escaped[1:-1]                    # step 3: strip outer quotes
        return inner

    return re.sub(r'\$\{([^}]+)\}', _replacer, collection_json_str)


# ═══════════════════════════════════════════════════════════════
# Task 1: Extract email metadata and discover test session files
# ═══════════════════════════════════════════════════════════════
@task
def extract_email_metadata(**kwargs):
    dag_run = kwargs.get("dag_run")
    conf = dag_run.conf

    email_id = conf.get("email_id")
    thread_id = conf.get("thread_id")
    email_headers = conf.get("email_headers", {})
    email_content = conf.get("email_content", "")

    base_dir = Variable.get("ltai.test.base_dir", default_var="/appz/pyunit_testing")
    session_dir = os.path.join(base_dir, thread_id)

    sender = email_headers.get("From", "")
    subject = email_headers.get("Subject", "")
    message_id = email_headers.get("Message-ID", "")
    references = email_headers.get("References", "")

    email_data_for_recipients = {"headers": email_headers}
    all_recipients = extract_all_recipients(email_data_for_recipients)

    result = {
        "email_id": email_id,
        "thread_id": thread_id,
        "sender": sender,
        "subject": subject,
        "message_id": message_id,
        "references": references,
        "all_recipients": all_recipients,
        "session_dir": session_dir,
        "session_exists": False,
        "test_files": [],
        "testdata_files": {},
    }

    if not os.path.isdir(session_dir):
        logging.warning(f"No test session directory found at {session_dir}")
        return result

    result["session_exists"] = True

    # Discover test files
    test_file_paths = sorted(glob.glob(os.path.join(session_dir, "test_*.py")))
    result["test_files"] = [os.path.basename(p) for p in test_file_paths]
    logging.info(f"Found {len(result['test_files'])} test file(s) in {session_dir}")

    # Load testdata JSON files
    testdata_dir = os.path.join(session_dir, "testdata")
    if os.path.isdir(testdata_dir):
        for json_path in glob.glob(os.path.join(testdata_dir, "*.json")):
            filename = os.path.basename(json_path)
            try:
                with open(json_path, "r", encoding="utf-8") as f:
                    result["testdata_files"][filename] = json.load(f)
                logging.info(f"Loaded testdata file: {filename}")
            except Exception as e:
                logging.warning(f"Failed to load testdata file {filename}: {e}")

    # Also load schema files from testdata/schemas/
    schemas_dir = os.path.join(testdata_dir, "schemas")
    if os.path.isdir(schemas_dir):
        for json_path in glob.glob(os.path.join(schemas_dir, "*.json")):
            filename = os.path.basename(json_path)
            try:
                with open(json_path, "r", encoding="utf-8") as f:
                    result["testdata_files"][filename] = json.load(f)
                logging.info(f"Loaded schema file: {filename}")
            except Exception as e:
                logging.warning(f"Failed to load schema file {filename}: {e}")

    return result


# ═══════════════════════════════════════════════════════════════
# Task 2: Convert test files to Postman collection via AI
# ═══════════════════════════════════════════════════════════════
@task
def convert_tests_to_postman(metadata: dict):
    thread_id = metadata["thread_id"]
    session_dir = metadata["session_dir"]
    test_files = metadata["test_files"]
    testdata_files = metadata["testdata_files"]

    if not metadata["session_exists"]:
        logging.warning("No test session found — returning error")
        return {"success": False, "error": "no_session", "collection": None}

    if not test_files:
        logging.warning("Session exists but no test files found")
        return {"success": False, "error": "no_test_files", "collection": None}

    # Master Postman collection shell
    master_collection = {
        "info": {
            "name": f"API Tests - {thread_id}",
            "_postman_id": thread_id,
            "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json",
        },
        "item": [],
        "variable": [
            {"key": "base_url", "value": "", "type": "string"}
        ],
    }

    CONVERSION_SYSTEM_PROMPT = (
        "You are a test-to-Postman converter. Given a Python pytest test file, "
        "convert each test function into a Postman v2.1 collection request item.\n\n"
        "Rules:\n"
        "- Extract HTTP method, URL, headers, query params, and request body from each test\n"
        "- When the test code reads JSON data from a file in testdata/ (e.g. "
        'open(..., "testdata/users.json")), use ${filename.json} as the body placeholder '
        "instead of inlining the data\n"
        "- Map os.getenv('VAR_NAME') calls to Postman {{VAR_NAME}} variable syntax\n"
        "- Convert assertions to pm.test() scripts in the Tests tab\n"
        "- If the test uses a base_url variable, use {{base_url}} in the URL\n"
        "- Return ONLY strict JSON, no markdown, no explanation\n\n"
        "Return format:\n"
        '{"items": [\n'
        "  {\n"
        '    "name": "Test name from function name",\n'
        '    "request": {\n'
        '      "method": "GET|POST|PUT|PATCH",\n'
        '      "header": [{"key": "...", "value": "...", "type": "text"}],\n'
        '      "url": {"raw": "{{base_url}}/path", "host": ["{{base_url}}"], "path": ["path"]},\n'
        '      "body": {"mode": "raw", "raw": "...", "options": {"raw": {"language": "json"}}}\n'
        "    },\n"
        '    "event": [{"listen": "test", "script": {"exec": ["pm.test(...)"], "type": "text/javascript"}}]\n'
        "  }\n"
        "]}"
    )

    total_requests = 0

    for test_file in test_files:
        file_path = os.path.join(session_dir, test_file)
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                file_content = f.read()
        except Exception as e:
            logging.error(f"Failed to read {test_file}: {e}")
            continue

        user_prompt = (
            f"Convert this pytest file to Postman collection items:\n\n"
            f"File: {test_file}\n"
            f"```python\n{file_content}\n```"
        )

        try:
            ai_response = get_ai_response(
                prompt=user_prompt,
                system_message=CONVERSION_SYSTEM_PROMPT,
                model=MODEL_NAME,
                stream=False,
            )
            parsed = extract_json_from_text(ai_response)

            if not parsed or "items" not in parsed:
                logging.warning(f"No valid items returned for {test_file}")
                continue

            items = parsed["items"]
            total_requests += len(items)

            # Wrap items in a folder named after the test file
            folder = {
                "name": test_file.replace(".py", ""),
                "item": items,
            }
            master_collection["item"].append(folder)
            logging.info(f"Converted {test_file}: {len(items)} request(s)")

        except Exception as e:
            logging.error(f"AI conversion failed for {test_file}: {e}")
            continue

    if total_requests == 0:
        logging.warning("AI could not convert any test files to Postman items")
        return {"success": False, "error": "conversion_failed", "collection": None}

    # Replace testdata placeholders with actual content
    collection_str = json.dumps(master_collection)
    collection_str = _replace_testdata_placeholders(collection_str, testdata_files)

    try:
        final_collection = json.loads(collection_str)
    except json.JSONDecodeError as e:
        logging.error(f"JSON decode error after placeholder replacement: {e}")
        final_collection = master_collection

    logging.info(
        f"Postman collection complete: {total_requests} request(s) "
        f"across {len(master_collection['item'])} folder(s)"
    )

    return {
        "success": True,
        "collection": final_collection,
        "total_requests": total_requests,
        "total_folders": len(final_collection["item"]),
    }


# ═══════════════════════════════════════════════════════════════
# Task 3: Generate reply email HTML
# ═══════════════════════════════════════════════════════════════
@task
def generate_reply_email(conversion_result: dict, metadata: dict):
    thread_id = metadata["thread_id"]
    test_files = metadata["test_files"]

    if conversion_result.get("success"):
        total_requests = conversion_result.get("total_requests", 0)
        total_folders = conversion_result.get("total_folders", 0)
        html_body = (
            "<p>Hi,</p>"
            "<p>Here is your <strong>Postman collection</strong> generated from the "
            f"test session <code>{thread_id}</code>.</p>"
            "<ul>"
            f"<li><strong>Total requests:</strong> {total_requests}</li>"
            f"<li><strong>Test files converted:</strong> {total_folders}</li>"
            "</ul>"
            "<p>Import the attached JSON file into Postman to use the collection. "
            "Environment variables referenced as <code>{{variable_name}}</code> will "
            "need to be configured in your Postman environment.</p>"
            "<p>Best regards,<br/>API Test Agent</p>"
        )
    else:
        error = conversion_result.get("error", "unknown")
        if error == "no_session":
            html_body = (
                "<p>Hi,</p>"
                "<p>No test session was found for this email thread. "
                "Please send your API documentation (Postman collection JSON and/or PDF) "
                "first so we can generate tests, then request the Postman collection export.</p>"
                "<p>Best regards,<br/>API Test Agent</p>"
            )
        elif error == "no_test_files":
            html_body = (
                "<p>Hi,</p>"
                "<p>A test session exists for this thread, but no test files were found. "
                "The tests may not have been generated yet. Please ensure the test pipeline "
                "has completed before requesting a Postman collection export.</p>"
                "<p>Best regards,<br/>API Test Agent</p>"
            )
        else:
            html_body = (
                "<p>Hi,</p>"
                "<p>We were unable to convert the test files into a Postman collection. "
                "Please try again or contact support if the issue persists.</p>"
                "<p>Best regards,<br/>API Test Agent</p>"
            )

    return {"html_body": html_body, "success": conversion_result.get("success", False)}


# ═══════════════════════════════════════════════════════════════
# Task 4: Send email (with attachment on success, plain on error)
# ═══════════════════════════════════════════════════════════════
@task
def send_postman_email(
    email_content: dict, conversion_result: dict, metadata: dict
):
    sender = metadata["sender"]
    subject = metadata["subject"]
    message_id = metadata["message_id"]
    references = metadata["references"]
    thread_id = metadata["thread_id"]
    email_id = metadata["email_id"]
    all_recipients = metadata["all_recipients"]
    html_body = email_content["html_body"]
    success = email_content["success"]

    service = authenticate_gmail(GMAIL_CREDENTIALS, GMAIL_FROM_ADDRESS)

    # Build references header
    references_header = references
    if references and message_id:
        if message_id not in references:
            references_header = f"{references} {message_id}"
    elif message_id:
        references_header = message_id

    reply_subject = subject if subject.lower().startswith("re:") else f"Re: {subject}"

    cc_list = [
        addr for addr in all_recipients.get("cc", [])
        if addr.lower() != GMAIL_FROM_ADDRESS.lower()
    ]

    if success:
        collection = conversion_result["collection"]
        attachment_data = json.dumps(collection, indent=2).encode("utf-8")
        attachment_filename = f"postman_collection_{thread_id}.json"

        result = send_email_with_attachment(
            service=service,
            recipient=sender,
            subject=reply_subject,
            body=html_body,
            in_reply_to=message_id,
            references=references_header,
            from_address=GMAIL_FROM_ADDRESS,
            attachment_data=attachment_data,
            attachment_filename=attachment_filename,
            attachment_mime_type="application/json",
            cc=cc_list if cc_list else None,
            thread_id=thread_id,
            agent_name="API Test Agent",
        )
    else:
        result = send_email(
            service=service,
            recipient=sender,
            subject=reply_subject,
            body=html_body,
            in_reply_to=message_id,
            references=references_header,
            from_address=GMAIL_FROM_ADDRESS,
            cc=cc_list if cc_list else None,
            thread_id=thread_id,
            agent_name="API Test Agent",
        )

    mark_email_as_read(service, email_id)

    logging.info(f"Postman export email sent to {sender} (success={success})")
    return {"sent": True, "success": success, "timestamp": datetime.now().isoformat()}


# ═══════════════════════════════════════════════════════════════
# DAG Definition
# ═══════════════════════════════════════════════════════════════
default_args = {
    "owner": "lowtouch.ai_developers",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=15),
}

with DAG(
    "postman_collection_exporter",
    default_args=default_args,
    description="Convert existing test session files into a Postman v2.1 collection and email it back",
    schedule=None,
    start_date=datetime(2024, 2, 24),
    catchup=False,
    doc_md="""
# Postman Collection Exporter

Triggered by the mailbox monitor when a user requests a Postman collection
from an existing test session.

**Pipeline**: extract_email_metadata → convert_tests_to_postman → generate_reply_email → send_postman_email

- Finds the test session directory by `thread_id`
- Reads each `test_*.py` file and asks AI to convert to Postman items
- Replaces `${filename.json}` placeholders with actual testdata content
- Sends the Postman v2.1 JSON as an email attachment
    """,
    tags=["api", "testing", "postman", "export"],
) as dag:

    metadata = extract_email_metadata()
    conversion_result = convert_tests_to_postman(metadata)
    email_content = generate_reply_email(conversion_result, metadata)
    send_result = send_postman_email(email_content, conversion_result, metadata)

    done = EmptyOperator(task_id="done", trigger_rule="all_success")
    send_result >> done
