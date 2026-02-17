from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.decorators import task
from datetime import datetime, timedelta
import os
import json
import logging
import re
import shutil

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
MODEL_NAME = Variable.get("ltai.api.test.model.name", default_var="APITestAgent:5.0")
server_host = Variable.get("ltai.server.host", default_var="http://localhost:8080")

MAX_FIX_ITERATIONS = 3
TEST_MODE = Variable.get("ltai.api.test.test_mode", default_var="false").lower() == "true"
TARGET_ENDPOINT = Variable.get("ltai.api.test.target_endpoint", default_var="")  # e.g. "/api/users" — only test this endpoint


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
            "them via a .env file: load with `load_dotenv(Path(__file__).parent / '.env', override=True)`\n"
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
        "    load_dotenv(Path(__file__).parent / '.env', override=True)",
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
# Helper: Parse proto files for package, service, and rpc names
# ═══════════════════════════════════════════════════════════════
def _parse_proto_services(test_dir: str, proto_filenames: list) -> list:
    """
    Parse .proto files to extract fully-qualified service/method names.

    Returns list of dicts: [{"fqn": "pkg.Service/Method", "service": ..., "method": ...}]
    """
    services = []
    for pf in proto_filenames:
        path = os.path.join(test_dir, pf)
        if not os.path.exists(path):
            continue
        with open(path, 'r') as f:
            content = f.read()
        pkg_match = re.search(r'^package\s+([\w.]+)\s*;', content, re.MULTILINE)
        package = pkg_match.group(1) if pkg_match else ""
        for svc_match in re.finditer(
            r'service\s+(\w+)\s*\{([^}]*)\}', content, re.DOTALL
        ):
            svc_name = svc_match.group(1)
            svc_body = svc_match.group(2)
            for rpc_match in re.finditer(r'rpc\s+(\w+)\s*\(', svc_body):
                method = rpc_match.group(1)
                fqn = f"{package}.{svc_name}/{method}" if package else f"{svc_name}/{method}"
                services.append({"fqn": fqn, "service": svc_name, "method": method, "package": package})
    return services


# ═══════════════════════════════════════════════════════════════
# Helper: Protocol-specific instructions (REST vs gRPC)
# ═══════════════════════════════════════════════════════════════
def _build_protocol_instructions(api_protocol: str, proto_files: list = None, grpc_services: list = None) -> str:
    """
    Return a block of text that tells the AI agent how to write tests
    for the given API protocol.
    """
    if api_protocol == "grpc":
        # Build proto file instructions when .proto files are available
        proto_note = ""
        service_fqn = "package.Service/Method"
        if proto_files:
            proto_list = ", ".join(proto_files)
            proto_flags = " ".join(f"-proto {pf}" for pf in proto_files)
            proto_note = f"""
        PROTO FILES (available in the test session directory):
            Files: {proto_list}
            These .proto files define the gRPC service descriptors.
            When using grpcurl, you MUST include these flags so grpcurl
            does not rely on server reflection:
                -import-path . {proto_flags}
            """

        # Include real service/method names parsed from proto files
        if grpc_services:
            svc_lines = "\n            ".join(
                f'- {s["fqn"]}' for s in grpc_services
            )
            proto_note += f"""
        DISCOVERED gRPC SERVICES & METHODS (use these EXACT fully-qualified names):
            {svc_lines}
            IMPORTANT: Use the fully-qualified name (package.ServiceName/MethodName) as shown above.
            Do NOT guess or fabricate service/method names.
            """
            service_fqn = grpc_services[0]["fqn"]

        grpcurl_example = (
            '    ["grpcurl", "-plaintext", "-d", f"@{tmp_path}",\n'
            f'                     BASE_URL, "{service_fqn}"]'
        )
        if proto_files:
            proto_args = ", ".join(
                '"-proto", "{}"'.format(pf) for pf in proto_files
            )
            grpcurl_example = (
                '    ["grpcurl", "-plaintext",\n'
                '                     "-import-path", ".", ' + proto_args + ',\n'
                '                     "-d", f"@{tmp_path}",\n'
                f'                     BASE_URL, "{service_fqn}"]'
            )

        return f"""
        API PROTOCOL: gRPC
        ──────────────────
        This is a gRPC API — do NOT use the `requests` library.
        {proto_note}
        REQUIRED IMPORTS:
            import grpc
            import json
            import os
            from google.protobuf.json_format import ParseDict, MessageToDict
            from google.protobuf import descriptor_pool, symbol_database

        CONNECTION:
            channel = grpc.insecure_channel(BASE_URL)
            # or grpc.secure_channel(BASE_URL, grpc.ssl_channel_credentials())

        LARGE PAYLOADS — set max message size on the channel:
            channel = grpc.insecure_channel(
                BASE_URL,
                options=[
                    ('grpc.max_send_message_length',    50 * 1024 * 1024),
                    ('grpc.max_receive_message_length',  50 * 1024 * 1024),
                ],
            )

        CALLING RPCs:
          Option A — Proto stubs (if *_pb2.py / *_pb2_grpc.py files exist):
            from generated import service_pb2, service_pb2_grpc
            stub = service_pb2_grpc.MyServiceStub(channel)
            request = service_pb2.MyRequest(field1="value")
            response = stub.MyMethod(request)

          Option B — grpcurl subprocess (when stubs are unavailable):
            import subprocess, json, tempfile, os
            payload = json.dumps(request_data)
            # IMPORTANT: Write payload to a temp file to avoid "Argument list too long"
            # errors with large payloads. Use grpcurl's -d @filename syntax.
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
                tmp.write(payload)
                tmp_path = tmp.name
            try:
                result = subprocess.run(
                {grpcurl_example},
                    capture_output=True, text=True, timeout=60,
                    cwd=os.path.dirname(os.path.abspath(__file__))
                )
            finally:
                os.unlink(tmp_path)
            assert result.returncode == 0
            response = json.loads(result.stdout)

          Option C — grpc_requests library (JSON-friendly):
            from grpc_requests import Client
            client = Client(BASE_URL)
            response = client.request("package.Service", "Method", request_data)

        ASSERTIONS — use gRPC status codes, not HTTP status codes:
            assert response is not None
            # For error tests, catch grpc.RpcError:
            with pytest.raises(grpc.RpcError) as exc_info:
                stub.MyMethod(bad_request)
            assert exc_info.value.code() == grpc.StatusCode.INVALID_ARGUMENT

        STREAMING RPCs:
            # Server streaming
            responses = list(stub.MyServerStream(request))
            assert len(responses) > 0

            # Client streaming
            def request_iterator():
                for item in items:
                    yield service_pb2.MyRequest(**item)
            response = stub.MyClientStream(request_iterator())

        METADATA (equivalent of HTTP headers):
            response = stub.MyMethod(request, metadata=[
                ('authorization', f'Bearer {{os.getenv("TOKEN")}}'),
            ])
        """
    else:
        return """
        API PROTOCOL: REST (HTTP)
        ─────────────────────────
        Use the `requests` library for all API calls.

        IMPORTS:
            import requests
            import json
            import os

        CALLING ENDPOINTS:
            response = requests.get(f"{BASE_URL}/endpoint")
            response = requests.post(f"{BASE_URL}/endpoint", json=data)
            response = requests.put(f"{BASE_URL}/endpoint/{id}", json=data)
            response = requests.patch(f"{BASE_URL}/endpoint/{id}", json=data)

        ASSERTIONS — use HTTP status codes:
            assert response.status_code == 200
            assert response.json()["field"] == expected_value

        HEADERS / AUTH:
            headers = {"Authorization": f"Bearer {os.getenv('TOKEN')}"}
            response = requests.get(url, headers=headers)
        """


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
        testing_type = conf.get('testing_type', 'api_only')
        api_protocol = conf.get('api_protocol', 'rest')
        proto_files = conf.get('proto_files', [])

        if not email_id:
            raise ValueError("No email_id provided in DAG configuration")

        logging.info(f"Processing email ID: {email_id}")
        logging.info(f"Thread ID: {thread_id}")
        logging.info(f"Testing type: {testing_type}")
        logging.info(f"API protocol: {api_protocol}")

        # Extract email metadata
        sender = email_headers.get("From", "")
        subject = email_headers.get("Subject", "")
        message_id = email_headers.get("Message-ID", "")
        references = email_headers.get("References", "")

        email_data_for_recipients = {"headers": email_headers}
        all_recipient = extract_all_recipients(email_data_for_recipients)

        # Load API documentation from available sources
        api_documentation = {}
        if json_files:
            json_path = json_files[0].get("path")
            with open(json_path, "r", encoding="utf-8") as f:
                api_documentation = json.load(f)
            logging.info(f"Loaded API documentation from JSON: {json_path}")
        else:
            logging.info(
                "No JSON attachments — will use PDF/email content as API documentation source"
            )

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
        api_docs_section = (
            f"API Docs (Postman Collection): {json.dumps(api_documentation, indent=2)[:2000]}..."
            if api_documentation
            else "API Docs: No Postman collection provided — extract API details from the email body and PDF documents below."
        )

        parse_prompt = f"""
        Extract test requirements from email and documents:

        Subject: {subject}
        Email: {email_content}
        {api_docs_section}
        PDF Context: {pdf_context[:3000] if pdf_context else "None"}

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

        # Copy .proto files into the test session directory
        proto_filenames = []
        for proto in proto_files:
            src_path = proto.get("path", "")
            original_name = proto.get("filename", "")
            if src_path and os.path.exists(src_path) and original_name:
                dest_path = os.path.join(test_dir, original_name)
                shutil.copy2(src_path, dest_path)
                proto_filenames.append(original_name)
                logging.info(f"Copied .proto file to test dir: {original_name}")
        if proto_filenames:
            logging.info(f"Total .proto files copied: {len(proto_filenames)}")

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
            "pdf_count": len(pdf_files),
            "testing_type": testing_type,
            "email_content": email_content,
            "api_protocol": api_protocol,
            "proto_files": proto_filenames,
        }

    except Exception as e:
        logging.error(f"Error extracting inputs: {str(e)}", exc_info=True)
        raise


# ═══════════════════════════════════════════════════════════════
# STEP 2: Generate Granular Sub-Test Scenarios
# ═══════════════════════════════════════════════════════════════


def _generate_api_sub_scenarios(email_data: dict) -> dict:
    """
    Generates standard per-endpoint sub-scenarios (existing behaviour).
    Each sub-scenario maps to one independent test file.
    """
    api_docs = email_data["api_documentation"]
    test_reqs = email_data["test_requirements"]
    special_instructions = email_data["special_instructions"]
    requires_auth = email_data["requires_authentication"]
    pdf_context = email_data["pdf_context"]
    pdf_insights = email_data["pdf_insights"]
    has_pdf = email_data["has_pdf"]
    api_protocol = email_data.get("api_protocol", "rest")

    pdf_info = ""
    if has_pdf and pdf_context:
        pdf_info = f"""
    **PDF Documentation**: {pdf_context[:2000]}...
    **Insights**: {pdf_insights}
    """

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    endpoint_constraint = ""
    if TARGET_ENDPOINT:
        endpoint_constraint = (
            f"IMPORTANT: Only generate scenarios for the endpoint: {TARGET_ENDPOINT}\n"
            "    Focus all test scenarios exclusively on this endpoint."
        )

    protocol_note = (
        "**API PROTOCOL**: gRPC — scenarios should target gRPC service methods "
        "(e.g. 'test_GetUser_positive', 'test_CreateOrder_validation'). "
        "Use gRPC status codes, not HTTP status codes."
        if api_protocol == "grpc"
        else "**API PROTOCOL**: REST — scenarios should target HTTP endpoints."
    )

    scenario_prompt = f"""
    Analyze the API documentation and generate a GRANULAR list of test sub-scenarios.
    Each sub-scenario will become ONE separate pytest test file.

    {protocol_note}

    API Docs: {json.dumps(api_docs, indent=2)[:3000]}
    Requirements: {test_reqs}
    Special Instructions: {special_instructions}
    Requires Auth: {requires_auth}
    {pdf_info}

    {endpoint_constraint}
    Generate 5-15 sub-scenarios. Each should be specific enough for a single test file.
    {"Examples: 'test_GetUser_positive', 'test_CreateOrder_validation', 'test_streaming_rpc', 'test_deadline_exceeded', 'test_metadata_auth', etc." if api_protocol == "grpc" else "Examples: 'test_user_crud_positive', 'test_input_validation', 'test_auth_flows', 'test_error_responses', 'test_pagination', 'test_rate_limiting', etc."}

    Consider: Functional (positive/negative), Security, Error Handling, Data Validation,
    Boundary Testing, Integration, Performance edge cases.
    - User-specified categories from email/PDF take priority
    - Include authentication tests ONLY if requires_auth=true
    - NO DELETE endpoint tests{"" if api_protocol == "grpc" else ""}
    {"- For gRPC: also consider streaming tests, deadline/timeout, large message, and metadata tests" if api_protocol == "grpc" else ""}

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

    if TEST_MODE and len(sub_scenarios) > 1:
        logging.info(f"TEST_MODE enabled — trimming {len(sub_scenarios)} scenarios to 1")
        sub_scenarios = sub_scenarios[:1]

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


def _generate_scenario_sub_scenarios(email_data: dict) -> dict:
    """
    Generates ordered, sequential sub-scenarios for end-to-end business flow testing.
    Each sub-scenario becomes one pytest file, executed in order via zero-padded filenames.
    Later steps can depend on data produced by earlier steps (shared via testdata/ JSON files).
    """
    api_docs = email_data["api_documentation"]
    test_reqs = email_data["test_requirements"]
    special_instructions = email_data["special_instructions"]
    requires_auth = email_data["requires_authentication"]
    pdf_context = email_data["pdf_context"]
    pdf_insights = email_data["pdf_insights"]
    has_pdf = email_data["has_pdf"]
    email_content = email_data.get("email_content", "")
    api_protocol = email_data.get("api_protocol", "rest")

    pdf_info = ""
    if has_pdf and pdf_context:
        pdf_info = f"""
    **PDF Documentation**: {pdf_context[:2000]}...
    **Insights**: {pdf_insights}
    """

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    protocol_note = (
        "**API PROTOCOL**: gRPC — steps should target gRPC service/method calls, "
        "use gRPC status codes, and handle streaming RPCs where applicable."
        if api_protocol == "grpc"
        else "**API PROTOCOL**: REST — steps should target HTTP endpoints."
    )

    scenario_prompt = f"""
    The user wants to test an END-TO-END BUSINESS FLOW / SCENARIO.
    Analyze the API documentation and the user's description to generate ORDERED,
    SEQUENTIAL test sub-scenarios that cover the full flow.

    {protocol_note}

    USER'S SCENARIO DESCRIPTION:
    {email_content[:3000]}

    API Docs: {json.dumps(api_docs, indent=2)[:3000]}
    Requirements: {test_reqs}
    Special Instructions: {special_instructions}
    Requires Auth: {requires_auth}
    {pdf_info}

    IMPORTANT RULES:
    - Generate scenarios in the EXACT ORDER they must execute
    - Each scenario becomes ONE pytest file
    - Use ZERO-PADDED file names so pytest runs them alphabetically in order:
      test_01_<step_name>_{timestamp}.py, test_02_<step_name>_{timestamp}.py, etc.
    - Each step may produce data (e.g. a created resource ID) that later steps consume
    - Specify what data each step produces and consumes via JSON files in testdata/
    - NO DELETE endpoint tests
    - Include authentication/setup as the first step if requires_auth=true
    - Generate 3-10 sequential steps that cover the described flow

    Return STRICT JSON:
    {{
        "scenario_name": "short name for the overall flow",
        "flow_description": "one-sentence summary of the end-to-end flow",
        "sub_scenarios": [
            {{
                "file_name": "test_01_create_product_{timestamp}.py",
                "description": "Step 1: Create a new product via POST /api/products",
                "endpoints": ["/api/products"],
                "test_type": "scenario_step",
                "priority": "high",
                "order": 1,
                "depends_on": [],
                "data_produces": ["product_id"],
                "data_consumes": []
            }},
            {{
                "file_name": "test_02_add_stock_{timestamp}.py",
                "description": "Step 2: Add stock to the created product",
                "endpoints": ["/api/products/{{product_id}}/stock"],
                "test_type": "scenario_step",
                "priority": "high",
                "order": 2,
                "depends_on": ["test_01_create_product_{timestamp}.py"],
                "data_produces": ["stock_id"],
                "data_consumes": ["product_id"]
            }}
        ],
        "total_scenarios": 0,
        "estimated_test_count": 0,
        "timestamp": "{timestamp}"
    }}
    """

    response = get_ai_response(scenario_prompt, model=MODEL_NAME)
    scenarios_data = extract_json_from_text(response)
    logging.info(f"Scenario sub-scenario generation response: {scenarios_data}")

    if not scenarios_data:
        raise ValueError(f"Invalid scenario JSON: {response[:500]}")

    sub_scenarios = scenarios_data.get("sub_scenarios", [])

    # Ensure ordering by the order field
    sub_scenarios.sort(key=lambda s: s.get("order", 0))

    if TEST_MODE and len(sub_scenarios) > 1:
        logging.info(f"TEST_MODE enabled — trimming {len(sub_scenarios)} scenarios to 1")
        sub_scenarios = sub_scenarios[:1]

    logging.info(f"Generated {len(sub_scenarios)} scenario sub-scenarios (sequential):")
    for idx, s in enumerate(sub_scenarios):
        logging.info(
            f"  {idx+1}. [{s.get('order', '?')}] {s['file_name']}: {s['description']}"
            f"  produces={s.get('data_produces', [])} consumes={s.get('data_consumes', [])}"
        )

    return {
        "sub_scenarios": sub_scenarios,
        "total_scenarios": len(sub_scenarios),
        "estimated_test_count": scenarios_data.get("estimated_test_count", 0),
        "timestamp": timestamp,
        "email_data": email_data,
        "testing_type": "scenario",
        "scenario_name": scenarios_data.get("scenario_name", ""),
        "flow_description": scenarios_data.get("flow_description", ""),
    }


@task
def generate_sub_test_scenarios(email_data: dict):
    """
    Generates granular per-file sub-scenarios instead of broad categories.
    Each sub-scenario maps to one test file.

    Dispatches to scenario-based or standard API generation based on testing_type.
    """
    testing_type = email_data.get("testing_type", "api_only")
    logging.info(f"Generating sub-scenarios with testing_type={testing_type}")

    if testing_type == "scenario":
        return _generate_scenario_sub_scenarios(email_data)
    else:
        return _generate_api_sub_scenarios(email_data)


# ═══════════════════════════════════════════════════════════════
# STEP 2.5: Extract Request Body Schemas from Postman Collection
# ═══════════════════════════════════════════════════════════════
@task
def extract_request_body_schemas(scenario_data: dict):
    """
    Asks the AI agent to extract request body examples from the Postman collection
    for every POST/PUT/PATCH endpoint and saves them as JSON schema files under
    {test_session_id}/testdata/schemas/.  These schemas are later fed to
    FakerDataGenerator to produce realistic test data.
    """
    email_data = scenario_data["email_data"]
    test_session_id = email_data["test_session_id"]
    api_docs = email_data["api_documentation"]

    base_dir = Variable.get("ltai.test.base_dir", default_var="/appz/pyunit_testing")
    schemas_dir = os.path.join(base_dir, test_session_id, "testdata", "schemas")
    os.makedirs(schemas_dir, exist_ok=True)

    extraction_prompt = f"""
    Analyze the following API documentation (Postman collection) and extract the
    request body for every POST, PUT, and PATCH endpoint.

    API Docs:
    {json.dumps(api_docs, indent=2)[:6000]}

    For each endpoint that has a request body, return a JSON object with the
    following structure.  Use placeholder values so they can be auto-detected
    by FakerDataGenerator (patterns: PLACEHOLDER_*, <FIELD_NAME>, {{{{field_name}}}}).

    Return STRICT JSON — no markdown, no explanation:
    {{
        "schemas": [
            {{
                "endpoint": "/api/rest/resource",
                "method": "POST",
                "filename": "create_resource.json",
                "body": {{
                    "field1": "PLACEHOLDER_FIRST_NAME",
                    "field2": "PLACEHOLDER_EMAIL",
                    "nested": {{
                        "field3": "PLACEHOLDER_PHONE"
                    }}
                }}
            }}
        ]
    }}

    Rules:
    - Only include endpoints that have a request body (POST/PUT/PATCH)
    - Use descriptive PLACEHOLDER_* values that hint at the data type
      (e.g. PLACEHOLDER_EMAIL, PLACEHOLDER_SSN, PLACEHOLDER_DATE_OF_BIRTH)
    - Keep the exact field names from the API docs
    - If the collection has example bodies, use their structure with placeholders
    - If no POST/PUT/PATCH endpoints exist, return {{"schemas": []}}
    """

    response = get_ai_response(extraction_prompt, model=MODEL_NAME)
    schemas_data = extract_json_from_text(response)

    saved_schemas = []

    if schemas_data and schemas_data.get("schemas"):
        for schema_entry in schemas_data["schemas"]:
            filename = schema_entry.get("filename", "unknown_schema.json")
            body = schema_entry.get("body", {})
            endpoint = schema_entry.get("endpoint", "")
            method = schema_entry.get("method", "")

            schema_path = os.path.join(schemas_dir, filename)
            with open(schema_path, "w", encoding="utf-8") as f:
                json.dump(body, f, indent=2)

            saved_schemas.append({
                "filename": filename,
                "endpoint": endpoint,
                "method": method,
            })
            logging.info(f"Saved request body schema: {filename} ({method} {endpoint})")
    else:
        logging.info("No POST/PUT/PATCH request bodies found in the API docs")

    logging.info(f"Extracted {len(saved_schemas)} request body schema(s)")

    # Pass through scenario_data with the schemas info added
    scenario_data["saved_schemas"] = saved_schemas
    return scenario_data


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
    saved_schemas = scenario_data.get("saved_schemas", [])

    test_session_id = email_data["test_session_id"]
    api_docs = email_data["api_documentation"]
    test_reqs = email_data["test_requirements"]
    special_instructions = email_data["special_instructions"]
    config_path = email_data["config_path"]
    base_url = email_data.get("base_url") or "http://connector:8000"
    pdf_insights = email_data["pdf_insights"]
    requires_auth = email_data["requires_authentication"]
    api_protocol = email_data.get("api_protocol", "rest")

    config_info = ""
    if config_path and os.path.exists(config_path):
        config_info = f"**Config**: {config_path} (use for auth and base_url)"

    # Build auth instructions from config.yaml so the agent knows
    # exactly which env vars exist and how to use them
    auth_instructions = ""
    if requires_auth and config_path:
        auth_instructions = _build_auth_instructions(config_path)

    # Protocol-specific instructions (REST vs gRPC)
    proto_files = email_data.get("proto_files", [])
    test_dir = Variable.get("ltai.test.base_dir", default_var="/appz/pyunit_testing") + f"/{test_session_id}"
    grpc_services = _parse_proto_services(test_dir, proto_files) if proto_files else []
    protocol_instructions = _build_protocol_instructions(api_protocol, proto_files=proto_files, grpc_services=grpc_services)

    # Build available schemas info for the prompt
    schemas_info = ""
    if saved_schemas:
        schema_lines = [f"  - {s['filename']} ({s['method']} {s['endpoint']})" for s in saved_schemas]
        schemas_info = (
            "AVAILABLE REQUEST BODY SCHEMAS (already saved in testdata/schemas/):\n"
            + "\n".join(schema_lines)
        )

    # Detect scenario mode from upstream data
    is_scenario_mode = scenario_data.get("testing_type") == "scenario"
    scenario_name = scenario_data.get("scenario_name", "")
    flow_description = scenario_data.get("flow_description", "")

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

        # Build scenario-specific instructions when in scenario mode
        scenario_context = ""
        if is_scenario_mode:
            order = scenario.get("order", idx + 1)
            depends_on = scenario.get("depends_on", [])
            data_produces = scenario.get("data_produces", [])
            data_consumes = scenario.get("data_consumes", [])

            scenario_context = f"""
        SCENARIO MODE — SEQUENTIAL BUSINESS FLOW TESTING
        ─────────────────────────────────────────────────
        Scenario: {scenario_name}
        Flow: {flow_description}
        This is step {order} of {len(sub_scenarios)} in a sequential end-to-end test flow.

        DATA SHARING BETWEEN STEPS:
        - This step CONSUMES data from previous steps: {', '.join(data_consumes) if data_consumes else 'none (first step or independent)'}
        - This step PRODUCES data for later steps: {', '.join(data_produces) if data_produces else 'none'}
        - Depends on files: {', '.join(depends_on) if depends_on else 'none'}

        DATA SHARING PATTERN — use JSON files in testdata/ directory:
          WRITE (produce data for later steps):
            DATA_DIR = os.path.join(os.path.dirname(__file__), "testdata")
            os.makedirs(DATA_DIR, exist_ok=True)
            with open(os.path.join(DATA_DIR, "step_{order:02d}_output.json"), "w") as f:
                json.dump({{"product_id": response.json()["id"]}}, f)

          READ (consume data from earlier steps):
            DATA_DIR = os.path.join(os.path.dirname(__file__), "testdata")
            with open(os.path.join(DATA_DIR, "step_XX_output.json")) as f:
                prev_data = json.load(f)
            product_id = prev_data["product_id"]

        TEST ORDERING:
        - File names use zero-padded prefixes (test_01_, test_02_, ...) so pytest
          runs them in the correct sequential order alphabetically.
        - Each file is ONE step in the flow — do NOT test steps out of order.
        """

        generation_prompt = f"""
        Generate a COMPLETE Python pytest test file for the following sub-scenario.
        Save it in subdirectory "{test_session_id}" with filename "{file_name}".

        Sub-scenario: {description}
        File: {file_name}
        Test type: {test_type}
        Priority: {priority}
        {"Service/Methods" if api_protocol == "grpc" else "Endpoints"}: {', '.join(endpoints) if endpoints else 'All relevant from the API docs'}
        Requires Auth: {requires_auth}
        {config_info}

        {protocol_instructions}

        {scenario_context}

        API Docs: {json.dumps(api_docs, indent=2)[:3000]}
        Requirements: {test_reqs}
        Special Instructions: {special_instructions}
        Base URL: {base_url}
        PDF Insights: {pdf_insights}

        {auth_instructions if auth_instructions else ""}

        Generate pytest file with:
        - All imports (pytest, {"grpc, google.protobuf" if api_protocol == "grpc" else "requests"}, json, etc.)
        - Fixtures if needed ({"channel, stub" if api_protocol == "grpc" else "auth, test data"})
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
        - Only test documented {"services/methods" if api_protocol == "grpc" else "endpoints"}
        - {"All credentials MUST come from .env via os.getenv() — NEVER hardcode secrets" if requires_auth else "Skip auth"}
        - Do NOT duplicate tests that were already generated in previous files
        {"- For gRPC with grpcurl: NEVER pass JSON payloads as -d command-line arguments (causes Errno 7 Argument list too long). ALWAYS write payload to a temp file and use -d @filepath. ALWAYS set cwd=os.path.dirname(os.path.abspath(__file__)) in subprocess.run() so grpcurl can find the .proto files." + (" Also ALWAYS include: -import-path . " + " ".join(f"-proto {pf}" for pf in proto_files) + " (the .proto files are in the test directory)." if proto_files else " If the server does not support reflection, use -proto <file> flags.") if api_protocol == "grpc" else ""}

        TEST DATA:
        - When creating test cases, generate test data as needed and load it in the test scenario
        - Request body schemas have been pre-extracted to {test_session_id}/testdata/schemas/
        {schemas_info}
        - Use FakerDataGenerator with workspace="pytest", workspace_id="{test_session_id}",
          schema_file="<filename>" to generate realistic test data to {test_session_id}/testdata/
        - In test files, load the generated JSON and use it in the test:
          DATA_DIR = os.path.join(os.path.dirname(__file__), "testdata")
          with open(os.path.join(DATA_DIR, "filename.json")) as f:
              test_data = json.load(f)
          {"response = stub.MethodName(ParseDict(test_data, pb2.RequestType()))" if api_protocol == "grpc" else "response = requests.post(url, json=test_data)"}
        - Generate separate data files for positive vs negative test scenarios

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
        "base_url": base_url,
        "config_path": config_path,
        "requires_authentication": requires_auth,
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
    generated_files = generation_data["generated_files"]
    base_url = generation_data.get("base_url") or "http://connector:8000"
    config_path = generation_data.get("config_path")
    requires_auth = generation_data.get("requires_authentication", False)

    logging.info(f"Running ALL tests in session: {test_session_id}")
    logging.info(f"Total test files: {len(generated_files)}")

    # Create .env with credentials resolved from Airflow Variables
    env_path = _create_env_file(test_session_id, config_path)
    if env_path:
        logging.info(f"Credentials .env ready at {env_path}")

    config_info = ""
    if config_path:
        config_info = f"Config file: {config_path}"

    try:
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
            "final_results": result,
            "iterations": 0,
            "outcome": "single_run",
            "test_session_id": test_session_id,
            "report_url": result.get("report_url", ""),
        }

    finally:
        # Clean up .env to avoid leaking credentials on disk
        _remove_env_file(test_session_id)
        logging.info("Credential .env cleanup complete")


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
    base_url = run_data.get("base_url") or "http://connector:8000"
    config_path = run_data.get("config_path")

    config_info = ""
    if config_path:
        config_info = f"Config file: {config_path}"

    auth_instructions = ""
    if run_data.get("requires_authentication") and config_path:
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
            "report_url": final_report_url
        }

    finally:
        # Always clean up .env to avoid leaking credentials on disk
        # _remove_env_file(test_session_id)
        # logging.info("Credential .env cleanup complete")
        pass


# ═══════════════════════════════════════════════════════════════
# STEP 6: Generate Email Content
# ═══════════════════════════════════════════════════════════════
@task
def generate_email_content(run_data: dict, email_data: dict):
    """
    Generates simple HTML email with overall metrics and link to detailed report.
    run_data comes from run_all_tests; email_data comes directly from
    extract_inputs_from_email (avoids threading large API docs through every task).
    """
    final_results = run_data["final_results"]
    iterations = run_data.get("iterations", 0)
    outcome = run_data.get("outcome", "single_run")
    test_session_id = run_data["test_session_id"]
    report_url = run_data.get("report_url", "")

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

    # Generate report link — ensure server_host is always prepended
    if not report_url:
        report_url = f"{server_host}/static/pytest_reports/{test_session_id}/index.html"
    elif not report_url.startswith(("http://", "https://")):
        report_url = f"{server_host}/{report_url.lstrip('/')}"

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

4. **Links Section** (prominent button):
   - 📊 View Detailed Report: {report_url}

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
    }


# ═══════════════════════════════════════════════════════════════
# STEP 7: Send Email
# ═══════════════════════════════════════════════════════════════
@task
def send_response_email(email_content: dict, email_data: dict):
    """
    Sends the email response.
    email_data comes directly from extract_inputs_from_email.
    """
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

    # Step 2.5: Extract request body schemas from Postman collection
    scenario_data_with_schemas = extract_request_body_schemas(scenario_data)

    # Step 3: Generate all test files
    generation_data = generate_all_test_files(scenario_data_with_schemas)

    # Step 4: Run all tests together (includes .env cleanup)
    run_data = run_all_tests(generation_data)

    # Step 5: Fix and retry loop — SKIPPED (function retained for future use)

    # Step 6: Generate email content (email_data passed directly, not through chain)
    email_content = generate_email_content(run_data, email_data)

    # Step 7: Send email (email_data passed directly, not through chain)
    send_result = send_response_email(email_content, email_data)

    # Done
    workflow_complete = EmptyOperator(
        task_id='workflow_complete',
        trigger_rule='all_success'
    )

    send_result >> workflow_complete
