from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.decorators import task
from datetime import datetime, timedelta
import os
import json
import logging
import re
import copy
import shutil

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

# ═══════════════════════════════════════════════════════════════
# Configuration
# ═══════════════════════════════════════════════════════════════
GMAIL_FROM_ADDRESS = Variable.get("ltai.api.test.from_address", default_var="")
GMAIL_CREDENTIALS = Variable.get("ltai.api.test.gmail_credentials", default_var="")
MODEL_NAME = Variable.get("ltai.api.test.model.name", default_var="APITestAgent:5.0")
server_host = Variable.get("ltai.server.host", default_var="http://localhost:8080")
CHUNK_SIZE = int(Variable.get("ltai.api.test.chunk_size", default_var="15000"))


# ═══════════════════════════════════════════════════════════════
# Helpers: .env credential management (shared with v2)
# ═══════════════════════════════════════════════════════════════
def _parse_config_yaml(config_path: str) -> dict:
    """Parse config.yaml and return the full config dict."""
    import yaml
    if not config_path or not os.path.exists(config_path):
        return {}
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _create_env_file(test_session_id: str, config_path: str) -> str:
    """
    Read credentials from config.yaml, resolve from Airflow Variables,
    write a .env file into the test session dir.
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
        return ""

    with open(env_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    logging.info(f"Created .env at {env_path} with {len(lines)} credential(s)")
    return env_path


def _build_auth_instructions(config_path: str) -> str:
    """
    Build a clear text block from config.yaml that tells the AI agent
    exactly how to set up authentication in test files.
    """
    config_data = _parse_config_yaml(config_path)
    credentials_map = config_data.get("credentials", {})
    auth_config = config_data.get("auth", {})

    if not credentials_map:
        return (
            "AUTHENTICATION SETUP:\n"
            "No credentials in config.yaml. Determine auth method from the API docs.\n"
            "If you find tokens/keys, use .env: load_dotenv(Path(__file__).parent / '.env', override=True)\n"
            "Read with os.getenv('VARIABLE_NAME'). NEVER hardcode secrets.\n"
        )

    env_vars = list(credentials_map.keys())
    lines = [
        "AUTHENTICATION SETUP:",
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
        u = auth_config.get("username_var", "USERNAME")
        p = auth_config.get("password_var", "PASSWORD")
        lines += [
            "Auth type: HTTP Basic Auth",
            f"    auth = (os.getenv('{u}'), os.getenv('{p}'))",
            "    response = requests.get(url, auth=auth)",
        ]
    elif auth_type == "bearer":
        t = auth_config.get("token_var", "BEARER_TOKEN")
        lines += [
            "Auth type: Bearer Token",
            f'    headers = {{"Authorization": f"Bearer {{os.getenv(\'{t}\')}}"}}',
        ]
    elif auth_type in ("header", "custom"):
        header_map = auth_config.get("headers", {})
        if header_map:
            lines.append("Auth type: Custom Headers")
            lines.append("    headers = {")
            for hdr, var in header_map.items():
                lines.append(f'        "{hdr}": os.getenv("{var}"),')
            lines.append("    }")
    elif auth_type == "api_key":
        k = auth_config.get("key_var", env_vars[0] if env_vars else "API_KEY")
        h = auth_config.get("header_name", "X-API-Key")
        lines += [
            "Auth type: API Key",
            f'    headers = {{"{h}": os.getenv("{k}")}}',
        ]
    else:
        lines += [
            "Auth type: NOT specified — determine from API docs.",
            "Read each credential with os.getenv('VARIABLE_NAME').",
        ]

    lines.append("")
    lines.append("CRITICAL: NEVER hardcode any credential values. Always use os.getenv().")
    return "\n".join(lines)


def _remove_env_file(test_session_id: str):
    """Remove the .env file from the test session directory."""
    base_dir = Variable.get("ltai.test.base_dir", default_var="/appz/pyunit_testing")
    env_path = os.path.join(base_dir, test_session_id, ".env")
    if os.path.exists(env_path):
        os.remove(env_path)
        logging.info(f"Removed .env at {env_path}")


# ═══════════════════════════════════════════════════════════════
# Helper: Protocol-specific instructions (REST vs gRPC)
# ═══════════════════════════════════════════════════════════════
def _build_protocol_instructions(api_protocol: str, proto_files: list = None) -> str:
    """
    Return a block of text that tells the AI agent how to write tests
    for the given API protocol.
    """
    if api_protocol == "grpc":
        # Build proto file instructions when .proto files are available
        proto_note = ""
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

        grpcurl_example = (
            '    ["grpcurl", "-plaintext", "-d", f"@{tmp_path}",\n'
            '                     BASE_URL, "package.Service/Method"]'
        )
        if proto_files:
            proto_args = ", ".join(
                '"-proto", "{}"'.format(pf) for pf in proto_files
            )
            grpcurl_example = (
                '    ["grpcurl", "-plaintext",\n'
                '                     "-import-path", ".", ' + proto_args + ',\n'
                '                     "-d", f"@{tmp_path}",\n'
                '                     BASE_URL, "package.Service/Method"]'
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
                    capture_output=True, text=True, timeout=60
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
# Helpers: Large-payload specific
# ═══════════════════════════════════════════════════════════════
def _read_first_n_lines(file_path: str, n: int = 70) -> str:
    """Read the first n lines of a file and return as a single string."""
    lines = []
    with open(file_path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            if i >= n:
                break
            lines.append(line)
    return "".join(lines)


def _get_structure_overview(data, prefix="", max_depth=3, depth=0) -> list:
    """
    Return a compact list of 'path: type' strings describing JSON structure.
    Shows keys and types only — no values — so even a 1.3M char JSON
    produces a small overview.
    """
    if depth >= max_depth:
        return [f"{prefix}: ..."]

    lines = []
    if isinstance(data, dict):
        for key, value in data.items():
            path = f"{prefix}.{key}" if prefix else key
            if isinstance(value, dict):
                lines.append(f"{path}: object ({len(value)} keys)")
                lines.extend(_get_structure_overview(value, path, max_depth, depth + 1))
            elif isinstance(value, list):
                item_type = type(value[0]).__name__ if value else "empty"
                lines.append(f"{path}: array[{item_type}] ({len(value)} items)")
                if value and isinstance(value[0], dict) and depth + 1 < max_depth:
                    lines.extend(_get_structure_overview(value[0], f"{path}[]", max_depth, depth + 1))
            else:
                vtype = type(value).__name__
                extra = ""
                if isinstance(value, str) and len(value) > 200:
                    extra = f" (len={len(value)})"
                lines.append(f"{path}: {vtype}{extra}")
    elif isinstance(data, list) and data:
        if isinstance(data[0], dict):
            lines.append(f"{prefix}[]: object ({len(data[0])} keys)")
            lines.extend(_get_structure_overview(data[0], f"{prefix}[]", max_depth, depth + 1))
        else:
            lines.append(f"{prefix}[]: {type(data[0]).__name__}")

    return lines


def _chunk_json_data(json_data, max_chars=15000) -> list:
    """
    Split JSON data into chunks that each fit within max_chars.

    Returns list of dicts:
        { "data": ..., "keys": [...], "char_count": int, "oversized": bool }

    For dicts  : groups consecutive top-level keys.
    For arrays : groups consecutive elements.
    If a single entry exceeds max_chars it becomes its own (oversized) chunk.
    """
    full_str = json.dumps(json_data, indent=2)

    # Small enough — single chunk
    if len(full_str) <= max_chars:
        keys = list(json_data.keys()) if isinstance(json_data, dict) else [f"[0..{len(json_data) - 1}]"]
        return [{"data": json_data, "keys": keys, "char_count": len(full_str), "oversized": False}]

    chunks = []

    if isinstance(json_data, dict):
        cur_data = {}
        cur_size = 2  # {}
        cur_keys = []

        for key, value in json_data.items():
            entry_str = json.dumps({key: value}, indent=2)
            entry_size = len(entry_str)

            # Flush current chunk if adding this entry would overflow
            if cur_size + entry_size > max_chars and cur_data:
                chunks.append({"data": cur_data, "keys": cur_keys, "char_count": cur_size, "oversized": False})
                cur_data = {}
                cur_size = 2
                cur_keys = []

            # Single entry bigger than max — its own oversized chunk
            if entry_size > max_chars and not cur_data:
                chunks.append({"data": {key: value}, "keys": [key], "char_count": entry_size, "oversized": True})
                continue

            cur_data[key] = value
            cur_size += entry_size
            cur_keys.append(key)

        if cur_data:
            chunks.append({"data": cur_data, "keys": cur_keys, "char_count": cur_size, "oversized": False})

    elif isinstance(json_data, list):
        cur_items = []
        cur_size = 2  # []
        start_idx = 0

        for i, item in enumerate(json_data):
            item_size = len(json.dumps(item, indent=2))

            if cur_size + item_size > max_chars and cur_items:
                chunks.append({
                    "data": cur_items,
                    "keys": [f"[{start_idx}..{i - 1}]"],
                    "char_count": cur_size,
                    "oversized": False,
                })
                cur_items = []
                cur_size = 2
                start_idx = i

            cur_items.append(item)
            cur_size += item_size

        if cur_items:
            chunks.append({
                "data": cur_items,
                "keys": [f"[{start_idx}..{start_idx + len(cur_items) - 1}]"],
                "char_count": cur_size,
                "oversized": False,
            })
    else:
        # Primitive — shouldn't happen but handle gracefully
        chunks.append({"data": json_data, "keys": ["root"], "char_count": len(full_str), "oversized": False})

    return chunks


def _extract_endpoints_from_postman(collection: dict) -> list:
    """
    Walk a Postman collection and return a lightweight list of endpoints:
    [{"name": ..., "method": ..., "url": ...}, ...]
    No request bodies or headers — keeps it small.
    """
    endpoints = []

    def _walk(items):
        for item in items:
            if "item" in item:
                _walk(item["item"])
            elif "request" in item:
                req = item["request"]
                method = req.get("method", "GET")
                url = req.get("url", "")
                if isinstance(url, dict):
                    url = url.get("raw", "")
                endpoints.append({
                    "name": item.get("name", ""),
                    "method": method,
                    "url": str(url),
                })

    if "item" in collection:
        _walk(collection["item"])
    return endpoints


# ═══════════════════════════════════════════════════════════════
# STEP 1 : Extract and Parse Inputs from Email
# ═══════════════════════════════════════════════════════════════
@task
def extract_inputs_from_email(**kwargs):
    """
    Lightweight extraction — saves JSON file PATHS only (no full content in
    XCom) to avoid blowing up the metadata DB with 1.3 M-char payloads.
    """
    dag_run = kwargs.get("dag_run")
    conf = dag_run.conf

    email_id = conf.get("email_id")
    thread_id = conf.get("thread_id")
    json_files = conf.get("json_files", [])
    pdf_files = conf.get("pdf_files", [])
    config_file = conf.get("config_file")
    has_pdf = conf.get("has_pdf", False)
    email_headers = conf.get("email_headers", {})
    email_content = conf.get("email_content", "")
    api_protocol = conf.get("api_protocol", "rest")
    proto_files_conf = conf.get("proto_files", [])

    if not email_id:
        raise ValueError("No email_id provided in DAG configuration")

    sender = email_headers.get("From", "")
    subject = email_headers.get("Subject", "")
    message_id = email_headers.get("Message-ID", "")
    references = email_headers.get("References", "")
    email_data_for_recipients = {"headers": email_headers}
    all_recipients = extract_all_recipients(email_data_for_recipients)

    config_path = config_file.get("path") if config_file else None

    # Create test session directory
    test_session_id = thread_id
    base_dir = Variable.get("ltai.test.base_dir", default_var="/appz/pyunit_testing")
    test_dir = os.path.join(base_dir, test_session_id)
    os.makedirs(test_dir, exist_ok=True)

    # Copy .proto files into the test session directory
    proto_filenames = []
    for proto in proto_files_conf:
        src_path = proto.get("path", "")
        original_name = proto.get("filename", "")
        if src_path and os.path.exists(src_path) and original_name:
            dest_path = os.path.join(test_dir, original_name)
            shutil.copy2(src_path, dest_path)
            proto_filenames.append(original_name)
            logging.info(f"Copied .proto file to test dir: {original_name}")
    if proto_filenames:
        logging.info(f"Total .proto files copied: {len(proto_filenames)}")

    # Collect JSON file paths (files are already saved to disk by the listener)
    json_file_paths = [jf.get("path") for jf in json_files if jf.get("path")]

    # Quick extraction from email body only (not the big JSONs)
    parse_prompt = f"""
    Extract test configuration from this email.

    Subject: {subject}
    Email body: {email_content[:3000]}

    Return STRICT JSON:
    {{
        "base_url": "the API base URL if mentioned, else empty string",
        "requires_authentication": true,
        "special_instructions": "any special testing instructions"
    }}
    """
    parsed = extract_json_from_text(get_ai_response(parse_prompt, model=MODEL_NAME)) or {}

    logging.info(f"Processing email {email_id}, thread {thread_id}, "
                 f"{len(json_file_paths)} JSON file(s), protocol={api_protocol}")

    return {
        "sender_email": sender,
        "email_subject": subject,
        "message_id": message_id,
        "references": references,
        "thread_id": thread_id,
        "test_session_id": test_session_id,
        "original_email_id": email_id,
        "all_recipients": all_recipients,
        "json_file_paths": json_file_paths,
        "config_path": config_path,
        "base_url": parsed.get("base_url", ""),
        "requires_authentication": parsed.get("requires_authentication", False),
        "special_instructions": parsed.get("special_instructions", ""),
        "has_pdf": has_pdf,
        "pdf_files": pdf_files,
        "email_content": email_content,
        "api_protocol": api_protocol,
        "proto_files": proto_filenames,
    }


# ═══════════════════════════════════════════════════════════════
# STEP 2 : Classify JSON files & save request schemas
# ═══════════════════════════════════════════════════════════════
@task
def classify_and_prepare_schemas(email_data: dict):
    """
    For each JSON file attached to the email:
      1. Read the first 70 lines
      2. Ask the LLM to classify: postman_collection / request_schema /
         response_example / api_spec / other
      3. If request_schema → copy as-is to testdata/schemas/
      4. If postman_collection → extract lightweight endpoint list
    """
    json_file_paths = email_data["json_file_paths"]
    test_session_id = email_data["test_session_id"]
    base_dir = Variable.get("ltai.test.base_dir", default_var="/appz/pyunit_testing")
    schemas_dir = os.path.join(base_dir, test_session_id, "testdata", "schemas")
    os.makedirs(schemas_dir, exist_ok=True)

    request_schemas = []
    postman_endpoints = []
    postman_base_url = ""
    detected_protocol = email_data.get("api_protocol", "rest")  # start with listener's classification

    for file_path in json_file_paths:
        if not file_path or not os.path.exists(file_path):
            logging.warning(f"JSON file not found: {file_path}")
            continue

        filename = os.path.basename(file_path)
        head_content = _read_first_n_lines(file_path, n=70)
        file_size = os.path.getsize(file_path)

        logging.info(f"Classifying {filename} ({file_size:,} bytes) — first 70 lines sent to LLM")

        classify_prompt = f"""
        Look at the first 70 lines of this JSON file and classify it.

        FILENAME: {filename}
        FILE SIZE: {file_size:,} bytes

        FILE HEAD (first 70 lines):
        {head_content}

        Classify into EXACTLY ONE category:
        - "postman_collection" : A Postman collection with API endpoint definitions
        - "request_schema"     : A JSON request body / payload for an API call
        - "response_example"   : An expected API response body
        - "api_spec"           : An OpenAPI / Swagger specification
        - "proto_descriptor"   : A protobuf / gRPC descriptor or service definition in JSON
        - "other"              : Something else

        Also determine the API protocol from the file contents:
        - "rest"  : Standard REST/HTTP API data (default)
        - "grpc"  : gRPC data — clues: protobuf message structure, service/method
          names, .proto references, gRPC-specific Postman settings, field names
          like "message", "service", "rpc", package names, etc.

        Return STRICT JSON:
        {{
            "classification": "...",
            "description": "one-line description of the file contents",
            "endpoints_detected": ["any API endpoint paths or service/method names visible"],
            "base_url_detected": "base URL or host:port if visible, else empty string",
            "api_protocol_detected": "rest or grpc"
        }}
        """

        resp = get_ai_response(classify_prompt, model=MODEL_NAME)
        result = extract_json_from_text(resp) or {"classification": "other"}
        classification = result.get("classification", "other")

        file_protocol = result.get("api_protocol_detected", "rest")
        logging.info(f"  {filename} → {classification} (protocol={file_protocol}): "
                     f"{result.get('description', '')}")

        # If any file signals gRPC, upgrade the detected protocol for the whole session
        if file_protocol == "grpc":
            detected_protocol = "grpc"

        # ── Handle based on classification ──────────────────────
        if classification in ("request_schema", "proto_descriptor"):
            # Save the JSON file as-is to schemas dir
            schema_dest = os.path.join(schemas_dir, filename)
            with open(file_path, "r", encoding="utf-8") as src, \
                 open(schema_dest, "w", encoding="utf-8") as dst:
                dst.write(src.read())

            request_schemas.append({
                "filename": filename,
                "schema_path": schema_dest,
                "description": result.get("description", ""),
                "original_size": file_size,
            })
            logging.info(f"  Saved request schema → {schema_dest}")

        elif classification == "postman_collection":
            # Extract lightweight endpoint metadata (no large bodies)
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    collection = json.load(f)
                endpoints = _extract_endpoints_from_postman(collection)
                postman_endpoints.extend(endpoints)
                detected_url = result.get("base_url_detected", "")
                if detected_url:
                    postman_base_url = detected_url
                logging.info(f"  Extracted {len(endpoints)} endpoint(s) from Postman collection")
            except Exception as e:
                logging.error(f"  Failed to parse Postman collection {filename}: {e}")

        elif classification == "response_example":
            # Save as reference for assertion generation
            resp_dest = os.path.join(schemas_dir, f"expected_response_{filename}")
            with open(file_path, "r", encoding="utf-8") as src, \
                 open(resp_dest, "w", encoding="utf-8") as dst:
                dst.write(src.read())
            logging.info(f"  Saved response example → {resp_dest}")

        elif classification == "api_spec":
            # Might contain endpoint info — save as schema too
            schema_dest = os.path.join(schemas_dir, filename)
            with open(file_path, "r", encoding="utf-8") as src, \
                 open(schema_dest, "w", encoding="utf-8") as dst:
                dst.write(src.read())
            request_schemas.append({
                "filename": filename,
                "schema_path": schema_dest,
                "description": result.get("description", ""),
                "original_size": file_size,
            })
            logging.info(f"  Saved api_spec as schema → {schema_dest}")

        else:
            # Fallback: the user attached this file for testing, so treat it
            # as a request schema even if the LLM couldn't categorize it.
            # Only Postman collections and response examples are excluded.
            schema_dest = os.path.join(schemas_dir, filename)
            with open(file_path, "r", encoding="utf-8") as src, \
                 open(schema_dest, "w", encoding="utf-8") as dst:
                dst.write(src.read())
            request_schemas.append({
                "filename": filename,
                "schema_path": schema_dest,
                "description": result.get("description", ""),
                "original_size": file_size,
            })
            logging.info(f"  Classification '{classification}' — treating as request schema "
                         f"(user attached it for testing) → {schema_dest}")

    # Use Postman-detected base_url if email didn't provide one
    base_url = email_data.get("base_url") or postman_base_url

    # Build a compact endpoint summary string for prompts
    endpoint_summary = ""
    if postman_endpoints:
        ep_lines = [f"  {ep['method']} {ep['url']}  — {ep['name']}" for ep in postman_endpoints]
        endpoint_summary = "AVAILABLE API ENDPOINTS:\n" + "\n".join(ep_lines)

    if not request_schemas:
        logging.warning("No JSON files classified as request_schema — no schemas to chunk")

    logging.info(f"Final detected protocol: {detected_protocol}")

    return {
        "email_data": email_data,
        "request_schemas": request_schemas,
        "endpoint_summary": endpoint_summary,
        "base_url": base_url,
        "api_protocol": detected_protocol,
    }


# ═══════════════════════════════════════════════════════════════
# STEP 3 : Chunk each schema & generate pytest files
# ═══════════════════════════════════════════════════════════════
@task
def generate_test_files_from_chunks(schema_data: dict):
    """
    For each request schema:
      1. Load the full JSON
      2. Build a compact structure overview (keys + types, no values)
      3. Split into chunks of ~CHUNK_SIZE chars
      4. For EACH chunk → call get_ai_response → AI writes one pytest file
      5. Continue until the entire schema file has been covered

    Each generated pytest file:
      - Loads the FULL schema from disk
      - Focuses on the fields present in its chunk
      - Creates VARIATIONS programmatically (deepcopy + mutations)
      - Uses @pytest.mark.parametrize for boundary / negative tests
    """
    email_data = schema_data["email_data"]
    request_schemas = schema_data["request_schemas"]
    endpoint_summary = schema_data.get("endpoint_summary", "")
    base_url = schema_data.get("base_url") or email_data.get("base_url") or ""
    test_session_id = email_data["test_session_id"]
    config_path = email_data.get("config_path")
    requires_auth = email_data.get("requires_authentication", False)
    special_instructions = email_data.get("special_instructions", "")
    api_protocol = schema_data.get("api_protocol", email_data.get("api_protocol", "rest"))

    base_dir = Variable.get("ltai.test.base_dir", default_var="/appz/pyunit_testing")
    test_dir = os.path.join(base_dir, test_session_id)

    auth_instructions = ""
    if requires_auth and config_path:
        auth_instructions = _build_auth_instructions(config_path)

    proto_files = email_data.get("proto_files", [])
    protocol_instructions = _build_protocol_instructions(api_protocol, proto_files=proto_files)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    conversation_history = []
    generated_files = []

    for schema_info in request_schemas:
        schema_path = schema_info["schema_path"]
        schema_filename = schema_info["filename"]
        schema_description = schema_info.get("description", "")

        logging.info(f"{'='*60}")
        logging.info(f"Processing schema: {schema_filename} ({schema_info['original_size']:,} bytes)")

        # Load full JSON
        with open(schema_path, "r", encoding="utf-8") as f:
            full_data = json.load(f)

        # Compact structure overview (keys + types, no values)
        structure_lines = _get_structure_overview(full_data)
        structure_str = "\n".join(structure_lines[:120])

        # Split into chunks
        chunks = _chunk_json_data(full_data, max_chars=CHUNK_SIZE)
        logging.info(f"  Split into {len(chunks)} chunk(s)")

        for chunk_idx, chunk in enumerate(chunks):
            file_name = (
                f"test_chunk_{chunk_idx + 1:03d}_"
                f"{schema_filename.replace('.json', '')}_{timestamp}.py"
            )

            logging.info(f"  Generating [{chunk_idx + 1}/{len(chunks)}]: {file_name}")
            logging.info(f"    Fields: {', '.join(chunk['keys'])}  ({chunk['char_count']:,} chars)")

            # Prepare the chunk data string for the prompt
            chunk_data_str = json.dumps(chunk["data"], indent=2)
            if chunk.get("oversized"):
                # Truncate in prompt; tell AI to load from file instead
                chunk_data_str = chunk_data_str[:CHUNK_SIZE] + (
                    "\n\n... [TRUNCATED — this field is very large. "
                    "Load the full value from the schema file in tests.]\n"
                )

            # Relative path from test file to schema
            rel_schema = f"testdata/schemas/{schema_filename}"

            # Build gRPC temp-file rule (avoids triple-quote nesting in f-string)
            grpc_tempfile_rule = ""
            if api_protocol == "grpc":
                proto_flags_str = ""
                if proto_files:
                    proto_flags_str = ' "-import-path", ".",\n' + "".join(
                        f'                         "-proto", "{pf}",\n' for pf in proto_files
                    ) + "                        "
                grpc_tempfile_rule = (
                    "10. For gRPC with grpcurl: NEVER pass JSON payloads as -d command-line arguments.\n"
                    "               Large payloads cause 'Argument list too long' (E2BIG / Errno 7) errors.\n"
                    "               ALWAYS write the payload to a temp file and use -d @filepath:\n"
                    "                 import tempfile\n"
                    "                 payload_json = json.dumps(modified_data)\n"
                    "                 with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:\n"
                    "                     tmp.write(payload_json)\n"
                    "                     tmp_path = tmp.name\n"
                    "                 try:\n"
                    '                     result = subprocess.run(\n'
                    '                         ["grpcurl", "-plaintext",\n'
                    f'{proto_flags_str}'
                    '                          "-d", f"@{{tmp_path}}",\n'
                    '                          BASE_URL, "service/Method"],\n'
                    "                         capture_output=True, text=True, timeout=60)\n"
                    "                 finally:\n"
                    "                     os.unlink(tmp_path)"
                )

            generation_prompt = f"""
            Generate a COMPLETE Python pytest test file.
            Save it in subdirectory "{test_session_id}" with filename "{file_name}".

            CONTEXT:
            This is chunk {chunk_idx + 1} of {len(chunks)} from schema "{schema_filename}".
            Schema description: {schema_description}
            {f'Special instructions: {special_instructions}' if special_instructions else ''}

            {protocol_instructions}

            FULL SCHEMA STRUCTURE (keys + types, no values):
            {structure_str}

            THIS CHUNK covers: {', '.join(chunk['keys'])}

            CHUNK DATA:
            {chunk_data_str}

            {endpoint_summary if endpoint_summary else ('Determine gRPC service/method from the data field names and structure.' if api_protocol == 'grpc' else 'Determine API endpoint from the data field names and structure.')}

            {f'BASE URL: {base_url}' if base_url else 'Determine base URL / host:port from the endpoint info above or use a configurable BASE_URL variable.'}

            {auth_instructions if auth_instructions else ''}

            ════════════════════════════════════════════
            GENERATION RULES — follow ALL of these:
            ════════════════════════════════════════════

            1. LOAD THE FULL SCHEMA FROM FILE (never inline large data):
               import json, os, copy
               SCHEMA_DIR = os.path.join(os.path.dirname(__file__), "testdata", "schemas")
               with open(os.path.join(SCHEMA_DIR, "{schema_filename}")) as f:
                   BASE_DATA = json.load(f)

            2. FOCUS on the fields in THIS chunk: {', '.join(chunk['keys'])}
               Do NOT test fields from other chunks — those are handled by other files.

            3. CREATE VARIATIONS PROGRAMMATICALLY using copy.deepcopy():
               - Write a helper function that deep-copies BASE_DATA and modifies a
                 specific field path, e.g.:
                   def set_field(data, path, value):
                       d = copy.deepcopy(data)
                       keys = path.split(".")
                       obj = d
                       for k in keys[:-1]:
                           obj = obj[k]
                       obj[keys[-1]] = value
                       return d
               - Use @pytest.mark.parametrize with tuples of (field_path, value,
                 expected_{"status_code" if api_protocol == "grpc" else "status"}) to drive variations:
                 * Valid alternate values
                 * Empty strings / None / null
                 * Wrong type (int where str expected, etc.)
                 * Boundary values (very long strings, 0, negative numbers)
                 * Missing required fields (pop the key)

            4. MAXIMUM 10 test functions per file, ONE assertion per test.
            5. NO DELETE endpoint tests.
            6. Use descriptive test names: test_<field>_<variation_type>
            7. {"All credentials MUST come from .env via os.getenv()" if requires_auth else ""}
            {"8. For gRPC: set max_send_message_length and max_receive_message_length channel options to handle large payloads (50MB)." if api_protocol == "grpc" else ""}
            {"9. For gRPC: use grpc.StatusCode for assertions (e.g. INVALID_ARGUMENT, NOT_FOUND), NOT HTTP status codes." if api_protocol == "grpc" else ""}
            {grpc_tempfile_rule}

            Save to: {test_session_id}/{file_name}
            """

            gen_response = get_ai_response(
                generation_prompt,
                model=MODEL_NAME,
                conversation_history=conversation_history[-3:] if conversation_history else None,
            )
            logging.info(f"    Generated ({len(gen_response)} chars)")

            conversation_history.append({
                "prompt": (
                    f"Generated tests for chunk {chunk_idx + 1} of {schema_filename}, "
                    f"fields: {', '.join(chunk['keys'])}"
                ),
                "response": gen_response[:800],
            })

            generated_files.append({
                "file_name": file_name,
                "description": (
                    f"Chunk {chunk_idx + 1}/{len(chunks)} of {schema_filename}: "
                    f"{', '.join(chunk['keys'])}"
                ),
                "test_type": "large_payload_variation",
                "priority": "high",
            })

    logging.info(f"{'='*60}")
    logging.info(f"Generated {len(generated_files)} test file(s) from {len(request_schemas)} schema(s)")

    return {
        "generated_files": generated_files,
        "total_files": len(generated_files),
        "test_session_id": test_session_id,
        "base_url": base_url,
        "config_path": config_path,
        "requires_authentication": requires_auth,
    }


# ═══════════════════════════════════════════════════════════════
# STEP 4 : Run ALL Tests Together
# ═══════════════════════════════════════════════════════════════
@task
def run_all_tests(generation_data: dict):
    """Single consolidated pytest run on the entire test session directory."""
    test_session_id = generation_data["test_session_id"]
    generated_files = generation_data["generated_files"]
    config_path = generation_data.get("config_path")

    logging.info(f"Running ALL tests in session: {test_session_id} ({len(generated_files)} files)")

    env_path = _create_env_file(test_session_id, config_path)
    if env_path:
        logging.info(f"Credentials .env ready at {env_path}")

    try:
        execution_prompt = f"""
        Run pytest on ALL test files in the "{test_session_id}" directory.

        Use the run_pytest tool with:
        - target_path: "{test_session_id}"
        - verbose: True
        - generate_html_report: True

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

        exec_response = get_ai_response(execution_prompt, model=MODEL_NAME)
        result = extract_json_from_text(exec_response)

        if not result:
            result = {
                "status": "error",
                "summary": {"total": 0, "passed": 0, "failed": 0, "errors": 0, "skipped": 0, "pass_rate": 0.0},
                "failed_tests": [],
                "report_url": "",
                "exit_code": -1,
                "raw_response": exec_response[:1000],
            }

        summary = result.get("summary", {})
        logging.info(f"=== TEST RESULTS ===")
        logging.info(f"Total: {summary.get('total', 0)}  Passed: {summary.get('passed', 0)}  "
                     f"Failed: {summary.get('failed', 0)}  Pass Rate: {summary.get('pass_rate', 0)}%")

        return {
            "final_results": result,
            "iterations": 0,
            "outcome": "single_run",
            "test_session_id": test_session_id,
            "report_url": result.get("report_url", ""),
        }

    finally:
        _remove_env_file(test_session_id)
        logging.info("Credential .env cleanup complete")


# ═══════════════════════════════════════════════════════════════
# STEP 5 : Generate Email Content
# ═══════════════════════════════════════════════════════════════
@task
def generate_email_content(run_data: dict, email_data: dict):
    """Generate HTML email with overall metrics and report link."""
    final_results = run_data["final_results"]
    test_session_id = run_data["test_session_id"]
    report_url = run_data.get("report_url", "")
    subject = email_data["email_subject"]
    sender = email_data["sender_email"]

    summary = final_results.get("summary", {})
    total_tests = summary.get("total", 0)
    passed = summary.get("passed", 0)
    failed = summary.get("failed", 0)
    errors = summary.get("errors", 0)
    skipped = summary.get("skipped", 0)
    pass_rate = summary.get("pass_rate", 0.0)
    if total_tests > 0 and pass_rate == 0.0:
        pass_rate = round(passed / total_tests * 100, 2)

    if not report_url:
        report_url = f"{server_host}/static/pytest_reports/{test_session_id}/index.html"
    elif not report_url.startswith(("http://", "https://")):
        report_url = f"{server_host}/{report_url.lstrip('/')}"

    if pass_rate >= 80:
        status_text, status_color = "Tests Passed", "#28a745"
    elif pass_rate >= 50:
        status_text, status_color = "Some Tests Failed", "#ffc107"
    else:
        status_text, status_color = "Critical Failures", "#dc3545"

    email_prompt = f"""Generate ONLY complete HTML email (<!DOCTYPE html>...</html>).
NO markdown, NO code blocks, NO explanations.

Create a CLEAN professional email:

1. Header: "Your Large-Payload API test execution is complete"
2. Metrics card:
   - Status: {status_text} (color: {status_color})
   - Total: {total_tests}, Passed: {passed}, Failed: {failed}, Errors: {errors}, Skipped: {skipped}
   - Pass Rate: {pass_rate}%  with a visual progress bar
3. Quick summary (1-2 sentences)
4. Report link button: {report_url}
5. Professional footer

Style: inline CSS, max-width 600px, mobile-friendly, card-based layout.
Output ONLY the HTML document."""

    response = get_ai_response(email_prompt, model=MODEL_NAME)

    html = response.strip()
    html = re.sub(r'^\s*<think>.*?</think>\s*', '', html, flags=re.DOTALL | re.IGNORECASE)
    html = html.removeprefix("```html").removesuffix("```").strip()
    html = html.removeprefix("```").removesuffix("```").strip()

    if not html.startswith(("<!DOCTYPE", "<html")):
        raise ValueError("Invalid HTML response from AI")

    return {
        "subject": f"Re: {subject}",
        "html_body": html,
    }


# ═══════════════════════════════════════════════════════════════
# STEP 6 : Send Email
# ═══════════════════════════════════════════════════════════════
@task
def send_response_email(email_content: dict, email_data: dict):
    """Send the results email as a threaded reply."""
    recipient = email_data["sender_email"]
    subject = email_content["subject"]
    html_body = email_content["html_body"]
    message_id = email_data["message_id"]
    references = email_data["references"]
    thread_id = email_data["thread_id"]
    original_email_id = email_data["original_email_id"]
    all_recipients = email_data["all_recipients"]
    cc_list = all_recipients.get("cc", [])

    service = authenticate_gmail(GMAIL_CREDENTIALS, GMAIL_FROM_ADDRESS)

    references_header = references
    if references and message_id:
        if message_id not in references:
            references_header = f"{references} {message_id}"
    elif message_id:
        references_header = message_id

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
        agent_name="API Test Agent - Large Payload",
    )

    if original_email_id:
        mark_email_as_read(service, original_email_id)

    logging.info(f"Email sent to {recipient} (thread: {thread_id})")
    return {"sent": True, "timestamp": datetime.now().isoformat()}


# ═══════════════════════════════════════════════════════════════
# DAG Definition
# ═══════════════════════════════════════════════════════════════
default_args = {
    "owner": "lowtouch.ai_developers",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 3,
    "retry_delay": timedelta(seconds=15),
}

with DAG(
    "api_test_executor_large_payload",
    default_args=default_args,
    description="API testing for large JSON payloads — classifies, chunks, and generates per-portion pytest files",
    schedule=None,
    start_date=datetime(2024, 2, 24),
    catchup=False,
    doc_md="""
# API Test Executor — Large Payload

Handles API testing when request/response JSON bodies are very large
(100K+ characters, e.g. gRPC clients with 13-lakh-character payloads).

## Pipeline

1. **Extract inputs** — parse email, collect JSON file paths (no large data in XCom)
2. **Classify & save schemas** — send first 70 lines of each JSON to the LLM to classify
   (postman_collection / request_schema / response_example / other).
   Request schemas are saved as-is to `testdata/schemas/`.
3. **Chunk & generate tests** — each schema is split into ~15 KB chunks.
   For each chunk the AI generates a pytest file that:
   - Loads the FULL schema from disk
   - Focuses on the fields in that chunk
   - Creates variations programmatically (deepcopy + mutations)
   - Uses @pytest.mark.parametrize for boundary/negative tests
4. **Run all tests** — single consolidated pytest run
5. **Email report** — HTML email with metrics + report link
6. **Send** — threaded Gmail reply

## Key Airflow Variable

| Variable | Purpose | Default |
|---|---|---|
| `ltai.api.test.chunk_size` | Max chars per chunk sent to AI | `15000` |

All other variables are shared with the v2 runner (see `apitest_runner_v2.py`).
    """,
    tags=["api", "testing", "large-payload", "pytest", "ai-agent"],
) as dag:

    # Step 1
    email_data = extract_inputs_from_email()

    # Step 2
    schema_data = classify_and_prepare_schemas(email_data)

    # Step 3
    generation_data = generate_test_files_from_chunks(schema_data)

    # Step 4
    run_data = run_all_tests(generation_data)

    # Step 5  (email_data passed directly — avoids threading huge payloads)
    email_content = generate_email_content(run_data, email_data)

    # Step 6
    send_result = send_response_email(email_content, email_data)

    # Done
    workflow_complete = EmptyOperator(
        task_id="workflow_complete",
        trigger_rule="all_success",
    )
    send_result >> workflow_complete
