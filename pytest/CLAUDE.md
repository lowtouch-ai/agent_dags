# API Testing DAGs

This directory contains the pytest-based API testing pipeline: a two-DAG system that monitors an email inbox for Postman collections and automatically generates, executes, and reports on API test suites.

## Architecture

```
Email (Postman JSON + optional PDF/config.yaml)
  │
  ▼
api_testing_listner.py  (DAG: api_testing_monitor_mailbox)
  │  polls Gmail every 1 min, extracts attachments
  │
  ▼  TriggerDagRunOperator
apitest_runner_v2.py    (DAG: api_test_executor_scenario_based)
  │  7-step pipeline: extract → scenarios → schemas → generate → run → email → send
  │
  ▼
Email reply with HTML report + Postman collection link
```

### api_testing_listner.py — `api_testing_monitor_mailbox`
- **Schedule**: every 1 minute
- **Purpose**: Polls Gmail for unread emails with JSON attachments (Postman collections). Optionally processes PDF docs and `config.yaml` for auth credentials.
- **Flow**: `fetch_unread_emails` → `branch_task` → `trigger_test_dag` | `no_email_found_task`
- **Trigger target**: `api_test_executor_scenario_based` (the v2 runner)
- **Timestamp tracking**: `/appz/cache/api_testing_last_processed_email.json` (milliseconds)
- **Attachments saved to**: `/appz/data/attachments/`

### apitest_runner_v2.py — `api_test_executor_scenario_based`
- **Schedule**: None (triggered by the listener)
- **Purpose**: End-to-end AI-driven API test generation, execution, and reporting.
- **Uses Airflow TaskFlow API** (`@task` decorator) — data flows via return values, not XCom push/pull.
- **Steps**:
  1. `extract_inputs_from_email` — parse dag_run.conf, load JSON/PDF, ask AI to extract requirements
  2. `generate_sub_test_scenarios` — AI generates 5-15 granular per-file test scenarios
  3. `extract_request_body_schemas` — AI extracts POST/PUT/PATCH body schemas to `testdata/schemas/`
  4. `generate_all_test_files` — AI generates all pytest files (max 10 tests each, conversation history avoids duplication)
  5. `run_all_tests` — single consolidated `pytest` run via AI agent tool, creates `.env` from config, cleans up after
  6. `generate_email_content` — AI produces HTML email with metrics + report link
  7. `send_response_email` — sends threaded Gmail reply, marks original as read

### api_testing_runner.py — v1 (legacy)
- **DAG ID**: `api_test_case_eapi_test_executor_scenario_based` (referenced in old listener)
- Uses traditional XCom push/pull instead of TaskFlow
- Generate-then-run per file (not batch)
- Kept for reference; v2 is the active runner

## Key Dependencies

- **Shared utilities** (`agent_dags/utils/`):
  - `email_utils.py` — `authenticate_gmail`, `send_email`, `mark_email_as_read`, `extract_all_recipients`
  - `agent_utils.py` — `get_ai_response`, `extract_json_from_text`
- **AI model**: configured via Airflow Variable `ltai.model.name` (default `APITestAgent:5.0` in v2, `PostmanAPITestAgent:3.0` in v1)
- **Gmail**: credentials in `ltai.api.test.gmail_credentials`, address in `ltai.api.test.from_address`

## Airflow Variables

| Variable | Purpose | Default |
|---|---|---|
| `ltai.api.test.from_address` | Gmail address to monitor | (required) |
| `ltai.api.test.gmail_credentials` | Gmail API credentials JSON | (required) |
| `ltai.model.name` | AI model for test generation | `APITestAgent:5.0` |
| `ltai.server.host` | Server host for report URLs | `http://localhost:8080` |
| `ltai.test.base_dir` | Base directory for test sessions | `/appz/pyunit_testing` |
| `ltai.api.test.test_mode` | When `"true"`, limits to 1 scenario | `"false"` |
| `ltai.api.test.target_endpoint` | Filter to test only this endpoint | `""` (all) |

## File Paths

- Test session dir: `{ltai.test.base_dir}/{thread_id}/`
- Schemas: `{test_session_dir}/testdata/schemas/`
- `.env` (runtime credentials): `{test_session_dir}/.env` (created and removed per run)
- Attachments: `/appz/data/attachments/`
- Timestamp file: `/appz/cache/api_testing_last_processed_email.json`
- Reports: `{server_host}/static/pytest_reports/{test_session_id}/index.html`

## Config.yaml Format (optional email attachment)

```yaml
credentials:
  API_KEY: ltai.credentials.api_key          # Airflow variable name → resolved at runtime
  USERNAME: ltai.credentials.username
  PASSWORD: ltai.credentials.password

auth:
  type: header | basic | bearer | api_key | custom
  headers:                    # for type=header or custom
    X-API-Key: API_KEY
  username_var: USERNAME      # for type=basic
  password_var: PASSWORD
  token_var: BEARER_TOKEN     # for type=bearer
```

## Conventions

- **No DELETE tests** — all prompts explicitly forbid DELETE endpoint testing
- **One assertion per test** — enforced in generation prompts
- **Max 10 test functions per file** — keeps files focused
- **Credentials via `.env` + `os.getenv()`** — never hardcoded
- **Fix-and-retry loop** (step 5) is currently **skipped** in the DAG wiring but the function is retained
- `MAX_FIX_ITERATIONS = 3` — caps the fix loop when re-enabled
- Thread continuity maintained via `Message-ID`, `References`, and `threadId`

## Common Tasks

### Adding a new Airflow Variable for credentials
1. Add the variable in Airflow UI
2. Reference it in `config.yaml` under `credentials:` with the variable name as the value
3. The `_create_env_file` helper resolves it at runtime

### Changing the AI model
Update `ltai.model.name` in Airflow Variables. Both DAGs pick it up on next run.

### Debugging test generation
- Check Airflow logs for the `generate_all_test_files` task — each file generation is logged with char count
- Generated files land in `{ltai.test.base_dir}/{thread_id}/`
- The conversation history is trimmed to last 5 entries to manage prompt size

### Re-enabling the fix-and-retry loop
In the DAG definition at the bottom of `apitest_runner_v2.py`, wire `fix_and_retry_loop` between `run_all_tests` and `generate_email_content`. Currently step 5 is skipped and `run_data` flows directly to email generation.
