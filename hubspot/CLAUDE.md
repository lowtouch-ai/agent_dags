# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is a set of Apache Airflow DAGs that power an AI-driven HubSpot CRM assistant. The system monitors a Gmail inbox, uses AI (via an Ollama-compatible API) to understand inbound emails, searches/creates HubSpot CRM entities, and replies via email. All DAGs run on an Airflow instance (Python 3.10).

## DAG Architecture

Six DAGs form an event-driven pipeline. The email listener is the entry point that triggers downstream DAGs via `TriggerDagRunOperator`.

```
hubspot_monitor_mailbox (every 1 min)
  ├── hubspot_search_entities (triggered) ──► hubspot_create_objects (triggered)
  ├── hubspot_task_completion_handler (triggered)
  └── (direct reply for simple queries)

hubspot_daily_task_reminders (hourly cron)
hubspot_retry_failed_tasks (every 5 min)
```

### DAG details

| DAG ID                            | File                        | Schedule | Purpose |
|-----------------------------------|-----------------------------|----------|---------|
| `hubspot_monitor_mailbox`         | `hubspot_email_listener.py` | 1 min    | Polls Gmail, classifies emails via AI, branches to search/create/reply/task-completion |
| `hubspot_search_entities`         | `hubspot_search.py`                    | None (triggered) | Searches HubSpot for contacts/companies/deals, validates associations, composes confirmation email |
| `hubspot_create_objects`          | `hubspot_object_creator.py`            | None (triggered) | Creates contacts, companies, deals, meetings, tasks in HubSpot after user confirmation |
| `hubspot_task_completion_handler` | `hubspot_task_completion_handler.py`   | None (triggered) | Handles task completion, deal stage updates, auto follow-up creation, and activity note logging |
| `hubspot_daily_task_reminders`    | `hubspot_task_scheduler.py` | `0 * * * *` | Sends per-task email reminders during each owner's business hours, respects holidays |
| `hubspot_retry_failed_tasks`      | `hubspot_retry_tasks.py` | 5 min | Auto-retries failed DAG runs (max 3 attempts, 20-min cooldown between retries) |

### Shared module pattern

`hubspot_email_listener.py` acts as a shared module. Other DAGs import from it via:
```python
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from hubspot_email_listener import send_fallback_email_on_failure, send_hubspot_slack_alert
```

Exported utilities: `send_fallback_email_on_failure`, `send_hubspot_slack_alert`, `get_email_thread`, `extract_all_recipients`.

### Common code across DAGs

Each DAG file (except retry) independently defines its own: `authenticate_gmail()`, `get_ai_response()`, `clear_retry_tracker_on_success()`, `update_retry_tracker_on_failure()`, `extract_json_from_text()`, and `default_args`. These are duplicated, not shared.

## External Services

- **Gmail API** (google-auth + googleapiclient) - reads/sends emails
- **HubSpot API** (REST, via `requests`) - CRUD on contacts, companies, deals, tasks, meetings, notes
- **AI/LLM** (Ollama-compatible API at `OLLAMA_HOST`) - email classification, entity extraction, response generation. Model: `hubspot:v6af_cl`. Client header: `x-ltai-client: hubspot-v6af_cl`
- **Slack** (webhook) - failure alerts

## Airflow Variables

All configuration is stored in Airflow Variables (not env vars). Key prefix: `ltai.v3.hubspot.*`

Core variables used across DAGs:
- `ltai.v3.hubspot.from.address` - Gmail sender address
- `ltai.v3.hubspot.gmail.credentials` - Gmail OAuth credentials JSON
- `ltai.v3.hubspot.ollama.host` - AI model endpoint (default: `http://agentomatic:8000`)
- `ltai.v3.husbpot.api.key` - HubSpot API key (**note the typo** in `husbpot` - this is the actual variable name, do not "fix" it)
- `ltai.v3.hubspot.url` - HubSpot API base URL
- `ltai.v3.hubspot.default.owner.id` / `.name` - Default CRM owner
- `ltai.v3.hubspot.task.owners` - JSON array of task owner configs (id, name, email, timezone)
- `hubspot_retry_tracker` - JSON dict tracking failed run retry state

## Data Flow

Inter-task communication uses **XCom** and **`dag_run.conf`**. When triggering downstream DAGs, `email_data` (containing thread ID, sender, subject, body) is passed via `conf`. The retry system also passes `original_run_id`, `original_dag_id`, and `retry_attempt` through `conf`.

### Retry system

The retry tracker (`hubspot_retry_tracker` Airflow Variable) tracks failed runs as a JSON dict keyed by `"{dag_id}:{run_id}"`. Each entry tracks `retry_count`, `status`, `failure_time`, and `max_retries_reached`. The retry DAG (`hubspot_retry_tasks.py`) increments `retry_count` when triggering each retry. The failure callback in `send_fallback_email_on_failure` records the current count without incrementing. After 3 failed retry attempts, status becomes `"max_retries_exceeded"` and no further retries are attempted. Cooldown between retries is 20 minutes.

`clear_retry_tracker_on_success` and `update_retry_tracker_on_failure` are called from `on_success_callback` / `on_failure_callback` in each DAG's `default_args`.

## Key Patterns

- **Branching**: `BranchPythonOperator` is used extensively in the email listener and search DAGs to route to different task paths based on AI classification. `branch_function` returns `"no_email_found_task"` when there are no unread emails.
- **Failure handling**: On failure, a fallback email is sent to the user and a Slack alert fires. Critical failures raise `ValueError` with descriptive messages rather than returning `None`. The object creator falls back to a structured error result (`status: "failure"`, `user_intent: "error"`) when AI response parsing fails.
- **AI JSON extraction**: `extract_json_from_text()` strips markdown code fences, tries parsing the full text first, then uses balanced-brace depth tracking to extract nested JSON objects. This handles nested JSON that simple regex cannot match.
- **Null-safety**: Access `email_data.get("headers", {})` (not `email_data["headers"]`), `dag_run.conf or {}` (not `dag_run.conf` directly), and `msg.get('role', 'unknown')` for chat history entries. Always guard against `None` values from XCom/conf.
- **Task threshold**: A `TASK_THRESHOLD = 15` limit prevents bulk task creation in a single request
- **XCom task_ids**: When pulling XCom values, specify `task_ids` explicitly (e.g., `ti.xcom_pull(key="chat_history", task_ids="load_context_from_dag_run")`) to avoid ambiguity when multiple tasks push the same key. The object creator enforces this for all context values loaded from `load_context_from_dag_run`.
- **Entity flag gating**: The search DAG uses `entity_search_flags` (pushed by `analyze_thread_entities`) to skip unnecessary work. Functions like `validate_deal_stage`, `validate_companies_against_associations`, and `validate_deals_against_associations` check their respective flags (`search_deals`, `search_companies`) and return early with empty results when the flag is `False`.

## Task Completion Reply Flow

When a user replies to a daily task reminder email, the flow is:

1. `branch_function` scans thread history for bot messages with `X-Task-ID` + `X-Task-Type: daily-reminder` headers
2. If found, sets `email["task_id"]` and `is_task_association=True`, routes to `other_emails` → AI classification
3. AI classifies the reply as `trigger_task_completion` → added to `task_completion_emails` (with `task_id` already set)
4. `trigger_task_completion_dag` reads `task_id` from the email, falls back to `check_if_task_completion_reply()` if missing, and triggers `hubspot_task_completion_handler`

The `check_if_task_completion_reply()` helper extracts `task_id` by checking direct headers first, then scanning thread history for the bot's reminder message. It is used as a fallback in both the AI routing path and the trigger function.

### Auto follow-up on task completion

When a task is marked completed, the handler automatically creates a follow-up task with a 2-week due date. This is a safety net: even if the AI fails to set `create_followup: true`, `process_task_completion` creates one if `mark_completed` is true and no follow-up was already created. The only way to suppress this is for the user to explicitly say "no follow-up" (or similar), which sets `suppress_followup: true` in the AI analysis.

Deal closure also implies task completion: "we lost the deal" sets `mark_completed: true` + `dealstage: Closed Lost` + `suppress_followup: true`.

### Activity note logging

After processing a task completion reply, `create_activity_note()` creates a HubSpot note that logs: the sender, their reply content, and all actions taken (completion, due date change, follow-up creation, deal updates). The note is associated with the task and all objects linked to that task (contacts, companies, deals) via HubSpot's v4 associations API.

## Task Owner Resolution

Task owners are resolved through a two-stage pipeline:

1. **Search DAG** (`compile_search_results`): Validates task owners via AI. Checks `task_owner_message` for phrases like "not valid", "not specified", or "not found" — if present, forces the default owner regardless of what `task_owner_id` the AI returned.

2. **Create DAG** (`create_tasks`): Prefers `task_owners` from `search_results` (passed via `dag_run.conf` from the search DAG). Falls back to AI-derived owners from `determine_owner` only when search results are absent. Uses `or` instead of `dict.get(..., default)` for owner fields to catch empty strings (e.g., `matching_owner.get("task_owner_id") or DEFAULT_OWNER_ID`).

## Deal Stage Validation Flow

The `hubspot_search_entities` DAG validates deal stages via two independent AI calls that run at different points in the pipeline:

1. **`validate_deal_stage`** (runs early) — First checks `entity_search_flags["search_deals"]` and skips entirely if no deals are mentioned. Otherwise parses deal stages from the user's email, validates them against `VALID_DEAL_STAGES`, and defaults invalid/missing stages to `"Lead"`. Pushes result to XCom as `deal_stage_info` with structure:
   ```json
   {"deal_stages": [{"deal_index": 1, "deal_name": "...", "validated_stage": "Lead"}]}
   ```

2. **Deal extraction** (`validate_deals_against_associations`, runs later) — Independently extracts deal details including `dealLabelName` from the email. This AI call may return invalid stages (e.g., "Discovery") and may produce different deal names than step 1.

3. **`compile_search_results`** — Applies the validated stage from step 1 to the deal from step 2. Uses **name match first**, then **index-based fallback** (`deal_index` → array position) to handle cases where the two AI calls return different deal names for the same deal.

### Valid Deal Stages

```python
VALID_DEAL_STAGES = [
    "Lead", "Qualified Lead", "Solution Discussion", "Proposal",
    "Negotiations", "Contracting", "Closed Won", "Closed Lost", "Inactive"
]
```

These are defined in `validate_deal_stage()`. Invalid or missing stages default to `"Lead"`.

### Known pattern: AI name mismatch

The two AI calls often return different deal names from the same email (e.g., `"PolicyStream – Policy Automation Program"` vs `"PolicyStream Insurance Technologies-Policy Automation Program"`). The index-based matching in `compile_search_results` handles this by mapping `deal_index` (1-based from `validate_deal_stage`) to array position in `new_deals`.

### Deal creation ownership

New deals are determined by `validate_deals_against_associations` (upstream). The later disambiguation step (`validate_deals_against_associations` → action resolution) **skips** any AI suggestion to "create_new" a deal and defers to the upstream `new_deals` list. This prevents duplicate deal creation from conflicting AI decisions.

### Company name normalization

When normalizing company names for comparison, the result is stored in `company_details["normalized_name"]` — the original `name` field is preserved unchanged.

## Validation Error Email

`compose_validation_error_email` uses an `ENTITY_TYPE_TO_SINGULAR` map to convert plural entity types (e.g., "Deals" → "Deal", "Companies" → "Company") when determining the primary entity for the email template. This ensures the correct template is used for non-task entities.

## Task Reminder Scheduling

The `hubspot_daily_task_reminders` DAG tracks sent reminders **per owner per day** using Airflow Variables keyed as `sent_reminders_{owner_local_date}_{owner_id}`. The `sent_today` set is fetched **per owner inside the loop** (with owner-specific `owner_id` and `timezone`), not once globally. This ensures each owner's reminders are evaluated in their own timezone, preventing duplicate sends when owners span different time zones.

## Common Pitfalls

These bugs have been fixed — avoid reintroducing them:

- **Set literal vs scalar**: `{DEFAULT_OWNER_ID}` creates a Python set, not a string. Use `DEFAULT_OWNER_ID` (no braces) when assigning to variables like `default_task_owner_id`.
- **Duplicate AI calls**: Do not call `get_ai_response()` twice for the same prompt. Each call is expensive and returns potentially different results.
- **Bare `raise` in non-except context**: Use `raise ValueError("descriptive message")` instead of bare `raise` — bare `raise` outside an `except` block causes `RuntimeError`.
- **`get_owner_name_from_id` return**: When an owner is found by ID, return `owner.get("name", DEFAULT_OWNER_NAME)` — not unconditionally `DEFAULT_OWNER_NAME`.
- **`validate_deal_stage` early return**: On error, push the default result to XCom AND return it. Missing the `return` causes fallthrough to code expecting `parsed_json`.
- **Amount formatting**: Use `int(float(det.get('amount', 0)))` to handle string amounts like `"50000.0"` — `int("50000.0")` raises `ValueError`.
- **Entity flag mismatch**: `validate_companies_against_associations` must check `search_companies` (not `search_contacts`). Similarly, `validate_deals_against_associations` must check `search_deals` (not `search_contacts`). Using the wrong flag silently skips validation for the wrong entity type.
- **Task owner "not valid" passthrough**: AI may return a `task_owner_message` like "not valid" or "not found" while still populating `task_owner_id` with a wrong value. Always check the message string and force defaults when it indicates invalidity.
- **Validation error entity type**: Use the `ENTITY_TYPE_TO_SINGULAR` map (not `rstrip('s')`) to convert entity types. `rstrip('s')` mangles "Companies" to "Companie" instead of "Company".

## Infrastructure

- **Docker containers**: `airflow` (base), `airflowsvr` (webserver), `airflowsch` (scheduler), `airflowwkr` (worker), `agentomatic` (AI model server)
- **Logs location**: `/appz/home/airflow/logs/dag_id=<dag_id>/run_id=<run_id>/task_id=<task_id>/attempt=N.log` (on `airflowwkr` container)
- **Airflow CLI**: Must run inside a container with DB access (e.g., `docker exec airflow airflow ...`). The `airflowsvr`/`airflowsch`/`airflowwkr` containers may have DB auth issues with CLI commands
