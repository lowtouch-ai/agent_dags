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
| `hubspot_task_completion_handler` | `hubspot_task_completion_handler.py`   | None (triggered) | Handles task completion and deal stage updates via email |
| `hubspot_daily_task_reminders`    | `hubspot_task_scheduler.py` | `0 * * * *` | Sends per-task email reminders during each owner's business hours, respects holidays |
| `hubspot_retry_failed_tasks`      | `hubspot_retry_tasks.py` | 5 min | Auto-retries failed DAG runs (max 3 attempts, 20-min cooldown) |
 
### Shared module pattern

`hubspot_email_listener.py` acts as a shared module. Other DAGs import from it via:
```python
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from hubspot_email_listener import send_fallback_email_on_failure, send_hubspot_slack_alert
```

Exported utilities: `send_fallback_email_on_failure`, `send_hubspot_slack_alert`, `get_email_thread`.

### Common code across DAGs

Each DAG file (except retry) independently defines its own: `authenticate_gmail()`, `get_ai_response()`, `clear_retry_tracker_on_success()`, `update_retry_tracker_on_failure()`, and `default_args`. These are duplicated, not shared.

## External Services

- **Gmail API** (google-auth + googleapiclient) - reads/sends emails
- **HubSpot API** (REST, via `requests`) - CRUD on contacts, companies, deals, tasks, meetings
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

## Key Patterns

- **Branching**: `BranchPythonOperator` is used extensively in the email listener and search DAGs to route to different task paths based on AI classification
- **Failure handling**: On failure, a fallback email is sent to the user and a Slack alert fires. The retry tracker (Airflow Variable) coordinates retry state across DAGs
- **AI JSON extraction**: AI responses often need cleanup - markdown code fences and HTML entities are stripped, then JSON is parsed with regex fallbacks
- **Task threshold**: A `TASK_THRESHOLD = 15` limit prevents bulk task creation in a single request
