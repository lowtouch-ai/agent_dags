# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This directory contains **5 Airflow 3.x DAGs** implementing a webshop email-driven e-commerce workflow. The DAGs handle email monitoring, AI-powered quote generation, daily data resets via dbt, on-demand sales reporting, and automated chat API testing. Part of a larger `agent_dags` repository (~62 production DAGs).

No local build/test/lint infrastructure — DAGs are deployed to and tested on the Airflow server at `airflow-server.lowtouchcloud.io`. Dependencies are managed at the parent Airflow deployment level.

## Architecture

```
Gmail Inbox
  │
  ▼  (polls every 1 min)
webshop-email-listener.py  (DAG: webshop_monitor_mailbox)
  │
  ▼  TriggerDagRunOperator → shared_send_message_email
webshop-email-respond.py   (DAG: shared_send_message_email)
  │  8-step AI pipeline: parse → lookup → discounts → summary → calc → QA → compose → send
  ▼
Gmail reply with HTML quote

webshop_reset_data.py      (DAG: webshop_reset_data)
  │  Daily 2:30 AM UTC — dbt seed (10 tables) → dbt run → dbt test → Elementary report
  ▼
Database refreshed to baseline

webshop_sales_report.py    (DAG: webshop_sales_report)
  │  On-demand — calls agentconnector API for analytics + top-selling products
  ▼
Report pushed to XCom

webshop_test.py            (DAG: webshop_run_chatapi_automation)
  │  Daily 14:30 UTC — Maven test suite → Slack notification on failure
  ▼
Slack alert if tests fail
```

### Email Listener → Responder Pipeline

The listener fetches unread emails via Gmail API, filters out no-reply addresses, and triggers `shared_send_message_email` for each valid email. The responder runs an 8-step sequential pipeline using Ollama AI (model: `webshop-email:0.5`) with conversation history maintained across steps via XCom. The final HTML quote email preserves Gmail thread continuity via `In-Reply-To`/`References` headers and `threadId`.

### Data Reset Pipeline

Uses dbt with Elementary data quality monitoring. Seeds 10 tables in parallel (address, articles, colors, customer, labels, order_positions, order_seed, products, stock, sizes), then runs the `order` model. Elementary report is generated and copied to `/appz/home/airflow/docs/edr_target/` for web access.

## Airflow Variables

| Variable | Purpose |
|---|---|
| `WEBSHOP_FROM_ADDRESS` | Gmail address to monitor and send from |
| `WEBSHOP_GMAIL_CREDENTIALS` | Gmail OAuth2 credentials (JSON, `deserialize_json=True`) |
| `WEBSHOP_OLLAMA_HOST` | Ollama server URL for AI quote generation |
| `WEBSHOP_POSTGRES_USER` | PostgreSQL username for dbt |
| `WEBSHOP_POSTGRES_PASSWORD` | PostgreSQL password for dbt |
| `API_TOKEN` | API auth token for Maven tests |
| `API_URL` | Target API URL for Maven tests |
| `SLACK_WEBHOOK_URL` | Slack webhook for test failure alerts |
| `SERVER` | Server name for Slack alert messages |

## Shared Utilities (`utils/`)

- **`email_utils.py`** — `authenticate_gmail`, `send_email` (with thread continuity), `fetch_unread_emails_with_attachments`, `mark_email_as_read`, `get_last_checked_timestamp`/`update_last_checked_timestamp`
- **`agent_utils.py`** — `get_ai_response` (Ollama client with streaming and conversation history), `extract_json_from_text`, `sanitize_text`, PDF/image processing

## Key Patterns (Airflow 3.x)

- **Imports**: `from airflow.sdk import DAG, Variable, TaskGroup, Param` and `from airflow.providers.standard.operators.python import PythonOperator`
- **Configuration**: `Variable.get()` with `default=` (not `default_var=`), `deserialize_json=True` for JSON values
- **Scheduling**: `schedule=` (not `schedule_interval=`)
- **Context**: `logical_date` (not `execution_date`)
- **No `provide_context=True`** — raises `TypeError` in Airflow 3.x
- **Timezone**: `pendulum` for timezone-aware `start_date`
- **DAG parameters**: Flat `Param` definitions for agent compatibility
- **Inter-DAG data**: `TriggerDagRunOperator` passes data via `conf`; intra-DAG uses XCom
- **AI calls**: `ollama.Client` with conversation history as `[{"role": ..., "content": ...}]`

## File Paths (on Airflow server)

- **dbt project**: `/appz/home/airflow/dags/agent_dags/dbt/webshop`
- **dbt executable**: `/dbt_venv/bin/dbt`
- **Email timestamp cache**: `/appz/cache/last_processed_email.json`
- **Elementary report**: `/appz/home/airflow/docs/edr_target/elementary_report.html`
- **Maven tests**: `/appz/home/airflow/dags/agent_dags/WebshopChatAPIAutomation`

## Conventions

- DAG files use mixed naming: kebab-case (`webshop-email-listener.py`) and snake_case (`webshop_reset_data.py`). DAG IDs always use underscores.
- The Airflow 3.x upgrade command (`/airflow3x-upgrade <file>`) automates migration from Airflow 2.x patterns — see `.claude/commands/airflow3x-upgrade.md` for the full checklist.
- Do not commit changes without user review. After making edits, summarize changes and let the user verify.
