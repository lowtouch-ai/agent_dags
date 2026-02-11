# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an **Apache Airflow 2.x DAG repository** containing ~62 production DAGs for workflow orchestration. The DAGs integrate with Gmail, HubSpot, Ollama (local AI), BigQuery, Akamai, and dbt. Dependencies are managed at the parent Airflow deployment level — there is no `requirements.txt` in this directory.

An Airflow 3.1.3 migration is in progress. Use the `/airflow3x-upgrade <filename>` slash command to upgrade individual DAG files.

## Architecture

### DAG Categories

- **Email listeners & responders** — Gmail-based workflows (hubspot, autofinix, helpdesk, webshop, invoflux). Listeners fetch unread emails, process them, then trigger corresponding responder DAGs via `TriggerDagRunOperator`.
- **HubSpot integration** — `hubspot_email_listener`, `hubspot_object_creator`, `hubspot_search`, `hubspot_task_scheduler`, `hubspot_task_completion_handler`, `hubspot_retry_tasks`. These form a pipeline from email intake to CRM object management.
- **RFP processing** — 10 nearly identical DAGs (`rfp_*_processing_dag.py`) for different RFP types, routed by `selector_dag.py`.
- **SRE monitoring** — Daily/weekly health-check DAGs for TradeIdeas, UnityFi, MediaMelon.
- **Uptime reporting** — Hourly trigger + daily/weekly/monthly report DAGs.
- **Recruitment** — `cv_listner.py` monitors email, `cv_analyse.py` processes CVs.
- **dbt** — `jaffle_shop.py` runs dbt models via `astronomer-cosmos`. dbt project lives in `dbt/webshop/`.

### Shared Utilities (`utils/`)

- **`email_utils.py`** — Gmail OAuth2 authentication (`authenticate_gmail`), send email with thread continuity, fetch unread emails with attachments, mark as read, recipient extraction, timestamp tracking.
- **`agent_utils.py`** — PDF text extraction (via `langchain_community.PyPDFLoader`), image-to-base64 conversion, file validation (10MB limit), Ollama AI client (`get_ai_response`), JSON extraction from text, text sanitization.

Both utilities pull config from `Variable.get()` and log at DEBUG/INFO levels.

### Key Patterns

- **Configuration**: Airflow `Variable.get()` for all secrets and config, with `deserialize_json=True` for complex values.
- **Branching**: `BranchPythonOperator` for conditional task routing (15 DAGs).
- **DAG triggering**: `TriggerDagRunOperator` passes data between DAGs via `conf`.
- **Retry tracking**: JSON-serialized retry state stored in Airflow Variables.
- **Email threading**: Proper RFC 5322 `In-Reply-To` / `References` headers for Gmail thread continuity.
- **AI calls**: `ollama.Client` with conversation history lists (`{"role": ..., "content": ...}`).
- **Timezone handling**: `pendulum` for timezone-aware `start_date` and scheduling.

## Airflow 2.x → 3.x Migration

The codebase is currently Airflow 2.x. The full migration checklist is in `.claude/commands/airflow3x-upgrade.md`. Key changes needed across the codebase:

| Pattern | Files affected | Change |
|---|---|---|
| `provide_context=True` | 54 | Remove entirely (raises `TypeError` in 3.x) |
| `schedule_interval=` | 47 | Rename to `schedule=` |
| `DummyOperator` | 9 | Replace with `EmptyOperator` |
| `execution_date` in context | 7 | Replace with `logical_date` |
| `default_var=` in `Variable.get()` | Many | Rename to `default=` |
| All `airflow.operators.*` imports | All | Move to `airflow.providers.standard.operators.*` |
| `from airflow import DAG` | All | Change to `from airflow.sdk import DAG` |
| `from airflow.models import Variable` | All | Change to `from airflow.sdk import Variable` |

## Conventions

- DAG files use mixed naming: kebab-case (`helpdesk-email-listner.py`) and snake_case (`hubspot_search.py`). DAG IDs use underscores.
- No local build/test/lint commands — DAGs are deployed to and tested on the Airflow server at `airflow-server.lowtouchcloud.io`.
- Do not commit changes without user review. After making edits, summarize changes and let the user verify.
