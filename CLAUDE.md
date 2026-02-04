# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an Apache Airflow DAG repository (`agent_dags`) for lowtouch.ai. It contains ~65 Python DAG files that orchestrate AI-powered business automation workflows including email monitoring/response, RFP document processing, voice messaging, SRE reporting, and CRM integration.

## Repository Structure

- **Root directory** — All Airflow DAG Python files live at the repo root (flat structure)
- **`/utils/`** — Shared utility modules:
  - `agent_utils.py` — PDF/image extraction, text sanitization, Ollama LLM client (`get_ai_response`)
  - `email_utils.py` — Gmail API authentication, email sending with threading, attachment processing
- **`/dbt/webshop/`** — dbt project for webshop data transformations (PostgreSQL, requires dbt >=1.0.0 <2.0.0)
- **`/Invoflux-ui-tests/`** — Java/Maven Selenium UI tests (Java 11, TestNG, Selenium 4.20)
- **`/WebshopChatAPIAutomation/`** — Java/Maven REST API tests (Java 8, TestNG)

## Architecture & Key Patterns

### DAG Design Patterns
- **Listener-Responder pattern**: Email listener DAGs poll Gmail every minute, store data in XCom, then trigger separate responder DAGs via `TriggerDagRunOperator`
- **XCom data passing**: Tasks communicate through `context['ti'].xcom_push/xcom_pull`
- **Branch logic**: `BranchPythonOperator` for conditional workflows
- **Configuration**: All secrets/config stored as Airflow Variables (e.g., `GMAIL_CREDENTIALS`, `OLLAMA_HOST`, `HUBSPOT_API_KEY`)

### DAG Categories
| Category | Key Files | Purpose |
|----------|-----------|---------|
| Email automation | `*-email-listener.py`, `*-email-respond.py` | Gmail monitoring + AI response generation (invoflux, webshop, helpdesk, autofinix) |
| RFP processing | `selector_dag.py`, `rfp_*_processing_dag.py` | PDF classification, extraction, AI-powered answer generation with quality validation |
| HubSpot CRM | `hubspot_*.py` | Contact/company search, object creation, task scheduling, email listener |
| SRE reporting | `sre-*.py`, `uptime-*.py` | BigQuery queries → HTML/PDF reports via ReportLab |
| Voice messaging | `send_voise_message.py`, `voice_text_transcribe.py` | Twilio outbound calls, Whisper transcription |
| Data pipelines | `jaffle_shop.py`, `webshop_reset_data.py` | dbt integration (see detailed section below) |

### AI Integration
- LLM inference goes through Ollama at `http://agentomatic:8000` using the `ollama` Python client
- Model names are stored in Airflow Variables (e.g., `LYNX_MODEL_NAME` defaults to `Lynx:3.0-sonnet-4-5`)
- RFP DAGs use a multi-attempt quality validation loop with a threshold score of 8/10
- Backend API at `http://agentconnector:8000` for RFP project management

### Data Pipeline DAGs (jaffle_shop & webshop_reset_data)

These two DAGs manage dbt pipelines but use different integration approaches.

#### `jaffle_shop.py` — Cosmos-based dbt integration
- **dbt project**: Separate repo at https://github.com/lowtouch-ai/jaffle_shop, deployed to `/appz/home/airflow/dags/dbt/jaffle_shop` on the Airflow server
- **Integration**: Uses [Astronomer Cosmos](https://github.com/astronomer/astronomer-cosmos) (`DbtTaskGroup`) to render dbt nodes as native Airflow tasks
- **Schedule**: Every 12 hours (`0 0/12 * * *`)
- **Tags**: `sample-dag`
- **Profile**: `jaffle_shop` / `dev` target
- **DAG run config**: Accepts optional `payment_type` parameter via `dag_run.conf`
- **Task pipeline**:
  ```
  print_variables → dbt_seeds_group (seeds/) → dbt_stg_group (models/staging/) → dbt_tg (remaining models) → post_dbt
  ```
- The three `DbtTaskGroup` instances split dbt execution into seeds, staging models, and final models using `RenderConfig(select=...)` / `RenderConfig(exclude=...)`
- Uses `on_failure_callback` that prints task/DAG metadata (no Slack or email alert)

#### `webshop_reset_data.py` — BashOperator-based dbt integration
- **dbt project**: `dbt/webshop/` (in this repo)
- **Integration**: Uses `BashOperator` to shell out to dbt CLI (sources `/dbt_venv/bin/activate` before each command)
- **Schedule**: Daily at 2:30 AM UTC / 8:00 AM IST (`30 2 * * *`)
- **Tags**: `reset`, `webshop`, `data`
- **Purpose**: Resets the webshop database to a baseline state from seed CSVs daily
- **Airflow Variables**: `WEBSHOP_POSTGRES_USER`, `WEBSHOP_POSTGRES_PASSWORD`
- **Documentation**: Reads `webshop.md` as `doc_md`
- **Task pipeline**:
  ```
  dbt_deps → dbt_seed (10 parallel seeds) → dbt_run (order model) → dbt_test → dbt_run_elementary → generate_elementary_report → copy_elementary_report
  ```
- `dbt_seed` runs 10 seeds in parallel within a `TaskGroup`: address, articles, colors, customer, labels, order_positions, order_seed, products, stock, sizes
- `dbt_run` executes only the `order` model
- `dbt_test` appends `|| true` so test failures do not block the pipeline
- Every task passes Elementary env vars (`ELEMENTARY_ORCHESTRATOR`, `ELEMENTARY_JOB_NAME`, `ELEMENTARY_JOB_ID`, `ELEMENTARY_JOB_RUN_ID`) and sets `DBT_PROFILES_DIR` to the project directory

#### Elementary Data observability (webshop_reset_data only)
- After dbt test, runs `dbt run --select elementary` to refresh Elementary models into the `webshop_elementary` schema
- Generates an HTML report via `/dbt_venv/bin/edr report` into `{dbt_project_dir}/edr_target/`
- Copies the report to `/appz/home/airflow/docs/edr_target/` for serving at `https://airflow2025a.lowtouch.ai/docs/edr_target/elementary_report.html`

#### Webshop dbt project details (`dbt/webshop/`)
- **Profile name**: `ai_lab`, **target**: `dev`
- **Database**: PostgreSQL at `agentconnector:5432`, database `webshop_v2`, schema `webshop`
- **dbt version**: requires `>=1.0.0, <2.0.0`
- **Package**: `elementary-data/elementary` v0.18.3
- **Materialization**: `table` in dev, `view` otherwise
- **Seed data**: 10 CSV files in `dbt/webshop/data/` with column types defined in `dbt_project.yml`
- **Models**: Single `order.sql` model — materialized as table, shifts `ordertimestamp` forward by the day difference between `NOW()` and the max seed timestamp, making historical seed data appear current
- **Tests**: Defined in `models/order_test.yml` and `data/seeds_schema.yml` — not_null, unique, and relationship (foreign key) tests across seeds and the order model
- **Seed relationships**: `order_seed` → `customer` → `address`; `order_positions` → `order_seed` + `articles`; `articles` → `products` + `colors`; `products` → `labels`; `stock` → `articles`

### Default DAG Configuration
- Owner: `lowtouch.ai_developers`
- Retries: 1-2, retry delay: 15-60s
- Failure callbacks: email notifications and Slack alerts
- Email monitors scheduled every 1 minute; reports scheduled daily/weekly

## Deployment Environment

- DAG directory: `/appz/home/airflow/dags/`
- dbt executable: `/dbt_venv/bin/dbt`
- Cache/data directories: `/appz/cache/`, `/appz/data/`
- Attachments: `/appz/data/attachments/`
- Database: PostgreSQL at `agentconnector:5432` (webshop_v2)

## Commands

### dbt — webshop project (from `dbt/webshop/`)
```bash
# Activate venv first
source /dbt_venv/bin/activate

# Install packages (elementary-data)
/dbt_venv/bin/dbt deps

# Load all seed data
/dbt_venv/bin/dbt seed

# Load a single seed
/dbt_venv/bin/dbt seed --select address

# Run the order model
/dbt_venv/bin/dbt run --select order

# Run tests
/dbt_venv/bin/dbt test

# Run Elementary models + generate report
/dbt_venv/bin/dbt run --select elementary
/dbt_venv/bin/edr report --project-dir . --profiles-dir . --target-path ./edr_target
```

Requires env vars: `WEBSHOP_POSTGRES_USER`, `WEBSHOP_POSTGRES_PASSWORD`, `DBT_PROFILES_DIR` (set to the project dir), and Elementary vars (`ELEMENTARY_ORCHESTRATOR`, `ELEMENTARY_JOB_NAME`, `ELEMENTARY_JOB_ID`, `ELEMENTARY_JOB_RUN_ID`).

### dbt — jaffle_shop project
The jaffle_shop dbt project is in a separate repo (https://github.com/lowtouch-ai/jaffle_shop), deployed to `/appz/home/airflow/dags/dbt/jaffle_shop`. The `jaffle_shop.py` DAG runs it via Cosmos `DbtTaskGroup` — no manual CLI needed.

### Java Tests
```bash
# Invoflux UI tests
cd Invoflux-ui-tests && mvn test

# WebshopChat API tests
cd WebshopChatAPIAutomation && mvn test
```

## External Service Dependencies

- **Gmail API** — OAuth credentials for email monitoring/sending
- **HubSpot API** — CRM operations (contacts, companies, tasks)
- **Twilio API** — Outbound voice calls and recordings
- **BigQuery** — SRE and uptime reporting data
- **Slack** — Failure notifications
- **Ollama (agentomatic)** — Local LLM inference
- **PostgreSQL (agentconnector)** — Application database

## Git Workflow

- Branch-based development with PR merges to `main`
- Branch naming: `{ticket_number}/{description}` (e.g., `10732/invoflux_email_fix`)
- Upstream fork: `Akshaidevopscc/airflow_dags`
