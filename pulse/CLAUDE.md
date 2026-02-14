# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This directory contains the **specification for lowtouch.ai's Weekly Content Engine** — an intelligence-to-content pipeline that monitors the enterprise agentic AI landscape and produces multi-channel thought leadership content. Part of a larger `agent_dags` repository (~62 production Airflow 3.x DAGs).

**Status:** First cut implemented. YouTube-only data source, GPT-4o for analysis/drafting, XCom output (no email). See `pulse_content_engine.py`.

**Owner:** Rejith Krishnan, Founder/CEO

## Architecture

The workflow is a three-layer pipeline:

```
Layer 1: Intelligence Gathering
  │  YouTube Data API v3 (25-30 channels, 180+ videos)
  │  LinkedIn signals (hashtags, high-engagement posts)
  │  Industry sources (RSS, Google Alerts, curated URLs)
  │  Competitor watch (Microsoft Copilot, ServiceNow, UiPath, CrewAI, LangChain, AutoGen)
  ▼
Layer 2: Analysis & Synthesis
  │  Keyword/trend extraction → theme clustering → pillar mapping
  │  Content recommendation engine with priority scoring
  ▼
Layer 3: Content Drafting
  │  3 YouTube ideas, 2-3 LinkedIn posts, 2 carousels, 3 Reel scripts, 2 blog outlines
  │  10-15 engagement targets, Founder's Notebook prompt, competitor snapshot
  │  Day-by-day posting calendar with discoverability tags
  ▼
Output: HTML email (auto mode, Monday 6 AM IST) or Markdown (interactive mode)
```

### Operating Modes

| Mode | Trigger | Output | Follow-up |
|---|---|---|---|
| **Interactive** | Manual from Claude project chat | Markdown in chat | Yes — partial runs, refinements |
| **Auto** | Cron, Monday 6:00 AM IST (Sunday 7:30 PM CT) | HTML email to rejith@lowtouch.ai | No |

Both modes use the same intelligence/analysis pipeline. State is passed between tasks via XCom (persistent storage via Postgres planned for a later version).

## Content Positioning Pillars

All generated content must map to one or more of these five pillars:

1. **Private by Architecture** — runs inside customer infrastructure, no data leaves, air-gapped support
2. **No-Code** — business/technical teams assemble agents without writing code
3. **Production in 4-6 Weeks** — pre-built agents, predictable timelines
4. **Governed Autonomy** — HITL controls, thought-logging, compliance (ISO 27001, SOC 2, GDPR, RBI)
5. **Enterprise Architecture** — ReAct/CodeAct, Airflow orchestration, multi-LLM, full observability

## Content Voice Rules

- Write as a builder/practitioner, not a marketer. Lead with business outcomes.
- No em dashes (use commas, periods, semicolons, parentheses). No exclamation marks.
- No hype words: "revolutionary," "game-changing," "cutting-edge."
- Never position lowtouch.ai as a chatbot, copilot, or consumer AI tool. Always say "private, no-code Agentic AI platform."
- LinkedIn: no external links in post body (kills reach ~30%); links go in first comment.
- Instagram Reels: hook must land in first 1.5 seconds. Teleprompter-formatted (no line > 10 words).
- YouTube: first 30 seconds must state what the viewer will learn and why it matters.

## DAG Creation Guidelines

Follow the patterns established in `webshop/webshop_sales_report.py` (the reference DAG).

### File Layout

```python
# 1. Imports — airflow.sdk first, then providers, then standard library
from airflow.sdk import DAG, Param, TaskGroup
from airflow.providers.standard.operators.python import PythonOperator
import pendulum
import logging

from agent_dags.utils.think_logging import get_logger, set_request_id

# 2. Dual loggers — standard Airflow logger + Redis thought logger
logger = logging.getLogger(__name__)
lot = get_logger("pulse_content_engine")

# 3. Module-level constants and helper functions

# 4. Task callables (each accepts **context)

# 5. DAG definition using `with DAG(...) as dag:` context manager
```

### Task Callable Pattern

Every task callable must:
1. Accept `**context`
2. Call `set_request_id(context)` as the first line (enables Redis thought logging)
3. Use `lot.info()` for user-visible progress messages (shown in WebUI)
4. Use `logger.info()` / `logger.warning()` for standard Airflow logs
5. Return data to auto-push to XCom, or use `ti.xcom_push(key=..., value=...)` for named keys

```python
def my_task(**context):
    set_request_id(context)
    lot.info("starting my task...")
    p = context["params"]           # access DAG Param values
    ti = context["ti"]              # task instance for XCom
    upstream = ti.xcom_pull(task_ids="group_name.task_id")  # pull from grouped task
    # ... do work ...
    lot.info("my task complete")
    return result                   # auto-pushed to XCom
```

### Thought Logging (Required)

Thought logging is essential for this DAG, especially in interactive mode where agentomatic streams task progress to the WebUI in real time. Every task must emit `lot.info()` messages at meaningful progress points.

**How it works:** When agentomatic triggers a DAG via `RunJobTool`, it passes `__request_id` in `dag_run.conf`. The `set_request_id(context)` call extracts this and stores it in a `ContextVar`. Every subsequent `lot.info()` publishes a JSON payload to Redis channel `think:{request_id}`, which agentomatic streams to the WebUI `<think>` output.

**Guidelines:**
- Call `set_request_id(context)` as the **first line** of every task callable (Celery workers use separate contexts, so each task must set it independently)
- Use `lot.info()` at the start ("fetching YouTube data..."), at intermediate progress points ("analyzed 15 of 30 channels"), and at completion ("YouTube data ready: 180 videos from 28 channels")
- Keep messages lowercase, concise, and informative — they appear in the UI as a live progress stream
- Graceful no-op when `__request_id` is absent (manual Airflow triggers) — no errors, no Redis traffic
- `lot` has `propagate=False` — messages go only to Redis, not Airflow's root logger. Continue using `logger` for standard Airflow logs

### DAG Definition

```python
default_args = {"owner": "pulse", "start_date": pendulum.datetime(2025, 1, 1), "retries": 1}

with DAG(
    "pulse_content_engine",
    default_args=default_args,
    schedule=None,                  # or cron string like "0 0 * * 1" — never schedule_interval=
    catchup=False,
    tags=["pulse", "content"],
    description="...",
    params={
        "param_name": Param(default=..., type="string", description="..."),
        # use type="string", "integer", "boolean"; add enum=[...] for choices
    },
) as dag:
    ...
```

### TaskGroups and Dependencies

Group related tasks with `TaskGroup`. Tasks within a group can run in parallel unless chained with `>>`. The final assembly task pulls from all groups.

```python
with TaskGroup("intelligence") as intel_tg:
    youtube = PythonOperator(task_id="youtube_fetch", python_callable=...)
    linkedin = PythonOperator(task_id="linkedin_fetch", python_callable=...)
    # youtube and linkedin run in parallel within the group

with TaskGroup("analysis") as analysis_tg:
    ...

# Groups run in parallel, then feed into assembly
[intel_tg, analysis_tg] >> assemble
```

### XCom for State

- Task return values auto-push to XCom
- Pull from grouped tasks: `ti.xcom_pull(task_ids="group_name.task_id")`
- Pull from ungrouped tasks: `ti.xcom_pull(task_ids="task_id")`
- For large payloads use `ti.xcom_push(key="report", value=json.dumps(data))`
- Default to `{}` or `[]` when pulling: `ti.xcom_pull(...) or {}`

### Error Handling

- Use `logger.warning()` with graceful fallbacks (return empty list/dict) — don't let one API failure crash the whole DAG
- Wrap external API calls in try/except, log the error, continue with available data

### Airflow 3.x Rules

- `schedule=` (not `schedule_interval=`)
- `logical_date` (not `execution_date`)
- `Variable.get(key, default=...)` (not `default_var=`)
- No `provide_context=True` on PythonOperator (raises `TypeError` in 3.x)
- `pendulum` for timezone-aware `start_date`

### Integration Points

- **YouTube Data API v3** for channel discovery and video metrics
- **Gmail API** for HTML email delivery (auto mode) — use `utils/email_utils.py`
- **Ollama** for trend analysis and content drafting — use `utils/agent_utils.py`
- **Redis thought logging** for WebUI progress — use `utils/think_logging.py`

### Email Design (Auto Mode)

- Inline CSS only (Gmail strips `<style>` blocks)
- Arial/Helvetica, 14px body, 1.5 line height, max-width 640px centered
- Colors: navy headers `#1B2A4A`, accent pink `#E91E8C`, light gray cards `#F5F5F5`, body text `#333333`
- No images in email body

### Deployment

- DAGs folder bind-mounted from host to container — copy files to **host mount path** (not `docker cp`)
- Stop `gitrunner` container first to prevent overwriting local changes
- Airflow server: `airflow-server.lowtouchcloud.io`

## Agentomatic Integration (DAG-as-Tool)

The agent is defined in `scripts/agents/pulse.yaml` as model `pulse:0.3` with RAG optimizer enabled (following the `webshop:0.5o` pattern). At startup, agentomatic's `generate_tools_from_dag()` fetches the DAG's metadata (description, params) from the Airflow REST API and dynamically creates `run_pulse_content_engine` and `check_pulse_content_engine_status` tools.

### Agent YAML (`scripts/agents/pulse.yaml`)

```yaml
models:
  pulse:0.3:
    prompt:
      md:
        - pulse/role.md
        - pulse/instructions.md
    opt:
      name: basic
      queries:
        - query: Your task
          results: 0
        - query: __query__
          results: 1
        - query: __role__ OR __history__ OR __query__ OR __now__ OR __tz__ OR __my_name__ OR __my_email__
          results: 8
      embedding:
        type: ollama
        model: mxbai-embed-large

dags:
  - dag_id: "pulse_content_engine"
    model:
      - pulse:0.3
    active: yes
    wait_for_results: 5
```

The `opt` config enables RAG-based prompt optimization: each markdown `#` heading in the prompt files becomes a separate document in the vector store, and only sections relevant to the user's query are included in the system prompt (using `mxbai-embed-large` embeddings via Ollama).

### How the Agent Triggers the DAG

```
User (WebUI) → pulse:0.3 agent → RunJobTool._run()
  │  injects __request_id into conf
  ▼
Airflow REST API → triggers pulse_content_engine DAG
  │
  ▼  (each task callable)
set_request_id(context)  →  lot.info("...")
  │                           │
  │                           ▼
  │                      Redis PUBLISH think:{request_id}
  │                           │
  ▼                           ▼
XCom ← task return      agentomatic AsyncRedisThoughtListener
                              │
                              ▼
                         WebUI <think> stream (real-time progress)
```

### Design Requirements for Agent Compatibility

1. **DAG `description`** — agentomatic uses this as the tool description for the LLM. Write it as a clear, action-oriented sentence that tells the agent when to use this tool.

2. **DAG `params`** — each `Param` becomes a tool input parameter. Use JSON Schema types (`"string"`, `"integer"`, `"boolean"`). For optional params, use type list with null: `["string", "null"]` (agentomatic normalizes this and sets `required=False`).

3. **`__request_id` passthrough** — `RunJobTool` injects `__request_id` into `dag_run.conf` automatically. DAG tasks must call `set_request_id(context)` to pick it up. The `AsyncRedisThoughtListener` in agent.py already subscribes to `think:*`, so no listener changes are needed.

4. **Thought logging payload** — messages published by `lot.info()` include `"source": "airflow_dag"` (vs `"agent_connector"` for connector logs). This allows the WebUI to distinguish DAG progress from other system logs.

5. **Return value** — the final assembly task should push the complete report to XCom. The agent can then use `check_pulse_content_engine_status` to poll for completion and retrieve the result.

### Prompt Files (`scripts/agents/pulse/`)

Prompt markdown files live in `scripts/agents/pulse/` inside the agentomatic container. With optimizer enabled, each `#` heading becomes a retrievable section, so structure content with clear, descriptive headings. Key files:

- **`role.md`** — agent identity, purpose, positioning pillars, voice rules
- **`instructions.md`** — behavioral guidelines, content format specs, DAG usage instructions

## Weekly Output Targets

| Content Type | Volume | Platform |
|---|---|---|
| YouTube long-form ideas | 3 | YouTube |
| LinkedIn text posts | 2-3 | LinkedIn |
| LinkedIn carousels | 2 | LinkedIn |
| Instagram Reel scripts | 3 | Instagram |
| Blog/article outlines | 2 | LinkedIn / company blog |
| Engagement targets | 10-15 | LinkedIn (commenting) |
| Founder's Notebook prompt | 1 | LinkedIn |
| Competitor snapshot | 1 | Internal |
