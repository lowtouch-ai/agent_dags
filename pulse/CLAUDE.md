# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This directory contains the **specification for lowtouch.ai's Weekly Content Engine** — an intelligence-to-content pipeline that monitors the enterprise agentic AI landscape and produces multi-channel thought leadership content. Part of a larger `agent_dags` repository (~62 production Airflow 3.x DAGs).

**Status:** First cut implemented. YouTube-only data source, GPT-4o for analysis/drafting, XCom output (no email). See `pulse_content_engine.py`. Article creator DAG (`pulse_article_creator.py`) expands report items into full LinkedIn articles with AI graphics.

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

## LinkedIn Article Best Practices

These guidelines are baked into `pulse_article_creator.py`'s `write_article` and `humanize_article` prompts. Any changes here should be reflected in those prompts.

### Article Length

- Target: **1,500-1,800 words** (sweet spot for LinkedIn dwell time and completion rate)
- Viable range: 1,200-2,000 words
- LinkedIn's algorithm measures completion rate, not raw length. Substantive 1,200-word articles outperform padded 2,000-word ones.
- Dwell time is the **#1 ranking factor** in LinkedIn's algorithm.

### 7-Part Article Structure

| Section | Purpose | Share |
|---------|---------|-------|
| Headline | Capture attention, include keywords | N/A |
| Hook (first 2-3 sentences) | Create urgency or curiosity | 5% |
| Context | Establish the problem and relevance | 10% |
| Core Insights (3-5 sections) | The substance | 50% |
| Proof/Evidence | Data, case examples, metrics | 20% |
| Takeaway | Key learning in 1-2 sentences | 5% |
| Call-to-Action | Specific, question-based ending | 10% |

### Headline

- **Under 60 characters** for mobile visibility (LinkedIn's max is 150)
- Include 1-2 target keywords (LinkedIn articles rank in Google; domain authority 98/100)
- Strong patterns: "Why [common belief] is wrong", "[Number] [things] that [outcome]", "We [did X]. Here is what [happened]."
- Avoid generic headlines: "AI in Enterprise", "Thoughts on Automation"

### Hook (First 200 Characters)

The first 200 characters are the **LinkedIn preview** that appears in the feed. This is the single most important part of the article for distribution.

**Proven techniques:**
- Surprising statistic: "63% of the people who kill your B2B deals never appear on your buying committee."
- Contrarian claim: "Most enterprise AI pilots fail not because of technology, but because of architecture decisions made in week one."
- Specific problem with stakes: "Your team spent six months building an AI agent. It works in the demo. It fails in production. Here is why."

**Never open with:** "In today's rapidly evolving...", "As we navigate...", "It's no secret that...", or any throat-clearing.

### Formatting for Mobile (60%+ of LinkedIn readers)

- Paragraphs: **1-3 sentences max** (long paragraphs become walls of text on mobile)
- Sentences: **under 20 words** on average
- **Bold** key phrases (not full sentences) for scanners
- Bullet points and numbered lists for sequences, comparisons, takeaways
- ## headers every **200-300 words** for scannability
- Tables produce **40% more engagement** than equivalent text
- Charts/data visualizations generate **3x more comments**
- One image per 300-400 words produces **2x more saves**
- Generous whitespace between paragraphs

### Call-to-Action

- End with a **specific, answerable question** that invites comments
- Ask about the reader's experience, a specific challenge, or whether they agree/disagree with a clear position
- Comments count **2x as much as likes** in the algorithm
- High comment velocity in the first 60-90 minutes ("golden hour") dramatically boosts reach
- **Never use:** "share if you agree", "like and follow for more", generic "let me know your thoughts" (engagement bait is penalized)

### SEO and Discoverability

- LinkedIn articles are **indexed by Google** (posts are rarely indexed)
- Front-load target keywords in the first 200 words (2-3 times)
- Include keyword variations in ## subheadings
- Use **3-5 hashtags**: 1-2 high-volume + 2-3 niche
- External links within articles do NOT carry the same algorithmic penalty as links in posts

### AI-isms to Remove (Humanize Pass)

These phrases signal AI-generated content and reduce engagement by ~43%:

| AI-ism | Replacement |
|--------|-------------|
| "In today's rapidly evolving landscape" | Cut entirely or replace with specific context |
| "It's worth noting" | Just state the point |
| "Let's dive in" | Cut |
| "At the end of the day" | "Practically" or "in production" |
| "Holistic approach" | Be specific about what the approach includes |
| "Robust solution" | Describe what makes it reliable |
| "Seamlessly integrate" | Describe the actual integration |
| "The reality is" | Just state the reality |
| "Here's the thing" | Cut |
| "Needless to say" | Cut, or just state it directly |

### Key Statistics (2025 Edelman-LinkedIn B2B Thought Leadership Report)

| Metric | Value |
|--------|-------|
| Hidden buyers spending 1+ hr/week on thought leadership | 63% |
| Hidden buyers more receptive to outreach after strong thought leadership | 95% |
| CEO-shared content engagement multiplier vs company page | 4x |
| CTA impact on click-through rates | Up to 285% improvement |
| AI-only content engagement penalty vs hybrid (AI-drafted + human-refined) | -43% |
| External links reach reduction in posts (not articles) | 25-40% |

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

**DAG files** (Airflow):
- DAGs folder bind-mounted from host to container: `/mnt/c/Users/krish/git/airflow/dags` → `/appz/home/airflow/dags`
- Copy DAG `.py` files to: `cp pulse_*.py /mnt/c/Users/krish/git/airflow/dags/agent_dags/pulse/`
- Stop `gitrunner` container first to prevent overwriting local changes: `docker stop gitrunner`
- Three containers share the mount: `airflowsvr`, `airflowsch`, `airflowwkr`
- Airflow server: `airflow-server.lowtouchcloud.io`

**Agent files** (agentomatic tools, prompts, YAML):
- Scripts folder bind-mounted: `/home/krish/git/AppZ-Images/agentomatic-3.1/scripts` → `/appz/docker/agentomatic-3.1/scripts` (dev mode) and `/appz/dev`
- Agent prompts bind-mounted: `scripts/agents/` → `/appz/agents/`
- In dev mode (`APPZ_DEV_MODE`), uvicorn runs with `--reload` — editing files on the host auto-reloads the agent. No container restart needed.
- To force reload: `docker exec agentomatic touch /appz/docker/agentomatic-3.1/scripts/agent.py`
- In production (no `APPZ_DEV_MODE`): must restart the container: `docker restart agentomatic`

## Agentomatic Integration (DAG-as-Tool)

The agent is defined in `scripts/agents/pulse.yaml` as model `pulse:0.3` with RAG optimizer enabled (following the `webshop:0.5o` pattern). At startup, agentomatic's `generate_tools_from_dag()` fetches each DAG's metadata (description, params) from the Airflow REST API and dynamically creates tools. For `pulse_content_engine`: `run_pulse_content_engine` and `check_pulse_content_engine_status`. For `pulse_article_creator`: `run_pulse_article_creator` and `check_pulse_article_creator_status`.

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
  │  injects __request_id and __user_query into conf
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

4. **`__user_query` passthrough** — `RunJobTool` injects `__user_query` (the user's exact WebUI message) into `dag_run.conf` automatically. DAGs can read this via `context["dag_run"].conf.get("__user_query")` to get the real user prompt, bypassing any LLM rephrasing in the agent's `instructions` param. This is critical for edit flows where the agent historically fails to pass the user's verbatim message.

5. **Thought logging payload** — messages published by `lot.info()` include `"source": "airflow_dag"` (vs `"agent_connector"` for connector logs). This allows the WebUI to distinguish DAG progress from other system logs.

6. **Return value** — the final assembly task should push the complete report to XCom. The agent can then use `check_pulse_content_engine_status` to poll for completion and retrieve the result.

### Prompt Files (`scripts/agents/pulse/`)

Prompt markdown files live in `scripts/agents/pulse/` inside the agentomatic container. With optimizer enabled, each `#` heading becomes a retrievable section, so structure content with clear, descriptive headings. Key files:

- **`role.md`** — agent identity, purpose, positioning pillars, voice rules
- **`instructions.md`** — behavioral guidelines, content format specs, DAG usage instructions

## Article Creator DAG (`pulse_article_creator.py`)

**Version:** Pulse Article Creator v0.3

A second DAG that expands any content item from a weekly report into a full LinkedIn article with AI-generated header graphic. Supports iterative editing with smart auto-detection.

### Pipeline

```
load_context → deep_research → write_article → humanize_article → generate_graphics → assemble_article
```

### Params

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `report_id` | string | required | UUID of the content report to pull source data from |
| `content_type` | string | `"blog_outline"` | Which content section: `blog_outline`, `linkedin_post`, `youtube_idea`, `carousel`, `reel`, `founders_notebook` |
| `content_index` | integer | `0` | 0-based index of the item within that content section |
| `article_id` | `["string", "null"]` | null | If editing an existing article, pass its UUID. Auto-resolved from Redis index if not provided. |
| `instructions` | string | required | User's message (fallback; DAG prefers `__user_query` from conf) |
| `regenerate_graphic` | boolean | `false` | Hint to regenerate graphic. DAG auto-detects this from user prompt too. |

### Auto-Detection (Smart Defaults)

The DAG minimizes reliance on the agent passing correct parameters:

- **`__user_query`**: The DAG reads the user's exact prompt from `dag_run.conf["__user_query"]` (injected by `RunJobTool`), overriding whatever the agent passes as `instructions`. This solves the problem of agents rephrasing or passing stale prompts.
- **Create vs edit**: Auto-detected via Redis index key (`pulse:article_index:{report_id}:{content_type}:{content_index}`). If an article exists for that slot, it's an edit. No agent logic needed.
- **`article_id` resolution**: Auto-resolved from the same Redis index key if not explicitly provided.
- **LLM intent classification**: On every run, `_classify_intent()` sends the user prompt to GPT-4o to determine: (1) whether to regenerate the graphic, (2) whether it's a graphic-only request, and (3) cleaned edit instructions. This replaces brittle keyword matching and handles any phrasing naturally (e.g., "redo the header", "give me a fresh banner", "new image and shorten the intro").
- **Graphic-only mode**: If the LLM classifies the request as purely about graphics, the DAG skips `write_article` and `humanize_article` entirely, preserving the existing article text and only regenerating the graphic.
- **Base64 stripping**: During edits, the stored `article_md` (which includes the full base64 graphic blob from `assemble_article`) is stripped of image data before being sent to GPT-4o, preventing context length overflow.

### How It Works

**New article flow:** `report_id` + `content_type` + `content_index` → DAG loads report from Redis, extracts the content item, runs deep research using report trends and source videos, writes a 1,200-2,000 word article via GPT-4o (following LinkedIn best practices: 7-part structure, hook-first opening, mobile-optimized formatting), humanizes it (removes AI-isms, validates hook and CTA, enforces voice rules), generates a header graphic via `gpt-image-1`, assembles everything, and persists to Redis under `pulse:article:{article_id}` with 7-day TTL.

**Edit flow:** Auto-detected when an article already exists. Loads existing article from Redis, skips deep research (reuses existing research), rewrites applying user instructions, re-humanizes, reuses existing graphic by default, saves as new version under same `article_id`.

**Graphic-only flow:** Auto-detected when user prompt contains only graphic-related keywords. Skips `write_article` and `humanize_article`, keeps existing article text, regenerates only the header graphic.

### Article Output Footer

```
*Article ID: {uuid} | Report: {report_id} | {word_count} words | Generated: {timestamp} | v{version} | Pulse Article Creator v0.3*
```

### Storage

- Articles persisted to Redis: `pulse:article:{article_id}` (7-day TTL)
- Article index keys: `pulse:article_index:{report_id}:{content_type}:{content_index}` → `article_id` (enables auto-resolution)
- Each edit increments the version number
- Stored data includes: article markdown (with base64 graphic inline), header graphic (base64), graphic prompt, research data, metadata

### Agent Tools

agentomatic generates `run_pulse_article_creator` and `check_pulse_article_creator_status` tools from this DAG. The agent uses these when the user says "create the article on blog outline 1" or "make it shorter".

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
