# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this folder.

## Overview

RFP (Request for Proposal) document processing pipeline. Uses Ollama LLMs to classify incoming RFP documents, extract questions, and auto-generate answers for investment management questionnaires.

## Architecture

### Entry Point: `selector_dag.py` (dag_id: `rfp_document_selector_dag`)

Triggered externally with `dag_run.conf` containing `project_id`, `workspace_uuid`, `x-ltai-user-email`, and optionally `document_type`. Flow:

1. **Fast-path check**: If `document_type` is provided (not `AUTO`), skips AI classification
2. **PDF fetch & text extraction**: Downloads PDF from `agentconnector` API, extracts text with `pypdf`
3. **AI classification**: Sends text to Ollama model `rfp/autogeneration_extraction:0.3af` to determine document type
4. **DAG routing**: Uses `TriggerDagRunOperator` to delegate to the appropriate processing DAG based on `DOCUMENT_TYPE_TO_DAG` mapping

### Shared Base: `rfp_processing_base.py`

Contains all shared logic via a `create_rfp_processing_dag(dag_id, description, tags)` factory function. Includes helpers, prompt templates, and the 6-task pipeline definition.

### Processing DAGs (10 variants)

Each `rfp_*_processing_dag.py` is a thin wrapper (~9 lines) that calls `create_rfp_processing_dag()` with its unique `dag_id`, `description`, and `tags`. The factory creates an identical 6-task pipeline:

1. `fetch_pdf_from_api` — Fetches project details (including `context_and_instructions`), downloads PDF, converts to markdown via `pymupdf4llm`
2. `extract_questions_with_ai` — Chunk-Map-Reduce extraction using `MODEL_FOR_EXTRACTION`
3. `validate_and_fix_questions` — Chunk-level validation: sends each text chunk + known keys to AI to recover missed questions
4. `generate_answers_with_ai` — Generates answers using `MODEL_FOR_ANSWERING` with RAG tool calls; incorporates project context
5. `log_completion` — Updates project status to `review`, saves `processing_dag_run_id`
6. `trigger_quality_audit` — Triggers quality audit DAG as background process (non-blocking)

### Quality Audit: `rfp_quality_audit_dag.py` (dag_id: `rfp_quality_audit_dag`)

Background quality assurance and auto-remediation process automatically triggered after processing DAG completion. Validates question-answer pairs, generates comprehensive quality reports, and **automatically fixes detected issues**.

**IMPORTANT:** This is a **background task** that does NOT affect project status. Project remains in `review` state regardless of audit outcome. Audit failures are logged but don't set project to `failed`.

**Pipeline (10 tasks):**
1. `fetch_project_data` — Retrieves project and all Q&A pairs from API (handles pagination to fetch all pages)
2. **Parallel Quality Checks (5 tasks):**
   - `check_duplicate_questions` — Fuzzy matching to detect similar/duplicate questions (85% similarity threshold, pattern-based)
   - `check_answer_completeness` — **Two-stage:** Pattern checks (empty, too short, error keywords) + **AI semantic checks** (off-topic, placeholder text, incomplete thoughts)
   - `check_answer_formatting` — **Two-stage:** Pattern checks (malformed JSON, repeated text, excessive length) + **AI structural checks** (wall of text, missing lists/tables)
   - `check_reference_validity` — Validates `sources_referenced` field; allows `["General LLM reasoning"]` for low confidence only; flags placeholders ("None", "N/A", "null")
   - `analyze_confidence_distribution` — Tracks High/Medium/Low confidence distribution, flags if >30% are Low (pattern-based)
3. `aggregate_quality_results` — Combines all checks, counts issues by severity (high/medium/low)
4. **Auto-Remediation (2 tasks, run SEQUENTIALLY):**
   - `remediate_duplicate_questions` — Deletes duplicate questions via API, keeps highest quality version (scored by confidence + answer length + sources), updates `question_count` and `answer_generated_count`
   - `remediate_low_quality_answers` — Regenerates missing/incomplete/low-confidence answers; reformats malformed answers; skips questions deleted as duplicates
5. `update_project_with_audit` — Saves audit results + remediation stats to project record as `quality_audit_summary` JSON

**Auto-Remediation Logic:**
- **Duplicate Removal**: Calculates quality score (confidence=30pts, answer_length=up_to_20pts, has_sources=15pts) and keeps the higher-scoring question; updates project question counts after deletion
- **Answer Regeneration**: Missing answers, incomplete answers (<10 chars), low-confidence answers → regenerated using same prompt as original generation (max 2 retries); skips questions deleted as duplicates; **automatically sets `sources_referenced = ["General LLM reasoning"]` for low confidence answers if AI returns empty sources**
- **Answer Reformatting**: Malformed JSON, repeated content, structural issues → AI reformats while preserving all original content; skips questions deleted as duplicates

**IMPORTANT:**
- Remediation tasks run **sequentially** (not parallel) to prevent race conditions. Answer remediation must wait for duplicate removal to complete so it can skip deleted questions.
- Low confidence answers are automatically enforced to have `["General LLM reasoning"]` as sources if the AI returns empty or invalid sources during regeneration.

**Configuration:**
- Auto-remediation: `ltai.v1.rfp.AUTO_REMEDIATION_ENABLED` (default: `true`)
- AI completeness checks: `ltai.v1.rfp.AI_COMPLETENESS_CHECK_ENABLED` (default: `true`)
- AI formatting checks: `ltai.v1.rfp.AI_FORMATTING_CHECK_ENABLED` (default: `true`)
- Regeneration model: `rfp/autogeneration_answering:0.3af`
- Reformatting/Quality audit model: `rfp/autogeneration_extraction:0.3af`

**Quality Thresholds:**
- Min answer length: 10 characters
- Max answer length: 50,000 characters
- Duplicate similarity: 85% (SequenceMatcher)
- Low confidence alert: >30% of answers
- Min length for AI checks: 50 characters

**AI-Enhanced Quality Checks:**
Quality checks use a **two-stage approach** combining fast pattern matching with selective AI validation:

1. **Completeness Check:**
   - Stage 1 (pattern): Empty, too short, error keywords
   - Stage 2 (AI): Semantic validation for answers ≥50 chars that pass Stage 1
     - Detects: off-topic answers, placeholder text, incomplete thoughts, circular answers
     - Example: "We will provide this later" → flagged as placeholder_text

2. **Formatting Check:**
   - Stage 1 (pattern): Malformed JSON, repeated text, excessive length
   - Stage 2 (AI): Structural validation for long unstructured text (>500 chars, no paragraph breaks)
     - Detects: wall of text, missing lists/tables, broken markdown
     - Example: 800-char single paragraph listing funds → flagged as missing_tables

3. **Source Validation (pattern-based):**
   - Valid: Actual references OR `["General LLM reasoning"]` for low confidence ONLY
   - Invalid: Empty list, placeholders ("None", "N/A", "null", "-", "TBD")
   - Example: High confidence with `["None"]` → flagged as placeholder_source_value

**AI Check Performance:**
- Completeness: ~40-45 calls per 53-question project (~80% of questions)
- Formatting: ~2-5 calls per project (~5-10% of questions)
- Total added time: ~1-2 minutes
- Can be disabled independently via Airflow Variables

**API Updates:**
- Saves `quality_audit_run_id`, `quality_audit_status` (passed/failed), and `quality_audit_summary` JSON to project
- **Does NOT modify the main project `status` field** (remains in `review` state)
- Remediation stats included: `removed_duplicates`, `regenerated_answers`, `reformatted_answers`
- Uses `DELETE /rfp/questions/{id}` to remove duplicates
- Uses `PATCH /rfp/questions/{id}` to update regenerated/reformatted answers

### Regeneration: `rfp_regeneration_dag.py` (dag_id: `rfp_regeneration_workflow`)

Re-generates answers for specific questions with quality scoring (threshold: 8/10, max 3 attempts).

## Document Type → DAG Mapping

| Code | DAG ID |
|------|--------|
| `RFP_PUBLIC_PENSION` | `rfp_public_pension_processing_dag` |
| `RFP_CORP_PENSION` | `rfp_corporate_pension_processing_dag` |
| `RFP_ENDOWMENT` | `rfp_endowment_foundation_processing_dag` |
| `RFP_INSURANCE` | `rfp_insurance_company_processing_dag` |
| `RFP_OCIO` | `rfp_ocio_processing_dag` |
| `RFP_SUBADVISORY` | `rfp_subadvisory_processing_dag` |
| `RFP_WRAP_SMA` | `rfp_wrap_sma_processing_dag` |
| `RFP_BD_PLATFORM` | `rfp_broker_dealer_platform_processing_dag` |
| `RFP_GATEKEEPER` | `rfp_gatekeeper_review_processing_dag` |
| `RFP_CONSULTANT` | `rfp_consultant_strategy_processing_dag` |

## Key Configuration (Airflow Variables)

- `ltai.v1.rfp.OLLAMA_HOST` — Ollama endpoint (default: `http://agentomatic:8000`)
- `ltai.v1.rfp.RFP_API_BASE` — Backend API (default: `http://agentconnector:8000`)
- `ltai.v1.rfp.REGENERATION_AGENT` — Model for regeneration (default: `rfp/regeneration:0.3af`)

## LLM Models

- `rfp/autogeneration_extraction:0.3af` — Document classification and question extraction
- `rfp/autogeneration_answering:0.3af` — Answer generation with RAG
- `rfp/regeneration:0.3af` — Answer regeneration with quality scoring

## API Endpoints Used

- `GET /rfp/projects/{id}` — Fetch project details including `context_and_instructions` (used by processing DAGs and quality audit)
- `GET /rfp/projects/{id}/rfpfile` — Download project PDF
- `GET /rfp/projects/{id}/questions` — Fetch all questions for a project (used by quality audit)
- `PATCH /rfp/projects/{id}` — Update project status, doc type, run IDs, answer count, quality audit results
- `POST /rfp/projects/{id}/questions` — Create extracted questions
- `PATCH /rfp/questions/{id}` — Save generated answers

All API calls require `WORKSPACE_UUID` and `x-ltai-user-email` headers.

**Quality Audit Fields (saved to project):**
- `quality_audit_run_id` — Airflow run ID of the audit DAG
- `quality_audit_status` — Overall result: "passed" or "failed"
- `quality_audit_summary` — JSON object containing detailed metrics (issue counts, severity breakdown, confidence distribution)

## Project Status Flow

**Main Pipeline:**
`analyzing` → `extracting` → `generating` → `review` (or `failed` on error)

**Background Quality Audit:**
Quality audit runs in parallel after processing completes. Results stored in `quality_audit_status` field (passed/failed) and detailed `quality_audit_summary` JSON. Project remains in `review` status regardless of audit outcome.

## Common Patterns

- All processing DAGs use `rfp_processing_base.py` via factory pattern — changes go in the base module only
- `dag_run.conf` always carries `project_id`, `workspace_uuid`, `x-ltai-user-email`
- Text chunking uses 12,000 char chunks with 1,500 char overlap
- Chunk-to-chunk deduplication: each chunk's extraction prompt receives the previous chunk's question keys so the AI skips already-extracted questions in overlap regions
- Key collision handling: if two chunks produce the same question key, the later one is suffixed (`_1`, `_2`, …) instead of overwriting
- Question extraction retries 3 times per chunk; answer generation retries 3 times per question
- `handle_task_failure` callback sets project status to `failed` on any task error

## Project Context and Instructions

The processing pipeline supports project-specific context and instructions via the `context_and_instructions` field in the project record.

**How It Works:**

1. **Fetch Stage** (`fetch_pdf_from_api` task):
   - Retrieves project details via `GET /rfp/projects/{id}` before downloading the PDF
   - Extracts the `context_and_instructions` field and stores it in XCom
   - If the field is empty or the fetch fails, the pipeline continues with empty context (graceful degradation)

2. **Answer Generation** (`generate_answers_with_ai` task):
   - Pulls the `context_and_instructions` from XCom
   - Injects it at the top of the answer prompt in a dedicated "PROJECT CONTEXT AND INSTRUCTIONS" section
   - The AI model sees this context before processing each question

**Use Cases:**
- Provide firm-specific information (fund names, strategies, AUM, key personnel)
- Set tone/style preferences (formal vs. conversational, technical depth)
- Define answer constraints (max length, required structure, terminology to use/avoid)
- Supply boilerplate text for common questions (e.g., firm history, regulatory status)

**Example Context:**
```
Our firm is XYZ Capital Management with $5B AUM. We focus on large-cap value strategies.
When answering questions about performance, always reference our flagship Large Cap Value Fund.
Keep responses professional and concise, avoiding jargon when possible.
```

**Technical Details:**
- Context is passed to `ANSWER_PROMPT_TEMPLATE` as the `{project_context}` variable
- If no context provided, prompt shows: "No additional project-specific context provided."
- Context is fetched once per DAG run and reused for all questions (efficient design)

## Extraction Logic

### Design Principles

The extraction prompts (`EXTRACTION_PROMPT_TEMPLATE` and `VALIDATION_PROMPT_TEMPLATE`) are **industry-agnostic** and work across all RFP types (software, construction, professional services, equipment, etc.).

**Core Approach:**
- Universal question patterns (narrative requests, form fields, tables, declarations, checkboxes, conditionals)
- Explicit exclusion rules to prevent false positives
- Context-aware handling of scope-of-work vs. vendor questions

### What Gets Excluded

**Not Extracted:**
- RFP process instructions (submission deadlines, how to submit, pre-bid meetings)
- Procurement boilerplate (agency rights, standard terms, legal disclaimers)
- Scope-of-work descriptions (work to be done after award, unless asking for vendor's approach)
- Client responsibilities (what the agency will provide)

**Extracted:**
- Vendor information requests (firm details, qualifications, certifications)
- Approach/methodology questions (describe your approach to...)
- Forms, tables, and data entry requirements
- Document submissions and confirmations

### Known Limitations

- **Duplicate detection**: Questions spanning chunk boundaries may be extracted twice (once partial, once complete). Key-based deduplication prevents duplicate keys but not duplicate content.
- **Scope ambiguity**: Statements like "Develop X" may be scope descriptions or capability questions depending on context. Prompt includes decision framework but edge cases remain.
