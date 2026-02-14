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

Contains all shared logic via a `create_rfp_processing_dag(dag_id, description, tags)` factory function. Includes helpers, prompt templates, and the 5-task pipeline definition.

### Processing DAGs (10 variants)

Each `rfp_*_processing_dag.py` is a thin wrapper (~9 lines) that calls `create_rfp_processing_dag()` with its unique `dag_id`, `description`, and `tags`. The factory creates an identical 5-task pipeline:

1. `fetch_pdf_from_api` — Downloads PDF, converts to markdown via `pymupdf4llm`
2. `extract_questions_with_ai` — Chunk-Map-Reduce extraction using `MODEL_FOR_EXTRACTION`
3. `validate_and_fix_questions` — Chunk-level validation: sends each text chunk + known keys to AI to recover missed questions
4. `generate_answers_with_ai` — Generates answers using `MODEL_FOR_ANSWERING` with RAG tool calls
5. `log_completion` — Updates project status to `review`, saves `processing_dag_run_id`

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

- `GET /rfp/projects/{id}/rfpfile` — Download project PDF
- `PATCH /rfp/projects/{id}` — Update project status, doc type, run IDs, answer count
- `POST /rfp/projects/{id}/questions` — Create extracted questions
- `PATCH /rfp/questions/{id}` — Save generated answers

All API calls require `WORKSPACE_UUID` and `x-ltai-user-email` headers.

## Project Status Flow

`analyzing` → `extracting` → `generating` → `review` (or `failed` on error)

## Common Patterns

- All processing DAGs use `rfp_processing_base.py` via factory pattern — changes go in the base module only
- `dag_run.conf` always carries `project_id`, `workspace_uuid`, `x-ltai-user-email`
- Text chunking uses 12,000 char chunks with 1,500 char overlap
- Chunk-to-chunk deduplication: each chunk's extraction prompt receives the previous chunk's question keys so the AI skips already-extracted questions in overlap regions
- Key collision handling: if two chunks produce the same question key, the later one is suffixed (`_1`, `_2`, …) instead of overwriting
- Question extraction retries 3 times per chunk; answer generation retries 3 times per question
- `handle_task_failure` callback sets project status to `failed` on any task error

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
