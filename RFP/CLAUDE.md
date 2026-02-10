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

### Processing DAGs (10 variants)

Each `rfp_*_processing_dag.py` follows an identical 5-task pipeline:

1. `fetch_pdf_from_api` — Downloads PDF, converts to markdown via `pymupdf4llm`
2. `extract_questions_with_ai` — Chunk-Map-Reduce extraction using `MODEL_FOR_EXTRACTION`
3. `validate_and_fix_questions` — Detects numbering gaps, recovers missing questions
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

- All processing DAGs share identical code structure — changes to one typically apply to all 10
- `dag_run.conf` always carries `project_id`, `workspace_uuid`, `x-ltai-user-email`
- Text chunking uses 12,000 char chunks with 1,500 char overlap
- Question extraction retries 3 times per chunk; answer generation retries 3 times per question
- `handle_task_failure` callback sets project status to `failed` on any task error
