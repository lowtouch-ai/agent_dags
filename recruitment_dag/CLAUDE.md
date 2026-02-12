# Recruitment DAG - AI-Powered CV Processing Pipeline

## Overview

This project is an automated recruitment pipeline built on **Apache Airflow**. It monitors a Gmail mailbox for incoming candidate emails, classifies them using AI, analyzes CVs against job descriptions, and sends automated responses — all without manual intervention.

## Architecture

The pipeline consists of three interconnected DAGs:

### 1. `cv_listner.py` — Mailbox Monitor (DAG ID: `cv_monitor_mailbox`)
- **Schedule**: Runs every 2 hours
- **Purpose**: Monitors the recruitment Gmail inbox for new unread emails
- **Workflow**:
  1. Fetches unread emails with attachments via Gmail API
  2. Extracts full thread history with PDF content from each conversation
  3. Extracts candidate email addresses from CV PDFs using AI
  4. Looks up candidates in Google Sheets to check existing status
  5. Classifies emails using AI into: `NEW_CV_APPLICATION`, `SCREENING_RESPONSE`, or `OTHER`
  6. Routes emails to the appropriate downstream DAG via `TriggerDagRunOperator`

### 2. `cv_analyse.py` — CV Analysis (DAG ID: `cv_analyse`)
- **Schedule**: None (triggered by `cv_monitor_mailbox`)
- **Purpose**: Analyzes new CV submissions and scores candidates
- **Workflow**:
  1. Extracts CV content from PDF attachments (with thread history fallback)
  2. Retrieves matching job descriptions from a vector database via AI (`VectorSearchByUUID` tool)
  3. Identifies the best matching job for the candidate
  4. Scores the CV against the JD (must-have skills, nice-to-have skills, experience, education)
  5. Saves candidate profile and scores to Google Sheets and local JSON (`/appz/data/recruitment/`)
  6. Sends response email: rejection or screening questions for eligible candidates. The email opening is context-aware — if the candidate applied for a specific role, it acknowledges that role; if the candidate sent a general inquiry (no specific role mentioned), it acknowledges their interest in the company and introduces the best matching role based on their profile. When a recruiter forwards a CV (detected via `extracted_candidate_email` from the listener), the screening questions are sent directly to the candidate with the recruiter CC'd; otherwise the email goes to the original sender.

### 3. `cv_initial_screening.py` — Screening Response Analysis (DAG ID: `screening_response_analysis`)
- **Schedule**: None (triggered by `cv_monitor_mailbox`)
- **Purpose**: Evaluates candidate responses to initial screening questions
- **Workflow**:
  1. Extracts candidate responses from the full email body (sourced from `thread_history.current_message.content`, with fallback to email snippet)
  2. Loads the candidate's saved profile from `/appz/data/recruitment/<email>.json`
  3. Analyzes responses using AI against 7 criteria (work arrangement, availability, salary, location, motivation, technical fit, qualifications)
  4. Updates candidate profile with screening results
  5. Sends acceptance (interview invite) or rejection email to the candidate
  6. If accepted, notifies the recruiter (Athira) via email to schedule an interview call with the candidate — includes candidate name, email, position (`job_title`), CV score (`total_score`), screening score (`overall_score`), and key credentials (experience, education, matched must-have and nice-to-have skills) from the saved profile (with configurable CC recipients). The notification also includes an AI-generated concise candidate summary (overall fit assessment), screening insights (strengths, concerns, detailed assessment from the analysis), and 5-6 suggested interview questions tailored to the role and candidate profile with expected answer patterns / what to look for in each answer.

## Data Flow

```
Gmail Inbox
    |
    v
cv_monitor_mailbox (listener)
    |
    +--> NEW_CV_APPLICATION --> cv_analyse --> Google Sheets + Response Email
    |
    +--> SCREENING_RESPONSE --> screening_response_analysis --> Updated Profile + Result Email + Recruiter Notification (if accepted)
    |
    +--> OTHER --> Skipped
```

## Key Dependencies

- **Airflow Variables** (all prefixed `ltai.v3.lowtouch.recruitment.*`):
  - `email_credentials` — Gmail API credentials
  - `from_address` — Recruitment sender email
  - `sheets_credentials` — Google Sheets API credentials
  - `sheets_id` — Google Sheets spreadsheet ID
  - `sheets_auth_type` — Auth type for Sheets (default: `oauth`)
  - `model_name` — AI model name (default: `recruitment:0.3af`)
  - `recruiter_email` — Recruiter email for interview scheduling notifications (default: `athira@lowtouch.ai`)
  - `recruiter_cc_emails` — Comma-separated CC recipients for recruiter notification emails (optional)

- **Shared Utilities** (from `agent_dags/utils/`):
  - `email_utils` — Gmail authentication, email fetching, sending, attachment processing
  - `agent_utils` — AI model calls (`get_ai_response`), JSON extraction, PDF content extraction
  - `sheets_utils` — Google Sheets CRUD operations for candidate tracking

- **External Services**: Gmail API, Google Sheets API, AI model endpoint (via `get_ai_response`), Vector database (via `VectorSearchByUUID`)

## Storage

- **Google Sheets**: Primary candidate tracking (name, email, job, scores, status)
- **Local JSON files**: `/appz/data/recruitment/<email>.json` — detailed candidate profiles with CV content and screening results
- **Attachments cache**: `/appz/data/cv_attachments/`
- **Last processed timestamp**: `/appz/cache/cv_last_processed_email.json`

## Scoring System

CV scoring uses a weighted formula:
- **Must-have skills**: 60% weight (100 if matched, 0 if missing)
- **Nice-to-have skills**: 30% weight (100 if matched, 0 if missing)
- **Other criteria** (experience + education): 10% weight
- **Eligibility threshold**: Candidates scoring below 50% total are automatically marked ineligible and receive a rejection email

## Shared Utility Notes

- **`extract_json_from_text`** (`agent_utils.py`): Uses balanced-brace parsing (not regex) to handle arbitrarily nested JSON structures. Returns the largest valid JSON object found in text.

## Testing

- **Unit tests**: `test_cv_initial_screening.py` — 94 edge-case tests for all 6 task functions in `cv_initial_screening.py`. Uses `unittest.mock` to mock Airflow, Gmail API, AI model, and filesystem. Run with: `python3 -m unittest agent_dags.recruitment_dag.test_cv_initial_screening -v` from the `airflow/dags` directory.

## Bug Fixes Applied (cv_initial_screening.py)

1. **Null check uses `is not None`** (`update_candidate_profile`): Changed `not all([...])` truthiness check to explicit `is None` checks so that an empty dict `{}` from the AI is no longer incorrectly rejected.
2. **Decision `None` defaults to `'PENDING'`** (`update_candidate_profile`): Changed `.get('decision', 'PENDING')` to `.get('decision') or 'PENDING'` so that an explicit `None` value correctly defaults.
3. **Case-insensitive decision comparisons** (`send_screening_result_email`, `notify_recruiter_for_interview`): Decision strings are now normalized with `.upper()` so `'reject'`/`'accept'` (lowercase from AI) are handled correctly.
4. **No double `Re:` prefix** (`send_screening_result_email`): Removed the manual `subject = f"Re: {subject}"` line — the `send_email()` utility already adds `Re:` if missing, so the DAG no longer duplicates it.
5. **Safe chained `.get()` on nullable dicts** (`notify_recruiter_for_interview`): Changed `.get('experience_match', {}).get(...)` to `(.get('experience_match') or {}).get(...)` so that an explicit `None` value for `experience_match` or `education_match` no longer crashes with `AttributeError`.
