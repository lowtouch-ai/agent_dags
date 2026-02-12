# Test Results: cv_initial_screening.py (screening_response_analysis DAG)

**Date:** 2026-02-12
**File Under Test:** `recruitment_dag/cv_initial_screening.py`
**Test File:** `recruitment_dag/test_cv_initial_screening.py`
**Test Framework:** Python `unittest` with `unittest.mock`
**Result:** **94/94 PASSED**

---

## Test Scope & Validity

### What These Tests ARE (Unit Tests)

These are **unit tests** that test each of the 6 Airflow task functions **in isolation**. All external dependencies (Gmail API, AI model, filesystem, Airflow Variables) are **mocked** using `unittest.mock`. This means:

- Each function's **business logic** is tested directly with controlled inputs
- **Edge cases** (null inputs, malformed data, AI failures, filesystem errors) are systematically covered
- Tests are **fast** (~0.14 seconds for all 94), **repeatable**, and **deterministic**
- No external services, credentials, or running Airflow instance required

### What These Tests ARE NOT (Integration/E2E Tests)

These tests do **NOT** validate:

| Not Tested | Why It Matters |
|---|---|
| Listener DAG (`cv_listner.py`) triggering this DAG | The real pipeline starts when the listener classifies an email as `SCREENING_RESPONSE` and triggers `screening_response_analysis` via `TriggerDagRunOperator`. This trigger mechanism is not tested here. |
| Real Airflow XCom data flow between tasks | We simulate XCom with mock objects. In production, Airflow serializes/deserializes XCom data, which could introduce issues (e.g., large payloads, serialization failures). |
| Airflow task dependency execution order | The DAG chain `extract -> load -> analyze -> update -> send_email -> notify_recruiter` is defined but not tested as a running DAG. |
| Real Gmail API calls | Email authentication and sending are mocked. Real API errors (rate limits, auth expiry, network issues) are not tested. |
| Real AI model responses | `get_ai_response` is mocked. The actual AI model's response format/quality is not validated. |
| File I/O on production server | Reading/writing candidate profiles to `/appz/data/recruitment/` is mocked. Real filesystem permissions/disk issues are not tested. |

### How to Get Full Coverage

For a complete test strategy, the team should complement these unit tests with:

1. **Integration Tests** (recommended next step): Deploy to staging, trigger the listener with a test email, and verify the full pipeline executes end-to-end.
2. **Contract Tests**: Validate that the AI model consistently returns the expected JSON schema.
3. **Smoke Tests on Airflow**: Use `airflow dags test screening_response_analysis <date>` with a sample `conf` payload on the staging environment.

---

## Test Results Summary

| Test Class | Tests | Pass | Fail | Focus Area |
|---|---|---|---|---|
| `TestExtractCandidateResponse` | 15 | 15 | 0 | Email data extraction, body source priority, null handling |
| `TestLoadCandidateProfile` | 13 | 13 | 0 | Email sanitization, file system errors, JSON parsing |
| `TestAnalyzeScreeningResponses` | 12 | 12 | 0 | AI response handling, CV content fallback, null inputs |
| `TestUpdateCandidateProfile` | 14 | 14 | 0 | Profile merge, default values, file write errors |
| `TestSendScreeningResultEmail` | 16 | 16 | 0 | Decision routing, email construction, Gmail failures |
| `TestNotifyRecruiterForInterview` | 20 | 20 | 0 | Decision gating, profile defaults, interview prep AI |
| `TestCrossCutting` | 4 | 4 | 0 | Pipeline flow, sanitization consistency |
| **Total** | **94** | **94** | **0** | |

---

## Detailed Test Cases

### 1. extract_candidate_response (15 tests)

| Test | Edge Case | Expected | Result |
|---|---|---|---|
| `test_no_email_data_key_in_conf` | `conf` has no `email_data` key | Returns `None`, no xcom_push | PASS |
| `test_email_data_is_empty_dict` | `email_data = {}` (falsy) | Returns `None` | PASS |
| `test_email_data_is_none` | `email_data = None` | Returns `None` | PASS |
| `test_missing_headers_key` | No `headers` key in email_data | Defaults: sender=`'Unknown'`, subject=`'No Subject'` | PASS |
| `test_empty_from_header` | `From: ''` | `sender_email` is `''` | PASS |
| `test_from_header_with_display_name` | `From: 'John Doe <john@example.com>'` | Extracts `john@example.com` | PASS |
| `test_from_header_unicode_display_name` | Unicode chars in display name | `parseaddr` handles it correctly | PASS |
| `test_body_from_thread_history` | Body in `thread_history.current_message.content` | Uses thread_history body (preferred) | PASS |
| `test_body_fallback_to_content` | Thread history body is `''` | Falls back to `email_data['content']` | PASS |
| `test_body_both_sources_empty` | Both body sources are `''` | `body` is `''` | PASS |
| `test_thread_history_is_none` | `thread_history = None` | No crash, falls back | PASS |
| `test_current_message_is_none` | `current_message = None` | No crash, falls back | PASS |
| `test_missing_date_defaults` | No `date` key | Defaults to valid ISO datetime | PASS |
| `test_missing_id_defaults_to_unknown` | No `id` key | `email_id = 'unknown'` | PASS |
| `test_xcom_push_called_correctly` | Valid data | `xcom_push(key='response_data', value=...)` called | PASS |

### 2. load_candidate_profile (13 tests)

| Test | Edge Case | Expected | Result |
|---|---|---|---|
| `test_no_response_data` | `xcom_pull` returns `None` | Returns `None` | PASS |
| `test_email_sanitization_at_sign` | `user@example.com` | `@` replaced with `_at_` | PASS |
| `test_email_sanitization_forward_slash` | `user/admin@example.com` | `/` replaced with `_` | PASS |
| `test_email_sanitization_backslash` | `user\admin@example.com` | `\` replaced with `_` | PASS |
| `test_path_traversal_attempt` | `../../etc/passwd@evil.com` | `/` sanitized, `..` remains; file not found returns `None` | PASS |
| `test_plus_addressing` | `user+tag@example.com` | `+` not sanitized (as expected) | PASS |
| `test_profile_file_not_found` | Profile file doesn't exist | Returns `None` | PASS |
| `test_profile_file_invalid_json` | File contains `'not json at all'` | Caught by except, returns `None` | PASS |
| `test_profile_file_empty` | Empty file | JSON parse error caught, returns `None` | PASS |
| `test_profile_file_permission_error` | `PermissionError` on open | Caught, returns `None` | PASS |
| `test_profile_is_json_array` | File contains `[1, 2, 3]` | Loaded as-is (no type validation) | PASS |
| `test_xcom_push_on_success` | Successful load | `xcom_push` called with profile | PASS |
| `test_xcom_push_not_called_on_file_not_found` | File not found | `xcom_push` NOT called | PASS |

### 3. analyze_screening_responses (12 tests)

| Test | Edge Case | Expected | Result |
|---|---|---|---|
| `test_no_response_data` | `response_data = None` | Returns `None` | PASS |
| `test_no_candidate_profile` | `candidate_profile = None` | Returns `None` | PASS |
| `test_both_inputs_none` | Both inputs `None` | Returns `None` | PASS |
| `test_cv_content_from_thread_history` | CV in `thread_history.original_cv_content` | Uses thread_history CV (preferred) | PASS |
| `test_cv_content_fallback_to_profile` | CV only in `candidate_profile` | Falls back to profile CV | PASS |
| `test_cv_content_neither_source` | No CV content anywhere | Proceeds with empty string, logs warning | PASS |
| `test_ai_returns_valid_json` | AI returns parseable JSON | Parsed and pushed to xcom | PASS |
| `test_ai_returns_garbage` | AI returns `"I don't understand"` | `extract_json` returns `None` | PASS |
| `test_ai_returns_empty_string` | AI returns `""` | Returns `None` | PASS |
| `test_ai_returns_error_string` | AI returns error message | Returns `None` | PASS |
| `test_ai_returns_json_with_extra_keys` | JSON has unexpected extra keys | Passes through (no schema validation) | PASS |
| `test_email_data_missing_from_conf` | No `email_data` in conf | Defaults to `{}`, no crash | PASS |

### 4. update_candidate_profile (14 tests)

| Test | Edge Case | Expected | Result |
|---|---|---|---|
| `test_missing_response_data` | `response_data = None` | Returns `None` | PASS |
| `test_missing_analysis_data` | `analysis_data = None` | Returns `None` | PASS |
| `test_missing_candidate_profile` | `candidate_profile = None` | Returns `None` | PASS |
| `test_analysis_data_empty_dict_accepted` | `analysis_data = {}` | Accepted (Bug #1 fixed: uses `is None` check) | PASS |
| `test_missing_decision_key_defaults_to_pending` | No `decision` key | Defaults to `'PENDING'` | PASS |
| `test_missing_overall_score_defaults_to_zero` | No `overall_score` key | Defaults to `0` | PASS |
| `test_decision_value_none_defaults_to_pending` | `decision: None` (key present) | Defaults to `'PENDING'` (Bug #2 fixed: uses `or 'PENDING'`) | PASS |
| `test_file_write_permission_error` | `PermissionError` on write | Caught, returns `None` | PASS |
| `test_profile_merge_preserves_existing_keys` | Profile has extra keys | All existing keys preserved | PASS |
| `test_profile_merge_overwrites_screening_stage` | Profile already has `screening_stage` | Overwritten with new data | PASS |
| `test_completion_date_is_valid_iso` | Normal execution | `completion_date` is valid ISO 8601 | PASS |
| `test_xcom_push_on_success` | Successful write | `xcom_push` called | PASS |
| `test_xcom_push_not_called_on_write_failure` | Write fails | `xcom_push` NOT called | PASS |

### 5. send_screening_result_email (16 tests)

| Test | Edge Case | Expected | Result |
|---|---|---|---|
| `test_missing_email_data` | `email_data = {}` (falsy) | Returns `"Missing data for email"` | PASS |
| `test_missing_analysis_data` | `analysis_data = None` | Returns `"Missing data for email"` | PASS |
| `test_missing_response_data` | `response_data = None` | Returns `"Missing data for email"` | PASS |
| `test_accept_decision_uses_acceptance_prompt` | `decision = 'ACCEPT'` | Acceptance email prompt sent to AI | PASS |
| `test_reject_decision_uses_rejection_prompt` | `decision = 'REJECT'` | Rejection email prompt sent to AI | PASS |
| `test_pending_decision_falls_to_acceptance` | `decision = 'PENDING'` | Falls to else branch (acceptance) | PASS |
| `test_lowercase_reject_now_matched` | `decision = 'reject'` (lowercase) | Normalized to `'REJECT'` (Bug #3 fixed: `.upper()`) | PASS |
| `test_subject_passed_as_is` | Normal subject | Passed as-is to `send_email` (Bug #4 fixed: no manual `Re:`) | PASS |
| `test_subject_already_has_re_no_double` | Subject already has `'Re:'` | No double `'Re: Re:'` (Bug #4 fixed) | PASS |
| `test_ai_response_html_fenced` | AI response in ` ```html ``` ` | Regex extracts inner HTML | PASS |
| `test_ai_response_no_fence` | AI response without fences | Falls back to `response.strip()` | PASS |
| `test_gmail_auth_failure` | `authenticate_gmail` returns `None` | Returns error, `send_email` NOT called | PASS |
| `test_send_email_failure` | `send_email` returns `None` | Returns `"Failed to send email"` | PASS |
| `test_send_email_success` | `send_email` returns truthy | Returns success message | PASS |
| `test_thread_id_passed_to_send_email` | `threadId` present | Passed through to `send_email` | PASS |
| `test_references_deduplication` | Message-ID already in References | Not duplicated | PASS |

### 6. notify_recruiter_for_interview (20 tests)

| Test | Edge Case | Expected | Result |
|---|---|---|---|
| `test_missing_analysis_data` | `analysis_data = None` | Returns `"Missing data - skipped"` | PASS |
| `test_missing_response_data` | `response_data = None` | Returns `"Missing data - skipped"` | PASS |
| `test_decision_reject_skips` | `decision = 'REJECT'` | Skips notification | PASS |
| `test_decision_pending_skips` | `decision = 'PENDING'` | Skips notification | PASS |
| `test_decision_missing_defaults_pending_skips` | No `decision` key | Defaults to `'PENDING'`, skips | PASS |
| `test_decision_lowercase_accept_now_proceeds` | `decision = 'accept'` (lowercase) | Normalized to `'ACCEPT'`, proceeds (Bug #3 fixed: `.upper()`) | PASS |
| `test_decision_accept_proceeds` | `decision = 'ACCEPT'` | Email sent to recruiter | PASS |
| `test_candidate_profile_is_none` | `candidate_profile = None` | Defaults: `'Unknown Candidate'`, `'N/A'` | PASS |
| `test_profile_missing_all_keys` | Profile is `{}` (no expected keys) | All fields use defaults | PASS |
| `test_empty_skills_arrays` | Skills arrays are `[]` | Skills string is `'N/A'` | PASS |
| `test_skills_with_no_matches` | All skills have `match: False` | Matched list empty, shows `'N/A'` | PASS |
| `test_skill_missing_skill_name_key` | Skill dict is `{'match': True}` (no name) | Empty string in join, no crash | PASS |
| `test_experience_match_value_is_none_handled` | `experience_match: None` | No crash (Bug #5 fixed: `or {}` pattern) | PASS |
| `test_education_match_value_is_none_handled` | `education_match: None` | No crash (Bug #5 fixed: `or {}` pattern) | PASS |
| `test_missing_strengths_concerns` | No strengths/concerns keys | Defaults to `'N/A'` / `'None identified'` | PASS |
| `test_ai_interview_prep_returns_garbage` | AI returns non-JSON | Summary defaults to `'N/A'`, questions to `[]` | PASS |
| `test_interview_questions_in_html` | Valid interview questions | Questions appear in HTML body | PASS |
| `test_gmail_auth_failure` | Gmail auth returns `None` | Returns error, no email sent | PASS |
| `test_send_email_failure` | `send_email` returns `None` | Returns failure message | PASS |
| `test_subject_contains_name_and_position` | Valid profile | Subject includes name and job title | PASS |

### 7. Cross-Cutting (4 tests)

| Test | Edge Case | Expected | Result |
|---|---|---|---|
| `test_email_sanitization_consistency` | Same email across functions | Identical sanitized filename | PASS |
| `test_full_pipeline_happy_path` | All functions with valid data | Data flows correctly through pipeline | PASS |
| `test_pipeline_early_none_from_extract` | First function returns `None` | All downstream functions return `None` gracefully | PASS |

---

## Bugs Discovered & Fixed

All 5 bugs discovered during testing have been **fixed** in the source code.

### Bug #1: Empty dict `{}` treated as missing data — **FIXED** (LOW)

**Location:** `update_candidate_profile`
**Issue:** `analysis_data = {}` is falsy in Python, so `all([response_data, {}, candidate_profile])` returned `False`.
**Fix Applied:** Changed `not all([...])` truthiness check to explicit `is None` checks so that an empty dict `{}` from the AI is no longer incorrectly rejected.

### Bug #2: `.get('decision', 'PENDING')` doesn't default when value is `None` — **FIXED** (LOW)

**Location:** `update_candidate_profile`
**Issue:** When `analysis_data = {'decision': None}`, `.get('decision', 'PENDING')` returned `None` (key exists), not `'PENDING'`.
**Fix Applied:** Changed `.get('decision', 'PENDING')` to `.get('decision') or 'PENDING'`.

### Bug #3: Case-sensitive decision comparisons — **FIXED** (MEDIUM)

**Location:** `send_screening_result_email`, `notify_recruiter_for_interview`
**Issue:** `decision == 'REJECT'` and `decision != 'ACCEPT'` were case-sensitive. Lowercase from AI caused wrong code paths.
**Fix Applied:** Decision strings are now normalized with `.upper()` before comparison.

### Bug #4: Double "Re:" prefix on reply subjects — **FIXED** (LOW)

**Location:** `send_screening_result_email`
**Issue:** `subject = f"Re: {subject}"` was always applied. The `send_email` utility already adds `Re:`, resulting in double prefix.
**Fix Applied:** Removed the manual `Re:` prefix — `send_email()` handles it.

### Bug #5: `experience_match: None` / `education_match: None` crashes — **FIXED** (HIGH)

**Location:** `notify_recruiter_for_interview`
**Issue:** `.get('experience_match', {}).get(...)` crashed when the value was explicitly `None` (key existed but value was `null`).
**Fix Applied:** Changed to `(candidate_profile.get('experience_match') or {}).get(...)` pattern.

---

## How to Run

```bash
# From the airflow/dags directory:
cd /path/to/airflow/dags

# Run with unittest (no extra dependencies needed):
python3 -m unittest agent_dags.recruitment_dag.test_cv_initial_screening -v

# Or with pytest (if installed):
python3 -m pytest agent_dags/recruitment_dag/test_cv_initial_screening.py -v
```

**Runtime:** ~0.14 seconds for all 94 tests.

---

## Recommended Next Steps for Full Validation

1. **Integration test on staging:** Trigger the listener DAG with a real test email to verify the full `cv_listner -> screening_response_analysis` pipeline
2. **Airflow DAG test:** Run `airflow dags test screening_response_analysis 2024-01-01` with a sample `conf` payload on staging
3. **AI contract test:** Validate that the AI model consistently returns the expected JSON schema for screening analysis and interview prep prompts
