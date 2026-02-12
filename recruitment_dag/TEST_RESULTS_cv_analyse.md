# Test Results: cv_analyse.py (cv_analyse DAG)

**Date:** 2026-02-12
**File Under Test:** `recruitment_dag/cv_analyse.py`
**Test File:** `recruitment_dag/test_cv_analyse.py`
**Test Framework:** Python `unittest` with `unittest.mock`
**Result:** **52/52 PASSED**

---

## Test Scope & Validity

### What These Tests ARE (Unit Tests)

These are **unit tests** that test each of the 7 Airflow task/helper functions **in isolation**. All external dependencies (Gmail API, Google Sheets API, AI model, vector database, filesystem, Airflow Variables) are **mocked** using `unittest.mock` and `sys.modules` injection. This means:

- Each function's **business logic** is tested directly with controlled inputs
- **Edge cases** (null inputs, malformed AI responses, filesystem errors, null skill lists) are systematically covered
- Tests are **fast** (~0.05 seconds for all 52), **repeatable**, and **deterministic**
- No external services, credentials, or running Airflow instance required

### What These Tests ARE NOT (Integration/E2E Tests)

These tests do **NOT** validate:

| Not Tested | Why It Matters |
|---|---|
| Trigger from `cv_monitor_mailbox` listener | This DAG is triggered via `TriggerDagRunOperator` with `conf` payload. The trigger mechanism is not tested here. |
| Real AI model scoring accuracy | `get_ai_response` is mocked. The actual AI model's skill matching and scoring quality is not validated. |
| Real vector database JD retrieval | The `VectorSearchByUUID` tool call is mocked. Real vector search relevance is not tested. |
| Real Gmail API email sending | `send_email` and `authenticate_gmail` are mocked. Real API errors not tested. |
| Real Google Sheets operations | `append_candidate_to_sheet`, `find_candidate_in_sheet`, `update_candidate_status` are mocked. |
| Real filesystem at `/appz/data/recruitment/` | File save (`json.dump`) and directory creation (`Path.mkdir`) are mocked. |
| Real Airflow XCom serialization | XCom data flow is simulated with mock objects. |

### How to Get Full Coverage

For a complete test strategy, the team should complement these unit tests with:

1. **Integration Tests** (recommended next step): Deploy to staging, trigger `cv_analyse` with a real CV email payload, and verify the full scoring + email pipeline.
2. **Contract Tests**: Validate that the AI model consistently returns the expected JSON schema for CV scoring (skills, experience, education fields).
3. **Smoke Tests on Airflow**: Use `airflow dags test cv_analyse <date>` with a sample `conf` payload on staging.

---

## Test Results Summary

| Test Class | Tests | Pass | Fail | Focus Area |
|---|---|---|---|---|
| `TestExtractCvContent` | 10 | 10 | 0 | PDF extraction, thread history fallback, image handling |
| `TestRetriveJdFromWeb` | 2 | 2 | 0 | AI JD retrieval, null response handling |
| `TestGetTheJdForCvAnalysis` | 3 | 3 | 0 | Two-stage AI matching, null/missing key regression |
| `TestCalculateCandidateScore` | 16 | 16 | 0 | Weighted scoring, threshold boundary, null skill lists |
| `TestGetTheScoreForCvAnalysis` | 5 | 5 | 0 | AI scoring, file save, null response regression |
| `TestSaveToGoogleSheets` | 7 | 7 | 0 | New/existing candidates, auth failure, null dict regression |
| `TestSendResponseEmail` | 9 | 9 | 0 | Rejection/selection emails, forwarded CV routing, double Re: regression |
| **Total** | **52** | **52** | **0** | |

---

## Detailed Test Cases

### 1. extract_cv_content (10 tests)

| Test | Edge Case | Expected | Result |
|---|---|---|---|
| `test_pdf_in_current_email` | PDF attachment with extracted content | Extracts content, type=`pdf` | PASS |
| `test_image_attachment` | Image attachment with base64 content | Stores as type=`image` with base64 | PASS |
| `test_no_email_data` | `conf` has no `email_data` | Returns `None` | PASS |
| `test_fallback_to_original_cv_content` | No PDF in current, CV in `thread_history.original_cv_content` | Uses thread history fallback | PASS |
| `test_fallback_to_history_messages` | No PDF anywhere, user PDF in history messages | Scans history for user PDFs | PASS |
| `test_no_cv_anywhere` | No CV in email or thread history | Empty attachments list | PASS |
| `test_multiple_attachments` | PDF + image in same email | Both attachments included | PASS |
| `test_empty_pdf_content` | PDF with empty extracted content | Not counted as found, triggers fallback | PASS |
| `test_unsupported_mime_type_skipped` | `.docx` attachment | Skipped (not PDF/image) | PASS |
| `test_history_only_assistant_pdfs_skipped` | Only assistant PDFs in history | Not used for fallback (only user PDFs) | PASS |

### 2. retrive_jd_from_web (2 tests)

| Test | Edge Case | Expected | Result |
|---|---|---|---|
| `test_successful_retrieval` | AI returns valid JD list | Pushed to xcom as `jd_data` | PASS |
| `test_ai_returns_none` | AI returns non-parseable text | `None` pushed to xcom | PASS |

### 3. get_the_jd_for_cv_analysis (3 tests)

| Test | Edge Case | Expected | Result |
|---|---|---|---|
| `test_successful_match_and_enrich` | Two-stage AI match + enrich succeeds | Returns enriched JD with job_title | PASS |
| `test_regression_ai_returns_none_for_match` | AI returns non-JSON for match | Returns `None` **(Bug #1 regression)** | PASS |
| `test_regression_missing_job_title_key` | AI returns dict without `job_title` | Uses default `"Unknown Position"` **(Bug #1 regression)** | PASS |

### 4. calculate_candidate_score (16 tests)

| Test | Edge Case | Expected | Result |
|---|---|---|---|
| `test_all_skills_matched` | All skills + experience + education matched | 100% total, eligible | PASS |
| `test_no_skills_matched` | Nothing matched | 0% total, ineligible with reasons | PASS |
| `test_partial_must_have_match` | 2/3 must-have matched | 67% must-have, 40% total, ineligible | PASS |
| `test_threshold_exactly_50` | Score exactly 50% | Eligible (threshold is >= 50) | PASS |
| `test_threshold_49_ineligible` | Score 48% (just below 50) | Ineligible | PASS |
| `test_empty_skills_lists` | Empty `[]` for both skill lists | Scores default to 0, only 10% from other | PASS |
| `test_regression_none_must_have_skills` | `must_have_skills: null` | No crash, score 0 **(Bug #2 regression)** | PASS |
| `test_regression_none_nice_to_have_skills` | `nice_to_have_skills: null` | No crash, score 0 **(Bug #2 regression)** | PASS |
| `test_regression_none_experience_match` | `experience_match: null` | No crash, treated as not matched **(Bug #2 regression)** | PASS |
| `test_regression_none_education_match` | `education_match: null` | No crash, treated as not matched **(Bug #2 regression)** | PASS |
| `test_only_experience_and_education` | No skills, both exp+edu matched | 10% total (only other criteria), ineligible | PASS |
| `test_single_must_have_skill_matched` | One must-have matched | 100% must-have, 60% total, eligible | PASS |
| `test_all_criteria_false` | Everything false | 0% with multiple ineligibility reasons | PASS |
| `test_ineligibility_reasons_deduplicated` | 3 missing must-haves | `"missing Must-Have skills"` appears once | PASS |
| `test_weighted_calculation_accuracy` | Known values: must=100, nice=100, other=50 | Exact weighted total = 95% | PASS |
| `test_missing_match_key_defaults_false` | Skill dict has no `match` key | Defaults to `False` | PASS |

### 5. get_the_score_for_cv_analysis (5 tests)

| Test | Edge Case | Expected | Result |
|---|---|---|---|
| `test_successful_scoring` | Valid AI score response | Saves file, pushes to xcom, includes `original_cv_content` | PASS |
| `test_missing_cv_data` | `cv_data = None` | Returns `None` | PASS |
| `test_missing_jd_data` | `jd_data = None` | Returns `None` | PASS |
| `test_regression_ai_returns_none` | AI returns non-JSON | Returns `None` **(Bug #3 regression)** | PASS |
| `test_file_save_failure_continues` | `Path.mkdir` raises `PermissionError` | Logs error but returns result (doesn't crash) | PASS |

### 6. save_to_google_sheets (7 tests)

| Test | Edge Case | Expected | Result |
|---|---|---|---|
| `test_new_candidate` | Candidate not in sheet | `append_candidate_to_sheet` called, returns `"Success"` | PASS |
| `test_existing_candidate` | Candidate already in sheet | `update_candidate_status` called, returns `"Success"` | PASS |
| `test_missing_score_data` | `score_data = None` | Returns `"Missing data"` | PASS |
| `test_missing_cv_data` | `cv_data = None` | Returns `"Missing data"` | PASS |
| `test_auth_failure` | Google Sheets auth fails | Returns `"Authentication failed"` | PASS |
| `test_regression_none_experience_match` | `experience_match: null` | No crash **(Bug #4 regression)** | PASS |
| `test_regression_none_education_match` | `education_match: null` | No crash **(Bug #4 regression)** | PASS |

### 7. send_response_email (9 tests)

| Test | Edge Case | Expected | Result |
|---|---|---|---|
| `test_rejection_email` | Ineligible candidate | Rejection HTML sent, body contains `"unable to proceed"` | PASS |
| `test_selection_email` | Eligible candidate | AI-generated selection email sent | PASS |
| `test_forwarded_cv_routing` | Recruiter forwarded CV | Sends to candidate, CC to recruiter | PASS |
| `test_missing_data` | All xcom data is `None` | Returns `"Missing data for email"` | PASS |
| `test_auth_failure` | Gmail auth fails | Returns `"Gmail authentication failed"` | PASS |
| `test_regression_no_double_re_prefix` | Subject `"Job Application"` (no Re:) | Passed as-is to `send_email` **(Bug #5 regression)** | PASS |
| `test_regression_subject_already_has_re` | Subject `"Re: Job Application"` | No double `"Re: Re:"` **(Bug #5 regression)** | PASS |
| `test_send_email_failure` | `send_email` returns `None` | Returns `"Failed to send email"` | PASS |
| `test_invalid_sender_email` | Empty `From` header | Returns `"Invalid sender email"` | PASS |

---

## Bugs Discovered & Fixed

### Bug #1: Null/KeyError guard on AI job match in `get_the_jd_for_cv_analysis` (HIGH)

**Location:** `get_the_jd_for_cv_analysis` — after `extract_json_from_text()` call
**Issue:** `matched_job["job_title"]` crashed with `TypeError` when `extract_json_from_text()` returned `None`, or `KeyError` when the AI returned a dict missing the `job_title` key.
**Impact:** The entire CV analysis pipeline crashes if the AI returns unexpected output for job matching.
**Fix:** Added `None` guard and changed to `.get()` with defaults: `matched_job.get("job_title", "Unknown Position")`.

### Bug #2: Null handling for skill lists in `calculate_candidate_score` (HIGH)

**Location:** `calculate_candidate_score` — skill list iteration
**Issue:** Used `.get('must_have_skills', [])` which returns `[]` only if the key is **missing** — but if the AI explicitly returns `null`, the value is `None` and the `for` loop crashes with `TypeError: 'NoneType' is not iterable`.
**Impact:** Any `null` value for skills, experience, or education from the AI crashes the scoring function.
**Fix:** Changed to `or []` / `or {}` pattern for all four fields: `analysis_json.get('must_have_skills') or []`.

### Bug #3: Null check on AI score response in `get_the_score_for_cv_analysis` (HIGH)

**Location:** `get_the_score_for_cv_analysis` — after `extract_json_from_text()` call
**Issue:** `calculate_candidate_score(score_data)` crashed when `extract_json_from_text()` returned `None` (non-JSON AI response).
**Impact:** The scoring task crashes if the AI returns non-parseable output.
**Fix:** Added explicit `None` guard: `if not score_data: return None`.

### Bug #4: Safe chained `.get()` on nullable dicts in `save_to_google_sheets` (MEDIUM)

**Location:** `save_to_google_sheets` — experience/education field extraction
**Issue:** `.get('experience_match', {}).get(...)` crashes when `experience_match` is explicitly `None` (key exists but value is `null`). `None.get(...)` raises `AttributeError`.
**Impact:** Google Sheets save fails if the AI returns `null` for experience or education match.
**Fix:** Changed to `(score_data.get('experience_match') or {}).get(...)`.

### Bug #5: No double `Re:` prefix in `send_response_email` (LOW)

**Location:** `send_response_email` — before `send_email()` call
**Issue:** Code had `subject = f"Re: {subject}"` which always prepended `Re:`. The `send_email()` utility already adds `Re:` if missing, resulting in `"Re: Re: Job Application"`.
**Impact:** Reply emails had double `Re:` prefix (cosmetic but unprofessional).
**Fix:** Removed the manual `subject = f"Re: {subject}"` line — let `send_email()` handle it.

---

## How to Run

```bash
# From the airflow/dags directory:
cd /path/to/airflow/dags

# Run with unittest (no extra dependencies needed):
python3 -m unittest agent_dags.recruitment_dag.test_cv_analyse -v

# Or with pytest (if installed):
python3 -m pytest agent_dags/recruitment_dag/test_cv_analyse.py -v
```

**Runtime:** ~0.05 seconds for all 52 tests.

---

## Recommended Next Steps for Full Validation

1. **Integration test on staging:** Trigger `cv_analyse` with a real CV email payload and verify scoring + Google Sheets save + email response end-to-end.
2. **AI contract test:** Validate that the AI model consistently returns the expected JSON schema for CV scoring (`must_have_skills`, `nice_to_have_skills`, `experience_match`, `education_match` fields).
3. **Scoring accuracy review:** Compare AI-scored candidates against human recruiter assessments to calibrate the 50% eligibility threshold.
4. **Airflow DAG test:** Run `airflow dags test cv_analyse 2024-01-01` with a sample `conf` payload on staging.
