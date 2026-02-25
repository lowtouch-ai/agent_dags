# Test Results: cv_listner.py (cv_monitor_mailbox DAG)

**Date:** 2026-02-12
**File Under Test:** `recruitment_dag/cv_listner.py`
**Test File:** `recruitment_dag/test_cv_listner.py`
**Test Framework:** Python `unittest` with `unittest.mock`
**Result:** **73/73 PASSED**

---

## Test Scope & Validity

### What These Tests ARE (Unit Tests)

These are **unit tests** that test each of the 11 Airflow task/helper functions **in isolation**. All external dependencies (Gmail API, Google Sheets API, AI model, BeautifulSoup, Airflow Variables) are **mocked** using `unittest.mock` and `sys.modules` injection. This means:

- Each function's **business logic** is tested directly with controlled inputs
- **Edge cases** (null inputs, malformed data, AI failures, API errors) are systematically covered
- Tests are **fast** (~0.07 seconds for all 73), **repeatable**, and **deterministic**
- No external services, credentials, or running Airflow instance required

### What These Tests ARE NOT (Integration/E2E Tests)

These tests do **NOT** validate:

| Not Tested | Why It Matters |
|---|---|
| Real Gmail API thread fetching | Thread extraction and attachment processing are mocked. Real API pagination, rate limits, and auth expiry are not tested. |
| Real Google Sheets candidate lookup | `find_candidate_in_sheet` and `get_candidate_details` are mocked. Real Sheets API errors not tested. |
| Real AI model classification/extraction | `get_ai_response` is mocked. The actual AI model's classification accuracy is not validated. |
| `TriggerDagRunOperator` execution | `route_emails_to_dags` builds trigger requests but the actual Airflow DAG triggering is not tested. |
| Real Airflow XCom serialization | XCom data flow is simulated with mock objects. |
| BeautifulSoup HTML parsing accuracy | HTML-to-text conversion is mocked. Real malformed HTML edge cases are not tested. |
| Timestamp file I/O | `get_last_checked_timestamp` and `update_last_checked_timestamp` are mocked. |

### How to Get Full Coverage

For a complete test strategy, the team should complement these unit tests with:

1. **Integration Tests** (recommended next step): Deploy to staging, send a real test email, and verify the full pipeline executes end-to-end.
2. **Contract Tests**: Validate that the AI model consistently returns the expected JSON schema for email classification and email extraction.
3. **Smoke Tests on Airflow**: Use `airflow dags test cv_monitor_mailbox <date>` on the staging environment.

---

## Test Results Summary

| Test Class | Tests | Pass | Fail | Focus Area |
|---|---|---|---|---|
| `TestExtractMessageBody` | 10 | 10 | 0 | Base64 decoding, multipart parsing, HTML fallback, null handling |
| `TestFormatHistoryForAI` | 13 | 13 | 0 | Conversation pairing, orphan messages, PDF append, regression tests |
| `TestBuildCurrentMessageWithPDF` | 7 | 7 | 0 | PDF content append, null/empty handling |
| `TestExtractEmailFromCv` | 5 | 5 | 0 | AI email extraction, null JSON response, exceptions |
| `TestFetchCvEmails` | 4 | 4 | 0 | Gmail auth, empty mailbox, timestamp update |
| `TestClassifyEmailType` | 5 | 5 | 0 | AI classification, fallback routing, auth failure |
| `TestCheckForEmails` | 4 | 4 | 0 | Branch routing logic |
| `TestRouteEmailsToDags` | 10 | 10 | 0 | DAG triggering, error resilience, summary counting |
| `TestLogNoEmails` | 2 | 2 | 0 | Return value validation |
| `TestFindCvInThread` | 6 | 6 | 0 | PDF/image detection, empty threads, API errors |
| `TestExtractThreadHistoryWithAttachments` | 7 | 7 | 0 | Role assignment, message ordering, error resilience |
| **Total** | **73** | **73** | **0** | |

---

## Detailed Test Cases

### 1. extract_message_body (10 tests)

| Test | Edge Case | Expected | Result |
|---|---|---|---|
| `test_direct_body_data` | Payload has `body.data` (base64url) | Decodes and returns text | PASS |
| `test_multipart_plain_text` | `text/plain` part in multipart | Extracts plain text body | PASS |
| `test_multipart_html_fallback` | Only `text/html` part (no plain) | Falls back to HTML via BeautifulSoup | PASS |
| `test_plain_preferred_over_html` | Both `text/plain` and `text/html` | Prefers plain text | PASS |
| `test_nested_multipart` | Nested `multipart/alternative` parts | Recurses into nested parts | PASS |
| `test_empty_payload` | `{}` | Returns `''` | PASS |
| `test_no_body_no_parts` | `body.size` present but no `data` or `parts` | Returns `''` | PASS |
| `test_body_key_but_no_data` | `body` dict without `data` key | Skips to parts check, returns `''` | PASS |
| `test_multipart_no_data_in_body` | Plain part has no data, HTML part has data | Skips empty plain, uses HTML fallback | PASS |
| `test_exception_returns_empty_string` | `None` payload | Exception caught, returns `''` | PASS |

### 2. format_history_for_ai (13 tests)

| Test | Edge Case | Expected | Result |
|---|---|---|---|
| `test_basic_user_assistant_pair` | Standard user+assistant pair | Creates prompt/response pair | PASS |
| `test_user_without_assistant_reply` | User message, no assistant reply | Response is `''` | PASS |
| `test_empty_history` | `history: []` | Returns `[]` | PASS |
| `test_empty_history_key_missing` | `{}` (no `history` key) | Returns `[]` | PASS |
| `test_standalone_assistant_skipped` | Orphan assistant message | Skipped, not in output | PASS |
| `test_regression_assistant_then_user_not_skipped` | Orphan assistant → user → assistant | User message captured **(Bug #1 regression)** | PASS |
| `test_regression_multiple_orphan_assistants_then_user` | Multiple orphan assistants → user | User message captured **(Bug #1 regression)** | PASS |
| `test_pdf_content_appended_to_user` | User with `has_pdf=True` | PDF content appended to prompt | PASS |
| `test_pdf_content_appended_to_assistant` | Assistant with `has_pdf=True` | PDF content appended to response | PASS |
| `test_multiple_conversation_pairs` | Two user-assistant pairs | Both pairs captured | PASS |
| `test_empty_user_content_skipped` | User content is only whitespace | Skipped (not in output) | PASS |
| `test_user_with_only_pdf_content_included` | Empty text but has PDF | Included with PDF content | PASS |
| `test_consecutive_users_without_assistants` | Two user messages in a row | Each gets its own pair with empty response | PASS |

### 3. build_current_message_with_pdf (7 tests)

| Test | Edge Case | Expected | Result |
|---|---|---|---|
| `test_basic_message_no_pdf` | No PDF attached | Returns just the content | PASS |
| `test_with_pdf_content` | Has PDF content | Content + `[Attached PDF Content]` + PDF text | PASS |
| `test_no_current_message` | `current_message: None` | Returns `''` | PASS |
| `test_empty_history_data` | `{}` | Returns `''` | PASS |
| `test_has_pdf_true_but_no_content` | `has_pdf=True`, `pdf_content=None` | No append, returns text only | PASS |
| `test_has_pdf_false_with_content` | `has_pdf=False` with pdf_content | No append (flag controls behavior) | PASS |
| `test_empty_content_string` | Content is `''` | Returns `''` | PASS |

### 4. extract_email_from_cv (5 tests)

| Test | Edge Case | Expected | Result |
|---|---|---|---|
| `test_successful_extraction` | AI finds email in CV | Returns extracted email | PASS |
| `test_not_found` | AI returns `NOT_FOUND` | Returns `None` | PASS |
| `test_regression_ai_returns_invalid_json` | AI returns non-JSON text | Returns `None` **(Bug #2 regression)** | PASS |
| `test_exception_in_ai_call` | AI service throws exception | Returns `None` | PASS |
| `test_empty_email_string` | AI returns empty email string | Returns `None` (falsy) | PASS |

### 5. fetch_cv_emails (4 tests)

| Test | Edge Case | Expected | Result |
|---|---|---|---|
| `test_successful_fetch` | Normal fetch with 2 emails | Pushes to xcom, updates timestamp to latest | PASS |
| `test_empty_mailbox` | No unread emails | Returns `[]`, timestamp NOT updated | PASS |
| `test_auth_failure` | `authenticate_gmail` returns `None` | Returns `[]` | PASS |
| `test_exception_propagated` | Network error during fetch | Exception re-raised | PASS |

### 6. classify_email_type (5 tests)

| Test | Edge Case | Expected | Result |
|---|---|---|---|
| `test_classify_new_cv` | Email with PDF attachment | Classified as `NEW_CV_APPLICATION` | PASS |
| `test_no_emails_to_classify` | `xcom_pull` returns `None` | Returns `[]` | PASS |
| `test_auth_failure_raises` | Gmail auth fails | Raises `RuntimeError` | PASS |
| `test_classification_error_fallback_with_attachment` | Classification crashes, has attachments | Fallback: `NEW_CV_APPLICATION` (confidence 50) | PASS |
| `test_classification_error_fallback_without_attachment` | Classification crashes, no attachments | Fallback: `OTHER` (target `none`) | PASS |

### 7. check_for_emails (4 tests)

| Test | Edge Case | Expected | Result |
|---|---|---|---|
| `test_has_emails` | Classified emails exist | Returns `'route_emails_to_dags'` | PASS |
| `test_no_emails_none` | `xcom_pull` returns `None` | Returns `'no_emails_found'` | PASS |
| `test_empty_list` | Empty classified list | Returns `'no_emails_found'` | PASS |
| `test_multiple_emails` | Multiple classified emails | Returns `'route_emails_to_dags'` | PASS |

### 8. route_emails_to_dags (10 tests)

| Test | Edge Case | Expected | Result |
|---|---|---|---|
| `test_route_to_cv_analyse` | `NEW_CV_APPLICATION` type | Trigger request for `cv_analyse` | PASS |
| `test_route_to_screening` | `SCREENING_RESPONSE` type | Trigger request for `screening_response_analysis` | PASS |
| `test_skip_other` | `OTHER` type | Skipped (no trigger) | PASS |
| `test_no_emails` | `xcom_pull` returns `None` | Returns `None` early | PASS |
| `test_mixed_routing` | Mix of types (CV + OTHER + screening) | Only actionable emails get triggers | PASS |
| `test_regression_unexpected_target_dag` | AI returns unknown `target_dag` | No KeyError crash **(Bug #3 regression)** | PASS |
| `test_other_type_skipped_even_with_target_dag` | `OTHER` type with non-`none` target_dag | Still skipped | PASS |
| `test_routing_error_continues` | One broken email in batch | Error caught, next email still processed | PASS |
| `test_extracted_email_counted` | Email has `extracted_candidate_email` | Counter incremented in summary | PASS |
| `test_thread_history_counted` | Email has non-empty thread history | Counter incremented in summary | PASS |

### 9. log_no_emails (2 tests)

| Test | Edge Case | Expected | Result |
|---|---|---|---|
| `test_returns_message` | Normal call | Returns `"No emails to process"` | PASS |
| `test_return_type` | Normal call | Return type is `str` | PASS |

### 10. find_cv_in_thread (6 tests)

| Test | Edge Case | Expected | Result |
|---|---|---|---|
| `test_pdf_found` | Thread has PDF attachment | Returns attachment list with PDF | PASS |
| `test_image_found` | Thread has image attachment | Returns attachment list with image | PASS |
| `test_no_cv_in_any_message` | Only non-CV attachments (`.txt`) | Returns `None` | PASS |
| `test_api_error` | Gmail API throws exception | Returns `None` | PASS |
| `test_empty_thread` | Thread has no messages | Returns `None` | PASS |
| `test_empty_attachments_list` | Messages have no attachments | Returns `None` | PASS |

### 11. extract_thread_history_with_attachments (7 tests)

| Test | Edge Case | Expected | Result |
|---|---|---|---|
| `test_basic_thread` | 3-message thread (user → bot → user) | History has 2 entries, current is last | PASS |
| `test_empty_thread` | No messages in thread | Empty history, `current_message=None` | PASS |
| `test_no_current_message_id_uses_last` | No `current_message_id` param | Last message becomes current | PASS |
| `test_role_assignment` | Bot sender vs. external sender | Bot=`assistant`, external=`user` | PASS |
| `test_api_error_returns_empty` | Gmail API throws exception | Empty history, `current_message=None` | PASS |
| `test_single_message_no_id` | Single message, no ID param | It becomes current, history is empty | PASS |
| `test_message_error_continues` | Error processing first message | Second message still processed | PASS |

---

## Bugs Discovered & Fixed

### Bug #1: Double-increment in `format_history_for_ai` (MEDIUM)

**Location:** `format_history_for_ai` — `else` branch for standalone assistant messages
**Issue:** The `else` branch (standalone assistant messages) had an extra `i += 1` that, combined with the outer `i += 1`, caused the loop to double-increment. This meant a user message following a standalone assistant message was silently dropped from the conversation history.
**Impact:** Lost conversation context sent to the AI for classification — the AI would not see some candidate messages, potentially leading to incorrect email classification.
**Fix:** Removed the redundant `i += 1` in the `else` branch.

### Bug #2: Null check on AI JSON response in `extract_email_from_cv` (HIGH)

**Location:** `extract_email_from_cv` — after `extract_json_from_text()` call
**Issue:** `extract_json_from_text()` can return `None` when the AI response contains no valid JSON. The subsequent `.get('email', 'NOT_FOUND')` call crashed with `AttributeError: 'NoneType' object has no attribute 'get'`.
**Impact:** The entire email classification pipeline crashes for that email if the AI returns a non-JSON response for email extraction.
**Fix:** Added explicit `None` guard: `if not result: return None`.

### Bug #3: Safe routing summary counter in `route_emails_to_dags` (MEDIUM)

**Location:** `route_emails_to_dags` — `routing_summary[target_dag] += 1`
**Issue:** Crashed with `KeyError` if the AI returned an unexpected `target_dag` value not in the pre-initialized dict. This would abort the entire routing function, losing all trigger requests for the batch.
**Impact:** One unexpected classification could prevent ALL emails in the batch from being routed to downstream DAGs.
**Fix:** Added membership check: `if target_dag in routing_summary: ... else: routing_summary[target_dag] = 1`.

---

## How to Run

```bash
# From the airflow/dags directory:
cd /path/to/airflow/dags

# Run with unittest (no extra dependencies needed):
python3 -m unittest agent_dags.recruitment_dag.test_cv_listner -v

# Or with pytest (if installed):
python3 -m pytest agent_dags/recruitment_dag/test_cv_listner.py -v
```

**Runtime:** ~0.07 seconds for all 73 tests.

---

## Recommended Next Steps for Full Validation

1. **Integration test on staging:** Send a real test email with a CV attachment and verify the full `cv_monitor_mailbox -> cv_analyse / screening_response_analysis` pipeline triggers correctly.
2. **AI contract test:** Validate that the AI model consistently returns the expected JSON schema for email classification (`email_type`, `target_dag`, `confidence`) and email extraction (`email`, `confidence`).
3. **Airflow DAG test:** Run `airflow dags test cv_monitor_mailbox 2024-01-01` on staging to verify task dependencies and XCom data flow.
4. **Gmail API resilience test:** Test with rate-limited or expired credentials to verify the auth retry/failure handling.
