# HubSpot DAGs Code Review

**Date**: 2026-02-11
**Scope**: All 6 DAG files (~20,000+ lines total)

---

## Critical Bugs (P0 - Fix Now)

### `hubspot_search.py`

1. **`get_owner_name_from_id` always returns default** (line 237) - When a match IS found, it returns `DEFAULT_OWNER_NAME` instead of the matched owner's name. Every owner name in confirmation emails will be wrong.

2. **Set literals instead of strings** (lines 924, 2678-2679) - `{DEFAULT_OWNER_ID}` creates a Python **set**, not a string. This breaks JSON serialization and downstream API calls.

3. **Company name destructively normalized** (lines 1363-1370) - `company_details["name"]` is mutated to lowercase-no-spaces in-place. The user-facing email will show `"acmecorp"` instead of `"Acme Corp"`.

4. **Duplicate AI calls** (lines 1310-1318, 1700-1705) - The same AI call is made twice with identical prompts in `search_contacts_with_associations` and `validate_deals_against_associations`. Doubles latency and cost.

5. **`validate_deal_stage` returns unbound variable** (line 1008) - If `json.loads` fails, `parsed_json` was never assigned, causing `NameError`.

### `hubspot_object_creator.py`

6. **`failed_list` references undefined variable `c`** (line 1175) - In the `create_contacts` error handler, `c` is not in scope. Any contact creation failure will crash with `NameError`.

7. **`failed_meetings/notes/tasks` XCom keys never pushed** (lines 3305-3307) - These are pulled but never pushed by any upstream task. Meeting/note/task creation errors are **silently dropped** from the user-facing email.

8. **Unbound `results` variable with swallowed exception** (lines 582-587) - In `analyze_user_response`, if JSON parsing fails, the exception is caught at INFO level and `results` is used on the next line without being defined.

### `hubspot_task_scheduler.py`

9. **Duplicate `except` clause - dead/invalid code** (lines 190-196) - Two consecutive `except Exception` blocks on the same `try` statement. The second is unreachable.

10. **`get_sent_reminders_today` called with `None` args** (line 700) - Passes `owner_id=None, owner_timezone=None`, which immediately crashes inside the function. The exception is swallowed, making aggregate dedup always empty.

### `hubspot_email_listener.py`

11. **Dead/unreachable code in `mark_message_as_read`** (lines 1535-1545) - Both branches of the first `try/except` return, so the second `try` block never executes. References undefined variables (`headers`, `fallback_html`).

12. **Retry counter never increments** (line 315) - `new_retry_count = current_retry_count if is_retry else 0` never advances the counter, potentially causing infinite retries.

13. **Inconsistent retry threshold** (line 141 vs 319) - Checks `>= 2` in one place and `>= 3` in another for the same conceptual max retries.

14. **Bare `raise` with no active exception** (line 5130) - `raise` in an else branch with no current exception causes `RuntimeError`.

15. **~210 lines unreachable code** (lines 3812-4021) - In `generate_final_response_or_trigger_report`, all branches end with `continue`, making the code after them dead.

---

## High Priority (P1)

| Issue | File | Lines |
|-------|------|-------|
| API failure treated as success (hardcoded `"status": "success"`) | object_creator | 1137-1161 |
| Branch runs `create_deals` parallel with `determine_owner` (race condition) | object_creator | 4736 |
| Module-level `Variable.get()` for secrets - runs every scheduler parse cycle | **All 6 files** | Various |
| `time.sleep(180)` blocks Airflow worker slot for minutes | task_scheduler | 853 |
| `total_skipped` counter never incremented (always reports 0) | retry_tasks | 277 |
| No alerting when max retries exceeded - permanent failures go silent | retry_tasks | 134 |
| `compose_validation_error_email` dead code - `primary_entity` always becomes `"entities"` | search | 3037-3085 |
| `extract_json_from_text` regex too restrictive for nested JSON | task_completion_handler | 130 |
| Silent failure on Gmail auth in `send_confirmation_email` | task_completion_handler | 1003 |
| No `on_failure_callback` on task scheduler DAG | task_scheduler | DAG def |
| Missing `trigger_rule` on `end_task` in object creator | object_creator | 4712 |
| File-system state for last processed email (breaks multi-worker) | email_listener | 866 |

---

## Security Concerns (P2)

| Issue | File | Lines |
|-------|------|-------|
| XSS - unsanitized user data in HTML emails (no `html.escape()`) | All DAGs | Throughout |
| Full `email_data` stored in Airflow Variables (visible in UI) | email_listener | 318-329 |
| Gmail OAuth credentials loaded at module level (parse time) | All DAGs | Various |
| PII (names, emails, phone numbers) logged at INFO/DEBUG level | All DAGs | Throughout |
| Sensitive data in retry DagRun conf (visible in Airflow UI) | retry_tasks | 222-228 |

---

## Architecture / Maintainability (P3)

### Code Duplication
- The 12 create/update functions in `object_creator.py` are ~200 lines each of identical boilerplate (~2,400 lines that could be ~300).
- `authenticate_gmail()`, `get_ai_response()`, retry callbacks are copy-pasted identically across 5 files instead of shared.
- `normalize_text` defined identically in 2 functions in `search.py`.
- Sender name extraction regex duplicated 4+ times across files.
- 7+ nearly-identical HTML email templates with same CSS in `email_listener.py`.

### Performance
- 16+ sequential AI calls per search DAG run (including duplicates).
- N+1 API pattern in task scheduler: 80+ sequential HubSpot API calls for 10 tasks.
- N+1 Gmail API calls in thread retrieval (should use `threads.get()`).
- `authenticate_gmail()` called multiple times per DAG run instead of once.

### File Sizes
- `hubspot_email_listener.py`: 5,264 lines
- `hubspot_search.py`: 5,174 lines
- `hubspot_object_creator.py`: 4,746 lines
- All three need decomposition.

### Deprecated Airflow APIs
- `schedule_interval` (use `schedule`) - all DAGs except retry_tasks
- `DummyOperator` (use `EmptyOperator`) - search, object_creator
- `provide_context=True` (unnecessary since Airflow 2.0) - all DAGs
- `logging.basicConfig` at module level overrides Airflow log routing - multiple files

### Other
- HTML email templates built via f-string concatenation (should use Jinja2)
- Hardcoded "Kishore" in AI prompts instead of using `DEFAULT_OWNER_NAME`
- AI prompts (hundreds of lines each) embedded inline in Python functions
- ~180 lines commented-out code in object_creator
- `sys.path.append` hack for cross-DAG imports (fragile)
- XCom key overwriting across tasks in search DAG
- Large XCom payloads (full conversation histories) in metadata DB
- Inconsistent error XCom key naming (e.g., `meetings_errors` pushed vs `failed_meetings` pulled)

---

## What's Working Well

- Event-driven pipeline architecture with clear DAG separation of concerns
- Retry tracker coordination across DAGs via Airflow Variables
- Business-day and timezone-aware task reminders with per-country holiday support
- Fallback search strategies (email -> name -> first name only) in contact search
- Validation rule engine before entity creation
- `max_active_runs=1` on the retry DAG to prevent races
- The retry DAG uses modern TaskFlow API (`@task` decorator)
- Robust AI response parsing with multiple fallback modes
- Spelling variant detection for entity name typos
- Exponential backoff with stream-to-non-stream fallback in AI calls
- 20-minute cooling period between retries prevents retry storms
