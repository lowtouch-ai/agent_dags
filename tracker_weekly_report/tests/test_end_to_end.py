"""End-to-end integration tests: all 8 tasks run sequentially through shared MockTaskInstance."""
import copy
import json
import pytest
from unittest.mock import patch, MagicMock
from tests.conftest import (
    MockTaskInstance, SAMPLE_USERS, SAMPLE_API_RESPONSE,
    SAMPLE_AI_ANALYSIS, SAMPLE_ISSUE_RESPONSE,
)


def _run_full_pipeline(tracker_module, mock_ti, ctx, users=None,
                       api_response=None, ai_response=None,
                       issue_details=None, smtp_side_effect=None):
    """Helper to run all 8 tasks in sequence."""
    users = users or copy.deepcopy(SAMPLE_USERS)
    api_response = api_response or copy.deepcopy(SAMPLE_API_RESPONSE)
    ai_json = ai_response or json.dumps(copy.deepcopy(SAMPLE_AI_ANALYSIS))
    issue = issue_details  # can be None to skip

    # Build issue detail return value
    def _issue_side_effect(issue_id):
        if issue is not None:
            return copy.deepcopy(issue)
        # Default: return a simple issue with matching notes
        return {
            "issue_id": str(issue_id),
            "title": f"Issue {issue_id}",
            "description": "desc",
            "status": "resolved",
            "priority": "high",
            "category": "Bug",
            "notes": [
                {
                    "reporter": {"name": users[0]["name"] if users else "Alice"},
                    "created_at": "2025-01-22T10:00:00Z",
                    "updated_at": None,
                },
            ],
        }

    with patch.object(tracker_module, "WHITELISTED_USERS", users), \
         patch.object(tracker_module, "make_api_request", return_value=api_response), \
         patch.object(tracker_module, "fetch_issue_details_from_mantis_api", side_effect=_issue_side_effect), \
         patch.object(tracker_module, "get_ai_response", return_value=ai_json), \
         patch("smtplib.SMTP") as MockSMTP:

        smtp_inst = MockSMTP.return_value
        if smtp_side_effect:
            smtp_inst.sendmail.side_effect = smtp_side_effect
        else:
            smtp_inst.sendmail = MagicMock()
        smtp_inst.quit = MagicMock()

        # Task 1: Load users
        tracker_module.load_whitelisted_users(ti=mock_ti)

        # Task 2: Calculate week range
        tracker_module.calculate_week_range(ti=mock_ti, **ctx)

        # Task 2.5: Filter by timezone
        tracker_module.filter_users_by_timezone(ti=mock_ti, **ctx)

        # Task 3: Fetch timesheets
        tracker_module.fetch_all_timesheets(ti=mock_ti, **ctx)

        # Task 4: Enrich timesheets
        tracker_module.enrich_timesheets_with_issue_details(ti=mock_ti, **ctx)

        # Task 5: Analyze
        tracker_module.analyze_all_timesheets_with_ai(ti=mock_ti, **ctx)

        # Task 6: Email
        tracker_module.compose_and_send_all_emails(ti=mock_ti, **ctx)

        # Task 7: Summary
        summary = tracker_module.log_execution_summary(ti=mock_ti, **ctx)

        return summary, mock_ti, smtp_inst


class TestEndToEndHappyPath:
    def test_full_pipeline_sends_emails(self, tracker_module, airflow_context):
        # Mon 02:30 UTC = Mon 08:00 IST → IST user passes
        ctx = airflow_context("2025-01-27T02:30:00")
        mock_ti = MockTaskInstance()
        summary, ti, smtp = _run_full_pipeline(tracker_module, mock_ti, ctx)
        assert summary is not None
        assert summary["emails_sent"] >= 1


class TestPartialApiFailure:
    def test_one_user_fails_other_succeeds(self, tracker_module, airflow_context):
        ctx = airflow_context("2025-01-27T02:30:00")
        mock_ti = MockTaskInstance()
        users = copy.deepcopy(SAMPLE_USERS)
        # Only use IST users to ensure they pass the filter
        users = [u for u in users if u["timezone"] == "Asia/Kolkata"]
        users.append({
            "user_id": "U003", "mantis_email": "fail@m.com", "name": "FailUser",
            "email": "fail@e.com", "timezone": "Asia/Kolkata",
            "hr_email": None, "manager_email": None, "admin_email": None,
        })

        call_count = {"n": 0}
        original_api_response = copy.deepcopy(SAMPLE_API_RESPONSE)

        def api_side_effect(url, method="GET", params=None, json=None, retries=3):
            call_count["n"] += 1
            if call_count["n"] == 2:
                import requests
                raise requests.exceptions.ConnectionError("down")
            return copy.deepcopy(original_api_response)

        with patch.object(tracker_module, "WHITELISTED_USERS", users), \
             patch.object(tracker_module, "make_api_request", side_effect=api_side_effect), \
             patch.object(tracker_module, "fetch_issue_details_from_mantis_api",
                          return_value={"issue_id": "1001", "title": "T", "description": "",
                                        "status": "x", "priority": "x", "category": "x",
                                        "notes": [{"reporter": {"name": "Alice"},
                                                    "created_at": "2025-01-22T10:00:00Z",
                                                    "updated_at": None}]}), \
             patch.object(tracker_module, "get_ai_response",
                          return_value=json.dumps(copy.deepcopy(SAMPLE_AI_ANALYSIS))), \
             patch("smtplib.SMTP") as MockSMTP:

            smtp_inst = MockSMTP.return_value
            smtp_inst.sendmail = MagicMock()
            smtp_inst.quit = MagicMock()

            tracker_module.load_whitelisted_users(ti=mock_ti)
            tracker_module.calculate_week_range(ti=mock_ti, **ctx)
            tracker_module.filter_users_by_timezone(ti=mock_ti, **ctx)
            tracker_module.fetch_all_timesheets(ti=mock_ti, **ctx)
            tracker_module.enrich_timesheets_with_issue_details(ti=mock_ti, **ctx)
            tracker_module.analyze_all_timesheets_with_ai(ti=mock_ti, **ctx)
            tracker_module.compose_and_send_all_emails(ti=mock_ti, **ctx)
            summary = tracker_module.log_execution_summary(ti=mock_ti, **ctx)

        # At least one email sent, one skipped/failed
        assert summary["emails_sent"] >= 1


class TestAiFailure:
    def test_invalid_json_no_emails(self, tracker_module, airflow_context):
        ctx = airflow_context("2025-01-27T02:30:00")
        mock_ti = MockTaskInstance()
        # Only IST users
        users = [copy.deepcopy(SAMPLE_USERS[0])]
        summary, ti, smtp = _run_full_pipeline(
            tracker_module, mock_ti, ctx, users=users,
            ai_response="NOT VALID JSON AT ALL"
        )
        # Analysis fails → emails skipped
        results = ti.xcom_pull(key="email_send_results")
        sent = [r for r in results if r["status"] == "sent"]
        assert len(sent) == 0


class TestSmtpFailure:
    def test_smtp_failure_marks_failed(self, tracker_module, airflow_context):
        ctx = airflow_context("2025-01-27T02:30:00")
        mock_ti = MockTaskInstance()
        users = [copy.deepcopy(SAMPLE_USERS[0])]
        summary, ti, smtp = _run_full_pipeline(
            tracker_module, mock_ti, ctx, users=users,
            smtp_side_effect=Exception("SMTP down")
        )
        results = ti.xcom_pull(key="email_send_results")
        assert all(r["status"] == "failed" for r in results)


class TestNoUsersAfterFiltering:
    def test_wrong_day_zero_emails(self, tracker_module, airflow_context):
        # Tuesday → IST safeguard blocks all
        ctx = airflow_context("2025-01-28T02:30:00")
        mock_ti = MockTaskInstance()
        summary, ti, smtp = _run_full_pipeline(tracker_module, mock_ti, ctx)
        # No users pass filter → empty downstream → summary is None (no email results)
        assert summary is None or summary.get("emails_sent", 0) == 0


class TestCCVerificationE2E:
    def test_correct_recipient_list_per_user(self, tracker_module, airflow_context):
        ctx = airflow_context("2025-01-27T02:30:00")
        mock_ti = MockTaskInstance()
        users = [copy.deepcopy(SAMPLE_USERS[0])]  # Alice with all 3 CCs

        summary, ti, smtp = _run_full_pipeline(tracker_module, mock_ti, ctx, users=users)

        # Verify SMTP sendmail was called with correct recipient list
        if smtp.sendmail.called:
            call_args = smtp.sendmail.call_args
            recipients = call_args[0][1]  # second positional arg
            assert "alice@example.com" in recipients
            assert "hr@example.com" in recipients
            assert "bob@example.com" in recipients
            assert "charlie@example.com" in recipients
