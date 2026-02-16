"""Tests for Task 6: compose_and_send_all_emails and send_email_smtp — CC verification."""
import copy
import json
import pytest
from unittest.mock import patch, MagicMock, call
from tests.conftest import SAMPLE_AI_ANALYSIS, SAMPLE_WEEK_RANGE


def _make_analysis(user_id="U001", username="Alice", email="alice@example.com",
                   hr_email="hr@example.com", manager_email="bob@example.com",
                   admin_email="charlie@example.com"):
    """Build a full analysis result dict."""
    analysis = copy.deepcopy(SAMPLE_AI_ANALYSIS)
    analysis.update({
        "user_id": user_id,
        "username": username,
        "user_email_address": email,
        "hr_name": "HR_Manager",
        "hr_email": hr_email,
        "manager_name": "Bob",
        "manager_email": manager_email,
        "admin_name": "Charlie",
        "admin_email": admin_email,
        "week_info": "Week 3 of January",
    })
    return analysis


class TestSendEmailSmtp:
    """Tests for send_email_smtp helper."""

    def test_happy_path(self, tracker_module):
        with patch("smtplib.SMTP") as MockSMTP:
            instance = MockSMTP.return_value
            instance.sendmail = MagicMock()
            instance.quit = MagicMock()
            result = tracker_module.send_email_smtp("to@example.com", "Subject", "<h1>Body</h1>")
        assert result is True
        instance.starttls.assert_called_once()
        instance.login.assert_called_once()
        instance.sendmail.assert_called_once()
        instance.quit.assert_called_once()

    def test_with_cc_sets_header_and_recipients(self, tracker_module):
        with patch("smtplib.SMTP") as MockSMTP:
            instance = MockSMTP.return_value
            instance.sendmail = MagicMock()
            instance.quit = MagicMock()
            tracker_module.send_email_smtp(
                "to@example.com", "Subject", "<h1>Body</h1>",
                cc_list=["cc1@example.com", "cc2@example.com"]
            )
        # Check sendmail was called with To + CC list
        sendmail_args = instance.sendmail.call_args
        recipients = sendmail_args[0][1]  # second positional arg
        assert "to@example.com" in recipients
        assert "cc1@example.com" in recipients
        assert "cc2@example.com" in recipients

    def test_no_cc_only_primary_recipient(self, tracker_module):
        with patch("smtplib.SMTP") as MockSMTP:
            instance = MockSMTP.return_value
            instance.sendmail = MagicMock()
            instance.quit = MagicMock()
            tracker_module.send_email_smtp("to@example.com", "Subject", "<h1>Body</h1>")
        sendmail_args = instance.sendmail.call_args
        recipients = sendmail_args[0][1]
        assert recipients == ["to@example.com"]

    def test_smtp_failure_raises(self, tracker_module):
        with patch("smtplib.SMTP") as MockSMTP:
            MockSMTP.side_effect = Exception("SMTP connection refused")
            with pytest.raises(Exception, match="SMTP connection refused"):
                tracker_module.send_email_smtp("to@example.com", "Subject", "<h1>Body</h1>")


class TestComposeAndSendAllEmails:
    """Tests for compose_and_send_all_emails DAG task."""

    def _setup_ti(self, mock_ti, analyses, week_range=None):
        mock_ti.xcom_push(key="week_range", value=week_range or copy.deepcopy(SAMPLE_WEEK_RANGE))
        mock_ti.xcom_push(key="analysis_results_all", value=analyses)

    @pytest.mark.parametrize("hr,mgr,admin,expected_total", [
        ("hr@e.com", "mgr@e.com", "admin@e.com", 4),   # All 3 CC + To
        ("hr@e.com", None, None, 2),                      # HR only + To
        (None, "mgr@e.com", None, 2),                     # Manager only + To
        (None, None, None, 1),                             # No CC, just To
    ], ids=["all_3_cc", "hr_only", "manager_only", "no_cc"])
    def test_cc_combinations(self, tracker_module, mock_ti, airflow_context,
                             hr, mgr, admin, expected_total):
        analysis = _make_analysis(hr_email=hr, manager_email=mgr, admin_email=admin)
        self._setup_ti(mock_ti, [analysis])
        ctx = airflow_context("2025-01-27T02:30:00")

        with patch.object(tracker_module, "send_email_smtp", return_value=True) as mock_send:
            tracker_module.compose_and_send_all_emails(ti=mock_ti, **ctx)

        _, kwargs = mock_send.call_args
        cc_list = kwargs.get("cc_list") or []
        total = 1 + len(cc_list)  # 1 primary + CCs
        assert total == expected_total

    def test_skips_failed_analysis(self, tracker_module, mock_ti, airflow_context):
        analysis = _make_analysis()
        analysis["analysis_failed"] = True
        analysis["error"] = "parse error"
        self._setup_ti(mock_ti, [analysis])
        ctx = airflow_context("2025-01-27T02:30:00")

        with patch.object(tracker_module, "send_email_smtp") as mock_send:
            result = tracker_module.compose_and_send_all_emails(ti=mock_ti, **ctx)
        mock_send.assert_not_called()
        assert result[0]["status"] == "skipped"

    def test_skips_skipped_analysis(self, tracker_module, mock_ti, airflow_context):
        analysis = _make_analysis()
        analysis["analysis_skipped"] = True
        analysis["error"] = "API failure"
        self._setup_ti(mock_ti, [analysis])
        ctx = airflow_context("2025-01-27T02:30:00")

        with patch.object(tracker_module, "send_email_smtp") as mock_send:
            result = tracker_module.compose_and_send_all_emails(ti=mock_ti, **ctx)
        mock_send.assert_not_called()

    def test_email_html_contains_username(self, tracker_module, mock_ti, airflow_context):
        analysis = _make_analysis(username="TestUser")
        self._setup_ti(mock_ti, [analysis])
        ctx = airflow_context("2025-01-27T02:30:00")

        with patch.object(tracker_module, "send_email_smtp", return_value=True) as mock_send:
            tracker_module.compose_and_send_all_emails(ti=mock_ti, **ctx)

        _, kwargs = mock_send.call_args
        assert "TestUser" in kwargs["body_html"]

    def test_subject_format(self, tracker_module, mock_ti, airflow_context):
        analysis = _make_analysis()
        self._setup_ti(mock_ti, [analysis])
        ctx = airflow_context("2025-01-27T02:30:00")

        with patch.object(tracker_module, "send_email_smtp", return_value=True) as mock_send:
            tracker_module.compose_and_send_all_emails(ti=mock_ti, **ctx)

        _, kwargs = mock_send.call_args
        assert "Weekly Timesheet Review" in kwargs["subject"]
        assert "Week 3 of January" in kwargs["subject"]

    def test_star_generation(self, tracker_module, mock_ti, airflow_context):
        analysis = _make_analysis()
        analysis["ratings"]["total_hours"] = 4
        self._setup_ti(mock_ti, [analysis])
        ctx = airflow_context("2025-01-27T02:30:00")

        with patch.object(tracker_module, "send_email_smtp", return_value=True) as mock_send:
            tracker_module.compose_and_send_all_emails(ti=mock_ti, **ctx)

        _, kwargs = mock_send.call_args
        # Rating 4 → "⭐⭐⭐⭐☆"
        assert "⭐⭐⭐⭐☆" in kwargs["body_html"]

    def test_multiple_users_multiple_sends(self, tracker_module, mock_ti, airflow_context):
        a1 = _make_analysis(user_id="U001", username="Alice", email="alice@e.com")
        a2 = _make_analysis(user_id="U002", username="Dave", email="dave@e.com")
        self._setup_ti(mock_ti, [a1, a2])
        ctx = airflow_context("2025-01-27T02:30:00")

        with patch.object(tracker_module, "send_email_smtp", return_value=True) as mock_send:
            tracker_module.compose_and_send_all_emails(ti=mock_ti, **ctx)

        assert mock_send.call_count == 2

    def test_empty_results(self, tracker_module, mock_ti, airflow_context):
        mock_ti.xcom_push(key="analysis_results_all", value=[])
        mock_ti.xcom_push(key="week_range", value=copy.deepcopy(SAMPLE_WEEK_RANGE))
        ctx = airflow_context("2025-01-27T02:30:00")
        result = tracker_module.compose_and_send_all_emails(ti=mock_ti, **ctx)
        assert result == []

    def test_smtp_failure_marks_failed(self, tracker_module, mock_ti, airflow_context):
        analysis = _make_analysis()
        self._setup_ti(mock_ti, [analysis])
        ctx = airflow_context("2025-01-27T02:30:00")

        with patch.object(tracker_module, "send_email_smtp", side_effect=Exception("SMTP down")):
            result = tracker_module.compose_and_send_all_emails(ti=mock_ti, **ctx)

        assert result[0]["status"] == "failed"

    def test_email_send_results_pushed_to_xcom(self, tracker_module, mock_ti, airflow_context):
        analysis = _make_analysis()
        self._setup_ti(mock_ti, [analysis])
        ctx = airflow_context("2025-01-27T02:30:00")

        with patch.object(tracker_module, "send_email_smtp", return_value=True):
            tracker_module.compose_and_send_all_emails(ti=mock_ti, **ctx)

        results = mock_ti.xcom_pull(key="email_send_results")
        assert isinstance(results, list)
        assert results[0]["status"] == "sent"

    def test_week_info_in_email_body(self, tracker_module, mock_ti, airflow_context):
        analysis = _make_analysis()
        self._setup_ti(mock_ti, [analysis])
        ctx = airflow_context("2025-01-27T02:30:00")

        with patch.object(tracker_module, "send_email_smtp", return_value=True) as mock_send:
            tracker_module.compose_and_send_all_emails(ti=mock_ti, **ctx)

        _, kwargs = mock_send.call_args
        assert "Week 3 of January" in kwargs["body_html"]

    def test_none_results(self, tracker_module, mock_ti, airflow_context):
        # No xcom push → pull returns None
        mock_ti.xcom_push(key="week_range", value=copy.deepcopy(SAMPLE_WEEK_RANGE))
        ctx = airflow_context("2025-01-27T02:30:00")
        result = tracker_module.compose_and_send_all_emails(ti=mock_ti, **ctx)
        assert result == []
