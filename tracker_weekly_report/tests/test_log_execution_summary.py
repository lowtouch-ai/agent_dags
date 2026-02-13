"""Tests for Task 7: log_execution_summary."""
import pytest


class TestLogExecutionSummary:
    def test_correct_counts(self, tracker_module, mock_ti, airflow_context):
        results = [
            {"user_id": "U1", "status": "sent", "final_score": 4.0},
            {"user_id": "U2", "status": "failed", "reason": "SMTP error"},
            {"user_id": "U3", "status": "skipped", "reason": "analysis failed"},
        ]
        mock_ti.xcom_push(key="email_send_results", value=results)
        ctx = airflow_context("2025-01-27T02:30:00")
        summary = tracker_module.log_execution_summary(ti=mock_ti, **ctx)
        assert summary["emails_sent"] == 1
        assert summary["emails_failed"] == 1
        assert summary["emails_skipped"] == 1
        assert summary["total_users"] == 3

    def test_average_score_from_successful_only(self, tracker_module, mock_ti, airflow_context):
        results = [
            {"user_id": "U1", "status": "sent", "final_score": 4.0},
            {"user_id": "U2", "status": "sent", "final_score": 2.0},
            {"user_id": "U3", "status": "failed", "reason": "err"},
        ]
        mock_ti.xcom_push(key="email_send_results", value=results)
        ctx = airflow_context("2025-01-27T02:30:00")
        summary = tracker_module.log_execution_summary(ti=mock_ti, **ctx)
        assert summary["average_score"] == 3.0

    def test_no_results_returns_none(self, tracker_module, mock_ti, airflow_context):
        # No xcom push → pull returns None
        ctx = airflow_context("2025-01-27T02:30:00")
        result = tracker_module.log_execution_summary(ti=mock_ti, **ctx)
        assert result is None

    def test_all_failed_average_zero(self, tracker_module, mock_ti, airflow_context):
        results = [
            {"user_id": "U1", "status": "failed", "reason": "err"},
            {"user_id": "U2", "status": "skipped", "reason": "err"},
        ]
        mock_ti.xcom_push(key="email_send_results", value=results)
        ctx = airflow_context("2025-01-27T02:30:00")
        summary = tracker_module.log_execution_summary(ti=mock_ti, **ctx)
        assert summary["average_score"] == 0

    def test_summary_pushed_to_xcom(self, tracker_module, mock_ti, airflow_context):
        results = [{"user_id": "U1", "status": "sent", "final_score": 3.5}]
        mock_ti.xcom_push(key="email_send_results", value=results)
        ctx = airflow_context("2025-01-27T02:30:00")
        tracker_module.log_execution_summary(ti=mock_ti, **ctx)
        summary = mock_ti.xcom_pull(key="execution_summary")
        assert summary is not None
        assert summary["emails_sent"] == 1
