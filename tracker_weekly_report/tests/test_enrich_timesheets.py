"""Tests for Task 4: enrich_timesheets_with_issue_details."""
import copy
import pytest
from unittest.mock import patch, MagicMock
from tests.conftest import SAMPLE_TIMESHEET_DATA, SAMPLE_ISSUE_RESPONSE


class TestFetchIssueDetailsFromMantisApi:
    """Tests for fetch_issue_details_from_mantis_api helper."""

    def test_happy_path_transforms_issue(self, tracker_module):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = copy.deepcopy(SAMPLE_ISSUE_RESPONSE)
        mock_resp.raise_for_status = MagicMock()
        with patch("requests.get", return_value=mock_resp):
            result = tracker_module.fetch_issue_details_from_mantis_api("1001")
        assert result is not None
        assert result["issue_id"] == "1001"
        assert result["title"] == "Fix login bug"
        assert len(result["notes"]) == 3

    def test_404_returns_none(self, tracker_module):
        import requests
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        http_error = requests.exceptions.HTTPError(response=mock_resp)
        mock_resp.raise_for_status.side_effect = http_error
        with patch("requests.get", return_value=mock_resp):
            result = tracker_module.fetch_issue_details_from_mantis_api("9999")
        assert result is None

    def test_network_error_returns_none(self, tracker_module):
        import requests
        with patch("requests.get", side_effect=requests.exceptions.ConnectionError("down")):
            result = tracker_module.fetch_issue_details_from_mantis_api("1001")
        assert result is None

    def test_empty_issues_array_returns_none(self, tracker_module):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"issues": []}
        mock_resp.raise_for_status = MagicMock()
        with patch("requests.get", return_value=mock_resp):
            result = tracker_module.fetch_issue_details_from_mantis_api("1001")
        assert result is None


class TestEnrichTimesheets:
    """Tests for enrich_timesheets_with_issue_details DAG task."""

    def _make_enriched_issue(self, issue_id="1001"):
        return {
            "issue_id": str(issue_id),
            "title": "Fix login bug",
            "description": "Users cannot login",
            "status": "resolved",
            "priority": "high",
            "category": "Bug",
            "notes": [
                {
                    "reporter": {"name": "Alice"},
                    "created_at": "2025-01-22T10:30:00Z",
                    "updated_at": "2025-01-22T14:00:00Z",
                    "text": "Fixed auth flow",
                },
                {
                    "reporter": {"name": "Alice"},
                    "created_at": "2025-01-23T09:00:00Z",
                    "updated_at": None,
                    "text": "Verified in staging",
                },
            ],
        }

    def test_extracts_work_dates_within_range(self, tracker_module, mock_ti, airflow_context):
        ts = copy.deepcopy(SAMPLE_TIMESHEET_DATA)
        # Only keep one entry for simplicity
        ts["entries"] = [ts["entries"][0]]
        mock_ti.xcom_push(key="timesheet_data_all", value=[ts])
        ctx = airflow_context("2025-01-27T02:30:00")

        with patch.object(tracker_module, "fetch_issue_details_from_mantis_api",
                          return_value=self._make_enriched_issue("1001")):
            result = tracker_module.enrich_timesheets_with_issue_details(ti=mock_ti, **ctx)

        assert len(result) == 1
        assert result[0].get("enriched") is True
        assert result[0]["unique_dates"] >= 1

    def test_ignores_other_users_notes(self, tracker_module, mock_ti, airflow_context):
        ts = copy.deepcopy(SAMPLE_TIMESHEET_DATA)
        ts["entries"] = [ts["entries"][0]]
        mock_ti.xcom_push(key="timesheet_data_all", value=[ts])
        ctx = airflow_context("2025-01-27T02:30:00")

        issue = self._make_enriched_issue("1001")
        # Only OtherUser notes
        issue["notes"] = [{"reporter": {"name": "OtherUser"},
                           "created_at": "2025-01-22T10:00:00Z", "updated_at": None}]

        with patch.object(tracker_module, "fetch_issue_details_from_mantis_api", return_value=issue):
            result = tracker_module.enrich_timesheets_with_issue_details(ti=mock_ti, **ctx)
        assert result[0]["unique_dates"] == 0

    def test_dates_outside_range_excluded(self, tracker_module, mock_ti, airflow_context):
        ts = copy.deepcopy(SAMPLE_TIMESHEET_DATA)
        ts["entries"] = [ts["entries"][0]]
        mock_ti.xcom_push(key="timesheet_data_all", value=[ts])
        ctx = airflow_context("2025-01-27T02:30:00")

        issue = self._make_enriched_issue("1001")
        # Date outside week range
        issue["notes"] = [{"reporter": {"name": "Alice"},
                           "created_at": "2025-01-15T10:00:00Z", "updated_at": None}]

        with patch.object(tracker_module, "fetch_issue_details_from_mantis_api", return_value=issue):
            result = tracker_module.enrich_timesheets_with_issue_details(ti=mock_ti, **ctx)
        assert result[0]["unique_dates"] == 0

    def test_skips_timesheets_with_error(self, tracker_module, mock_ti, airflow_context):
        ts = copy.deepcopy(SAMPLE_TIMESHEET_DATA)
        ts["error"] = "API failure"
        mock_ti.xcom_push(key="timesheet_data_all", value=[ts])
        ctx = airflow_context("2025-01-27T02:30:00")
        result = tracker_module.enrich_timesheets_with_issue_details(ti=mock_ti, **ctx)
        assert "error" in result[0]
        assert result[0].get("enriched") is not True

    def test_empty_input(self, tracker_module, mock_ti, airflow_context):
        mock_ti.xcom_push(key="timesheet_data_all", value=[])
        ctx = airflow_context("2025-01-27T02:30:00")
        result = tracker_module.enrich_timesheets_with_issue_details(ti=mock_ti, **ctx)
        assert result == []

    def test_none_input(self, tracker_module, mock_ti, airflow_context):
        # No xcom push → pull returns None
        ctx = airflow_context("2025-01-27T02:30:00")
        result = tracker_module.enrich_timesheets_with_issue_details(ti=mock_ti, **ctx)
        assert result == []

    def test_handles_iso_timestamps(self, tracker_module, mock_ti, airflow_context):
        ts = copy.deepcopy(SAMPLE_TIMESHEET_DATA)
        ts["entries"] = [ts["entries"][0]]
        mock_ti.xcom_push(key="timesheet_data_all", value=[ts])
        ctx = airflow_context("2025-01-27T02:30:00")

        issue = self._make_enriched_issue("1001")
        issue["notes"] = [{"reporter": {"name": "Alice"},
                           "created_at": "2025-01-20T10:30:00Z", "updated_at": "2025-01-20T14:00:00Z"}]

        with patch.object(tracker_module, "fetch_issue_details_from_mantis_api", return_value=issue):
            result = tracker_module.enrich_timesheets_with_issue_details(ti=mock_ti, **ctx)
        assert "2025-01-20" in result[0].get("work_dates_list", [])

    def test_handles_date_strings(self, tracker_module, mock_ti, airflow_context):
        ts = copy.deepcopy(SAMPLE_TIMESHEET_DATA)
        ts["entries"] = [ts["entries"][0]]
        mock_ti.xcom_push(key="timesheet_data_all", value=[ts])
        ctx = airflow_context("2025-01-27T02:30:00")

        issue = self._make_enriched_issue("1001")
        issue["notes"] = [{"reporter": {"name": "Alice"},
                           "created_at": "2025-01-21", "updated_at": None}]

        with patch.object(tracker_module, "fetch_issue_details_from_mantis_api", return_value=issue):
            result = tracker_module.enrich_timesheets_with_issue_details(ti=mock_ti, **ctx)
        assert "2025-01-21" in result[0].get("work_dates_list", [])

    def test_unique_dates_count_matches(self, tracker_module, mock_ti, airflow_context):
        ts = copy.deepcopy(SAMPLE_TIMESHEET_DATA)
        ts["entries"] = [ts["entries"][0]]
        mock_ti.xcom_push(key="timesheet_data_all", value=[ts])
        ctx = airflow_context("2025-01-27T02:30:00")

        issue = self._make_enriched_issue("1001")
        # 2 unique dates: 22nd and 23rd
        issue["notes"] = [
            {"reporter": {"name": "Alice"}, "created_at": "2025-01-22T10:00:00Z", "updated_at": None},
            {"reporter": {"name": "Alice"}, "created_at": "2025-01-23T09:00:00Z", "updated_at": None},
            {"reporter": {"name": "Alice"}, "created_at": "2025-01-22T15:00:00Z", "updated_at": None},  # same day
        ]

        with patch.object(tracker_module, "fetch_issue_details_from_mantis_api", return_value=issue):
            result = tracker_module.enrich_timesheets_with_issue_details(ti=mock_ti, **ctx)
        assert result[0]["unique_dates"] == 2
