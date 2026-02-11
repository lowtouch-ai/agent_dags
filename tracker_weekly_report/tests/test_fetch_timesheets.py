"""Tests for Task 3: fetch_all_timesheets and fetch_mantis_timesheet_data_direct_api."""
import copy
import pytest
from unittest.mock import patch, MagicMock
from tests.conftest import SAMPLE_API_RESPONSE, SAMPLE_USERS, SAMPLE_WEEK_RANGE


class TestFetchMantisTimesheetDataDirectApi:
    """Tests for fetch_mantis_timesheet_data_direct_api helper."""

    def test_happy_path(self, tracker_module):
        with patch.object(tracker_module, "make_api_request", return_value=copy.deepcopy(SAMPLE_API_RESPONSE)):
            result = tracker_module.fetch_mantis_timesheet_data_direct_api("Alice", "2025-01-20", "2025-01-26")
        assert result["total_hours"] == 42.5
        assert result["entry_count"] == 4
        assert "error" not in result

    def test_invalid_time_format_returns_error(self, tracker_module):
        bad_response = {"total_time": "abc:xyz", "project": [], "tickets_worked": []}
        with patch.object(tracker_module, "make_api_request", return_value=bad_response):
            result = tracker_module.fetch_mantis_timesheet_data_direct_api("Alice", "2025-01-20", "2025-01-26")
        assert "error" in result
        assert result["total_hours"] == 0

    def test_zero_time(self, tracker_module):
        resp = {"total_time": "0:0", "project": [], "tickets_worked": []}
        with patch.object(tracker_module, "make_api_request", return_value=resp):
            result = tracker_module.fetch_mantis_timesheet_data_direct_api("Alice", "2025-01-20", "2025-01-26")
        assert result["total_hours"] == 0.0
        assert result["entry_count"] == 0

    def test_empty_tickets_worked(self, tracker_module):
        resp = {"total_time": "10:00", "project": ["P1"], "tickets_worked": []}
        with patch.object(tracker_module, "make_api_request", return_value=resp):
            result = tracker_module.fetch_mantis_timesheet_data_direct_api("Alice", "2025-01-20", "2025-01-26")
        assert result["entry_count"] == 0
        assert result["total_hours"] == 10.0

    def test_network_timeout_returns_error(self, tracker_module):
        import requests
        with patch.object(tracker_module, "make_api_request",
                          side_effect=requests.exceptions.Timeout("Connection timed out")):
            result = tracker_module.fetch_mantis_timesheet_data_direct_api("Alice", "2025-01-20", "2025-01-26")
        assert "error" in result
        assert result["total_hours"] == 0

    def test_tasks_over_4_hours_counted(self, tracker_module):
        resp = copy.deepcopy(SAMPLE_API_RESPONSE)
        with patch.object(tracker_module, "make_api_request", return_value=resp):
            result = tracker_module.fetch_mantis_timesheet_data_direct_api("Alice", "2025-01-20", "2025-01-26")
        # 10h, 8.5h, 12h, 12h → all > 4h
        assert result["tasks_over_4_hours"] == 4


class TestFetchAllTimesheets:
    """Tests for fetch_all_timesheets DAG task."""

    def _setup_ti(self, mock_ti, users, week_range):
        mock_ti.xcom_push(key="users_to_process", value=users)
        mock_ti.xcom_push(key="week_range", value=week_range)

    def test_no_users_returns_empty(self, tracker_module, mock_ti, airflow_context):
        mock_ti.xcom_push(key="users_to_process", value=[])
        mock_ti.xcom_push(key="week_range", value=copy.deepcopy(SAMPLE_WEEK_RANGE))
        ctx = airflow_context("2025-01-27T02:30:00")
        result = tracker_module.fetch_all_timesheets(ti=mock_ti, **ctx)
        assert result == []

    def test_iterates_all_users(self, tracker_module, mock_ti, airflow_context):
        users = copy.deepcopy(SAMPLE_USERS)
        self._setup_ti(mock_ti, users, copy.deepcopy(SAMPLE_WEEK_RANGE))
        ctx = airflow_context("2025-01-27T02:30:00")
        with patch.object(tracker_module, "fetch_mantis_timesheet_data_direct_api",
                          return_value={"total_hours": 40, "entries": [], "entry_count": 0,
                                        "unique_dates": 0, "tasks_over_4_hours": 0}):
            result = tracker_module.fetch_all_timesheets(ti=mock_ti, **ctx)
        assert len(result) == 2

    def test_adds_user_metadata(self, tracker_module, mock_ti, airflow_context):
        users = [copy.deepcopy(SAMPLE_USERS[0])]
        self._setup_ti(mock_ti, users, copy.deepcopy(SAMPLE_WEEK_RANGE))
        ctx = airflow_context("2025-01-27T02:30:00")
        with patch.object(tracker_module, "fetch_mantis_timesheet_data_direct_api",
                          return_value={"total_hours": 40, "entries": [], "entry_count": 0,
                                        "unique_dates": 0, "tasks_over_4_hours": 0}):
            result = tracker_module.fetch_all_timesheets(ti=mock_ti, **ctx)
        assert result[0]["username"] == "Alice"
        assert result[0]["user_email_address"] == "alice@example.com"
        assert result[0]["week_info"] == "Week 3 of January"

    def test_one_user_fails_others_continue(self, tracker_module, mock_ti, airflow_context):
        users = copy.deepcopy(SAMPLE_USERS)
        self._setup_ti(mock_ti, users, copy.deepcopy(SAMPLE_WEEK_RANGE))
        ctx = airflow_context("2025-01-27T02:30:00")
        call_count = {"n": 0}

        def side_effect(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise Exception("API exploded")
            return {"total_hours": 40, "entries": [], "entry_count": 0,
                    "unique_dates": 0, "tasks_over_4_hours": 0}

        with patch.object(tracker_module, "fetch_mantis_timesheet_data_direct_api", side_effect=side_effect):
            result = tracker_module.fetch_all_timesheets(ti=mock_ti, **ctx)
        assert len(result) == 2
        assert "error" in result[0]
        assert "error" not in result[1]

    def test_pushes_consolidated_xcom(self, tracker_module, mock_ti, airflow_context):
        users = [copy.deepcopy(SAMPLE_USERS[0])]
        self._setup_ti(mock_ti, users, copy.deepcopy(SAMPLE_WEEK_RANGE))
        ctx = airflow_context("2025-01-27T02:30:00")
        with patch.object(tracker_module, "fetch_mantis_timesheet_data_direct_api",
                          return_value={"total_hours": 40, "entries": [], "entry_count": 0,
                                        "unique_dates": 0, "tasks_over_4_hours": 0}):
            tracker_module.fetch_all_timesheets(ti=mock_ti, **ctx)
        all_data = mock_ti.xcom_pull(key="timesheet_data_all")
        assert isinstance(all_data, list)
        assert len(all_data) == 1


class TestMakeApiRequest:
    """Tests for make_api_request helper."""

    def test_get_request_returns_json(self, tracker_module):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"ok": True}
        mock_resp.content = b'{"ok": true}'
        mock_resp.raise_for_status = MagicMock()

        with patch("requests.Session") as MockSession:
            session_instance = MagicMock()
            session_instance.get.return_value = mock_resp
            MockSession.return_value = session_instance
            result = tracker_module.make_api_request("https://api.example.com/data")
        assert result == {"ok": True}
