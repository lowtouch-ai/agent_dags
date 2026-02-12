"""Tests for Task 2: calculate_week_range and get_week_range helper."""
import pytest
from datetime import datetime, timedelta


class TestGetWeekRange:
    """Tests for the get_week_range() helper function."""

    def test_monday_returns_previous_week(self, tracker_module):
        # Monday 2025-01-27 → previous week Mon 2025-01-20 to Sun 2025-01-26
        execution_date = datetime(2025, 1, 27)
        result = tracker_module.get_week_range(execution_date)
        assert result["start"] == "2025-01-20"
        assert result["end"] == "2025-01-26"

    def test_month_boundary_crossing(self, tracker_module):
        # Monday 2025-02-03 → Mon 2025-01-27 to Sun 2025-02-02
        execution_date = datetime(2025, 2, 3)
        result = tracker_module.get_week_range(execution_date)
        assert result["start"] == "2025-01-27"
        assert result["end"] == "2025-02-02"

    def test_year_boundary_crossing(self, tracker_module):
        # Monday 2025-01-06 → Mon 2024-12-30 to Sun 2025-01-05
        execution_date = datetime(2025, 1, 6)
        result = tracker_module.get_week_range(execution_date)
        assert result["start"] == "2024-12-30"
        assert result["end"] == "2025-01-05"

    def test_date_format(self, tracker_module):
        execution_date = datetime(2025, 1, 27)
        result = tracker_module.get_week_range(execution_date)
        # Verify YYYY-MM-DD format
        datetime.strptime(result["start"], "%Y-%m-%d")
        datetime.strptime(result["end"], "%Y-%m-%d")

    def test_week_number_within_month(self, tracker_module):
        execution_date = datetime(2025, 1, 27)
        result = tracker_module.get_week_range(execution_date)
        assert result["week_number"] == 3  # 20th day → (20-1)//7+1 = 3

    def test_month_name(self, tracker_module):
        execution_date = datetime(2025, 1, 27)
        result = tracker_module.get_week_range(execution_date)
        assert result["month"] == "January"

    def test_year_field(self, tracker_module):
        execution_date = datetime(2025, 1, 27)
        result = tracker_module.get_week_range(execution_date)
        assert result["year"] == 2025


class TestCalculateWeekRangeTask:
    """Tests for the calculate_week_range() DAG task function."""

    def test_pushes_week_range_to_xcom(self, tracker_module, mock_ti, airflow_context):
        ctx = airflow_context("2025-01-27T08:30:00")
        tracker_module.calculate_week_range(ti=mock_ti, **ctx)
        wr = mock_ti.xcom_pull(key="week_range")
        assert wr is not None
        assert wr["start"] == "2025-01-20"
        assert wr["end"] == "2025-01-26"
