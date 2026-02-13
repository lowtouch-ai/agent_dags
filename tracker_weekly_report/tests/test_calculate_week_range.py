"""Tests for Task 2: calculate_week_range, get_week_range, and count_working_days helpers."""
import pytest
from datetime import datetime, timedelta


class TestCountWorkingDays:
    """Tests for the count_working_days() helper function."""

    def test_full_week_mon_to_sun(self, tracker_module):
        # Mon 2025-01-20 to Sun 2025-01-26 → 5 working days
        assert tracker_module.count_working_days("2025-01-20", "2025-01-26") == 5

    def test_full_week_mon_to_fri(self, tracker_module):
        # Mon 2025-01-20 to Fri 2025-01-24 → 5 working days
        assert tracker_module.count_working_days("2025-01-20", "2025-01-24") == 5

    def test_two_weeks(self, tracker_module):
        # Mon 2025-01-20 to Sun 2025-02-02 → 10 working days
        assert tracker_module.count_working_days("2025-01-20", "2025-02-02") == 10

    def test_12_calendar_days_feb_1_to_12(self, tracker_module):
        # Feb 1 (Sun) to Feb 12 (Thu) 2026 → 9 working days
        # Sun 1 (skip), Mon-Fri 2-6 (5), Sat 7, Sun 8 (skip), Mon-Thu 9-12 (4) = 9
        assert tracker_module.count_working_days("2026-02-01", "2026-02-12") == 9

    def test_single_weekday(self, tracker_module):
        assert tracker_module.count_working_days("2025-01-20", "2025-01-20") == 1

    def test_single_weekend_day(self, tracker_module):
        # Saturday
        assert tracker_module.count_working_days("2025-01-25", "2025-01-25") == 0

    def test_weekend_only(self, tracker_module):
        # Sat-Sun
        assert tracker_module.count_working_days("2025-01-25", "2025-01-26") == 0


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
        assert mock_ti.xcom_pull(key="custom_date_range") is False

    def test_default_when_conf_is_empty(self, tracker_module, mock_ti, airflow_context):
        ctx = airflow_context("2025-01-27T08:30:00", conf={})
        tracker_module.calculate_week_range(ti=mock_ti, **ctx)
        wr = mock_ti.xcom_pull(key="week_range")
        assert wr["start"] == "2025-01-20"
        assert wr["end"] == "2025-01-26"
        assert mock_ti.xcom_pull(key="custom_date_range") is False


class TestCalculateWeekRangeCustomDates:
    """Tests for custom date range via dag_run.conf."""

    def test_custom_date_range_used(self, tracker_module, mock_ti, airflow_context):
        conf = {"week_start": "2025-02-10", "week_end": "2025-02-16"}
        ctx = airflow_context("2025-01-27T08:30:00", conf=conf)
        result = tracker_module.calculate_week_range(ti=mock_ti, **ctx)
        assert result["start"] == "2025-02-10"
        assert result["end"] == "2025-02-16"
        assert result["month"] == "February"
        assert result["year"] == 2025
        assert mock_ti.xcom_pull(key="custom_date_range") is True

    def test_custom_range_week_info_is_empty(self, tracker_module, mock_ti, airflow_context):
        """week_info should be empty for custom ranges to avoid duplicate dates in email."""
        conf = {"week_start": "2026-02-01", "week_end": "2026-02-12"}
        ctx = airflow_context("2026-02-12T08:30:00", conf=conf)
        result = tracker_module.calculate_week_range(ti=mock_ti, **ctx)
        assert result["week_info"] == ""

    def test_custom_range_scaled_hours_for_12_days(self, tracker_module, mock_ti, airflow_context):
        """Feb 1-12, 2026 has 9 working days → min 72 hrs, max 81 hrs."""
        conf = {"week_start": "2026-02-01", "week_end": "2026-02-12"}
        ctx = airflow_context("2026-02-12T08:30:00", conf=conf)
        result = tracker_module.calculate_week_range(ti=mock_ti, **ctx)
        assert result["working_days"] == 9
        assert result["scaled_min_hours"] == 72
        assert result["scaled_max_hours"] == 81

    def test_custom_range_standard_week_scaled_hours(self, tracker_module, mock_ti, airflow_context):
        """Mon-Sun standard week → 5 working days → 40-45 hrs."""
        conf = {"week_start": "2025-02-10", "week_end": "2025-02-16"}
        ctx = airflow_context("2025-01-27T08:30:00", conf=conf)
        result = tracker_module.calculate_week_range(ti=mock_ti, **ctx)
        assert result["working_days"] == 5
        assert result["scaled_min_hours"] == 40
        assert result["scaled_max_hours"] == 45

    def test_custom_dates_override_execution_date(self, tracker_module, mock_ti, airflow_context):
        """Custom conf dates should be used regardless of execution_date."""
        conf = {"week_start": "2024-12-23", "week_end": "2024-12-29"}
        ctx = airflow_context("2025-01-27T08:30:00", conf=conf)
        result = tracker_module.calculate_week_range(ti=mock_ti, **ctx)
        assert result["start"] == "2024-12-23"
        assert result["end"] == "2024-12-29"
        assert result["month"] == "December"
        assert result["year"] == 2024

    def test_custom_start_only_falls_back_to_default(self, tracker_module, mock_ti, airflow_context):
        """If only week_start is provided (no week_end), fall back to default calculation."""
        conf = {"week_start": "2025-02-10"}
        ctx = airflow_context("2025-01-27T08:30:00", conf=conf)
        result = tracker_module.calculate_week_range(ti=mock_ti, **ctx)
        # Falls back to default
        assert result["start"] == "2025-01-20"
        assert mock_ti.xcom_pull(key="custom_date_range") is False

    def test_custom_end_only_falls_back_to_default(self, tracker_module, mock_ti, airflow_context):
        """If only week_end is provided (no week_start), fall back to default calculation."""
        conf = {"week_end": "2025-02-16"}
        ctx = airflow_context("2025-01-27T08:30:00", conf=conf)
        result = tracker_module.calculate_week_range(ti=mock_ti, **ctx)
        assert result["start"] == "2025-01-20"
        assert mock_ti.xcom_pull(key="custom_date_range") is False

    def test_invalid_date_format_raises(self, tracker_module, mock_ti, airflow_context):
        conf = {"week_start": "10-02-2025", "week_end": "16-02-2025"}
        ctx = airflow_context("2025-01-27T08:30:00", conf=conf)
        with pytest.raises(ValueError, match="Invalid date format"):
            tracker_module.calculate_week_range(ti=mock_ti, **ctx)

    def test_start_after_end_raises(self, tracker_module, mock_ti, airflow_context):
        conf = {"week_start": "2025-02-16", "week_end": "2025-02-10"}
        ctx = airflow_context("2025-01-27T08:30:00", conf=conf)
        with pytest.raises(ValueError, match="must be before"):
            tracker_module.calculate_week_range(ti=mock_ti, **ctx)

    def test_empty_string_dates_fall_back_to_default(self, tracker_module, mock_ti, airflow_context):
        """Empty string params (Airflow Param defaults) should fall back to default."""
        conf = {"week_start": "", "week_end": ""}
        ctx = airflow_context("2025-01-27T08:30:00", conf=conf)
        result = tracker_module.calculate_week_range(ti=mock_ti, **ctx)
        assert result["start"] == "2025-01-20"
        assert mock_ti.xcom_pull(key="custom_date_range") is False
