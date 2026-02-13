"""Tests for Task 2.5: filter_users_by_timezone — the most complex task."""
import copy
import pytest
import pendulum
from tests.conftest import SAMPLE_USERS


class TestISTSafeguards:
    """IST day-of-week and hour window enforcement."""

    def test_not_monday_in_ist_skips_all(self, tracker_module, mock_ti_with_data, airflow_context):
        # Tuesday 2025-01-28 08:30 UTC = Tuesday 14:00 IST → skip
        ctx = airflow_context("2025-01-28T08:30:00")
        result = tracker_module.filter_users_by_timezone(ti=mock_ti_with_data, **ctx)
        assert result == []
        assert mock_ti_with_data.xcom_pull(key="total_users_to_process") == 0

    def test_ist_hour_before_8_skips_all(self, tracker_module, mock_ti_with_data, airflow_context):
        # Mon 2025-01-27 01:00 UTC = Mon 06:30 IST → outside window
        ctx = airflow_context("2025-01-27T01:00:00")
        result = tracker_module.filter_users_by_timezone(ti=mock_ti_with_data, **ctx)
        assert result == []

    def test_ist_hour_12_still_in_window(self, tracker_module, mock_ti_with_data, airflow_context):
        # Mon 2025-01-27 06:30 UTC = Mon 12:00 IST → inside wider window [8,24)
        ctx = airflow_context("2025-01-27T06:30:00")
        result = tracker_module.filter_users_by_timezone(ti=mock_ti_with_data, **ctx)
        assert len(result) >= 1

    def test_ist_hour_after_12_still_in_window(self, tracker_module, mock_ti_with_data, airflow_context):
        # Mon 2025-01-27 08:00 UTC = Mon 13:30 IST → inside wider window [8,24)
        ctx = airflow_context("2025-01-27T08:00:00")
        result = tracker_module.filter_users_by_timezone(ti=mock_ti_with_data, **ctx)
        assert len(result) >= 1


class TestISTBoundary:
    """Exact boundary tests for the 8:00 AM - 11:59 PM IST window."""

    def test_mon_0759_ist_rejected(self, tracker_module, mock_ti_with_data, airflow_context):
        # Mon 07:59 IST = Mon 02:29 UTC
        ctx = airflow_context("2025-01-27T02:29:00")
        result = tracker_module.filter_users_by_timezone(ti=mock_ti_with_data, **ctx)
        assert result == []

    def test_mon_0800_ist_accepted(self, tracker_module, mock_ti_with_data, airflow_context):
        # Mon 08:00 IST = Mon 02:30 UTC → inside window
        ctx = airflow_context("2025-01-27T02:30:00")
        result = tracker_module.filter_users_by_timezone(ti=mock_ti_with_data, **ctx)
        # At least IST user should be included
        assert len(result) >= 1
        ist_user_included = any(u["timezone"] == "Asia/Kolkata" for u in result)
        assert ist_user_included

    def test_mon_1159_ist_accepted(self, tracker_module, mock_ti_with_data, airflow_context):
        # Mon 11:59 IST = Mon 06:29 UTC → still inside window
        ctx = airflow_context("2025-01-27T06:29:00")
        result = tracker_module.filter_users_by_timezone(ti=mock_ti_with_data, **ctx)
        assert len(result) >= 1


class TestCrossTimezone:
    """Cross-timezone filtering logic."""

    def test_utc_0230_ist_user_passes_us_user_skipped(self, tracker_module, mock_ti_with_data, airflow_context):
        # Mon 02:30 UTC = Mon 08:00 IST but Sun 21:30 EST → US user is still Sunday
        ctx = airflow_context("2025-01-27T02:30:00")
        result = tracker_module.filter_users_by_timezone(ti=mock_ti_with_data, **ctx)
        user_names = [u["name"] for u in result]
        assert "Alice" in user_names  # IST user
        assert "Dave" not in user_names  # EST user (still Sunday)

    def test_utc_1300_us_user_included(self, tracker_module, mock_ti_with_data, airflow_context):
        # Mon 13:00 UTC = Mon 08:00 EST → US user should pass local Monday 8 AM check
        # IST is 18:30 → inside wider [8,24) window → both users can be processed
        ctx = airflow_context("2025-01-27T13:00:00")
        result = tracker_module.filter_users_by_timezone(ti=mock_ti_with_data, **ctx)
        user_names = [u["name"] for u in result]
        # Both IST and US users should be included at this time
        assert "Alice" in user_names
        assert "Dave" in user_names

    def test_utc_0430_both_ist_and_us_users(self, tracker_module, airflow_context, mock_ti):
        """At Mon 04:30 UTC = Mon 10:00 IST (in window), but Mon 23:30 EST (Sun) —
        only IST user passes."""
        import pendulum
        users = copy.deepcopy(SAMPLE_USERS)
        for u in users:
            try:
                u["timezone_obj"] = pendulum.timezone(u["timezone"])
            except Exception:
                u["timezone_obj"] = pendulum.timezone("Asia/Kolkata")
        mock_ti.xcom_push(key="whitelisted_users", value=users)
        ctx = airflow_context("2025-01-27T04:30:00")
        result = tracker_module.filter_users_by_timezone(ti=mock_ti, **ctx)
        user_names = [u["name"] for u in result]
        assert "Alice" in user_names
        assert "Dave" not in user_names


class TestAlreadyInitiated:
    """Dedup: already-initiated user is skipped."""

    def test_already_initiated_user_skipped(self, tracker_module, mock_ti_with_data, airflow_context):
        from unittest.mock import patch as _patch
        # get_initiated_users_today uses pendulum.now() to build the XCom key,
        # so we mock it to return a time on 2025-01-27 in IST
        fake_now = pendulum.parse("2025-01-27T08:00:00", tz="Asia/Kolkata")
        # Pre-mark Alice as initiated using the key the function will generate
        mock_ti_with_data.xcom_push(key="initiated_users_2025-01-27_U001", value=True)
        ctx = airflow_context("2025-01-27T02:30:00")
        with _patch("pendulum.now", return_value=fake_now):
            result = tracker_module.filter_users_by_timezone(ti=mock_ti_with_data, **ctx)
        user_ids = [u["user_id"] for u in result]
        assert "U001" not in user_ids


class TestCustomDateRangeBypass:
    """When custom date range is provided via dag_run.conf, all checks are bypassed."""

    def test_custom_range_bypasses_not_monday(self, tracker_module, mock_ti_with_data, airflow_context):
        """On a Tuesday with custom_date_range=True, ALL users should be processed."""
        mock_ti_with_data.xcom_push(key="custom_date_range", value=True)
        # Tuesday 2025-01-28 08:30 UTC — would normally skip all users
        ctx = airflow_context("2025-01-28T08:30:00")
        result = tracker_module.filter_users_by_timezone(ti=mock_ti_with_data, **ctx)
        assert len(result) == 2
        user_names = [u["name"] for u in result]
        assert "Alice" in user_names
        assert "Dave" in user_names

    def test_custom_range_bypasses_ist_window(self, tracker_module, mock_ti_with_data, airflow_context):
        """Outside IST window with custom_date_range=True, ALL users should be processed."""
        mock_ti_with_data.xcom_push(key="custom_date_range", value=True)
        # Mon 2025-01-27 13:00 UTC = Mon 18:30 IST — outside 8-12 window
        ctx = airflow_context("2025-01-27T13:00:00")
        result = tracker_module.filter_users_by_timezone(ti=mock_ti_with_data, **ctx)
        assert len(result) == 2

    def test_no_custom_range_keeps_original_behavior(self, tracker_module, mock_ti_with_data, airflow_context):
        """Without custom_date_range, original IST/Monday checks apply."""
        mock_ti_with_data.xcom_push(key="custom_date_range", value=False)
        # Tuesday → should skip all
        ctx = airflow_context("2025-01-28T08:30:00")
        result = tracker_module.filter_users_by_timezone(ti=mock_ti_with_data, **ctx)
        assert result == []


class TestEdgeCases:
    """Empty/None user list and invalid timezone."""

    def test_empty_user_list(self, tracker_module, mock_ti, airflow_context):
        mock_ti.xcom_push(key="whitelisted_users", value=[])
        ctx = airflow_context("2025-01-27T02:30:00")
        result = tracker_module.filter_users_by_timezone(ti=mock_ti, **ctx)
        assert result == []

    def test_none_user_list(self, tracker_module, mock_ti, airflow_context):
        # No xcom push at all → xcom_pull returns None
        ctx = airflow_context("2025-01-27T02:30:00")
        result = tracker_module.filter_users_by_timezone(ti=mock_ti, **ctx)
        assert result == []

    def test_invalid_timezone_user_skipped(self, tracker_module, mock_ti, airflow_context):
        import pendulum
        users = [{
            "user_id": "U099", "mantis_email": "x@m.com", "name": "BadTZ",
            "email": "x@e.com", "timezone": "Invalid/Timezone",
        }]
        mock_ti.xcom_push(key="whitelisted_users", value=users)
        ctx = airflow_context("2025-01-27T02:30:00")
        result = tracker_module.filter_users_by_timezone(ti=mock_ti, **ctx)
        # Invalid timezone raises exception → user skipped via continue
        user_ids = [u["user_id"] for u in result]
        assert "U099" not in user_ids

    def test_pushes_users_to_process_xcom(self, tracker_module, mock_ti_with_data, airflow_context):
        ctx = airflow_context("2025-01-27T02:30:00")
        tracker_module.filter_users_by_timezone(ti=mock_ti_with_data, **ctx)
        val = mock_ti_with_data.xcom_pull(key="users_to_process")
        assert isinstance(val, list)
