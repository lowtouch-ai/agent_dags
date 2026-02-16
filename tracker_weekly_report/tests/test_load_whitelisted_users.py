"""Tests for Task 1: load_whitelisted_users."""
import copy
import json
import pytest
from unittest.mock import patch


class TestLoadWhitelistedUsersHappy:
    def test_loads_users_and_returns_count(self, tracker_module, mock_ti):
        result = tracker_module.load_whitelisted_users(ti=mock_ti)
        assert isinstance(result, int)
        assert result > 0

    def test_pushes_users_to_xcom(self, tracker_module, mock_ti):
        tracker_module.load_whitelisted_users(ti=mock_ti)
        users = mock_ti.xcom_pull(key="whitelisted_users")
        assert isinstance(users, list)
        assert len(users) > 0

    def test_pushes_total_users_to_xcom(self, tracker_module, mock_ti):
        count = tracker_module.load_whitelisted_users(ti=mock_ti)
        total = mock_ti.xcom_pull(key="total_users")
        assert total == count


class TestLoadWhitelistedUsersValidation:
    def test_missing_required_field_raises(self, tracker_module, mock_ti):
        bad_users = [{"user_id": "U099", "name": "NoEmail", "mantis_email": "a@b.com"}]
        with patch.object(tracker_module, "WHITELISTED_USERS", bad_users):
            with pytest.raises(ValueError, match="missing fields"):
                tracker_module.load_whitelisted_users(ti=mock_ti)

    def test_null_users_raises(self, tracker_module, mock_ti):
        with patch.object(tracker_module, "WHITELISTED_USERS", None):
            with pytest.raises(ValueError, match="not properly configured"):
                tracker_module.load_whitelisted_users(ti=mock_ti)

    def test_empty_list_raises(self, tracker_module, mock_ti):
        with patch.object(tracker_module, "WHITELISTED_USERS", []):
            with pytest.raises(ValueError, match="not properly configured"):
                tracker_module.load_whitelisted_users(ti=mock_ti)

    def test_wrong_type_raises(self, tracker_module, mock_ti):
        with patch.object(tracker_module, "WHITELISTED_USERS", "not-a-list"):
            with pytest.raises(ValueError, match="not properly configured"):
                tracker_module.load_whitelisted_users(ti=mock_ti)


class TestLoadWhitelistedUsersDefaults:
    def test_missing_hr_name_defaults(self, tracker_module, mock_ti):
        users = [{"user_id": "U010", "mantis_email": "x@m.com", "name": "X", "email": "x@e.com"}]
        with patch.object(tracker_module, "WHITELISTED_USERS", users):
            tracker_module.load_whitelisted_users(ti=mock_ti)
        loaded = mock_ti.xcom_pull(key="whitelisted_users")
        assert loaded[0]["hr_name"] == "HR_Manager"

    def test_missing_timezone_defaults_to_ist(self, tracker_module, mock_ti):
        users = [{"user_id": "U010", "mantis_email": "x@m.com", "name": "X", "email": "x@e.com"}]
        with patch.object(tracker_module, "WHITELISTED_USERS", users):
            tracker_module.load_whitelisted_users(ti=mock_ti)
        loaded = mock_ti.xcom_pull(key="whitelisted_users")
        assert loaded[0]["timezone"] == "Asia/Kolkata"

    @pytest.mark.parametrize("field,expected_default", [
        ("hr_email", None),
        ("manager_email", None),
        ("admin_email", None),
    ])
    def test_optional_field_defaults(self, tracker_module, mock_ti, field, expected_default):
        users = [{"user_id": "U010", "mantis_email": "x@m.com", "name": "X", "email": "x@e.com"}]
        with patch.object(tracker_module, "WHITELISTED_USERS", users):
            tracker_module.load_whitelisted_users(ti=mock_ti)
        loaded = mock_ti.xcom_pull(key="whitelisted_users")
        assert loaded[0][field] == expected_default
