"""Tests for Task 5: analyze_all_timesheets_with_ai, get_ai_response, extract_json_from_response."""
import copy
import json
import pytest
from unittest.mock import patch, MagicMock
from tests.conftest import SAMPLE_TIMESHEET_DATA, SAMPLE_AI_ANALYSIS


class TestGetAiResponse:
    """Tests for get_ai_response helper."""

    def _mock_chat(self, content):
        resp = MagicMock()
        resp.message.content = content
        return resp

    def test_strips_markdown_fences(self, tracker_module):
        raw = '```json\n{"key": "value"}\n```'
        mock_client = MagicMock()
        mock_client.return_value.chat.return_value = self._mock_chat(raw)
        with patch("tracker_weekly_report.Client", mock_client):
            result = tracker_module.get_ai_response("prompt")
        assert result == '{"key": "value"}'

    def test_expect_json_prepends_system_message(self, tracker_module):
        mock_client = MagicMock()
        mock_client.return_value.chat.return_value = self._mock_chat('{"ok": true}')
        with patch("tracker_weekly_report.Client", mock_client):
            tracker_module.get_ai_response("prompt", expect_json=True)
        call_args = mock_client.return_value.chat.call_args
        messages = call_args[1]["messages"] if "messages" in call_args[1] else call_args[0][0]
        # First message should be system
        assert messages[0]["role"] == "system"
        assert "JSON" in messages[0]["content"]

    def test_conversation_history_appended(self, tracker_module):
        mock_client = MagicMock()
        mock_client.return_value.chat.return_value = self._mock_chat("response")
        history = [{"prompt": "prev question", "response": "prev answer"}]
        with patch("tracker_weekly_report.Client", mock_client):
            tracker_module.get_ai_response("new prompt", conversation_history=history)
        call_args = mock_client.return_value.chat.call_args
        messages = call_args[1]["messages"] if "messages" in call_args[1] else call_args[0][0]
        # Should have history user + assistant + current user = 3 messages
        user_msgs = [m for m in messages if m["role"] == "user"]
        assert len(user_msgs) == 2

    def test_ollama_error_reraised(self, tracker_module):
        mock_client = MagicMock()
        mock_client.return_value.chat.side_effect = Exception("Ollama is down")
        with patch("tracker_weekly_report.Client", mock_client):
            with pytest.raises(Exception, match="Ollama is down"):
                tracker_module.get_ai_response("prompt")


class TestExtractJsonFromResponse:
    """Tests for extract_json_from_response helper."""

    def test_valid_json(self, tracker_module):
        text = '{"key": "value"}'
        result = tracker_module.extract_json_from_response(text)
        assert '"key"' in result

    def test_embedded_json_in_text(self, tracker_module):
        text = 'Some text before {"key": "value"} and after'
        result = tracker_module.extract_json_from_response(text)
        parsed = json.loads(result)
        assert parsed["key"] == "value"

    def test_no_json_returns_cleaned(self, tracker_module):
        text = "No JSON here at all"
        result = tracker_module.extract_json_from_response(text)
        assert isinstance(result, str)


class TestAnalyzeAllTimesheetsWithAi:
    """Tests for analyze_all_timesheets_with_ai DAG task."""

    def test_valid_ai_response(self, tracker_module, mock_ti, airflow_context):
        ts = copy.deepcopy(SAMPLE_TIMESHEET_DATA)
        ts["enriched"] = True
        mock_ti.xcom_push(key="enriched_timesheet_data_all", value=[ts])
        ctx = airflow_context("2025-01-27T02:30:00")

        ai_json = json.dumps(copy.deepcopy(SAMPLE_AI_ANALYSIS))
        with patch.object(tracker_module, "get_ai_response", return_value=ai_json):
            result = tracker_module.analyze_all_timesheets_with_ai(ti=mock_ti, **ctx)

        assert len(result) == 1
        assert "ratings" in result[0]
        assert "findings" in result[0]
        assert "recommendations" in result[0]
        assert "final_score" in result[0]

    def test_invalid_json_marks_failed(self, tracker_module, mock_ti, airflow_context):
        ts = copy.deepcopy(SAMPLE_TIMESHEET_DATA)
        ts["enriched"] = True
        mock_ti.xcom_push(key="enriched_timesheet_data_all", value=[ts])
        ctx = airflow_context("2025-01-27T02:30:00")

        with patch.object(tracker_module, "get_ai_response", return_value="not json at all!!!"):
            result = tracker_module.analyze_all_timesheets_with_ai(ti=mock_ti, **ctx)

        assert result[0].get("analysis_failed") is True

    def test_missing_required_field_marks_failed(self, tracker_module, mock_ti, airflow_context):
        ts = copy.deepcopy(SAMPLE_TIMESHEET_DATA)
        ts["enriched"] = True
        mock_ti.xcom_push(key="enriched_timesheet_data_all", value=[ts])
        ctx = airflow_context("2025-01-27T02:30:00")

        # Missing 'ratings' field
        incomplete = {"findings": {}, "recommendations": {}, "final_score": 3}
        with patch.object(tracker_module, "get_ai_response", return_value=json.dumps(incomplete)):
            result = tracker_module.analyze_all_timesheets_with_ai(ti=mock_ti, **ctx)

        assert result[0].get("analysis_failed") is True

    def test_errored_timesheet_skipped(self, tracker_module, mock_ti, airflow_context):
        ts = copy.deepcopy(SAMPLE_TIMESHEET_DATA)
        ts["error"] = "API failure"
        mock_ti.xcom_push(key="enriched_timesheet_data_all", value=[ts])
        ctx = airflow_context("2025-01-27T02:30:00")

        result = tracker_module.analyze_all_timesheets_with_ai(ti=mock_ti, **ctx)
        assert result[0].get("analysis_skipped") is True

    def test_empty_input(self, tracker_module, mock_ti, airflow_context):
        mock_ti.xcom_push(key="enriched_timesheet_data_all", value=[])
        ctx = airflow_context("2025-01-27T02:30:00")
        result = tracker_module.analyze_all_timesheets_with_ai(ti=mock_ti, **ctx)
        assert result == []

    @pytest.mark.parametrize("ai_response,expected_failed", [
        (json.dumps(SAMPLE_AI_ANALYSIS), False),
        ('{"findings": {}, "recommendations": {}, "final_score": 3}', True),
        ("absolutely not json", True),
        ("", True),
        # get_ai_response strips fences internally; since we mock it, return clean JSON
        (json.dumps(SAMPLE_AI_ANALYSIS), False),
    ], ids=["valid", "missing_ratings", "not_json", "empty", "with_fences"])
    def test_ai_response_variations(self, tracker_module, mock_ti, airflow_context,
                                    ai_response, expected_failed):
        ts = copy.deepcopy(SAMPLE_TIMESHEET_DATA)
        ts["enriched"] = True
        mock_ti.xcom_push(key="enriched_timesheet_data_all", value=[ts])
        ctx = airflow_context("2025-01-27T02:30:00")

        with patch.object(tracker_module, "get_ai_response", return_value=ai_response):
            result = tracker_module.analyze_all_timesheets_with_ai(ti=mock_ti, **ctx)

        if expected_failed:
            assert result[0].get("analysis_failed") is True or result[0].get("error") is not None
        else:
            assert "ratings" in result[0]
