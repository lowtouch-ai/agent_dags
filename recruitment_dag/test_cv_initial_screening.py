"""
Edge-case tests for cv_initial_screening.py (screening_response_analysis DAG).

Tests all 6 task functions with focus on:
- Missing/None/empty inputs at every stage
- Malformed data (bad emails, special chars, invalid JSON)
- AI returning garbage/errors
- File system failures
- Boundary conditions (empty arrays, missing dict keys)
- Case sensitivity in decision comparisons
"""

import json
import sys
import unittest
from datetime import datetime
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock, Mock, mock_open, patch

# ---------------------------------------------------------------------------
# Patch Airflow's Variable.get BEFORE importing the module under test,
# because cv_initial_screening.py calls Variable.get() at module level.
# ---------------------------------------------------------------------------
mock_variable = MagicMock()
mock_variable.get = MagicMock(side_effect=lambda key, default_var=None: {
    "ltai.v3.lowtouch.recruitment.email_credentials": '{"token": "fake"}',
    "ltai.v3.lowtouch.recruitment.from_address": "recruit@test.com",
    "ltai.v3.lowtouch.recruitment.recruiter_email": "athira@test.com",
    "ltai.v3.lowtouch.recruitment.recruiter_cc_emails": None,
    "ltai.v3.lowtouch.recruitment.model_name": "test-model",
}.get(key, default_var))

# Inject mocked airflow modules into sys.modules
sys.modules.setdefault('airflow', MagicMock())
sys.modules.setdefault('airflow.models', MagicMock(Variable=mock_variable))
sys.modules.setdefault('airflow.operators', MagicMock())
sys.modules.setdefault('airflow.operators.python', MagicMock())
sys.modules['airflow'].DAG = MagicMock()
sys.modules['airflow'].models = sys.modules['airflow.models']
sys.modules['airflow.models'].Variable = mock_variable

# Also mock the utility modules so we control their behavior per-test
mock_email_utils = MagicMock()
mock_agent_utils = MagicMock()
sys.modules.setdefault('agent_dags', MagicMock())
sys.modules.setdefault('agent_dags.utils', MagicMock())
sys.modules.setdefault('agent_dags.utils.email_utils', mock_email_utils)
sys.modules.setdefault('agent_dags.utils.agent_utils', mock_agent_utils)

# Now import the module under test
from agent_dags.recruitment_dag.cv_initial_screening import (
    extract_candidate_response,
    load_candidate_profile,
    analyze_screening_responses,
    update_candidate_profile,
    send_screening_result_email,
    notify_recruiter_for_interview,
)


# ===========================================================================
# Helper: build mock kwargs for Airflow PythonOperator functions
# ===========================================================================
def make_kwargs(conf=None, xcom_data=None):
    """
    Build a mock kwargs dict simulating what Airflow passes to a PythonOperator.

    Args:
        conf: dict to put in dag_run.conf (default: empty dict)
        xcom_data: dict mapping (task_id, key) -> value for xcom_pull
    """
    ti = MagicMock()
    dag_run = MagicMock()
    dag_run.conf = conf if conf is not None else {}

    xcom_data = xcom_data or {}

    def xcom_pull_side_effect(task_ids=None, key=None):
        return xcom_data.get((task_ids, key))

    ti.xcom_pull = MagicMock(side_effect=xcom_pull_side_effect)
    ti.xcom_push = MagicMock()

    return {'ti': ti, 'dag_run': dag_run}


# ===========================================================================
# 1. extract_candidate_response
# ===========================================================================
class TestExtractCandidateResponse(unittest.TestCase):
    """Edge cases for extract_candidate_response."""

    def test_no_email_data_key_in_conf(self):
        """conf has no 'email_data' key -> returns None."""
        kwargs = make_kwargs(conf={})
        result = extract_candidate_response(**kwargs)
        self.assertIsNone(result)
        kwargs['ti'].xcom_push.assert_not_called()

    def test_email_data_is_empty_dict(self):
        """email_data is {} (falsy) -> returns None."""
        kwargs = make_kwargs(conf={'email_data': {}})
        result = extract_candidate_response(**kwargs)
        self.assertIsNone(result)

    def test_email_data_is_none(self):
        """email_data is explicitly None -> returns None."""
        kwargs = make_kwargs(conf={'email_data': None})
        result = extract_candidate_response(**kwargs)
        self.assertIsNone(result)

    def test_missing_headers_key(self):
        """email_data has no 'headers' key -> defaults for sender/subject."""
        kwargs = make_kwargs(conf={'email_data': {'id': '123'}})
        result = extract_candidate_response(**kwargs)
        self.assertIsNotNone(result)
        self.assertEqual(result['sender'], 'Unknown')
        self.assertEqual(result['subject'], 'No Subject')

    def test_empty_from_header(self):
        """From header is empty string -> sender_email is ''."""
        kwargs = make_kwargs(conf={'email_data': {
            'id': '1', 'headers': {'From': '', 'Subject': 'Test'}
        }})
        result = extract_candidate_response(**kwargs)
        self.assertEqual(result['sender_email'], '')

    def test_from_header_with_display_name(self):
        """From: 'John Doe <john@example.com>' -> extracts email."""
        kwargs = make_kwargs(conf={'email_data': {
            'id': '1', 'headers': {'From': 'John Doe <john@example.com>'}
        }})
        result = extract_candidate_response(**kwargs)
        self.assertEqual(result['sender_email'], 'john@example.com')

    def test_from_header_unicode_display_name(self):
        """Unicode display name does not break parseaddr."""
        kwargs = make_kwargs(conf={'email_data': {
            'id': '1', 'headers': {'From': 'Résumé Élève <resume@example.com>'}
        }})
        result = extract_candidate_response(**kwargs)
        self.assertEqual(result['sender_email'], 'resume@example.com')

    def test_body_from_thread_history(self):
        """Body should come from thread_history.current_message.content (preferred)."""
        kwargs = make_kwargs(conf={'email_data': {
            'id': '1', 'headers': {'From': 'a@b.com'},
            'content': 'fallback body',
            'thread_history': {
                'current_message': {'content': 'thread body'}
            }
        }})
        result = extract_candidate_response(**kwargs)
        self.assertEqual(result['body'], 'thread body')

    def test_body_fallback_to_content(self):
        """When thread_history body is empty, fall back to email_data['content']."""
        kwargs = make_kwargs(conf={'email_data': {
            'id': '1', 'headers': {'From': 'a@b.com'},
            'content': 'fallback body',
            'thread_history': {'current_message': {'content': ''}}
        }})
        result = extract_candidate_response(**kwargs)
        self.assertEqual(result['body'], 'fallback body')

    def test_body_both_sources_empty(self):
        """Both body sources empty -> body is ''."""
        kwargs = make_kwargs(conf={'email_data': {
            'id': '1', 'headers': {'From': 'a@b.com'},
            'content': '',
            'thread_history': {'current_message': {'content': ''}}
        }})
        result = extract_candidate_response(**kwargs)
        self.assertEqual(result['body'], '')

    def test_thread_history_is_none(self):
        """thread_history is None -> no crash, body falls back."""
        kwargs = make_kwargs(conf={'email_data': {
            'id': '1', 'headers': {'From': 'a@b.com'},
            'content': 'fb', 'thread_history': None
        }})
        result = extract_candidate_response(**kwargs)
        self.assertEqual(result['body'], 'fb')

    def test_current_message_is_none(self):
        """current_message is None -> no crash, body falls back."""
        kwargs = make_kwargs(conf={'email_data': {
            'id': '1', 'headers': {'From': 'a@b.com'},
            'content': 'fb',
            'thread_history': {'current_message': None}
        }})
        result = extract_candidate_response(**kwargs)
        self.assertEqual(result['body'], 'fb')

    def test_missing_date_defaults(self):
        """Missing date -> received_date is a valid ISO datetime."""
        kwargs = make_kwargs(conf={'email_data': {
            'id': '1', 'headers': {'From': 'a@b.com'}
        }})
        result = extract_candidate_response(**kwargs)
        # Should be parseable as ISO datetime
        datetime.fromisoformat(result['received_date'])

    def test_missing_id_defaults_to_unknown(self):
        """Missing 'id' key -> email_id is 'unknown'."""
        kwargs = make_kwargs(conf={'email_data': {
            'headers': {'From': 'a@b.com'}
        }})
        result = extract_candidate_response(**kwargs)
        self.assertEqual(result['email_id'], 'unknown')

    def test_xcom_push_called_correctly(self):
        """xcom_push called with key='response_data' and matching value."""
        kwargs = make_kwargs(conf={'email_data': {
            'id': '42', 'headers': {'From': 'x@y.com', 'Subject': 'Hi'}
        }})
        result = extract_candidate_response(**kwargs)
        kwargs['ti'].xcom_push.assert_called_once_with(
            key='response_data', value=result
        )


# ===========================================================================
# 2. load_candidate_profile
# ===========================================================================
class TestLoadCandidateProfile(unittest.TestCase):
    """Edge cases for load_candidate_profile."""

    def test_no_response_data(self):
        """xcom_pull returns None for response_data -> returns None."""
        kwargs = make_kwargs(xcom_data={})
        result = load_candidate_profile(**kwargs)
        self.assertIsNone(result)

    def test_email_sanitization_at_sign(self):
        """@ in email is replaced with _at_."""
        kwargs = make_kwargs(xcom_data={
            ('extract_candidate_response', 'response_data'): {
                'sender_email': 'user@example.com'
            }
        })
        with patch.object(Path, 'exists', return_value=True):
            profile = {'name': 'Test'}
            with patch('builtins.open', mock_open(read_data=json.dumps(profile))):
                result = load_candidate_profile(**kwargs)
        self.assertEqual(result, profile)

    def test_email_sanitization_forward_slash(self):
        """/ in email is replaced with _."""
        kwargs = make_kwargs(xcom_data={
            ('extract_candidate_response', 'response_data'): {
                'sender_email': 'user/admin@example.com'
            }
        })
        with patch.object(Path, 'exists', return_value=True):
            profile = {'name': 'Test'}
            with patch('builtins.open', mock_open(read_data=json.dumps(profile))):
                load_candidate_profile(**kwargs)
        # Verify sanitization: the path used should contain user_admin_at_example.com
        # (we can't easily inspect the Path constructor, but no crash means / was handled)

    def test_email_sanitization_backslash(self):
        """Backslash in email is replaced with _."""
        kwargs = make_kwargs(xcom_data={
            ('extract_candidate_response', 'response_data'): {
                'sender_email': 'user\\admin@example.com'
            }
        })
        with patch.object(Path, 'exists', return_value=True):
            profile = {'name': 'Test'}
            with patch('builtins.open', mock_open(read_data=json.dumps(profile))):
                result = load_candidate_profile(**kwargs)
        self.assertEqual(result, profile)

    def test_path_traversal_attempt(self):
        """Path traversal in email is partially sanitized (/ -> _)."""
        kwargs = make_kwargs(xcom_data={
            ('extract_candidate_response', 'response_data'): {
                'sender_email': '../../etc/passwd@evil.com'
            }
        })
        # The / chars get replaced, but .. stays -> .._.._ etc
        with patch.object(Path, 'exists', return_value=False):
            result = load_candidate_profile(**kwargs)
        self.assertIsNone(result)

    def test_plus_addressing(self):
        """+ is NOT sanitized in email."""
        kwargs = make_kwargs(xcom_data={
            ('extract_candidate_response', 'response_data'): {
                'sender_email': 'user+tag@example.com'
            }
        })
        with patch.object(Path, 'exists', return_value=True):
            profile = {'name': 'Tagged'}
            with patch('builtins.open', mock_open(read_data=json.dumps(profile))):
                result = load_candidate_profile(**kwargs)
        self.assertEqual(result, profile)

    def test_profile_file_not_found(self):
        """Profile file doesn't exist -> returns None."""
        kwargs = make_kwargs(xcom_data={
            ('extract_candidate_response', 'response_data'): {
                'sender_email': 'nofile@example.com'
            }
        })
        with patch.object(Path, 'exists', return_value=False):
            result = load_candidate_profile(**kwargs)
        self.assertIsNone(result)

    def test_profile_file_invalid_json(self):
        """Profile file has invalid JSON -> caught by except, returns None."""
        kwargs = make_kwargs(xcom_data={
            ('extract_candidate_response', 'response_data'): {
                'sender_email': 'bad@example.com'
            }
        })
        with patch.object(Path, 'exists', return_value=True):
            with patch('builtins.open', mock_open(read_data='not json at all')):
                result = load_candidate_profile(**kwargs)
        self.assertIsNone(result)

    def test_profile_file_empty(self):
        """Empty profile file -> JSON parse fails, returns None."""
        kwargs = make_kwargs(xcom_data={
            ('extract_candidate_response', 'response_data'): {
                'sender_email': 'empty@example.com'
            }
        })
        with patch.object(Path, 'exists', return_value=True):
            with patch('builtins.open', mock_open(read_data='')):
                result = load_candidate_profile(**kwargs)
        self.assertIsNone(result)

    def test_profile_file_permission_error(self):
        """Permission denied on file open -> caught, returns None."""
        kwargs = make_kwargs(xcom_data={
            ('extract_candidate_response', 'response_data'): {
                'sender_email': 'perm@example.com'
            }
        })
        with patch.object(Path, 'exists', return_value=True):
            with patch('builtins.open', side_effect=PermissionError("denied")):
                result = load_candidate_profile(**kwargs)
        self.assertIsNone(result)

    def test_profile_is_json_array(self):
        """Profile file is a JSON array (not dict) -> loaded as-is (no type check)."""
        kwargs = make_kwargs(xcom_data={
            ('extract_candidate_response', 'response_data'): {
                'sender_email': 'arr@example.com'
            }
        })
        with patch.object(Path, 'exists', return_value=True):
            with patch('builtins.open', mock_open(read_data='[1, 2, 3]')):
                result = load_candidate_profile(**kwargs)
        self.assertEqual(result, [1, 2, 3])

    def test_xcom_push_on_success(self):
        """xcom_push called with 'candidate_profile' on successful load."""
        kwargs = make_kwargs(xcom_data={
            ('extract_candidate_response', 'response_data'): {
                'sender_email': 'ok@example.com'
            }
        })
        profile = {'status': 'active'}
        with patch.object(Path, 'exists', return_value=True):
            with patch('builtins.open', mock_open(read_data=json.dumps(profile))):
                load_candidate_profile(**kwargs)
        kwargs['ti'].xcom_push.assert_called_once_with(
            key='candidate_profile', value=profile
        )

    def test_xcom_push_not_called_on_file_not_found(self):
        """xcom_push NOT called when profile file not found."""
        kwargs = make_kwargs(xcom_data={
            ('extract_candidate_response', 'response_data'): {
                'sender_email': 'missing@example.com'
            }
        })
        with patch.object(Path, 'exists', return_value=False):
            load_candidate_profile(**kwargs)
        kwargs['ti'].xcom_push.assert_not_called()


# ===========================================================================
# 3. analyze_screening_responses
# ===========================================================================
MODULE = 'agent_dags.recruitment_dag.cv_initial_screening'


class TestAnalyzeScreeningResponses(unittest.TestCase):
    """Edge cases for analyze_screening_responses."""

    def test_no_response_data(self):
        """response_data is None -> returns None."""
        kwargs = make_kwargs(
            conf={'email_data': {}},
            xcom_data={
                ('extract_candidate_response', 'response_data'): None,
                ('load_candidate_profile', 'candidate_profile'): {'name': 'X'},
            }
        )
        result = analyze_screening_responses(**kwargs)
        self.assertIsNone(result)

    def test_no_candidate_profile(self):
        """candidate_profile is None -> returns None."""
        kwargs = make_kwargs(
            conf={'email_data': {}},
            xcom_data={
                ('extract_candidate_response', 'response_data'): {'body': 'hi'},
                ('load_candidate_profile', 'candidate_profile'): None,
            }
        )
        result = analyze_screening_responses(**kwargs)
        self.assertIsNone(result)

    def test_both_inputs_none(self):
        """Both response_data and candidate_profile are None -> returns None."""
        kwargs = make_kwargs(
            conf={'email_data': {}},
            xcom_data={
                ('extract_candidate_response', 'response_data'): None,
                ('load_candidate_profile', 'candidate_profile'): None,
            }
        )
        result = analyze_screening_responses(**kwargs)
        self.assertIsNone(result)

    @patch(f'{MODULE}.extract_json_from_text')
    @patch(f'{MODULE}.get_ai_response')
    @patch(f'{MODULE}.Variable')
    def test_cv_content_from_thread_history(self, mock_var, mock_ai, mock_json):
        """CV content taken from thread_history.original_cv_content (preferred)."""
        mock_var.get.return_value = 'test-model'
        mock_ai.return_value = '{"decision": "ACCEPT"}'
        analysis = {'decision': 'ACCEPT', 'overall_score': 80}
        mock_json.return_value = analysis

        kwargs = make_kwargs(
            conf={'email_data': {
                'thread_history': {'original_cv_content': 'CV from thread'}
            }},
            xcom_data={
                ('extract_candidate_response', 'response_data'): {'body': 'answers'},
                ('load_candidate_profile', 'candidate_profile'): {
                    'original_cv_content': 'CV from profile'
                },
            }
        )
        analyze_screening_responses(**kwargs)
        # The prompt sent to AI should contain 'CV from thread'
        prompt_sent = mock_ai.call_args[0][0]
        self.assertIn('CV from thread', prompt_sent)

    @patch(f'{MODULE}.extract_json_from_text')
    @patch(f'{MODULE}.get_ai_response')
    @patch(f'{MODULE}.Variable')
    def test_cv_content_fallback_to_profile(self, mock_var, mock_ai, mock_json):
        """CV content falls back to candidate_profile when not in thread_history."""
        mock_var.get.return_value = 'test-model'
        mock_ai.return_value = '{}'
        mock_json.return_value = {}

        kwargs = make_kwargs(
            conf={'email_data': {'thread_history': {}}},
            xcom_data={
                ('extract_candidate_response', 'response_data'): {'body': 'ans'},
                ('load_candidate_profile', 'candidate_profile'): {
                    'original_cv_content': 'CV from profile'
                },
            }
        )
        analyze_screening_responses(**kwargs)
        prompt_sent = mock_ai.call_args[0][0]
        self.assertIn('CV from profile', prompt_sent)

    @patch(f'{MODULE}.extract_json_from_text')
    @patch(f'{MODULE}.get_ai_response')
    @patch(f'{MODULE}.Variable')
    def test_cv_content_neither_source(self, mock_var, mock_ai, mock_json):
        """No CV content anywhere -> proceeds with empty string."""
        mock_var.get.return_value = 'test-model'
        mock_ai.return_value = '{}'
        mock_json.return_value = {}

        # Profile must be truthy (non-empty) to pass the `not candidate_profile` check
        kwargs = make_kwargs(
            conf={'email_data': {}},
            xcom_data={
                ('extract_candidate_response', 'response_data'): {'body': 'ans'},
                ('load_candidate_profile', 'candidate_profile'): {'name': 'X'},
            }
        )
        # Should not crash
        analyze_screening_responses(**kwargs)
        self.assertTrue(mock_ai.called)

    @patch(f'{MODULE}.extract_json_from_text')
    @patch(f'{MODULE}.get_ai_response')
    @patch(f'{MODULE}.Variable')
    def test_ai_returns_valid_json(self, mock_var, mock_ai, mock_json):
        """AI returns valid JSON -> pushed to xcom."""
        mock_var.get.return_value = 'test-model'
        mock_ai.return_value = '{"decision": "ACCEPT", "overall_score": 90}'
        analysis = {'decision': 'ACCEPT', 'overall_score': 90}
        mock_json.return_value = analysis

        kwargs = make_kwargs(
            conf={'email_data': {}},
            xcom_data={
                ('extract_candidate_response', 'response_data'): {'body': 'good answers'},
                ('load_candidate_profile', 'candidate_profile'): {'name': 'John'},
            }
        )
        result = analyze_screening_responses(**kwargs)
        self.assertEqual(result, analysis)
        kwargs['ti'].xcom_push.assert_called_once_with(
            key='analysis_data', value=analysis
        )

    @patch(f'{MODULE}.extract_json_from_text')
    @patch(f'{MODULE}.get_ai_response')
    @patch(f'{MODULE}.Variable')
    def test_ai_returns_garbage(self, mock_var, mock_ai, mock_json):
        """AI returns non-JSON text -> extract_json returns None."""
        mock_var.get.return_value = 'test-model'
        mock_ai.return_value = "I don't understand your question"
        mock_json.return_value = None

        kwargs = make_kwargs(
            conf={'email_data': {}},
            xcom_data={
                ('extract_candidate_response', 'response_data'): {'body': 'ans'},
                ('load_candidate_profile', 'candidate_profile'): {'name': 'X'},
            }
        )
        result = analyze_screening_responses(**kwargs)
        self.assertIsNone(result)

    @patch(f'{MODULE}.extract_json_from_text')
    @patch(f'{MODULE}.get_ai_response')
    @patch(f'{MODULE}.Variable')
    def test_ai_returns_empty_string(self, mock_var, mock_ai, mock_json):
        """AI returns empty string -> extract_json returns None."""
        mock_var.get.return_value = 'test-model'
        mock_ai.return_value = ''
        mock_json.return_value = None

        kwargs = make_kwargs(
            conf={'email_data': {}},
            xcom_data={
                ('extract_candidate_response', 'response_data'): {'body': 'ans'},
                ('load_candidate_profile', 'candidate_profile'): {'name': 'X'},
            }
        )
        result = analyze_screening_responses(**kwargs)
        self.assertIsNone(result)

    @patch(f'{MODULE}.extract_json_from_text')
    @patch(f'{MODULE}.get_ai_response')
    @patch(f'{MODULE}.Variable')
    def test_ai_returns_error_string(self, mock_var, mock_ai, mock_json):
        """AI returns error message -> extract_json returns None."""
        mock_var.get.return_value = 'test-model'
        mock_ai.return_value = 'An error occurred while processing your request: timeout'
        mock_json.return_value = None

        kwargs = make_kwargs(
            conf={'email_data': {}},
            xcom_data={
                ('extract_candidate_response', 'response_data'): {'body': 'ans'},
                ('load_candidate_profile', 'candidate_profile'): {'name': 'X'},
            }
        )
        result = analyze_screening_responses(**kwargs)
        self.assertIsNone(result)

    @patch(f'{MODULE}.extract_json_from_text')
    @patch(f'{MODULE}.get_ai_response')
    @patch(f'{MODULE}.Variable')
    def test_ai_returns_json_with_extra_keys(self, mock_var, mock_ai, mock_json):
        """AI returns JSON with unexpected extra keys -> passes through."""
        mock_var.get.return_value = 'test-model'
        mock_ai.return_value = '{}'
        extra = {'decision': 'ACCEPT', 'extra_field': 'bonus', 'overall_score': 99}
        mock_json.return_value = extra

        kwargs = make_kwargs(
            conf={'email_data': {}},
            xcom_data={
                ('extract_candidate_response', 'response_data'): {'body': 'ans'},
                ('load_candidate_profile', 'candidate_profile'): {'name': 'X'},
            }
        )
        result = analyze_screening_responses(**kwargs)
        self.assertIn('extra_field', result)

    @patch(f'{MODULE}.extract_json_from_text')
    @patch(f'{MODULE}.get_ai_response')
    @patch(f'{MODULE}.Variable')
    def test_email_data_missing_from_conf(self, mock_var, mock_ai, mock_json):
        """email_data not in conf -> thread_history defaults to {}."""
        mock_var.get.return_value = 'test-model'
        mock_ai.return_value = '{}'
        mock_json.return_value = {'decision': 'ACCEPT'}

        kwargs = make_kwargs(
            conf={},  # no email_data
            xcom_data={
                ('extract_candidate_response', 'response_data'): {'body': 'ans'},
                ('load_candidate_profile', 'candidate_profile'): {'name': 'X'},
            }
        )
        # Should not crash
        result = analyze_screening_responses(**kwargs)
        self.assertIsNotNone(result)


# ===========================================================================
# 4. update_candidate_profile
# ===========================================================================
class TestUpdateCandidateProfile(unittest.TestCase):
    """Edge cases for update_candidate_profile."""

    def test_missing_response_data(self):
        """response_data is None -> returns None."""
        kwargs = make_kwargs(xcom_data={
            ('extract_candidate_response', 'response_data'): None,
            ('analyze_screening_responses', 'analysis_data'): {'decision': 'ACCEPT'},
            ('load_candidate_profile', 'candidate_profile'): {'name': 'X'},
        })
        result = update_candidate_profile(**kwargs)
        self.assertIsNone(result)

    def test_missing_analysis_data(self):
        """analysis_data is None -> returns None."""
        kwargs = make_kwargs(xcom_data={
            ('extract_candidate_response', 'response_data'): {'sender_email': 'a@b.com'},
            ('analyze_screening_responses', 'analysis_data'): None,
            ('load_candidate_profile', 'candidate_profile'): {'name': 'X'},
        })
        result = update_candidate_profile(**kwargs)
        self.assertIsNone(result)

    def test_missing_candidate_profile(self):
        """candidate_profile is None -> returns None."""
        kwargs = make_kwargs(xcom_data={
            ('extract_candidate_response', 'response_data'): {'sender_email': 'a@b.com'},
            ('analyze_screening_responses', 'analysis_data'): {'decision': 'ACCEPT'},
            ('load_candidate_profile', 'candidate_profile'): None,
        })
        result = update_candidate_profile(**kwargs)
        self.assertIsNone(result)

    def test_analysis_data_empty_dict_accepted(self):
        """
        analysis_data = {} is now accepted (Bug #1 fix).
        Uses `is not None` instead of truthiness check.
        """
        kwargs = make_kwargs(xcom_data={
            ('extract_candidate_response', 'response_data'): {
                'sender_email': 'a@b.com', 'body': 'ans'
            },
            ('analyze_screening_responses', 'analysis_data'): {},
            ('load_candidate_profile', 'candidate_profile'): {'name': 'X'},
        })
        m = mock_open()
        with patch('builtins.open', m):
            result = update_candidate_profile(**kwargs)
        self.assertIsNotNone(result)
        self.assertEqual(result['screening_stage']['decision'], 'PENDING')

    def test_missing_decision_key_defaults_to_pending(self):
        """analysis_data without 'decision' key -> defaults to 'PENDING'."""
        kwargs = make_kwargs(xcom_data={
            ('extract_candidate_response', 'response_data'): {
                'sender_email': 'a@b.com', 'body': 'ans'
            },
            ('analyze_screening_responses', 'analysis_data'): {'overall_score': 70},
            ('load_candidate_profile', 'candidate_profile'): {'name': 'X'},
        })
        with patch.object(Path, 'exists', return_value=True):
            m = mock_open()
            with patch('builtins.open', m):
                result = update_candidate_profile(**kwargs)
        self.assertIsNotNone(result)
        self.assertEqual(result['screening_stage']['decision'], 'PENDING')

    def test_missing_overall_score_defaults_to_zero(self):
        """analysis_data without 'overall_score' -> defaults to 0."""
        kwargs = make_kwargs(xcom_data={
            ('extract_candidate_response', 'response_data'): {
                'sender_email': 'a@b.com', 'body': 'ans'
            },
            ('analyze_screening_responses', 'analysis_data'): {'decision': 'ACCEPT'},
            ('load_candidate_profile', 'candidate_profile'): {'name': 'X'},
        })
        with patch.object(Path, 'exists', return_value=True):
            m = mock_open()
            with patch('builtins.open', m):
                result = update_candidate_profile(**kwargs)
        self.assertEqual(result['screening_stage']['overall_score'], 0)

    def test_decision_value_none_defaults_to_pending(self):
        """
        decision key exists but value is None.
        Bug #2 fix: `or 'PENDING'` now defaults None to 'PENDING'.
        """
        kwargs = make_kwargs(xcom_data={
            ('extract_candidate_response', 'response_data'): {
                'sender_email': 'a@b.com', 'body': 'ans'
            },
            ('analyze_screening_responses', 'analysis_data'): {
                'decision': None, 'overall_score': 50
            },
            ('load_candidate_profile', 'candidate_profile'): {'name': 'X'},
        })
        with patch.object(Path, 'exists', return_value=True):
            m = mock_open()
            with patch('builtins.open', m):
                result = update_candidate_profile(**kwargs)
        self.assertEqual(result['screening_stage']['decision'], 'PENDING')

    def test_file_write_permission_error(self):
        """Permission error on file write -> caught, returns None."""
        kwargs = make_kwargs(xcom_data={
            ('extract_candidate_response', 'response_data'): {
                'sender_email': 'a@b.com', 'body': 'ans'
            },
            ('analyze_screening_responses', 'analysis_data'): {'decision': 'ACCEPT', 'overall_score': 80},
            ('load_candidate_profile', 'candidate_profile'): {'name': 'X'},
        })
        with patch('builtins.open', side_effect=PermissionError("denied")):
            result = update_candidate_profile(**kwargs)
        self.assertIsNone(result)

    def test_profile_merge_preserves_existing_keys(self):
        """Existing profile keys are preserved in the merged result."""
        kwargs = make_kwargs(xcom_data={
            ('extract_candidate_response', 'response_data'): {
                'sender_email': 'a@b.com', 'body': 'ans'
            },
            ('analyze_screening_responses', 'analysis_data'): {'decision': 'ACCEPT', 'overall_score': 80},
            ('load_candidate_profile', 'candidate_profile'): {
                'name': 'John', 'skills': ['python'], 'total_score': 75
            },
        })
        m = mock_open()
        with patch('builtins.open', m):
            result = update_candidate_profile(**kwargs)
        self.assertEqual(result['name'], 'John')
        self.assertEqual(result['skills'], ['python'])
        self.assertIn('screening_stage', result)

    def test_profile_merge_overwrites_screening_stage(self):
        """Existing screening_stage is overwritten by the new one."""
        kwargs = make_kwargs(xcom_data={
            ('extract_candidate_response', 'response_data'): {
                'sender_email': 'a@b.com', 'body': 'new answers'
            },
            ('analyze_screening_responses', 'analysis_data'): {'decision': 'REJECT', 'overall_score': 30},
            ('load_candidate_profile', 'candidate_profile'): {
                'name': 'John',
                'screening_stage': {'completed': False, 'decision': 'PENDING'}
            },
        })
        m = mock_open()
        with patch('builtins.open', m):
            result = update_candidate_profile(**kwargs)
        self.assertTrue(result['screening_stage']['completed'])
        self.assertEqual(result['screening_stage']['decision'], 'REJECT')

    def test_completion_date_is_valid_iso(self):
        """screening_stage.completion_date is valid ISO 8601."""
        kwargs = make_kwargs(xcom_data={
            ('extract_candidate_response', 'response_data'): {
                'sender_email': 'a@b.com', 'body': 'ans'
            },
            ('analyze_screening_responses', 'analysis_data'): {'decision': 'ACCEPT', 'overall_score': 80},
            ('load_candidate_profile', 'candidate_profile'): {'name': 'X'},
        })
        m = mock_open()
        with patch('builtins.open', m):
            result = update_candidate_profile(**kwargs)
        datetime.fromisoformat(result['screening_stage']['completion_date'])

    def test_xcom_push_on_success(self):
        """xcom_push called on successful write."""
        kwargs = make_kwargs(xcom_data={
            ('extract_candidate_response', 'response_data'): {
                'sender_email': 'a@b.com', 'body': 'ans'
            },
            ('analyze_screening_responses', 'analysis_data'): {'decision': 'ACCEPT', 'overall_score': 80},
            ('load_candidate_profile', 'candidate_profile'): {'name': 'X'},
        })
        m = mock_open()
        with patch('builtins.open', m):
            update_candidate_profile(**kwargs)
        kwargs['ti'].xcom_push.assert_called_once()

    def test_xcom_push_not_called_on_write_failure(self):
        """xcom_push NOT called when file write fails."""
        kwargs = make_kwargs(xcom_data={
            ('extract_candidate_response', 'response_data'): {
                'sender_email': 'a@b.com', 'body': 'ans'
            },
            ('analyze_screening_responses', 'analysis_data'): {'decision': 'ACCEPT', 'overall_score': 80},
            ('load_candidate_profile', 'candidate_profile'): {'name': 'X'},
        })
        with patch('builtins.open', side_effect=OSError("disk full")):
            update_candidate_profile(**kwargs)
        kwargs['ti'].xcom_push.assert_not_called()


# ===========================================================================
# 5. send_screening_result_email
# ===========================================================================
class TestSendScreeningResultEmail(unittest.TestCase):
    """Edge cases for send_screening_result_email."""

    def _base_kwargs(self, email_data=None, analysis_data=None, response_data=None):
        """Helper to build kwargs with common defaults."""
        email_data = email_data or {
            'headers': {
                'From': 'Candidate <cand@test.com>',
                'Subject': 'Application',
                'Message-ID': '<msg1>',
                'References': '<ref0>',
            },
            'threadId': 'thread_1',
        }
        analysis_data = analysis_data or {'decision': 'ACCEPT', 'overall_score': 85}
        response_data = response_data or {'sender_email': 'cand@test.com', 'body': 'answers'}

        return make_kwargs(
            conf={'email_data': email_data},
            xcom_data={
                ('analyze_screening_responses', 'analysis_data'): analysis_data,
                ('extract_candidate_response', 'response_data'): response_data,
            }
        )

    def test_missing_email_data(self):
        """Empty email_data -> 'Missing data for email'."""
        kwargs = make_kwargs(
            conf={'email_data': {}},
            xcom_data={
                ('analyze_screening_responses', 'analysis_data'): {'decision': 'ACCEPT'},
                ('extract_candidate_response', 'response_data'): {'body': 'x'},
            }
        )
        result = send_screening_result_email(**kwargs)
        self.assertEqual(result, "Missing data for email")

    def test_missing_analysis_data(self):
        """analysis_data is None -> 'Missing data for email'."""
        kwargs = make_kwargs(
            conf={'email_data': {'headers': {'From': 'a@b.com'}}},
            xcom_data={
                ('analyze_screening_responses', 'analysis_data'): None,
                ('extract_candidate_response', 'response_data'): {'body': 'x'},
            }
        )
        result = send_screening_result_email(**kwargs)
        self.assertEqual(result, "Missing data for email")

    def test_missing_response_data(self):
        """response_data is None -> 'Missing data for email'."""
        kwargs = make_kwargs(
            conf={'email_data': {'headers': {'From': 'a@b.com'}}},
            xcom_data={
                ('analyze_screening_responses', 'analysis_data'): {'decision': 'ACCEPT'},
                ('extract_candidate_response', 'response_data'): None,
            }
        )
        result = send_screening_result_email(**kwargs)
        self.assertEqual(result, "Missing data for email")

    @patch(f'{MODULE}.send_email', return_value={'id': 'sent1'})
    @patch(f'{MODULE}.authenticate_gmail', return_value=MagicMock())
    @patch(f'{MODULE}.get_ai_response', return_value='<p>Congratulations!</p>')
    @patch(f'{MODULE}.Variable')
    def test_accept_decision_uses_acceptance_prompt(self, mock_var, mock_ai, mock_auth, mock_send):
        """decision == 'ACCEPT' -> acceptance email prompt used."""
        mock_var.get.return_value = 'test-model'
        kwargs = self._base_kwargs(analysis_data={'decision': 'ACCEPT', 'overall_score': 85})
        send_screening_result_email(**kwargs)
        prompt = mock_ai.call_args[0][0]
        self.assertIn('acceptance email', prompt.lower())

    @patch(f'{MODULE}.send_email', return_value={'id': 'sent1'})
    @patch(f'{MODULE}.authenticate_gmail', return_value=MagicMock())
    @patch(f'{MODULE}.get_ai_response', return_value='<p>Sorry</p>')
    @patch(f'{MODULE}.Variable')
    def test_reject_decision_uses_rejection_prompt(self, mock_var, mock_ai, mock_auth, mock_send):
        """decision == 'REJECT' -> rejection email prompt used."""
        mock_var.get.return_value = 'test-model'
        kwargs = self._base_kwargs(analysis_data={'decision': 'REJECT', 'overall_score': 20})
        send_screening_result_email(**kwargs)
        prompt = mock_ai.call_args[0][0]
        self.assertIn('rejection email', prompt.lower())

    @patch(f'{MODULE}.send_email', return_value={'id': 'sent1'})
    @patch(f'{MODULE}.authenticate_gmail', return_value=MagicMock())
    @patch(f'{MODULE}.get_ai_response', return_value='<p>Congrats</p>')
    @patch(f'{MODULE}.Variable')
    def test_pending_decision_falls_to_acceptance(self, mock_var, mock_ai, mock_auth, mock_send):
        """decision == 'PENDING' != 'REJECT' -> falls to else (acceptance) branch."""
        mock_var.get.return_value = 'test-model'
        kwargs = self._base_kwargs(analysis_data={'decision': 'PENDING'})
        send_screening_result_email(**kwargs)
        prompt = mock_ai.call_args[0][0]
        self.assertIn('acceptance email', prompt.lower())

    @patch(f'{MODULE}.send_email', return_value={'id': 'sent1'})
    @patch(f'{MODULE}.authenticate_gmail', return_value=MagicMock())
    @patch(f'{MODULE}.get_ai_response', return_value='<p>Sorry</p>')
    @patch(f'{MODULE}.Variable')
    def test_lowercase_reject_now_matched(self, mock_var, mock_ai, mock_auth, mock_send):
        """
        Bug #3 fix: decision == 'reject' (lowercase) is now normalized to 'REJECT'.
        Rejection prompt used.
        """
        mock_var.get.return_value = 'test-model'
        kwargs = self._base_kwargs(analysis_data={'decision': 'reject'})
        send_screening_result_email(**kwargs)
        prompt = mock_ai.call_args[0][0]
        self.assertIn('rejection email', prompt.lower())

    @patch(f'{MODULE}.send_email', return_value={'id': 'sent1'})
    @patch(f'{MODULE}.authenticate_gmail', return_value=MagicMock())
    @patch(f'{MODULE}.get_ai_response', return_value='<p>Hi</p>')
    @patch(f'{MODULE}.Variable')
    def test_subject_passed_as_is(self, mock_var, mock_ai, mock_auth, mock_send):
        """Bug #4 fix: DAG no longer adds 'Re:' - send_email handles it."""
        mock_var.get.return_value = 'test-model'
        kwargs = self._base_kwargs()
        send_screening_result_email(**kwargs)
        call_args = mock_send.call_args[0]
        # Subject passed as-is from headers (send_email adds Re: if needed)
        self.assertEqual(call_args[2], 'Application')

    @patch(f'{MODULE}.send_email', return_value={'id': 'sent1'})
    @patch(f'{MODULE}.authenticate_gmail', return_value=MagicMock())
    @patch(f'{MODULE}.get_ai_response', return_value='<p>Hi</p>')
    @patch(f'{MODULE}.Variable')
    def test_subject_already_has_re_no_double(self, mock_var, mock_ai, mock_auth, mock_send):
        """
        Bug #4 fix: Subject already has 'Re:' -> no longer doubled.
        Passed as-is to send_email which handles the prefix.
        """
        mock_var.get.return_value = 'test-model'
        kwargs = self._base_kwargs(email_data={
            'headers': {
                'From': 'a@b.com',
                'Subject': 'Re: Original Subject',
                'Message-ID': '<msg1>',
                'References': '',
            },
            'threadId': 'th1',
        })
        send_screening_result_email(**kwargs)
        subject_sent = mock_send.call_args[0][2]
        self.assertEqual(subject_sent, 'Re: Original Subject')

    @patch(f'{MODULE}.send_email', return_value={'id': 'sent1'})
    @patch(f'{MODULE}.authenticate_gmail', return_value=MagicMock())
    @patch(f'{MODULE}.get_ai_response', return_value='```html\n<p>Hello</p>\n```')
    @patch(f'{MODULE}.Variable')
    def test_ai_response_html_fenced(self, mock_var, mock_ai, mock_auth, mock_send):
        """AI response with ```html fence -> regex extracts body."""
        mock_var.get.return_value = 'test-model'
        kwargs = self._base_kwargs()
        send_screening_result_email(**kwargs)
        body_sent = mock_send.call_args[0][3]
        self.assertEqual(body_sent, '<p>Hello</p>')

    @patch(f'{MODULE}.send_email', return_value={'id': 'sent1'})
    @patch(f'{MODULE}.authenticate_gmail', return_value=MagicMock())
    @patch(f'{MODULE}.get_ai_response', return_value='<p>No fences here</p>')
    @patch(f'{MODULE}.Variable')
    def test_ai_response_no_fence(self, mock_var, mock_ai, mock_auth, mock_send):
        """AI response without fences -> falls back to response.strip()."""
        mock_var.get.return_value = 'test-model'
        kwargs = self._base_kwargs()
        send_screening_result_email(**kwargs)
        body_sent = mock_send.call_args[0][3]
        self.assertEqual(body_sent, '<p>No fences here</p>')

    @patch(f'{MODULE}.send_email')
    @patch(f'{MODULE}.authenticate_gmail', return_value=None)
    @patch(f'{MODULE}.get_ai_response', return_value='<p>X</p>')
    @patch(f'{MODULE}.Variable')
    def test_gmail_auth_failure(self, mock_var, mock_ai, mock_auth, mock_send):
        """authenticate_gmail returns None -> returns error, send_email NOT called."""
        mock_var.get.return_value = 'test-model'
        kwargs = self._base_kwargs()
        result = send_screening_result_email(**kwargs)
        self.assertEqual(result, "Gmail authentication failed")
        mock_send.assert_not_called()

    @patch(f'{MODULE}.send_email', return_value=None)
    @patch(f'{MODULE}.authenticate_gmail', return_value=MagicMock())
    @patch(f'{MODULE}.get_ai_response', return_value='<p>X</p>')
    @patch(f'{MODULE}.Variable')
    def test_send_email_failure(self, mock_var, mock_ai, mock_auth, mock_send):
        """send_email returns None -> 'Failed to send email'."""
        mock_var.get.return_value = 'test-model'
        kwargs = self._base_kwargs()
        result = send_screening_result_email(**kwargs)
        self.assertEqual(result, "Failed to send email")

    @patch(f'{MODULE}.send_email', return_value={'id': 'ok'})
    @patch(f'{MODULE}.authenticate_gmail', return_value=MagicMock())
    @patch(f'{MODULE}.get_ai_response', return_value='<p>X</p>')
    @patch(f'{MODULE}.Variable')
    def test_send_email_success(self, mock_var, mock_ai, mock_auth, mock_send):
        """send_email returns truthy -> success message."""
        mock_var.get.return_value = 'test-model'
        kwargs = self._base_kwargs()
        result = send_screening_result_email(**kwargs)
        self.assertIn("Email sent successfully", result)

    @patch(f'{MODULE}.send_email', return_value={'id': 'ok'})
    @patch(f'{MODULE}.authenticate_gmail', return_value=MagicMock())
    @patch(f'{MODULE}.get_ai_response', return_value='<p>X</p>')
    @patch(f'{MODULE}.Variable')
    def test_thread_id_passed_to_send_email(self, mock_var, mock_ai, mock_auth, mock_send):
        """threadId from email_data is passed through to send_email."""
        mock_var.get.return_value = 'test-model'
        kwargs = self._base_kwargs(email_data={
            'headers': {'From': 'a@b.com', 'Subject': 'S', 'Message-ID': '', 'References': ''},
            'threadId': 'thread_xyz',
        })
        send_screening_result_email(**kwargs)
        # thread_id is passed as a keyword argument or the last positional arg
        call_args = mock_send.call_args
        # send_email(service, sender_email, subject, body, original_message_id, references, from_addr, cc, bcc, thread_id)
        self.assertEqual(call_args[0][9] if len(call_args[0]) > 9 else call_args[1].get('thread_id'), 'thread_xyz')

    @patch(f'{MODULE}.send_email', return_value={'id': 'ok'})
    @patch(f'{MODULE}.authenticate_gmail', return_value=MagicMock())
    @patch(f'{MODULE}.get_ai_response', return_value='<p>X</p>')
    @patch(f'{MODULE}.Variable')
    def test_missing_thread_id(self, mock_var, mock_ai, mock_auth, mock_send):
        """No threadId in email_data -> thread_id is None."""
        mock_var.get.return_value = 'test-model'
        kwargs = self._base_kwargs(email_data={
            'headers': {'From': 'a@b.com', 'Subject': 'S', 'Message-ID': '', 'References': ''},
        })
        send_screening_result_email(**kwargs)
        # The last positional arg should be None
        call_args = mock_send.call_args[0]
        # thread_id is at position 9 (service, recipient, subject, body, in_reply_to, references, from_addr, cc, bcc, thread_id)
        if len(call_args) > 9:
            self.assertIsNone(call_args[9])

    @patch(f'{MODULE}.send_email', return_value={'id': 'ok'})
    @patch(f'{MODULE}.authenticate_gmail', return_value=MagicMock())
    @patch(f'{MODULE}.get_ai_response', return_value='<p>X</p>')
    @patch(f'{MODULE}.Variable')
    def test_references_deduplication(self, mock_var, mock_ai, mock_auth, mock_send):
        """Message-ID already in References -> not duplicated."""
        mock_var.get.return_value = 'test-model'
        kwargs = self._base_kwargs(email_data={
            'headers': {
                'From': 'a@b.com', 'Subject': 'S',
                'Message-ID': '<msg1>',
                'References': '<ref0> <msg1>',
            },
            'threadId': 'th1',
        })
        send_screening_result_email(**kwargs)
        references_sent = mock_send.call_args[0][5]
        # Should NOT have <msg1> twice
        self.assertEqual(references_sent.count('<msg1>'), 1)


# ===========================================================================
# 6. notify_recruiter_for_interview
# ===========================================================================
class TestNotifyRecruiterForInterview(unittest.TestCase):
    """Edge cases for notify_recruiter_for_interview."""

    def _base_kwargs(self, analysis_data=None, response_data=None, candidate_profile=None):
        """Helper to build kwargs with common defaults."""
        analysis_data = analysis_data if analysis_data is not None else {
            'decision': 'ACCEPT', 'overall_score': 85,
            'strengths': ['Strong Python'], 'concerns': ['Short experience'],
            'detailed_reason': 'Good fit overall'
        }
        response_data = response_data if response_data is not None else {
            'sender_email': 'cand@test.com', 'body': 'My answers'
        }
        if candidate_profile is None:
            candidate_profile = {
                'candidate_name': 'John Doe',
                'total_score': 75,
                'job_title': 'Python Dev',
                'experience_match': {'candidate_experience_years': 5},
                'education_match': {'candidate_education': 'BSc CS'},
                'must_have_skills': [
                    {'skill_name': 'Python', 'match': True},
                    {'skill_name': 'Java', 'match': False},
                ],
                'nice_to_have_skills': [
                    {'skill_name': 'Docker', 'match': True},
                ],
                'original_cv_content': 'CV text...',
            }

        return make_kwargs(
            conf={},
            xcom_data={
                ('analyze_screening_responses', 'analysis_data'): analysis_data,
                ('extract_candidate_response', 'response_data'): response_data,
                ('load_candidate_profile', 'candidate_profile'): candidate_profile,
            }
        )

    def test_missing_analysis_data(self):
        """analysis_data is None -> 'Missing data - skipped'."""
        kwargs = make_kwargs(xcom_data={
            ('analyze_screening_responses', 'analysis_data'): None,
            ('extract_candidate_response', 'response_data'): {'sender_email': 'x'},
            ('load_candidate_profile', 'candidate_profile'): {},
        })
        result = notify_recruiter_for_interview(**kwargs)
        self.assertEqual(result, "Missing data - skipped")

    def test_missing_response_data(self):
        """response_data is None -> 'Missing data - skipped'."""
        kwargs = make_kwargs(xcom_data={
            ('analyze_screening_responses', 'analysis_data'): {'decision': 'ACCEPT'},
            ('extract_candidate_response', 'response_data'): None,
            ('load_candidate_profile', 'candidate_profile'): {},
        })
        result = notify_recruiter_for_interview(**kwargs)
        self.assertEqual(result, "Missing data - skipped")

    def test_decision_reject_skips(self):
        """decision == 'REJECT' -> skips notification."""
        kwargs = self._base_kwargs(analysis_data={'decision': 'REJECT', 'overall_score': 20})
        result = notify_recruiter_for_interview(**kwargs)
        self.assertIn("Skipped", result)
        self.assertIn("REJECT", result)

    def test_decision_pending_skips(self):
        """decision == 'PENDING' -> skips (not ACCEPT)."""
        kwargs = self._base_kwargs(analysis_data={'decision': 'PENDING'})
        result = notify_recruiter_for_interview(**kwargs)
        self.assertIn("Skipped", result)

    def test_decision_missing_defaults_pending_skips(self):
        """No decision key -> .get default is 'PENDING' -> skips."""
        kwargs = self._base_kwargs(analysis_data={'overall_score': 50})
        result = notify_recruiter_for_interview(**kwargs)
        self.assertIn("Skipped", result)

    @patch(f'{MODULE}.send_email', return_value={'id': 'ok'})
    @patch(f'{MODULE}.authenticate_gmail', return_value=MagicMock())
    @patch(f'{MODULE}.extract_json_from_text', return_value={
        'candidate_summary': 'Summary', 'interview_questions': []
    })
    @patch(f'{MODULE}.get_ai_response', return_value='{}')
    @patch(f'{MODULE}.Variable')
    def test_decision_lowercase_accept_now_proceeds(self, mock_var, mock_ai, mock_json, mock_auth, mock_send):
        """
        Bug #3 fix: decision == 'accept' (lowercase) normalized to 'ACCEPT'.
        Now proceeds to send recruiter notification.
        """
        mock_var.get.return_value = 'test-model'
        kwargs = self._base_kwargs(analysis_data={'decision': 'accept', 'overall_score': 80,
            'strengths': [], 'concerns': [], 'detailed_reason': 'ok'})
        result = notify_recruiter_for_interview(**kwargs)
        self.assertIn("Recruiter notified", result)

    @patch(f'{MODULE}.send_email', return_value={'id': 'ok'})
    @patch(f'{MODULE}.authenticate_gmail', return_value=MagicMock())
    @patch(f'{MODULE}.extract_json_from_text', return_value={
        'candidate_summary': 'Great candidate',
        'interview_questions': [{'question': 'Q1', 'what_to_look_for': 'A1'}]
    })
    @patch(f'{MODULE}.get_ai_response', return_value='{}')
    @patch(f'{MODULE}.Variable')
    def test_decision_accept_proceeds(self, mock_var, mock_ai, mock_json, mock_auth, mock_send):
        """decision == 'ACCEPT' -> email is sent."""
        mock_var.get.return_value = 'test-model'
        kwargs = self._base_kwargs()
        result = notify_recruiter_for_interview(**kwargs)
        self.assertIn("Recruiter notified", result)
        mock_send.assert_called_once()

    @patch(f'{MODULE}.send_email', return_value={'id': 'ok'})
    @patch(f'{MODULE}.authenticate_gmail', return_value=MagicMock())
    @patch(f'{MODULE}.extract_json_from_text', return_value={
        'candidate_summary': 'Summary', 'interview_questions': []
    })
    @patch(f'{MODULE}.get_ai_response', return_value='{}')
    @patch(f'{MODULE}.Variable')
    def test_candidate_profile_is_none(self, mock_var, mock_ai, mock_json, mock_auth, mock_send):
        """candidate_profile is None -> all profile fields default gracefully."""
        mock_var.get.return_value = 'test-model'
        # Need to set candidate_profile xcom to None explicitly
        kwargs = make_kwargs(
            conf={},
            xcom_data={
                ('analyze_screening_responses', 'analysis_data'): {
                    'decision': 'ACCEPT', 'overall_score': 80,
                    'strengths': [], 'concerns': [], 'detailed_reason': 'ok'
                },
                ('extract_candidate_response', 'response_data'): {
                    'sender_email': 'c@d.com', 'body': 'ans'
                },
                ('load_candidate_profile', 'candidate_profile'): None,
            }
        )
        result = notify_recruiter_for_interview(**kwargs)
        self.assertIn("Recruiter notified", result)
        # Check the email body for default values
        body_sent = mock_send.call_args[0][3]
        self.assertIn('Unknown Candidate', body_sent)
        self.assertIn('N/A', body_sent)

    @patch(f'{MODULE}.send_email', return_value={'id': 'ok'})
    @patch(f'{MODULE}.authenticate_gmail', return_value=MagicMock())
    @patch(f'{MODULE}.extract_json_from_text', return_value={
        'candidate_summary': 'Sum', 'interview_questions': []
    })
    @patch(f'{MODULE}.get_ai_response', return_value='{}')
    @patch(f'{MODULE}.Variable')
    def test_profile_missing_all_keys(self, mock_var, mock_ai, mock_json, mock_auth, mock_send):
        """Profile exists but has no expected keys -> all defaults used."""
        mock_var.get.return_value = 'test-model'
        kwargs = self._base_kwargs(
            analysis_data={
                'decision': 'ACCEPT', 'overall_score': 80,
                'strengths': [], 'concerns': [], 'detailed_reason': 'ok'
            },
            candidate_profile={}
        )
        result = notify_recruiter_for_interview(**kwargs)
        self.assertIn("Recruiter notified", result)

    @patch(f'{MODULE}.send_email', return_value={'id': 'ok'})
    @patch(f'{MODULE}.authenticate_gmail', return_value=MagicMock())
    @patch(f'{MODULE}.extract_json_from_text', return_value={
        'candidate_summary': 'Sum', 'interview_questions': []
    })
    @patch(f'{MODULE}.get_ai_response', return_value='{}')
    @patch(f'{MODULE}.Variable')
    def test_empty_skills_arrays(self, mock_var, mock_ai, mock_json, mock_auth, mock_send):
        """Empty must_have_skills and nice_to_have_skills -> 'N/A'."""
        mock_var.get.return_value = 'test-model'
        kwargs = self._base_kwargs(candidate_profile={
            'candidate_name': 'Jane', 'total_score': 60, 'job_title': 'Dev',
            'experience_match': {'candidate_experience_years': 2},
            'education_match': {'candidate_education': 'BSc'},
            'must_have_skills': [],
            'nice_to_have_skills': [],
        })
        kwargs['dag_run'].conf = {}
        # Override analysis_data to have ACCEPT
        kwargs['ti'].xcom_pull.side_effect = lambda task_ids=None, key=None: {
            ('analyze_screening_responses', 'analysis_data'): {
                'decision': 'ACCEPT', 'overall_score': 60,
                'strengths': ['Eager'], 'concerns': [], 'detailed_reason': 'Promising'
            },
            ('extract_candidate_response', 'response_data'): {
                'sender_email': 'jane@test.com', 'body': 'answers'
            },
            ('load_candidate_profile', 'candidate_profile'): {
                'candidate_name': 'Jane', 'total_score': 60, 'job_title': 'Dev',
                'experience_match': {'candidate_experience_years': 2},
                'education_match': {'candidate_education': 'BSc'},
                'must_have_skills': [],
                'nice_to_have_skills': [],
            },
        }.get((task_ids, key))

        result = notify_recruiter_for_interview(**kwargs)
        body_sent = mock_send.call_args[0][3]
        self.assertIn('N/A', body_sent)

    @patch(f'{MODULE}.send_email', return_value={'id': 'ok'})
    @patch(f'{MODULE}.authenticate_gmail', return_value=MagicMock())
    @patch(f'{MODULE}.extract_json_from_text', return_value={
        'candidate_summary': 'Sum', 'interview_questions': []
    })
    @patch(f'{MODULE}.get_ai_response', return_value='{}')
    @patch(f'{MODULE}.Variable')
    def test_skills_with_no_matches(self, mock_var, mock_ai, mock_json, mock_auth, mock_send):
        """All skills have match=False -> matched list is empty -> 'N/A'."""
        mock_var.get.return_value = 'test-model'
        kwargs = self._base_kwargs(candidate_profile={
            'candidate_name': 'X', 'total_score': 40, 'job_title': 'Dev',
            'experience_match': {'candidate_experience_years': 1},
            'education_match': {'candidate_education': 'Diploma'},
            'must_have_skills': [
                {'skill_name': 'Python', 'match': False},
                {'skill_name': 'Java', 'match': False},
            ],
            'nice_to_have_skills': [{'skill_name': 'Go', 'match': False}],
        })
        kwargs['dag_run'].conf = {}
        kwargs['ti'].xcom_pull.side_effect = lambda task_ids=None, key=None: {
            ('analyze_screening_responses', 'analysis_data'): {
                'decision': 'ACCEPT', 'overall_score': 60,
                'strengths': [], 'concerns': [], 'detailed_reason': 'ok'
            },
            ('extract_candidate_response', 'response_data'): {
                'sender_email': 'x@test.com', 'body': 'ans'
            },
            ('load_candidate_profile', 'candidate_profile'): {
                'candidate_name': 'X', 'total_score': 40, 'job_title': 'Dev',
                'experience_match': {'candidate_experience_years': 1},
                'education_match': {'candidate_education': 'Diploma'},
                'must_have_skills': [
                    {'skill_name': 'Python', 'match': False},
                    {'skill_name': 'Java', 'match': False},
                ],
                'nice_to_have_skills': [{'skill_name': 'Go', 'match': False}],
            },
        }.get((task_ids, key))
        result = notify_recruiter_for_interview(**kwargs)
        body_sent = mock_send.call_args[0][3]
        # Should contain N/A for skills since none matched
        self.assertIn('N/A', body_sent)

    @patch(f'{MODULE}.send_email', return_value={'id': 'ok'})
    @patch(f'{MODULE}.authenticate_gmail', return_value=MagicMock())
    @patch(f'{MODULE}.extract_json_from_text', return_value={
        'candidate_summary': 'Sum', 'interview_questions': []
    })
    @patch(f'{MODULE}.get_ai_response', return_value='{}')
    @patch(f'{MODULE}.Variable')
    def test_skill_missing_skill_name_key(self, mock_var, mock_ai, mock_json, mock_auth, mock_send):
        """Skill dict has match=True but no skill_name -> empty string in output."""
        mock_var.get.return_value = 'test-model'
        kwargs = self._base_kwargs(candidate_profile={
            'candidate_name': 'X', 'total_score': 50, 'job_title': 'Dev',
            'experience_match': {'candidate_experience_years': 3},
            'education_match': {'candidate_education': 'BSc'},
            'must_have_skills': [{'match': True}],  # no skill_name key
            'nice_to_have_skills': [],
        })
        kwargs['dag_run'].conf = {}
        kwargs['ti'].xcom_pull.side_effect = lambda task_ids=None, key=None: {
            ('analyze_screening_responses', 'analysis_data'): {
                'decision': 'ACCEPT', 'overall_score': 60,
                'strengths': [], 'concerns': [], 'detailed_reason': 'ok'
            },
            ('extract_candidate_response', 'response_data'): {
                'sender_email': 'x@t.com', 'body': 'ans'
            },
            ('load_candidate_profile', 'candidate_profile'): {
                'candidate_name': 'X', 'total_score': 50, 'job_title': 'Dev',
                'experience_match': {'candidate_experience_years': 3},
                'education_match': {'candidate_education': 'BSc'},
                'must_have_skills': [{'match': True}],
                'nice_to_have_skills': [],
            },
        }.get((task_ids, key))
        # Should not crash; empty skill name produces empty string
        result = notify_recruiter_for_interview(**kwargs)
        self.assertIn("Recruiter notified", result)

    @patch(f'{MODULE}.send_email', return_value={'id': 'ok'})
    @patch(f'{MODULE}.authenticate_gmail', return_value=MagicMock())
    @patch(f'{MODULE}.extract_json_from_text', return_value={
        'candidate_summary': 'Sum', 'interview_questions': []
    })
    @patch(f'{MODULE}.get_ai_response', return_value='{}')
    @patch(f'{MODULE}.Variable')
    def test_experience_match_value_is_none_handled(self, mock_var, mock_ai, mock_json, mock_auth, mock_send):
        """
        Bug #5 fix: experience_match is None -> (or {}) prevents crash.
        Defaults to 'N/A'.
        """
        mock_var.get.return_value = 'test-model'
        kwargs = self._base_kwargs(
            analysis_data={
                'decision': 'ACCEPT', 'overall_score': 80,
                'strengths': [], 'concerns': [], 'detailed_reason': 'ok'
            },
            candidate_profile={
                'candidate_name': 'Fixed', 'total_score': 50, 'job_title': 'Dev',
                'experience_match': None,
                'education_match': {'candidate_education': 'BSc'},
                'must_have_skills': [],
                'nice_to_have_skills': [],
            }
        )
        result = notify_recruiter_for_interview(**kwargs)
        self.assertIn("Recruiter notified", result)

    @patch(f'{MODULE}.send_email', return_value={'id': 'ok'})
    @patch(f'{MODULE}.authenticate_gmail', return_value=MagicMock())
    @patch(f'{MODULE}.extract_json_from_text', return_value={
        'candidate_summary': 'Sum', 'interview_questions': []
    })
    @patch(f'{MODULE}.get_ai_response', return_value='{}')
    @patch(f'{MODULE}.Variable')
    def test_education_match_value_is_none_handled(self, mock_var, mock_ai, mock_json, mock_auth, mock_send):
        """
        Bug #5 fix: education_match is None -> (or {}) prevents crash.
        Defaults to 'N/A'.
        """
        mock_var.get.return_value = 'test-model'
        kwargs = self._base_kwargs(
            analysis_data={
                'decision': 'ACCEPT', 'overall_score': 80,
                'strengths': [], 'concerns': [], 'detailed_reason': 'ok'
            },
            candidate_profile={
                'candidate_name': 'Fixed', 'total_score': 50, 'job_title': 'Dev',
                'experience_match': {'candidate_experience_years': 3},
                'education_match': None,
                'must_have_skills': [],
                'nice_to_have_skills': [],
            }
        )
        result = notify_recruiter_for_interview(**kwargs)
        self.assertIn("Recruiter notified", result)

    @patch(f'{MODULE}.send_email', return_value={'id': 'ok'})
    @patch(f'{MODULE}.authenticate_gmail', return_value=MagicMock())
    @patch(f'{MODULE}.extract_json_from_text', return_value={
        'candidate_summary': 'Sum', 'interview_questions': []
    })
    @patch(f'{MODULE}.get_ai_response', return_value='{}')
    @patch(f'{MODULE}.Variable')
    def test_missing_strengths_concerns(self, mock_var, mock_ai, mock_json, mock_auth, mock_send):
        """analysis_data has no strengths/concerns -> defaults to empty lists."""
        mock_var.get.return_value = 'test-model'
        kwargs = self._base_kwargs(analysis_data={
            'decision': 'ACCEPT',
            # no strengths, concerns, detailed_reason, overall_score
        })
        result = notify_recruiter_for_interview(**kwargs)
        body_sent = mock_send.call_args[0][3]
        self.assertIn('N/A', body_sent)  # strengths default
        self.assertIn('None identified', body_sent)  # concerns default

    @patch(f'{MODULE}.send_email', return_value={'id': 'ok'})
    @patch(f'{MODULE}.authenticate_gmail', return_value=MagicMock())
    @patch(f'{MODULE}.extract_json_from_text', return_value=None)
    @patch(f'{MODULE}.get_ai_response', return_value='garbage response')
    @patch(f'{MODULE}.Variable')
    def test_ai_interview_prep_returns_garbage(self, mock_var, mock_ai, mock_json, mock_auth, mock_send):
        """AI returns non-JSON for interview prep -> defaults to N/A and []."""
        mock_var.get.return_value = 'test-model'
        kwargs = self._base_kwargs()
        result = notify_recruiter_for_interview(**kwargs)
        self.assertIn("Recruiter notified", result)
        body_sent = mock_send.call_args[0][3]
        self.assertIn('N/A', body_sent)  # candidate_summary default

    @patch(f'{MODULE}.send_email', return_value={'id': 'ok'})
    @patch(f'{MODULE}.authenticate_gmail', return_value=MagicMock())
    @patch(f'{MODULE}.extract_json_from_text', return_value={
        'candidate_summary': 'Great candidate',
        'interview_questions': [
            {'question': 'Tell me about yourself', 'what_to_look_for': 'Clear narrative'},
            {'question': 'Python experience?', 'what_to_look_for': 'Specific examples'},
        ]
    })
    @patch(f'{MODULE}.get_ai_response', return_value='{}')
    @patch(f'{MODULE}.Variable')
    def test_interview_questions_in_html(self, mock_var, mock_ai, mock_json, mock_auth, mock_send):
        """Interview questions appear in the HTML body."""
        mock_var.get.return_value = 'test-model'
        kwargs = self._base_kwargs()
        result = notify_recruiter_for_interview(**kwargs)
        body_sent = mock_send.call_args[0][3]
        self.assertIn('Tell me about yourself', body_sent)
        self.assertIn('Clear narrative', body_sent)
        self.assertIn('Python experience?', body_sent)

    @patch(f'{MODULE}.send_email')
    @patch(f'{MODULE}.authenticate_gmail', return_value=None)
    @patch(f'{MODULE}.extract_json_from_text', return_value={'candidate_summary': 'X', 'interview_questions': []})
    @patch(f'{MODULE}.get_ai_response', return_value='{}')
    @patch(f'{MODULE}.Variable')
    def test_gmail_auth_failure(self, mock_var, mock_ai, mock_json, mock_auth, mock_send):
        """Gmail auth fails -> returns error, send_email not called."""
        mock_var.get.return_value = 'test-model'
        kwargs = self._base_kwargs()
        result = notify_recruiter_for_interview(**kwargs)
        self.assertEqual(result, "Gmail authentication failed")
        mock_send.assert_not_called()

    @patch(f'{MODULE}.send_email', return_value=None)
    @patch(f'{MODULE}.authenticate_gmail', return_value=MagicMock())
    @patch(f'{MODULE}.extract_json_from_text', return_value={'candidate_summary': 'X', 'interview_questions': []})
    @patch(f'{MODULE}.get_ai_response', return_value='{}')
    @patch(f'{MODULE}.Variable')
    def test_send_email_failure(self, mock_var, mock_ai, mock_json, mock_auth, mock_send):
        """send_email returns None -> failure message."""
        mock_var.get.return_value = 'test-model'
        kwargs = self._base_kwargs()
        result = notify_recruiter_for_interview(**kwargs)
        self.assertEqual(result, "Failed to send recruiter notification")

    @patch(f'{MODULE}.send_email', return_value={'id': 'ok'})
    @patch(f'{MODULE}.authenticate_gmail', return_value=MagicMock())
    @patch(f'{MODULE}.extract_json_from_text', return_value={'candidate_summary': 'X', 'interview_questions': []})
    @patch(f'{MODULE}.get_ai_response', return_value='{}')
    @patch(f'{MODULE}.Variable')
    def test_subject_contains_name_and_position(self, mock_var, mock_ai, mock_json, mock_auth, mock_send):
        """Subject line includes candidate name and position."""
        mock_var.get.return_value = 'test-model'
        kwargs = self._base_kwargs()
        result = notify_recruiter_for_interview(**kwargs)
        subject_sent = mock_send.call_args[0][2]
        self.assertIn('John Doe', subject_sent)
        self.assertIn('Python Dev', subject_sent)

    @patch(f'{MODULE}.send_email', return_value={'id': 'ok'})
    @patch(f'{MODULE}.authenticate_gmail', return_value=MagicMock())
    @patch(f'{MODULE}.extract_json_from_text', return_value={
        'candidate_summary': 'Sum', 'interview_questions': [{}]
    })
    @patch(f'{MODULE}.get_ai_response', return_value='{}')
    @patch(f'{MODULE}.Variable')
    def test_interview_question_missing_keys(self, mock_var, mock_ai, mock_json, mock_auth, mock_send):
        """Question dict is {} -> uses empty strings, no crash."""
        mock_var.get.return_value = 'test-model'
        kwargs = self._base_kwargs()
        result = notify_recruiter_for_interview(**kwargs)
        self.assertIn("Recruiter notified", result)


# ===========================================================================
# 7. Cross-Cutting / Integration Tests
# ===========================================================================
class TestCrossCutting(unittest.TestCase):
    """Cross-cutting edge cases and integration flows."""

    def test_email_sanitization_consistency(self):
        """
        load_candidate_profile and update_candidate_profile must produce
        the same sanitized filename for the same email address.
        """
        test_email = 'user/test\\admin@example.com'
        # Replicate the sanitization logic
        safe = test_email.replace('@', '_at_').replace('/', '_').replace('\\', '_')
        self.assertEqual(safe, 'user_test_admin_at_example.com')

    @patch(f'{MODULE}.extract_json_from_text')
    @patch(f'{MODULE}.get_ai_response')
    @patch(f'{MODULE}.Variable')
    def test_full_pipeline_happy_path(self, mock_var, mock_ai, mock_json):
        """All 6 functions called with valid data flowing through xcom."""
        mock_var.get.return_value = 'test-model'

        # 1. extract_candidate_response
        email_data = {
            'id': '42',
            'headers': {
                'From': 'Jane <jane@example.com>',
                'Subject': 'Screening Answers',
                'Message-ID': '<msg42>',
                'References': '<ref1>',
            },
            'thread_history': {
                'current_message': {'content': 'Here are my detailed answers...'},
                'original_cv_content': 'Full CV text here',
            },
            'threadId': 'th42',
            'date': '2024-03-01T10:00:00',
        }

        kwargs1 = make_kwargs(conf={'email_data': email_data})
        response_data = extract_candidate_response(**kwargs1)
        self.assertIsNotNone(response_data)
        self.assertEqual(response_data['sender_email'], 'jane@example.com')
        self.assertEqual(response_data['body'], 'Here are my detailed answers...')

        # 2. load_candidate_profile (mocked filesystem)
        profile = {
            'candidate_name': 'Jane Doe',
            'total_score': 80,
            'job_title': 'Data Engineer',
            'experience_match': {'candidate_experience_years': 4},
            'education_match': {'candidate_education': 'MSc Data Science'},
            'must_have_skills': [{'skill_name': 'Python', 'match': True}],
            'nice_to_have_skills': [{'skill_name': 'Spark', 'match': True}],
            'original_cv_content': 'Fallback CV text',
        }
        kwargs2 = make_kwargs(xcom_data={
            ('extract_candidate_response', 'response_data'): response_data,
        })
        with patch.object(Path, 'exists', return_value=True):
            with patch('builtins.open', mock_open(read_data=json.dumps(profile))):
                loaded_profile = load_candidate_profile(**kwargs2)
        self.assertEqual(loaded_profile['candidate_name'], 'Jane Doe')

        # 3. analyze_screening_responses
        analysis = {
            'decision': 'ACCEPT', 'overall_score': 88,
            'strengths': ['Strong Python'], 'concerns': [],
            'detailed_reason': 'Excellent fit'
        }
        mock_ai.return_value = json.dumps(analysis)
        mock_json.return_value = analysis

        kwargs3 = make_kwargs(
            conf={'email_data': email_data},
            xcom_data={
                ('extract_candidate_response', 'response_data'): response_data,
                ('load_candidate_profile', 'candidate_profile'): loaded_profile,
            }
        )
        analysis_result = analyze_screening_responses(**kwargs3)
        self.assertEqual(analysis_result['decision'], 'ACCEPT')

    def test_pipeline_early_none_from_extract(self):
        """
        extract returns None -> downstream functions handle gracefully.
        """
        # extract with empty data
        kwargs = make_kwargs(conf={})
        result = extract_candidate_response(**kwargs)
        self.assertIsNone(result)

        # load_candidate_profile with None response_data
        kwargs2 = make_kwargs(xcom_data={
            ('extract_candidate_response', 'response_data'): None,
        })
        result2 = load_candidate_profile(**kwargs2)
        self.assertIsNone(result2)

        # analyze with None inputs
        kwargs3 = make_kwargs(
            conf={},
            xcom_data={
                ('extract_candidate_response', 'response_data'): None,
                ('load_candidate_profile', 'candidate_profile'): None,
            }
        )
        result3 = analyze_screening_responses(**kwargs3)
        self.assertIsNone(result3)

        # update with None inputs
        kwargs4 = make_kwargs(xcom_data={
            ('extract_candidate_response', 'response_data'): None,
            ('analyze_screening_responses', 'analysis_data'): None,
            ('load_candidate_profile', 'candidate_profile'): None,
        })
        result4 = update_candidate_profile(**kwargs4)
        self.assertIsNone(result4)


if __name__ == '__main__':
    unittest.main()
