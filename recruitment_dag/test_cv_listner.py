"""
Comprehensive edge-case unit tests for cv_listner.py — CV Mailbox Monitor DAG.
Tests all 11 functions with regression tests for fixed bugs.

Run with:
    python3 -m unittest agent_dags.recruitment_dag.test_cv_listner -v
from the airflow/dags directory.
"""

import json
import sys
import unittest
import base64
from unittest.mock import patch, MagicMock, call

# ---------------------------------------------------------------------------
# Patch Airflow and external modules into sys.modules BEFORE importing the
# module under test, because cv_listner.py calls Variable.get() at module
# level and several dependencies (bs4, ollama, googleapiclient, etc.) may
# not be installed in the test environment.
# ---------------------------------------------------------------------------
_mock_variable = MagicMock()
_mock_variable.get = MagicMock(side_effect=lambda key, default_var=None: {
    "ltai.v3.lowtouch.recruitment.email_credentials": '{"token": "fake"}',
    "ltai.v3.lowtouch.recruitment.from_address": "recruit@test.com",
    "ltai.v3.lowtouch.recruitment.sheets_credentials": '{"creds": "fake"}',
    "ltai.v3.lowtouch.recruitment.sheets_id": "sheet123",
    "ltai.v3.lowtouch.recruitment.sheets_auth_type": "oauth",
    "ltai.v3.lowtouch.recruitment.model_name": "test-model",
}.get(key, default_var))

# Airflow modules
sys.modules.setdefault('airflow', MagicMock())
sys.modules.setdefault('airflow.models', MagicMock(Variable=_mock_variable))
sys.modules.setdefault('airflow.operators', MagicMock())
sys.modules.setdefault('airflow.operators.python', MagicMock())
sys.modules.setdefault('airflow.operators.trigger_dagrun', MagicMock())
sys.modules['airflow'].DAG = MagicMock()
sys.modules['airflow'].models = sys.modules['airflow.models']
sys.modules['airflow.models'].Variable = _mock_variable

# External dependencies that may not be installed
sys.modules.setdefault('bs4', MagicMock())
sys.modules.setdefault('ollama', MagicMock())
sys.modules.setdefault('googleapiclient', MagicMock())
sys.modules.setdefault('googleapiclient.discovery', MagicMock())
sys.modules.setdefault('googleapiclient.errors', MagicMock())
sys.modules.setdefault('google.oauth2.credentials', MagicMock())
sys.modules.setdefault('google.oauth2.service_account', MagicMock())
sys.modules.setdefault('langchain_community', MagicMock())
sys.modules.setdefault('langchain_community.document_loaders', MagicMock())

# Mock utility modules so we control their behavior per-test
_mock_email_utils = MagicMock()
_mock_agent_utils = MagicMock()
_mock_sheets_utils = MagicMock()
sys.modules.setdefault('agent_dags', MagicMock())
sys.modules.setdefault('agent_dags.utils', MagicMock())
sys.modules.setdefault('agent_dags.utils.email_utils', _mock_email_utils)
sys.modules.setdefault('agent_dags.utils.agent_utils', _mock_agent_utils)
sys.modules.setdefault('agent_dags.utils.sheets_utils', _mock_sheets_utils)

from agent_dags.recruitment_dag.cv_listner import (
    extract_message_body,
    extract_thread_history_with_attachments,
    format_history_for_ai,
    build_current_message_with_pdf,
    find_cv_in_thread,
    extract_email_from_cv,
    fetch_cv_emails,
    classify_email_type,
    check_for_emails,
    route_emails_to_dags,
    log_no_emails,
)


# ── helpers ──────────────────────────────────────────────────────────────────

def _b64(text):
    """Base64url-encode a UTF-8 string (Gmail API style)."""
    return base64.urlsafe_b64encode(text.encode('utf-8')).decode('utf-8')


def make_kwargs(xcom_data=None, conf=None):
    """Build mock Airflow **kwargs with ti, dag_run."""
    ti = MagicMock()
    if xcom_data:
        def _pull(task_ids=None, key=None):
            return xcom_data.get((task_ids, key))
        ti.xcom_pull.side_effect = _pull

    dag_run = MagicMock()
    dag_run.conf = conf or {}

    return {'ti': ti, 'dag_run': dag_run}


# ════════════════════════════════════════════════════════════════════════════
# 1. extract_message_body
# ════════════════════════════════════════════════════════════════════════════

class TestExtractMessageBody(unittest.TestCase):

    def test_direct_body_data(self):
        """Extracts body from payload.body.data (base64url)."""
        payload = {"body": {"data": _b64("Hello World")}}
        self.assertEqual(extract_message_body(payload), "Hello World")

    def test_multipart_plain_text(self):
        """Extracts text/plain from multipart payload."""
        payload = {
            "parts": [
                {"mimeType": "text/plain", "body": {"data": _b64("Plain text body")}},
            ]
        }
        self.assertEqual(extract_message_body(payload), "Plain text body")

    @patch('agent_dags.recruitment_dag.cv_listner.BeautifulSoup')
    def test_multipart_html_fallback(self, mock_bs):
        """Falls back to HTML when no text/plain part."""
        mock_bs.return_value.get_text.return_value = "Hello HTML"
        payload = {
            "parts": [
                {"mimeType": "text/html", "body": {"data": _b64("<p>Hello HTML</p>")}},
            ]
        }
        result = extract_message_body(payload)
        self.assertEqual(result, "Hello HTML")
        mock_bs.assert_called_once()

    def test_plain_preferred_over_html(self):
        """text/plain is preferred over text/html."""
        payload = {
            "parts": [
                {"mimeType": "text/plain", "body": {"data": _b64("Plain version")}},
                {"mimeType": "text/html", "body": {"data": _b64("<b>HTML version</b>")}},
            ]
        }
        self.assertEqual(extract_message_body(payload), "Plain version")

    def test_nested_multipart(self):
        """Recurses into nested multipart parts."""
        payload = {
            "parts": [
                {
                    "mimeType": "multipart/alternative",
                    "parts": [
                        {"mimeType": "text/plain", "body": {"data": _b64("Nested plain")}},
                    ]
                }
            ]
        }
        self.assertEqual(extract_message_body(payload), "Nested plain")

    def test_empty_payload(self):
        """Empty payload returns empty string."""
        self.assertEqual(extract_message_body({}), "")

    def test_no_body_no_parts(self):
        """Payload with neither body.data nor parts returns empty string."""
        payload = {"body": {"size": 0}}
        self.assertEqual(extract_message_body(payload), "")

    def test_body_key_but_no_data(self):
        """Payload with body dict but no 'data' key skips to parts check."""
        payload = {"body": {"size": 100}}
        self.assertEqual(extract_message_body(payload), "")

    @patch('agent_dags.recruitment_dag.cv_listner.BeautifulSoup')
    def test_multipart_no_data_in_body(self, mock_bs):
        """Part with mimeType but no data in body is skipped, HTML fallback used."""
        mock_bs.return_value.get_text.return_value = "Fallback"
        payload = {
            "parts": [
                {"mimeType": "text/plain", "body": {"size": 50}},
                {"mimeType": "text/html", "body": {"data": _b64("<p>Fallback</p>")}},
            ]
        }
        result = extract_message_body(payload)
        self.assertEqual(result, "Fallback")

    def test_exception_returns_empty_string(self):
        """Exceptions are caught and empty string returned."""
        # Pass something that will cause an error in processing
        result = extract_message_body(None)
        self.assertEqual(result, "")


# ════════════════════════════════════════════════════════════════════════════
# 2. format_history_for_ai
# ════════════════════════════════════════════════════════════════════════════

class TestFormatHistoryForAI(unittest.TestCase):

    def test_basic_user_assistant_pair(self):
        """User+assistant pair creates correct prompt/response."""
        data = {'history': [
            {'role': 'user', 'content': 'Hello', 'has_pdf': False, 'pdf_content': None, 'message_id': 'm1'},
            {'role': 'assistant', 'content': 'Hi there', 'has_pdf': False, 'pdf_content': None, 'message_id': 'm2'},
        ]}
        result = format_history_for_ai(data)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['prompt'], 'Hello')
        self.assertEqual(result[0]['response'], 'Hi there')

    def test_user_without_assistant_reply(self):
        """User message without following assistant gets empty response."""
        data = {'history': [
            {'role': 'user', 'content': 'Question', 'has_pdf': False, 'pdf_content': None, 'message_id': 'm1'},
        ]}
        result = format_history_for_ai(data)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['prompt'], 'Question')
        self.assertEqual(result[0]['response'], '')

    def test_empty_history(self):
        """Empty history returns empty list."""
        self.assertEqual(format_history_for_ai({'history': []}), [])

    def test_empty_history_key_missing(self):
        """Missing 'history' key returns empty list."""
        self.assertEqual(format_history_for_ai({}), [])

    def test_standalone_assistant_skipped(self):
        """Standalone assistant message is skipped."""
        data = {'history': [
            {'role': 'assistant', 'content': 'Orphan', 'has_pdf': False, 'pdf_content': None, 'message_id': 'm1'},
        ]}
        result = format_history_for_ai(data)
        self.assertEqual(len(result), 0)

    def test_regression_assistant_then_user_not_skipped(self):
        """REGRESSION: User message after standalone assistant must NOT be skipped.

        Bug: The else branch (standalone assistant) had an extra `i += 1`
        that, combined with the outer `i += 1`, double-incremented the index,
        causing the next message to be silently skipped.
        """
        data = {'history': [
            {'role': 'assistant', 'content': 'Orphan', 'has_pdf': False, 'pdf_content': None, 'message_id': 'm1'},
            {'role': 'user', 'content': 'My question', 'has_pdf': False, 'pdf_content': None, 'message_id': 'm2'},
            {'role': 'assistant', 'content': 'My answer', 'has_pdf': False, 'pdf_content': None, 'message_id': 'm3'},
        ]}
        result = format_history_for_ai(data)
        # The user message at index 1 must appear
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['prompt'], 'My question')
        self.assertEqual(result[0]['response'], 'My answer')

    def test_regression_multiple_orphan_assistants_then_user(self):
        """REGRESSION: Multiple orphan assistants followed by user → user is captured."""
        data = {'history': [
            {'role': 'assistant', 'content': 'A1', 'has_pdf': False, 'pdf_content': None, 'message_id': 'a1'},
            {'role': 'assistant', 'content': 'A2', 'has_pdf': False, 'pdf_content': None, 'message_id': 'a2'},
            {'role': 'user', 'content': 'Important Q', 'has_pdf': False, 'pdf_content': None, 'message_id': 'u1'},
        ]}
        result = format_history_for_ai(data)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['prompt'], 'Important Q')

    def test_pdf_content_appended_to_user(self):
        """PDF content is appended to user message."""
        data = {'history': [
            {'role': 'user', 'content': 'See CV', 'has_pdf': True, 'pdf_content': 'Resume text', 'message_id': 'm1'},
        ]}
        result = format_history_for_ai(data)
        self.assertIn('Resume text', result[0]['prompt'])
        self.assertIn('[Attached PDF Content]', result[0]['prompt'])

    def test_pdf_content_appended_to_assistant(self):
        """PDF content is appended to assistant message."""
        data = {'history': [
            {'role': 'user', 'content': 'Hello', 'has_pdf': False, 'pdf_content': None, 'message_id': 'm1'},
            {'role': 'assistant', 'content': 'Here is info', 'has_pdf': True, 'pdf_content': 'Doc text', 'message_id': 'm2'},
        ]}
        result = format_history_for_ai(data)
        self.assertIn('Doc text', result[0]['response'])

    def test_multiple_conversation_pairs(self):
        """Multiple user-assistant pairs are all captured."""
        data = {'history': [
            {'role': 'user', 'content': 'Q1', 'has_pdf': False, 'pdf_content': None, 'message_id': 'm1'},
            {'role': 'assistant', 'content': 'A1', 'has_pdf': False, 'pdf_content': None, 'message_id': 'm2'},
            {'role': 'user', 'content': 'Q2', 'has_pdf': False, 'pdf_content': None, 'message_id': 'm3'},
            {'role': 'assistant', 'content': 'A2', 'has_pdf': False, 'pdf_content': None, 'message_id': 'm4'},
        ]}
        result = format_history_for_ai(data)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['prompt'], 'Q1')
        self.assertEqual(result[1]['prompt'], 'Q2')

    def test_empty_user_content_skipped(self):
        """User message with only whitespace is skipped."""
        data = {'history': [
            {'role': 'user', 'content': '   ', 'has_pdf': False, 'pdf_content': None, 'message_id': 'm1'},
        ]}
        result = format_history_for_ai(data)
        self.assertEqual(len(result), 0)

    def test_user_with_only_pdf_content_included(self):
        """User with empty text but has PDF content is included."""
        data = {'history': [
            {'role': 'user', 'content': '', 'has_pdf': True, 'pdf_content': 'CV content here', 'message_id': 'm1'},
        ]}
        result = format_history_for_ai(data)
        # Empty text + pdf → content is just the pdf part
        self.assertEqual(len(result), 1)
        self.assertIn('CV content here', result[0]['prompt'])

    def test_consecutive_users_without_assistants(self):
        """Multiple consecutive user messages each get their own pair."""
        data = {'history': [
            {'role': 'user', 'content': 'Q1', 'has_pdf': False, 'pdf_content': None, 'message_id': 'm1'},
            {'role': 'user', 'content': 'Q2', 'has_pdf': False, 'pdf_content': None, 'message_id': 'm2'},
        ]}
        result = format_history_for_ai(data)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['response'], '')
        self.assertEqual(result[1]['response'], '')


# ════════════════════════════════════════════════════════════════════════════
# 3. build_current_message_with_pdf
# ════════════════════════════════════════════════════════════════════════════

class TestBuildCurrentMessageWithPDF(unittest.TestCase):

    def test_basic_message_no_pdf(self):
        """Message without PDF returns just the content."""
        data = {'current_message': {'content': 'Hello', 'has_pdf': False, 'pdf_content': None}}
        self.assertEqual(build_current_message_with_pdf(data), 'Hello')

    def test_with_pdf_content(self):
        """PDF content is appended to message."""
        data = {'current_message': {'content': 'See attached', 'has_pdf': True, 'pdf_content': 'CV text'}}
        result = build_current_message_with_pdf(data)
        self.assertIn('See attached', result)
        self.assertIn('[Attached PDF Content]', result)
        self.assertIn('CV text', result)

    def test_no_current_message(self):
        """No current message returns empty string."""
        self.assertEqual(build_current_message_with_pdf({'current_message': None}), '')

    def test_empty_history_data(self):
        """Empty dict returns empty string."""
        self.assertEqual(build_current_message_with_pdf({}), '')

    def test_has_pdf_true_but_no_content(self):
        """has_pdf=True but pdf_content is None → no append."""
        data = {'current_message': {'content': 'Text', 'has_pdf': True, 'pdf_content': None}}
        self.assertEqual(build_current_message_with_pdf(data), 'Text')

    def test_has_pdf_false_with_content(self):
        """has_pdf=False even with pdf_content → no append."""
        data = {'current_message': {'content': 'Text', 'has_pdf': False, 'pdf_content': 'Should not appear'}}
        self.assertEqual(build_current_message_with_pdf(data), 'Text')

    def test_empty_content_string(self):
        """Empty content string returns just the empty string (no PDF)."""
        data = {'current_message': {'content': '', 'has_pdf': False, 'pdf_content': None}}
        self.assertEqual(build_current_message_with_pdf(data), '')


# ════════════════════════════════════════════════════════════════════════════
# 4. extract_email_from_cv
# ════════════════════════════════════════════════════════════════════════════

class TestExtractEmailFromCv(unittest.TestCase):

    @patch('agent_dags.recruitment_dag.cv_listner.extract_json_from_text')
    @patch('agent_dags.recruitment_dag.cv_listner.get_ai_response')
    def test_successful_extraction(self, mock_ai, mock_json):
        """Successful email extraction from CV."""
        mock_ai.return_value = '{"email": "john@example.com", "confidence": 95}'
        mock_json.return_value = {"email": "john@example.com", "confidence": 95, "location": "header"}

        result = extract_email_from_cv("John Doe\njohn@example.com\nPython Developer", "model1")
        self.assertEqual(result, "john@example.com")

    @patch('agent_dags.recruitment_dag.cv_listner.extract_json_from_text')
    @patch('agent_dags.recruitment_dag.cv_listner.get_ai_response')
    def test_not_found(self, mock_ai, mock_json):
        """AI returns NOT_FOUND → returns None."""
        mock_ai.return_value = '{"email": "NOT_FOUND"}'
        mock_json.return_value = {"email": "NOT_FOUND", "confidence": 0}

        result = extract_email_from_cv("No contact info here", "model1")
        self.assertIsNone(result)

    @patch('agent_dags.recruitment_dag.cv_listner.extract_json_from_text')
    @patch('agent_dags.recruitment_dag.cv_listner.get_ai_response')
    def test_regression_ai_returns_invalid_json(self, mock_ai, mock_json):
        """REGRESSION: AI returns non-JSON → extract_json_from_text returns None.

        Bug: .get('email', 'NOT_FOUND') on None crashed with AttributeError.
        Fix: Added explicit None guard.
        """
        mock_ai.return_value = "I cannot find any email in this text."
        mock_json.return_value = None

        result = extract_email_from_cv("Some text", "model1")
        self.assertIsNone(result)

    @patch('agent_dags.recruitment_dag.cv_listner.get_ai_response')
    def test_exception_in_ai_call(self, mock_ai):
        """Exception during AI call returns None."""
        mock_ai.side_effect = Exception("AI service down")

        result = extract_email_from_cv("Some CV text", "model1")
        self.assertIsNone(result)

    @patch('agent_dags.recruitment_dag.cv_listner.extract_json_from_text')
    @patch('agent_dags.recruitment_dag.cv_listner.get_ai_response')
    def test_empty_email_string(self, mock_ai, mock_json):
        """Empty email string is falsy → returns None."""
        mock_ai.return_value = '{"email": ""}'
        mock_json.return_value = {"email": "", "confidence": 0}

        result = extract_email_from_cv("Text", "model1")
        self.assertIsNone(result)


# ════════════════════════════════════════════════════════════════════════════
# 5. fetch_cv_emails
# ════════════════════════════════════════════════════════════════════════════

class TestFetchCvEmails(unittest.TestCase):

    @patch('agent_dags.recruitment_dag.cv_listner.update_last_checked_timestamp')
    @patch('agent_dags.recruitment_dag.cv_listner.fetch_unread_emails_with_attachments')
    @patch('agent_dags.recruitment_dag.cv_listner.get_last_checked_timestamp')
    @patch('agent_dags.recruitment_dag.cv_listner.authenticate_gmail')
    def test_successful_fetch(self, mock_auth, mock_ts, mock_fetch, mock_update):
        """Successful fetch pushes emails to xcom and updates timestamp."""
        mock_auth.return_value = MagicMock()
        mock_ts.return_value = 1000000
        emails = [
            {'id': 'e1', 'timestamp': 2000000},
            {'id': 'e2', 'timestamp': 3000000},
        ]
        mock_fetch.return_value = emails

        kwargs = make_kwargs()
        result = fetch_cv_emails(**kwargs)

        self.assertEqual(len(result), 2)
        kwargs['ti'].xcom_push.assert_called_once_with(key='unread_emails', value=emails)
        mock_update.assert_called_once_with(
            "/appz/cache/cv_last_processed_email.json", 3000000
        )

    @patch('agent_dags.recruitment_dag.cv_listner.update_last_checked_timestamp')
    @patch('agent_dags.recruitment_dag.cv_listner.fetch_unread_emails_with_attachments')
    @patch('agent_dags.recruitment_dag.cv_listner.get_last_checked_timestamp')
    @patch('agent_dags.recruitment_dag.cv_listner.authenticate_gmail')
    def test_empty_mailbox(self, mock_auth, mock_ts, mock_fetch, mock_update):
        """No emails → doesn't update timestamp."""
        mock_auth.return_value = MagicMock()
        mock_ts.return_value = 1000000
        mock_fetch.return_value = []

        kwargs = make_kwargs()
        result = fetch_cv_emails(**kwargs)

        self.assertEqual(result, [])
        mock_update.assert_not_called()

    @patch('agent_dags.recruitment_dag.cv_listner.authenticate_gmail')
    def test_auth_failure(self, mock_auth):
        """Auth failure returns empty list."""
        mock_auth.return_value = None

        kwargs = make_kwargs()
        result = fetch_cv_emails(**kwargs)
        self.assertEqual(result, [])

    @patch('agent_dags.recruitment_dag.cv_listner.fetch_unread_emails_with_attachments')
    @patch('agent_dags.recruitment_dag.cv_listner.get_last_checked_timestamp')
    @patch('agent_dags.recruitment_dag.cv_listner.authenticate_gmail')
    def test_exception_propagated(self, mock_auth, mock_ts, mock_fetch):
        """Exceptions in fetch are re-raised."""
        mock_auth.return_value = MagicMock()
        mock_ts.return_value = 1000
        mock_fetch.side_effect = Exception("Network error")

        kwargs = make_kwargs()
        with self.assertRaises(Exception):
            fetch_cv_emails(**kwargs)


# ════════════════════════════════════════════════════════════════════════════
# 6. classify_email_type
# ════════════════════════════════════════════════════════════════════════════

class TestClassifyEmailType(unittest.TestCase):

    @patch('agent_dags.recruitment_dag.cv_listner.extract_json_from_text')
    @patch('agent_dags.recruitment_dag.cv_listner.get_ai_response')
    @patch('agent_dags.recruitment_dag.cv_listner.build_current_message_with_pdf')
    @patch('agent_dags.recruitment_dag.cv_listner.format_history_for_ai')
    @patch('agent_dags.recruitment_dag.cv_listner.extract_thread_history_with_attachments')
    @patch('agent_dags.recruitment_dag.cv_listner.get_candidate_details')
    @patch('agent_dags.recruitment_dag.cv_listner.find_candidate_in_sheet')
    @patch('agent_dags.recruitment_dag.cv_listner.authenticate_google_sheets')
    @patch('agent_dags.recruitment_dag.cv_listner.Variable')
    @patch('agent_dags.recruitment_dag.cv_listner.authenticate_gmail')
    def test_classify_new_cv(self, mock_auth, mock_var, mock_sheets_auth,
                              mock_find, mock_details, mock_thread,
                              mock_format, mock_build, mock_ai, mock_json):
        """Classifies email with CV attachment as NEW_CV_APPLICATION."""
        mock_auth.return_value = MagicMock()
        mock_var.get.return_value = "model:test"
        mock_sheets_auth.return_value = MagicMock()
        mock_find.return_value = None
        mock_thread.return_value = {'history': [], 'current_message': {'content': 'Attached CV', 'has_pdf': False, 'pdf_content': None}}
        mock_format.return_value = []
        mock_build.return_value = "Attached CV"
        mock_ai.return_value = '{"email_type": "NEW_CV_APPLICATION"}'
        mock_json.return_value = {
            "email_type": "NEW_CV_APPLICATION",
            "confidence": 95,
            "reasoning": "Has PDF",
            "target_dag": "cv_analyse"
        }

        email = {
            'id': 'e1', 'threadId': 't1',
            'headers': {'From': 'john@test.com', 'Subject': 'My CV'},
            'attachments': [{'mime_type': 'application/pdf', 'path': '/tmp/cv.pdf'}],
            'content': 'Please find my CV attached',
        }
        kwargs = make_kwargs(
            xcom_data={('fetch_cv_emails', 'unread_emails'): [email]}
        )

        result = classify_email_type(**kwargs)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['classification']['type'], 'NEW_CV_APPLICATION')

    def test_no_emails_to_classify(self):
        """No emails returns empty list."""
        kwargs = make_kwargs(
            xcom_data={('fetch_cv_emails', 'unread_emails'): None}
        )
        result = classify_email_type(**kwargs)
        self.assertEqual(result, [])

    @patch('agent_dags.recruitment_dag.cv_listner.authenticate_gmail')
    def test_auth_failure_raises(self, mock_auth):
        """Gmail auth failure raises RuntimeError."""
        mock_auth.return_value = None
        email = {'id': 'e1', 'threadId': 't1', 'headers': {'From': 'a@b.com', 'Subject': 'X'}, 'attachments': []}
        kwargs = make_kwargs(
            xcom_data={('fetch_cv_emails', 'unread_emails'): [email]}
        )
        with self.assertRaises(RuntimeError):
            classify_email_type(**kwargs)

    @patch('agent_dags.recruitment_dag.cv_listner.extract_thread_history_with_attachments')
    @patch('agent_dags.recruitment_dag.cv_listner.authenticate_google_sheets')
    @patch('agent_dags.recruitment_dag.cv_listner.Variable')
    @patch('agent_dags.recruitment_dag.cv_listner.authenticate_gmail')
    def test_classification_error_fallback_with_attachment(self, mock_auth, mock_var,
                                                           mock_sheets_auth, mock_thread):
        """Classification error with attachments → fallback NEW_CV_APPLICATION."""
        mock_auth.return_value = MagicMock()
        mock_var.get.return_value = "model:test"
        mock_sheets_auth.return_value = None  # sheets unavailable
        mock_thread.side_effect = Exception("Thread extraction failed")

        email = {
            'id': 'e1', 'threadId': 't1',
            'headers': {'From': 'john@test.com', 'Subject': 'CV'},
            'attachments': [{'mime_type': 'application/pdf'}],
            'content': 'CV attached',
        }
        kwargs = make_kwargs(
            xcom_data={('fetch_cv_emails', 'unread_emails'): [email]}
        )
        result = classify_email_type(**kwargs)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['classification']['type'], 'NEW_CV_APPLICATION')
        self.assertEqual(result[0]['classification']['confidence'], 50)

    @patch('agent_dags.recruitment_dag.cv_listner.extract_thread_history_with_attachments')
    @patch('agent_dags.recruitment_dag.cv_listner.authenticate_google_sheets')
    @patch('agent_dags.recruitment_dag.cv_listner.Variable')
    @patch('agent_dags.recruitment_dag.cv_listner.authenticate_gmail')
    def test_classification_error_fallback_without_attachment(self, mock_auth, mock_var,
                                                              mock_sheets_auth, mock_thread):
        """Classification error without attachments → fallback OTHER."""
        mock_auth.return_value = MagicMock()
        mock_var.get.return_value = "model:test"
        mock_sheets_auth.return_value = None
        mock_thread.side_effect = Exception("Failed")

        email = {
            'id': 'e2', 'threadId': 't2',
            'headers': {'From': 'john@test.com', 'Subject': 'Question'},
            'attachments': [],
            'content': 'Just a question',
        }
        kwargs = make_kwargs(
            xcom_data={('fetch_cv_emails', 'unread_emails'): [email]}
        )
        result = classify_email_type(**kwargs)
        self.assertEqual(result[0]['classification']['type'], 'OTHER')
        self.assertEqual(result[0]['classification']['target_dag'], 'none')


# ════════════════════════════════════════════════════════════════════════════
# 7. check_for_emails
# ════════════════════════════════════════════════════════════════════════════

class TestCheckForEmails(unittest.TestCase):

    def test_has_emails(self):
        """Returns route task when emails exist."""
        kwargs = make_kwargs(
            xcom_data={('classify_email_type', 'classified_emails'): [{'id': 'e1'}]}
        )
        self.assertEqual(check_for_emails(**kwargs), 'route_emails_to_dags')

    def test_no_emails_none(self):
        """None from xcom → no_emails_found."""
        kwargs = make_kwargs(
            xcom_data={('classify_email_type', 'classified_emails'): None}
        )
        self.assertEqual(check_for_emails(**kwargs), 'no_emails_found')

    def test_empty_list(self):
        """Empty list → no_emails_found."""
        kwargs = make_kwargs(
            xcom_data={('classify_email_type', 'classified_emails'): []}
        )
        self.assertEqual(check_for_emails(**kwargs), 'no_emails_found')

    def test_multiple_emails(self):
        """Multiple emails → route_emails_to_dags."""
        kwargs = make_kwargs(
            xcom_data={('classify_email_type', 'classified_emails'): [{'id': '1'}, {'id': '2'}]}
        )
        self.assertEqual(check_for_emails(**kwargs), 'route_emails_to_dags')


# ════════════════════════════════════════════════════════════════════════════
# 8. route_emails_to_dags
# ════════════════════════════════════════════════════════════════════════════

class TestRouteEmailsToDags(unittest.TestCase):

    def _make_email(self, eid, email_type, target_dag, sender='a@test.com',
                     extracted_email=None, thread_history=None):
        return {
            'id': eid,
            'classification': {
                'type': email_type,
                'target_dag': target_dag,
                'confidence': 90,
                'candidate_exists': False,
                'candidate_profile': None,
            },
            'headers': {'From': sender},
            'extracted_candidate_email': extracted_email,
            'search_email': extracted_email or sender,
            'thread_history': thread_history or {},
        }

    def test_route_to_cv_analyse(self):
        """Routes NEW_CV_APPLICATION to cv_analyse."""
        email = self._make_email('e1', 'NEW_CV_APPLICATION', 'cv_analyse')
        kwargs = make_kwargs(
            xcom_data={('classify_email_type', 'classified_emails'): [email]}
        )
        result = route_emails_to_dags(**kwargs)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['trigger_dag_id'], 'cv_analyse')

    def test_route_to_screening(self):
        """Routes SCREENING_RESPONSE to screening_response_analysis."""
        email = self._make_email('e1', 'SCREENING_RESPONSE', 'screening_response_analysis')
        kwargs = make_kwargs(
            xcom_data={('classify_email_type', 'classified_emails'): [email]}
        )
        result = route_emails_to_dags(**kwargs)
        self.assertEqual(result[0]['trigger_dag_id'], 'screening_response_analysis')

    def test_skip_other(self):
        """OTHER type is skipped (no trigger)."""
        email = self._make_email('e1', 'OTHER', 'none')
        kwargs = make_kwargs(
            xcom_data={('classify_email_type', 'classified_emails'): [email]}
        )
        result = route_emails_to_dags(**kwargs)
        self.assertEqual(len(result), 0)

    def test_no_emails(self):
        """No emails returns early (None)."""
        kwargs = make_kwargs(
            xcom_data={('classify_email_type', 'classified_emails'): None}
        )
        result = route_emails_to_dags(**kwargs)
        self.assertIsNone(result)

    def test_mixed_routing(self):
        """Mixed types: only actionable emails get triggers."""
        emails = [
            self._make_email('e1', 'NEW_CV_APPLICATION', 'cv_analyse'),
            self._make_email('e2', 'OTHER', 'none'),
            self._make_email('e3', 'SCREENING_RESPONSE', 'screening_response_analysis'),
        ]
        kwargs = make_kwargs(
            xcom_data={('classify_email_type', 'classified_emails'): emails}
        )
        result = route_emails_to_dags(**kwargs)
        self.assertEqual(len(result), 2)
        dag_ids = [r['trigger_dag_id'] for r in result]
        self.assertIn('cv_analyse', dag_ids)
        self.assertIn('screening_response_analysis', dag_ids)

    def test_regression_unexpected_target_dag(self):
        """REGRESSION: Unexpected target_dag must not crash with KeyError.

        Bug: routing_summary[target_dag] += 1 crashed if target_dag wasn't
        in the pre-initialized dict. Fix: membership check before incrementing.
        """
        email = self._make_email('e1', 'NEW_CV_APPLICATION', 'unknown_dag')
        kwargs = make_kwargs(
            xcom_data={('classify_email_type', 'classified_emails'): [email]}
        )
        # Must not raise KeyError
        result = route_emails_to_dags(**kwargs)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['trigger_dag_id'], 'unknown_dag')

    def test_other_type_skipped_even_with_target_dag(self):
        """type=OTHER is skipped even if target_dag is not 'none'."""
        email = self._make_email('e1', 'OTHER', 'cv_analyse')
        kwargs = make_kwargs(
            xcom_data={('classify_email_type', 'classified_emails'): [email]}
        )
        result = route_emails_to_dags(**kwargs)
        self.assertEqual(len(result), 0)

    def test_routing_error_continues(self):
        """Error routing one email doesn't stop processing of next."""
        # Use a real dict where classification is a string (not dict),
        # so classification.get('target_dag') raises AttributeError downstream,
        # but email.get('id') still works in the error handler.
        bad_email = {'id': 'bad', 'classification': 'not_a_dict', 'headers': {}}

        good_email = self._make_email('e2', 'NEW_CV_APPLICATION', 'cv_analyse')
        kwargs = make_kwargs(
            xcom_data={('classify_email_type', 'classified_emails'): [bad_email, good_email]}
        )
        result = route_emails_to_dags(**kwargs)
        self.assertEqual(len(result), 1)

    def test_extracted_email_counted(self):
        """Emails with extracted_candidate_email are counted in summary."""
        email = self._make_email('e1', 'NEW_CV_APPLICATION', 'cv_analyse',
                                  extracted_email='candidate@cv.com')
        kwargs = make_kwargs(
            xcom_data={('classify_email_type', 'classified_emails'): [email]}
        )
        route_emails_to_dags(**kwargs)
        push_call = kwargs['ti'].xcom_push.call_args
        summary = push_call[1]['value'] if push_call[1] else push_call[0][1]
        self.assertEqual(summary['emails_extracted_from_cv'], 1)

    def test_thread_history_counted(self):
        """Emails with non-empty thread history are counted."""
        email = self._make_email('e1', 'NEW_CV_APPLICATION', 'cv_analyse',
                                  thread_history={'history': [{'role': 'user'}]})
        kwargs = make_kwargs(
            xcom_data={('classify_email_type', 'classified_emails'): [email]}
        )
        route_emails_to_dags(**kwargs)
        push_call = kwargs['ti'].xcom_push.call_args
        summary = push_call[1]['value'] if push_call[1] else push_call[0][1]
        self.assertEqual(summary['emails_with_thread_context'], 1)


# ════════════════════════════════════════════════════════════════════════════
# 9. log_no_emails
# ════════════════════════════════════════════════════════════════════════════

class TestLogNoEmails(unittest.TestCase):

    def test_returns_message(self):
        """Returns the expected 'no emails' message."""
        result = log_no_emails(**make_kwargs())
        self.assertEqual(result, "No emails to process")

    def test_return_type(self):
        """Return value is a string."""
        self.assertIsInstance(log_no_emails(**make_kwargs()), str)


# ════════════════════════════════════════════════════════════════════════════
# 10. find_cv_in_thread
# ════════════════════════════════════════════════════════════════════════════

class TestFindCvInThread(unittest.TestCase):

    @patch('agent_dags.recruitment_dag.cv_listner.process_email_attachments')
    def test_pdf_found(self, mock_process):
        """PDF CV found in thread."""
        service = MagicMock()
        service.users().threads().get().execute.return_value = {
            'messages': [{'id': 'm1', 'payload': {'headers': []}}]
        }
        mock_process.return_value = [
            {'mime_type': 'application/pdf', 'path': '/tmp/cv.pdf', 'filename': 'cv.pdf'}
        ]
        result = find_cv_in_thread(service, 'thread1', '/tmp/att')
        self.assertIsNotNone(result)
        self.assertEqual(result[0]['mime_type'], 'application/pdf')

    @patch('agent_dags.recruitment_dag.cv_listner.process_email_attachments')
    def test_image_found(self, mock_process):
        """Image CV found in thread."""
        service = MagicMock()
        service.users().threads().get().execute.return_value = {
            'messages': [{'id': 'm1', 'payload': {'headers': []}}]
        }
        mock_process.return_value = [
            {'mime_type': 'image/jpeg', 'path': '/tmp/cv.jpg', 'filename': 'cv.jpg'}
        ]
        result = find_cv_in_thread(service, 'thread1', '/tmp/att')
        self.assertIsNotNone(result)

    @patch('agent_dags.recruitment_dag.cv_listner.process_email_attachments')
    def test_no_cv_in_any_message(self, mock_process):
        """No CV-like attachment returns None."""
        service = MagicMock()
        service.users().threads().get().execute.return_value = {
            'messages': [{'id': 'm1', 'payload': {'headers': []}}]
        }
        mock_process.return_value = [
            {'mime_type': 'text/plain', 'path': '/tmp/note.txt', 'filename': 'note.txt'}
        ]
        self.assertIsNone(find_cv_in_thread(service, 'thread1', '/tmp/att'))

    def test_api_error(self):
        """Gmail API error returns None."""
        service = MagicMock()
        service.users().threads().get().execute.side_effect = Exception("API Error")
        self.assertIsNone(find_cv_in_thread(service, 'thread1', '/tmp/att'))

    @patch('agent_dags.recruitment_dag.cv_listner.process_email_attachments')
    def test_empty_thread(self, mock_process):
        """Empty thread (no messages) returns None."""
        service = MagicMock()
        service.users().threads().get().execute.return_value = {'messages': []}
        self.assertIsNone(find_cv_in_thread(service, 'thread1', '/tmp/att'))
        mock_process.assert_not_called()

    @patch('agent_dags.recruitment_dag.cv_listner.process_email_attachments')
    def test_empty_attachments_list(self, mock_process):
        """Messages with no attachments → returns None."""
        service = MagicMock()
        service.users().threads().get().execute.return_value = {
            'messages': [{'id': 'm1', 'payload': {'headers': []}}]
        }
        mock_process.return_value = []
        self.assertIsNone(find_cv_in_thread(service, 'thread1', '/tmp/att'))


# ════════════════════════════════════════════════════════════════════════════
# 11. extract_thread_history_with_attachments
# ════════════════════════════════════════════════════════════════════════════

class TestExtractThreadHistoryWithAttachments(unittest.TestCase):

    def _make_service(self, messages, bot_email='bot@test.com'):
        service = MagicMock()
        service.users().threads().get().execute.return_value = {'messages': messages}
        service.users().getProfile().execute.return_value = {'emailAddress': bot_email}
        return service

    def _make_msg(self, msg_id, sender, body='', ts='1000'):
        return {
            'id': msg_id,
            'internalDate': ts,
            'payload': {
                'headers': [{'name': 'From', 'value': sender}],
                'body': {'data': _b64(body)} if body else {'size': 0},
            }
        }

    @patch('agent_dags.recruitment_dag.cv_listner.process_email_attachments')
    def test_basic_thread(self, mock_process):
        """Multi-message thread extracts history and current."""
        mock_process.return_value = []
        msgs = [
            self._make_msg('m1', 'user@test.com', 'Hello', '1000'),
            self._make_msg('m2', 'bot@test.com', 'Hi', '2000'),
            self._make_msg('m3', 'user@test.com', 'Thanks', '3000'),
        ]
        service = self._make_service(msgs)
        result = extract_thread_history_with_attachments(service, 't1', '/tmp', current_message_id='m3')

        self.assertEqual(len(result['history']), 2)
        self.assertEqual(result['current_message']['message_id'], 'm3')

    @patch('agent_dags.recruitment_dag.cv_listner.process_email_attachments')
    def test_empty_thread(self, mock_process):
        """Empty thread returns empty history."""
        service = self._make_service([])
        result = extract_thread_history_with_attachments(service, 't1', '/tmp')
        self.assertEqual(result['history'], [])
        self.assertIsNone(result['current_message'])

    @patch('agent_dags.recruitment_dag.cv_listner.process_email_attachments')
    def test_no_current_message_id_uses_last(self, mock_process):
        """No current_message_id → last message is current."""
        mock_process.return_value = []
        msgs = [
            self._make_msg('m1', 'user@test.com', 'First', '1000'),
            self._make_msg('m2', 'user@test.com', 'Last', '2000'),
        ]
        service = self._make_service(msgs)
        result = extract_thread_history_with_attachments(service, 't1', '/tmp')

        self.assertEqual(result['current_message']['message_id'], 'm2')
        self.assertEqual(len(result['history']), 1)

    @patch('agent_dags.recruitment_dag.cv_listner.process_email_attachments')
    def test_role_assignment(self, mock_process):
        """Role is 'assistant' for bot sender, 'user' for others."""
        mock_process.return_value = []
        msgs = [
            self._make_msg('m1', 'candidate@test.com', 'Q', '1000'),
            self._make_msg('m2', 'bot@test.com', 'A', '2000'),
        ]
        service = self._make_service(msgs, bot_email='bot@test.com')
        result = extract_thread_history_with_attachments(service, 't1', '/tmp', current_message_id='extra')

        self.assertEqual(result['history'][0]['role'], 'user')
        self.assertEqual(result['history'][1]['role'], 'assistant')

    def test_api_error_returns_empty(self):
        """API error returns empty result."""
        service = MagicMock()
        service.users().threads().get().execute.side_effect = Exception("Error")
        result = extract_thread_history_with_attachments(service, 't1', '/tmp')
        self.assertEqual(result['history'], [])
        self.assertIsNone(result['current_message'])

    @patch('agent_dags.recruitment_dag.cv_listner.process_email_attachments')
    def test_single_message_no_id(self, mock_process):
        """Single message without current_message_id → it becomes current."""
        mock_process.return_value = []
        msgs = [self._make_msg('m1', 'user@test.com', 'Solo', '1000')]
        service = self._make_service(msgs)
        result = extract_thread_history_with_attachments(service, 't1', '/tmp')

        self.assertEqual(len(result['history']), 0)
        self.assertEqual(result['current_message']['message_id'], 'm1')

    @patch('agent_dags.recruitment_dag.cv_listner.process_email_attachments')
    def test_message_error_continues(self, mock_process):
        """Error in one message doesn't stop processing of others."""
        call_count = [0]
        def side_effect(**kw):
            call_count[0] += 1
            if kw['message_id'] == 'm1':
                raise Exception("Bad message")
            return []
        mock_process.side_effect = side_effect

        msgs = [
            self._make_msg('m1', 'user@test.com', 'Bad', '1000'),
            self._make_msg('m2', 'user@test.com', 'Good', '2000'),
        ]
        service = self._make_service(msgs)
        result = extract_thread_history_with_attachments(service, 't1', '/tmp', current_message_id='m2')

        self.assertIsNotNone(result['current_message'])
        self.assertEqual(result['current_message']['message_id'], 'm2')


if __name__ == '__main__':
    unittest.main()
