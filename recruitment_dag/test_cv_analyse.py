"""
Comprehensive edge-case unit tests for cv_analyse.py — CV Analysis DAG.
Tests all 7 functions with regression tests for fixed bugs.

Run with:
    python3 -m unittest agent_dags.recruitment_dag.test_cv_analyse -v
from the airflow/dags directory.
"""

import json
import sys
import unittest
import os
from unittest.mock import patch, MagicMock, mock_open

# ---------------------------------------------------------------------------
# Patch Airflow and external modules into sys.modules BEFORE importing the
# module under test, because cv_analyse.py calls Variable.get() at module
# level and several dependencies may not be installed in the test environment.
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

from agent_dags.recruitment_dag.cv_analyse import (
    extract_cv_content,
    retrive_jd_from_web,
    get_the_jd_for_cv_analysis,
    calculate_candidate_score,
    get_the_score_for_cv_analysis,
    save_to_google_sheets,
    send_response_email,
)


# ── helpers ──────────────────────────────────────────────────────────────────

def make_kwargs(xcom_data=None, conf=None):
    """Build mock Airflow **kwargs."""
    ti = MagicMock()
    if xcom_data:
        def _pull(task_ids=None, key=None):
            return xcom_data.get((task_ids, key))
        ti.xcom_pull.side_effect = _pull

    dag_run = MagicMock()
    dag_run.conf = conf or {}

    return {'ti': ti, 'dag_run': dag_run}


# ════════════════════════════════════════════════════════════════════════════
# 1. extract_cv_content
# ════════════════════════════════════════════════════════════════════════════

class TestExtractCvContent(unittest.TestCase):

    def test_pdf_in_current_email(self):
        """Extracts PDF content from current email attachment."""
        email_data = {
            'id': 'e1',
            'headers': {'From': 'John <john@test.com>', 'Subject': 'My CV'},
            'attachments': [{
                'filename': 'resume.pdf',
                'mime_type': 'application/pdf',
                'extracted_content': {'content': 'John Doe Resume', 'metadata': [{'page': 1}]},
                'path': '/tmp/resume.pdf',
            }],
            'content': 'Please find my CV',
            'thread_history': {},
        }
        kwargs = make_kwargs(conf={'email_data': email_data})
        result = extract_cv_content(**kwargs)

        self.assertIsNotNone(result)
        self.assertEqual(len(result['attachments']), 1)
        self.assertEqual(result['attachments'][0]['type'], 'pdf')
        self.assertEqual(result['attachments'][0]['content'], 'John Doe Resume')

    def test_image_attachment(self):
        """Stores image attachment with base64 content."""
        email_data = {
            'id': 'e1',
            'headers': {'From': 'test@test.com', 'Subject': 'CV'},
            'attachments': [{
                'filename': 'scan.png',
                'mime_type': 'image/png',
                'base64_content': 'base64data==',
                'path': '/tmp/scan.png',
            }],
            'content': '',
            'thread_history': {},
        }
        kwargs = make_kwargs(conf={'email_data': email_data})
        result = extract_cv_content(**kwargs)

        self.assertEqual(len(result['attachments']), 1)
        self.assertEqual(result['attachments'][0]['type'], 'image')
        self.assertEqual(result['attachments'][0]['base64_content'], 'base64data==')

    def test_no_email_data(self):
        """No email data returns None."""
        kwargs = make_kwargs(conf={})
        result = extract_cv_content(**kwargs)
        self.assertIsNone(result)

    def test_fallback_to_original_cv_content(self):
        """Falls back to thread_history.original_cv_content when no PDF in current."""
        email_data = {
            'id': 'e1',
            'headers': {'From': 'test@test.com', 'Subject': 'Follow up'},
            'attachments': [],
            'content': 'Regarding my application',
            'thread_history': {
                'original_cv_content': 'Original CV text here',
                'original_cv_path': '/tmp/original.pdf',
            },
        }
        kwargs = make_kwargs(conf={'email_data': email_data})
        result = extract_cv_content(**kwargs)

        self.assertEqual(len(result['attachments']), 1)
        self.assertEqual(result['attachments'][0]['content'], 'Original CV text here')

    def test_fallback_to_history_messages(self):
        """Falls back to scanning history messages for user PDFs."""
        email_data = {
            'id': 'e1',
            'headers': {'From': 'test@test.com', 'Subject': 'Follow up'},
            'attachments': [],
            'content': 'Any update?',
            'thread_history': {
                'history': [
                    {'role': 'user', 'has_pdf': True, 'pdf_content': 'Historical CV', 'message_id': 'old1'},
                    {'role': 'assistant', 'has_pdf': False, 'pdf_content': None, 'message_id': 'old2'},
                ],
            },
        }
        kwargs = make_kwargs(conf={'email_data': email_data})
        result = extract_cv_content(**kwargs)

        self.assertEqual(len(result['attachments']), 1)
        self.assertEqual(result['attachments'][0]['content'], 'Historical CV')
        self.assertEqual(result['attachments'][0]['filename'], 'historical_resume.pdf')

    def test_no_cv_anywhere(self):
        """No CV found in current email or thread history."""
        email_data = {
            'id': 'e1',
            'headers': {'From': 'test@test.com', 'Subject': 'Question'},
            'attachments': [],
            'content': 'Just a question',
            'thread_history': {'history': []},
        }
        kwargs = make_kwargs(conf={'email_data': email_data})
        result = extract_cv_content(**kwargs)

        self.assertEqual(len(result['attachments']), 0)

    def test_multiple_attachments(self):
        """Handles multiple attachments (PDF + image)."""
        email_data = {
            'id': 'e1',
            'headers': {'From': 'test@test.com', 'Subject': 'CV'},
            'attachments': [
                {
                    'filename': 'resume.pdf',
                    'mime_type': 'application/pdf',
                    'extracted_content': {'content': 'PDF content', 'metadata': []},
                    'path': '/tmp/resume.pdf',
                },
                {
                    'filename': 'photo.jpg',
                    'mime_type': 'image/jpeg',
                    'base64_content': 'imgdata==',
                    'path': '/tmp/photo.jpg',
                },
            ],
            'content': '',
            'thread_history': {},
        }
        kwargs = make_kwargs(conf={'email_data': email_data})
        result = extract_cv_content(**kwargs)
        self.assertEqual(len(result['attachments']), 2)

    def test_empty_pdf_content(self):
        """PDF with empty extracted content is not counted as found."""
        email_data = {
            'id': 'e1',
            'headers': {'From': 'test@test.com', 'Subject': 'CV'},
            'attachments': [{
                'filename': 'empty.pdf',
                'mime_type': 'application/pdf',
                'extracted_content': {'content': '', 'metadata': []},
                'path': '/tmp/empty.pdf',
            }],
            'content': '',
            'thread_history': {},
        }
        kwargs = make_kwargs(conf={'email_data': email_data})
        result = extract_cv_content(**kwargs)
        # The attachment is still added (empty content), but pdf_found stays False
        # so thread history fallback would trigger if available
        self.assertEqual(len(result['attachments']), 0)

    def test_unsupported_mime_type_skipped(self):
        """Non-PDF, non-image attachments are skipped."""
        email_data = {
            'id': 'e1',
            'headers': {'From': 'test@test.com', 'Subject': 'Docs'},
            'attachments': [{
                'filename': 'notes.docx',
                'mime_type': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                'path': '/tmp/notes.docx',
            }],
            'content': '',
            'thread_history': {},
        }
        kwargs = make_kwargs(conf={'email_data': email_data})
        result = extract_cv_content(**kwargs)
        self.assertEqual(len(result['attachments']), 0)

    def test_history_only_assistant_pdfs_skipped(self):
        """Only user PDFs in history are used for fallback, not assistant PDFs."""
        email_data = {
            'id': 'e1',
            'headers': {'From': 'test@test.com', 'Subject': 'Follow up'},
            'attachments': [],
            'content': 'Any news?',
            'thread_history': {
                'history': [
                    {'role': 'assistant', 'has_pdf': True, 'pdf_content': 'Bot PDF', 'message_id': 'a1'},
                ],
            },
        }
        kwargs = make_kwargs(conf={'email_data': email_data})
        result = extract_cv_content(**kwargs)
        self.assertEqual(len(result['attachments']), 0)


# ════════════════════════════════════════════════════════════════════════════
# 2. retrive_jd_from_web
# ════════════════════════════════════════════════════════════════════════════

class TestRetriveJdFromWeb(unittest.TestCase):

    @patch('agent_dags.recruitment_dag.cv_analyse.Variable')
    @patch('agent_dags.recruitment_dag.cv_analyse.extract_json_from_text')
    @patch('agent_dags.recruitment_dag.cv_analyse.get_ai_response')
    def test_successful_retrieval(self, mock_ai, mock_json, mock_var):
        """Successful JD retrieval pushes to xcom."""
        mock_var.get.return_value = "model:test"
        mock_ai.return_value = '{"job_name": "ML Engineer"}'
        jd = [{"job_name": "ML Engineer", "summary": "Build ML models"}]
        mock_json.return_value = jd

        kwargs = make_kwargs(xcom_data={
            ('extract_cv_content', 'cv_data'): {'attachments': []},
            ('extract_cv_content', 'email_content'): 'Looking for ML role',
        })
        result = retrive_jd_from_web(**kwargs)

        self.assertEqual(result, jd)
        kwargs['ti'].xcom_push.assert_called_once_with(key='jd_data', value=jd)

    @patch('agent_dags.recruitment_dag.cv_analyse.Variable')
    @patch('agent_dags.recruitment_dag.cv_analyse.extract_json_from_text')
    @patch('agent_dags.recruitment_dag.cv_analyse.get_ai_response')
    def test_ai_returns_none(self, mock_ai, mock_json, mock_var):
        """AI returns non-parseable response → None pushed."""
        mock_var.get.return_value = "model:test"
        mock_ai.return_value = "No matching jobs found."
        mock_json.return_value = None

        kwargs = make_kwargs(xcom_data={
            ('extract_cv_content', 'cv_data'): {'attachments': []},
            ('extract_cv_content', 'email_content'): 'Test',
        })
        result = retrive_jd_from_web(**kwargs)
        self.assertIsNone(result)


# ════════════════════════════════════════════════════════════════════════════
# 3. get_the_jd_for_cv_analysis
# ════════════════════════════════════════════════════════════════════════════

class TestGetTheJdForCvAnalysis(unittest.TestCase):

    @patch('agent_dags.recruitment_dag.cv_analyse.Variable')
    @patch('agent_dags.recruitment_dag.cv_analyse.extract_json_from_text')
    @patch('agent_dags.recruitment_dag.cv_analyse.get_ai_response')
    def test_successful_match_and_enrich(self, mock_ai, mock_json, mock_var):
        """Successful two-stage AI call returns enriched JD."""
        mock_var.get.return_value = "model:test"
        mock_ai.side_effect = ['match response', 'enrich response']
        mock_json.side_effect = [
            {"job_title": "ML Engineer", "job_summary": "Build ML pipelines"},
            {"job_title": "ML Engineer", "job_description": "Full JD", "Must have skills": ["Python"]},
        ]

        kwargs = make_kwargs(xcom_data={
            ('retrive_jd_from_web', 'jd_data'): [{"job_name": "ML Engineer"}],
            ('extract_cv_content', 'cv_data'): {'attachments': []},
            ('extract_cv_content', 'email_content'): 'ML role',
        })
        result = get_the_jd_for_cv_analysis(**kwargs)

        self.assertIsNotNone(result)
        self.assertEqual(result['job_title'], 'ML Engineer')

    @patch('agent_dags.recruitment_dag.cv_analyse.Variable')
    @patch('agent_dags.recruitment_dag.cv_analyse.extract_json_from_text')
    @patch('agent_dags.recruitment_dag.cv_analyse.get_ai_response')
    def test_regression_ai_returns_none_for_match(self, mock_ai, mock_json, mock_var):
        """REGRESSION: AI returns non-JSON for match → should return None.

        Bug: matched_job["job_title"] crashed with TypeError when
        extract_json_from_text returned None.
        """
        mock_var.get.return_value = "model:test"
        mock_ai.return_value = "I cannot determine a match."
        mock_json.return_value = None

        kwargs = make_kwargs(xcom_data={
            ('retrive_jd_from_web', 'jd_data'): [],
            ('extract_cv_content', 'cv_data'): {'attachments': []},
            ('extract_cv_content', 'email_content'): 'Test',
        })
        result = get_the_jd_for_cv_analysis(**kwargs)
        self.assertIsNone(result)

    @patch('agent_dags.recruitment_dag.cv_analyse.Variable')
    @patch('agent_dags.recruitment_dag.cv_analyse.extract_json_from_text')
    @patch('agent_dags.recruitment_dag.cv_analyse.get_ai_response')
    def test_regression_missing_job_title_key(self, mock_ai, mock_json, mock_var):
        """REGRESSION: AI returns dict without job_title → should use default.

        Bug: matched_job["job_title"] crashed with KeyError.
        """
        mock_var.get.return_value = "model:test"
        mock_ai.side_effect = ['match response', 'enrich response']
        mock_json.side_effect = [
            {"some_other_key": "value"},  # no job_title
            {"job_title": "Unknown Position", "job_description": "..."},
        ]

        kwargs = make_kwargs(xcom_data={
            ('retrive_jd_from_web', 'jd_data'): [],
            ('extract_cv_content', 'cv_data'): {'attachments': []},
            ('extract_cv_content', 'email_content'): 'Test',
        })
        # Should not raise KeyError
        result = get_the_jd_for_cv_analysis(**kwargs)
        self.assertIsNotNone(result)


# ════════════════════════════════════════════════════════════════════════════
# 4. calculate_candidate_score (pure function — thorough testing)
# ════════════════════════════════════════════════════════════════════════════

class TestCalculateCandidateScore(unittest.TestCase):

    def test_all_skills_matched(self):
        """All skills + experience + education matched → 100% eligible."""
        data = {
            'must_have_skills': [{'skill_name': 'Python', 'match': True}, {'skill_name': 'ML', 'match': True}],
            'nice_to_have_skills': [{'skill_name': 'Docker', 'match': True}],
            'experience_match': {'match': True},
            'education_match': {'match': True},
        }
        result = calculate_candidate_score(data)
        self.assertEqual(result['must_have_score'], 100)
        self.assertEqual(result['nice_to_have_score'], 100)
        self.assertEqual(result['other_criteria_score'], 100)
        self.assertEqual(result['total_score'], 100)
        self.assertTrue(result['eligible'])
        self.assertEqual(result['remarks'], 'Eligible.')

    def test_no_skills_matched(self):
        """No skills matched → 0% ineligible."""
        data = {
            'must_have_skills': [{'skill_name': 'Python', 'match': False}],
            'nice_to_have_skills': [{'skill_name': 'Docker', 'match': False}],
            'experience_match': {'match': False},
            'education_match': {'match': False},
        }
        result = calculate_candidate_score(data)
        self.assertEqual(result['total_score'], 0)
        self.assertFalse(result['eligible'])
        self.assertIn('missing Must-Have skills', result['remarks'])
        self.assertIn('experience mismatch', result['remarks'])
        self.assertIn('education mismatch', result['remarks'])

    def test_partial_must_have_match(self):
        """2 of 3 must-have matched → 67% must-have avg, weighted 60%."""
        data = {
            'must_have_skills': [
                {'skill_name': 'A', 'match': True},
                {'skill_name': 'B', 'match': True},
                {'skill_name': 'C', 'match': False},
            ],
            'nice_to_have_skills': [],
            'experience_match': {'match': False},
            'education_match': {'match': False},
        }
        result = calculate_candidate_score(data)
        # must_have_avg = round(200/3) = 67
        self.assertEqual(result['must_have_score'], 67)
        # weighted: (67/100)*60 = 40.2 → total = round(40.2) = 40
        self.assertEqual(result['total_score'], 40)
        self.assertFalse(result['eligible'])

    def test_threshold_exactly_50(self):
        """Score of exactly 50 → eligible."""
        # Need must_have to give exactly 50 when weighted
        # (83/100)*60 = 49.8 ≈ 50? Let's find exact values
        # 5 of 6 must-have skills = 500/6 = 83.33 → round = 83
        # (83/100)*60 = 49.8 → round(49.8) = 50 ✓
        data = {
            'must_have_skills': [
                {'skill_name': str(i), 'match': i < 5} for i in range(6)
            ],
            'nice_to_have_skills': [],
            'experience_match': {'match': False},
            'education_match': {'match': False},
        }
        result = calculate_candidate_score(data)
        self.assertEqual(result['total_score'], 50)
        self.assertTrue(result['eligible'])

    def test_threshold_49_ineligible(self):
        """Score of 49 → ineligible."""
        # 4 of 5 must-have: avg = 80, weighted = 48
        data = {
            'must_have_skills': [
                {'skill_name': str(i), 'match': i < 4} for i in range(5)
            ],
            'nice_to_have_skills': [],
            'experience_match': {'match': False},
            'education_match': {'match': False},
        }
        result = calculate_candidate_score(data)
        self.assertEqual(result['total_score'], 48)
        self.assertFalse(result['eligible'])

    def test_empty_skills_lists(self):
        """Empty skill lists → avg defaults to 0."""
        data = {
            'must_have_skills': [],
            'nice_to_have_skills': [],
            'experience_match': {'match': True},
            'education_match': {'match': True},
        }
        result = calculate_candidate_score(data)
        self.assertEqual(result['must_have_score'], 0)
        self.assertEqual(result['nice_to_have_score'], 0)
        self.assertEqual(result['other_criteria_score'], 100)
        # Only 10% from other = 10
        self.assertEqual(result['total_score'], 10)

    def test_regression_none_must_have_skills(self):
        """REGRESSION: must_have_skills is explicitly None → should not crash.

        Bug: .get('must_have_skills', []) returned None (not []) when key
        existed with value null. The for loop then crashed with TypeError.
        Fix: Used `or []` pattern.
        """
        data = {
            'must_have_skills': None,
            'nice_to_have_skills': [{'skill_name': 'A', 'match': True}],
            'experience_match': {'match': True},
            'education_match': {'match': True},
        }
        result = calculate_candidate_score(data)
        self.assertEqual(result['must_have_score'], 0)
        self.assertEqual(result['nice_to_have_score'], 100)

    def test_regression_none_nice_to_have_skills(self):
        """REGRESSION: nice_to_have_skills is None → should not crash."""
        data = {
            'must_have_skills': [{'skill_name': 'A', 'match': True}],
            'nice_to_have_skills': None,
            'experience_match': {'match': True},
            'education_match': {'match': True},
        }
        result = calculate_candidate_score(data)
        self.assertEqual(result['nice_to_have_score'], 0)

    def test_regression_none_experience_match(self):
        """REGRESSION: experience_match is None → should not crash."""
        data = {
            'must_have_skills': [],
            'nice_to_have_skills': [],
            'experience_match': None,
            'education_match': {'match': True},
        }
        result = calculate_candidate_score(data)
        # experience treated as not matched
        self.assertEqual(result['other_criteria_score'], 50)

    def test_regression_none_education_match(self):
        """REGRESSION: education_match is None → should not crash."""
        data = {
            'must_have_skills': [],
            'nice_to_have_skills': [],
            'experience_match': {'match': True},
            'education_match': None,
        }
        result = calculate_candidate_score(data)
        self.assertEqual(result['other_criteria_score'], 50)

    def test_only_experience_and_education(self):
        """No skills at all, both experience and education matched → 10%."""
        data = {
            'must_have_skills': [],
            'nice_to_have_skills': [],
            'experience_match': {'match': True},
            'education_match': {'match': True},
        }
        result = calculate_candidate_score(data)
        self.assertEqual(result['total_score'], 10)
        self.assertFalse(result['eligible'])

    def test_single_must_have_skill_matched(self):
        """Single must-have skill matched → 60% from must-have."""
        data = {
            'must_have_skills': [{'skill_name': 'Python', 'match': True}],
            'nice_to_have_skills': [],
            'experience_match': {'match': False},
            'education_match': {'match': False},
        }
        result = calculate_candidate_score(data)
        self.assertEqual(result['must_have_score'], 100)
        self.assertEqual(result['total_score'], 60)
        self.assertTrue(result['eligible'])

    def test_all_criteria_false(self):
        """Everything false → 0% with multiple ineligibility reasons."""
        data = {
            'must_have_skills': [{'skill_name': 'A', 'match': False}, {'skill_name': 'B', 'match': False}],
            'nice_to_have_skills': [{'skill_name': 'C', 'match': False}],
            'experience_match': {'match': False},
            'education_match': {'match': False},
        }
        result = calculate_candidate_score(data)
        self.assertEqual(result['total_score'], 0)
        self.assertIn('Ineligible due to', result['remarks'])

    def test_ineligibility_reasons_deduplicated(self):
        """Multiple missing must-haves → single 'missing Must-Have skills' reason."""
        data = {
            'must_have_skills': [
                {'skill_name': 'A', 'match': False},
                {'skill_name': 'B', 'match': False},
                {'skill_name': 'C', 'match': False},
            ],
            'nice_to_have_skills': [],
            'experience_match': {'match': True},
            'education_match': {'match': True},
        }
        result = calculate_candidate_score(data)
        # "missing Must-Have skills" should appear only once
        count = result['remarks'].count('missing Must-Have skills')
        self.assertEqual(count, 1)

    def test_weighted_calculation_accuracy(self):
        """Verify exact weighted calculation: 60% must + 30% nice + 10% other."""
        data = {
            'must_have_skills': [{'skill_name': 'A', 'match': True}],  # 100
            'nice_to_have_skills': [{'skill_name': 'B', 'match': True}],  # 100
            'experience_match': {'match': True},  # 100
            'education_match': {'match': False},  # 0 → avg=50
        }
        result = calculate_candidate_score(data)
        # must: (100/100)*60 = 60
        # nice: (100/100)*30 = 30
        # other: (50/100)*10 = 5
        # total: 95
        self.assertEqual(result['total_score'], 95)

    def test_missing_match_key_defaults_false(self):
        """Skill without 'match' key defaults to False."""
        data = {
            'must_have_skills': [{'skill_name': 'A'}],  # no 'match' key
            'nice_to_have_skills': [],
            'experience_match': {},  # no 'match' key
            'education_match': {},
        }
        result = calculate_candidate_score(data)
        self.assertEqual(result['must_have_score'], 0)
        self.assertEqual(result['other_criteria_score'], 0)


# ════════════════════════════════════════════════════════════════════════════
# 5. get_the_score_for_cv_analysis
# ════════════════════════════════════════════════════════════════════════════

class TestGetTheScoreForCvAnalysis(unittest.TestCase):

    @patch('pathlib.Path.mkdir')
    @patch('builtins.open', new_callable=mock_open)
    @patch('agent_dags.recruitment_dag.cv_analyse.Variable')
    @patch('agent_dags.recruitment_dag.cv_analyse.extract_json_from_text')
    @patch('agent_dags.recruitment_dag.cv_analyse.get_ai_response')
    def test_successful_scoring(self, mock_ai, mock_json, mock_var,
                                 mock_file, mock_mkdir):
        """Successful scoring saves file and pushes to xcom."""
        mock_var.get.return_value = "model:test"
        mock_ai.return_value = '{"candidate_name": "John"}'
        score_data = {
            'candidate_name': 'John Doe',
            'candidate_email': 'john@test.com',
            'job_title': 'ML Engineer',
            'must_have_skills': [{'skill_name': 'Python', 'match': True}],
            'nice_to_have_skills': [],
            'experience_match': {'match': True},
            'education_match': {'match': True},
        }
        mock_json.return_value = score_data

        cv_data = {
            'attachments': [{'type': 'pdf', 'content': 'CV text'}],
        }
        kwargs = make_kwargs(xcom_data={
            ('extract_cv_content', 'cv_data'): cv_data,
            ('get_the_jd_for_cv_analysis', 'jd_data'): {'job_title': 'ML Engineer'},
        })
        result = get_the_score_for_cv_analysis(**kwargs)

        self.assertIsNotNone(result)
        self.assertIn('total_score', result)
        self.assertIn('original_cv_content', result)
        kwargs['ti'].xcom_push.assert_called_once()

    @patch('agent_dags.recruitment_dag.cv_analyse.Variable')
    def test_missing_cv_data(self, mock_var):
        """Missing cv_data returns None."""
        mock_var.get.return_value = "model:test"
        kwargs = make_kwargs(xcom_data={
            ('extract_cv_content', 'cv_data'): None,
            ('get_the_jd_for_cv_analysis', 'jd_data'): {'job_title': 'X'},
        })
        result = get_the_score_for_cv_analysis(**kwargs)
        self.assertIsNone(result)

    @patch('agent_dags.recruitment_dag.cv_analyse.Variable')
    def test_missing_jd_data(self, mock_var):
        """Missing jd_data returns None."""
        mock_var.get.return_value = "model:test"
        kwargs = make_kwargs(xcom_data={
            ('extract_cv_content', 'cv_data'): {'attachments': []},
            ('get_the_jd_for_cv_analysis', 'jd_data'): None,
        })
        result = get_the_score_for_cv_analysis(**kwargs)
        self.assertIsNone(result)

    @patch('agent_dags.recruitment_dag.cv_analyse.Variable')
    @patch('agent_dags.recruitment_dag.cv_analyse.extract_json_from_text')
    @patch('agent_dags.recruitment_dag.cv_analyse.get_ai_response')
    def test_regression_ai_returns_none(self, mock_ai, mock_json, mock_var):
        """REGRESSION: AI returns non-JSON → should return None.

        Bug: calculate_candidate_score(None) crashed with AttributeError.
        """
        mock_var.get.return_value = "model:test"
        mock_ai.return_value = "Cannot analyze"
        mock_json.return_value = None

        kwargs = make_kwargs(xcom_data={
            ('extract_cv_content', 'cv_data'): {'attachments': []},
            ('get_the_jd_for_cv_analysis', 'jd_data'): {'job_title': 'X'},
        })
        result = get_the_score_for_cv_analysis(**kwargs)
        self.assertIsNone(result)

    @patch('pathlib.Path.mkdir', side_effect=PermissionError("No write access"))
    @patch('agent_dags.recruitment_dag.cv_analyse.Variable')
    @patch('agent_dags.recruitment_dag.cv_analyse.extract_json_from_text')
    @patch('agent_dags.recruitment_dag.cv_analyse.get_ai_response')
    def test_file_save_failure_continues(self, mock_ai, mock_json, mock_var, mock_mkdir):
        """File save failure logs error but doesn't crash."""
        mock_var.get.return_value = "model:test"
        mock_ai.return_value = '{}'
        mock_json.return_value = {
            'candidate_email': 'test@test.com',
            'must_have_skills': [],
            'nice_to_have_skills': [],
            'experience_match': {'match': False},
            'education_match': {'match': False},
        }

        kwargs = make_kwargs(xcom_data={
            ('extract_cv_content', 'cv_data'): {'attachments': []},
            ('get_the_jd_for_cv_analysis', 'jd_data'): {'job_title': 'X'},
        })
        # Should not raise despite file save failure
        result = get_the_score_for_cv_analysis(**kwargs)
        self.assertIsNotNone(result)


# ════════════════════════════════════════════════════════════════════════════
# 6. save_to_google_sheets
# ════════════════════════════════════════════════════════════════════════════

class TestSaveToGoogleSheets(unittest.TestCase):

    @patch('agent_dags.recruitment_dag.cv_analyse.append_candidate_to_sheet')
    @patch('agent_dags.recruitment_dag.cv_analyse.find_candidate_in_sheet')
    @patch('agent_dags.recruitment_dag.cv_analyse.authenticate_google_sheets')
    @patch('agent_dags.recruitment_dag.cv_analyse.Variable')
    def test_new_candidate(self, mock_var, mock_auth, mock_find, mock_append):
        """New candidate is appended to sheet."""
        mock_var.get.return_value = "oauth"
        mock_auth.return_value = MagicMock()
        mock_find.return_value = None
        mock_append.return_value = True

        score_data = {
            'candidate_email': 'new@test.com',
            'candidate_name': 'New Person',
            'job_title': 'Engineer',
            'total_score': 80,
            'must_have_score': 90,
            'nice_to_have_score': 70,
            'other_criteria_score': 80,
            'eligible': True,
            'remarks': 'Eligible.',
            'experience_match': {'candidate_experience_years': '5'},
            'education_match': {'candidate_education': 'MS CS'},
        }
        kwargs = make_kwargs(xcom_data={
            ('get_the_score_for_cv_analysis', 'score_data'): score_data,
            ('extract_cv_content', 'cv_data'): {'subject': 'CV', 'sender': 'new@test.com'},
        })
        result = save_to_google_sheets(**kwargs)
        self.assertEqual(result, "Success")
        mock_append.assert_called_once()

    @patch('agent_dags.recruitment_dag.cv_analyse.update_candidate_status')
    @patch('agent_dags.recruitment_dag.cv_analyse.find_candidate_in_sheet')
    @patch('agent_dags.recruitment_dag.cv_analyse.authenticate_google_sheets')
    @patch('agent_dags.recruitment_dag.cv_analyse.Variable')
    def test_existing_candidate(self, mock_var, mock_auth, mock_find, mock_update):
        """Existing candidate triggers status update."""
        mock_var.get.return_value = "oauth"
        mock_auth.return_value = MagicMock()
        mock_find.return_value = {'email': 'existing@test.com'}
        mock_update.return_value = True

        score_data = {
            'candidate_email': 'existing@test.com',
            'candidate_name': 'Old Person',
            'job_title': 'Engineer',
            'eligible': True,
            'experience_match': {'candidate_experience_years': '3'},
            'education_match': {'candidate_education': 'BS CS'},
        }
        kwargs = make_kwargs(xcom_data={
            ('get_the_score_for_cv_analysis', 'score_data'): score_data,
            ('extract_cv_content', 'cv_data'): {'subject': 'CV', 'sender': 'existing@test.com'},
        })
        result = save_to_google_sheets(**kwargs)
        self.assertEqual(result, "Success")
        mock_update.assert_called_once()

    def test_missing_score_data(self):
        """Missing score_data returns 'Missing data'."""
        kwargs = make_kwargs(xcom_data={
            ('get_the_score_for_cv_analysis', 'score_data'): None,
            ('extract_cv_content', 'cv_data'): {'subject': 'CV'},
        })
        result = save_to_google_sheets(**kwargs)
        self.assertEqual(result, "Missing data")

    def test_missing_cv_data(self):
        """Missing cv_data returns 'Missing data'."""
        kwargs = make_kwargs(xcom_data={
            ('get_the_score_for_cv_analysis', 'score_data'): {'candidate_email': 'x'},
            ('extract_cv_content', 'cv_data'): None,
        })
        result = save_to_google_sheets(**kwargs)
        self.assertEqual(result, "Missing data")

    @patch('agent_dags.recruitment_dag.cv_analyse.authenticate_google_sheets')
    @patch('agent_dags.recruitment_dag.cv_analyse.Variable')
    def test_auth_failure(self, mock_var, mock_auth):
        """Auth failure returns 'Authentication failed'."""
        mock_var.get.return_value = "oauth"
        mock_auth.return_value = None

        score_data = {'candidate_email': 'x', 'experience_match': {}, 'education_match': {}}
        kwargs = make_kwargs(xcom_data={
            ('get_the_score_for_cv_analysis', 'score_data'): score_data,
            ('extract_cv_content', 'cv_data'): {'subject': 'CV'},
        })
        result = save_to_google_sheets(**kwargs)
        self.assertEqual(result, "Authentication failed")

    @patch('agent_dags.recruitment_dag.cv_analyse.append_candidate_to_sheet')
    @patch('agent_dags.recruitment_dag.cv_analyse.find_candidate_in_sheet')
    @patch('agent_dags.recruitment_dag.cv_analyse.authenticate_google_sheets')
    @patch('agent_dags.recruitment_dag.cv_analyse.Variable')
    def test_regression_none_experience_match(self, mock_var, mock_auth, mock_find, mock_append):
        """REGRESSION: experience_match is explicitly None → should not crash.

        Bug: .get('experience_match', {}).get(...) crashed when value was None.
        Fix: (... or {}).get(...)
        """
        mock_var.get.return_value = "oauth"
        mock_auth.return_value = MagicMock()
        mock_find.return_value = None
        mock_append.return_value = True

        score_data = {
            'candidate_email': 'test@test.com',
            'candidate_name': 'Test',
            'job_title': 'Dev',
            'experience_match': None,  # explicitly None
            'education_match': {'candidate_education': 'BS'},
            'eligible': True,
        }
        kwargs = make_kwargs(xcom_data={
            ('get_the_score_for_cv_analysis', 'score_data'): score_data,
            ('extract_cv_content', 'cv_data'): {'subject': 'CV', 'sender': 'test@test.com'},
        })
        # Must not crash
        result = save_to_google_sheets(**kwargs)
        self.assertEqual(result, "Success")

    @patch('agent_dags.recruitment_dag.cv_analyse.append_candidate_to_sheet')
    @patch('agent_dags.recruitment_dag.cv_analyse.find_candidate_in_sheet')
    @patch('agent_dags.recruitment_dag.cv_analyse.authenticate_google_sheets')
    @patch('agent_dags.recruitment_dag.cv_analyse.Variable')
    def test_regression_none_education_match(self, mock_var, mock_auth, mock_find, mock_append):
        """REGRESSION: education_match is explicitly None → should not crash."""
        mock_var.get.return_value = "oauth"
        mock_auth.return_value = MagicMock()
        mock_find.return_value = None
        mock_append.return_value = True

        score_data = {
            'candidate_email': 'test@test.com',
            'candidate_name': 'Test',
            'job_title': 'Dev',
            'experience_match': {'candidate_experience_years': '2'},
            'education_match': None,  # explicitly None
            'eligible': False,
        }
        kwargs = make_kwargs(xcom_data={
            ('get_the_score_for_cv_analysis', 'score_data'): score_data,
            ('extract_cv_content', 'cv_data'): {'subject': 'CV', 'sender': 'test@test.com'},
        })
        result = save_to_google_sheets(**kwargs)
        self.assertEqual(result, "Success")


# ════════════════════════════════════════════════════════════════════════════
# 7. send_response_email
# ════════════════════════════════════════════════════════════════════════════

class TestSendResponseEmail(unittest.TestCase):

    def _base_setup(self, eligible=True, sender='John <john@test.com>',
                     subject='My Application', extracted_email=None):
        """Build common email_data, score_data, cv_data for tests."""
        email_data = {
            'id': 'e1',
            'threadId': 't1',
            'headers': {
                'From': sender,
                'Subject': subject,
                'Message-ID': '<msg1@test.com>',
                'References': '',
            },
            'extracted_candidate_email': extracted_email,
        }
        score_data = {
            'eligible': eligible,
            'candidate_name': 'John Doe',
            'total_score': 80 if eligible else 30,
        }
        cv_data = {
            'subject': subject,
            'sender': sender,
            'attachments': [{'type': 'pdf', 'content': 'CV text'}],
        }
        return email_data, score_data, cv_data

    @patch('agent_dags.recruitment_dag.cv_analyse.send_email')
    @patch('agent_dags.recruitment_dag.cv_analyse.authenticate_gmail')
    def test_rejection_email(self, mock_auth, mock_send):
        """Ineligible candidate gets rejection email."""
        mock_auth.return_value = MagicMock()
        mock_send.return_value = {'id': 'sent1'}

        email_data, score_data, cv_data = self._base_setup(eligible=False)
        kwargs = make_kwargs(
            xcom_data={
                ('get_the_score_for_cv_analysis', 'score_data'): score_data,
                ('extract_cv_content', 'cv_data'): cv_data,
                ('extract_cv_content', 'email_content'): 'Original email',
                ('get_the_jd_for_cv_analysis', 'jd_data'): {},
            },
            conf={'email_data': email_data},
        )
        result = send_response_email(**kwargs)
        self.assertIn('sent successfully', result)
        # Check rejection body contains expected text
        # send_email(service, recipient, subject, body, ...) — body is index [3]
        sent_body = mock_send.call_args[0][3]
        self.assertIn('unable to proceed', sent_body)

    @patch('agent_dags.recruitment_dag.cv_analyse.Variable')
    @patch('agent_dags.recruitment_dag.cv_analyse.send_email')
    @patch('agent_dags.recruitment_dag.cv_analyse.authenticate_gmail')
    @patch('agent_dags.recruitment_dag.cv_analyse.get_ai_response')
    def test_selection_email(self, mock_ai, mock_auth, mock_send, mock_var):
        """Eligible candidate gets AI-generated selection email."""
        mock_var.get.return_value = "model:test"
        mock_auth.return_value = MagicMock()
        mock_send.return_value = {'id': 'sent1'}
        mock_ai.return_value = "```html\n<p>Congratulations!</p>\n```"

        email_data, score_data, cv_data = self._base_setup(eligible=True)
        kwargs = make_kwargs(
            xcom_data={
                ('get_the_score_for_cv_analysis', 'score_data'): score_data,
                ('extract_cv_content', 'cv_data'): cv_data,
                ('extract_cv_content', 'email_content'): 'My application',
                ('get_the_jd_for_cv_analysis', 'jd_data'): {'job_title': 'Engineer'},
            },
            conf={'email_data': email_data},
        )
        result = send_response_email(**kwargs)
        self.assertIn('sent successfully', result)

    @patch('agent_dags.recruitment_dag.cv_analyse.send_email')
    @patch('agent_dags.recruitment_dag.cv_analyse.authenticate_gmail')
    def test_forwarded_cv_routing(self, mock_auth, mock_send):
        """Forwarded CV: sends to candidate, CC to original sender."""
        mock_auth.return_value = MagicMock()
        mock_send.return_value = {'id': 'sent1'}

        email_data, score_data, cv_data = self._base_setup(
            eligible=False,
            sender='Recruiter <recruiter@company.com>',
            extracted_email='candidate@personal.com',
        )
        kwargs = make_kwargs(
            xcom_data={
                ('get_the_score_for_cv_analysis', 'score_data'): score_data,
                ('extract_cv_content', 'cv_data'): cv_data,
                ('extract_cv_content', 'email_content'): '',
                ('get_the_jd_for_cv_analysis', 'jd_data'): {},
            },
            conf={'email_data': email_data},
        )
        result = send_response_email(**kwargs)

        # send_email(service, recipient, subject, body, ...) — recipient is index [1]
        call_args = mock_send.call_args
        recipient = call_args[0][1]
        self.assertEqual(recipient, 'candidate@personal.com')
        # CC should be the original sender (recruiter)
        self.assertEqual(call_args[1]['cc'], 'recruiter@company.com')

    def test_missing_data(self):
        """Missing data returns error message."""
        kwargs = make_kwargs(
            xcom_data={
                ('get_the_score_for_cv_analysis', 'score_data'): None,
                ('extract_cv_content', 'cv_data'): None,
                ('extract_cv_content', 'email_content'): None,
                ('get_the_jd_for_cv_analysis', 'jd_data'): None,
            },
            conf={'email_data': {}},
        )
        result = send_response_email(**kwargs)
        self.assertIn('Missing data', result)

    @patch('agent_dags.recruitment_dag.cv_analyse.authenticate_gmail')
    def test_auth_failure(self, mock_auth):
        """Gmail auth failure returns error."""
        mock_auth.return_value = None

        email_data, score_data, cv_data = self._base_setup(eligible=False)
        kwargs = make_kwargs(
            xcom_data={
                ('get_the_score_for_cv_analysis', 'score_data'): score_data,
                ('extract_cv_content', 'cv_data'): cv_data,
                ('extract_cv_content', 'email_content'): '',
                ('get_the_jd_for_cv_analysis', 'jd_data'): {},
            },
            conf={'email_data': email_data},
        )
        result = send_response_email(**kwargs)
        self.assertEqual(result, "Gmail authentication failed")

    @patch('agent_dags.recruitment_dag.cv_analyse.send_email')
    @patch('agent_dags.recruitment_dag.cv_analyse.authenticate_gmail')
    def test_regression_no_double_re_prefix(self, mock_auth, mock_send):
        """REGRESSION: Subject without Re: → send_email receives original subject.

        Bug: Code added "Re:" manually via `subject = f"Re: {subject}"`,
        then send_email() added another "Re:". Fix: Removed the manual prefix.
        """
        mock_auth.return_value = MagicMock()
        mock_send.return_value = {'id': 'sent1'}

        email_data, score_data, cv_data = self._base_setup(
            eligible=False, subject='Job Application'
        )
        kwargs = make_kwargs(
            xcom_data={
                ('get_the_score_for_cv_analysis', 'score_data'): score_data,
                ('extract_cv_content', 'cv_data'): cv_data,
                ('extract_cv_content', 'email_content'): '',
                ('get_the_jd_for_cv_analysis', 'jd_data'): {},
            },
            conf={'email_data': email_data},
        )
        send_response_email(**kwargs)

        # send_email(service, recipient, subject, body, ...) — subject is index [2]
        sent_subject = mock_send.call_args[0][2]
        self.assertEqual(sent_subject, 'Job Application')
        self.assertFalse(sent_subject.startswith('Re:'))

    @patch('agent_dags.recruitment_dag.cv_analyse.send_email')
    @patch('agent_dags.recruitment_dag.cv_analyse.authenticate_gmail')
    def test_regression_subject_already_has_re(self, mock_auth, mock_send):
        """REGRESSION: Subject already with 'Re:' → no double prefix."""
        mock_auth.return_value = MagicMock()
        mock_send.return_value = {'id': 'sent1'}

        email_data, score_data, cv_data = self._base_setup(
            eligible=False, subject='Re: Job Application'
        )
        kwargs = make_kwargs(
            xcom_data={
                ('get_the_score_for_cv_analysis', 'score_data'): score_data,
                ('extract_cv_content', 'cv_data'): cv_data,
                ('extract_cv_content', 'email_content'): '',
                ('get_the_jd_for_cv_analysis', 'jd_data'): {},
            },
            conf={'email_data': email_data},
        )
        send_response_email(**kwargs)

        # send_email(service, recipient, subject, body, ...) — subject is index [2]
        sent_subject = mock_send.call_args[0][2]
        self.assertEqual(sent_subject, 'Re: Job Application')
        self.assertFalse(sent_subject.startswith('Re: Re:'))

    @patch('agent_dags.recruitment_dag.cv_analyse.send_email')
    @patch('agent_dags.recruitment_dag.cv_analyse.authenticate_gmail')
    def test_send_email_failure(self, mock_auth, mock_send):
        """send_email returns None → returns failure message."""
        mock_auth.return_value = MagicMock()
        mock_send.return_value = None

        email_data, score_data, cv_data = self._base_setup(eligible=False)
        kwargs = make_kwargs(
            xcom_data={
                ('get_the_score_for_cv_analysis', 'score_data'): score_data,
                ('extract_cv_content', 'cv_data'): cv_data,
                ('extract_cv_content', 'email_content'): '',
                ('get_the_jd_for_cv_analysis', 'jd_data'): {},
            },
            conf={'email_data': email_data},
        )
        result = send_response_email(**kwargs)
        self.assertEqual(result, "Failed to send email")

    @patch('agent_dags.recruitment_dag.cv_analyse.send_email')
    @patch('agent_dags.recruitment_dag.cv_analyse.authenticate_gmail')
    def test_invalid_sender_email(self, mock_auth, mock_send):
        """Empty sender email returns error."""
        mock_auth.return_value = MagicMock()

        email_data = {
            'id': 'e1', 'threadId': 't1',
            'headers': {'From': '', 'Subject': 'Test', 'Message-ID': '<m1>', 'References': ''},
            'extracted_candidate_email': None,
        }
        score_data = {'eligible': False}
        cv_data = {'subject': 'Test', 'sender': ''}

        kwargs = make_kwargs(
            xcom_data={
                ('get_the_score_for_cv_analysis', 'score_data'): score_data,
                ('extract_cv_content', 'cv_data'): cv_data,
                ('extract_cv_content', 'email_content'): '',
                ('get_the_jd_for_cv_analysis', 'jd_data'): {},
            },
            conf={'email_data': email_data},
        )
        result = send_response_email(**kwargs)
        self.assertEqual(result, "Invalid sender email")


if __name__ == '__main__':
    unittest.main()
