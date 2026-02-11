"""
Comprehensive tests for HubSpot DAG functions.

Test Scenarios:
1. Meeting minutes with everything new (contacts, companies, deals, meetings, notes, tasks)
2. Add a note or task to an existing deal
3. Existing contacts and new company

All external dependencies (Airflow, Gmail API, HubSpot API, Ollama AI) are mocked
via sys.modules stubs so the DAG files can be imported without Airflow installed.
"""

import json
import sys
import os
import types
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta, timezone

# ============================================================================
# STEP 1: Create stub modules for Airflow and other dependencies
#          BEFORE the DAG files are imported.
# ============================================================================

MOCK_VARIABLES = {
    "ltai.v3.hubspot.from.address": "bot@lowtouch.ai",
    "ltai.v3.hubspot.gmail.credentials": json.dumps({
        "client_id": "test", "client_secret": "test",
        "refresh_token": "test", "token": "test",
        "token_uri": "https://oauth2.googleapis.com/token",
        "scopes": ["https://www.googleapis.com/auth/gmail.modify"],
    }),
    "ltai.v3.hubspot.ollama.host": "http://localhost:8000",
    "ltai.v3.husbpot.api.key": "test-api-key",
    "ltai.v3.hubspot.url": "https://api.hubapi.com",
    "ltai.v3.hubspot.default.owner.id": "159242825",
    "ltai.v3.hubspot.default.owner.name": "Kishore",
    "ltai.v3.hubspot.task.owners": json.dumps([
        {"id": "159242825", "name": "Kishore", "email": "kishore@test.com", "timezone": "Asia/Kolkata"},
        {"id": "200000001", "name": "Vivek", "email": "vivek@test.com", "timezone": "Asia/Kolkata"},
    ]),
    "hubspot_retry_tracker": json.dumps({}),
    "ltai.v3.hubspot.ui.url": "https://app.hubspot.com",
    "ltai.v3.hubspot.task.ui.url": "https://app.hubspot.com/tasks",
}


class _MockVariable:
    """Mimics airflow.models.Variable with in-memory store."""

    @staticmethod
    def get(key, default_var=None, deserialize_json=False):
        val = MOCK_VARIABLES.get(key, default_var)
        if deserialize_json and isinstance(val, str):
            return json.loads(val)
        return val

    @staticmethod
    def set(key, value):
        MOCK_VARIABLES[key] = value


def _make_module(name, attrs=None):
    """Create a stub module and register it in sys.modules."""
    mod = types.ModuleType(name)
    if attrs:
        for k, v in attrs.items():
            setattr(mod, k, v)
    sys.modules[name] = mod
    return mod


def _setup_airflow_stubs():
    """Install stub airflow.* modules so DAG files can be imported."""

    # airflow
    airflow = _make_module("airflow", {"DAG": MagicMock()})

    # airflow.models
    models = _make_module("airflow.models", {"Variable": _MockVariable})

    # airflow.operators.*
    _make_module("airflow.operators", {})
    _make_module("airflow.operators.python", {
        "PythonOperator": MagicMock(),
        "BranchPythonOperator": MagicMock(),
    })
    _make_module("airflow.operators.dummy", {"DummyOperator": MagicMock()})
    _make_module("airflow.operators.trigger_dagrun", {"TriggerDagRunOperator": MagicMock()})

    # airflow.decorators
    _make_module("airflow.decorators", {"task": lambda f: f})

    # airflow.api.*
    _make_module("airflow.api", {})
    _make_module("airflow.api.common", {})
    _make_module("airflow.api.common.trigger_dag", {"trigger_dag": MagicMock()})


def _setup_ollama_stub():
    """Stub the ollama module."""
    ollama = _make_module("ollama", {"Client": MagicMock()})


def _ensure_google_stubs():
    """Only stub google modules if they're not already installed."""
    for mod_name in [
        "google", "google.oauth2", "google.oauth2.credentials",
        "googleapiclient", "googleapiclient.discovery", "googleapiclient.errors",
    ]:
        if mod_name not in sys.modules:
            _make_module(mod_name, {
                "Credentials": MagicMock(),
                "build": MagicMock(),
                "HttpError": type("HttpError", (Exception,), {}),
            })


def _ensure_bs4_stub():
    """Only stub bs4 if not installed."""
    if "bs4" not in sys.modules:
        mock_soup = MagicMock()
        mock_soup.return_value.get_text.return_value = "clean text"
        _make_module("bs4", {"BeautifulSoup": mock_soup})


def _ensure_optional_stubs():
    """Stub optional packages if not installed."""
    for mod in ["openpyxl", "openpyxl.utils", "openpyxl.styles",
                "pandas", "pytz", "dateutil", "dateutil.parser"]:
        if mod not in sys.modules:
            _make_module(mod, {
                "get_column_letter": MagicMock(),
                "Font": MagicMock(),
                "timezone": MagicMock(),
                "parser": MagicMock(),
            })


# Install ALL stubs before any DAG imports
_setup_airflow_stubs()
_setup_ollama_stub()
_ensure_google_stubs()
_ensure_bs4_stub()
_ensure_optional_stubs()

# Add the module directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# NOW import the DAG modules
import hubspot_email_listener as listener
import hubspot_object_creator as creator
import hubspot_search as search


# ============================================================================
# Helper: Mock XCom-capable TaskInstance
# ============================================================================

class MockXCom:
    """Simulates Airflow XCom push/pull."""

    def __init__(self, initial_data=None):
        self._store = {}
        if initial_data:
            for key, value in initial_data.items():
                self._store[key] = value

    def xcom_push(self, key, value):
        self._store[key] = value

    def xcom_pull(self, key=None, task_ids=None, default=None,
                  dag_id=None, include_prior_dates=False):
        if key in self._store:
            return self._store[key]
        return default

    def get(self, key, default=None):
        return self._store.get(key, default)


class MockTaskInstance:
    def __init__(self, try_number=1, max_tries=3):
        self.try_number = try_number
        self.max_tries = max_tries
        self.task_id = "test_task"


class MockDagRun:
    def __init__(self, conf=None, dag_id="test_dag", run_id="test_run"):
        self.conf = conf or {}
        self.dag_id = dag_id
        self.run_id = run_id


def make_context(dag_run_conf=None, try_number=1, max_tries=3):
    dag_run = MockDagRun(conf=dag_run_conf or {})
    task_instance = MockTaskInstance(try_number=try_number, max_tries=max_tries)
    return {
        "dag_run": dag_run,
        "task_instance": task_instance,
        "ti": MockXCom(),
        "execution_date": datetime.now(),
    }


# ============================================================================
# Shared email fixture
# ============================================================================

def _base_email_data(subject="Meeting Minutes - Acme Corp",
                     sender="Vivek Kumar <vivek@acme.com>",
                     body=""):
    return {
        "id": "msg_001",
        "threadId": "thread_001",
        "content": body,
        "headers": {
            "From": sender,
            "To": "bot@lowtouch.ai",
            "Cc": "",
            "Subject": subject,
            "Message-ID": "<msg001@mail.gmail.com>",
            "References": "",
            "In-Reply-To": "",
        },
    }


# ============================================================================
# SCENARIO 1: Meeting Minutes — Everything New
# ============================================================================

class TestMeetingMinutesEverythingNew:
    """
    User sends meeting minutes → new contacts, company, deal, meeting,
    notes, and tasks are created.
    """

    EMAIL_BODY = (
        "Hi, here are the meeting minutes from our call with Acme Corp.\n\n"
        "Attendees: John Smith (john@acme.com), Jane Doe (jane@acme.com)\n"
        "Company: Acme Corp\n"
        "Deal: Acme Corp - Cloud Migration Project, amount 50000, stage Lead\n"
        "Meeting: Discovery Call on 2026-02-10 10:00 to 11:00 at Zoom\n"
        "Notes: Discussed cloud migration timeline and budget.\n"
        "Tasks:\n"
        "1. Send proposal to John by 2026-02-20 (owner: Kishore, priority: HIGH)\n"
        "2. Schedule follow-up demo by 2026-02-25 (owner: Vivek, priority: MEDIUM)\n"
        "Please proceed."
    )

    ANALYSIS_RESULT = {
        "status": "success",
        "user_intent": "PROCEED",
        "casual_comments_detected": False,
        "entities_to_create": {
            "contacts": [
                {"firstname": "John", "lastname": "Smith", "email": "john@acme.com",
                 "phone": "", "address": "", "jobtitle": ""},
                {"firstname": "Jane", "lastname": "Doe", "email": "jane@acme.com",
                 "phone": "", "address": "", "jobtitle": ""},
            ],
            "companies": [
                {"name": "Acme Corp", "domain": "acme.com", "address": "",
                 "city": "", "state": "", "zip": "", "country": "", "phone": "",
                 "description": "Cloud services prospect", "type": "PROSPECT"},
            ],
            "deals": [
                {"dealName": "Acme Corp-Cloud Migration Project",
                 "dealLabelName": "Lead", "dealAmount": "50000",
                 "closeDate": "2026-05-10", "dealOwnerName": "Kishore"},
            ],
            "meetings": [
                {"meeting_title": "Discovery Call - Acme Corp",
                 "start_time": "2026-02-10T10:00:00Z",
                 "end_time": "2026-02-10T11:00:00Z",
                 "location": "Zoom",
                 "outcome": "Discussed cloud migration timeline and budget",
                 "timestamp": "2026-02-10T10:00:00Z",
                 "attendees": ["John Smith", "Jane Doe", "Vivek Kumar"],
                 "meeting_type": "discovery_call",
                 "meeting_status": "COMPLETED"},
            ],
            "notes": [
                {"note_content": "Vivek Kumar mentioned: Discussed cloud migration "
                                 "timeline and budget. John mentioned Q3 target.",
                 "timestamp": "2026-02-10 10:00:00",
                 "note_type": "meeting_note",
                 "speaker_name": "Vivek Kumar",
                 "speaker_email": "vivek@acme.com"},
            ],
            "tasks": [
                {"task_details": "Send proposal to John",
                 "task_owner_name": "Kishore", "task_owner_id": "159242825",
                 "due_date": "2026-02-20", "priority": "HIGH", "task_index": 1},
                {"task_details": "Schedule follow-up demo",
                 "task_owner_name": "Vivek", "task_owner_id": "200000001",
                 "due_date": "2026-02-25", "priority": "MEDIUM", "task_index": 2},
            ],
        },
        "entities_to_update": {
            "contacts": [], "companies": [], "deals": [],
            "meetings": [], "notes": [], "tasks": [],
        },
        "selected_entities": {"contacts": [], "companies": [], "deals": []},
        "reasoning": "User sent meeting minutes. Creating all entities.",
        "tasks_to_execute": [
            "determine_owner", "check_task_threshold",
            "create_contacts", "create_companies", "create_deals",
            "create_meetings", "create_notes", "create_tasks",
            "create_associations", "compose_response_html",
            "collect_and_save_results", "send_final_email",
        ],
        "should_determine_owner": True,
        "should_check_task_threshold": True,
    }

    OWNER_INFO = {
        "contact_owner_id": "159242825",
        "contact_owner_name": "Kishore",
        "contact_owner_message": "Default owner assigned.",
        "deal_owner_id": "159242825",
        "deal_owner_name": "Kishore",
        "deal_owner_message": "Default owner assigned.",
        "task_owners": [
            {"task_index": 1, "task_owner_id": "159242825",
             "task_owner_name": "Kishore",
             "task_owner_message": "Kishore assigned."},
            {"task_index": 2, "task_owner_id": "200000001",
             "task_owner_name": "Vivek",
             "task_owner_message": "Vivek assigned."},
        ],
        "all_owners_table": [
            {"id": "159242825", "name": "Kishore", "email": "kishore@test.com"},
            {"id": "200000001", "name": "Vivek", "email": "vivek@test.com"},
        ],
    }

    # -- Tests ---------------------------------------------------------------

    def test_load_context_from_dag_run(self):
        """load_context_from_dag_run extracts conf into XCom correctly."""
        email_data = _base_email_data(body=self.EMAIL_BODY)
        conf = {
            "thread_id": "thread_001",
            "email_data": email_data,
            "chat_history": [{"role": "user", "content": "hello"}],
            "thread_history": [
                {"content": self.EMAIL_BODY,
                 "headers": email_data["headers"],
                 "from_bot": False}
            ],
            "search_results": {},
        }
        ctx = make_context(dag_run_conf=conf)
        ti = ctx["ti"]
        result = creator.load_context_from_dag_run(ti, **ctx)

        assert result["thread_id"] == "thread_001"
        assert result["email_data"]["threadId"] == "thread_001"
        assert ti.xcom_pull(key="thread_id") == "thread_001"
        assert ti.xcom_pull(key="latest_message") == self.EMAIL_BODY

    @patch.object(creator, "get_ai_response")
    def test_analyze_user_response_meeting_minutes(self, mock_ai):
        """analyze_user_response correctly parses AI JSON for meeting minutes."""
        ai_json = {
            "casual_comments_detected": False,
            "user_intent": "PROCEED",
            "selected_entities": {"contacts": [], "companies": [], "deals": []},
            "entities_to_create": self.ANALYSIS_RESULT["entities_to_create"],
            "entities_to_update": self.ANALYSIS_RESULT["entities_to_update"],
            "reasoning": "User sent meeting minutes with all new entities.",
        }
        mock_ai.return_value = json.dumps(ai_json)

        email_data = _base_email_data(body=self.EMAIL_BODY)
        ti = MockXCom({
            "chat_history": [{"role": "user", "content": self.EMAIL_BODY}],
            "thread_history": [
                {"content": self.EMAIL_BODY,
                 "headers": email_data["headers"], "from_bot": False}
            ],
            "thread_id": "thread_001",
            "latest_message": self.EMAIL_BODY,
            "email_data": email_data,
        })
        ctx = make_context()
        ctx["ti"] = ti

        result = creator.analyze_user_response(ti, **ctx)

        assert result["status"] == "success"
        assert result["user_intent"] == "PROCEED"
        assert len(result["entities_to_create"]["contacts"]) == 2
        assert len(result["entities_to_create"]["companies"]) == 1
        assert len(result["entities_to_create"]["deals"]) == 1
        assert len(result["entities_to_create"]["meetings"]) == 1
        assert len(result["entities_to_create"]["notes"]) == 1
        assert len(result["entities_to_create"]["tasks"]) == 2
        # All creation tasks should be scheduled
        for t in ["create_contacts", "create_companies", "create_deals",
                   "create_meetings", "create_notes", "create_tasks",
                   "determine_owner"]:
            assert t in result["tasks_to_execute"], f"{t} missing from tasks_to_execute"

    def test_branch_to_creation_tasks_all_entities(self):
        """branch returns all creation task IDs for meeting minutes scenario."""
        ti = MockXCom({"analysis_results": self.ANALYSIS_RESULT})
        ctx = make_context()
        ctx["ti"] = ti

        result = creator.branch_to_creation_tasks(ti, **ctx)

        for t in ["create_contacts", "create_companies", "create_deals",
                   "create_meetings", "create_notes", "create_tasks",
                   "determine_owner", "check_task_threshold"]:
            assert t in result

    @patch.object(creator, "get_ai_response")
    def test_create_contacts_success(self, mock_ai):
        """create_contacts creates 2 new contacts successfully."""
        mock_ai.return_value = json.dumps({
            "status": "success",
            "created_contacts": [
                {"id": "501", "details": {"firstname": "John", "lastname": "Smith",
                                          "email": "john@acme.com",
                                          "contactOwnerName": "Kishore"}},
                {"id": "502", "details": {"firstname": "Jane", "lastname": "Doe",
                                          "email": "jane@acme.com",
                                          "contactOwnerName": "Kishore"}},
            ],
            "errors": [], "reason": "",
        })
        ti = MockXCom({
            "analysis_results": self.ANALYSIS_RESULT,
            "chat_history": [],
            "owner_info": self.OWNER_INFO,
        })
        ctx = make_context()
        ctx["ti"] = ti

        result = creator.create_contacts(ti, **ctx)

        assert len(result) == 2
        assert result[0]["id"] == "501"
        assert result[1]["details"]["email"] == "jane@acme.com"
        assert ti.xcom_pull(key="contact_creation_final_status") == "success"

    @patch.object(creator, "get_ai_response")
    def test_create_companies_success(self, mock_ai):
        """create_companies creates Acme Corp."""
        mock_ai.return_value = json.dumps({
            "status": "success",
            "created_companies": [
                {"id": "601", "details": {"name": "Acme Corp",
                                          "domain": "acme.com",
                                          "type": "PROSPECT"}},
            ],
            "errors": [], "reason": "",
        })
        ti = MockXCom({
            "analysis_results": self.ANALYSIS_RESULT,
            "chat_history": [],
        })
        ctx = make_context()
        ctx["ti"] = ti

        result = creator.create_companies(ti, **ctx)

        assert len(result) == 1
        assert result[0]["details"]["name"] == "Acme Corp"
        assert ti.xcom_pull(key="company_creation_final_status") == "success"

    @patch.object(creator, "get_ai_response")
    def test_create_deals_success(self, mock_ai):
        """create_deals creates the Cloud Migration deal with correct owner."""
        mock_ai.return_value = json.dumps({
            "status": "success",
            "created_deals": [
                {"id": "701", "details": {
                    "dealName": "Acme Corp-Cloud Migration Project",
                    "dealLabelName": "Lead", "dealAmount": "50000",
                    "closeDate": "2026-05-10", "dealOwnerName": "Kishore"}},
            ],
            "errors": [], "reason": "",
        })
        ti = MockXCom({
            "analysis_results": self.ANALYSIS_RESULT,
            "chat_history": [],
            "owner_info": self.OWNER_INFO,
        })
        ctx = make_context()
        ctx["ti"] = ti

        result = creator.create_deals(ti, **ctx)

        assert len(result) == 1
        assert result[0]["details"]["dealName"] == "Acme Corp-Cloud Migration Project"
        assert result[0]["details"]["dealAmount"] == "50000"
        assert ti.xcom_pull(key="deal_creation_final_status") == "success"

    @patch.object(creator, "get_ai_response")
    def test_create_meetings_success(self, mock_ai):
        """create_meetings creates the discovery call meeting."""
        mock_ai.return_value = json.dumps({
            "status": "success",
            "created_meetings": [
                {"id": "801", "details": {
                    "meeting_title": "Discovery Call - Acme Corp",
                    "start_time": "2026-02-10T10:00:00Z",
                    "end_time": "2026-02-10T11:00:00Z",
                    "location": "Zoom",
                    "outcome": "Discussed cloud migration timeline and budget",
                    "attendees": ["John Smith", "Jane Doe", "Vivek Kumar"]}},
            ],
            "errors": [], "reason": "",
        })
        ti = MockXCom({
            "analysis_results": self.ANALYSIS_RESULT,
            "chat_history": [],
        })
        ctx = make_context()
        ctx["ti"] = ti

        result = creator.create_meetings(ti, **ctx)

        assert len(result) == 1
        assert result[0]["details"]["meeting_title"] == "Discovery Call - Acme Corp"
        assert "John Smith" in result[0]["details"]["attendees"]
        assert ti.xcom_pull(key="meeting_creation_final_status") == "success"

    @patch.object(creator, "get_ai_response")
    def test_create_notes_success(self, mock_ai):
        """create_notes creates meeting note preserving speaker name."""
        mock_ai.return_value = json.dumps({
            "status": "success",
            "created_notes": [
                {"id": "901", "details": {
                    "note_content": "Vivek Kumar mentioned: Discussed cloud migration "
                                    "timeline and budget. John mentioned Q3 target.",
                    "timestamp": "2026-02-10"}},
            ],
            "errors": [], "reason": "",
        })
        ti = MockXCom({
            "analysis_results": self.ANALYSIS_RESULT,
            "chat_history": [],
        })
        ctx = make_context()
        ctx["ti"] = ti

        result = creator.create_notes(ti, **ctx)

        assert len(result) == 1
        assert "Vivek Kumar" in result[0]["details"]["note_content"]
        assert ti.xcom_pull(key="note_creation_status")["status"] == "success"

    @patch.object(creator, "get_ai_response")
    def test_create_tasks_with_correct_owners(self, mock_ai):
        """create_tasks assigns correct owners (Kishore + Vivek)."""
        mock_ai.return_value = json.dumps({
            "status": "success",
            "created_tasks": [
                {"id": "1001", "details": {
                    "task_details": "Send proposal to John",
                    "task_owner_name": "Kishore", "task_owner_id": "159242825",
                    "due_date": "2026-02-20", "priority": "HIGH", "task_index": 1}},
                {"id": "1002", "details": {
                    "task_details": "Schedule follow-up demo",
                    "task_owner_name": "Vivek", "task_owner_id": "200000001",
                    "due_date": "2026-02-25", "priority": "MEDIUM", "task_index": 2}},
            ],
            "errors": [], "reason": "",
        })
        ti = MockXCom({
            "analysis_results": self.ANALYSIS_RESULT,
            "chat_history": [],
            "owner_info": self.OWNER_INFO,
        })
        ctx = make_context()
        ctx["ti"] = ti

        result = creator.create_tasks(ti, **ctx)

        assert len(result) == 2
        assert result[0]["details"]["task_owner_id"] == "159242825"
        assert result[1]["details"]["task_owner_id"] == "200000001"
        assert result[0]["details"]["priority"] == "HIGH"
        assert result[1]["details"]["priority"] == "MEDIUM"
        assert ti.xcom_pull(key="task_creation_final_status") == "success"

    def test_collect_and_save_results_all_entities(self):
        """collect_and_save_results aggregates all created entities."""
        ti = MockXCom({
            "created_contacts": [{"id": "501"}, {"id": "502"}],
            "created_companies": [{"id": "601"}],
            "created_deals": [{"id": "701"}],
            "created_meetings": [{"id": "801"}],
            "created_notes": [{"id": "901"}],
            "created_tasks": [{"id": "1001"}, {"id": "1002"}],
            "updated_contacts": [], "updated_companies": [],
            "updated_deals": [], "updated_meetings": [],
            "updated_notes": [], "updated_tasks": [],
            "associations_created": [
                {"single": {"contact_id": "501", "company_id": "601"}}
            ],
            "analysis_results": self.ANALYSIS_RESULT,
        })
        ctx = make_context()
        ctx["ti"] = ti

        result = creator.collect_and_save_results(ti, **ctx)

        assert result["created_contacts"]["total"] == 2
        assert result["created_companies"]["total"] == 1
        assert result["created_deals"]["total"] == 1
        assert result["created_meetings"]["total"] == 1
        assert result["created_notes"]["total"] == 1
        assert result["created_tasks"]["total"] == 2
        assert result["associations_created"]["total"] == 1

    def test_empty_contacts_no_ai_call(self):
        """create_contacts returns [] when list is empty, no AI call made."""
        analysis = {
            "entities_to_create": {"contacts": []},
        }
        ti = MockXCom({
            "analysis_results": analysis,
            "chat_history": [], "owner_info": {},
        })
        ctx = make_context()
        ctx["ti"] = ti

        result = creator.create_contacts(ti, **ctx)

        assert result == []
        assert ti.xcom_pull(key="contact_creation_final_status") == "success"


# ============================================================================
# SCENARIO 2: Add Note/Task to Existing Deal
# ============================================================================

class TestAddNoteTaskToExistingDeal:
    """
    User wants to add a note and task to an already-existing deal.
    No new contacts/companies/deals are created.
    """

    EMAIL_BODY = (
        "Please add a note to the Acme Corp - Cloud Migration deal:\n"
        "Note: Client confirmed Q3 budget allocation for phase 1.\n"
        "Also add a task: Follow up on contract signing by 2026-03-01 "
        "(owner: Kishore, priority: HIGH)"
    )

    ANALYSIS_RESULT = {
        "status": "success",
        "user_intent": "PROCEED",
        "casual_comments_detected": False,
        "entities_to_create": {
            "contacts": [], "companies": [], "deals": [],
            "meetings": [],
            "notes": [
                {"note_content": "Client confirmed Q3 budget allocation for phase 1.",
                 "timestamp": "2026-02-11 09:00:00",
                 "note_type": "general",
                 "speaker_name": "Vivek Kumar",
                 "speaker_email": "vivek@acme.com"},
            ],
            "tasks": [
                {"task_details": "Follow up on contract signing",
                 "task_owner_name": "Kishore", "task_owner_id": "159242825",
                 "due_date": "2026-03-01", "priority": "HIGH", "task_index": 1},
            ],
        },
        "entities_to_update": {
            "contacts": [], "companies": [], "deals": [],
            "meetings": [], "notes": [], "tasks": [],
        },
        "selected_entities": {
            "contacts": [],
            "companies": [],
            "deals": [{
                "dealId": "701",
                "dealName": "Acme Corp-Cloud Migration Project",
                "dealLabelName": "Lead", "dealAmount": "50000",
                "closeDate": "2026-05-10", "dealOwnerName": "Kishore",
            }],
        },
        "reasoning": "Adding note and task to existing deal.",
        "tasks_to_execute": [
            "determine_owner", "check_task_threshold",
            "create_notes", "create_tasks",
            "create_associations", "compose_response_html",
            "collect_and_save_results", "send_final_email",
        ],
        "should_determine_owner": True,
        "should_check_task_threshold": True,
    }

    OWNER_INFO = {
        "contact_owner_id": "159242825",
        "contact_owner_name": "Kishore",
        "deal_owner_id": "159242825",
        "deal_owner_name": "Kishore",
        "task_owners": [
            {"task_index": 1, "task_owner_id": "159242825",
             "task_owner_name": "Kishore"},
        ],
        "all_owners_table": [
            {"id": "159242825", "name": "Kishore", "email": "kishore@test.com"},
        ],
    }

    def test_branch_only_notes_and_tasks(self):
        """branch returns only note/task creation, not contacts/companies/deals."""
        ti = MockXCom({"analysis_results": self.ANALYSIS_RESULT})
        ctx = make_context()
        ctx["ti"] = ti

        result = creator.branch_to_creation_tasks(ti, **ctx)

        assert "create_notes" in result
        assert "create_tasks" in result
        assert "determine_owner" in result
        assert "create_contacts" not in result
        assert "create_companies" not in result
        assert "create_deals" not in result
        assert "create_meetings" not in result

    @patch.object(creator, "get_ai_response")
    def test_create_note_for_existing_deal(self, mock_ai):
        """create_notes creates note to be associated with existing deal."""
        mock_ai.return_value = json.dumps({
            "status": "success",
            "created_notes": [
                {"id": "902", "details": {
                    "note_content": "Client confirmed Q3 budget allocation for phase 1.",
                    "timestamp": "2026-02-11"}},
            ],
            "errors": [], "reason": "",
        })
        ti = MockXCom({
            "analysis_results": self.ANALYSIS_RESULT,
            "chat_history": [],
        })
        ctx = make_context()
        ctx["ti"] = ti

        result = creator.create_notes(ti, **ctx)

        assert len(result) == 1
        assert "Q3 budget" in result[0]["details"]["note_content"]

    @patch.object(creator, "get_ai_response")
    def test_create_task_for_existing_deal(self, mock_ai):
        """create_tasks creates task with correct owner for existing deal."""
        mock_ai.return_value = json.dumps({
            "status": "success",
            "created_tasks": [
                {"id": "1003", "details": {
                    "task_details": "Follow up on contract signing",
                    "task_owner_name": "Kishore", "task_owner_id": "159242825",
                    "due_date": "2026-03-01", "priority": "HIGH", "task_index": 1}},
            ],
            "errors": [], "reason": "",
        })
        ti = MockXCom({
            "analysis_results": self.ANALYSIS_RESULT,
            "chat_history": [],
            "owner_info": self.OWNER_INFO,
        })
        ctx = make_context()
        ctx["ti"] = ti

        result = creator.create_tasks(ti, **ctx)

        assert len(result) == 1
        assert result[0]["details"]["task_owner_id"] == "159242825"
        assert result[0]["details"]["due_date"] == "2026-03-01"

    def test_collect_results_existing_deal_selected(self):
        """collect_and_save_results shows selected deal + new note/task."""
        ti = MockXCom({
            "created_contacts": [], "created_companies": [],
            "created_deals": [], "created_meetings": [],
            "created_notes": [{"id": "902"}],
            "created_tasks": [{"id": "1003"}],
            "updated_contacts": [], "updated_companies": [],
            "updated_deals": [], "updated_meetings": [],
            "updated_notes": [], "updated_tasks": [],
            "associations_created": [
                {"single": {"deal_id": "701", "note_id": "902", "task_id": "1003"}}
            ],
            "analysis_results": self.ANALYSIS_RESULT,
        })
        ctx = make_context()
        ctx["ti"] = ti

        result = creator.collect_and_save_results(ti, **ctx)

        assert result["created_contacts"]["total"] == 0
        assert result["created_companies"]["total"] == 0
        assert result["created_deals"]["total"] == 0
        assert result["created_notes"]["total"] == 1
        assert result["created_tasks"]["total"] == 1
        assert result["selected_deals"]["total"] == 1
        assert result["selected_deals"]["results"][0]["dealId"] == "701"

    @patch.object(creator, "get_ai_response")
    def test_analyze_identifies_note_task_intent(self, mock_ai):
        """analyze_user_response identifies note+task intent for existing deal."""
        mock_ai.return_value = json.dumps({
            "casual_comments_detected": False,
            "user_intent": "PROCEED",
            "selected_entities": self.ANALYSIS_RESULT["selected_entities"],
            "entities_to_create": self.ANALYSIS_RESULT["entities_to_create"],
            "entities_to_update": self.ANALYSIS_RESULT["entities_to_update"],
            "reasoning": "Adding note and task to existing deal.",
        })
        email_data = _base_email_data(
            subject="Re: Acme Corp Deal", body=self.EMAIL_BODY)
        ti = MockXCom({
            "chat_history": [{"role": "user", "content": self.EMAIL_BODY}],
            "thread_history": [
                {"content": self.EMAIL_BODY,
                 "headers": email_data["headers"], "from_bot": False}
            ],
            "thread_id": "thread_002",
            "latest_message": self.EMAIL_BODY,
            "email_data": email_data,
        })
        ctx = make_context()
        ctx["ti"] = ti

        result = creator.analyze_user_response(ti, **ctx)

        assert result["status"] == "success"
        assert len(result["entities_to_create"]["contacts"]) == 0
        assert len(result["entities_to_create"]["deals"]) == 0
        assert len(result["entities_to_create"]["notes"]) == 1
        assert len(result["entities_to_create"]["tasks"]) == 1
        assert len(result["selected_entities"]["deals"]) == 1


# ============================================================================
# SCENARIO 3: Existing Contacts + New Company
# ============================================================================

class TestExistingContactsNewCompany:
    """
    Contacts exist in HubSpot (found by search). Company is new.
    Tests both search DAG validation and object_creator pipeline.
    """

    EMAIL_BODY = (
        "Hi, I met with John Smith from TechStart Inc.\n"
        "Please create the company TechStart Inc and link it to John.\n"
        "Also create a deal: TechStart-Consulting, amount 25000, "
        "stage Qualified Lead."
    )

    # --- Search DAG data ---

    CONTACT_INFO = {
        "contact_results": {
            "total": 1,
            "results": [
                {"contactId": "501", "firstname": "John", "lastname": "Smith",
                 "email": "john@acme.com", "phone": "555-0100",
                 "address": "", "jobtitle": "CTO",
                 "contactOwnerId": "159242825",
                 "contactOwnerName": "Kishore"},
            ],
        },
        "new_contacts": [],
        "associated_companies": [],
        "associated_deals": [],
    }

    COMPANY_INFO = {
        "company_results": {"total": 0, "results": []},
        "new_companies": [
            {"name": "TechStart Inc", "domain": "techstart.com",
             "address": "", "city": "", "state": "", "zip": "",
             "country": "", "phone": "", "description": "",
             "type": "PROSPECT"},
        ],
        "partner_status": None,
    }

    DEAL_INFO = {
        "deal_results": {"total": 0, "results": []},
        "new_deals": [
            {"dealName": "TechStart-Consulting Engagement",
             "dealLabelName": "Qualified Lead", "dealAmount": "25000",
             "closeDate": "2026-05-11", "dealOwnerName": "Kishore"},
        ],
    }

    OWNER_INFO = {
        "contact_owner_id": "159242825",
        "contact_owner_name": "Kishore",
        "deal_owner_id": "159242825",
        "deal_owner_name": "Kishore",
        "task_owners": [],
        "all_owners_table": [
            {"id": "159242825", "name": "Kishore", "email": "kishore@test.com"},
        ],
    }

    NOTES_TASKS_MEETING = {"notes": [], "tasks": [], "meeting_details": []}

    # --- Search DAG tests ---

    def test_validate_entity_creation_rules_passes(self):
        """Validation passes: existing contact + new company + new deal."""
        email_data = _base_email_data(body=self.EMAIL_BODY)
        ti = MockXCom({
            "entity_search_flags": {},
            "contact_info_with_associations": self.CONTACT_INFO,
            "company_info": self.COMPANY_INFO,
            "deal_info": self.DEAL_INFO,
            "notes_tasks_meeting": self.NOTES_TASKS_MEETING,
            "email_data": email_data,
        })
        ctx = make_context()
        ctx["ti"] = ti

        result = search.validate_entity_creation_rules(ti, **ctx)

        assert result["is_valid"] is True
        assert len(result["errors"]) == 0
        assert result["entity_summary"]["contacts"]["existing"] == 1
        assert result["entity_summary"]["contacts"]["new"] == 0
        assert result["entity_summary"]["companies"]["new"] == 1
        assert result["entity_summary"]["deals"]["new"] == 1

    def test_validate_fails_no_contacts_for_new_company(self):
        """Validation fails: new company with zero contacts."""
        email_data = _base_email_data(body=self.EMAIL_BODY)
        empty_contacts = {
            "contact_results": {"total": 0, "results": []},
            "new_contacts": [],
        }
        ti = MockXCom({
            "entity_search_flags": {},
            "contact_info_with_associations": empty_contacts,
            "company_info": self.COMPANY_INFO,
            "deal_info": self.DEAL_INFO,
            "notes_tasks_meeting": self.NOTES_TASKS_MEETING,
            "email_data": email_data,
        })
        ctx = make_context()
        ctx["ti"] = ti

        result = search.validate_entity_creation_rules(ti, **ctx)

        assert result["is_valid"] is False
        error_types = [e["entity_type"] for e in result["errors"]]
        assert "Companies" in error_types
        assert "Deals" in error_types

    def test_compile_search_results_existing_contact_new_company(self):
        """compile_search_results merges existing contact with new company+deal."""
        deal_stage_info = {
            "deal_stages": [
                {"deal_index": 1,
                 "deal_name": "TechStart-Consulting Engagement",
                 "original_stage": "Qualified Lead",
                 "validated_stage": "Qualified Lead",
                 "stage_message": "Valid stage."},
            ],
        }
        ti = MockXCom({
            "owner_info": self.OWNER_INFO,
            "merged_deal_info": None,
            "deal_info": self.DEAL_INFO,
            "contact_info_with_associations": self.CONTACT_INFO,
            "merged_company_info": None,
            "company_info": self.COMPANY_INFO,
            "notes_tasks_meeting": self.NOTES_TASKS_MEETING,
            "task_threshold_info": {},
            "deal_stage_info": deal_stage_info,
            "thread_id": "thread_003",
            "email_data": _base_email_data(body=self.EMAIL_BODY),
        })
        ctx = make_context()
        ctx["ti"] = ti

        result = search.compile_search_results(ti, **ctx)

        assert result["contact_results"]["total"] == 1
        assert result["contact_results"]["results"][0]["contactId"] == "501"
        assert result["company_results"]["total"] == 0
        assert len(result["new_entity_details"]["companies"]) == 1
        assert result["new_entity_details"]["companies"][0]["name"] == "TechStart Inc"
        assert len(result["new_entity_details"]["deals"]) == 1
        assert result["new_entity_details"]["deals"][0]["dealLabelName"] == "Qualified Lead"
        assert result["confirmation_needed"] is True

    # --- Object Creator tests ---

    CREATOR_ANALYSIS = {
        "status": "success",
        "user_intent": "PROCEED",
        "casual_comments_detected": False,
        "entities_to_create": {
            "contacts": [],
            "companies": [
                {"name": "TechStart Inc", "domain": "techstart.com",
                 "address": "", "city": "", "state": "", "zip": "",
                 "country": "", "phone": "", "description": "",
                 "type": "PROSPECT"},
            ],
            "deals": [
                {"dealName": "TechStart-Consulting Engagement",
                 "dealLabelName": "Qualified Lead", "dealAmount": "25000",
                 "closeDate": "2026-05-11", "dealOwnerName": "Kishore"},
            ],
            "meetings": [], "notes": [], "tasks": [],
        },
        "entities_to_update": {
            "contacts": [], "companies": [], "deals": [],
            "meetings": [], "notes": [], "tasks": [],
        },
        "selected_entities": {
            "contacts": [
                {"contactId": "501", "firstname": "John", "lastname": "Smith",
                 "email": "john@acme.com", "phone": "555-0100",
                 "address": "", "jobtitle": "CTO",
                 "contactOwnerName": "Kishore"},
            ],
            "companies": [], "deals": [],
        },
        "reasoning": "Existing contact John found. Creating new company and deal.",
        "tasks_to_execute": [
            "determine_owner", "create_companies", "create_deals",
            "create_associations", "compose_response_html",
            "collect_and_save_results", "send_final_email",
        ],
        "should_determine_owner": True,
        "should_check_task_threshold": False,
    }

    def test_branch_company_deal_only(self):
        """branch returns company+deal creation only (no contacts)."""
        ti = MockXCom({"analysis_results": self.CREATOR_ANALYSIS})
        ctx = make_context()
        ctx["ti"] = ti

        result = creator.branch_to_creation_tasks(ti, **ctx)

        assert "create_companies" in result
        assert "create_deals" in result
        assert "determine_owner" in result
        assert "create_contacts" not in result
        assert "create_notes" not in result
        assert "create_tasks" not in result

    @patch.object(creator, "get_ai_response")
    def test_create_company_techstart(self, mock_ai):
        """create_companies creates TechStart Inc."""
        mock_ai.return_value = json.dumps({
            "status": "success",
            "created_companies": [
                {"id": "602", "details": {"name": "TechStart Inc",
                                          "domain": "techstart.com",
                                          "type": "PROSPECT"}},
            ],
            "errors": [], "reason": "",
        })
        ti = MockXCom({
            "analysis_results": self.CREATOR_ANALYSIS,
            "chat_history": [],
        })
        ctx = make_context()
        ctx["ti"] = ti

        result = creator.create_companies(ti, **ctx)

        assert len(result) == 1
        assert result[0]["details"]["name"] == "TechStart Inc"

    @patch.object(creator, "get_ai_response")
    def test_create_deal_for_new_company(self, mock_ai):
        """create_deals creates deal with correct stage."""
        mock_ai.return_value = json.dumps({
            "status": "success",
            "created_deals": [
                {"id": "702", "details": {
                    "dealName": "TechStart-Consulting Engagement",
                    "dealLabelName": "Qualified Lead",
                    "dealAmount": "25000",
                    "closeDate": "2026-05-11",
                    "dealOwnerName": "Kishore"}},
            ],
            "errors": [], "reason": "",
        })
        ti = MockXCom({
            "analysis_results": self.CREATOR_ANALYSIS,
            "chat_history": [],
            "owner_info": self.OWNER_INFO,
        })
        ctx = make_context()
        ctx["ti"] = ti

        result = creator.create_deals(ti, **ctx)

        assert len(result) == 1
        assert result[0]["details"]["dealLabelName"] == "Qualified Lead"
        assert result[0]["details"]["dealAmount"] == "25000"

    def test_collect_results_selected_contact_new_company_deal(self):
        """collect shows selected contact + created company and deal."""
        ti = MockXCom({
            "created_contacts": [],
            "created_companies": [{"id": "602"}],
            "created_deals": [{"id": "702"}],
            "created_meetings": [], "created_notes": [], "created_tasks": [],
            "updated_contacts": [], "updated_companies": [],
            "updated_deals": [], "updated_meetings": [],
            "updated_notes": [], "updated_tasks": [],
            "associations_created": [
                {"single": {"contact_id": "501", "company_id": "602",
                            "deal_id": "702"}},
            ],
            "analysis_results": self.CREATOR_ANALYSIS,
        })
        ctx = make_context()
        ctx["ti"] = ti

        result = creator.collect_and_save_results(ti, **ctx)

        assert result["created_contacts"]["total"] == 0
        assert result["created_companies"]["total"] == 1
        assert result["created_deals"]["total"] == 1
        assert result["selected_contacts"]["total"] == 1
        assert result["selected_contacts"]["results"][0]["contactId"] == "501"


# ============================================================================
# Deal Stage Validation (Search DAG)
# ============================================================================

class TestDealStageValidation:
    """Tests validate_deal_stage and compile_search_results stage matching."""

    @patch.object(search, "get_ai_response")
    def test_valid_stage_accepted(self, mock_ai):
        """validate_deal_stage accepts a valid stage."""
        mock_ai.return_value = json.dumps({
            "deal_stages": [
                {"deal_index": 1, "deal_name": "Test Deal",
                 "original_stage": "Qualified Lead",
                 "validated_stage": "Qualified Lead",
                 "stage_message": "Valid stage."},
            ],
        })
        ti = MockXCom({
            "chat_history": [],
            "latest_message": "Create deal Test Deal at Qualified Lead",
        })
        ctx = make_context()
        ctx["ti"] = ti

        result = search.validate_deal_stage(ti, **ctx)

        assert result["deal_stages"][0]["validated_stage"] == "Qualified Lead"

    @patch.object(search, "get_ai_response")
    def test_invalid_stage_defaults_to_lead(self, mock_ai):
        """validate_deal_stage defaults invalid stage to 'Lead'."""
        mock_ai.return_value = json.dumps({
            "deal_stages": [
                {"deal_index": 1, "deal_name": "Test Deal",
                 "original_stage": "Discovery",
                 "validated_stage": "Lead",
                 "stage_message": "Invalid, defaulting to Lead."},
            ],
        })
        ti = MockXCom({
            "chat_history": [],
            "latest_message": "Create deal at stage Discovery",
        })
        ctx = make_context()
        ctx["ti"] = ti

        result = search.validate_deal_stage(ti, **ctx)

        assert result["deal_stages"][0]["validated_stage"] == "Lead"

    def test_index_based_stage_fallback(self):
        """compile_search_results uses index fallback when AI deal names differ."""
        deal_info = {
            "deal_results": {"total": 0, "results": []},
            "new_deals": [
                {"dealName": "PolicyStream Insurance-Policy Automation",
                 "dealLabelName": "Discovery",
                 "dealAmount": "10000", "closeDate": "2026-06-01",
                 "dealOwnerName": "Kishore"},
            ],
        }
        deal_stage_info = {
            "deal_stages": [
                {"deal_index": 1,
                 "deal_name": "PolicyStream-Policy Automation Program",
                 "validated_stage": "Lead"},
            ],
        }
        ti = MockXCom({
            "owner_info": {"contact_owner_id": "159242825",
                           "contact_owner_name": "Kishore",
                           "deal_owner_id": "159242825",
                           "deal_owner_name": "Kishore",
                           "task_owners": [], "all_owners_table": []},
            "merged_deal_info": None,
            "deal_info": deal_info,
            "contact_info_with_associations": {
                "contact_results": {"total": 0, "results": []},
                "new_contacts": []},
            "merged_company_info": None,
            "company_info": {"company_results": {"total": 0, "results": []},
                             "new_companies": [], "partner_status": None},
            "notes_tasks_meeting": {"notes": [], "tasks": [],
                                     "meeting_details": []},
            "task_threshold_info": {},
            "deal_stage_info": deal_stage_info,
            "thread_id": "thread_004",
            "email_data": _base_email_data(),
        })
        ctx = make_context()
        ctx["ti"] = ti

        result = search.compile_search_results(ti, **ctx)

        # Index fallback corrects "Discovery" → "Lead"
        assert result["new_entity_details"]["deals"][0]["dealLabelName"] == "Lead"


# ============================================================================
# Error Handling & Edge Cases
# ============================================================================

class TestErrorHandling:

    def test_analyze_no_thread_id_raises(self):
        """analyze_user_response raises ValueError when no thread_id."""
        email_data = _base_email_data(body="test")
        ti = MockXCom({
            "chat_history": [], "thread_history": [],
            "thread_id": None,
            "latest_message": "test",
            "email_data": email_data,
        })
        ctx = make_context()
        ctx["ti"] = ti

        with pytest.raises(ValueError, match="No thread_id provided"):
            creator.analyze_user_response(ti, **ctx)

    @patch.object(creator, "get_ai_response")
    def test_create_contacts_invalid_json_raises(self, mock_ai):
        """create_contacts raises on invalid JSON from AI."""
        mock_ai.return_value = "this is not json at all"
        analysis = {
            "entities_to_create": {
                "contacts": [{"firstname": "Test", "lastname": "User",
                              "email": "test@test.com"}],
            },
        }
        ti = MockXCom({
            "analysis_results": analysis,
            "chat_history": [], "owner_info": {},
        })
        ctx = make_context(try_number=1, max_tries=1)
        ctx["ti"] = ti

        with pytest.raises(Exception, match="Invalid JSON"):
            creator.create_contacts(ti, **ctx)

    def test_branch_join_when_nothing_to_create(self):
        """branch returns join_creations when no creation tasks."""
        analysis = {
            "tasks_to_execute": [
                "create_associations", "compose_response_html",
                "collect_and_save_results", "send_final_email"],
        }
        ti = MockXCom({"analysis_results": analysis})
        ctx = make_context()
        ctx["ti"] = ti

        assert creator.branch_to_creation_tasks(ti, **ctx) == ["join_creations"]

    def test_branch_end_workflow_when_fallback_sent(self):
        """branch returns end_workflow when fallback email was already sent."""
        analysis = {
            "fallback_email_sent": True,
            "tasks_to_execute": ["create_contacts"],
        }
        ti = MockXCom({"analysis_results": analysis})
        ctx = make_context()
        ctx["ti"] = ti

        assert creator.branch_to_creation_tasks(ti, **ctx) == ["end_workflow"]

    def test_validate_notes_without_base_entity_fails(self):
        """Validation fails when notes exist but no base entities."""
        email_data = _base_email_data(body="Add a note")
        ti = MockXCom({
            "entity_search_flags": {},
            "contact_info_with_associations": {
                "contact_results": {"total": 0, "results": []},
                "new_contacts": []},
            "company_info": {
                "company_results": {"total": 0, "results": []},
                "new_companies": []},
            "deal_info": {
                "deal_results": {"total": 0, "results": []},
                "new_deals": []},
            "notes_tasks_meeting": {
                "notes": [{"note_content": "Standalone note"}],
                "tasks": [], "meeting_details": []},
            "email_data": email_data,
        })
        ctx = make_context()
        ctx["ti"] = ti

        result = search.validate_entity_creation_rules(ti, **ctx)

        assert result["is_valid"] is False
        assert any(e["entity_type"] == "Notes" for e in result["errors"])

    def test_validate_tasks_without_base_entity_fails(self):
        """Validation fails when tasks exist but no base entities."""
        email_data = _base_email_data(body="Add a task")
        ti = MockXCom({
            "entity_search_flags": {},
            "contact_info_with_associations": {
                "contact_results": {"total": 0, "results": []},
                "new_contacts": []},
            "company_info": {
                "company_results": {"total": 0, "results": []},
                "new_companies": []},
            "deal_info": {
                "deal_results": {"total": 0, "results": []},
                "new_deals": []},
            "notes_tasks_meeting": {
                "notes": [],
                "tasks": [{"task_details": "Something", "due_date": "2026-03-01"}],
                "meeting_details": []},
            "email_data": email_data,
        })
        ctx = make_context()
        ctx["ti"] = ti

        result = search.validate_entity_creation_rules(ti, **ctx)

        assert result["is_valid"] is False
        assert any(e["entity_type"] == "Tasks" for e in result["errors"])

    @patch.object(creator, "get_ai_response")
    def test_create_deals_empty(self, mock_ai):
        """create_deals handles empty list without calling AI."""
        ti = MockXCom({
            "analysis_results": {"entities_to_create": {"deals": []}},
            "chat_history": [], "owner_info": {},
        })
        ctx = make_context()
        ctx["ti"] = ti

        assert creator.create_deals(ti, **ctx) == []
        assert ti.xcom_pull(key="deal_creation_final_status") == "success"
        mock_ai.assert_not_called()

    @patch.object(creator, "get_ai_response")
    def test_create_meetings_empty(self, mock_ai):
        """create_meetings handles empty list without calling AI."""
        ti = MockXCom({
            "analysis_results": {"entities_to_create": {"meetings": []}},
            "chat_history": [],
        })
        ctx = make_context()
        ctx["ti"] = ti

        assert creator.create_meetings(ti, **ctx) == []
        assert ti.xcom_pull(key="meeting_creation_final_status") == "success"
        mock_ai.assert_not_called()


# ============================================================================
# Task Owner Assignment
# ============================================================================

class TestTaskOwnerAssignment:

    @patch.object(creator, "get_ai_response")
    def test_owner_from_owner_info_overrides_default(self, mock_ai):
        """Task owners from owner_info override default."""
        mock_ai.return_value = json.dumps({
            "status": "success",
            "created_tasks": [
                {"id": "2001", "details": {
                    "task_details": "Task for Vivek",
                    "task_owner_name": "Vivek", "task_owner_id": "200000001",
                    "due_date": "2026-03-15", "priority": "MEDIUM",
                    "task_index": 1}},
            ],
            "errors": [], "reason": "",
        })
        analysis = {
            "entities_to_create": {
                "tasks": [
                    {"task_details": "Task for Vivek",
                     "task_owner_name": "", "task_owner_id": "",
                     "due_date": "2026-03-15", "priority": "MEDIUM",
                     "task_index": 1},
                ],
            },
        }
        owner_info = {
            "task_owners": [
                {"task_index": 1, "task_owner_id": "200000001",
                 "task_owner_name": "Vivek"},
            ],
        }
        ti = MockXCom({
            "analysis_results": analysis,
            "chat_history": [],
            "owner_info": owner_info,
        })
        ctx = make_context()
        ctx["ti"] = ti

        result = creator.create_tasks(ti, **ctx)

        assert result[0]["details"]["task_owner_id"] == "200000001"

    @patch.object(creator, "get_ai_response")
    def test_default_owner_when_no_match(self, mock_ai):
        """Default owner assigned when owner_info has no matching task_index."""
        mock_ai.return_value = json.dumps({
            "status": "success",
            "created_tasks": [
                {"id": "2002", "details": {
                    "task_details": "Unmatched task",
                    "task_owner_name": "Kishore", "task_owner_id": "159242825",
                    "due_date": "2026-03-20", "priority": "LOW",
                    "task_index": 1}},
            ],
            "errors": [], "reason": "",
        })
        analysis = {
            "entities_to_create": {
                "tasks": [
                    {"task_details": "Unmatched task",
                     "due_date": "2026-03-20", "priority": "LOW",
                     "task_index": 1},
                ],
            },
        }
        owner_info = {
            "task_owners": [
                {"task_index": 99, "task_owner_id": "200000001",
                 "task_owner_name": "Vivek"},
            ],
        }
        ti = MockXCom({
            "analysis_results": analysis,
            "chat_history": [],
            "owner_info": owner_info,
        })
        ctx = make_context()
        ctx["ti"] = ti

        creator.create_tasks(ti, **ctx)

        # setdefault applies: task gets default owner
        task_data = analysis["entities_to_create"]["tasks"][0]
        assert task_data["task_owner_id"] == "159242825"
        assert task_data["task_owner_name"] == "Kishore"


# ============================================================================
# Main
# ============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
