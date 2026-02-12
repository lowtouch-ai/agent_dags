"""
Shared fixtures for tracker_weekly_report.py test suite.

The DAG has 13 Variable.get() calls at module scope (lines 26-44) that execute
at import time. We patch airflow.models.Variable.get BEFORE importing the module
via importlib.import_module().
"""
import copy
import json
import importlib
import sys
from unittest.mock import MagicMock, patch, PropertyMock
import pytest


# ---------------------------------------------------------------------------
# Variable.get mock values
# ---------------------------------------------------------------------------
VARIABLE_STORE = {
    "ltai.v1.tracker.SMTP_USER": "test@smtp.example.com",
    "ltai.v1.tracker.SMTP_PASSWORD": "smtp-pass-123",
    "ltai.v1.tracker.SMTP_HOST": "mail.authsmtp.com",
    "ltai.v1.tracker.SMTP_PORT": "2525",
    "ltai.v1.tracker.SMTP_FROM_SUFFIX": "via lowtouch.ai <webmaster@ecloudcontrol.com>",
    "ltai.v1.tracker.FROM_ADDRESS": "test@smtp.example.com",
    "ltai.v1.tracker.OLLAMA_HOST": "http://agentomatic:8000/",
    "ltai.v1.tracker.MANTIS_API_BASE": "https://mantis.example.com",
    "ltai.v1.tracker.MANTIS_API_KEY": "mantis-api-key-123",
    "ltai.v1.tracker.MANTIS_TIMESHEET_API_URL": "https://mantis-ts.example.com",
    "ltai.v1.tracker.whitelisted_users": json.dumps([
        {
            "user_id": "U001",
            "mantis_email": "alice@mantis.example.com",
            "name": "Alice",
            "email": "alice@example.com",
            "hr_name": "HR_Manager",
            "hr_email": "hr@example.com",
            "manager_name": "Bob",
            "manager_email": "bob@example.com",
            "admin_name": "Charlie",
            "admin_email": "charlie@example.com",
            "timezone": "Asia/Kolkata",
        },
        {
            "user_id": "U002",
            "mantis_email": "dave@mantis.example.com",
            "name": "Dave",
            "email": "dave@example.com",
            "hr_name": "HR_Manager",
            "hr_email": "hr@example.com",
            "manager_name": None,
            "manager_email": None,
            "admin_name": None,
            "admin_email": None,
            "timezone": "America/New_York",
        },
    ]),
    "ltai.v1.tracker.whitelisted.hr_email": "hr-global@example.com",
    "ltai.v1.tracker.whitelisted.manager_email": "mgr-global@example.com",
    "ltai.v1.tracker.whitelisted.admin_email": "admin-global@example.com",
}


def _mock_variable_get(key, *args, **kwargs):
    """Handle all 4 Variable.get() calling conventions used in the DAG."""
    deserialize_json = kwargs.get("deserialize_json", False)

    # Determine default value
    if "default_var" in kwargs:
        default = kwargs["default_var"]
    elif "default" in kwargs:
        default = kwargs["default"]
    elif args:
        default = args[0]
    else:
        default = _SENTINEL

    if key in VARIABLE_STORE:
        value = VARIABLE_STORE[key]
        if deserialize_json and isinstance(value, str):
            return json.loads(value)
        return value

    if default is not _SENTINEL:
        return default

    raise KeyError(f"Variable {key} does not exist")


_SENTINEL = object()


# ---------------------------------------------------------------------------
# Import the DAG module with Variable.get patched
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def tracker_module():
    """
    Patch Variable.get, then import tracker_weekly_report via importlib.
    Returns the module object so tests can access its functions and DAG.
    """
    # Create the mock Variable class
    mock_variable_cls = MagicMock()
    mock_variable_cls.get = MagicMock(side_effect=_mock_variable_get)

    # Build a fake airflow package tree so the import doesn't need a real DB
    airflow_mod = MagicMock()
    airflow_mod.DAG = _make_dag_class()
    airflow_mod.models = MagicMock()
    airflow_mod.models.Variable = mock_variable_cls

    operators_mod = MagicMock()
    python_mod = MagicMock()
    python_mod.PythonOperator = _make_python_operator_class()
    operators_mod.python = python_mod

    # Register fake airflow modules
    sys.modules.setdefault("airflow", airflow_mod)
    sys.modules.setdefault("airflow.models", airflow_mod.models)
    sys.modules["airflow.models"].Variable = mock_variable_cls
    sys.modules.setdefault("airflow.operators", operators_mod)
    sys.modules.setdefault("airflow.operators.python", python_mod)

    # Stub google and ollama (not needed for unit tests)
    for mod_name in [
        "google", "google.oauth2", "google.oauth2.credentials",
        "googleapiclient", "googleapiclient.discovery",
    ]:
        sys.modules.setdefault(mod_name, MagicMock())

    # Provide a real-ish ollama.Client mock
    ollama_mock = MagicMock()
    sys.modules.setdefault("ollama", ollama_mock)

    # Now import the actual DAG file
    mod = importlib.import_module("tracker_weekly_report")
    return mod


def _make_dag_class():
    """Return a lightweight DAG stand-in that records tasks."""

    class FakeDAG:
        def __init__(self, dag_id, **kwargs):
            self.dag_id = dag_id
            self.default_args = kwargs.get("default_args", {})
            self.description = kwargs.get("description", "")
            self.schedule_interval = kwargs.get("schedule_interval")
            self.catchup = kwargs.get("catchup", True)
            self.tags = kwargs.get("tags", [])
            self._tasks = {}

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def _register_task(self, task):
            self._tasks[task.task_id] = task

    return FakeDAG


def _make_python_operator_class():
    """Return a PythonOperator stand-in that tracks dependencies."""

    class FakePythonOperator:
        def __init__(self, task_id, python_callable, dag=None, **kwargs):
            self.task_id = task_id
            self.python_callable = python_callable
            self.upstream_task_ids = set()
            self.downstream_task_ids = set()
            self._dag = dag

        def __rshift__(self, other):
            self.downstream_task_ids.add(other.task_id)
            other.upstream_task_ids.add(self.task_id)
            return other

        def __lshift__(self, other):
            self.upstream_task_ids.add(other.task_id)
            other.downstream_task_ids.add(self.task_id)
            return other

    return FakePythonOperator


# ---------------------------------------------------------------------------
# MockTaskInstance  (simulates XCom push/pull)
# ---------------------------------------------------------------------------
class MockTaskInstance:
    """In-memory XCom store."""

    def __init__(self, dag_id="weekly_timesheet_review"):
        self.dag_id = dag_id
        self._xcom = {}

    def xcom_push(self, key, value):
        self._xcom[key] = value

    def xcom_pull(self, key=None, task_ids=None, include_prior_dates=False, dag_id=None):
        if key is None:
            return None
        return self._xcom.get(key)

    def get(self, key, default=None):
        return self._xcom.get(key, default)


@pytest.fixture
def mock_ti():
    """Fresh TaskInstance per test."""
    return MockTaskInstance()


# ---------------------------------------------------------------------------
# Sample data fixtures
# ---------------------------------------------------------------------------
SAMPLE_USERS = [
    {
        "user_id": "U001",
        "mantis_email": "alice@mantis.example.com",
        "name": "Alice",
        "email": "alice@example.com",
        "hr_name": "HR_Manager",
        "hr_email": "hr@example.com",
        "manager_name": "Bob",
        "manager_email": "bob@example.com",
        "admin_name": "Charlie",
        "admin_email": "charlie@example.com",
        "timezone": "Asia/Kolkata",
    },
    {
        "user_id": "U002",
        "mantis_email": "dave@mantis.example.com",
        "name": "Dave",
        "email": "dave@example.com",
        "hr_name": "HR_Manager",
        "hr_email": "hr@example.com",
        "manager_name": None,
        "manager_email": None,
        "admin_name": None,
        "admin_email": None,
        "timezone": "America/New_York",
    },
]

SAMPLE_WEEK_RANGE = {
    "start": "2025-01-20",
    "end": "2025-01-26",
    "week_number": 3,
    "month": "January",
    "year": 2025,
}

SAMPLE_API_RESPONSE = {
    "total_time": "42:30",
    "project": ["ProjectA"],
    "tickets_worked": [
        {"issue_id": "1001", "issue_time": "10:00"},
        {"issue_id": "1002", "issue_time": "8:30"},
        {"issue_id": "1003", "issue_time": "12:00"},
        {"issue_id": "1004", "issue_time": "12:00"},
    ],
}

SAMPLE_TIMESHEET_DATA = {
    "user_id": "U001",
    "username": "Alice",
    "user_email_address": "alice@example.com",
    "user_timezone": "Asia/Kolkata",
    "hr_name": "HR_Manager",
    "hr_email": "hr@example.com",
    "manager_name": "Bob",
    "manager_email": "bob@example.com",
    "admin_name": "Charlie",
    "admin_email": "charlie@example.com",
    "week_start": "2025-01-20",
    "week_end": "2025-01-26",
    "week_info": "Week 3 of January",
    "total_hours": 42.5,
    "total_time_str": "42:30",
    "projects": ["ProjectA"],
    "entries": [
        {"issue_id": "1001", "task_id": "1001", "hours": 10.0, "time_str": "10:00"},
        {"issue_id": "1002", "task_id": "1002", "hours": 8.5, "time_str": "8:30"},
        {"issue_id": "1003", "task_id": "1003", "hours": 12.0, "time_str": "12:00"},
        {"issue_id": "1004", "task_id": "1004", "hours": 12.0, "time_str": "12:00"},
    ],
    "unique_dates": 0,
    "tasks_over_4_hours": 4,
    "entry_count": 4,
}

SAMPLE_AI_ANALYSIS = {
    "week_info": "Week 3 of January",
    "ratings": {
        "total_hours": 4,
        "task_description": 3,
        "daily_updates": 3,
        "task_granularity": 2,
        "time_accuracy": 4,
    },
    "findings": {
        "total_hours": "Total logged time is 42.5 hours.",
        "task_description": "Descriptions are adequate.",
        "daily_updates": "Entries made across 4 days.",
        "task_granularity": "Multiple tasks exceed 4 hours.",
        "time_accuracy": "Time accuracy is good.",
    },
    "recommendations": {
        "total_hours": "Keep at 42-45 hours.",
        "task_description": "Add objective and result.",
        "daily_updates": "Update timesheet daily.",
        "task_granularity": "Split tasks under 4 hours.",
        "time_accuracy": "Maintain accuracy.",
    },
    "final_score": 3.5,
}

SAMPLE_ISSUE_RESPONSE = {
    "issues": [
        {
            "id": 1001,
            "summary": "Fix login bug",
            "description": "Users cannot login",
            "status": {"name": "resolved"},
            "priority": {"name": "high"},
            "category": {"name": "Bug"},
            "notes": [
                {
                    "reporter": {"name": "Alice"},
                    "created_at": "2025-01-22T10:30:00Z",
                    "updated_at": "2025-01-22T14:00:00Z",
                    "text": "Fixed authentication flow",
                },
                {
                    "reporter": {"name": "Alice"},
                    "created_at": "2025-01-23T09:00:00Z",
                    "updated_at": None,
                    "text": "Verified in staging",
                },
                {
                    "reporter": {"name": "OtherUser"},
                    "created_at": "2025-01-24T11:00:00Z",
                    "updated_at": None,
                    "text": "Confirmed fix works",
                },
            ],
        }
    ]
}


@pytest.fixture
def sample_users():
    return copy.deepcopy(SAMPLE_USERS)


@pytest.fixture
def sample_week_range():
    return copy.deepcopy(SAMPLE_WEEK_RANGE)


@pytest.fixture
def sample_api_response():
    return copy.deepcopy(SAMPLE_API_RESPONSE)


@pytest.fixture
def sample_timesheet_data():
    return copy.deepcopy(SAMPLE_TIMESHEET_DATA)


@pytest.fixture
def sample_ai_analysis():
    return copy.deepcopy(SAMPLE_AI_ANALYSIS)


@pytest.fixture
def sample_issue_response():
    return copy.deepcopy(SAMPLE_ISSUE_RESPONSE)


@pytest.fixture
def mock_ti_with_data(mock_ti, sample_users, sample_week_range):
    """Pre-populated TaskInstance with Task 1 & 2 outputs."""
    import pendulum

    # Simulate load_whitelisted_users output
    for user in sample_users:
        if "timezone" in user:
            try:
                user["timezone_obj"] = pendulum.timezone(user["timezone"])
            except Exception:
                user["timezone_obj"] = pendulum.timezone("Asia/Kolkata")
    mock_ti.xcom_push(key="whitelisted_users", value=sample_users)
    mock_ti.xcom_push(key="total_users", value=len(sample_users))

    # Simulate calculate_week_range output
    mock_ti.xcom_push(key="week_range", value=sample_week_range)

    return mock_ti


@pytest.fixture
def airflow_context():
    """Factory that returns a mock Airflow context dict with pendulum execution_date."""
    import pendulum

    def _make_context(dt_str="2025-01-27T08:30:00", tz="UTC"):
        dt = pendulum.parse(dt_str, tz=tz)
        return {
            "execution_date": dt,
            "logical_date": dt,
            "ds": dt.format("YYYY-MM-DD"),
            "ts": dt.isoformat(),
        }

    return _make_context
