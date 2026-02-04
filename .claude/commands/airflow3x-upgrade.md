# Airflow 3.1.3 DAG Upgrade

Upgrade the specified DAG file(s) from Airflow 2.x to Airflow 3.1.3. Apply ONLY the changes that are relevant to the file — do not modify code that is already compatible.

**Target file(s):** $ARGUMENTS

## Instructions

1. Read the target file(s)
2. Walk through every rule in the checklist below
3. Apply only the rules that match code found in the file
4. Commit the changes to branch `10419/airflow-upgrade-3.0`
5. If already on that branch, commit directly; otherwise cherry-pick onto it

---

## Migration Checklist

### 1. DAG import

| Before | After |
|---|---|
| `from airflow import DAG` | `from airflow.sdk import DAG` |
| `from airflow.models import DAG` | `from airflow.sdk import DAG` |
| `from airflow.models.dag import DAG` | `from airflow.sdk import DAG` |

### 2. Operator imports — moved to `apache-airflow-providers-standard`

| Operator | Before | After |
|---|---|---|
| BashOperator | `from airflow.operators.bash import BashOperator` | `from airflow.providers.standard.operators.bash import BashOperator` |
| PythonOperator | `from airflow.operators.python import PythonOperator` | `from airflow.providers.standard.operators.python import PythonOperator` |
| EmptyOperator | `from airflow.operators.empty import EmptyOperator` | `from airflow.providers.standard.operators.empty import EmptyOperator` |
| BranchPythonOperator | `from airflow.operators.python import BranchPythonOperator` | `from airflow.providers.standard.operators.python import BranchPythonOperator` |
| ShortCircuitOperator | `from airflow.operators.python import ShortCircuitOperator` | `from airflow.providers.standard.operators.python import ShortCircuitOperator` |
| EmailOperator | `from airflow.operators.email import EmailOperator` | `from airflow.providers.smtp.operators.smtp import EmailOperator` |
| EmailOperator (old path) | `from airflow.operators.email_operator import EmailOperator` | `from airflow.providers.smtp.operators.smtp import EmailOperator` |
| DummyOperator | `from airflow.operators.dummy import DummyOperator` | Replace with `from airflow.providers.standard.operators.empty import EmptyOperator` and rename usage |
| TriggerDagRunOperator | `from airflow.operators.trigger_dagrun import TriggerDagRunOperator` | `from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator` |

### 3. Sensor imports — moved to `apache-airflow-providers-standard`

| Sensor | Before | After |
|---|---|---|
| BaseSensorOperator | `from airflow.sensors.base import BaseSensorOperator` | `from airflow.sdk import BaseSensorOperator` |
| ExternalTaskSensor | `from airflow.sensors.external_task import ExternalTaskSensor` | `from airflow.providers.standard.sensors.external_task import ExternalTaskSensor` |
| TimeSensor | `from airflow.sensors.time_sensor import TimeSensor` | `from airflow.providers.standard.sensors.time import TimeSensor` |
| TimeDeltaSensor | `from airflow.sensors.time_delta import TimeDeltaSensor` | `from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor` |

### 4. SDK imports — `airflow.models` / `airflow.utils` / `airflow.decorators` to `airflow.sdk`

| What | Before | After |
|---|---|---|
| Variable | `from airflow.models import Variable` | `from airflow.sdk import Variable` |
| Connection | `from airflow.models import Connection` | `from airflow.sdk import Connection` |
| TaskGroup | `from airflow.utils.task_group import TaskGroup` | `from airflow.sdk import TaskGroup` |
| BaseOperator | `from airflow.models import BaseOperator` | `from airflow.sdk import BaseOperator` |
| BaseHook | `from airflow.hooks.base import BaseHook` | `from airflow.sdk import BaseHook` |
| Context | `from airflow.utils.context import Context` | `from airflow.sdk import Context` |
| chain | `from airflow.models.baseoperator import chain` | `from airflow.sdk import chain` |
| Param | `from airflow.models.param import Param` | `from airflow.sdk import Param` |
| @dag | `from airflow.decorators import dag` | `from airflow.sdk import dag` |
| @task | `from airflow.decorators import task` | `from airflow.sdk import task` |
| @task_group | `from airflow.decorators import task_group` | `from airflow.sdk import task_group` |
| @setup | `from airflow.decorators import setup` | `from airflow.sdk import setup` |
| @teardown | `from airflow.decorators import teardown` | `from airflow.sdk import teardown` |
| get_current_context | `from airflow.operators.python import get_current_context` | `from airflow.sdk import get_current_context` |

Combine multiple `airflow.sdk` imports into a single line where practical, e.g.:
```python
from airflow.sdk import DAG, Variable, TaskGroup
```

### 5. Dataset renamed to Asset

| Before | After |
|---|---|
| `from airflow.datasets import Dataset` | `from airflow.sdk import Asset` |
| `from airflow.datasets import DatasetAlias` | `from airflow.sdk import AssetAlias` |
| `from airflow.datasets import DatasetAll` | `from airflow.sdk import AssetAll` |
| `from airflow.datasets import DatasetAny` | `from airflow.sdk import AssetAny` |

Also rename all usage of `Dataset(...)` to `Asset(...)` in the file.

### 6. `schedule_interval` to `schedule`

| Before | After |
|---|---|
| `schedule_interval="0 0 * * *"` | `schedule="0 0 * * *"` |
| `schedule_interval="@daily"` | `schedule="@daily"` |
| `schedule_interval=timedelta(hours=1)` | `schedule=timedelta(hours=1)` |
| `timetable=...` | `schedule=...` |

### 7. `provide_context=True` — REMOVED

Delete `provide_context=True` from any `PythonOperator`, `BranchPythonOperator`, or `ShortCircuitOperator` call. Also remove it from `default_args` if present. Passing it raises `TypeError` in Airflow 3.

### 8. `execution_date` — REMOVED from context

Replace all callback/template references:

| Before | After |
|---|---|
| `context.get("execution_date")` | `context.get("logical_date")` |
| `context["execution_date"]` | `context["logical_date"]` |
| `context.get("next_execution_date")` | `context.get("data_interval_end")` |
| `context.get("prev_execution_date")` | `context.get("data_interval_start")` |
| `context.get("next_ds")` | Remove or compute from `logical_date` |
| `context.get("prev_ds")` | Remove or compute from `logical_date` |
| `context.get("tomorrow_ds")` | Remove or compute from `logical_date` |
| `context.get("yesterday_ds")` | Remove or compute from `logical_date` |

Also check Jinja templates: `{{ execution_date }}` should become `{{ logical_date }}`.

### 9. `TriggerDagRunOperator` — `execution_date` parameter removed

| Before | After |
|---|---|
| `TriggerDagRunOperator(..., execution_date=...)` | `TriggerDagRunOperator(..., logical_date=...)` |

### 10. `Variable.get()` — `default_var` renamed to `default`

| Before | After |
|---|---|
| `Variable.get("KEY", default_var="value")` | `Variable.get("KEY", default="value")` |

Positional usage `Variable.get("KEY", "value")` is unchanged.

### 11. `concurrency` DAG parameter renamed

| Before | After |
|---|---|
| `DAG(..., concurrency=16)` | `DAG(..., max_active_tasks=16)` |

### 12. Timezone-aware `start_date`

Replace naive datetimes with pendulum:

| Before | After |
|---|---|
| `from datetime import datetime` + `datetime(2024, 1, 1)` | `import pendulum` + `pendulum.datetime(2024, 1, 1)` |
| `from pendulum import datetime` + `datetime(2024, 1, 1)` | `import pendulum` + `pendulum.datetime(2024, 1, 1)` |
| `days_ago(N)` | `pendulum.today("UTC").add(days=-N)` |

If `from pendulum import datetime` shadows stdlib `datetime`, change to `import pendulum` and update all call sites. Remove `datetime` from `from datetime import datetime, timedelta` if no longer needed (keep `timedelta` if used).

### 13. `DummyOperator` to `EmptyOperator`

| Before | After |
|---|---|
| `from airflow.operators.dummy import DummyOperator` | `from airflow.providers.standard.operators.empty import EmptyOperator` |
| `DummyOperator(task_id="...")` | `EmptyOperator(task_id="...")` |

### 14. Unused imports

After applying all changes, remove any imports that are no longer referenced in the file (e.g., `PostgresUserPasswordProfileMapping`, `EmailOperator` if only in comments, etc.).

### 15. Task dependency chain inside DAG context

Ensure all `>>` / `<<` dependency chains are indented inside the `with DAG(...):` block. Code at column 0 after a `with DAG` block will not be associated with the DAG in Airflow 3.

### 16. Cosmos compatibility (if applicable)

Cosmos import paths are UNCHANGED. No action needed for:
```python
from cosmos import DbtTaskGroup, DbtDag, RenderConfig
from cosmos.config import ProfileConfig, ProjectConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
```

Verify `astronomer-cosmos>=1.10.0` is installed for Airflow 3 compatibility.

### 17. XCom pickling

If the file uses `xcom_push`/`xcom_pull` with non-JSON-serializable objects (e.g., pickled Python objects), these will fail with the default XCom backend. Ensure values are JSON-serializable or use a custom XCom backend.

---

## After applying changes

- Remove any now-unused imports
- Verify the file has no syntax errors
- Commit to branch `10419/airflow-upgrade-3.0` with message: `Upgrade <filename> to Airflow 3.1.3`
