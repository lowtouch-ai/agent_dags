# SRE Tradeideas DAGs

## Overview

SRE monitoring DAGs for the **Tradeideas** client. Each DAG collects Prometheus metrics, compares current vs previous period, generates an AI summary (via Ollama), compiles a markdown/HTML/PDF report, and emails it.

## Files

| File | DAG ID | Schedule | Environment |
|------|--------|----------|-------------|
| `sre-tradeideas-daily.py` | `sre-tradeideas_daily` | `30 5 * * *` (11:00 AM IST daily) | Pre-prod |
| `sre-tradeideas-weekly.py` | `sre-tradeideas_weekly` | `30 5 * * 1` (11:00 AM IST Monday) | Pre-prod |
| `sre-tradeideas-prod-daily.py` | `sre-tradeideas-prod_daily` | `30 5 * * *` (11:00 AM IST daily) | Production |
| `sre-tradeideas-prod-weekly.py` | `sre-tradeideas-prod_weekly` | `30 5 * * 1` (11:00 AM IST Monday) | Production |

Pre-prod and prod files differ only in Prometheus URL/credentials Variable keys (suffix `_PROD`).
Daily files compare today vs yesterday; weekly files compare this week vs last week.

## Airflow Version

These DAGs target **Airflow 3.0**. Use the correct import paths:

```python
from airflow.sdk import DAG, task, task_group, Variable
from airflow.providers.standard.operators.python import PythonOperator
```

Do NOT use deprecated imports (`from airflow import DAG`, `from airflow.models import Variable`, `from airflow.operators.python import PythonOperator`).

## Task Pipeline (DAG Structure)

```
Phase 1 - Data Collection (parallel):
  Current period:  node_cpu_today, node_memory_today, node_disk_today,
                   mysql_health_today, lke_pvc_storage_details, pod_details_today
  Previous period: node_cpu_yesterday, node_memory_yesterday, node_disk_yesterday,
                   mysql_health_yesterday, lke_pvc_storage_details_yesterday, pod_details_yesterday
  Static checks:   node_readiness_check, pod_restart_today, kubernetes_version_check,
                   kubernetes_eol_and_next_version, microk8s_expiry_check

Phase 2 - Comparisons (depend on both current + previous):
  [current, previous] >> comparison
  CPU, Memory, Disk, MySQL, PVC, Pod comparisons

Phase 3 - Report (sequential):
  overall_summary >> compile_sre_report >> generate_pdf >> convert_to_html >> send_sre_email
```

## XCom Pattern

Each metric has a 3-function pattern:

1. **Detailed fetch** (`fetch_*_detailed`) - Generates markdown table + pushes raw JSON via `xcom_push(key="{key}_data", value=json.dumps(data))`
2. **Basic fetch** (`fetch_*_basic`) - Pushes only raw JSON for comparison period
3. **Comparison** (`*_today_vs_yesterday` / `*_thisweek_vs_lastweek`) - Pulls both via `xcom_pull(task_ids="<task_id>", key="<key>_data")`

**Critical:** Always specify `task_ids` in `xcom_pull` when using custom keys. Without it, Airflow may return `None` even when the upstream task pushed data correctly.

```python
# Correct
raw = ti.xcom_pull(task_ids="node_cpu_today", key="node_cpu_today_data")

# Wrong - may return None
raw = ti.xcom_pull(key="node_cpu_today_data")
```

## Airflow Variables

All variables use the prefix `ltai.v1.sretradeideas.`.

### Required (no defaults)
- `SMTP_USER` / `SMTP_PASSWORD` - Email credentials
- `AGENT_PROMETHEUS_USER_TRADEIDEAS` / `AGENT_PROMETHEUS_PASSWORD_TRADEIDEAS` - Pre-prod Prometheus auth
- `AGENT_PROMETHEUS_USER_TRADEIDEAS_PROD` / `AGENT_PROMETHEUS_PASSWORD_TRADEIDEAS_PROD` - Prod Prometheus auth

### With defaults
- `SMTP_HOST` (default: `mail.authsmtp.com`)
- `SMTP_PORT` (default: `2525`)
- `SMTP_FROM_SUFFIX` (default: `via lowtouch.ai <webmaster@ecloudcontrol.com>`)
- `TRADEIDEAS_FROM_ADDRESS` / `TRADEIDEAS_TO_ADDRESS`
- `TRADEIDEAS_OLLAMA_HOST` (default: `http://agentomatic:8000/`)
- `TRADEIDEAS_PROMETHEUS_URL` (default: `https://ti-pre-prod-prometheus.lowtouchcloud.io`)
- `TRADEIDEAS_PROD_PROMETHEUS_URL` (default: `https://tiprod-promethues.lowtouchcloud.io`)
- `pod.namespaces` (default: `["alpha-prod","tipreprod-prod"]`)

## Key Helpers

- `get_node_name(instance)` - Resolves Prometheus instance IP to human-readable node name via `NODE_MAPPING`
- `query_prometheus_range(query, start, end, step)` - Range query returning a pandas DataFrame
- `query_prometheus_instant(query, time)` - Instant query returning a DataFrame
- `get_ai_response(prompt)` - Sends prompt to Ollama for AI-generated summary

## Adding a New Metric

1. Create `fetch_<metric>_detailed(ti, key, start, end, date_str)` - generates markdown + pushes `{key}_data`
2. Create `fetch_<metric>_basic(ti, key, start, end)` - pushes only `{key}_data`
3. Create `<metric>_today_vs_yesterday(ti, **context)` - pulls both with `task_ids`, compares, pushes markdown
4. Add PythonOperator tasks in the DAG block
5. Wire dependencies: `[today_task, yesterday_task] >> comparison_task`
6. Add the comparison task to `summary_dependencies`
7. Pull the comparison result in `overall_summary` and `compile_sre_report`
8. Repeat for all 4 files (daily, weekly, prod-daily, prod-weekly)

## Notes

- All times are IST-based (UTC+5:30). The DAG computes 24h windows (daily) or 7-day windows (weekly) anchored at 11:00 AM IST.
- Reports go through: markdown >> AI summary >> compiled markdown >> PDF >> HTML email
- The 4 files are largely duplicated. When changing shared logic, update all 4.
