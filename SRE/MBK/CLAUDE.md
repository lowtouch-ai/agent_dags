# SRE MBK DAG

## Overview

SRE monitoring DAG for the **MBK** client. Collects Prometheus metrics, compares today vs yesterday, generates per-metric AI summaries (via Ollama), compiles a markdown/HTML/PDF report, and emails it.

## Files

| File | DAG ID | Schedule | Environment |
|------|--------|----------|-------------|
| `sre-mbk_daily.py` | `sre-mbk_daily` | `30 5 * * *` (11:00 AM IST daily) | Production |

Single file only (no weekly or separate prod variant).

## Airflow Version

This DAG targets **Airflow 3.0**. Use the correct import paths:

```python
from airflow.sdk import DAG, task, task_group, Variable
from airflow.providers.standard.operators.python import PythonOperator
```

Do NOT use deprecated imports (`from airflow import DAG`, `from airflow.models import Variable`, `from airflow.operators.python import PythonOperator`).

## Differences from Tradeideas DAGs

| Aspect | Tradeideas | MBK |
|--------|-----------|-----|
| Files | 4 (daily/weekly x preprod/prod) | 1 (daily only) |
| Variable prefix | `ltai.v1.sretradeideas.` | `ltai.v1.srembk.` |
| Node mapping | Hardcoded `NODE_MAPPING` dict | Variable `ltai.v1.srembk.NODE_IP_MAPPING` (JSON) |
| MySQL health | Yes | No |
| LKE PVC storage | Yes | No |
| SSL cert expiry | No | Yes (`check_ssl_cert_expiry`) |
| K8s version check | Yes | No (only EOL check) |
| Per-metric AI summaries | No (single overall_summary) | Yes (summarize_node_cpu, summarize_node_memory, etc.) |
| Default pod namespaces | `["alpha-prod","tipreprod-prod"]` | `["ecloudcontrol-prod","ecloudcontrol-dev","cloudbourne-dev","cloudbourne-qa","cloudbourne-prod","alpha-dev","kube-system","haproxy-controller"]` |

## Task Pipeline (DAG Structure)

```
Phase 1 - Data Collection (parallel):
  Current period:  node_cpu_today, node_memory_today, node_disk_today, pod_details_today
  Previous period: node_cpu_yesterday, node_memory_yesterday, node_disk_yesterday, pod_details_yesterday
  Static checks:   node_readiness_check, pod_restart_today, ssl_cert_expiry_check,
                   kubernetes_eol_and_next_version, microk8s_expiry_check

Phase 2 - Comparisons (depend on both current + previous):
  [current, previous] >> comparison
  CPU, Memory, Disk, Pod comparisons

Phase 3 - Per-metric AI Summaries (sequential):
  summarize_node_cpu >> summarize_node_memory >> summarize_node_disk
    >> summarize_pod_restarts >> summarize_health_checks

Phase 4 - Report (sequential):
  overall_summary >> compile_sre_report >> generate_pdf >> convert_to_html >> send_sre_email
```

## XCom Pattern

Each metric follows a 3-function pattern:

1. **Detailed fetch** (`fetch_*_detailed`) - Generates markdown table + pushes raw JSON via `xcom_push(key="{key}_data", value=json.dumps(data))`
2. **Basic fetch** (`fetch_*_basic`) - Pushes only raw JSON for the previous period
3. **Comparison** (`*_today_vs_yesterday`) - Pulls both with `task_ids`, compares, pushes markdown result

All `xcom_pull` calls already use `task_ids` (correct pattern):

```python
raw = ti.xcom_pull(key="node_cpu_today_data", task_ids="node_cpu_today")
```

## Airflow Variables

All variables use the prefix `ltai.v1.srembk.`.

### Required (no defaults)
- `SMTP_USER` / `SMTP_PASSWORD` - Email credentials
- `AGENT_PROMETHEUS_USER_MBK` / `AGENT_PROMETHEUS_PASSWORD_MBK` - Prometheus auth

### With defaults
- `SMTP_HOST` (default: `mail.authsmtp.com`)
- `SMTP_PORT` (default: `2525`)
- `SMTP_FROM_SUFFIX` (default: `via lowtouch.ai <webmaster@ecloudcontrol.com>`)
- `MBK_FROM_ADDRESS` / `MBK_TO_ADDRESS`
- `MBK_OLLAMA_HOST` (default: `http://agentomatic:8000/`)
- `MBK_PROMETHEUS_URL` (default: `https://mbk-prometheus.lowtouchcloud.io`)
- `NODE_IP_MAPPING` - JSON dict of IP-to-node-name (fetched with `deserialize_json=True`)
- `pod.namespaces` (default: `["ecloudcontrol-prod","ecloudcontrol-dev","cloudbourne-dev","cloudbourne-qa","cloudbourne-prod","alpha-dev","kube-system","haproxy-controller"]`)

## Key Helpers

- `get_node_name(instance)` - Resolves Prometheus instance IP via the `NODE_IP_MAPPING` Variable (dynamic, not hardcoded)
- `query_prometheus_range(query, start, end, step)` - Range query returning a pandas DataFrame
- `query_prometheus_instant(query, time)` - Instant query returning a DataFrame
- `get_ai_response(prompt)` - Sends prompt to Ollama for AI-generated summary
- `check_ssl_cert_expiry(ti)` - Queries Blackbox exporter for SSL cert expiry dates

## Per-Metric Summary Functions

MBK has an extra summarization phase not present in Tradeideas. Each function pulls today's data + comparison, sends a focused prompt to Ollama, and pushes the AI response:

| Function | XCom Key | Prompt Focus |
|----------|----------|-------------|
| `summarize_node_cpu` | `summary_cpu` | CPU health, spike detection (>20% change) |
| `summarize_node_memory` | `summary_memory` | Low available memory, usage changes |
| `summarize_node_disk` | `summary_disk` | Disk usage trends |
| `summarize_pod_restarts` | `summary_pod_restarts` | Restart patterns |
| `summarize_health_checks` | `summary_health_checks` | Readiness, K8s EOL, certs, SSL |

These run sequentially after all data collection/comparison tasks complete, and before `overall_summary`.

## Adding a New Metric

1. Create `fetch_<metric>_detailed(ti, key, start, end, date_str)` - generates markdown + pushes `{key}_data`
2. Create `fetch_<metric>_basic(ti, key, start, end)` - pushes only `{key}_data`
3. Create `<metric>_today_vs_yesterday(ti, **context)` - pulls both with `task_ids`, compares, pushes markdown
4. (Optional) Create `summarize_<metric>(ti, **context)` - AI summary for the metric
5. Add PythonOperator tasks in the DAG block
6. Wire dependencies: `[today_task, yesterday_task] >> comparison_task`
7. Add to `summary_dependencies` and the summary chain
8. Pull results in `overall_summary` and `compile_sre_report`

## Notes

- All times are IST-based (UTC+5:30). The DAG computes 24h windows anchored at 11:00 AM IST.
- Reports go through: markdown >> per-metric AI summaries >> overall AI summary >> compiled markdown >> PDF >> HTML email
- Unlike Tradeideas, there is only 1 file to maintain.
