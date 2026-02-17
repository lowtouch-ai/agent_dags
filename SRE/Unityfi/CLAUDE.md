# SRE Unityfi DAG

## Overview

SRE monitoring DAG for the **Unityfi** client. Monitors **Windows VMs** via Prometheus (windows_exporter metrics), compares this week vs previous week, generates AI summaries (via Ollama), compiles a markdown/HTML/PDF report, and emails it every Thursday.

## Files

| File | DAG ID | Schedule | Environment |
|------|--------|----------|-------------|
| `sre-unityfi-weekly.py` | `sre-unityfi_weekly` | `30 05 * * 4` (11:00 AM IST Thursday) | Production |

Single file only (weekly, no daily variant).

## Airflow Version

This DAG targets **Airflow 3.0**. Use the correct import paths:

```python
from airflow.sdk import DAG, task, task_group, Variable
from airflow.providers.standard.operators.python import PythonOperator
```

Do NOT use deprecated imports (`from airflow import DAG`, `from airflow.models import Variable`, `from airflow.operators.python import PythonOperator`).

## Differences from Tradeideas / MBK DAGs

| Aspect | Tradeideas / MBK | Unityfi |
|--------|-----------------|---------|
| Platform | Linux nodes (node_exporter) | Windows VMs (windows_exporter) |
| Schedule | Daily (+ weekly for Tradeideas) | Weekly only (Thursdays) |
| Time windows | Thursday-to-Thursday (7-day) | Thursday-to-Thursday (7-day) |
| Time format | Unix timestamps | ISO 8601 strings with timezone |
| Metric fetch | Per-metric functions (CPU, Memory, Disk separately) | Single bulk fetch (`fetch_all_metrics_this_week`) |
| Comparison | Per-metric comparison functions | Single `generate_windows_vm_wow_comparison` |
| Disk metrics | Linux mountpoints | Windows drive letters (C:, D:, E:, F:) |
| Peak analysis | No | Yes (CPU, Memory, Disk peaks at 90%+ threshold) |
| Prometheus auth | Module-level `auth` object | Created per-call inside `query_prometheus_range` |
| No `task_ids` in `xcom_pull` | Fixed in Tradeideas/MBK | Still missing (uses key-only pulls) |

## Task Pipeline (DAG Structure)

```
Phase 1 - Data Collection (parallel):
  fetch_this_week     - Fetches all Windows VM metrics for current week
  fetch_previous_week - Fetches CPU/Memory metrics for previous week

Phase 2 - Analysis (sequential):
  gen_windows_wow  - Week-over-week comparison tables (CPU, Memory, Disk)
  gen_cpu_peaks    - High CPU peaks (>=90%)
  gen_mem_peaks    - High Memory peaks (>=90%)
  gen_disk_peaks   - High Disk peaks (>=90%, per drive letter)

Phase 3 - Report (sequential):
  ai_summary >> ai_conclusion >> compile_report >> generate_pdf >> convert_to_html >> send_sre_email
```

Dependencies:
```
[fetch_this, fetch_prev] >> gen_windows >> gen_cpu_peaks >> gen_mem_peaks >> gen_disk_peaks
[gen_windows, gen_cpu_peaks, gen_mem_peaks, gen_disk_peaks] >> ai_summary
ai_summary >> ai_conclusion >> compile_report >> generate_pdf >> convert_to_html >> send_sre_email
```

## XCom Pattern

Unlike Tradeideas/MBK, Unityfi uses a **bulk fetch** approach:

1. `fetch_all_metrics_this_week` - Runs all Prometheus queries at once, pushes a single JSON blob to `metrics_this_week`
2. `fetch_all_metrics_previous_week` - Same for previous week, pushes to `metrics_previous_week`
3. Analysis functions pull the full blob and extract what they need

XCom keys:
| Key | Pushed by | Contents |
|-----|-----------|----------|
| `metrics_this_week` | `fetch_this_week` | JSON dict of all metric results |
| `metrics_previous_week` | `fetch_previous_week` | JSON dict of previous week metrics |
| `period_this_week` | `fetch_this_week` | Human-readable period string |
| `period_previous_week` | `fetch_previous_week` | Human-readable period string |
| `section_windows_wow` | `gen_windows_wow` | Markdown WoW comparison tables |
| `section_high_cpu_peaks` | `gen_cpu_peaks` | Markdown CPU peaks table |
| `section_high_memory_peaks` | `gen_mem_peaks` | Markdown memory peaks table |
| `section_high_disk_peaks` | `gen_disk_peaks` | Markdown disk peaks table |
| `ai_weekly_summary` | `ai_summary` | AI-generated summary |
| `ai_conclusion_summary` | `ai_conclusion` | AI-generated conclusion |
| `sre_full_report` | `compile_report` | Final compiled markdown |

**Note:** `xcom_pull` calls currently do NOT use `task_ids`. This works because each key is unique across the DAG, but adding `task_ids` would be more robust (see Tradeideas CLAUDE.md for rationale).

## Airflow Variables

All variables use the prefix `ltai.v1.sreunityfi.`.

### Required (no defaults)
- `SMTP_USER` / `SMTP_PASSWORD` - Email credentials
- `AGENT_PROMETHEUS_USER_UNITYFI` / `AGENT_PROMETHEUS_PASSWORD_UNITYFI` - Prometheus auth (note: these lack the `ltai.v1.sreunityfi.` prefix)

### With defaults
- `SMTP_HOST` (default: `mail.authsmtp.com`)
- `SMTP_PORT` (default: `2525`)
- `SMTP_FROM_SUFFIX` (default: `via lowtouch.ai <webmaster@ecloudcontrol.com>`)
- `UNITYFI_FROM_ADDRESS` / `UNITYFI_TO_ADDRESS`
- `UNITYFI_OLLAMA_HOST` (default: `http://agentomatic:8000/`)
- `UNITYFI_PROMETHEUS_URL` (default: `https://unityfi-prod-promethues.lowtouchcloud.io`)

## Prometheus Metrics Collected

All metrics are Windows-specific (`windows_*` exporters):

| Metric Key | Query Type | Description |
|-----------|-----------|-------------|
| `windows_cpu_avg` / `windows_cpu_peak` | `avg_over_time` / `max_over_time` | CPU utilization % |
| `windows_mem_avg` / `windows_mem_peak` | `avg_over_time` / `max_over_time` | Memory utilization % |
| `windows_cdisk_avg` / `_peak` | `avg_over_time` / `max_over_time` | C: drive usage % |
| `windows_ddisk_avg` / `_peak` | Same | D: drive usage % |
| `windows_edisk_avg` / `_peak` | Same | E: drive usage % |
| `windows_fdisk_avg` / `_peak` | Same | F: drive usage % |

## Key Helpers

- `get_thursday_ranges()` - Computes Thursday-to-Thursday 7-day windows anchored at 11:00 AM IST; returns ISO 8601 strings
- `query_prometheus_range(query, start, end, step)` - Range query returning a pandas DataFrame; creates auth per-call from Variables
- `get_ai_response(prompt)` - Sends prompt to Ollama for AI-generated summary

## Notes

- Time windows are **Thursday-to-Thursday** (not Monday-based). The DAG runs every Thursday at 11:00 AM IST.
- Prometheus queries use ISO 8601 timestamps (not Unix epoch like Tradeideas/MBK).
- The previous week fetch only queries CPU and Memory averages (not disk or peaks), so WoW disk comparison is not available.
- Reports go through: bulk metrics >> analysis sections >> AI summary >> AI conclusion >> compiled markdown >> PDF >> HTML email.
